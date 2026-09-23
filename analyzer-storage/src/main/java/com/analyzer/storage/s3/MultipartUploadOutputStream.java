package com.analyzer.storage.s3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * An {@link OutputStream} that uploads to S3, buffering one part at a time.
 *
 * <p>Exists because the callers that produce analysis JSON own their write loop — they hand
 * Jackson a stream and generate tokens into it — so the SDK's Transfer Manager (which wants a
 * file or a known-length body) cannot be used. This inverts that: the caller keeps its loop and
 * the parts go out underneath it. Nothing but a single part is ever held in memory, so a
 * multi-GB artifact costs bounded heap.
 *
 * <p><b>Length is usually not known up front.</b> The JSON is generated streaming, so we cannot
 * tell whether a single PutObject would do until close(). Hence: buffer up to one part; if
 * close() arrives before the buffer fills, do a single PutObject and never create a multipart
 * upload at all (the common case — most artifacts are small).
 *
 * <h2>Part sizing</h2>
 * S3 allows at most {@value #MAX_PARTS} parts, so a fixed part size imposes a hard ceiling on
 * object size: 16 MiB parts cap out at 156 GiB and 5 MiB parts at 48 GiB. Artifact size here is
 * effectively unbounded, so the part size <b>ramps</b>: it doubles every
 * {@value #PARTS_PER_TIER} parts, from {@value #INITIAL_PART_SIZE} bytes up to
 * {@value #MAX_PART_SIZE} bytes. That keeps small uploads cheap (a 64 KiB buffer that grows only
 * as bytes arrive) while raising the ceiling to {@link #maxSupportedBytes()} ≈ 1.69 TiB, without
 * ever needing to know the total length. See {@link #partsRequired(long)}.
 *
 * <p>Not thread-safe; one instance per write, which matches the single-threaded write path.
 */
public final class MultipartUploadOutputStream extends OutputStream {

    private static final Logger log = LoggerFactory.getLogger(MultipartUploadOutputStream.class);

    /** S3 hard limits. */
    public static final int MAX_PARTS = 10_000;
    /** S3 floor for every part except the last. */
    static final int MIN_PART_SIZE = 5 * 1024 * 1024;

    public static final int INITIAL_PART_SIZE = 16 * 1024 * 1024;      // 16 MiB
    public static final int MAX_PART_SIZE = 256 * 1024 * 1024;         // 256 MiB
    public static final int PARTS_PER_TIER = 1_000;                    // double size every N parts

    /**
     * Largest declared content length that still gets a guaranteed single PutObject. Above this
     * the length hint is ignored and the normal ramp applies, because the alternative is holding
     * an arbitrarily large caller-declared buffer in heap.
     */
    static final long SINGLE_PUT_MAX = 64L * 1024 * 1024;

    private static final int INITIAL_BUFFER = 64 * 1024;

    private final S3Client s3;
    private final String bucket;
    private final String key;

    private byte[] buffer;
    private int bufferLen;
    private int currentPartSize;

    /**
     * Fixed part size in bytes, or 0 for the normal ramp. Exists only so a test can drive the
     * genuine multipart path against a fake endpoint without moving 16 MiB per case; production
     * code never sets it, and S3's 5 MiB floor is only waived when it is set.
     */
    private final int fixedPartSize;

    private String uploadId;                                   // null until multipart starts
    private final List<CompletedPart> completed = new ArrayList<>();
    private int partNumber = 1;
    private long totalBytes;
    private boolean closed;

    MultipartUploadOutputStream(S3Client s3, String bucket, String key) {
        this(s3, bucket, key, -1);
    }

    /**
     * @param contentLength declared byte count, or -1 when unknown. A small known length pins the
     *                      first part large enough that close() can use a single PutObject.
     */
    MultipartUploadOutputStream(S3Client s3, String bucket, String key, long contentLength) {
        this(s3, bucket, key, contentLength, 0);
    }

    MultipartUploadOutputStream(S3Client s3, String bucket, String key, long contentLength,
                                int fixedPartSize) {
        this.s3 = s3;
        this.bucket = bucket;
        this.key = key;
        this.fixedPartSize = fixedPartSize;
        if (fixedPartSize > 0) {
            this.currentPartSize = fixedPartSize;
            this.buffer = new byte[Math.min(INITIAL_BUFFER, fixedPartSize)];
            return;
        }
        this.currentPartSize = partSizeForPart(1);
        if (contentLength >= 0 && contentLength <= SINGLE_PUT_MAX) {
            // Known and small: make the first "part" at least this big so the buffer never fills,
            // no multipart upload is created, and close() issues one PutObject.
            this.currentPartSize = (int) Math.max(currentPartSize, contentLength);
        }
        // Start small and grow. Allocating a full part up front reserved 16 MiB of heap for every
        // stream, including the many small sidecar files (_header.json is ~190 bytes).
        // ensureCapacity() grows towards currentPartSize as data actually arrives.
        this.buffer = new byte[(int) Math.min(Math.max(INITIAL_BUFFER,
                contentLength < 0 ? 0 : Math.min(contentLength, SINGLE_PUT_MAX)), currentPartSize)];
    }

    // ── part-size arithmetic (pure, unit-testable without a client) ──────────

    /** Size in bytes of part {@code n} (1-based) under the doubling ramp. */
    public static int partSizeForPart(int n) {
        int tier = (n - 1) / PARTS_PER_TIER;
        if (tier >= 31) return MAX_PART_SIZE;
        long size = (long) INITIAL_PART_SIZE << tier;
        return (int) Math.min(size, MAX_PART_SIZE);
    }

    /** How many parts a stream of {@code totalBytes} consumes. -1 if it exceeds {@link #MAX_PARTS}. */
    public static int partsRequired(long totalBytes) {
        if (totalBytes <= 0) return 1;
        long remaining = totalBytes;
        for (int n = 1; n <= MAX_PARTS; n++) {
            remaining -= partSizeForPart(n);
            if (remaining <= 0) return n;
        }
        return -1;
    }

    /** Largest object this ramp can upload: ~1.69 TiB. */
    public static long maxSupportedBytes() {
        long total = 0;
        for (int n = 1; n <= MAX_PARTS; n++) total += partSizeForPart(n);
        return total;
    }

    // ── stream ───────────────────────────────────────────────────────────────

    @Override
    public void write(int b) throws IOException {
        ensureCapacity(1);
        buffer[bufferLen++] = (byte) b;
        totalBytes++;
        flushPartIfFull();
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (off < 0 || len < 0 || off + len > b.length) throw new IndexOutOfBoundsException();
        int remaining = len;
        int pos = off;
        while (remaining > 0) {
            int space = currentPartSize - bufferLen;
            int chunk = Math.min(space, remaining);
            ensureCapacity(chunk);
            System.arraycopy(b, pos, buffer, bufferLen, chunk);
            bufferLen += chunk;
            totalBytes += chunk;
            pos += chunk;
            remaining -= chunk;
            flushPartIfFull();
        }
    }

    private void ensureCapacity(int extra) {
        if (bufferLen + extra <= buffer.length) return;
        int target = Math.min(currentPartSize, Math.max(buffer.length * 2, bufferLen + extra));
        byte[] bigger = new byte[target];
        System.arraycopy(buffer, 0, bigger, 0, bufferLen);
        buffer = bigger;
    }

    private void flushPartIfFull() throws IOException {
        if (bufferLen < currentPartSize) return;
        if (uploadId == null) beginMultipart();
        uploadPart(false);
        // Recomputed from the (already incremented) part number so the ramp is a pure function of
        // position, not of how uploadPart happened to be reached.
        currentPartSize = fixedPartSize > 0 ? fixedPartSize : partSizeForPart(partNumber);
    }

    private void beginMultipart() {
        CreateMultipartUploadResponse r = s3.createMultipartUpload(
                CreateMultipartUploadRequest.builder().bucket(bucket).key(key).build());
        uploadId = r.uploadId();
        log.debug("s3://{}/{}: began multipart upload", bucket, key);
    }

    private void uploadPart(boolean last) throws IOException {
        if (partNumber > MAX_PARTS) {
            abortQuietly();
            throw new IOException("S3 part limit (" + MAX_PARTS + ") exceeded for " + key
                    + " after " + totalBytes + " bytes — part-size ramp failed to keep up");
        }
        if (!last && fixedPartSize <= 0 && bufferLen < MIN_PART_SIZE) {
            throw new IOException("internal: non-final part below S3 minimum part size");
        }
        UploadPartResponse r;
        try {
            r = s3.uploadPart(
                    UploadPartRequest.builder().bucket(bucket).key(key)
                            .uploadId(uploadId).partNumber(partNumber).contentLength((long) bufferLen)
                            .build(),
                    RequestBody.fromInputStream(new ByteArrayInputStream(buffer, 0, bufferLen), bufferLen));
        } catch (RuntimeException e) {
            // A part can fail mid-write, long before close(). Abort here too, or the parts already
            // uploaded stay billable forever — and surface an IOException, because this is reached
            // through OutputStream.write() and a caller must not have to catch an SDK type.
            abortQuietly();
            closed = true;
            throw new IOException("Upload of part " + partNumber + " failed for s3://"
                    + bucket + "/" + key, e);
        }
        completed.add(CompletedPart.builder().partNumber(partNumber).eTag(r.eTag()).build());
        partNumber++;
        bufferLen = 0;
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        try {
            if (uploadId == null) {
                // Never grew past one part: a single PutObject is cheaper and needs no cleanup.
                s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key)
                                .contentLength(totalBytes).build(),
                        RequestBody.fromInputStream(
                                new ByteArrayInputStream(buffer, 0, bufferLen), bufferLen));
                log.debug("s3://{}/{}: single-part upload, {} bytes", bucket, key, totalBytes);
                return;
            }
            if (bufferLen > 0) uploadPart(true);
            s3.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                    .bucket(bucket).key(key).uploadId(uploadId)
                    .multipartUpload(CompletedMultipartUpload.builder().parts(completed).build())
                    .build());
            log.debug("s3://{}/{}: completed multipart, {} parts, {} bytes",
                    bucket, key, completed.size(), totalBytes);
        } catch (RuntimeException e) {
            // Abort so the parts are not billed indefinitely. A bucket lifecycle rule for
            // incomplete multipart uploads is still required as a backstop, because a hard
            // process kill never reaches this line.
            abortQuietly();
            throw new IOException("Upload failed for s3://" + bucket + "/" + key, e);
        } finally {
            buffer = null;
        }
    }

    private void abortQuietly() {
        if (uploadId == null) return;
        try {
            s3.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                    .bucket(bucket).key(key).uploadId(uploadId).build());
        } catch (RuntimeException e) {
            log.warn("Could not abort multipart upload for s3://{}/{}: {}",
                    bucket, key, e.getMessage());
        }
    }
}
