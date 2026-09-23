package com.analyzer.storage.s3;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.LocalHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * S3-backed {@link ArtifactStore}.
 *
 * <p>Artifact size is treated as unbounded — analysis documents reach multiple GB — so nothing
 * here ever holds a whole object. Writes stream through {@link MultipartUploadOutputStream};
 * reads hand back the SDK's response stream directly; listings page lazily.
 *
 * <p><b>Latency, not bandwidth, is the cost model.</b> A HEAD is ~25 ms against ~10 µs for a local
 * stat, so anything that would be N round trips is expressed as one: {@link #statPrefix} is a
 * single listing and {@link #deletePrefix} batches 1,000 keys per DeleteObjects call.
 *
 * <p>Credentials are never handled by this class. The {@link S3Client} is built elsewhere from the
 * default AWS provider chain; nothing here reads, stores or logs a secret.
 */
public class S3ArtifactStore implements ArtifactStore {

    private static final Logger log = LoggerFactory.getLogger(S3ArtifactStore.class);

    /** DeleteObjects caps at 1,000 keys per request. */
    private static final int DELETE_BATCH = 1_000;

    /** CopyObject caps at 5 GiB; above that S3 requires a multipart copy. */
    private static final long MAX_COPY_OBJECT_BYTES = 5L * 1024 * 1024 * 1024;

    private final S3Client s3;
    private final String bucket;
    private final String prefix;

    public S3ArtifactStore(S3Client s3, String bucket, String prefix) {
        if (s3 == null) throw new IllegalArgumentException("s3 client is required");
        if (bucket == null || bucket.isBlank()) {
            throw new IllegalArgumentException("app.storage.s3.bucket must be set for mode=s3");
        }
        this.s3 = s3;
        this.bucket = bucket;
        this.prefix = normalizePrefix(prefix);
    }

    /** Trims blanks and any leading/trailing '/' so key joining has exactly one separator. */
    static String normalizePrefix(String raw) {
        if (raw == null) return "";
        String p = raw.trim();
        while (p.startsWith("/")) p = p.substring(1);
        while (p.endsWith("/")) p = p.substring(0, p.length() - 1);
        return p;
    }

    public String bucket() {
        return bucket;
    }

    /** The configured root prefix, without surrounding slashes. */
    public String rootPrefix() {
        return prefix;
    }

    /** Object key for an artifact. */
    String objectKey(ArtifactKey key) {
        return key.s3Key(prefix);
    }

    /** Listing prefix for a "directory" key — always ends in '/' so siblings do not leak in. */
    String listPrefix(ArtifactKey key) {
        return objectKey(key) + "/";
    }

    // ── writes ───────────────────────────────────────────────────────────────

    @Override
    public OutputStream write(ArtifactKey key) {
        return newUpload(objectKey(key), -1);
    }

    @Override
    public OutputStream write(ArtifactKey key, long contentLength) {
        // A known, small length lets the stream commit with a single PutObject instead of a
        // three-request multipart dance. A wrong (too small) hint is harmless: the stream simply
        // rolls over into multipart as the bytes keep coming.
        return newUpload(objectKey(key), contentLength);
    }

    /**
     * Sole construction point for the upload stream. Overridable (package-private) purely so a
     * test can shrink the part size and drive the real multipart path cheaply; production
     * behaviour is this one line.
     */
    OutputStream newUpload(String objectKey, long contentLength) {
        return new MultipartUploadOutputStream(s3, bucket, objectKey, contentLength);
    }

    /** The client, for a subclass that needs to build its own upload stream. */
    S3Client client() {
        return s3;
    }

    // ── reads ────────────────────────────────────────────────────────────────

    @Override
    public InputStream read(ArtifactKey key) throws IOException {
        try {
            return s3.getObject(GetObjectRequest.builder().bucket(bucket).key(objectKey(key)).build());
        } catch (NoSuchKeyException e) {
            throw new NoSuchFileException(key.toString());
        }
    }

    @Override
    public InputStream read(ArtifactKey key, long offset, long length) throws IOException {
        try {
            InputStream in = s3.getObject(GetObjectRequest.builder()
                    .bucket(bucket).key(objectKey(key)).range(rangeHeader(offset, length)).build());
            // A zero-length range has no HTTP representation, so rangeHeader asks for one byte.
            // Locally a zero length yields an immediately-empty stream; discard the byte to match.
            return length == 0 ? new EmptyOnTop(in) : in;
        } catch (NoSuchKeyException e) {
            throw new NoSuchFileException(key.toString());
        } catch (S3Exception e) {
            // LocalArtifactStore's skipNBytes throws EOFException when the offset is past the end
            // of the file; S3 answers 416 InvalidRange. Translate so callers see one behaviour.
            if (e.statusCode() == 416) {
                throw new EOFException("range " + rangeHeader(offset, length) + " past end of " + key);
            }
            throw e;
        }
    }

    /**
     * HTTP Range for the (offset, length) contract. Range is inclusive at BOTH ends, so the last
     * byte is {@code offset + length - 1}; a zero length has no valid representation and is
     * short-circuited by the caller-visible contract of returning nothing.
     */
    static String rangeHeader(long offset, long length) {
        if (offset < 0) throw new IllegalArgumentException("offset must be >= 0, was " + offset);
        if (length < 0) return "bytes=" + offset + "-";
        if (length == 0) return "bytes=" + offset + "-" + offset;   // smallest legal range
        return "bytes=" + offset + "-" + (offset + length - 1);
    }

    // ── metadata ─────────────────────────────────────────────────────────────

    /** @return null when the object does not exist; a 404 is an answer here, not a failure. */
    private HeadObjectResponse head(ArtifactKey key) {
        try {
            return s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(objectKey(key)).build());
        } catch (NoSuchKeyException | NoSuchBucketException e) {
            return null;
        } catch (S3Exception e) {
            // HeadObject has no body, so the SDK cannot always model the error; a bare 404 comes
            // back as a generic S3Exception.
            if (e.statusCode() == 404) return null;
            throw e;
        }
    }

    @Override
    public boolean exists(ArtifactKey key) {
        return head(key) != null;
    }

    @Override
    public long size(ArtifactKey key) {
        HeadObjectResponse h = head(key);
        return h == null ? -1 : h.contentLength();
    }

    @Override
    public long lastModified(ArtifactKey key) {
        HeadObjectResponse h = head(key);
        return h == null ? -1 : h.lastModified().toEpochMilli();
    }

    // ── deletes ──────────────────────────────────────────────────────────────

    @Override
    public boolean delete(ArtifactKey key) {
        // DeleteObject is idempotent and reports success for a missing key, so the HEAD is the
        // only way to honour the interface's boolean.
        if (!exists(key)) return false;
        s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(objectKey(key)).build());
        return true;
    }

    @Override
    public int deletePrefix(ArtifactKey prefixKey) {
        String p = listPrefix(prefixKey);
        int removed = 0;
        List<ObjectIdentifier> batch = new ArrayList<>(DELETE_BATCH);
        Iterator<ListObjectsV2Response> pages =
                pages(ListObjectsV2Request.builder().bucket(bucket).prefix(p).build());
        while (pages.hasNext()) {
            for (S3Object o : pages.next().contents()) {
                batch.add(ObjectIdentifier.builder().key(o.key()).build());
                if (batch.size() == DELETE_BATCH) {
                    removed += deleteBatch(batch);
                    batch.clear();
                }
            }
        }
        if (!batch.isEmpty()) removed += deleteBatch(batch);
        return removed;
    }

    private int deleteBatch(List<ObjectIdentifier> batch) {
        DeleteObjectsResponse r = s3.deleteObjects(DeleteObjectsRequest.builder()
                .bucket(bucket)
                .delete(Delete.builder().objects(batch).quiet(true).build())
                .build());
        int failed = r.errors() == null ? 0 : r.errors().size();
        if (failed > 0) {
            log.warn("{} object(s) failed to delete, first: {}", failed, r.errors().get(0).message());
        }
        return batch.size() - failed;
    }

    // ── listing ──────────────────────────────────────────────────────────────

    /**
     * Lazy page iterator. Hand-rolled rather than the SDK paginator so that {@link #list} can be
     * a genuinely lazy {@link Stream} with nothing to leak on close — the only state is a
     * continuation token, and abandoning the iterator abandons an in-flight page at worst.
     */
    private Iterator<ListObjectsV2Response> pages(ListObjectsV2Request first) {
        return new Iterator<>() {
            private ListObjectsV2Request next = first;

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public ListObjectsV2Response next() {
                if (next == null) throw new NoSuchElementException();
                ListObjectsV2Response page = s3.listObjectsV2(next);
                next = Boolean.TRUE.equals(page.isTruncated()) && page.nextContinuationToken() != null
                        ? next.toBuilder().continuationToken(page.nextContinuationToken()).build()
                        : null;
                return page;
            }
        };
    }

    private Stream<ListObjectsV2Response> pageStream(ListObjectsV2Request first) {
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(pages(first), Spliterator.ORDERED | Spliterator.NONNULL),
                false);
    }

    @Override
    public Stream<ArtifactEntry> list(ArtifactKey prefixKey, boolean recursive) {
        String p = listPrefix(prefixKey);
        ListObjectsV2Request.Builder req = ListObjectsV2Request.builder().bucket(bucket).prefix(p);
        // Non-recursive: let S3 roll everything below the next '/' into a common prefix, rather
        // than listing millions of keys and filtering client-side.
        if (!recursive) req.delimiter("/");

        // flatMap over the page stream keeps this lazy: one HTTP round trip per 1,000 entries
        // consumed, and none at all if the caller closes the stream after the first few.
        return pageStream(req.build()).flatMap(page -> {
            List<ArtifactEntry> entries = new ArrayList<>(
                    page.commonPrefixes().size() + page.contents().size());
            for (CommonPrefix cp : page.commonPrefixes()) {
                ArtifactKey k = relativeKey(prefixKey, p, cp.prefix());
                if (k != null) entries.add(new ArtifactEntry(k, 0, -1, true));
            }
            for (S3Object o : page.contents()) {
                ArtifactKey k = relativeKey(prefixKey, p, o.key());
                if (k == null) continue;
                entries.add(new ArtifactEntry(k, o.size(), o.lastModified().toEpochMilli(), false));
            }
            return entries.stream();
        });
    }

    /** Turns an absolute object key back into an {@link ArtifactKey} under {@code base}. */
    private static ArtifactKey relativeKey(ArtifactKey base, String listPrefix, String objectKey) {
        if (!objectKey.startsWith(listPrefix)) return null;
        String rel = objectKey.substring(listPrefix.length());
        while (rel.endsWith("/")) rel = rel.substring(0, rel.length() - 1);
        if (rel.isEmpty()) return null;      // the zero-byte "directory marker" for the prefix itself
        ArtifactKey key = base;
        for (String seg : rel.split("/")) {
            if (seg.isEmpty()) return null;
            key = key.resolve(seg);
        }
        return key;
    }

    /**
     * One listing, never N HEADs. This is the whole reason the method is on the interface: the
     * loop-of-HEADs version of this costs ~25 ms per artifact.
     */
    @Override
    public Map<ArtifactKey, ArtifactEntry> statPrefix(ArtifactKey prefixKey) {
        String p = listPrefix(prefixKey);
        Map<ArtifactKey, ArtifactEntry> out = new LinkedHashMap<>();
        Iterator<ListObjectsV2Response> pages =
                pages(ListObjectsV2Request.builder().bucket(bucket).prefix(p).build());
        while (pages.hasNext()) {
            for (S3Object o : pages.next().contents()) {
                ArtifactKey k = relativeKey(prefixKey, p, o.key());
                if (k == null) continue;
                out.put(k, new ArtifactEntry(k, o.size(), o.lastModified().toEpochMilli(), false));
            }
        }
        return out;
    }

    // ── copy / move / materialize ────────────────────────────────────────────

    @Override
    public void copy(ArtifactKey from, ArtifactKey to) throws IOException {
        long len = size(from);
        if (len < 0) throw new NoSuchFileException(from.toString());
        // Above 5 GiB S3 requires UploadPartCopy. Rather than implement a multipart copy for a
        // path only used for small sidecars today, fail loudly instead of truncating silently.
        if (len > MAX_COPY_OBJECT_BYTES) {
            throw new IOException("copy() of " + len + " bytes exceeds S3's 5 GiB CopyObject limit; "
                    + "multipart copy is not implemented (" + from + ")");
        }
        s3.copyObject(CopyObjectRequest.builder()
                .sourceBucket(bucket).sourceKey(objectKey(from))
                .destinationBucket(bucket).destinationKey(objectKey(to)).build());
    }

    /** S3 has no rename: copy, then delete the source only once the copy has succeeded. */
    @Override
    public void move(ArtifactKey from, ArtifactKey to) throws IOException {
        copy(from, to);
        delete(from);
    }

    @Override
    public LocalHandle materialize(ArtifactKey key) throws IOException {
        // Keep the leaf name in the temp file name: JarFile and CFR both care about the .jar
        // suffix. ArtifactKey guarantees the leaf has no separators, so this is safe.
        Path temp = Files.createTempFile("artifact-", "-" + key.leaf());
        try (InputStream in = read(key)) {
            Files.copy(in, temp, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException | RuntimeException e) {
            try {
                Files.deleteIfExists(temp);
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
        return new LocalHandle() {
            @Override public Path path() { return temp; }
            @Override public boolean temporary() { return true; }
            @Override public void close() {
                try {
                    Files.deleteIfExists(temp);
                } catch (IOException e) {
                    log.warn("Could not delete materialized temp {}: {}", temp, e.getMessage());
                }
            }
        };
    }

    /** Reports EOF immediately, but still owns and closes the underlying response stream. */
    private static final class EmptyOnTop extends InputStream {
        private final InputStream in;
        EmptyOnTop(InputStream in) { this.in = in; }
        @Override public int read() { return -1; }
        @Override public int read(byte[] b, int off, int len) { return -1; }
        @Override public int available() { return 0; }
        @Override public void close() { abortQuietly(in); }
    }

    /**
     * Aborts a read without draining it.
     *
     * <p>Closing a {@link ResponseInputStream} normally reads the remainder of the body so the
     * connection can be reused. On a multi-GB object that is a multi-minute silent stall. Any code
     * path that stops reading early — every {@code stream*} method that finds what it wants and
     * returns — should abort instead of close.
     */
    public static void abortQuietly(InputStream in) {
        if (in instanceof ResponseInputStream<?> ris) {
            ris.abort();
        } else if (in != null) {
            try {
                in.close();
            } catch (IOException ignored) {
                // best effort
            }
        }
    }
}
