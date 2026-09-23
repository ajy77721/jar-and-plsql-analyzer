package com.analyzer.storage.mongo;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.ReturnDocument;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;

/**
 * The write path for {@link MongoArtifactStore}, and the reason that store can honour
 * {@code ArtifactStore}'s atomic-on-close contract without a transaction.
 *
 * <p>Mirrors {@code MultipartUploadOutputStream}'s rollover shape: buffer small artifacts on the
 * heap, and the moment one byte arrives past the inline threshold, switch to streaming into GridFS.
 * PLAN-01 measured {@code _header.json} at 191 B and {@code connections.json} at 130 B against a
 * {@code stored.jar} at 70 MB and an {@code analysis.json} at 912 MB, so neither strategy alone is
 * right: GridFS costs two documents and two round trips for a 191-byte file, and a single BSON
 * document cannot hold 912 MB at all.
 *
 * <h2>Why this is atomic</h2>
 *
 * <p><b>The manifest document is the only thing that makes an artifact visible.</b> GridFS chunks
 * exist under a fresh {@link ObjectId} that nothing references until {@link #close()} writes the
 * manifest, and a single-document write in MongoDB is atomic. So exactly one operation flips
 * visibility, and the three conformance tests that pin this
 * ({@code abandonedWriteLeavesPreviousContentIntact}, {@code abandonedWriteOfNewKeyLeavesNothingVisible},
 * {@code closeIsIdempotent}) pass for a structural reason rather than a timing one.
 *
 * <p>Abandoning the stream therefore replaces nothing: the previous manifest still stands and still
 * points at the previous payload. The cost is that a rolled-over-but-abandoned GridFS file is an
 * orphan — invisible and unreachable, but occupying space until collected. That is the one thing
 * the local backend has no equivalent of, because it deletes its temp file on the way out.
 *
 * <h2>Replacing an artifact that was previously large</h2>
 *
 * <p>{@link #close()} uses {@code findOneAndReplace(returnDocument = BEFORE)} on <em>both</em>
 * paths, not just the GridFS one. The case that forces it: a key written large (GridFS) and then
 * rewritten small (inline). If the inline path used a plain {@code replaceOne}, the old GridFS file
 * would be silently orphaned on every overwrite — and {@code analysis_corrected.json} is rewritten
 * on every correction run, so this would leak the whole previous payload each time rather than
 * occasionally.
 */
final class GridFsRolloverOutputStream extends OutputStream {

    private static final Logger log = LoggerFactory.getLogger(GridFsRolloverOutputStream.class);

    private final MongoCollection<Document> manifests;
    private final GridFSBucket gridFs;
    private final String docId;
    private final String parent;
    private final String leaf;
    private final int inlineThreshold;

    /** Non-null until rollover; null afterwards, so the heap copy cannot be retained by accident. */
    private ByteArrayOutputStream buffer;

    /** Non-null after rollover. */
    private OutputStream gridFsUpload;
    private ObjectId gridFsId;

    private long written;
    private boolean closed;

    GridFsRolloverOutputStream(MongoCollection<Document> manifests, GridFSBucket gridFs,
                               String docId, String parent, String leaf,
                               int inlineThreshold, long contentLengthHint) {
        this.manifests = manifests;
        this.gridFs = gridFs;
        this.docId = docId;
        this.parent = parent;
        this.leaf = leaf;
        this.inlineThreshold = inlineThreshold;
        // The hint only sizes the initial buffer; it never decides the storage path. A caller that
        // declares 100 bytes and writes 100 MB must still produce a correct artifact, so rollover
        // is driven by the bytes actually written and the hint is advisory exactly as the
        // interface says. Clamped so a bogus hint cannot preallocate a huge array.
        int initial = contentLengthHint >= 0 && contentLengthHint <= inlineThreshold
                ? (int) Math.max(32, contentLengthHint)
                : Math.min(8 * 1024, inlineThreshold);
        this.buffer = new ByteArrayOutputStream(initial);
    }

    @Override
    public void write(int b) throws IOException {
        write(new byte[]{(byte) b}, 0, 1);
    }

    /**
     * The bulk path. Overriding this (rather than inheriting the byte-at-a-time
     * {@code OutputStream} default) is what keeps a 4 MiB write from becoming four million calls —
     * the regression {@code largeArtifactRoundTripsWithoutCorruption} exists to catch.
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (closed) throw new IOException("stream already closed: " + docId);
        if (len <= 0) return;
        try {
            if (buffer != null) {
                if (written + len <= inlineThreshold) {
                    buffer.write(b, off, len);
                    written += len;
                    return;
                }
                // This write is the one that crosses the threshold. Roll the whole thing over
                // rather than splitting it: partially inline and partially GridFS is not a state
                // this class has.
                rollOver();
            }
            gridFsUpload.write(b, off, len);
            written += len;
        } catch (RuntimeException e) {
            throw wrap(e);
        }
    }

    /**
     * Opens the GridFS upload and hands it the bytes buffered so far.
     *
     * <p>The file gets a fresh {@link ObjectId} and <b>nothing points at it</b> until
     * {@link #close()}. The heap buffer is released immediately, which is what bounds this class's
     * memory at {@code inlineThreshold} regardless of artifact size.
     */
    private void rollOver() throws IOException {
        byte[] pending = buffer.toByteArray();
        buffer = null;
        gridFsId = new ObjectId();
        gridFsUpload = gridFs.openUploadStream(new org.bson.BsonObjectId(gridFsId), docId);
        if (pending.length > 0) gridFsUpload.write(pending);
    }

    @Override
    public void flush() throws IOException {
        // Deliberately NOT a partial commit. flush() on this stream cannot make bytes visible,
        // because visibility is the manifest write in close() and nothing else — a caller that
        // flushes mid-write (BufferedOutputStream does, and ArtifactScanRepository wraps this
        // store's streams in one) must not thereby publish a half-written artifact.
        if (gridFsUpload != null) gridFsUpload.flush();
    }

    /** True while the payload is still on the heap. For assertions in tests. */
    boolean inline() {
        return buffer != null;
    }

    @Override
    public void close() throws IOException {
        if (closed) return;      // idempotent, as LocalArtifactStore:62,73-74
        closed = true;
        try {
            Document manifest = new Document("_id", docId)
                    .append("parent", parent)
                    .append("leaf", leaf)
                    .append("len", written)
                    .append("mtime", new Date());
            if (buffer != null) {
                manifest.append("inline", new Binary(buffer.toByteArray()));
                buffer = null;
            } else {
                gridFsUpload.close();      // chunks durable, still unreferenced
                manifest.append("gridfsId", gridFsId);
            }

            // The commit. One single-document write flips visibility; BEFORE tells us whether we
            // just displaced a payload that now needs collecting.
            Document previous = manifests.findOneAndReplace(
                    Filters.eq("_id", docId), manifest,
                    new FindOneAndReplaceOptions()
                            .upsert(true)
                            .returnDocument(ReturnDocument.BEFORE)
                            // Never pull the old `inline` payload back across the wire just to
                            // learn whether there was a gridfsId — that would make every overwrite
                            // of a 1 MiB artifact cost an extra 1 MiB read.
                            .projection(Projections.include("gridfsId")));

            deleteDisplacedGridFsFile(previous);
        } catch (RuntimeException e) {
            throw wrap(e);
        }
    }

    /**
     * Removes the GridFS payload this write displaced.
     *
     * <p>Best-effort by design: the manifest already points at the new payload, so the artifact is
     * correct and the caller's write has succeeded. Failing the {@code close()} here would report a
     * write error for an artifact that was written, which is the more damaging of the two outcomes
     * — a leaked chunk costs space, a spurious failure costs a re-run of the analysis. Logged at
     * WARN so it is not invisible.
     */
    private void deleteDisplacedGridFsFile(Document previous) {
        if (previous == null) return;
        ObjectId old = previous.getObjectId("gridfsId");
        if (old == null || old.equals(gridFsId)) return;
        try {
            gridFs.delete(old);
        } catch (RuntimeException e) {
            log.warn("[mongo] orphaned GridFS file {} displaced by a write to {}: {}",
                    old, docId, e.getMessage());
        }
    }

    private static IOException wrap(RuntimeException e) {
        return e instanceof com.mongodb.MongoException
                ? new IOException("MongoDB write failed: " + e.getMessage(), e)
                : new IOException(e);
    }
}
