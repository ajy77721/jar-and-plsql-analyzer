package com.analyzer.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Routes writes for selected key paths straight to S3, bypassing the hybrid size threshold.
 *
 * <p>Motivation: the PL/SQL parser's raw SQL sources (keys like
 * {@code plsql-parse/.../sources/ERP_USP_RUN_CYCLE.sql}) are individually small — under the hybrid
 * big-threshold — so pure size-based routing parks them in MongoDB. The intent for parser artifacts
 * is the opposite split: the <em>actual source</em> lives in S3, while everything else (the small
 * "details" JSON the entity-ingest reads to populate the {@code am_*} collections) stays in Mongo.
 * Size cannot express that; a path rule can.
 *
 * <p>Property: {@code app.storage.hybrid.s3-always-paths} — a comma-separated list of key path
 * <b>segments</b>. A key whose path contains any listed segment (e.g. the {@code sources} segment
 * in {@code plsql-parse/ORACLE/.../sources/X.fnc}) is written to S3 no matter how small it is.
 * Unmatched keys fall through to the wrapped hybrid store's normal size-based routing.
 *
 * <p>Only reads/metadata/listing/deletes delegate unconditionally: the hybrid store already tries
 * Mongo first and falls back to S3, which is exactly the "details from Mongo, source from S3"
 * read behaviour wanted here. Writes are the only thing this class intercepts — plus a best-effort
 * sweep of any stale Mongo copy after a routed write commits, so a key that used to be small
 * enough for Mongo cannot keep shadowing its new S3 copy (the same stale-copy problem
 * {@code HybridArtifactStore.cleanupOther} solves for the size split, mirrored for the path split).
 *
 * <p>Must wrap a {@link HybridArtifactStore} — the router reaches through it to the S3 half for
 * routed writes and the Mongo half for the stale-copy sweep. The credential guard still applies:
 * {@link CredentialGuardStore} sits at the boundary in every mode, and this class re-checks the
 * guard's leaf names so a routed path can never become a way around it.
 */
public final class S3AlwaysPathsStore implements ArtifactStore {

    private static final Logger log = LoggerFactory.getLogger(S3AlwaysPathsStore.class);

    private final ArtifactStore delegate;
    private final com.analyzer.storage.s3.S3ArtifactStore s3;
    private final com.analyzer.storage.mongo.MongoArtifactStore mongo;
    private final Set<String> pathSegments;

    /** @param pathSegments key path segments whose writes always go to S3; empty means no routing. */
    public S3AlwaysPathsStore(ArtifactStore delegate, Set<String> pathSegments) {
        // StorageFactory.create always returns the hybrid wrapped in CredentialGuardStore; unwrap
        // one level to reach the halves, but keep the guard OUTSIDE this router (the caller wraps
        // us around it) so routed writes still pass the credential check first.
        ArtifactStore backend = delegate instanceof CredentialGuardStore guard ? guard.delegate() : delegate;
        if (!(backend instanceof com.analyzer.storage.hybrid.HybridArtifactStore hybrid)) {
            throw new IllegalArgumentException(
                    "S3AlwaysPathsStore only makes sense over a HYBRID store, got "
                            + backend.getClass().getSimpleName());
        }
        this.delegate = delegate;
        this.s3 = hybrid.s3Half();
        this.mongo = hybrid.mongoHalf();
        this.pathSegments = Set.copyOf(pathSegments);
    }

    private boolean routesToS3(ArtifactKey key) {
        return !pathSegments.isEmpty()
                && !CredentialGuardStore.isCredentialFile(key)
                && key.segments().stream().anyMatch(pathSegments::contains);
    }

    // ── writes — the only intercepted operations ─────────────────────────────

    @Override
    public OutputStream write(ArtifactKey key) throws IOException {
        return write(key, -1);
    }

    @Override
    public OutputStream write(ArtifactKey key, long contentLength) throws IOException {
        if (!routesToS3(key)) return delegate.write(key, contentLength);
        return new SweepMongoOnClose(s3.write(key, contentLength), key);
    }

    /**
     * Closes the S3 write, then removes any stale Mongo copy of the same key — a source file
     * rewritten after a threshold change (or an older run of this code) could still sit in Mongo,
     * where {@link #read} would prefer it forever. Best-effort, like {@code cleanupOther}.
     */
    private final class SweepMongoOnClose extends OutputStream {
        private final OutputStream delegate;
        private final ArtifactKey key;
        private boolean closed;

        SweepMongoOnClose(OutputStream delegate, ArtifactKey key) {
            this.delegate = delegate;
            this.key = key;
        }

        @Override public void write(int b) throws IOException { delegate.write(b); }

        @Override public void write(byte[] b, int off, int len) throws IOException {
            delegate.write(b, off, len);
        }

        @Override public void flush() throws IOException { delegate.flush(); }

        @Override
        public void close() throws IOException {
            if (closed) return;
            closed = true;
            delegate.close();
            try {
                mongo.delete(key);
            } catch (RuntimeException e) {
                log.warn("[s3-always] could not sweep stale Mongo copy of {}: {}", key, e.getMessage());
            }
        }
    }

    // ── everything else delegates unchanged ──────────────────────────────────

    @Override
    public InputStream read(ArtifactKey key) throws IOException { return delegate.read(key); }

    @Override
    public InputStream read(ArtifactKey key, long offset, long length) throws IOException {
        return delegate.read(key, offset, length);
    }

    @Override
    public boolean exists(ArtifactKey key) { return delegate.exists(key); }

    @Override
    public long size(ArtifactKey key) { return delegate.size(key); }

    @Override
    public long lastModified(ArtifactKey key) { return delegate.lastModified(key); }

    @Override
    public boolean delete(ArtifactKey key) { return delegate.delete(key); }

    @Override
    public int deletePrefix(ArtifactKey prefix) throws IOException {
        return delegate.deletePrefix(prefix);
    }

    @Override
    public Stream<ArtifactEntry> list(ArtifactKey prefix, boolean recursive) throws IOException {
        return delegate.list(prefix, recursive);
    }

    @Override
    public void copy(ArtifactKey from, ArtifactKey to) throws IOException {
        delegate.copy(from, to);
    }

    @Override
    public void move(ArtifactKey from, ArtifactKey to) throws IOException {
        delegate.move(from, to);
    }

    @Override
    public LocalHandle materialize(ArtifactKey key) throws IOException {
        return delegate.materialize(key);
    }
}
