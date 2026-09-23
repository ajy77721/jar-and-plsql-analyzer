package com.analyzer.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.stream.Stream;

/**
 * An {@link ArtifactStore} whose backing implementation can be replaced at runtime, without any
 * consumer seeing a change.
 *
 * <h2>Why this exists</h2>
 *
 * <p>Every consumer injects the {@code ArtifactStore} bean directly — roughly forty
 * {@code @Qualifier("parserArtifactStore")} / {@code @Qualifier("jarArtifactStore")} constructor
 * injections across {@code unified-web}, {@code jar-analyzer-core} and friends — and holds that
 * reference for the life of the context. A bean is handed out exactly once, so "rebuild the store
 * and swap the bean" is not possible: the injection already happened. The only way to change where
 * an already-injected store reads and writes is for the value they hold to be a stable object whose
 * <em>inner</em> delegate can change.
 *
 * <p>This is that stable object. It is deliberately transparent — every method forwards to the
 * current delegate and nothing else — so the {@link ArtifactStore} abstraction is preserved: a
 * caller still cannot tell which backend (or which bucket) is live, only that it is some
 * {@code ArtifactStore}.
 *
 * <h2>Thread-safety and the swap boundary</h2>
 *
 * <p>{@link #swapTo} publishes the new delegate through a {@code volatile} field, so a swap is
 * atomic with respect to the next method call. An operation already in flight completes against
 * the delegate it started with; the swap takes effect for calls that begin afterwards. That is the
 * right granularity here — the store is only swapped when a user explicitly connects a new S3
 * target, and an analysis run in progress finishing its current artifact against the previous
 * store is exactly the behaviour that avoids a half-written object. The class never closes the old
 * delegate: unlike a database connection, a store holds no per-instance resources that must be
 * released (the S3 client is pooled and garbage-collected), so retaining an old one briefly costs
 * nothing.
 */
public final class SwappableArtifactStore implements ArtifactStore {

    private static final Logger log = LoggerFactory.getLogger(SwappableArtifactStore.class);

    /** A short label ("parser"/"jar") purely so the swap log line names what changed. */
    private final String name;

    /**
     * The live delegate. {@code volatile} is the whole synchronisation story: a swap is a single
     * reference assignment and every read is a volatile read, which is cheaper than a lock around
     * every store call and sufficient because the delegate itself is thread-safe.
     */
    private volatile ArtifactStore delegate;

    public SwappableArtifactStore(String name, ArtifactStore delegate) {
        if (delegate == null) throw new IllegalArgumentException("delegate store is required");
        this.name = name;
        this.delegate = delegate;
    }

    /**
     * Replaces the backing store. Called by {@code SsoStoreSwapper} once a new target has been
     * successfully built — never with a partially constructed store, so a caller looking at this
     * object always sees either the old, working backend or the new, working one.
     */
    public void swapTo(ArtifactStore newDelegate) {
        if (newDelegate == null) throw new IllegalArgumentException("new delegate store is required");
        ArtifactStore previous = this.delegate;
        this.delegate = newDelegate;
        log.info("[storage] {} store swapped: {} -> {}", name,
                previous.getClass().getSimpleName(), newDelegate.getClass().getSimpleName());
    }

    /** The currently live delegate, for diagnostics and tests. */
    public ArtifactStore delegate() {
        return delegate;
    }

    // ── pure delegation — no other behaviour lives here ────────────────────────

    @Override
    public OutputStream write(ArtifactKey key) throws IOException {
        return delegate.write(key);
    }

    @Override
    public OutputStream write(ArtifactKey key, long contentLength) throws IOException {
        return delegate.write(key, contentLength);
    }

    @Override
    public InputStream read(ArtifactKey key) throws IOException {
        return delegate.read(key);
    }

    @Override
    public InputStream read(ArtifactKey key, long offset, long length) throws IOException {
        return delegate.read(key, offset, length);
    }

    @Override
    public boolean exists(ArtifactKey key) {
        return delegate.exists(key);
    }

    @Override
    public long size(ArtifactKey key) {
        return delegate.size(key);
    }

    @Override
    public long lastModified(ArtifactKey key) {
        return delegate.lastModified(key);
    }

    @Override
    public boolean delete(ArtifactKey key) {
        return delegate.delete(key);
    }

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

    @Override
    public java.util.Map<ArtifactKey, ArtifactEntry> statPrefix(ArtifactKey prefix) throws IOException {
        return delegate.statPrefix(prefix);
    }
}
