package com.analyzer.storage;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The prefetch pool must carry the calling thread's routing to its readers.
 *
 * <p>{@link PrefetchReadStore}'s fixed pool threads have no Spring request context, so a delegate
 * that routes by thread — the per-user SSO store — would resolve to the shared default target on
 * every prefetch read, exactly the silent wrong-bucket / empty-result failure
 * {@link ShallowTreeRequestScopeTest} covers for listing. The store below fails loudly if a worker
 * is read without the caller's "session"; with the scope propagated every worker has it.
 */
class PrefetchReadStoreScopeTest {

    /** Stand-in for the request-bound session a store would route on. */
    private static final ThreadLocal<String> SESSION = new ThreadLocal<>();

    private static final class ScopedStore implements RequestScopedArtifactStore {

        private final InMemoryArtifactStore inner = new InMemoryArtifactStore();

        @Override
        public Scope captureScope() {
            String captured = SESSION.get();
            if (captured == null) return Scope.NONE;
            return () -> {
                String previous = SESSION.get();
                SESSION.set(captured);
                return () -> {
                    if (previous == null) SESSION.remove();
                    else SESSION.set(previous);
                };
            };
        }

        private void requireSession() {
            if (SESSION.get() == null) {
                throw new IllegalStateException("worker called the store with no request session");
            }
        }

        @Override public OutputStream write(ArtifactKey key) throws IOException {
            requireSession();
            return inner.write(key);
        }

        @Override public OutputStream write(ArtifactKey key, long contentLength) throws IOException {
            requireSession();
            return inner.write(key, contentLength);
        }

        @Override public InputStream read(ArtifactKey key) throws IOException {
            requireSession();
            return inner.read(key);
        }

        @Override public InputStream read(ArtifactKey key, long offset, long length) throws IOException {
            requireSession();
            return inner.read(key, offset, length);
        }

        @Override public boolean exists(ArtifactKey key) {
            requireSession();
            return inner.exists(key);
        }

        @Override public long size(ArtifactKey key) {
            requireSession();
            return inner.size(key);
        }

        @Override public long lastModified(ArtifactKey key) {
            requireSession();
            return inner.lastModified(key);
        }

        @Override public boolean delete(ArtifactKey key) {
            requireSession();
            return inner.delete(key);
        }

        @Override public int deletePrefix(ArtifactKey prefix) throws IOException {
            requireSession();
            return inner.deletePrefix(prefix);
        }

        @Override public Stream<ArtifactEntry> list(ArtifactKey prefix, boolean recursive)
                throws IOException {
            requireSession();
            return inner.list(prefix, recursive);
        }

        @Override public void copy(ArtifactKey from, ArtifactKey to) throws IOException {
            requireSession();
            inner.copy(from, to);
        }

        @Override public void move(ArtifactKey from, ArtifactKey to) throws IOException {
            requireSession();
            inner.move(from, to);
        }

        @Override public LocalHandle materialize(ArtifactKey key) throws IOException {
            requireSession();
            return inner.materialize(key);
        }
    }

    private static void seed(InMemoryArtifactStore store, ArtifactKey key) throws IOException {
        try (OutputStream out = store.write(key)) {
            out.write("{}".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test
    void prefetchInstallsTheCallersScopeOnEveryReader() throws IOException {
        ScopedStore delegate = new ScopedStore();
        ArtifactKey root = ArtifactKey.of("jar").resolve("web-1.0.0-SNAPSHOT", "endpoints");
        seed(delegate.inner, root.resolve("a.json"));
        seed(delegate.inner, root.resolve("b.json"));

        PrefetchReadStore store = new PrefetchReadStore(delegate);
        int cached;
        SESSION.set("alice");
        try {
            // Without scope propagation every worker read throws (no session) and nothing is
            // cached — the prefetch silently degrades to read-through against the wrong target.
            cached = store.prefetch(root);
        } finally {
            SESSION.remove();
        }

        assertEquals(2, cached, "every worker read must run with the caller's routing scope");
    }

    @Test
    void aStoreWithoutRequestScopePrefetchesUnchanged() throws IOException {
        InMemoryArtifactStore plain = new InMemoryArtifactStore();
        ArtifactKey root = ArtifactKey.of("jar").resolve("web-1.0.0-SNAPSHOT", "endpoints");
        seed(plain, root.resolve("a.json"));

        PrefetchReadStore store = new PrefetchReadStore(plain);
        assertEquals(1, store.prefetch(root), "the no-scope fallback must be untouched");
        assertTrue(store.exists(root.resolve("a.json")));
    }
}
