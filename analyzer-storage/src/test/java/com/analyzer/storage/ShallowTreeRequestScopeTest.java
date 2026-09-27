package com.analyzer.storage;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A fan-out must carry the calling thread's routing to its workers.
 *
 * <p>{@link ShallowTree}'s pool threads have no Spring request context, so a store that routes by
 * thread — the per-user SSO store — would resolve to the shared default target there. The store
 * doubles below fail loudly if a worker is asked to list without the caller's "session", which is
 * exactly what happened in production (the worker left with the default AWS chain and the read
 * failed into an empty list). With the scope propagated, every worker sees it.
 */
class ShallowTreeRequestScopeTest {

    /** Stand-in for the request-bound session a store would route on. */
    private static final ThreadLocal<String> SESSION = new ThreadLocal<>();

    /**
     * In-memory store that only answers a call made with a session bound, and captures one to bind
     * on workers. A real routing store behaves the same way: no session means the wrong target.
     */
    private static final class ScopedStore implements RequestScopedArtifactStore {

        private final InMemoryArtifactStore inner = new InMemoryArtifactStore();

        InMemoryArtifactStore inner() {
            return inner;
        }

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

    private static void seedAnalysis(InMemoryArtifactStore store, ArtifactKey jarDir)
            throws IOException {
        try (OutputStream out = store.write(jarDir.resolve("analysis.json"))) {
            out.write("{}".getBytes(StandardCharsets.UTF_8));
        }
    }

    /** Mirrors the real discovery rule: record at an analysis root, descend otherwise. */
    private static ShallowTree.Decision discover(ArtifactKey dir, List<ArtifactEntry> children) {
        return ShallowTree.hasFile(children, "analysis.json")
                ? ShallowTree.Decision.RECORD
                : ShallowTree.Decision.DESCEND;
    }

    @Test
    void findTargetsRoutesEveryWorkerAsTheCaller() throws IOException {
        ScopedStore store = new ScopedStore();
        seedAnalysis(store.inner(), ArtifactKey.of("jar").resolve("web-1.0.0-SNAPSHOT"));

        SESSION.set("alice");
        try {
            List<ShallowTree.Found> found =
                    ShallowTree.findTargets(store, ArtifactKey.of("jar"), 1,
                            ShallowTreeRequestScopeTest::discover);

            assertEquals(1, found.size());
            assertEquals("web-1.0.0-SNAPSHOT", found.get(0).dir().leaf());
        } finally {
            SESSION.remove();
        }
    }

    @Test
    void parallelMapRoutesEveryWorkerAsTheCaller() throws IOException {
        ScopedStore store = new ScopedStore();
        ArtifactKey key = ArtifactKey.of("jar").resolve("web-1.0.0-SNAPSHOT", "analysis.json");
        seedAnalysis(store.inner(), key.parent());

        SESSION.set("alice");
        try {
            List<String> seen = ShallowTree.parallelMap(store, List.of(key), k -> {
                // The read would throw on a worker with no propagated session.
                assertTrue(store.exists(k));
                return k.toString();
            });

            assertEquals(List.of(key.toString()), seen);
        } finally {
            SESSION.remove();
        }
    }

    @Test
    void aStoreWithoutRequestScopeStillWalksUnchanged() throws IOException {
        InMemoryArtifactStore plain = new InMemoryArtifactStore();
        seedAnalysis(plain, ArtifactKey.of("jar").resolve("web-1.0.0-SNAPSHOT"));

        List<ShallowTree.Found> found =
                ShallowTree.findTargets(plain, ArtifactKey.of("jar"), 1,
                        ShallowTreeRequestScopeTest::discover);

        assertEquals(1, found.size(), "the no-scope fallback must be untouched");
    }
}
