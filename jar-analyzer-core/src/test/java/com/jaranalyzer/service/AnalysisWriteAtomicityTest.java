package com.jaranalyzer.service;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.jaranalyzer.model.ClassInfo;
import com.jaranalyzer.model.EndpointInfo;
import com.jaranalyzer.model.JarAnalysis;
import com.jaranalyzer.model.MethodInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A failed analysis write must not damage the analysis that is already stored.
 *
 * <p>This is a regression test for real data loss. An encoder error thrown 23.7 MB into a 53 MB
 * document ("Incomplete surrogate pair: first char 0xDF00") left <b>three</b> analyses truncated
 * with {@code Unexpected end-of-input}. Worse, {@code reanalyze-all} reads the metadata it needs
 * from the very file that had just been destroyed, so it reported <b>0 JARs queued</b>: the failure
 * removed both the data and the means of rebuilding it.
 *
 * <p>The fix is staging: the document is written to {@code <leaf>.partial} and committed with a
 * single {@code store.move}. {@code move} is already in the frozen {@code ArtifactStore} SPI, so
 * this needed no interface change.
 */
class AnalysisWriteAtomicityTest {

    @TempDir
    Path tmp;

    private static final String JAR = "demo.jar";

    private JarDataPaths paths(com.analyzer.storage.ArtifactStore store) {
        return new JarDataPaths(tmp.resolve("jar").toString(), store);
    }

    /** An analysis with a class whose name will be rejected by the failing store below. */
    private JarAnalysis analysis(String marker) {
        JarAnalysis a = new JarAnalysis();
        a.setJarName(JAR);
        a.setProjectName(marker);
        a.setTotalClasses(1);
        a.setAnalysisMode("STATIC");
        ClassInfo c = new ClassInfo();
        c.setFullyQualifiedName("com.demo." + marker);
        c.setSimpleName(marker);
        MethodInfo m = new MethodInfo();
        m.setName("run");
        m.setDescriptor("()V");
        c.getMethods().add(m);
        a.setClasses(List.of(c));
        a.setEndpoints(List.of());
        return a;
    }

    /**
     * A store that fails partway through writing one particular key.
     *
     * <p>Delegates rather than extends — {@link InMemoryArtifactStore} is final, which is the right
     * call for a conformance-tested reference implementation.
     */
    private static final class FailsMidWrite implements com.analyzer.storage.ArtifactStore {
        private final InMemoryArtifactStore delegate = new InMemoryArtifactStore();
        private final String failOnLeaf;
        private final long failAfterBytes;

        FailsMidWrite(String failOnLeaf, long failAfterBytes) {
            this.failOnLeaf = failOnLeaf;
            this.failAfterBytes = failAfterBytes;
        }

        java.util.Set<String> keys() { return delegate.keys(); }

        @Override
        public OutputStream write(ArtifactKey key) throws java.io.IOException {
            OutputStream out = delegate.write(key);
            if (!key.leaf().equals(failOnLeaf)) return out;
            return new OutputStream() {
                private long written;
                @Override public void write(int b) throws java.io.IOException {
                    if (++written > failAfterBytes) {
                        // Mirrors the real failure: the encoder throws mid-document.
                        throw new java.io.IOException("Incomplete surrogate pair: first char 0xDF00");
                    }
                    out.write(b);
                }
                @Override public void write(byte[] b, int off, int len) throws java.io.IOException {
                    for (int i = 0; i < len; i++) write(b[off + i]);
                }
                @Override public void close() throws java.io.IOException { out.close(); }
            };
        }

        @Override public OutputStream write(ArtifactKey k, long len) throws java.io.IOException { return write(k); }
        @Override public java.io.InputStream read(ArtifactKey k) throws java.io.IOException { return delegate.read(k); }
        @Override public java.io.InputStream read(ArtifactKey k, long o, long l) throws java.io.IOException { return delegate.read(k, o, l); }
        @Override public boolean exists(ArtifactKey k) { return delegate.exists(k); }
        @Override public long size(ArtifactKey k) { return delegate.size(k); }
        @Override public long lastModified(ArtifactKey k) { return delegate.lastModified(k); }
        @Override public boolean delete(ArtifactKey k) { return delegate.delete(k); }
        @Override public int deletePrefix(ArtifactKey p) throws java.io.IOException { return delegate.deletePrefix(p); }
        @Override public java.util.stream.Stream<com.analyzer.storage.ArtifactEntry> list(ArtifactKey p, boolean r) throws java.io.IOException { return delegate.list(p, r); }
        @Override public void copy(ArtifactKey f, ArtifactKey t) throws java.io.IOException { delegate.copy(f, t); }
        @Override public void move(ArtifactKey f, ArtifactKey t) throws java.io.IOException { delegate.move(f, t); }
        @Override public com.analyzer.storage.LocalHandle materialize(ArtifactKey k) throws java.io.IOException { return delegate.materialize(k); }
    }

    @Test
    @DisplayName("a write that fails mid-document leaves the PREVIOUS analysis intact")
    void failedWriteDoesNotDestroyTheExistingAnalysis() throws Exception {
        // A good analysis is already stored.
        var goodStore = new InMemoryArtifactStore();
        var goodPaths = paths(goodStore);
        new PersistenceService(goodPaths, goodStore).save(analysis("Good"));
        ArtifactKey key = goodPaths.analysisKey(JAR);
        byte[] before = goodStore.read(key).readAllBytes();
        assertTrue(before.length > 0);
        assertTrue(new String(before, StandardCharsets.UTF_8).contains("Good"));

        // Now a second write over the same store fails partway through.
        var failing = new FailsMidWrite("analysis.json.partial", 200);
        try (OutputStream out = failing.write(key)) {
            out.write(before);   // seed the same good document into the failing store
        }
        byte[] seeded = failing.read(key).readAllBytes();

        var service = new PersistenceService(paths(failing), failing);
        assertThrows(Exception.class, () -> service.save(analysis("Replacement")),
                "the seeded failure must surface, not be swallowed");

        // THE property: the old document is byte-for-byte untouched.
        byte[] after = failing.read(key).readAllBytes();
        assertArrayEquals(seeded, after,
                "a failed write must leave the previous analysis exactly as it was");
        assertTrue(new String(after, StandardCharsets.UTF_8).contains("Good"),
                "the surviving document must still be the old, complete one");
    }

    @Test
    @DisplayName("no staging artifact is left behind after a failure")
    void stagingIsCleanedUpOnFailure() {
        var failing = new FailsMidWrite("analysis.json.partial", 200);
        var service = new PersistenceService(paths(failing), failing);

        assertThrows(Exception.class, () -> service.save(analysis("Doomed")));

        // A leftover .partial is inert (nothing reads it) but it would accumulate one dead copy of
        // a multi-GB document per failure.
        long partials = failing.keys().stream().filter(k -> k.endsWith(".partial")).count();
        assertEquals(0, partials, "the staging object must be removed when the write fails");
    }

    @Test
    @DisplayName("a successful write commits and leaves no staging artifact")
    void successCommitsCleanly() throws Exception {
        var store = new InMemoryArtifactStore();
        var p = paths(store);
        new PersistenceService(p, store).save(analysis("Fresh"));

        assertTrue(store.exists(p.analysisKey(JAR)), "the analysis must be committed");
        assertEquals(0, store.keys().stream().filter(k -> k.endsWith(".partial")).count(),
                "staging must not survive a successful write");
        assertTrue(new String(store.read(p.analysisKey(JAR)).readAllBytes(), StandardCharsets.UTF_8)
                        .contains("Fresh"));
    }

    @Test
    @DisplayName("a replacement write swaps the document wholesale")
    void successfulReplacementIsComplete() throws Exception {
        var store = new InMemoryArtifactStore();
        var p = paths(store);
        var service = new PersistenceService(p, store);
        service.save(analysis("First"));
        service.save(analysis("Second"));

        String doc = new String(store.read(p.analysisKey(JAR)).readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(doc.contains("Second"), "the new analysis must be the one served");
        assertFalse(doc.contains("First"),
                "and no remnant of the previous document may survive the swap");
    }
}
