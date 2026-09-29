package com.jaranalyzer.service;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.LocalArtifactStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A decompiled class must be written to its own {@link ArtifactKey} — {@code
 * jar/<id>/decompiled/<className>.java}, one artifact per class, never a whole-jar blob — and a
 * second decompile of the same class must be served straight from that artifact rather than
 * re-decompiling.
 *
 * <p>Uses a real {@link LocalArtifactStore} against a temp directory and a real one-class jar run
 * through CFR, not mocks — the same discipline as {@link DecompilerServiceNestedJarIndexTest}.
 */
class DecompilerServiceCacheTest {

    /** A trivial real .class file: java.lang.Object itself, lifted from the running JVM. */
    private static byte[] realClassBytes() throws IOException {
        try (var in = Object.class.getResourceAsStream("Object.class")) {
            assertNotNull(in, "java.lang.Object.class must be resolvable from the JDK");
            return in.readAllBytes();
        }
    }

    private static void writeJarWithOneClass(Path out, String classFilePath, byte[] classBytes) throws IOException {
        try (OutputStream os = Files.newOutputStream(out);
             JarOutputStream jos = new JarOutputStream(os)) {
            jos.putNextEntry(new JarEntry(classFilePath));
            jos.write(classBytes);
            jos.closeEntry();
        }
    }

    @Test
    void secondDecompileIsServedFromTheCachedPerClassArtifact(@TempDir Path tmp) throws Exception {
        Path dataDir = tmp.resolve("data").resolve("jar");
        Files.createDirectories(dataDir);
        ArtifactStore store = new LocalArtifactStore(tmp.resolve("data"));
        JarDataPaths paths = new JarDataPaths(dataDir.toString(), store);

        String jarId = "cache-demo";
        ArtifactKey jarKey = paths.storedJarKey(jarId);
        Path jarFile = tmp.resolve("source.jar");
        writeJarWithOneClass(jarFile, "java/lang/Object.class", realClassBytes());
        try (var in = Files.newInputStream(jarFile); OutputStream out = store.write(jarKey)) {
            in.transferTo(out);
        }

        DecompilerService svc = new DecompilerService(paths, store);
        String className = "java.lang.Object";

        ArtifactKey cacheKey = paths.decompiledClassKey(jarId, className);
        assertFalse(store.exists(cacheKey), "no cache entry before the first decompile");

        String cold = svc.decompile(jarKey, className);
        assertNotNull(cold, "java.lang.Object should decompile cleanly with CFR");
        assertTrue(store.exists(cacheKey), "decompile() must write the per-class cache artifact");

        // Exactly one artifact under decompiled/ — the one class actually requested, not a
        // whole-jar batch write.
        long decompiledArtifacts;
        try (var listed = store.list(paths.jarRootKey(jarId).resolve("decompiled"), true)) {
            decompiledArtifacts = listed.filter(e -> !e.directory()).count();
        }
        assertEquals(1, decompiledArtifacts, "only the requested class should be cached, not the whole jar");

        String warm = svc.decompile(jarKey, className);
        assertEquals(cold, warm, "cache hit must return byte-identical source to the cold decompile");

        // Prove it really was a cache hit and not a lucky re-decompile: corrupt the jar so a
        // second real decompile attempt would fail, then confirm the cached read still succeeds.
        try (OutputStream out = store.write(jarKey)) {
            out.write("not a jar".getBytes(StandardCharsets.UTF_8));
        }
        String stillWarm = svc.decompile(jarKey, className);
        assertEquals(cold, stillWarm,
                "a cache hit must not touch the (now-corrupt) jar at all");
    }

    /**
     * §New: {@code decompileWithMeta} must report which path served the response ({@code JAR} on
     * a live decompile, {@code CACHE} on a hit), and an expired cache entry must be evicted and
     * re-decompiled live rather than served stale. Uses a real millisecond TTL (via the
     * {@link java.time.Duration} constructor) so the test can wait it out for real instead of
     * mocking the clock.
     */
    @Test
    void reportsOriginAndHonoursTtlExpiry(@TempDir Path tmp) throws Exception {
        Path dataDir = tmp.resolve("data").resolve("jar");
        Files.createDirectories(dataDir);
        ArtifactStore store = new LocalArtifactStore(tmp.resolve("data"));
        JarDataPaths paths = new JarDataPaths(dataDir.toString(), store);

        String jarId = "ttl-demo";
        ArtifactKey jarKey = paths.storedJarKey(jarId);
        Path jarFile = tmp.resolve("source.jar");
        writeJarWithOneClass(jarFile, "java/lang/Object.class", realClassBytes());
        try (var in = Files.newInputStream(jarFile); OutputStream out = store.write(jarKey)) {
            in.transferTo(out);
        }

        DecompilerService svc = new DecompilerService(paths, store, java.time.Duration.ofMillis(200));
        String className = "java.lang.Object";

        DecompilerService.DecompileResult first = svc.decompileWithMeta(jarKey, className);
        assertNotNull(first);
        assertEquals(DecompilerService.DecompileSource.JAR, first.origin(),
                "first decompile of a class must report it came from a live decompile, not the cache");

        DecompilerService.DecompileResult second = svc.decompileWithMeta(jarKey, className);
        assertEquals(DecompilerService.DecompileSource.CACHE, second.origin(),
                "a repeat decompile within the TTL must report it was served from the cache");
        assertEquals(first.source(), second.source());

        Thread.sleep(300); // outlive the 200ms TTL

        DecompilerService.DecompileResult third = svc.decompileWithMeta(jarKey, className);
        assertEquals(DecompilerService.DecompileSource.JAR, third.origin(),
                "an expired cache entry must be evicted and re-decompiled live, not served stale");
        assertEquals(first.source(), third.source(), "the re-decompiled source is still the same class");
    }

    /**
     * §New: the enrich path resolves the jar ONCE and decompiles every class from that local file,
     * with the artifact key used ONLY as the decompile-cache identity. The decompiled class must
     * still be cached under {@code jar/<id>/decompiled/<class>.java}, a repeat call must be served
     * from that cache without reading the local jar again, and a {@code null} file must fall back
     * (return null) rather than throw.
     */
    @Test
    void decompilesFromLocalJarWithTheKeyAsCacheIdentity(@TempDir Path tmp) throws Exception {
        Path dataDir = tmp.resolve("data").resolve("jar");
        Files.createDirectories(dataDir);
        ArtifactStore store = new LocalArtifactStore(tmp.resolve("data"));
        JarDataPaths paths = new JarDataPaths(dataDir.toString(), store);

        String jarId = "local-once";
        ArtifactKey jarKey = paths.storedJarKey(jarId);
        Path storedJar = tmp.resolve("source.jar");
        writeJarWithOneClass(storedJar, "java/lang/Object.class", realClassBytes());
        try (var in = Files.newInputStream(storedJar); OutputStream out = store.write(jarKey)) {
            in.transferTo(out);
        }

        DecompilerService svc = new DecompilerService(paths, store);
        String className = "java.lang.Object";
        ArtifactKey cacheKey = paths.decompiledClassKey(jarId, className);

        String cold = svc.decompile(storedJar, jarKey, className);
        assertNotNull(cold, "Object must decompile from the one local jar file");
        assertTrue(store.exists(cacheKey), "the key must still be used as the cache identity");

        // Corrupt the local file: a second call must be a cache hit and not read it at all.
        Files.write(storedJar, "not a jar".getBytes(StandardCharsets.UTF_8));
        assertEquals(cold, svc.decompile(storedJar, jarKey, className),
                "a cache hit must be served without re-reading the local jar");

        assertNull(svc.decompile(null, jarKey, className),
                "a null local file must fall back safely, not throw");
    }

    /**
     * §New: {@code pinJar} materializes the artifact once (null-safe for a missing key/artifact),
     * so an enrich job that was not handed a local file still reads the jar once.
     */
    @Test
    void pinJarMaterializesOnceAndIsNullSafe(@TempDir Path tmp) throws Exception {
        Path dataDir = tmp.resolve("data").resolve("jar");
        Files.createDirectories(dataDir);
        ArtifactStore store = new LocalArtifactStore(tmp.resolve("data"));
        JarDataPaths paths = new JarDataPaths(dataDir.toString(), store);

        String jarId = "pin-demo";
        ArtifactKey jarKey = paths.storedJarKey(jarId);
        Path jarFile = tmp.resolve("source.jar");
        writeJarWithOneClass(jarFile, "java/lang/Object.class", realClassBytes());
        try (var in = Files.newInputStream(jarFile); OutputStream out = store.write(jarKey)) {
            in.transferTo(out);
        }

        DecompilerService svc = new DecompilerService(paths, store);
        try (DecompilerService.PinnedJar pinned = svc.pinJar(jarKey)) {
            assertNotNull(pinned, "an existing stored jar must pin");
            assertNotNull(pinned.path());
            assertTrue(Files.isRegularFile(pinned.path()));
        }
        assertNull(svc.pinJar(null), "a null key must pin nothing");
        assertNull(svc.pinJar(paths.storedJarKey("does-not-exist")),
                "a missing artifact must pin nothing, not throw");
    }

    @Test
    void decompilesAClassStoredUnderWebInfClasses(@TempDir Path tmp) throws Exception {
        Path dataDir = tmp.resolve("data").resolve("jar");
        Files.createDirectories(dataDir);
        ArtifactStore store = new LocalArtifactStore(tmp.resolve("data"));
        JarDataPaths paths = new JarDataPaths(dataDir.toString(), store);

        String jarId = "war-demo";
        ArtifactKey jarKey = paths.storedJarKey(jarId);
        Path warFile = tmp.resolve("source.war");
        writeJarWithOneClass(warFile, "WEB-INF/classes/java/lang/Object.class", realClassBytes());
        try (var in = Files.newInputStream(warFile); OutputStream out = store.write(jarKey)) {
            in.transferTo(out);
        }

        DecompilerService svc = new DecompilerService(paths, store);
        String source = svc.decompile(jarKey, "java.lang.Object");
        assertNotNull(source, "a class under WEB-INF/classes/ must decompile, not silently return null");
    }
}
