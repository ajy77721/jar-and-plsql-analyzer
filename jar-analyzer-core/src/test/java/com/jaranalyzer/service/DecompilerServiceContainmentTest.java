package com.jaranalyzer.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The data-root containment check must keep refusing arbitrary outside paths, while still letting
 * the decompiler read a temp THIS app created from its own upload/materialize.
 *
 * <p>Regression cover for the live-staging failure: {@code ArtifactStore.materialize()} in
 * s3/mongo mode downloads to {@code ${java.io.tmpdir}/artifact-<n>-stored.jar} — outside the data
 * root — so the guard refused it for every class and source enrichment produced nothing.
 */
class DecompilerServiceContainmentTest {

    /** A trivial real .class file: java.lang.Object itself, lifted from the running JVM. */
    private static byte[] realClassBytes() throws IOException {
        try (var in = Object.class.getResourceAsStream("Object.class")) {
            assertNotNull(in, "java.lang.Object.class must be resolvable from the JDK");
            return in.readAllBytes();
        }
    }

    private static void writeJarWithOneClass(Path out, byte[] classBytes) throws IOException {
        try (OutputStream os = Files.newOutputStream(out);
             JarOutputStream jos = new JarOutputStream(os)) {
            jos.putNextEntry(new JarEntry("java/lang/Object.class"));
            jos.write(classBytes);
            jos.closeEntry();
        }
    }

    @Test
    void decompilesFromAppOwnedTempEvenThoughItIsOutsideTheDataRoot(@TempDir Path tmp) throws Exception {
        // Base dir deliberately elsewhere, so a /tmp path cannot key against it.
        JarDataPaths paths = new JarDataPaths(tmp.resolve("data").resolve("jar").toString());
        DecompilerService svc = new DecompilerService(paths, null);

        // The exact shape ArtifactStore.materialize produces.
        Path tempJar = Files.createTempFile("artifact-", "-stored.jar");
        try {
            writeJarWithOneClass(tempJar, realClassBytes());
            String source = svc.decompile(tempJar, "java.lang.Object");
            assertNotNull(source, "an app-owned materialize temp must still decompile");
            assertTrue(source.contains("class Object"), "CFR source expected, got: " + source);
        } finally {
            Files.deleteIfExists(tempJar);
        }
    }

    @Test
    void stillRefusesAnOutsidePathThatIsNotOneOfOurTemps(@TempDir Path tmp) throws Exception {
        JarDataPaths paths = new JarDataPaths(tmp.resolve("data").resolve("jar").toString());
        DecompilerService svc = new DecompilerService(paths, null);

        // Real jar, outside the data root, but NOT one of this app's temp prefixes — must be refused.
        Path outside = Files.createTempFile("not-app-owned-", ".jar");
        try {
            writeJarWithOneClass(outside, realClassBytes());
            assertNull(svc.decompile(outside, "java.lang.Object"),
                    "the containment guard must stay closed to non-app-owned outside paths");
        } finally {
            Files.deleteIfExists(outside);
        }
    }
}
