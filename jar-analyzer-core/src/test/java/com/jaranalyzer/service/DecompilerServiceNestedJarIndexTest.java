package com.jaranalyzer.service;

import com.analyzer.storage.ArtifactKey;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The nested-JAR index must belong to the JAR it was built from.
 *
 * <p>Regression cover for a service-wide {@code packageToNestedJar} map guarded by one
 * {@code nestedJarIndexBuilt} boolean: it was filled from whichever artifact happened to be
 * decompiled first and then reused for every later one. Two Spring Boot fat JARs that bundle the
 * same package under {@code BOOT-INF/lib/} would therefore both resolve to the FIRST JAR's nested
 * library — the lookup succeeds, so nothing fails; the wrong artifact's bytes are simply returned.
 * And a package the first JAR did not bundle stayed missing for all subsequent JARs.
 */
class DecompilerServiceNestedJarIndexTest {

    /** Not a real class file — extractClassBytes copies entry bytes out verbatim. */
    private static byte[] marker(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    /** A nested library jar containing {@code com/shared/Widget.class} with recognisable bytes. */
    private static byte[] libJar(String marker) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(bos)) {
            zos.putNextEntry(new ZipEntry("com/shared/Widget.class"));
            zos.write(marker(marker));
            zos.closeEntry();
        }
        return bos.toByteArray();
    }

    /** A Spring Boot style fat jar whose only content is one bundled library. */
    private static void fatJar(Path out, String libName, byte[] lib) throws Exception {
        try (OutputStream os = Files.newOutputStream(out);
             JarOutputStream jos = new JarOutputStream(os)) {
            jos.putNextEntry(new JarEntry("BOOT-INF/lib/" + libName));
            jos.write(lib);
            jos.closeEntry();
        }
    }

    @Test
    void eachJarResolvesNestedClassesFromItsOwnBundledLibraries(@TempDir Path tmp) throws Exception {
        Path jarA = tmp.resolve("a.jar");
        Path jarB = tmp.resolve("b.jar");
        fatJar(jarA, "widget-a.jar", libJar("FROM-A"));
        fatJar(jarB, "widget-b.jar", libJar("FROM-B"));

        // Null collaborators: extractClassBytes works purely off the materialized local file and
        // the key it is told the file came from.
        DecompilerService svc = new DecompilerService(null, null);

        ArtifactKey keyA = ArtifactKey.of("jar", "a", "stored.jar");
        ArtifactKey keyB = ArtifactKey.of("jar", "b", "stored.jar");

        // A first — this is what used to poison the index for everything after it.
        byte[] fromA = svc.extractClassBytes(keyA, jarA, "com.shared.Widget");
        byte[] fromB = svc.extractClassBytes(keyB, jarB, "com.shared.Widget");

        assertNotNull(fromA, "com.shared.Widget should resolve from a.jar's bundled library");
        assertNotNull(fromB, "com.shared.Widget should resolve from b.jar's bundled library");
        assertEquals("FROM-A", new String(fromA, StandardCharsets.UTF_8));
        assertEquals("FROM-B", new String(fromB, StandardCharsets.UTF_8),
                "b.jar resolved against a.jar's nested-JAR index");

        // And re-reading A still gets A: caching per jar must not have swapped the entry.
        assertEquals("FROM-A",
                new String(svc.extractClassBytes(keyA, jarA, "com.shared.Widget"), StandardCharsets.UTF_8));
    }

    @Test
    void aPackageAbsentFromTheFirstJarIsStillFoundInTheSecond(@TempDir Path tmp) throws Exception {
        Path empty = tmp.resolve("empty.jar");
        try (OutputStream os = Files.newOutputStream(empty);
             JarOutputStream jos = new JarOutputStream(os)) {
            jos.putNextEntry(new JarEntry("BOOT-INF/classes/"));
            jos.closeEntry();
        }
        Path withLib = tmp.resolve("with-lib.jar");
        fatJar(withLib, "widget.jar", libJar("FROM-LIB"));

        DecompilerService svc = new DecompilerService(null, null);

        // First JAR bundles nothing: under the old shared index this cached an empty map as
        // "built" and every later JAR inherited the miss.
        assertNull(svc.extractClassBytes(ArtifactKey.of("jar", "empty", "stored.jar"), empty,
                "com.shared.Widget"));

        byte[] found = svc.extractClassBytes(ArtifactKey.of("jar", "withlib", "stored.jar"), withLib,
                "com.shared.Widget");
        assertNotNull(found, "second JAR's own bundled library was not indexed");
        assertEquals("FROM-LIB", new String(found, StandardCharsets.UTF_8));
    }
}
