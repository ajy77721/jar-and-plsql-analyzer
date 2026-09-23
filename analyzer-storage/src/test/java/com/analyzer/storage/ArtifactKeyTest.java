package com.analyzer.storage;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@link ArtifactKey} is the single choke point for artifact location construction, so its
 * validation is what lets every caller drop its own traversal guard. These tests pin that
 * guarantee.
 */
class ArtifactKeyTest {

    @Test
    void joinsSegmentsWithSlash() {
        assertEquals("jar/demo/analysis.json",
                ArtifactKey.of("jar", "demo", "analysis.json").toString());
    }

    @Test
    void resolveAppendsWithoutMutating() {
        ArtifactKey base = ArtifactKey.of("jar", "demo");
        ArtifactKey child = base.resolve("endpoints", "1.json");
        assertEquals("jar/demo", base.toString());
        assertEquals("jar/demo/endpoints/1.json", child.toString());
    }

    @Test
    void parentDropsLastSegmentAndIsNullAtRoot() {
        assertEquals("jar/demo", ArtifactKey.of("jar", "demo", "a.json").parent().toString());
        assertNull(ArtifactKey.of("solo").parent());
    }

    @Test
    void leafIsFinalSegment() {
        assertEquals("a.json", ArtifactKey.of("jar", "demo", "a.json").leaf());
    }

    // ── the guarantees that matter ──────────────────────────────────────────

    @Test
    void rejectsRelativeSegments() {
        assertThrows(IllegalArgumentException.class, () -> ArtifactKey.of("jar", ".."));
        assertThrows(IllegalArgumentException.class, () -> ArtifactKey.of("jar", "."));
        assertThrows(IllegalArgumentException.class, () -> ArtifactKey.of("..", "etc", "passwd"));
    }

    @Test
    void rejectsEmbeddedSeparators() {
        assertThrows(IllegalArgumentException.class, () -> ArtifactKey.of("jar/demo"));
        assertThrows(IllegalArgumentException.class, () -> ArtifactKey.of("jar\\demo"));
        assertThrows(IllegalArgumentException.class, () -> ArtifactKey.of("a", "../b"));
    }

    @Test
    void rejectsNullBlankAndNul() {
        assertThrows(IllegalArgumentException.class, () -> ArtifactKey.of((String) null));
        assertThrows(IllegalArgumentException.class, () -> ArtifactKey.of(""));
        assertThrows(IllegalArgumentException.class, () -> ArtifactKey.of("   "));
        assertThrows(IllegalArgumentException.class, () -> ArtifactKey.of("a\0b"));
    }

    @Test
    void rejectsEmptyKey() {
        assertThrows(IllegalArgumentException.class, ArtifactKey::of);
    }

    @Test
    void resolveValidatesToo() {
        ArtifactKey base = ArtifactKey.of("jar");
        assertThrows(IllegalArgumentException.class, () -> base.resolve(".."));
        assertThrows(IllegalArgumentException.class, () -> base.resolve("a/b"));
    }

    @Test
    void toPathAlwaysStaysUnderBase() {
        Path base = Path.of("/data").toAbsolutePath();
        Path resolved = ArtifactKey.of("jar", "demo", "a.json").toPath(base).normalize();
        assertTrue(resolved.startsWith(base), resolved + " escaped " + base);
    }

    // ── S3 key mapping ──────────────────────────────────────────────────────

    @Test
    void s3KeyAppliesPrefixAndTolerAtesTrailingSlash() {
        ArtifactKey k = ArtifactKey.of("jar", "demo", "a.json");
        assertEquals("root/jar/demo/a.json", k.s3Key("root"));
        assertEquals("root/jar/demo/a.json", k.s3Key("root/"));
    }

    @Test
    void s3KeyWithoutPrefixIsBareKey() {
        ArtifactKey k = ArtifactKey.of("jar", "demo", "a.json");
        assertEquals("jar/demo/a.json", k.s3Key(null));
        assertEquals("jar/demo/a.json", k.s3Key(""));
    }

    @Test
    void equalityIsBySegments() {
        assertEquals(ArtifactKey.of("a", "b"), ArtifactKey.of("a", "b"));
        assertEquals(ArtifactKey.of("a", "b").hashCode(), ArtifactKey.of("a", "b").hashCode());
        assertNotEquals(ArtifactKey.of("a", "b"), ArtifactKey.of("a", "c"));
    }
}
