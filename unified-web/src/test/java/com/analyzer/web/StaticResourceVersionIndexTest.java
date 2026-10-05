package com.analyzer.web;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The content-hash index behind the versioned URLs: the hash is content-derived (so it changes when
 * the bytes change), the startup walk sees the real classpath tree, and relative/absolute references
 * normalise to the same rooted path.
 */
class StaticResourceVersionIndexTest {

    private static StaticResourceVersionIndex index;

    @BeforeAll
    static void index() {
        index = new StaticResourceVersionIndex();
        index.buildIndex();
    }

    @Test
    @DisplayName("the hash changes with content and is stable for identical content")
    void hashIsContentSensitive() {
        byte[] a = "console.log('a');".getBytes(StandardCharsets.UTF_8);
        byte[] b = "console.log('b');".getBytes(StandardCharsets.UTF_8);

        assertEquals(StaticResourceVersionIndex.contentHash(a), StaticResourceVersionIndex.contentHash(a));
        assertNotEquals(StaticResourceVersionIndex.contentHash(a), StaticResourceVersionIndex.contentHash(b),
                "a one-character change must produce a different version, or the browser keeps the old file");
    }

    @Test
    @DisplayName("the startup walk indexed the shared and per-app JS/CSS")
    void knownResourcesAreIndexed() {
        assertNotNull(index.versionFor("/shared/escape.js"));
        assertNotNull(index.versionFor("/parser/js/app.js"));
        assertNotNull(index.versionFor("/parser/css/variables.css"));
        assertNotNull(index.versionFor("/jar/style.css"));
    }

    @Test
    @DisplayName("an unknown path has no version")
    void unknownResourceHasNoVersion() {
        assertNull(index.versionFor("/does/not/exist.js"));
        assertNull(index.versionFor("/shared/../secret.js"));
    }

    @Test
    @DisplayName("relative references resolve against the page directory; absolute ones do not")
    void resolveNormalisesReferences() {
        assertEquals("/shared/escape.js",
                StaticResourceVersionIndex.resolve("/parser/", "../shared/escape.js"));
        assertEquals("/parser/css/variables.css",
                StaticResourceVersionIndex.resolve("/parser/", "css/variables.css"));
        assertEquals("/shared/escape.js",
                StaticResourceVersionIndex.resolve("/", "shared/escape.js"));
        assertEquals("/jar/js/utils.js",
                StaticResourceVersionIndex.resolve("/war/", "/jar/js/utils.js"));
    }

    @Test
    @DisplayName("baseDir is the page's own directory")
    void baseDirOfPage() {
        assertEquals("/parser/", StaticResourceVersionIndex.baseDir("/parser/index.html"));
        assertEquals("/parser/", StaticResourceVersionIndex.baseDir("/parser/"));
        assertEquals("/", StaticResourceVersionIndex.baseDir("/index.html"));
        assertEquals("/", StaticResourceVersionIndex.baseDir("/"));
    }
}
