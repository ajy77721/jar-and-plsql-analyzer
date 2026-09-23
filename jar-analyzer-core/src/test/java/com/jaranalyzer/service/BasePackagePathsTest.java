package com.jaranalyzer.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Which {@code BOOT-INF/lib} JARs Phase 2 will look inside.
 *
 * <p>Phase 2 skips any nested JAR containing no class under the base package, and
 * {@code detectBasePackage} infers a <b>single</b> two-segment prefix. An organisation shipping a
 * second internal group could not express it, so that library was skipped entirely — its method
 * bodies and every call they made were absent from the analysis, with nothing marking the gap. An
 * interceptor or repository living there was simply invisible.
 *
 * <p>The single-value cases below are the important ones: they must behave exactly as before, or
 * every existing analysis and stored {@code basePackage} header changes meaning.
 */
class BasePackagePathsTest {

    @Test
    @DisplayName("one package behaves exactly as it always did")
    void singlePackageUnchanged() {
        assertEquals(List.of("com/acme/"), JarParserService.basePackagePaths("com.acme"));
        assertEquals(List.of("com/acme/platform/"),
                JarParserService.basePackagePaths("com.acme.platform"));
    }

    @Test
    @DisplayName("several packages are all accepted")
    void multiplePackages() {
        assertEquals(List.of("com/acme/platform/", "com/enterprise/"),
                JarParserService.basePackagePaths("com.acme.platform,com.enterprise"),
                "the second internal group is the whole point of the change");
    }

    @Test
    @DisplayName("whitespace around a comma is tolerated")
    void whitespaceTolerated() {
        assertEquals(List.of("com/a/", "com/b/", "com/c/"),
                JarParserService.basePackagePaths("com.a , com.b,  com.c "));
    }

    @Test
    @DisplayName("a trailing glob or dot is stripped per entry, not just at the end")
    void trailingGlobStripped() {
        // People write "com.acme.*" naturally, and the controller's sanitiser only strips the tail
        // of the whole string — so with a list, an inner "com.a.*" would otherwise become the
        // prefix "com/a/*/" and match nothing at all.
        assertEquals(List.of("com/a/", "com/b/"),
                JarParserService.basePackagePaths("com.a.*,com.b"));
        assertEquals(List.of("com/a/", "com/b/"),
                JarParserService.basePackagePaths("com.a.,com.b.*"));
    }

    @Test
    @DisplayName("duplicates collapse")
    void duplicatesCollapse() {
        assertEquals(List.of("com/a/"), JarParserService.basePackagePaths("com.a,com.a,com.a.*"));
    }

    @Test
    @DisplayName("absent or blank means no filtering, not filtering to nothing")
    void blankMeansNoFilter() {
        // An empty list is the signal Phase 2 uses to skip the peek entirely and scan every nested
        // JAR. Returning a list containing "" would instead match everything by accident, and
        // returning null would NPE the loop — both are wrong for different reasons.
        assertTrue(JarParserService.basePackagePaths(null).isEmpty());
        assertTrue(JarParserService.basePackagePaths("").isEmpty());
        assertTrue(JarParserService.basePackagePaths("   ").isEmpty());
        assertTrue(JarParserService.basePackagePaths(",,").isEmpty());
        assertTrue(JarParserService.basePackagePaths(".*").isEmpty(),
                "a bare glob carries no package information");
    }

    @Test
    @DisplayName("a prefix always ends in a separator so it cannot match a sibling package")
    void prefixIsSeparatorTerminated() {
        // Without the trailing slash, base package "com.acme" would also match "com/acmecorp/..."
        // and pull an unrelated third-party library into the scan.
        List<String> paths = JarParserService.basePackagePaths("com.acme");
        assertEquals("com/acme/", paths.get(0));
        assertFalse("com/acmecorp/Foo.class".startsWith(paths.get(0)));
        assertTrue("com/acme/Foo.class".startsWith(paths.get(0)));
    }
}
