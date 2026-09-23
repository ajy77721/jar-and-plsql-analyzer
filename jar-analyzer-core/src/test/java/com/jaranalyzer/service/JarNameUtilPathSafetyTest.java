package com.jaranalyzer.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Guards the path-traversal fix in {@link JarNameUtil}.
 *
 * '.', '-' and '_' are all inside the safe-character set, so "." and ".." used to survive
 * normalization unchanged and were then used directly as directory names — an escape out of the
 * data root. These tests pin both the fix and, just as importantly, the fact that the fix did NOT
 * change the mapping for any ordinary name (existing stored analyses must keep resolving).
 */
class JarNameUtilPathSafetyTest {

    @Nested
    @DisplayName("dots-only keys cannot become relative path segments")
    class TraversalGuard {

        @Test
        void doubleDotIsNeutralized() {
            assertNotEquals("..", JarNameUtil.normalizeKey(".."));
            assertNotEquals("..", JarNameUtil.sanitize(".."));
        }

        @Test
        void singleDotIsNeutralized() {
            assertNotEquals(".", JarNameUtil.normalizeKey("."));
            assertNotEquals(".", JarNameUtil.sanitize("."));
        }

        @Test
        void longerDotRunsAreNeutralized() {
            for (String s : new String[]{"...", "....", "........"}) {
                assertFalse(JarNameUtil.normalizeKey(s).chars().allMatch(c -> c == '.'),
                        "dots-only key survived normalization: " + s);
            }
        }

        @Test
        void traversalViaJarSuffixIsNeutralized() {
            // "...jar" -> strips ".jar" -> ".." which is the dangerous case
            assertNotEquals("..", JarNameUtil.normalizeKey("...jar"));
        }

        @Test
        void separatorsAreStrippedSoNoNestedTraversal() {
            // Slashes are not in the safe set, so they become underscores and cannot form a path.
            assertFalse(JarNameUtil.normalizeKey("../../etc/passwd").contains("/"));
            assertFalse(JarNameUtil.sanitize("..\\..\\windows").contains("\\"));
        }
    }

    @Nested
    @DisplayName("ordinary names are unchanged — existing stored data must keep resolving")
    class BackwardCompatibility {

        @Test
        void plainJarNameStripsSuffixOnly() {
            assertEquals("my-service", JarNameUtil.normalizeKey("my-service.jar"));
        }

        @Test
        void dotsInsideNamesAreUntouched() {
            assertEquals("spring-core-6.1.2", JarNameUtil.normalizeKey("spring-core-6.1.2.jar"));
            assertEquals("a.b.c", JarNameUtil.normalizeKey("a.b.c"));
        }

        @Test
        void unsafeCharactersStillBecomeUnderscores() {
            assertEquals("my_service", JarNameUtil.normalizeKey("my service.jar"));
        }

        @Test
        void nullAndBlankStillYieldEmpty() {
            assertEquals("", JarNameUtil.normalizeKey(null));
            assertEquals("", JarNameUtil.normalizeKey("   "));
            assertEquals("", JarNameUtil.sanitize(null));
        }

        @Test
        void sanitizeKeepsJarSuffix() {
            assertEquals("my-service.jar", JarNameUtil.sanitize("my-service.jar"));
        }
    }
}
