package com.jaranalyzer.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("JarDataPaths — version-aware classTreeCache")
class JarDataPathsTest {

    @TempDir
    Path tmp;

    private JarDataPaths paths() {
        return new JarDataPaths(tmp.resolve("jar").toString());
    }

    @Test
    @DisplayName("null version returns default _class-tree.json")
    void nullVersionUsesDefault() {
        Path result = paths().classTreeCache("my-jar", null);
        assertEquals("_class-tree.json", result.getFileName().toString());
    }

    @Test
    @DisplayName("blank version returns default _class-tree.json")
    void blankVersionUsesDefault() {
        Path result = paths().classTreeCache("my-jar", "  ");
        assertEquals("_class-tree.json", result.getFileName().toString());
    }

    @Test
    @DisplayName("'corrected' version uses distinct cache file")
    void correctedVersionDistinctFile() {
        Path def = paths().classTreeCache("my-jar", null);
        Path corrected = paths().classTreeCache("my-jar", "corrected");
        assertNotEquals(def, corrected);
        assertEquals("_class-tree_corrected.json", corrected.getFileName().toString());
    }

    @Test
    @DisplayName("version with path-injection chars is sanitised")
    void versionPathInjectionSanitised() {
        Path result = paths().classTreeCache("my-jar", "../../evil");
        String name = result.getFileName().toString();
        assertFalse(name.contains("/"), "must not contain /");
        assertFalse(name.contains(".."), "must not contain ..");
        // sanitised version ends with .json under the jar root
        assertTrue(result.startsWith(paths().jarRoot("my-jar")));
    }

    @Test
    @DisplayName("zero-arg classTreeCache matches two-arg with null version")
    void zeroArgMatchesNullVersion() {
        JarDataPaths p = paths();
        assertEquals(p.classTreeCache("my-jar"), p.classTreeCache("my-jar", null));
    }

    @Test
    @DisplayName("different versions produce different files under the same jar root")
    void differentVersionsDifferentFiles() {
        JarDataPaths p = paths();
        Path v1 = p.classTreeCache("my-jar", "v1");
        Path v2 = p.classTreeCache("my-jar", "v2");
        assertNotEquals(v1, v2);
        assertEquals(v1.getParent(), v2.getParent(), "both must be under the same jar root");
    }
}
