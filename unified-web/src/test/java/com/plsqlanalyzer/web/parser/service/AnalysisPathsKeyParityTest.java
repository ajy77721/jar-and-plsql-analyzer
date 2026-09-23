package com.plsqlanalyzer.web.parser.service;

import com.analyzer.storage.ArtifactKey;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Every {@code ArtifactKey} accessor on {@link AnalysisPaths} must address the same location as
 * its {@code Path} twin.
 *
 * <p>During the storage migration both forms coexist, and a key that disagrees with its path is
 * the worst possible failure: local mode keeps working while S3 mode silently reads and writes a
 * different location, so the divergence only shows up as missing data in production.
 *
 * <p>The relationship being asserted: the {@code Path} forms are resolved against a base that
 * already contains the subtree segment ({@code {root}/plsql-parse} for parser output,
 * {@code {root}/plsql} for the Claude sidecar), whereas keys are relative to the DATA ROOT
 * ({@code app.data-dir.base} — see StorageConfig). A key must therefore equal
 * {@code "plsql-parse/" + <path relative to the parse base>}, and likewise {@code "plsql/"}.
 */
class AnalysisPathsKeyParityTest {

    /** A fully scoped identity name: DBTYPE ~ PROJECT ~ ENV ~ OBJECT. */
    private static final String NAME = "DB2~Db2Demo~local~ERP_USP_RUN_CYCLE";
    /** A scopeless (legacy flat) name — no context segments. */
    private static final String FLAT = "ERP_USP_RUN_CYCLE";

    private static final Path PARSE_BASE = Path.of("data", AnalysisPaths.PARSE_ROOT);
    private static final Path CLAUDE_BASE = Path.of("data", AnalysisPaths.CLAUDE_ROOT);

    /** Renders a Path as the key string it ought to correspond to. */
    private String asKeyString(Path base, String rootSegment, Path path) {
        Path rel = base.toAbsolutePath().normalize().relativize(path.toAbsolutePath().normalize());
        return rootSegment + "/" + rel.toString().replace('\\', '/');
    }

    private void assertParseParity(String label, Path path, ArtifactKey key) {
        assertEquals(asKeyString(PARSE_BASE, AnalysisPaths.PARSE_ROOT, path), key.toString(),
                label + ": key and path must address the same location");
    }

    private void assertClaudeParity(String label, Path path, ArtifactKey key) {
        assertEquals(asKeyString(CLAUDE_BASE, AnalysisPaths.CLAUDE_ROOT, path), key.toString(),
                label + ": key and path must address the same location");
    }

    @Test
    @DisplayName("every parse-tree key accessor matches its Path twin")
    void parseKeysMatchPaths() {
        assertParseParity("dir",        AnalysisPaths.dir(PARSE_BASE, NAME),
                                        AnalysisPaths.dirKey(NAME));
        assertParseParity("parentDir",  AnalysisPaths.parentDir(PARSE_BASE, NAME),
                                        AnalysisPaths.parentDirKey(NAME));
        for (String rel : new String[]{
                "api/index.json", "api/call_graph.json", "api/tables/index.json",
                "api/node_id_map.json", "api/nodes/foo.json", "claude/tables_merged.json",
                "resolver/q1_object_resolution.json", "sources/ERP_USP_RUN_CYCLE.sql"}) {
            assertParseParity(rel, AnalysisPaths.resolve(PARSE_BASE, NAME, rel),
                    AnalysisPaths.resolveKey(NAME, rel));
        }
    }

    @Test
    @DisplayName("Claude sidecar keys match their Path twins")
    void claudeKeysMatchPaths() {
        assertClaudeParity("claudeDir", CLAUDE_BASE.resolve(NAME), AnalysisPaths.claudeDirKey(NAME));
        for (String file : new String[]{
                "analysis_claude.json", "analysis_claude_prev.json", "metadata.json"}) {
            assertClaudeParity(file, CLAUDE_BASE.resolve(NAME).resolve(file),
                    AnalysisPaths.claudeFileKey(NAME, file));
        }
    }

    @Test
    @DisplayName("a scopeless name maps to a flat single-segment folder on both sides")
    void scopelessNameAgrees() {
        assertParseParity("flat dir", AnalysisPaths.dir(PARSE_BASE, FLAT), AnalysisPaths.dirKey(FLAT));
        assertParseParity("flat index", AnalysisPaths.resolve(PARSE_BASE, FLAT, "api/index.json"),
                AnalysisPaths.resolveKey(FLAT, "api/index.json"));
        // parentDir of a flat name is the base itself, so the key is the bare subtree root.
        assertEquals(AnalysisPaths.PARSE_ROOT, AnalysisPaths.parentDirKey(FLAT).toString());
    }

    @Test
    @DisplayName("keys carry the explicit subtree prefix that Paths get from their base")
    void keysArePrefixedWithTheirSubtree() {
        // Regression guard: without these segments every key would land one level too high —
        // {root}/api/index.json instead of {root}/plsql-parse/<...>/api/index.json.
        assertTrue(AnalysisPaths.dirKey(NAME).toString().startsWith("plsql-parse/"),
                "parse keys are relative to the DATA ROOT, so they must name the subtree");
        assertTrue(AnalysisPaths.claudeDirKey(NAME).toString().startsWith("plsql/"),
                "claude keys are relative to the DATA ROOT, so they must name the subtree");
    }

    @Test
    @DisplayName("the '~' delimiter becomes nested key segments, never one flat segment")
    void separatorExpandsToSegments() {
        assertEquals(
                java.util.List.of("plsql-parse", "DB2", "Db2Demo", "local", "ERP_USP_RUN_CYCLE"),
                AnalysisPaths.dirKey(NAME).segments());
        // The Claude sidecar is the deliberate exception: it is stored FLAT, keyed by the whole
        // identity name, matching Path.of(dataBaseDir).resolve(analysisName).
        assertEquals(java.util.List.of("plsql", NAME), AnalysisPaths.claudeDirKey(NAME).segments());
    }

    @Test
    @DisplayName("nameOfKey is the inverse of dirKey")
    void nameOfKeyRoundTrips() {
        assertEquals(NAME, AnalysisPaths.nameOfKey(AnalysisPaths.dirKey(NAME)));
        assertEquals(FLAT, AnalysisPaths.nameOfKey(AnalysisPaths.dirKey(FLAT)));
        // ... and agrees with the Path-based nameOf it replaces.
        assertEquals(AnalysisPaths.nameOf(PARSE_BASE, AnalysisPaths.dir(PARSE_BASE, NAME)),
                AnalysisPaths.nameOfKey(AnalysisPaths.dirKey(NAME)));
    }

    @Test
    @DisplayName("a name needing sanitisation normalises identically on both sides")
    void normalisationAgrees() {
        String messy = AnalysisPaths.name("db2", "My Project", "pre-prod", "erp.usp run/cycle");
        assertParseParity("messy name", AnalysisPaths.dir(PARSE_BASE, messy),
                AnalysisPaths.dirKey(messy));
    }
}
