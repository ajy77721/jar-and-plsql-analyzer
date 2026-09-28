package com.analyzer.repo;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.analyzer.storage.repo.AnalysisQuery;
import com.analyzer.storage.repo.AnalysisRef;
import com.analyzer.storage.repo.RepositoryMode;
import com.analyzer.storage.repo.RowStream;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.service.JarDataPaths;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The parts of Increment C that have logic of their own rather than a delegation.
 *
 * <p>The delegating methods ({@code document}) are deliberately not re-tested here: they call the
 * very reader the controller used to call, those readers already have their own tests, and the
 * guarantee that matters for them is a byte-for-byte endpoint comparison, which a unit test cannot
 * make. What IS tested is everything that is new code — the analysis listing, the streaming row
 * source, and the two value types that reject a malformed query before it reaches storage.
 *
 * <p>Built on {@link InMemoryArtifactStore} rather than a temp directory, because the whole claim
 * of {@code ArtifactScanRepository} is that it is written against {@code ArtifactStore} alone. A
 * test that needed a real filesystem would be evidence against that claim.
 */
class ArtifactScanRepositoryTest {

    private InMemoryArtifactStore store;
    private ObjectMapper mapper;
    private ArtifactScanRepository repo;

    @BeforeEach
    void setUp() {
        store = new InMemoryArtifactStore();
        mapper = new ObjectMapper();
        // The PL/SQL readers and the JAR provider factory are the delegation targets of methods
        // this test does not exercise; passing null proves those methods are not reached and keeps
        // the fixture to the collaborators the tested paths actually use.
        // One InMemoryArtifactStore stands in for both the parser-side and jar-side beans: this
        // fixture predates the parser/jar ArtifactStore split and nothing here exercises different
        // backends per feature.
        repo = new ArtifactScanRepository(store, store, mapper, null, null,
                new com.plsqlanalyzer.web.parser.service.AnalysisViewStore(store), null,
                new JarDataPaths("data/jar", store));
    }

    private void put(ArtifactKey key, String body) throws IOException {
        try (OutputStream out = store.write(key)) {
            out.write(body.getBytes(StandardCharsets.UTF_8));
        }
    }

    /* ───────────────────────── analyses ───────────────────────── */

    @Test
    void analysesFindsEveryIndexDocumentAndNothingElse() throws IOException {
        put(ArtifactKey.of("plsql-parse", "ORACLE~P~DEV~A", "api", "index.json"), "{}");
        put(ArtifactKey.of("plsql-parse", "ORACLE~P~DEV~B", "api", "index.json"), "{}");
        // Neither of these is an analysis index: one is a different document under api/, the other
        // an index.json somewhere else entirely. A looser match would list phantom analyses.
        put(ArtifactKey.of("plsql-parse", "ORACLE~P~DEV~A", "api", "tables", "index.json"), "{}");
        put(ArtifactKey.of("plsql-parse", "ORACLE~P~DEV~A", "index.json"), "{}");

        List<AnalysisRef> refs = repo.analyses(AnalysisQuery.Dataset.PLSQL_ANALYSES);

        assertEquals(2, refs.size(), "only api/index.json documents are analyses");
        assertTrue(refs.stream().allMatch(r -> r.index().leaf().equals("index.json")));
        assertTrue(refs.stream().anyMatch(r -> r.name().endsWith("ORACLE~P~DEV~A")));
        assertTrue(refs.stream().anyMatch(r -> r.name().endsWith("ORACLE~P~DEV~B")));
    }

    @Test
    void analysesAreNewestFirst() throws IOException {
        // Written oldest-to-newest, so a stable-sort bug would leave them in insertion order.
        put(ArtifactKey.of("plsql-parse", "OLD", "api", "index.json"), "{}");
        put(ArtifactKey.of("plsql-parse", "MIDDLE", "api", "index.json"), "{}");
        put(ArtifactKey.of("plsql-parse", "NEWEST", "api", "index.json"), "{}");

        List<AnalysisRef> refs = repo.analyses(AnalysisQuery.Dataset.PLSQL_ANALYSES);
        List<Long> stamps = refs.stream().map(AnalysisRef::lastModified).toList();
        for (int i = 1; i < stamps.size(); i++) {
            assertTrue(stamps.get(i - 1) >= stamps.get(i),
                    "newest first, got " + stamps);
        }
    }

    @Test
    void analysesIsEmptyWhenNothingHasBeenAnalysed() throws IOException {
        assertEquals(List.of(), repo.analyses(AnalysisQuery.Dataset.PLSQL_ANALYSES));
    }

    @Test
    void analysesRejectsADatasetItDoesNotServe() {
        assertThrows(IllegalArgumentException.class,
                () -> repo.analyses(AnalysisQuery.Dataset.PLSQL_JOINS));
    }

    /* ───────────────────────── rows ───────────────────────── */

    @Test
    void bundledJarRowsAreStreamedAndProjected() throws IOException {
        put(ArtifactKey.of("jar", "demo", "bundled-jars.json"),
                "[{\"name\":\"a.jar\",\"manifest\":\"BIG\"},{\"name\":\"b.jar\"}]");

        List<String> names = new ArrayList<>();
        AnalysisQuery q = AnalysisQuery.of(AnalysisQuery.Dataset.JAR_BUNDLED_JARS, "demo");
        try (RowStream rows = repo.rows(q, row -> row)) {
            while (rows.hasNext()) names.add(rows.next().path("name").asText());
        }
        assertEquals(List.of("a.jar", "b.jar"), names);
    }

    @Test
    void aProjectionReturningNullDropsTheRowDuringTheScan() throws IOException {
        put(ArtifactKey.of("jar", "demo", "bundled-jars.json"),
                "[{\"name\":\"a.jar\"},{\"name\":\"b.jar\"},{\"name\":\"c.jar\"}]");

        List<String> kept = new ArrayList<>();
        AnalysisQuery q = AnalysisQuery.of(AnalysisQuery.Dataset.JAR_BUNDLED_JARS, "demo");
        try (RowStream rows = repo.rows(q, row -> "b.jar".equals(row.path("name").asText())
                ? row : null)) {
            while (rows.hasNext()) kept.add(rows.next().path("name").asText());
        }
        // The point is not the filter but the cost: the other rows are never built into a node.
        assertEquals(List.of("b.jar"), kept);
    }

    @Test
    void existsDistinguishesAnAbsentArtifactFromAnEmptyOne() throws IOException {
        AnalysisQuery q = AnalysisQuery.of(AnalysisQuery.Dataset.JAR_BUNDLED_JARS, "demo");
        assertFalse(repo.exists(q), "nothing written yet");

        put(ArtifactKey.of("jar", "demo", "bundled-jars.json"), "[]");
        assertTrue(repo.exists(q), "an empty list is still a recorded answer");
    }

    @Test
    void existsIsRefusedRatherThanGuessedForSynthesizedFacets() {
        AnalysisQuery q = AnalysisQuery.of(AnalysisQuery.Dataset.PLSQL_JOINS, "A");
        assertThrows(UnsupportedOperationException.class, () -> repo.exists(q));
    }

    @Test
    void rowsAndDocumentRefuseEachOthersDatasets() {
        assertThrows(UnsupportedOperationException.class,
                () -> repo.rows(AnalysisQuery.of(AnalysisQuery.Dataset.PLSQL_JOINS, "A"), null));
        assertThrows(UnsupportedOperationException.class,
                () -> repo.document(AnalysisQuery.of(
                        AnalysisQuery.Dataset.JAR_BUNDLED_JARS, "demo")));
    }

    @Test
    void indexedIsFalseBecauseThisImplementationScans() {
        assertFalse(repo.indexed());
    }

    /* ───────────────────────── value types ───────────────────────── */

    @Test
    void aTableScopedQueryWithoutATableIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> AnalysisQuery.of(AnalysisQuery.Dataset.PLSQL_TABLE_JOINS, "A"));
    }

    @Test
    void aTableOnANonTableScopedQueryIsRejected() {
        // Silently ignoring it would be worse: the caller would believe the result was scoped.
        assertThrows(IllegalArgumentException.class,
                () -> AnalysisQuery.ofTable(AnalysisQuery.Dataset.PLSQL_JOINS, "A", "ORDERS"));
    }

    @Test
    void anAnalysisScopedQueryWithoutAnAnalysisIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> AnalysisQuery.of(AnalysisQuery.Dataset.PLSQL_JOINS, "  "));
    }

    @Test
    void repositoryModeAcceptsTheWireSpellingAndDefaultsToScanning() {
        assertEquals(RepositoryMode.ARTIFACT_SCAN, RepositoryMode.parse("artifact-scan"));
        assertEquals(RepositoryMode.ARTIFACT_SCAN, RepositoryMode.parse("ARTIFACT_SCAN"));
        assertEquals(RepositoryMode.ARTIFACT_SCAN, RepositoryMode.parse(null));
        assertEquals(RepositoryMode.ARTIFACT_SCAN, RepositoryMode.parse(""));
        assertEquals(RepositoryMode.MONGO, RepositoryMode.parse("mongo"));
        assertEquals("artifact-scan", RepositoryMode.ARTIFACT_SCAN.wireName());
    }

    @Test
    void anUnknownRepositoryModeNamesTheValidOnes() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> RepositoryMode.parse("mongodb"));
        assertTrue(e.getMessage().contains("artifact-scan"), e.getMessage());
        assertTrue(e.getMessage().contains("mongo"), e.getMessage());
    }

    @Test
    void anEmptyRowStreamNeedsNoParser() throws IOException {
        try (RowStream rows = RowStream.empty()) {
            assertFalse(rows.hasNext());
        }
    }

    @Test
    void anAbandonedRowStreamStillClosesTheUnderlyingStream() throws IOException {
        put(ArtifactKey.of("jar", "demo", "bundled-jars.json"),
                "[{\"name\":\"a.jar\"},{\"name\":\"b.jar\"}]");
        AnalysisQuery q = AnalysisQuery.of(AnalysisQuery.Dataset.JAR_BUNDLED_JARS, "demo");
        RowStream rows = repo.rows(q, row -> row);
        JsonNode first = rows.next();
        assertEquals("a.jar", first.path("name").asText());
        // Closing after one row of two is the paged case: a page is bounded, the artifact is not.
        rows.close();
        assertTrue(store.leakedTempFiles().isEmpty());
    }
}
