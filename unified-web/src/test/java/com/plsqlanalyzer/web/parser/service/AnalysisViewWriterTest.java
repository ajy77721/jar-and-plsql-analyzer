package com.plsqlanalyzer.web.parser.service;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.analyzer.storage.repo.AnalysisQuery;
import com.analyzer.repo.ArtifactScanRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The writer side: after an analysis completes, {@link AnalysisViewWriter#writeAll} must leave a
 * sidecar for every migrated facet, each equal to the endpoint's own document; and the grouped
 * one-pass table scans it uses must equal the per-table legacy scans they summarise.
 */
class AnalysisViewWriterTest {

    private static final ObjectMapper M = new ObjectMapper();
    private static final String NAME = "ORACLE~P~DEV~DEMO";

    private InMemoryArtifactStore store;
    private AnalysisViewStore views;
    private StaticAnalysisReader staticReader;
    private ArtifactScanRepository repository;
    private AnalysisViewWriter writer;

    @BeforeEach
    void setUp() throws IOException {
        store = new InMemoryArtifactStore();
        views = new AnalysisViewStore(store);
        staticReader = new StaticAnalysisReader(store);
        ClaudeAnalysisReader claude = new ClaudeAnalysisReader(staticReader,
                new ClaudePersistenceService(store));
        AnalysisReaderFactory factory = new AnalysisReaderFactory(staticReader, claude);
        repository = new ArtifactScanRepository(store, store, M, factory, staticReader, views, null,
                new com.jaranalyzer.service.JarDataPaths("data/jar", store));

        ComplexityScoringService complexity = mock(ComplexityScoringService.class);
        when(complexity.getScores(eq(NAME), any(), any(), anyString(), anyDouble()))
                .thenReturn(ResponseEntity.ok((Object) M.createObjectNode().put("score", 1)));
        SummaryStatsService stats = mock(SummaryStatsService.class);
        when(stats.getStats(eq(NAME), any(), any(), any()))
                .thenReturn(ResponseEntity.ok((Object) M.createObjectNode().put("totalNodes", 2)));

        writer = new AnalysisViewWriter(repository, staticReader, complexity, stats, views, M);

        writeFixture();
    }

    private void index(String json) throws IOException {
        put(AnalysisPaths.resolveKey(NAME, "api/index.json"), json);
    }

    private void node(String file, String json) throws IOException {
        put(AnalysisPaths.resolveKey(NAME, "api/nodes/" + file), json);
    }

    private void put(ArtifactKey key, String body) throws IOException {
        try (OutputStream out = store.write(key)) {
            out.write(body.getBytes(StandardCharsets.UTF_8));
        }
    }

    private void writeFixture() throws IOException {
        index("{\"entryPoint\":\"DEMO\",\"nodes\":[]}");
        node("P1.json", """
                {"nodeId":"ERP$P1","name":"P1","sourceFile":"P1.sql",
                 "sequences":[{"name":"SEQ_ORDERS","schema":"ERP","operations":{"NEXTVAL":[3]}}],
                 "cursors":[{"name":"C1","query":"SELECT 1","forLoop":true,"line":5}],
                 "dynamicSql":[{"type":"EXECUTE_IMMEDIATE","line":9,"sqlExpression":"SELECT 1"}],
                 "variables":{"typeRefs":[{"name":"V_SKU","dataType":"ORDERS.SKU%TYPE",
                    "refTable":"ORDERS","refField":"SKU","line":7}]},
                 "parameters":[{"name":"P_ID","dataType":"ORDERS.ID%TYPE","line":2}],
                 "tables":[{"name":"ORDERS","alias":"o","joins":[{"joinType":"JOIN",
                    "joinedTable":"CUSTOMER","condition":"ORDERS.CUST_ID=c.id","line":10}]}]}""");
        node("P2.json", """
                {"nodeId":"ERP$P2","name":"P2","sourceFile":"P2.sql",
                 "tables":[{"name":"ORDERS","joins":[{"joinType":"LEFT","joinedTable":"CUSTOMER",
                    "condition":"c.id=o.cust","line":20}]}]}""");
    }

    private ArtifactKey viewKey(String view) {
        return ArtifactKey.of(AnalysisPaths.PARSE_ROOT).resolve(NAME.split("~")).resolve("views", view);
    }

    @Test
    void writeAllLeavesASidecarForEveryFacet() {
        writer.writeAll(NAME);
        for (String view : new String[]{AnalysisViewStore.SEQUENCES, AnalysisViewStore.CURSORS,
                AnalysisViewStore.DYNAMIC_SQL, AnalysisViewStore.JOINS_SUMMARY,
                AnalysisViewStore.TABLE_JOINS, AnalysisViewStore.TABLE_COLUMNS,
                AnalysisViewStore.COMPLEXITY, AnalysisViewStore.SUMMARY_STATS}) {
            assertTrue(store.exists(viewKey(view)), "missing sidecar " + view);
        }
    }

    @Test
    void sidecarContentMatchesTheEndpointDocument() throws IOException {
        writer.writeAll(NAME);
        assertArrayEquals(M.writeValueAsBytes(views.read(NAME, AnalysisViewStore.SEQUENCES)),
                M.writeValueAsBytes(repository.document(AnalysisQuery.of(
                        AnalysisQuery.Dataset.PLSQL_SEQUENCES, NAME))));
        assertArrayEquals(M.writeValueAsBytes(views.read(NAME, AnalysisViewStore.JOINS_SUMMARY)),
                M.writeValueAsBytes(repository.document(AnalysisQuery.of(
                        AnalysisQuery.Dataset.PLSQL_JOINS, NAME))));
    }

    @Test
    void groupedTableJoinsEqualThePerTableLegacyScan() throws IOException {
        Map<String, JsonNode> grouped = staticReader.getAllTableJoins(NAME);
        assertTrue(grouped.containsKey("ORDERS"));
        for (String table : grouped.keySet()) {
            assertArrayEquals(M.writeValueAsBytes(staticReader.getTableJoins(NAME, table)),
                    M.writeValueAsBytes(grouped.get(table)),
                    "grouped joins differ from the per-table scan for " + table);
        }
        // Both directions are present: CUSTOMER is reached only as an incoming join.
        assertTrue(grouped.containsKey("CUSTOMER"));
    }

    @Test
    void groupedTableColumnsEqualThePerTableLegacyScan() throws IOException {
        Map<String, JsonNode> grouped = staticReader.getAllTableColumnRefs(NAME);
        assertTrue(grouped.containsKey("ORDERS"));
        for (String table : grouped.keySet()) {
            assertArrayEquals(M.writeValueAsBytes(staticReader.getTableColumnRefs(NAME, table)),
                    M.writeValueAsBytes(grouped.get(table)),
                    "grouped columns differ from the per-table scan for " + table);
        }
    }

    @Test
    void tableSidecarServesTheSameRowsTheLegacyScanWould() throws IOException {
        writer.writeAll(NAME);
        for (String table : new String[]{"ORDERS", "CUSTOMER", "NO_SUCH_TABLE"}) {
            assertArrayEquals(M.writeValueAsBytes(staticReader.getTableJoins(NAME, table)),
                    M.writeValueAsBytes(repository.document(
                            AnalysisQuery.ofTable(AnalysisQuery.Dataset.PLSQL_TABLE_JOINS, NAME, table))),
                    "table-joins sidecar differs for " + table);
        }
    }

    @Test
    void headerSidecarRoundTripsAndIgnoresAnOlderFormat() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        var scalars = mapper.createObjectNode();
        scalars.put("entryPoint", "DEMO");
        scalars.put("totalNodes", 2);
        views.writeHeader(NAME, scalars);

        JsonNode read = views.readHeader(NAME);
        assertNotNull(read);
        assertEquals("DEMO", read.path("entryPoint").asText());
        assertEquals(AnalysisViewStore.FORMAT_VERSION, read.path("formatVersion").asInt());

        // An unrecognised formatVersion is ignored rather than misread.
        var wrong = mapper.createObjectNode();
        wrong.put("formatVersion", 999);
        views.write(NAME, AnalysisViewStore.HEADER, wrong);
        assertNull(views.readHeader(NAME));
    }
}
