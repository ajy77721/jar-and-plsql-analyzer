package com.analyzer.integration;

import com.analyzer.repo.ArtifactScanRepository;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsqlanalyzer.web.page.FacetPageResponder;
import com.plsqlanalyzer.web.parser.config.JoinComplexityConfig;
import com.plsqlanalyzer.web.parser.controller.ParserAnalysisController;
import com.plsqlanalyzer.web.parser.service.AnalysisPaths;
import com.plsqlanalyzer.web.parser.service.AnalysisReaderFactory;
import com.plsqlanalyzer.web.parser.service.AnalysisService;
import com.plsqlanalyzer.web.parser.service.AnalysisViewStore;
import com.plsqlanalyzer.web.parser.service.ClaudeAnalysisReader;
import com.plsqlanalyzer.web.parser.service.ClaudePersistenceService;
import com.plsqlanalyzer.web.parser.service.ComplexityScoringService;
import com.plsqlanalyzer.web.parser.service.ParserScopeResolver;
import com.plsqlanalyzer.web.parser.service.StaticAnalysisReader;
import com.plsqlanalyzer.web.parser.service.SummaryStatsService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Parameter;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * F3 + the parser half of F4: the PL/SQL flow in-process — a fixture "database" (the crawl's own
 * artifact layout) is read through the real {@link AnalysisService}, readers and
 * {@link ParserAnalysisController}, and every serving layer (listing, index, stats, header counts,
 * tables, procedures, sequences, cursors) returns the fixture's data.
 *
 * <p>The crawler itself cannot run without a live JDBC database (the subagent confirmed there is no
 * embedded-DB harness), so the "fixture DB" here is the exact artifact tree a completed crawl
 * produces — the same tree {@code AnalysisViewsGoldenTest} drives the real read path against. F4
 * then proves a re-analysis invalidates the precomputed sidecar rather than serving stale rows.
 */
@DisplayName("Integration F3/F4: PL/SQL fixture crawl -> facets -> serving")
class EndToEndParserFlowTest {

    private static final ObjectMapper M = new ObjectMapper();
    private static final String NAME = "ORACLE~P~DEV~DEMO";

    private InMemoryArtifactStore store;
    private AnalysisViewStore views;
    private StaticAnalysisReader staticReader;
    private ArtifactScanRepository repository;
    private AnalysisService service;
    private ParserAnalysisController controller;

    @BeforeEach
    void setUp() {
        store = new InMemoryArtifactStore();
        views = new AnalysisViewStore(store);
        staticReader = new StaticAnalysisReader(store);
        ClaudeAnalysisReader claude = new ClaudeAnalysisReader(staticReader, new ClaudePersistenceService(store));
        AnalysisReaderFactory factory = new AnalysisReaderFactory(staticReader, claude);
        repository = new ArtifactScanRepository(store, store, M, factory, staticReader, views, null,
                new com.jaranalyzer.service.JarDataPaths("data/jar", store));
        service = new AnalysisService("data", factory, staticReader, views,
                mock(com.plsqlanalyzer.config.PlsqlConfig.class),
                new ClaudePersistenceService(store), store, repository,
                mock(com.plsqlanalyzer.web.controller.detail.ObjectResolutionService.class),
                null, mock(com.analyzer.usercfg.UserConnectionStore.class));
        controller = buildParserController(new ParserScopeResolver(service));
    }

    private ParserAnalysisController buildParserController(ParserScopeResolver scopes) {
        try {
            Constructor<?> ctor = ParserAnalysisController.class.getConstructors()[0];
            Parameter[] params = ctor.getParameters();
            Object[] args = new Object[params.length];
            for (int i = 0; i < params.length; i++) {
                Class<?> type = params[i].getType();
                if (ObjectMapper.class.equals(type)) args[i] = M;
                else if (AnalysisService.class.equals(type)) args[i] = service;
                else if (AnalysisViewStore.class.equals(type)) args[i] = views;
                else if (com.analyzer.storage.AnalysisRepository.class.equals(type)) args[i] = repository;
                else if (FacetPageResponder.class.equals(type)) args[i] = new FacetPageResponder(M, 10000);
                else if (ParserScopeResolver.class.equals(type)) args[i] = scopes;
                else if (SummaryStatsService.class.equals(type)) args[i] = new SummaryStatsService(service, scopes, M);
                else if (com.plsqlanalyzer.web.parser.service.DetailHeaderCountsService.class.equals(type)) {
                    args[i] = new com.plsqlanalyzer.web.parser.service.DetailHeaderCountsService(service, scopes, M);
                }
                else if (ComplexityScoringService.class.equals(type)) args[i] = new ComplexityScoringService(service, M);
                else if (JoinComplexityConfig.class.equals(type)) args[i] = new JoinComplexityConfig();
                else if (type.isPrimitive()) args[i] = (type == boolean.class) ? false : 0;
                else args[i] = mock(type);
            }
            return (ParserAnalysisController) ctor.newInstance(args);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /* ------------------------------- fixture ------------------------------- */

    private void put(String relative, String body) throws IOException {
        try (OutputStream out = store.write(AnalysisPaths.resolveKey(NAME, relative))) {
            out.write(body.getBytes(StandardCharsets.UTF_8));
        }
    }

    private void writeFixture() throws IOException {
        put("api/index.json", """
                {"entryPoint":"DEMO","entrySchema":"ERP","totalNodes":2,"totalTables":2,"totalEdges":1,
                 "totalLinesOfCode":150,"maxDepth":2,"crawlTimeMs":1234,"totalDurationMs":5678,
                 "parseMs":10,"discoverMs":20,"saveMs":30,"dbCallCount":3,"errors":[],
                 "nodes":[
                  {"nodeId":"ERP$P1","name":"P1","schema":"ERP","objectType":"PROCEDURE",
                   "linesOfCode":100,"depth":2,"readable":true,"sourceFile":"P1.sql",
                   "counts":{"tables":2,"internalCalls":1,"externalCalls":0,"callsOut":1,"cursors":1,
                             "parameters":1,"variables":1,"exceptionHandlers":0,"sequences":1,
                             "statements":{"SELECT":2,"EXECUTE_IMMEDIATE":1}}},
                  {"nodeId":"ERP$P2","name":"P2","schema":"BILLING","objectType":"FUNCTION",
                   "linesOfCode":50,"depth":1,"readable":true,
                   "counts":{"tables":1,"internalCalls":0,"externalCalls":1,"callsOut":0,"cursors":1,
                             "parameters":0,"variables":0,"exceptionHandlers":1,
                             "statements":{"SELECT":1}}}]}""");
        put("api/nodes/P1.json", """
                {"nodeId":"ERP$P1","name":"P1","sourceFile":"P1.sql","schema":"ERP",
                 "sequences":[{"name":"SEQ_ORDERS","schema":"ERP","operations":{"NEXTVAL":[3,4]}}],
                 "cursors":[{"name":"C1","query":"SELECT * FROM ORDERS","forLoop":true,"line":5}],
                 "dynamicSql":[{"type":"EXECUTE_IMMEDIATE","line":9,"sqlExpression":"SELECT 1 FROM DUAL"}],
                 "variables":{"typeRefs":[{"name":"V_SKU","dataType":"ORDERS.SKU%TYPE",
                    "refTable":"ORDERS","refField":"SKU","line":7}]},
                 "parameters":[{"name":"P_ID","dataType":"ORDERS.ID%TYPE","line":2}],
                 "tables":[{"name":"ORDERS","alias":"o","joins":[{"joinType":"JOIN",
                    "joinedTable":"CUSTOMER","joinedTableAlias":"c",
                    "condition":"ORDERS.CUST_ID=c.id AND ORDERS.STATUS='X'","line":10}]}]}""");
        put("api/nodes/P2.json", """
                {"nodeId":"ERP$P2","name":"P2","sourceFile":"P2.sql","schema":"ERP",
                 "cursors":[{"name":"C2","query":"SELECT 1","refCursor":true,"line":6}],
                 "tables":[{"name":"ORDERS","joins":[{"joinType":"LEFT","joinedTable":"CUSTOMER",
                    "condition":"c.id=o.cust","line":20}]}]}""");
        put("api/call_graph.json", """
                {"edges":[
                  {"from":"P1","to":"P2","fromNodeId":"ERP$P1","toNodeId":"ERP$P2",
                   "fromSchema":"ERP","toSchema":"ERP","lines":[10],"count":1}]}""");
        put("api/tables/index.json", """
                {"tables":[
                 {"name":"ORDERS","schema":"ERP","objectType":"TABLE","usedBy":[]},
                 {"name":"CUSTOMER","schema":"ERP","objectType":"TABLE","usedBy":[]}
                ]}""");
    }

    private static JsonNode body(ResponseEntity<?> response) {
        assertNotNull(response, "endpoint returned null");
        assertTrue(response.getStatusCode().is2xxSuccessful(), "status " + response.getStatusCode());
        assertNotNull(response.getBody(), "endpoint returned no body");
        return M.valueToTree(response.getBody());
    }

    /**
     * The row count of a facet response, whether it is the legacy raw array or the paged
     * {@code {items:[...]}} envelope (the controller keeps the raw shape when no paging is asked for).
     */
    private static int rows(JsonNode body) {
        if (body.isArray()) return body.size();
        if (body.path("items").isArray()) return body.path("items").size();
        return 0;
    }

    @Test
    @DisplayName("the fixture crawl is listed, and every serving facet returns its data")
    void fixtureIsServedAcrossEveryLayer() throws Exception {
        writeFixture();

        // Listing (repository scan over api/index.json).
        var analyses = service.listAnalyses();
        assertEquals(1, analyses.size(), "the fixture analysis must be listable");
        assertEquals(NAME, analyses.get(0).getName());

        // Index. The endpoint deliberately serves the index WITHOUT nodes; the full document (which
        // the crawler wrote) still carries them.
        JsonNode index = body(controller.getIndex(NAME));
        assertTrue(index.path("entryPoint").asText().equals("DEMO") || index.size() > 0,
                "the index must describe the analysis");
        assertEquals(2, service.getIndex(NAME).path("nodes").size(),
                "the full index must carry both nodes");

        // Summary stats.
        JsonNode stats = body(controller.getSummaryStats(NAME, null, null, null));
        assertTrue(stats.size() > 0, "summary stats must carry data");

        // Header counts.
        JsonNode counts = body(controller.getDetailHeaderCounts(NAME, null, null));
        assertTrue(counts.size() > 0, "detail-header counts must carry data");

        // Tables.
        JsonNode tables = body(controller.getTables(NAME));
        assertEquals(2, tables.path("tables").size());

        // Procedures (raw array when no paging is requested).
        JsonNode procedures = body(controller.getProcedures(NAME, null, null, null, null, null,
                null, null, null, null, null));
        assertEquals(2, rows(procedures), "both routines must be served");

        // Sequences facet (from the P1 node).
        JsonNode sequences = body(controller.getSequences(NAME, null, null, null, null, null,
                null, null, null, null, null));
        assertEquals(1, rows(sequences));
        assertEquals("SEQ_ORDERS", sequences.get(0).path("sequenceName").asText());

        // Cursors facet (one per node).
        JsonNode cursors = body(controller.getCursors(NAME, null, null, null, null, null,
                null, null, null, null, null, null, null));
        assertEquals(2, rows(cursors));
    }

    @Test
    @DisplayName("F4: a re-analysis invalidates the precomputed sidecar instead of serving stale rows")
    void reAnalysisInvalidatesTheSidecar() throws Exception {
        writeFixture();
        // First serve computes and backfills the sequences sidecar.
        assertEquals(1, rows(body(controller.getSequences(NAME, null, null, null, null, null, null,
                null, null, null, null))));
        ArtifactKey sidecar = AnalysisPaths.resolveKey(NAME, "views/" + AnalysisViewStore.SEQUENCES);
        assertTrue(store.exists(sidecar), "the first serve must backfill the sidecar");

        // The identity is re-analysed: a new node document and a new index, with a fresh mtime.
        Thread.sleep(10);
        put("api/nodes/P1.json", """
                {"nodeId":"ERP$P1","name":"P1","sourceFile":"P1.sql","schema":"ERP",
                 "sequences":[{"name":"SEQ_NEW","schema":"ERP","operations":{"NEXTVAL":[1]}}]}""");
        put("api/index.json", "{\"entryPoint\":\"DEMO\",\"nodes\":[]}");
        // What a re-analysis does to the readers' in-process caches.
        staticReader.evictCache(NAME);

        JsonNode after = body(controller.getSequences(NAME, null, null, null, null, null, null,
                null, null, null, null));
        String rendered = after.toString();
        assertTrue(rendered.contains("SEQ_NEW"), "the re-analysis's rows must be served");
        assertTrue(!rendered.contains("SEQ_ORDERS"), "the superseded rows must not be served");
    }
}
