package com.plsqlanalyzer.web.parser.controller;

import com.analyzer.storage.AnalysisRepository;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsqlanalyzer.web.page.FacetPageResponder;
import com.plsqlanalyzer.web.parser.config.JoinComplexityConfig;
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
import com.analyzer.repo.ArtifactScanRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Parameter;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * The Phase-1 view-summary contract, at the endpoint boundary: <b>for every migrated endpoint the
 * response served from the precomputed sidecar is byte-identical to the response the legacy
 * computation produces</b>, the legacy path is what an absent sidecar falls back to, and a
 * re-analysis or correction invalidates the sidecar rather than serving stale data.
 *
 * <p>Built on a real {@link ArtifactScanRepository} over an {@link InMemoryArtifactStore} and the
 * real readers, so the comparison is between the actual code paths a request takes, not between a
 * mock and itself. The fixture is deliberately small but exercises each facet's own merge and
 * de-duplication rules.
 */
class AnalysisViewsGoldenTest {

    private static final ObjectMapper M = new ObjectMapper();
    private static final String NAME = "ORACLE~P~DEV~DEMO";

    private InMemoryArtifactStore store;
    private AnalysisViewStore views;
    private StaticAnalysisReader staticReader;
    private ArtifactScanRepository repository;
    private AnalysisService service;
    private ParserAnalysisController controller;
    private TableDetailController tableController;

    @BeforeEach
    void setUp() {
        store = new InMemoryArtifactStore();
        views = new AnalysisViewStore(store);
        staticReader = new StaticAnalysisReader(store);
        ClaudeAnalysisReader claude = new ClaudeAnalysisReader(staticReader,
                new ClaudePersistenceService(store));
        AnalysisReaderFactory factory = new AnalysisReaderFactory(staticReader, claude);
        repository = new ArtifactScanRepository(store, store, M, factory, staticReader, views, null,
                new com.jaranalyzer.service.JarDataPaths("data/jar", store));

        // A real AnalysisService over the same store, so SummaryStats/Complexity read the fixture
        // exactly as they would in production. plsqlConfig/objectResolutionService/user store are
        // never touched by the read paths these endpoints take.
        service = new AnalysisService("data", factory, staticReader, views,
                mock(com.plsqlanalyzer.config.PlsqlConfig.class),
                new ClaudePersistenceService(store), store, repository,
                mock(com.plsqlanalyzer.web.controller.detail.ObjectResolutionService.class),
                null, mock(com.analyzer.usercfg.UserConnectionStore.class));
        ParserScopeResolver scopes = new ParserScopeResolver(service);

        controller = buildParserController(scopes);
        tableController = new TableDetailController(repository, new FacetPageResponder(M, 10000),
                scopes, M);
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
                else if (AnalysisRepository.class.equals(type)) args[i] = repository;
                else if (FacetPageResponder.class.equals(type)) args[i] = new FacetPageResponder(M, 10000);
                else if (ParserScopeResolver.class.equals(type)) args[i] = scopes;
                else if (SummaryStatsService.class.equals(type)) args[i] = new SummaryStatsService(service, scopes, M);
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
        index("""
                {"entryPoint":"DEMO","entrySchema":"ERP","totalNodes":2,"totalTables":2,"totalEdges":1,
                 "totalLinesOfCode":150,"maxDepth":2,"crawlTimeMs":1234,"totalDurationMs":5678,
                 "parseMs":10,"discoverMs":20,"saveMs":30,"dbCallCount":3,"errors":["warn1"],
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
        node("P1.json", """
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
        node("P2.json", """
                {"nodeId":"ERP$P2","name":"P2","sourceFile":"P2.sql","schema":"ERP",
                 "cursors":[{"name":"C2","query":"SELECT 1","refCursor":true,"line":6}],
                 "tables":[{"name":"ORDERS","joins":[{"joinType":"LEFT","joinedTable":"CUSTOMER",
                    "condition":"c.id=o.cust","line":20}]}]}""");
    }

    /* --------------------------- golden assertions --------------------------- */

    /** Captures the response body; the first call is the legacy value (and seeds the sidecar). */
    private byte[] bytes(ResponseEntity<?> response) throws IOException {
        assertNotNull(response, "endpoint returned null");
        assertTrue(response.getStatusCode().is2xxSuccessful(), "status " + response.getStatusCode());
        assertNotNull(response.getBody(), "endpoint returned no body");
        return M.writeValueAsBytes(response.getBody());
    }

    /**
     * Runs {@code call} once with no sidecar (legacy), asserts the sidecar now exists, then runs it
     * again (served from the sidecar) and requires the two responses to be byte-identical.
     */
    private void golden(String view, Supplier<ResponseEntity<?>> call) throws IOException {
        views.invalidate(NAME);
        byte[] legacy = bytes(call.get());
        assertTrue(store.exists(ArtifactKey.of(AnalysisPaths.PARSE_ROOT)
                        .resolve(NAME.split("~")).resolve("views", view)),
                "sidecar " + view + " was not written on the legacy call");
        byte[] cached = bytes(call.get());
        assertArrayEquals(legacy, cached, "sidecar response differs from legacy for " + view);
    }

    @Test
    void sequencesEndpointIsByteIdenticalFromSidecar() throws Exception {
        writeFixture();
        golden(AnalysisViewStore.SEQUENCES,
                () -> controller.getSequences(NAME, null, null, null, null, null, null, null, null,
                        null, null));
    }

    @Test
    void sequencesSummaryIsByteIdenticalFromSidecar() throws Exception {
        writeFixture();
        golden(AnalysisViewStore.SEQUENCES, () -> controller.getSequencesSummary(NAME, null, null));
    }

    @Test
    void joinsSummaryIsByteIdenticalFromSidecar() throws Exception {
        writeFixture();
        golden(AnalysisViewStore.JOINS_SUMMARY, () -> controller.getJoinsSummary(NAME, null, null));
    }

    @Test
    void cursorsEndpointIsByteIdenticalFromSidecar() throws Exception {
        writeFixture();
        golden(AnalysisViewStore.CURSORS,
                () -> controller.getCursors(NAME, null, null, null, null, null, null, null, null,
                        null, null, null, null));
    }

    @Test
    void cursorsSummaryIsByteIdenticalFromSidecar() throws Exception {
        writeFixture();
        golden(AnalysisViewStore.CURSORS, () -> controller.getCursorsSummary(NAME, null, null));
    }

    @Test
    void dynamicSqlEndpointIsByteIdenticalFromSidecar() throws Exception {
        writeFixture();
        golden(AnalysisViewStore.DYNAMIC_SQL,
                () -> controller.getDynamicSql(NAME, null, null, null, null, null, null, null, null,
                        null, null));
    }

    @Test
    void dynamicSqlSummaryIsByteIdenticalFromSidecar() throws Exception {
        writeFixture();
        golden(AnalysisViewStore.DYNAMIC_SQL, () -> controller.getDynamicSqlSummary(NAME, null, null));
    }

    @Test
    void complexityScoresDefaultIsByteIdenticalFromSidecar() throws Exception {
        writeFixture();
        golden(AnalysisViewStore.COMPLEXITY,
                () -> controller.getComplexityScores(NAME, 0.3, 15, 10, 8, 20, 5, 50, 150,
                        "linear", 1.3));
    }

    @Test
    void summaryStatsDefaultIsByteIdenticalFromSidecar() throws Exception {
        writeFixture();
        golden(AnalysisViewStore.SUMMARY_STATS,
                () -> controller.getSummaryStats(NAME, null, null, null));
    }

    @Test
    void tableJoinsEndpointIsByteIdenticalFromSidecar() throws Exception {
        writeFixture();
        golden(AnalysisViewStore.TABLE_JOINS,
                () -> tableController.tableJoins(NAME, "ORDERS", null, null, null, null, null, null,
                        null, null));
    }

    @Test
    void tableColumnsEndpointIsByteIdenticalFromSidecar() throws Exception {
        writeFixture();
        golden(AnalysisViewStore.TABLE_COLUMNS,
                () -> tableController.tableColumns(NAME, "ORDERS", null, null, null, null, null, null,
                        null, null, null));
    }

    @Test
    void tableJoinsSummaryIsByteIdenticalFromSidecar() throws Exception {
        writeFixture();
        golden(AnalysisViewStore.TABLE_JOINS,
                () -> tableController.tableJoinsSummary(NAME, "ORDERS", null, null));
    }

    @Test
    void tableColumnsSummaryIsByteIdenticalFromSidecar() throws Exception {
        writeFixture();
        golden(AnalysisViewStore.TABLE_COLUMNS,
                () -> tableController.tableColumnsSummary(NAME, "ORDERS", null, null));
    }

    @Test
    void joinsQuerySummaryIsByteIdenticalFromSidecar() throws Exception {
        writeFixture();
        golden(AnalysisViewStore.JOINS_SUMMARY, () -> controller.getJoinsQuerySummary(NAME));
    }

    /* ------------------------------ fallback ------------------------------ */

    @Test
    void absentSidecarFallsBackToTheLegacyRead() throws Exception {
        writeFixture();
        // The reader's own result is the pre-change response; with no sidecar the endpoint must
        // return exactly it.
        byte[] legacy = M.writeValueAsBytes(staticReader.getSequences(NAME));
        views.invalidate(NAME);
        assertEquals(new String(legacy),
                new String(bytes(controller.getSequences(NAME, null, null, null, null, null, null,
                        null, null, null, null))),
                "an absent sidecar must serve the legacy read unchanged");
    }

    /* ------------------------------ staleness ------------------------------ */

    @Test
    void aReAnalysisInvalidatesTheSidecar() throws Exception {
        writeFixture();
        bytes(controller.getSequences(NAME, null, null, null, null, null, null, null, null, null, null));

        // A new run replaces the node document and rewrites the index — the index's mtime moves
        // past the sidecar's, so the sidecar is stale and must not be served.
        Thread.sleep(10);
        node("P1.json", """
                {"nodeId":"ERP$P1","name":"P1","sourceFile":"P1.sql","schema":"ERP",
                 "sequences":[{"name":"SEQ_NEW","schema":"ERP","operations":{"NEXTVAL":[1]}}]}""");
        index("{\"entryPoint\":\"DEMO\",\"nodes\":[]}");
        // What a re-analysis does to the readers' in-process caches, so this test isolates the
        // sidecar's own staleness rather than the reader memoisation that predates it.
        staticReader.evictCache(NAME);

        String body = new String(bytes(controller.getSequences(NAME, null, null, null, null, null,
                null, null, null, null, null)));
        assertTrue(body.contains("SEQ_NEW"), "a re-analysis must invalidate the stale sidecar");
        assertTrue(!body.contains("SEQ_ORDERS"), "the superseded run's rows must be gone");
    }

    @Test
    void correctionInvalidationDropsSidecars() throws Exception {
        writeFixture();
        bytes(controller.getSequences(NAME, null, null, null, null, null, null, null, null, null, null));
        ArtifactKey sidecar = ArtifactKey.of(AnalysisPaths.PARSE_ROOT)
                .resolve(NAME.split("~")).resolve("views", AnalysisViewStore.SEQUENCES);
        assertTrue(store.exists(sidecar));

        // What the correction path (ClaudePersistenceService) runs on every apply/update/revert.
        new ClaudePersistenceService(store).saveClaudeResult(NAME,
                new com.plsqlanalyzer.web.parser.service.ClaudeVerificationModels.VerificationResult());

        assertTrue(!store.exists(sidecar), "a correction must drop the precomputed views");
    }
}
