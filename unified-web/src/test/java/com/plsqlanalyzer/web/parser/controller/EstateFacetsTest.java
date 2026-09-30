package com.plsqlanalyzer.web.parser.controller;

import com.analyzer.repo.ArtifactScanRepository;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Parameter;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * Estate facets — views/MVs, triggers, package globals — end to end over a real
 * {@link ArtifactScanRepository} and the real readers, at the endpoint boundary.
 *
 * <p>Asserts three things per facet: (1) the reader synthesizes the expected rows and chains from
 * the analysis's own artifacts; (2) the endpoint served from the precomputed sidecar is
 * byte-identical to the legacy computation; (3) an absent facet degrades to the standard empty
 * answer, and a re-analysis invalidates the sidecar.
 */
class EstateFacetsTest {

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
        ClaudeAnalysisReader claude = new ClaudeAnalysisReader(staticReader,
                new ClaudePersistenceService(store));
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

    /* ───────────────────────────── fixture ───────────────────────────── */

    private void put(String relative, String body) throws IOException {
        try (OutputStream out = store.write(AnalysisPaths.resolveKey(NAME, relative))) {
            out.write(body.getBytes(StandardCharsets.UTF_8));
        }
    }

    private void writeFixture() throws IOException {
        put("api/index.json", "{\"entryPoint\":\"DEMO\",\"nodes\":[]}");
        // Two views chained (VIEW_A -> VIEW_B -> BASE_A), one MV, and two triggers chained
        // (TRG1 on T1 touches T2, which has TRG2).
        put("api/tables/index.json", """
                {"tables":[
                 {"name":"BASE_A","schema":"S","objectType":"TABLE",
                  "usedBy":[{"nodeId":"S$P1","nodeName":"P1","nodeDepth":0,"operations":{"SELECT":[1]}}]},
                 {"name":"VIEW_B","schema":"S","objectType":"VIEW",
                  "dbColumns":[{"columnName":"X","dataType":"NUMBER","nullable":true}],
                  "viewDefinition":"SELECT x FROM base_a",
                  "viewTableOperations":[{"tableName":"BASE_A","operation":"SELECT"}],
                  "usedBy":[{"nodeId":"S$P2","nodeName":"P2","nodeDepth":0,"operations":{"SELECT":[2]}}]},
                 {"name":"VIEW_A","schema":"S","objectType":"VIEW",
                  "dbColumns":[{"columnName":"Y","dataType":"NUMBER","nullable":true}],
                  "viewDefinition":"SELECT y FROM view_b",
                  "viewTableOperations":[{"tableName":"VIEW_B","operation":"SELECT"}],
                  "usedBy":[]},
                 {"name":"MV_C","schema":"S","objectType":"MATERIALIZED_VIEW",
                  "dbColumns":[],
                  "viewDefinition":"SELECT 1 FROM base_a",
                  "viewTableOperations":[{"tableName":"BASE_A","operation":"SELECT"}],
                  "usedBy":[]},
                 {"name":"T1","schema":"S","objectType":"TABLE",
                  "triggers":[{"nodeId":"S$TRG1","name":"TRG1","schema":"S","event":"INSERT","timing":"AFTER"}],
                  "usedBy":[{"nodeId":"S$P3","nodeName":"P3","nodeDepth":0,"operations":{"INSERT":[5]}}]},
                 {"name":"T2","schema":"S","objectType":"TABLE",
                  "triggers":[{"name":"TRG2","schema":"S","event":"UPDATE","timing":"AFTER",
                               "triggerType":"AFTER STATEMENT","status":"ENABLED",
                               "definition":"TRIGGER TRG2 AFTER UPDATE ON T2 BEGIN NULL; END;",
                               "tableOps":[{"tableName":"T3","operation":"INSERT"}]}],
                  "usedBy":[{"nodeId":"S$P4","nodeName":"P4","nodeDepth":0,"operations":{"UPDATE":[9]}}]}
                ]}""");
        put("api/nodes/S$TRG1.json", """
                {"nodeId":"S$TRG1","name":"TRG1","schema":"S","objectType":"TRIGGER",
                 "triggerTable":"T1","triggerEvent":"INSERT","triggerTiming":"AFTER",
                 "tables":[{"name":"T2","schema":"S","objectType":"TABLE","operations":{"UPDATE":[3]}}]}""");
        put("api/nodes/S$TRG2.json", """
                {"nodeId":"S$TRG2","name":"TRG2","schema":"S","objectType":"TRIGGER",
                 "triggerTable":"T2","triggerEvent":"UPDATE","triggerTiming":"AFTER","tables":[]}""");
        put("api/nodes/S$P1.json", """
                {"nodeId":"S$P1","name":"P1","schema":"S","objectType":"PROCEDURE","sourceFile":"P1.prc",
                 "externalPackageVarRefDetails":[
                   {"packageName":"PKG_A","variableName":"G_X","line":10,"accessType":"READ"},
                   {"packageName":"PKG_A","variableName":"G_X","line":12,"accessType":"WRITE"},
                   {"packageName":"PKG_B","variableName":"G_Y","line":3,"accessType":"READ"}]}""");
    }

    /* ─────────────────────────── reader-level ─────────────────────────── */

    @Test
    void viewsIndexHasColumnsDefinitionAndBaseTables() throws Exception {
        writeFixture();
        JsonNode viewsDoc = staticReader.getViews(NAME);
        assertEquals(3, viewsDoc.size(), "VIEW_B, VIEW_A, MV_C");
        JsonNode viewA = row(viewsDoc, "name", "VIEW_A");
        assertEquals("VIEW", viewA.path("objectType").asText());
        assertEquals(1, viewA.path("columnCount").asInt());
        assertEquals("SELECT y FROM view_b", viewA.path("definition").asText());
        assertEquals("VIEW_B", viewA.path("baseTables").get(0).path("name").asText());
        JsonNode mv = row(viewsDoc, "name", "MV_C");
        assertTrue(mv.path("isMaterialized").asBoolean());
        assertTrue(mv.path("mvRefreshMode").isNull(), "refresh mode is not persisted — reported null");
    }

    @Test
    void viewChainResolvesViewOnViewToBaseTable() throws Exception {
        writeFixture();
        JsonNode chain = staticReader.getViewChain(NAME, "VIEW_A");
        JsonNode levels = chain.path("levels");
        assertEquals(3, levels.size(), "VIEW_A -> VIEW_B -> BASE_A");
        assertEquals("VIEW_A", levels.get(0).get(0).asText());
        assertEquals("VIEW_B", levels.get(1).get(0).asText());
        assertEquals("BASE_A", levels.get(2).get(0).asText());
        assertFalse(chain.path("cyclic").asBoolean());
    }

    @Test
    void viewUsedByIsTheConsumerList() throws Exception {
        writeFixture();
        JsonNode usedBy = staticReader.getViewUsedBy(NAME, "VIEW_B");
        assertEquals(1, usedBy.path("count").asInt());
        assertEquals("S$P2", usedBy.path("usedBy").get(0).path("nodeId").asText());
    }

    @Test
    void triggerIndexCarriesLevelBodyAndTouches() throws Exception {
        writeFixture();
        JsonNode triggers = staticReader.getTriggers(NAME);
        JsonNode trg2 = row(triggers, "name", "TRG2");
        assertEquals("T2", trg2.path("table").asText());
        assertEquals("STATEMENT", trg2.path("level").asText(), "level derived from triggerType");
        assertTrue(trg2.path("bodyAvailable").asBoolean());
        assertEquals("T3", trg2.path("touches").get(0).path("name").asText());
        // TRG1's touches come from its node detail's own table operations.
        JsonNode trg1 = row(triggers, "name", "TRG1");
        assertEquals("T2", trg1.path("touches").get(0).path("name").asText());
    }

    @Test
    void triggerChainFollowsTouchedTableToItsTriggers() throws Exception {
        writeFixture();
        JsonNode chain = staticReader.getTriggerChain(NAME, "TRG1");
        assertEquals(2, chain.path("levels").size(), "TRG1 -> TRG2");
        assertEquals("TRG1", chain.path("levels").get(0).get(0).asText());
        assertEquals("TRG2", chain.path("levels").get(1).get(0).asText());
        assertEquals("T2", chain.path("edges").get(0).path("via").asText());
    }

    @Test
    void triggerUsedByIsTheFiringDml() throws Exception {
        writeFixture();
        JsonNode fired = staticReader.getTriggerUsedBy(NAME, "TRG1");
        assertEquals(1, fired.path("count").asInt());
        assertEquals("S$P3", fired.path("firedBy").get(0).path("nodeId").asText());
    }

    @Test
    void globalsGroupByPackageWithReadWriteCounts() throws Exception {
        writeFixture();
        JsonNode globals = staticReader.getGlobals(NAME);
        JsonNode pkgA = row(globals, "package", "PKG_A");
        assertEquals(1, pkgA.path("variableCount").asInt());
        assertEquals("G_X", pkgA.path("variables").get(0).path("name").asText());
        assertEquals(1, pkgA.path("readCount").asInt());
        assertEquals(1, pkgA.path("writeCount").asInt());
        // A global used across packages resolves by variable name.
        JsonNode usedBy = staticReader.getGlobalUsedBy(NAME, "G_X");
        assertEquals(2, usedBy.path("count").asInt(), "read + write on PKG_A.G_X");
    }

    /**
     * The fields the crawl now persists must reach the facet reader: MV refresh metadata
     * (mvRefreshMode/mvRefreshMethod/lastRefresh on the tables-index per-view entry), the
     * source-parsed trigger WHEN/level/body (whenClause/level/definition on the tables-index
     * triggers[] row), and the package-global DECLARATIONS (packageGlobalDeclarations on the
     * package node). One fixture carrying all three proves the whole persisted→reader path.
     */
    @Test
    void persistedMvRefreshTriggerWhenLevelBodyAndGlobalDeclarationsFlowThrough() throws Exception {
        put("api/index.json", "{\"entryPoint\":\"DEMO\",\"nodes\":[]}");
        put("api/tables/index.json", """
                {"tables":[
                 {"name":"MV_C","schema":"S","objectType":"MATERIALIZED_VIEW","dbColumns":[],
                  "viewDefinition":"SELECT 1 FROM base_a","usedBy":[],
                  "mvRefreshMode":"DEMAND","mvRefreshMethod":"FAST",
                  "lastRefresh":"2026-09-01T10:15:30Z","lastRefreshType":"FAST"},
                 {"name":"T1","schema":"S","objectType":"TABLE",
                  "triggers":[{"nodeId":"S$TRG1","name":"TRG1","schema":"S",
                               "event":"INSERT","timing":"AFTER","level":"ROW",
                               "whenClause":"NEW.STATUS = 'ACTIVE'","source":"SOURCE",
                               "definition":"TRIGGER TRG1 AFTER INSERT ON T1 FOR EACH ROW WHEN (NEW.STATUS = 'ACTIVE') BEGIN NULL; END;"}],
                  "usedBy":[]}
                ]}""");
        // Package node carrying its own global declarations (what the Oracle visitor now persists).
        put("api/nodes/S$PKG_A.json", """
                {"nodeId":"S$PKG_A","name":"PKG_A","objectName":"PKG_A","schema":"S",
                 "objectType":"PACKAGE_BODY",
                 "packageGlobalDeclarations":[
                   {"name":"G_X","dataType":"NUMBER","defaultValue":"0","constant":false}]}""");
        // A reference to PKG_A.G_X so the package also appears through the usage path.
        put("api/nodes/S$P1.json", """
                {"nodeId":"S$P1","name":"P1","schema":"S","objectType":"PROCEDURE",
                 "externalPackageVarRefDetails":[
                   {"packageName":"PKG_A","variableName":"G_X","line":4,"accessType":"READ"}]}""");

        // MV refresh metadata flows off the tables-index entry.
        JsonNode mv = row(staticReader.getViews(NAME), "name", "MV_C");
        assertEquals("DEMAND", mv.path("mvRefreshMode").asText());
        assertEquals("FAST", mv.path("mvRefreshMethod").asText());
        assertEquals("2026-09-01T10:15:30Z", mv.path("lastRefresh").asText());

        // Source-parsed trigger WHEN/level/body flow off the tables-index triggers[] row.
        JsonNode trg = row(staticReader.getTriggers(NAME), "name", "TRG1");
        assertEquals("ROW", trg.path("level").asText(), "persisted level wins over derivation");
        assertEquals("NEW.STATUS = 'ACTIVE'", trg.path("whenClause").asText());
        assertTrue(trg.path("bodyAvailable").asBoolean(), "body persisted from the trigger definition");

        // Package-global declarations flow off the package node detail.
        JsonNode gx = row(staticReader.getGlobals(NAME), "package", "PKG_A")
                .path("variables").get(0);
        assertEquals("G_X", gx.path("name").asText());
        assertTrue(gx.path("declared").asBoolean(), "declaration present → declared:true");
        assertEquals("NUMBER", gx.path("dataType").asText());
        assertEquals("0", gx.path("defaultValue").asText());
    }

    @Test
    void absentFacetsDegradeToEmpty() throws Exception {
        put("api/index.json", "{\"entryPoint\":\"DEMO\",\"nodes\":[]}");
        put("api/tables/index.json", "{\"tables\":[]}");
        assertEquals(0, staticReader.getViews(NAME).size());
        assertEquals(0, staticReader.getTriggers(NAME).size());
        assertEquals(0, staticReader.getGlobals(NAME).size());
        assertNull(staticReader.getViewDetail(NAME, "NOPE"));
    }

    /* ─────────────────────── endpoint byte-equivalence ─────────────────────── */

    private byte[] bytes(ResponseEntity<?> response) throws IOException {
        assertNotNull(response, "endpoint returned null");
        assertTrue(response.getStatusCode().is2xxSuccessful(), "status " + response.getStatusCode());
        assertNotNull(response.getBody(), "endpoint returned no body");
        return M.writeValueAsBytes(response.getBody());
    }

    /** Runs {@code call} with no sidecar, asserts the sidecar now exists, then runs it again
     *  (served from the sidecar) and requires the two responses to be byte-identical. */
    private void golden(String view, Supplier<ResponseEntity<?>> call) throws IOException {
        views.invalidate(NAME);
        byte[] legacy = bytes(call.get());
        ArtifactKey sidecar = AnalysisPaths.resolveKey(NAME, "views/" + view);
        assertTrue(store.exists(sidecar), "sidecar " + view + " was not written on the legacy call");
        byte[] cached = bytes(call.get());
        assertArrayEquals(legacy, cached, "sidecar response differs from legacy for " + view);
    }

    @Test
    void viewsEndpointIsByteIdenticalFromSidecar() throws Exception {
        writeFixture();
        golden(AnalysisViewStore.VIEWS_INDEX,
                () -> controller.getViews(NAME, null, null, null, null, null, null, null, null, null));
    }

    @Test
    void triggersEndpointIsByteIdenticalFromSidecar() throws Exception {
        writeFixture();
        golden(AnalysisViewStore.TRIGGERS_INDEX,
                () -> controller.getTriggers(NAME, null, null, null, null, null, null, null, null, null));
    }

    @Test
    void globalsEndpointIsByteIdenticalFromSidecar() throws Exception {
        writeFixture();
        golden(AnalysisViewStore.GLOBALS_INDEX,
                () -> controller.getGlobals(NAME, null, null, null, null, null, null, null, null, null));
    }

    @Test
    void viewDetailAndChainAndUsedByAreServed() throws Exception {
        writeFixture();
        assertTrue(controller.getViewDetail(NAME, "VIEW_A").getStatusCode().is2xxSuccessful());
        JsonNode chain = controller.getViewChain(NAME, "VIEW_A").getBody();
        assertEquals(3, chain.path("levels").size());
        assertTrue(controller.getViewUsedBy(NAME, "VIEW_B").getStatusCode().is2xxSuccessful());
        assertTrue(controller.getTriggerChain(NAME, "TRG1").getBody().path("levels").size() >= 2);
        assertTrue(controller.getGlobalUsedBy(NAME, "G_X").getBody().path("count").asInt() >= 1);
        // A name the analysis does not have is a 404, not an empty 200.
        assertEquals(404, controller.getViewDetail(NAME, "NOPE").getStatusCode().value());
    }

    @Test
    void aReAnalysisInvalidatesTheEstateSidecars() throws Exception {
        writeFixture();
        bytes(controller.getViews(NAME, null, null, null, null, null, null, null, null, null));
        ArtifactKey sidecar = AnalysisPaths.resolveKey(NAME, "views/" + AnalysisViewStore.VIEWS_INDEX);
        assertTrue(store.exists(sidecar));

        Thread.sleep(10);
        put("api/tables/index.json", """
                {"tables":[{"name":"NEW_V","schema":"S","objectType":"VIEW","dbColumns":[],
                  "viewDefinition":"SELECT 1","viewTableOperations":[],"usedBy":[]}]}""");
        put("api/index.json", "{\"entryPoint\":\"DEMO\",\"nodes\":[]}");
        staticReader.evictCache(NAME);

        String body = new String(bytes(controller.getViews(NAME, null, null, null, null, null, null,
                null, null, null)));
        assertTrue(body.contains("NEW_V"), "a re-analysis must invalidate the stale sidecar");
        assertTrue(!body.contains("VIEW_A"), "the superseded run's rows must be gone");
    }

    private static JsonNode row(JsonNode array, String field, String value) {
        for (JsonNode n : array) {
            if (value.equalsIgnoreCase(n.path(field).asText(""))) return n;
        }
        fail("no row with " + field + "=" + value + " in " + array);
        return null;
    }
}
