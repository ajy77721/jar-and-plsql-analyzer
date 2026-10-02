package com.plsqlanalyzer.web.parser.controller;

import com.analyzer.repo.ArtifactScanRepository;
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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Parameter;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * Task #33 — {@code OracleDemo} and {@code ORACLEDEMO} must resolve to the SAME analysis on every
 * endpoint. The fixture is written under the canonical stored name; every endpoint is then called
 * twice, once with the canonical spelling and once with a mixed-case spelling, and the two
 * responses must be byte-identical (stats + facets + details).
 *
 * <p>The harness mirrors {@link EstateFacetsTest}: a real {@link ArtifactScanRepository}, real
 * readers and a real {@link ParserAnalysisController} over an {@link InMemoryArtifactStore}.
 */
@DisplayName("Case-insensitive identity: both spellings hit the same analysis")
class CaseInsensitiveIdentityEndpointTest {

    private static final ObjectMapper M = new ObjectMapper();
    /** What the crawler stores on disk (segments uppercased by AnalysisPaths.name/seg). */
    private static final String CANONICAL = "ORACLE~ORACLEDEMO~LOCAL~DEMO";
    /** How a user may type it in a URL. */
    private static final String MIXED = "ORACLE~OracleDemo~Local~Demo";

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
        repository = new ArtifactScanRepository(store, store, M, factory,
                staticReader, views, null,
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

    private void put(String relative, String body) throws IOException {
        // Deliberately store under the canonical path, as the crawler does.
        try (OutputStream out = store.write(AnalysisPaths.resolveKey(CANONICAL, relative))) {
            out.write(body.getBytes(StandardCharsets.UTF_8));
        }
    }

    private void writeFixture() throws IOException {
        put("api/index.json", """
                {"entryPoint":"DEMO","nodes":[
                 {"nodeId":"S$P1","name":"P1","objectType":"PROCEDURE"}]}""");
        put("api/tables/index.json", """
                {"tables":[
                 {"name":"BASE_A","schema":"S","objectType":"TABLE","usedBy":[]},
                 {"name":"VIEW_B","schema":"S","objectType":"VIEW",
                  "dbColumns":[{"columnName":"X","dataType":"NUMBER","nullable":true}],
                  "viewDefinition":"SELECT x FROM base_a",
                  "viewTableOperations":[{"tableName":"BASE_A","operation":"SELECT"}],
                  "usedBy":[]}
                ]}""");
    }

    private byte[] bytes(ResponseEntity<?> response) throws IOException {
        assertNotNull(response, "endpoint returned null");
        assertTrue(response.getStatusCode().is2xxSuccessful(), "status " + response.getStatusCode());
        assertNotNull(response.getBody(), "endpoint returned no body");
        return M.writeValueAsBytes(response.getBody());
    }

    @Test
    @DisplayName("stats: both spellings return the identical response")
    void statsResolvesBothSpellings() throws Exception {
        writeFixture();
        byte[] canonical = bytes(controller.getSummaryStats(CANONICAL, null, null, null));
        byte[] mixed = bytes(controller.getSummaryStats(MIXED, null, null, null));
        assertArrayEquals(canonical, mixed, "stats must not depend on the spelling of the name");
    }

    @Test
    @DisplayName("facet (views): both spellings return the identical response")
    void viewsFacetResolvesBothSpellings() throws Exception {
        writeFixture();
        byte[] canonical = bytes(controller.getViews(CANONICAL, null, null, null, null, null, null,
                null, null, null));
        byte[] mixed = bytes(controller.getViews(MIXED, null, null, null, null, null, null,
                null, null, null));
        assertArrayEquals(canonical, mixed, "the views facet must resolve both spellings");
        assertTrue(new String(canonical).contains("VIEW_B"));
    }

    @Test
    @DisplayName("details (tables + index): both spellings return the identical response")
    void detailEndpointsResolveBothSpellings() throws Exception {
        writeFixture();
        assertArrayEquals(bytes(controller.getTables(CANONICAL)), bytes(controller.getTables(MIXED)),
                "getTables must resolve both spellings");
        assertArrayEquals(bytes(controller.getIndex(CANONICAL)), bytes(controller.getIndex(MIXED)),
                "getIndex must resolve both spellings");
    }

    @Test
    @DisplayName("a spelling that is not an analysis still 404s")
    void unknownNameStillNotFound() {
        // No fixture written: neither spelling exists, so the canonicalising lookup must not
        // fabricate a result.
        assertFalse(controller.getTables(MIXED).getStatusCode().is2xxSuccessful());
    }
}
