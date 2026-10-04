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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * Scheduled jobs — end to end over a real {@link ArtifactScanRepository} and the real readers.
 *
 * <p>Asserts the scheduled-entry-point contract: the {@code /jobs} facet serves a row per
 * discovered job (name, schedule, enabled, target), the target procedure carries the
 * {@code scheduled} badge in the procedures facet, and an analysis with no jobs artifact degrades
 * to an empty facet rather than a 404.
 */
class ScheduledJobsFacetTest {

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

    private void put(String relative, String body) throws IOException {
        try (OutputStream out = store.write(AnalysisPaths.resolveKey(NAME, relative))) {
            out.write(body.getBytes(StandardCharsets.UTF_8));
        }
    }

    private void writeFixture() throws IOException {
        put("api/index.json", """
                {"entryPoint":"DEMO","nodes":[
                 {"nodeId":"ERP$LOAD_ORDERS","name":"LOAD_ORDERS","schema":"ERP",
                  "objectType":"PROCEDURE","depth":0},
                 {"nodeId":"ERP$JOB$NIGHTLY_LOAD","name":"NIGHTLY_LOAD","schema":"ERP",
                  "objectType":"JOB","depth":1,"schedule":"FREQ=DAILY;BYHOUR=2",
                  "jobEnabled":true,"jobTarget":"LOAD_ORDERS"}
                ]}""");
        put("api/jobs/index.json", """
                {"totalJobs":1,"jobs":[
                 {"name":"NIGHTLY_LOAD","schema":"ERP","type":"JOB",
                  "schedule":"FREQ=DAILY;BYHOUR=2","enabled":true,
                  "target":"LOAD_ORDERS","targetSchema":null,
                  "targetNodeId":"ERP$LOAD_ORDERS","nodeId":"ERP$JOB$NIGHTLY_LOAD","callCount":1}
                ]}""");
    }

    private static JsonNode row(JsonNode array, String field, String value) {
        for (JsonNode n : array) {
            if (value.equalsIgnoreCase(n.path(field).asText())) return n;
        }
        return null;
    }

    @Test
    void jobsFacetCarriesScheduleEnabledAndTarget() throws Exception {
        writeFixture();
        JsonNode jobs = staticReader.getJobs(NAME);
        assertEquals(1, jobs.size());
        JsonNode job = row(jobs, "name", "NIGHTLY_LOAD");
        assertNotNull(job);
        assertEquals("FREQ=DAILY;BYHOUR=2", job.path("schedule").asText());
        assertTrue(job.path("enabled").asBoolean());
        assertEquals("LOAD_ORDERS", job.path("target").asText());
        assertEquals("ERP$LOAD_ORDERS", job.path("targetNodeId").asText());
    }

    @Test
    void jobsEndpointServesTheFacet() throws Exception {
        writeFixture();
        ResponseEntity<JsonNode> res = controller.getJobs(NAME, null, null, null, null, null, null,
                null, null, null);
        assertEquals(200, res.getStatusCode().value());
        JsonNode body = res.getBody();
        assertNotNull(body);
        // No paging params → legacy bare array; a cursor request returns an envelope.
        JsonNode rows = body.isArray() ? body : body.path("rows");
        assertEquals(1, rows.size());
        assertEquals("NIGHTLY_LOAD", rows.get(0).path("name").asText());
    }

    @Test
    void targetProcedureCarriesScheduledBadge() throws Exception {
        writeFixture();
        JsonNode procedures = staticReader.getProcedures(NAME);
        JsonNode target = row(procedures, "nodeId", "ERP$LOAD_ORDERS");
        assertNotNull(target);
        assertTrue(target.path("scheduled").asBoolean(),
                "the job's target procedure must carry the scheduled badge");
        // Entity separation: the JOB node is not a procedure, so it does not appear in the
        // procedures facet at all — it lives in the scheduled-jobs facet.
        assertNull(row(procedures, "nodeId", "ERP$JOB$NIGHTLY_LOAD"),
                "a JOB node must not be listed among the procedures");
    }

    @Test
    void absentJobsArtifactDegradesToEmpty() throws Exception {
        put("api/index.json", "{\"entryPoint\":\"DEMO\",\"nodes\":[]}");
        JsonNode jobs = staticReader.getJobs(NAME);
        assertNotNull(jobs);
        assertEquals(0, jobs.size());
        // No jobs → procedures are returned untouched.
        assertEquals(0, staticReader.getProcedures(NAME).size());
    }
}
