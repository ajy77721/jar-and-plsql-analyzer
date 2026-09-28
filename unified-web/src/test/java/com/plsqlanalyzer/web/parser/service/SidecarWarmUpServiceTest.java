package com.plsqlanalyzer.web.parser.service;

import com.analyzer.repo.ArtifactScanRepository;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.service.JarDataPaths;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The warm-up sweep's skip/compute decision. The compute itself is {@link AnalysisViewWriter}'s own
 * (covered by {@link AnalysisViewWriterTest}); this test is about what the sweep chooses to do: warm
 * an analysis whose set is incomplete, skip one that is already complete, and contain a store it
 * cannot reach instead of throwing or warning per facet.
 */
class SidecarWarmUpServiceTest {

    private static final ObjectMapper M = new ObjectMapper();
    private static final String NAME = "ORACLE~P~DEV~DEMO";

    private InMemoryArtifactStore store;
    private AnalysisViewStore views;
    private StaticAnalysisReader staticReader;
    private AnalysisViewWriter writer;
    private AnalysisService analysisService;
    private SidecarWarmUpService warmUp;

    @BeforeEach
    void setUp() throws IOException {
        store = new InMemoryArtifactStore();
        views = new AnalysisViewStore(store);
        staticReader = new StaticAnalysisReader(store);
        ClaudeAnalysisReader claude = new ClaudeAnalysisReader(staticReader,
                new ClaudePersistenceService(store));
        AnalysisReaderFactory factory = new AnalysisReaderFactory(staticReader, claude);
        ArtifactScanRepository repository = new ArtifactScanRepository(store, store, M, factory,
                staticReader, views, null, new JarDataPaths("data/jar", store));

        ComplexityScoringService complexity = mock(ComplexityScoringService.class);
        when(complexity.getScores(eq(NAME), any(), any(), anyString(), anyDouble()))
                .thenReturn(ResponseEntity.ok((Object) M.createObjectNode().put("score", 1)));
        SummaryStatsService stats = mock(SummaryStatsService.class);
        when(stats.getStats(eq(NAME), any(), any(), any()))
                .thenReturn(ResponseEntity.ok((Object) M.createObjectNode().put("totalNodes", 2)));
        writer = new AnalysisViewWriter(repository, staticReader, complexity, stats, views, M);

        analysisService = mock(AnalysisService.class);
        // A real header write, so the sweep sees a complete set after warming (a mock that returned
        // true without writing would leave hasCompleteViewSet false and re-warm forever).
        when(analysisService.writeHeaderSidecar(anyString(), anyBoolean())).thenAnswer(inv -> {
            var scalars = M.createObjectNode();
            scalars.put("entryPoint", "DEMO");
            return views.writeHeader(inv.getArgument(0), scalars, false);
        });

        warmUp = new SidecarWarmUpService(repository, writer, views, analysisService, null, true, 6, true);

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
                 "tables":[{"name":"ORDERS","alias":"o","joins":[{"joinType":"JOIN",
                    "joinedTable":"CUSTOMER","condition":"ORDERS.CUST_ID=c.id","line":10}]}]}""");
        node("P2.json", """
                {"nodeId":"ERP$P2","name":"P2","sourceFile":"P2.sql",
                 "tables":[{"name":"ORDERS","joins":[{"joinType":"LEFT","joinedTable":"CUSTOMER",
                    "condition":"c.id=o.cust","line":20}]}]}""");
    }

    @Test
    @DisplayName("a sweep warms an analysis whose sidecar set is not complete")
    void sweepWarmsAnIncompleteAnalysis() {
        assertFalse(views.hasCompleteViewSet(NAME), "fixture starts with no sidecars");

        assertEquals(1, warmUp.sweep());
        assertTrue(views.hasCompleteViewSet(NAME), "the sweep must leave a complete set");
    }

    @Test
    @DisplayName("a second sweep skips the analysis it already warmed")
    void sweepSkipsAnAlreadyCompleteAnalysis() {
        assertEquals(1, warmUp.sweep());
        assertEquals(0, warmUp.sweep(), "an already-warm analysis is not recomputed");
    }

    @Test
    @DisplayName("warmOne is a no-op returning true once the set is complete")
    void warmOneReturnsTrueWhenAlreadyComplete() {
        assertEquals(1, warmUp.sweep());
        assertTrue(warmUp.warmOne(NAME), "a complete analysis needs no recompute");
    }

    @Test
    @DisplayName("a login-triggered sweep runs once per interval, not once per login")
    void loginSweepRunsOncePerInterval() {
        assertTrue(warmUp.triggerLoginSweep(null, null, "session-abcdefgh"),
                "the first login starts the sweep");
        assertFalse(warmUp.triggerLoginSweep(null, null, "session-abcdefgh"),
                "a later login inside the interval must not re-sweep");
    }

    @Test
    @DisplayName("a store that cannot be listed fails the sweep quietly, never by throwing")
    void sweepNeverThrowsWhenTheListingFails() throws IOException {
        com.analyzer.storage.AnalysisRepository listingFails =
                mock(com.analyzer.storage.AnalysisRepository.class);
        when(listingFails.analyses(any())).thenThrow(new IOException("store unreachable"));
        SidecarWarmUpService unreachable = new SidecarWarmUpService(
                listingFails, writer, views, analysisService, null, true, 6, true);

        // One INFO line, no exception; the next sweep tries again.
        assertEquals(0, unreachable.sweep());
        assertEquals(0, unreachable.sweep());
    }
}
