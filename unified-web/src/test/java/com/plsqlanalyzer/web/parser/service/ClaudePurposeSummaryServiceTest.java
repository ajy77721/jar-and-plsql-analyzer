package com.plsqlanalyzer.web.parser.service;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.LocalArtifactStore;
import com.plsqlanalyzer.web.parser.service.ClaudePurposeSummaryModels.PurposeCacheEntry;
import com.plsqlanalyzer.web.parser.service.ClaudePurposeSummaryModels.PurposeSummaryRunResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * §267 — real (not mocked-away) end-to-end test of the purpose-summary pipeline: a real
 * {@link LocalArtifactStore} against a temp directory, a real {@link StaticAnalysisReader} reading
 * fixture {@code api/index.json}/{@code api/nodes/*.json}/source files written in the exact shape
 * {@code ChunkedFlowWriter} produces, and a stubbed {@link ClaudeProcessRunner} (there is no existing
 * Claude-CLI test-mocking convention in this codebase to reuse — confirmed by search — so this
 * subclasses the runner exactly as one would subclass any other collaborator with a real public
 * method to override) returning a canned JSON response instead of shelling out to the real CLI.
 */
class ClaudePurposeSummaryServiceTest {

    private static final String ANALYSIS = "TEST~PROJ~LOCAL~ERP_DEMO";

    @TempDir
    Path tempDir;

    private LocalArtifactStore store;
    private StaticAnalysisReader reader;

    @BeforeEach
    void setUp() throws Exception {
        store = new LocalArtifactStore(tempDir);
        reader = new StaticAnalysisReader(store);
        writeFixture();
    }

    private void writeFixture() throws Exception {
        ArtifactKey dir = AnalysisPaths.dirKey(ANALYSIS);

        String index = "{\n" +
                "  \"nodes\": [\n" +
                "    {\"nodeId\": \"N1\", \"name\": \"ERP_USP_WRITE_AUDIT_TRAIL\", \"schema\": \"ERP\", " +
                "\"objectType\": \"PROCEDURE\", \"sourceFile\": \"erp_usp_write_audit_trail.sql\", " +
                "\"lineStart\": 1, \"lineEnd\": 6, \"detailFile\": \"nodes/N1.json\"},\n" +
                "    {\"nodeId\": \"N2\", \"name\": \"ERP_USP_NOOP\", \"schema\": \"ERP\", " +
                "\"objectType\": \"PROCEDURE\", \"sourceFile\": \"erp_usp_noop.sql\", " +
                "\"lineStart\": 1, \"lineEnd\": 3, \"detailFile\": \"nodes/N2.json\"}\n" +
                "  ]\n" +
                "}";
        writeText(dir.resolve("api").resolve("index.json"), index);

        String detail1 = "{\"tables\": [{\"name\": \"AUDIT_LOG\", \"operations\": {\"INSERT\": [3]}}]}";
        writeText(dir.resolve("api").resolve("nodes").resolve("N1.json"), detail1);
        String detail2 = "{\"tables\": []}";
        writeText(dir.resolve("api").resolve("nodes").resolve("N2.json"), detail2);

        writeText(dir.resolve("sources").resolve("erp_usp_write_audit_trail.sql"),
                "PROCEDURE ERP_USP_WRITE_AUDIT_TRAIL IS\n" +
                "BEGIN\n" +
                "  INSERT INTO AUDIT_LOG (msg) VALUES ('changed');\n" +
                "END;\n");
        writeText(dir.resolve("sources").resolve("erp_usp_noop.sql"),
                "PROCEDURE ERP_USP_NOOP IS\nBEGIN\n  NULL;\nEND;\n");
    }

    private void writeText(ArtifactKey key, String text) throws Exception {
        try (OutputStream out = store.write(key)) {
            out.write(text.getBytes(StandardCharsets.UTF_8));
        }
    }

    /** A stub runner: no real CLI process, returns a fixed response per call (round-robin). */
    static class StubRunner extends ClaudeProcessRunner {
        final String[] responses;
        final AtomicInteger calls = new AtomicInteger(0);
        StubRunner(String... responses) { this.responses = responses; }
        @Override
        public String run(String prompt, String sessionId, int timeoutSeconds) {
            int i = calls.getAndIncrement();
            return i < responses.length ? responses[i] : responses[responses.length - 1];
        }
    }

    @Test
    void generate_producesRealSummaries_andCachesThem() {
        String canned = "{\"procedures\":[" +
                "{\"nodeId\":\"N1\",\"purpose\":\"writes an audit log entry for a change\"," +
                "\"perStatement\":[{\"line\":3,\"intent\":\"insert audit row\"}]}," +
                "{\"nodeId\":\"N2\",\"purpose\":\"does nothing\"}" +
                "]}";
        StubRunner runner = new StubRunner(canned);
        ClaudePurposeSummaryService service = new ClaudePurposeSummaryService(runner, reader, store);

        PurposeSummaryRunResult result = service.generate(ANALYSIS, msg -> {});

        assertNull(result.error);
        assertEquals(2, result.totalProcedures);
        assertEquals(2, result.generated);
        assertEquals(0, result.skippedCached);
        assertEquals(1, result.claudeCallCount, "both procedures should batch into one chunk/call");

        Map<String, PurposeCacheEntry> all = service.getSummaries(ANALYSIS);
        assertEquals(2, all.size());
        assertEquals("writes an audit log entry for a change", all.get("N1").purposeSummary);
        assertEquals(1, all.get("N1").perStatement.size());
        assertEquals(3, all.get("N1").perStatement.get(0).line);
        assertEquals("does nothing", all.get("N2").purposeSummary);

        PurposeCacheEntry single = service.getSummary(ANALYSIS, "N1");
        assertNotNull(single);
        assertNotNull(single.sourceDigest);
    }

    @Test
    void generate_skipsCachedEntry_whenSourceUnchanged() {
        String canned = "{\"procedures\":[" +
                "{\"nodeId\":\"N1\",\"purpose\":\"first run\"}," +
                "{\"nodeId\":\"N2\",\"purpose\":\"second\"}" +
                "]}";
        StubRunner runner = new StubRunner(canned);
        ClaudePurposeSummaryService service = new ClaudePurposeSummaryService(runner, reader, store);

        PurposeSummaryRunResult first = service.generate(ANALYSIS, msg -> {});
        assertEquals(2, first.generated);
        assertEquals(1, runner.calls.get());

        // Re-run with no source change: everything should be skipped as already-cached, zero new
        // Claude calls — this is the "only regenerate if source changed" cache contract.
        PurposeSummaryRunResult second = service.generate(ANALYSIS, msg -> {});
        assertEquals(0, second.generated);
        assertEquals(2, second.skippedCached);
        assertEquals(1, runner.calls.get(), "no new Claude call should have been made");
    }

    @Test
    void getSummaries_emptyWhenNeverGenerated() {
        ClaudePurposeSummaryService service =
                new ClaudePurposeSummaryService(new StubRunner("{}"), reader, store);
        assertTrue(service.getSummaries(ANALYSIS).isEmpty());
        assertNull(service.getSummary(ANALYSIS, "N1"));
    }
}
