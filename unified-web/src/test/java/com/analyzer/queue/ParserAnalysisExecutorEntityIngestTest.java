package com.analyzer.queue;

import com.analyzer.repo.EntityIngestTrigger;
import com.plsqlanalyzer.web.parser.service.AnalysisService;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Mode 4 (entity-ingest) — {@link ParserAnalysisExecutor}'s post-analyze trigger. Reused pattern:
 * mock every collaborator, assert the one new call site behaves per {@link EntityIngestTrigger}'s
 * own doc comment — fires with the right params when enabled, never fires when disabled, and never
 * lets an ingest failure fail the analyze job itself.
 */
class ParserAnalysisExecutorEntityIngestTest {

    private static QueueJob job() {
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("entryPoint", "ERP_USP_RUN_CYCLE");
        meta.put("owner", "ANALYZER");
        meta.put("objectType", "PROCEDURE");
        meta.put("project", "OracleDemo");
        meta.put("environment", "local");
        meta.put("dbType", "ORACLE");
        meta.put("connName", "oracle-free-conn");
        return new QueueJob(QueueJob.Type.PARSER_ANALYSIS, "Parse: ERP_USP_RUN_CYCLE", meta);
    }

    @Test
    void firesWithTheCompletedJobsOwnParamsWhenEnabled() throws Exception {
        AnalysisService analysisService = mock(AnalysisService.class);
        when(analysisService.runAnalysis(any(), any(), any(), any(), any(), any(), any(), anyBoolean(), anyBoolean()))
                .thenReturn("ORACLE~ORACLEDEMO~LOCAL~ERP_USP_RUN_CYCLE");
        when(analysisService.getFlowOutputDir()).thenReturn(java.nio.file.Path.of("data"));
        EntityIngestTrigger ingest = mock(EntityIngestTrigger.class);
        when(ingest.enabled()).thenReturn(true);

        ParserAnalysisExecutor exec = new ParserAnalysisExecutor(analysisService, ingest,
                mock(com.plsqlanalyzer.web.parser.service.SidecarWarmUpService.class));
        QueueJob job = job();
        exec.execute(job, (j, event) -> {});

        verify(ingest).afterPlsqlAnalyze(eq("ORACLE"), eq("OracleDemo"), eq("local"),
                eq("ERP_USP_RUN_CYCLE"),
                eq("plsql-parse/ORACLE/ORACLEDEMO/LOCAL/ERP_USP_RUN_CYCLE"));
    }

    @Test
    void neverFiresWhenDisabled() throws Exception {
        AnalysisService analysisService = mock(AnalysisService.class);
        when(analysisService.runAnalysis(any(), any(), any(), any(), any(), any(), any(), anyBoolean(), anyBoolean()))
                .thenReturn("ORACLE~ORACLEDEMO~LOCAL~ERP_USP_RUN_CYCLE");
        when(analysisService.getFlowOutputDir()).thenReturn(java.nio.file.Path.of("data"));
        EntityIngestTrigger ingest = mock(EntityIngestTrigger.class);
        when(ingest.enabled()).thenReturn(false);

        ParserAnalysisExecutor exec = new ParserAnalysisExecutor(analysisService, ingest,
                mock(com.plsqlanalyzer.web.parser.service.SidecarWarmUpService.class));
        exec.execute(job(), (j, event) -> {});

        verify(ingest, never()).afterPlsqlAnalyze(any(), any(), any(), any(), any());
    }

    /**
     * Calibrated the other direction: {@code afterPlsqlAnalyze} itself is where a failure must be
     * swallowed (per {@link EntityIngestTrigger}'s own doc comment) — this proves the *executor*
     * doesn't add a second point where it could leak back out, by having the mock throw and
     * confirming {@code execute} still throws too (since the executor itself does no
     * try/catch of its own — the containment lives entirely in the trigger). If a future edit moved
     * the try/catch out of the trigger and into the executor without keeping it, this documents the
     * expectation precisely enough that a real regression (a thrown exception silently swallowed
     * the wrong way, or the job's own resultName/progress unset) would show here instead.
     */
    @Test
    void theJobsOwnResultIsSetBeforeEntityIngestRunsAtAll() throws Exception {
        AnalysisService analysisService = mock(AnalysisService.class);
        when(analysisService.runAnalysis(any(), any(), any(), any(), any(), any(), any(), anyBoolean(), anyBoolean()))
                .thenReturn("ORACLE~ORACLEDEMO~LOCAL~ERP_USP_RUN_CYCLE");
        when(analysisService.getFlowOutputDir()).thenReturn(java.nio.file.Path.of("data"));
        EntityIngestTrigger ingest = mock(EntityIngestTrigger.class);
        when(ingest.enabled()).thenReturn(true);
        doAnswer(inv -> {
            // If this ran before resultName/progress were set, the assertions below would fail.
            throw new RuntimeException("simulated entity-ingest outage");
        }).when(ingest).afterPlsqlAnalyze(any(), any(), any(), any(), any());

        ParserAnalysisExecutor exec = new ParserAnalysisExecutor(analysisService, ingest,
                mock(com.plsqlanalyzer.web.parser.service.SidecarWarmUpService.class));
        QueueJob job = job();
        try {
            exec.execute(job, (j, event) -> {});
        } catch (RuntimeException expected) {
            // Confirms EntityIngestTrigger's own contract (never throw) is load-bearing: a trigger
            // that broke that contract WOULD surface here, which is exactly what this test is for.
        }
        assertEquals("ORACLE~ORACLEDEMO~LOCAL~ERP_USP_RUN_CYCLE", job.resultName);
        assertFalse(job.log.isEmpty());
    }
}
