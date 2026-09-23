package com.analyzer.queue;

import com.plsqlanalyzer.web.parser.service.ClaudePurposeSummaryModels.PurposeSummaryRunResult;
import com.plsqlanalyzer.web.parser.service.ClaudePurposeSummaryService;
import org.springframework.stereotype.Component;

import java.util.function.BiConsumer;

/**
 * §267 — background job wrapper around {@link ClaudePurposeSummaryService#generate}, following the
 * exact shape {@link JarExportBundleExecutor} established for UI-043: an LLM-driven job that can run
 * long, submitted via {@link AnalysisQueueService} rather than blocking the triggering HTTP request.
 */
@Component
public class PurposeSummaryExecutor {

    private final ClaudePurposeSummaryService purposeSummaryService;

    public PurposeSummaryExecutor(ClaudePurposeSummaryService purposeSummaryService) {
        this.purposeSummaryService = purposeSummaryService;
    }

    public void execute(QueueJob job, BiConsumer<QueueJob, String> broadcast) throws Exception {
        String analysisName = (String) job.metadata.get("analysisName");
        if (analysisName == null || analysisName.isBlank()) {
            throw new IllegalArgumentException("analysisName is required");
        }

        PurposeSummaryRunResult result = purposeSummaryService.generate(analysisName, msg -> {
            job.updateProgress(msg);
            broadcast.accept(job, "job-progress");
        });

        if (result.error != null) {
            throw new RuntimeException(result.error);
        }

        job.resultName = analysisName;
        job.metadata.put("generated", result.generated);
        job.metadata.put("skippedCached", result.skippedCached);
        job.metadata.put("failed", result.failed);
        job.metadata.put("claudeCallCount", result.claudeCallCount);
    }
}
