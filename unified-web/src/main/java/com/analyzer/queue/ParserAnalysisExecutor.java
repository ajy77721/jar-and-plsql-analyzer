package com.analyzer.queue;

import com.plsqlanalyzer.web.parser.service.AnalysisService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.function.BiConsumer;

@Component
public class ParserAnalysisExecutor {

    private static final Logger log = LoggerFactory.getLogger(ParserAnalysisExecutor.class);

    private final AnalysisService analysisService;

    public ParserAnalysisExecutor(@Qualifier("parserAnalysisService") AnalysisService analysisService) {
        this.analysisService = analysisService;
    }

    public void execute(QueueJob job, BiConsumer<QueueJob, String> broadcast) throws Exception {
        String entryPoint = (String) job.metadata.get("entryPoint");
        String owner = (String) job.metadata.get("owner");
        String objectType = (String) job.metadata.get("objectType");

        progress(job, broadcast, "[1/3] Starting PL/SQL parse analysis: " + entryPoint);

        progress(job, broadcast, "[2/3] Running flow analysis...");
        String folderName = analysisService.runAnalysis(entryPoint, owner, objectType);

        if (job.status == QueueJob.Status.CANCELLED) {
            return;
        }

        job.resultName = folderName;
        progress(job, broadcast, "[3/3] Analysis complete: " + folderName);
    }

    private void progress(QueueJob job, BiConsumer<QueueJob, String> broadcast, String message) {
        job.updateProgress(message);
        broadcast.accept(job, "job-progress");
    }
}
