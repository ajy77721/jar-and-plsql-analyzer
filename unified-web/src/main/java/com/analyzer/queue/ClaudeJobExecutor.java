package com.analyzer.queue;

import com.jaranalyzer.model.CorrectionResult;
import com.jaranalyzer.model.EndpointInfo;
import com.jaranalyzer.model.JarAnalysis;
import com.jaranalyzer.service.*;
import com.plsqlanalyzer.analyzer.model.AnalysisResult;
import com.plsqlanalyzer.analyzer.service.AnalysisService;
import com.plsqlanalyzer.web.service.ClaudeVerificationService;
import com.plsqlanalyzer.web.service.ClaudeVerificationService.VerificationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

@Component
public class ClaudeJobExecutor {

    private static final Logger log = LoggerFactory.getLogger(ClaudeJobExecutor.class);

    private final PersistenceService jarPersistence;
    private final ClaudeAnalysisService claudeAnalysisService;
    private final ClaudeEnrichmentTracker claudeTracker;
    private final ProgressService jarProgressService;
    private final SourceEnrichmentService sourceEnrichmentService;
    private final ClaudeCorrectionService correctionService;
    private final CorrectionPersistence correctionPersistence;
    private final CorrectionMerger correctionMerger;
    private final ClaudeSessionManager jarSessionManager;

    private final AnalysisService plsqlAnalysisService;
    private final ClaudeVerificationService verificationService;
    private final com.plsqlanalyzer.web.service.PersistenceService plsqlPersistence;
    private final com.plsqlanalyzer.web.service.ClaudeSessionManager plsqlSessionManager;

    public ClaudeJobExecutor(PersistenceService jarPersistence,
                              ClaudeAnalysisService claudeAnalysisService,
                              ClaudeEnrichmentTracker claudeTracker,
                              ProgressService jarProgressService,
                              SourceEnrichmentService sourceEnrichmentService,
                              ClaudeCorrectionService correctionService,
                              CorrectionPersistence correctionPersistence,
                              CorrectionMerger correctionMerger,
                              ClaudeSessionManager jarSessionManager,
                              AnalysisService plsqlAnalysisService,
                              ClaudeVerificationService verificationService,
                              @Qualifier("plsqlPersistenceService")
                              com.plsqlanalyzer.web.service.PersistenceService plsqlPersistence,
                              @Qualifier("plsqlClaudeSessionManager")
                              com.plsqlanalyzer.web.service.ClaudeSessionManager plsqlSessionManager) {
        this.jarPersistence = jarPersistence;
        this.claudeAnalysisService = claudeAnalysisService;
        this.claudeTracker = claudeTracker;
        this.jarProgressService = jarProgressService;
        this.sourceEnrichmentService = sourceEnrichmentService;
        this.correctionService = correctionService;
        this.correctionPersistence = correctionPersistence;
        this.correctionMerger = correctionMerger;
        this.jarSessionManager = jarSessionManager;
        this.plsqlAnalysisService = plsqlAnalysisService;
        this.verificationService = verificationService;
        this.plsqlPersistence = plsqlPersistence;
        this.plsqlSessionManager = plsqlSessionManager;
    }

    public void execute(QueueJob job, BiConsumer<QueueJob, String> broadcast) throws Exception {
        boolean isPlsql = job.type == QueueJob.Type.PLSQL_CLAUDE_VERIFY;
        String name = isPlsql ? (String) job.metadata.get("analysisName")
                              : (String) job.metadata.get("jarName");
        ClaudeSessionManager.SessionType sType = mapSessionType(job.type);

        String sessionId = isPlsql
                ? plsqlSessionManager.registerSession(name != null ? name : "unknown", job.displayName)
                : jarSessionManager.registerSession(name != null ? name : "unknown", sType, job.displayName);

        try {
            switch (job.type) {
                case CLAUDE_ENRICH -> executeEnrich(job, broadcast);
                case CLAUDE_ENRICH_SINGLE -> executeEnrichSingle(job, broadcast);
                case CLAUDE_RESCAN -> executeRescan(job, broadcast);
                case CLAUDE_FULL_SCAN -> executeFullScan(job, broadcast);
                case CLAUDE_CORRECT -> executeCorrect(job, broadcast);
                case CLAUDE_CORRECT_SINGLE -> executeCorrectSingle(job, broadcast);
                case PLSQL_CLAUDE_VERIFY -> executePlsqlVerify(job, broadcast);
                default -> throw new IllegalArgumentException("Unknown Claude job type: " + job.type);
            }
            if (!isPlsql) jarSessionManager.completeSession(sessionId, true, null);
            else plsqlSessionManager.completeSession(sessionId, true, null);
        } catch (Exception e) {
            if (Thread.currentThread().isInterrupted()) {
                if (!isPlsql) jarSessionManager.killSession(sessionId);
                else plsqlSessionManager.killSession(sessionId);
            } else {
                if (!isPlsql) jarSessionManager.completeSession(sessionId, false, e.getMessage());
                else plsqlSessionManager.completeSession(sessionId, false, e.getMessage());
            }
            throw e;
        }
    }

    private ClaudeSessionManager.SessionType mapSessionType(QueueJob.Type type) {
        return switch (type) {
            case CLAUDE_ENRICH -> ClaudeSessionManager.SessionType.UPLOAD;
            case CLAUDE_ENRICH_SINGLE -> ClaudeSessionManager.SessionType.SINGLE_ENDPOINT;
            case CLAUDE_RESCAN -> ClaudeSessionManager.SessionType.RESCAN;
            case CLAUDE_FULL_SCAN -> ClaudeSessionManager.SessionType.FULL_SCAN;
            case CLAUDE_CORRECT, CLAUDE_CORRECT_SINGLE -> ClaudeSessionManager.SessionType.FRESH_SCAN;
            default -> ClaudeSessionManager.SessionType.FRESH_SCAN;
        };
    }

    private void executeEnrich(QueueJob job, BiConsumer<QueueJob, String> broadcast) throws Exception {
        String jarName = (String) job.metadata.get("jarName");
        String projectPath = (String) job.metadata.get("projectPath");
        String projectName = (String) job.metadata.get("projectName");
        long fileSize = ((Number) job.metadata.getOrDefault("fileSize", 0L)).longValue();
        String analyzedAt = (String) job.metadata.get("analyzedAt");
        int totalClasses = ((Number) job.metadata.getOrDefault("totalClasses", 0)).intValue();
        String classesFilePath = (String) job.metadata.get("classesFilePath");

        JarAnalysis analysis = jarPersistence.load(jarName);
        if (analysis == null) throw new RuntimeException("JAR not found: " + jarName);
        List<EndpointInfo> endpoints = analysis.getEndpoints();

        List<String> epKeys = endpoints.stream()
                .map(ep -> ep.getControllerSimpleName() + "." + ep.getMethodName())
                .toList();
        claudeTracker.startTracking(jarName, epKeys);

        progress(job, broadcast, "Claude enrichment starting (" + endpoints.size() + " endpoints)...");

        try {
            if (projectPath != null && !projectPath.isBlank()) {
                claudeAnalysisService.enrichEndpoints(endpoints, projectPath, jarName, jarProgressService);
            } else {
                claudeAnalysisService.enrichFromAnalysis(endpoints, jarName, jarProgressService);
            }

            if (classesFilePath != null) {
                Path classesFile = Path.of(classesFilePath);
                if (Files.exists(classesFile)) {
                    jarPersistence.writeAnalysisStreamingCorrected(
                            jarName, projectName, fileSize, analyzedAt,
                            totalClasses, endpoints.size(),
                            classesFile, endpoints, "CORRECTED"
                    );
                    Files.deleteIfExists(classesFile);
                }
            } else {
                analysis.setAnalysisMode("CORRECTED");
                jarPersistence.saveCorrected(analysis);
            }

            claudeTracker.markComplete(jarName);
            job.resultName = jarName;
            progress(job, broadcast, "Claude enrichment complete");
        } catch (Exception e) {
            claudeTracker.markFailed(jarName, e.getMessage());
            throw e;
        }
    }

    private void executeEnrichSingle(QueueJob job, BiConsumer<QueueJob, String> broadcast) throws Exception {
        String jarName = (String) job.metadata.get("jarName");
        String endpointName = (String) job.metadata.get("endpointName");

        JarAnalysis analysis = jarPersistence.load(jarName);
        if (analysis == null) throw new RuntimeException("JAR not found: " + jarName);

        EndpointInfo target = findEndpoint(analysis, endpointName);
        claudeTracker.startTracking(jarName, List.of(endpointName));

        progress(job, broadcast, "Enriching endpoint: " + endpointName);

        claudeAnalysisService.enrichFromAnalysis(List.of(target), jarName, jarProgressService);
        analysis.setAnalysisMode("CORRECTED");
        jarPersistence.saveCorrected(analysis);
        claudeTracker.markComplete(jarName);
        job.resultName = jarName;
        progress(job, broadcast, "Endpoint enrichment complete");
    }

    private void executeRescan(QueueJob job, BiConsumer<QueueJob, String> broadcast) throws Exception {
        String jarName = (String) job.metadata.get("jarName");
        boolean resume = Boolean.TRUE.equals(job.metadata.get("resume"));

        JarAnalysis analysis = jarPersistence.load(jarName);
        if (analysis == null) throw new RuntimeException("JAR not found: " + jarName);
        List<EndpointInfo> endpoints = analysis.getEndpoints();

        List<String> epKeys = endpoints.stream()
                .map(ep -> ep.getControllerSimpleName() + "." + ep.getMethodName())
                .toList();
        claudeTracker.startTracking(jarName, epKeys);

        progress(job, broadcast, (resume ? "Resuming" : "Starting") + " rescan (" + endpoints.size() + " endpoints)...");

        claudeAnalysisService.enrichFromAnalysis(endpoints, jarName, jarProgressService, resume);
        analysis.setAnalysisMode("CORRECTED");
        jarPersistence.saveCorrected(analysis);
        claudeTracker.markComplete(jarName);
        job.resultName = jarName;
        progress(job, broadcast, "Rescan complete");
    }

    private void executeFullScan(QueueJob job, BiConsumer<QueueJob, String> broadcast) throws Exception {
        String jarName = (String) job.metadata.get("jarName");
        boolean resume = Boolean.TRUE.equals(job.metadata.get("resume"));

        JarAnalysis analysis = jarPersistence.load(jarName);
        if (analysis == null) throw new RuntimeException("JAR not found: " + jarName);
        List<EndpointInfo> endpoints = analysis.getEndpoints();

        List<String> epKeys = endpoints.stream()
                .map(ep -> ep.getControllerSimpleName() + "." + ep.getMethodName())
                .toList();
        claudeTracker.startTracking(jarName, epKeys);

        progress(job, broadcast, "[1/4] Generating corrections...");
        if (!resume) correctionPersistence.deleteAll(jarName);
        correctionService.correctEndpoints(endpoints, jarName, jarProgressService, resume);

        progress(job, broadcast, "[2/4] Loading corrections...");
        Map<String, CorrectionResult> corrections = correctionPersistence.loadAllCorrections(jarName);
        if (corrections.isEmpty()) {
            claudeTracker.markComplete(jarName);
            progress(job, broadcast, "No corrections generated");
            return;
        }

        progress(job, broadcast, "[3/4] Merging into static base...");
        JarAnalysis fresh = jarPersistence.loadStatic(jarName);
        if (fresh == null) throw new RuntimeException("Static analysis file missing");

        Map<String, Object> versionInfo = jarPersistence.getVersionInfo(jarName);
        int prevIteration = ((Number) versionInfo.getOrDefault("claudeIteration", 0)).intValue();
        jarPersistence.rotateCorrected(jarName);

        int changes = correctionMerger.applyCorrections(fresh, corrections);

        progress(job, broadcast, "[4/4] Saving corrected version...");
        fresh.setAnalysisMode("CORRECTED");
        fresh.setCorrectionAppliedAt(LocalDateTime.now().toString());
        fresh.setCorrectionCount(corrections.size());
        fresh.setClaudeIteration(prevIteration + 1);
        jarPersistence.saveCorrected(fresh);

        claudeTracker.markComplete(jarName);
        job.resultName = jarName;
        progress(job, broadcast, "Full scan complete — " + corrections.size() + " corrections, " + changes + " changes");
    }

    private void executeCorrect(QueueJob job, BiConsumer<QueueJob, String> broadcast) throws Exception {
        String jarName = (String) job.metadata.get("jarName");
        boolean resume = Boolean.TRUE.equals(job.metadata.get("resume"));

        JarAnalysis analysis = jarPersistence.load(jarName);
        if (analysis == null) throw new RuntimeException("JAR not found: " + jarName);

        progress(job, broadcast, "Correction scan (" + analysis.getEndpoints().size() + " endpoints)...");
        if (!resume) correctionPersistence.deleteAll(jarName);
        correctionService.correctEndpoints(analysis.getEndpoints(), jarName, jarProgressService, resume);
        job.resultName = jarName;
        progress(job, broadcast, "Correction scan complete");
    }

    private void executeCorrectSingle(QueueJob job, BiConsumer<QueueJob, String> broadcast) throws Exception {
        String jarName = (String) job.metadata.get("jarName");
        String endpointName = (String) job.metadata.get("endpointName");

        JarAnalysis analysis = jarPersistence.load(jarName);
        if (analysis == null) throw new RuntimeException("JAR not found: " + jarName);

        EndpointInfo target = findEndpoint(analysis, endpointName);
        progress(job, broadcast, "Correcting: " + endpointName);

        CorrectionResult result = correctionService.correctSingleEndpoint(target, jarName);
        if (result != null) {
            correctionPersistence.saveCorrection(jarName, endpointName, result);
        }
        job.resultName = jarName;
        progress(job, broadcast, "Single correction complete");
    }

    private void executePlsqlVerify(QueueJob job, BiConsumer<QueueJob, String> broadcast) throws Exception {
        String analysisName = (String) job.metadata.get("analysisName");
        boolean resume = Boolean.TRUE.equals(job.metadata.get("resume"));

        AnalysisResult result = plsqlPersistence.loadStatic(analysisName);
        if (result == null) {
            result = plsqlAnalysisService.getLatestResult();
        }
        if (result == null) throw new RuntimeException("No PL/SQL analysis found: " + analysisName);

        final AnalysisResult analysisResult = result;
        progress(job, broadcast, (resume ? "Resuming" : "Starting") + " verification: " + analysisName);

        VerificationResult verResult;
        if (resume) {
            verResult = verificationService.resume(analysisResult, msg -> progress(job, broadcast, msg));
        } else {
            verResult = verificationService.verify(analysisResult, msg -> progress(job, broadcast, msg));
        }

        if (verResult != null && verResult.error == null && verResult.newCount > 0) {
            progress(job, broadcast, "Merging " + verResult.newCount + " new findings...");
        }

        job.resultName = analysisName;
        progress(job, broadcast, "Verification complete");
    }

    private EndpointInfo findEndpoint(JarAnalysis analysis, String endpointName) {
        for (EndpointInfo ep : analysis.getEndpoints()) {
            String key = ep.getControllerSimpleName() + "." + ep.getMethodName();
            if (key.equals(endpointName)) return ep;
        }
        throw new RuntimeException("Endpoint not found: " + endpointName);
    }

    private void progress(QueueJob job, BiConsumer<QueueJob, String> broadcast, String message) {
        job.updateProgress(message);
        broadcast.accept(job, "job-progress");
    }
}
