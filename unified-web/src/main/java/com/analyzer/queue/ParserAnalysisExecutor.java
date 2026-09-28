package com.analyzer.queue;

import com.plsqlanalyzer.web.parser.service.AnalysisService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

@Component
public class ParserAnalysisExecutor {

    private static final Logger log = LoggerFactory.getLogger(ParserAnalysisExecutor.class);

    private final AnalysisService analysisService;
    private final com.analyzer.repo.EntityIngestTrigger entityIngest;
    private final com.plsqlanalyzer.web.parser.service.SidecarWarmUpService sidecarWarmUp;

    public ParserAnalysisExecutor(@Qualifier("parserAnalysisService") AnalysisService analysisService,
                                   com.analyzer.repo.EntityIngestTrigger entityIngest,
                                   com.plsqlanalyzer.web.parser.service.SidecarWarmUpService sidecarWarmUp) {
        this.analysisService = analysisService;
        this.entityIngest = entityIngest;
        this.sidecarWarmUp = sidecarWarmUp;
    }

    public void execute(QueueJob job, BiConsumer<QueueJob, String> broadcast) throws Exception {
        String entryPoint = (String) job.metadata.get("entryPoint");
        String owner = (String) job.metadata.get("owner");
        String objectType = (String) job.metadata.get("objectType");
        String project = (String) job.metadata.get("project");
        String environment = (String) job.metadata.get("environment");
        String dbType = (String) job.metadata.get("dbType");
        String connName = (String) job.metadata.get("connName");

        progress(job, broadcast, "[1/3] Starting PL/SQL parse analysis: " + entryPoint);
        progress(job, broadcast, "[2/3] Running flow analysis...");

        // B7: heartbeat — broadcast every 5s during the synchronous analysis so the UI
        // does not appear frozen. Updates the elapsed-time display in lastMessage.
        AtomicLong elapsed = new AtomicLong(0);
        long startMs = System.currentTimeMillis();
        ScheduledExecutorService heartbeat = Executors.newSingleThreadScheduledExecutor(
                r -> { Thread t = new Thread(r, "analysis-heartbeat"); t.setDaemon(true); return t; });
        heartbeat.scheduleAtFixedRate(() -> {
            long sec = (System.currentTimeMillis() - startMs) / 1000;
            job.lastMessage = "[2/3] Running flow analysis... (" + sec + "s)";
            broadcast.accept(job, "job-progress");
        }, 5, 5, TimeUnit.SECONDS);

        String folderName;
        try {
            folderName = analysisService.runAnalysis(entryPoint, owner, objectType, project, environment, dbType, connName);
        } finally {
            heartbeat.shutdownNow();
        }

        if (job.status == QueueJob.Status.CANCELLED) {
            return;
        }

        job.resultName = folderName;

        // Attach the parse/discover/save timing breakdown DependencyCrawler already wrote into
        // this analysis's api/index.json, so the Performance Summary UI can show it for PL/SQL
        // jobs the same way it does for JAR/WAR jobs.
        try {
            var info = analysisService.getAnalysisInfo(folderName);
            if (info != null) {
                job.setTiming(info.getParseMs(), info.getDiscoverMs(), info.getSaveMs(),
                        info.getTotalDurationMs());
            }
        } catch (Exception e) {
            log.warn("Could not attach timing breakdown to job {}: {}", job.id, e.getMessage());
        }

        // Precompute this analysis's view summaries now that every artifact is written, so read
        // endpoints serve a few KB instead of re-scanning the node documents. This is the staging
        // path that matters: the job runs with its submitter's captured stores and credentials
        // installed (SsoJobContext, wrapped around this executor in AnalysisQueueService), so the
        // warm-up can reach the store here even when the background sweep — which has no session
        // and falls back to a credential-less default store on a pod — cannot. Best-effort: the
        // primary analysis has already succeeded, a miss self-heals on first read (see
        // AnalysisViewStore's lazy backfill), and warmOne never throws, so this cannot fail the job.
        if (!sidecarWarmUp.warmOne(folderName)) {
            log.warn("[ParserAnalysis] View summaries incomplete for {}; the read path will "
                    + "backfill them", folderName);
        }

        progress(job, broadcast, "[3/3] Analysis complete: " + folderName);

        // Mode 4 (entity-ingest) — best-effort, in-process, after the primary artifact write has
        // already succeeded; see EntityIngestTrigger's own doc comment for why a failure here must
        // never fail this job. enabled() short-circuits the (cheap but pointless) key-building below
        // in the default configuration, where this runs for every single analysis.
        if (entityIngest.enabled()) {
            // The analysis's own root directory, NOT its api/ subfolder: MongoIngestService's own
            // mappers read paths already prefixed with api/ (StatementsMapper.ARTIFACT =
            // "api/statements/index.json"), and the CLI's own probe file, tables.json, sits at this
            // root too (a flat legacy summary, distinct from api/tables/index.json) — confirmed
            // against a real analysis's own artifact keys, not assumed from the api/ path this
            // trigger originally (wrongly) used.
            String rootKeyPrefix = com.plsqlanalyzer.web.parser.service.AnalysisPaths
                    .dirKey(folderName).toString();
            entityIngest.afterPlsqlAnalyze(dbType, project, environment, entryPoint, rootKeyPrefix);
        }

        // Schedule table/view/trigger metadata validation as a follow-up queue job.
        // It runs after this job completes (AnalysisQueueService checks followUpJob in finally).
        // The configPath is read from plsql-env-config.yaml inside the output folder (persisted
        // by AnalysisService so it survives server restarts).
        // folderName is the identity name; map it to its nested <DBTYPE>/<PROJECT>/<ENV>/<OBJECT> dir.
        Path outputDir = com.plsqlanalyzer.web.parser.service.AnalysisPaths
                .dir(analysisService.getFlowOutputDir(), folderName);
        Path persistedConfig = outputDir.resolve("plsql-env-config.yaml");
        // Deliberately a LOCAL Files check, not store.exists(): plsql-env-config.yaml holds
        // plaintext production database passwords and must never enter the artifact store.
        // CredentialGuardStore blocks that leaf name outright — and reports it as absent — so
        // routing this probe through the store would always answer "no config" and silently
        // disable follow-up validation. AnalysisService.buildEnvConfigFile writes it locally for
        // the same reason; DbConnectionManager consumes it as a filesystem path.
        String cfgPath = Files.isRegularFile(persistedConfig)
                ? persistedConfig.toString()
                : null; // ParserValidationExecutor will handle null safely

        if (cfgPath != null) {
            Map<String, Object> followUp = new LinkedHashMap<>();
            followUp.put("type", QueueJob.Type.PARSER_VALIDATION.name());
            followUp.put("displayName", "Validate: " + folderName);
            followUp.put("analysisName", folderName);
            followUp.put("configPath", cfgPath);
            followUp.put("outputDir", outputDir.toAbsolutePath().toString());
            job.followUpJob = followUp;
            log.info("[ParserAnalysis] Scheduled follow-up validation for: {}", folderName);
        } else {
            log.warn("[ParserAnalysis] No persisted config found for {}; skipping follow-up validation", folderName);
        }
    }

    private void progress(QueueJob job, BiConsumer<QueueJob, String> broadcast, String message) {
        job.updateProgress(message);
        broadcast.accept(job, "job-progress");
    }
}
