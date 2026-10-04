package com.analyzer.queue;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsqlanalyzer.web.parser.service.AnalysisPaths;
import com.plsqlanalyzer.web.parser.service.AnalysisService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

@Component
public class ParserAnalysisExecutor {

    private static final Logger log = LoggerFactory.getLogger(ParserAnalysisExecutor.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final AnalysisService analysisService;
    private final com.analyzer.repo.EntityIngestTrigger entityIngest;
    private final com.plsqlanalyzer.web.parser.service.SidecarWarmUpService sidecarWarmUp;
    /**
     * The parser's routed artifact store — used to read the crawler's own {@code progress.json}
     * for the live per-procedure N/M. Through the store (not a local path) so the poll works in
     * remote storage mode too, and so the direct-I/O gate stays satisfied.
     */
    private final ArtifactStore store;

    public ParserAnalysisExecutor(@Qualifier("parserAnalysisService") AnalysisService analysisService,
                                   com.analyzer.repo.EntityIngestTrigger entityIngest,
                                   com.plsqlanalyzer.web.parser.service.SidecarWarmUpService sidecarWarmUp,
                                   @Qualifier("parserArtifactStore") ArtifactStore store) {
        this.analysisService = analysisService;
        this.entityIngest = entityIngest;
        this.sidecarWarmUp = sidecarWarmUp;
        this.store = store;
    }

    public void execute(QueueJob job, BiConsumer<QueueJob, String> broadcast) throws Exception {
        String entryPoint = (String) job.metadata.get("entryPoint");
        String owner = (String) job.metadata.get("owner");
        String objectType = (String) job.metadata.get("objectType");
        String project = (String) job.metadata.get("project");
        String environment = (String) job.metadata.get("environment");
        String dbType = (String) job.metadata.get("dbType");
        String connName = (String) job.metadata.get("connName");
        boolean forceRefresh = "true".equalsIgnoreCase(String.valueOf(job.metadata.get("forceRefresh")));
        // Fresh-default switch: only an explicit "Fast re-run" tick reads the cross-run caches.
        boolean fastReRun = "true".equalsIgnoreCase(String.valueOf(job.metadata.get("fastReRun")));

        // [1/8] Connect & discover — begins now; the crawl-progress poller below completes it the
        // moment the closure wave has enumerated the reachable objects (the count it needs early).
        job.beginStage("connect-discover", 0);
        progress(job, broadcast, "Connecting and discovering dependencies: " + entryPoint);

        // The crawl writes a per-object progress.json (visitKey -> PENDING/PROCESSING/DONE) as it
        // runs. Polling it is how the source-fetch and parse stages get a live N/M that scales with
        // the discovered procedure count, without the crawler needing any callback. Local-mode only:
        // the file lives under the local plsql-parse root; in remote storage mode it is absent and
        // the stages fall back to their elapsed-time estimate.
        long startMs = System.currentTimeMillis();
        ScheduledExecutorService heartbeat = Executors.newSingleThreadScheduledExecutor(
                r -> { Thread t = new Thread(r, "analysis-heartbeat"); t.setDaemon(true); return t; });
        ArtifactKey progressKey = crawlProgressKey(job.metadata);
        long progressStamp = lastModified(progressKey);
        heartbeat.scheduleAtFixedRate(() -> {
            long sec = (System.currentTimeMillis() - startMs) / 1000;
            job.lastMessage = "Discovering and parsing… (" + sec + "s)";
            pollCrawlProgress(job, progressKey, progressStamp);
            broadcast.accept(job, "job-progress");
        }, 2, 2, TimeUnit.SECONDS);

        String folderName;
        try {
            folderName = analysisService.runAnalysis(entryPoint, owner, objectType, project, environment, dbType, connName, forceRefresh, fastReRun);
        } finally {
            heartbeat.shutdownNow();
        }

        if (job.status == QueueJob.Status.CANCELLED) {
            return;
        }

        job.resultName = folderName;

        // The crawl has returned: discovery, source fetch and parse are all done (the poller kept
        // their N/M current), and the call graph + artifact save happened inside runAnalysis.
        job.completeStage("connect-discover");
        job.completeStage("source-fetch");
        job.completeStage("parse");
        job.beginStage("call-graph", 0);
        job.completeStage("call-graph");
        job.beginStage("save", 0);
        job.completeStage("save");
        // The artifacts are written and browsable now, even though warm-up/ingest remain.
        job.markReadyToBrowse();

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

        // [6/8] Sidecar warm-up — the facet precompute. A live N/14 and its own ETA, because it is
        // first-class work now: the job does not complete until it does.
        progress(job, broadcast, "Warming up view facets (0/14)…");
        job.beginStage("sidecar-warmup", com.plsqlanalyzer.web.parser.service.AnalysisViewWriter.VIEW_COUNT);
        final int facetTotal = com.plsqlanalyzer.web.parser.service.AnalysisViewWriter.VIEW_COUNT;
        boolean warmed = false;
        try {
            warmed = sidecarWarmUp.warmOne(folderName, done -> {
                job.advanceStage("sidecar-warmup", done, facetTotal);
                broadcast.accept(job, "job-progress");
            });
        } catch (Exception e) {
            log.warn("[ParserAnalysis] Sidecar warm-up failed for {}: {}", folderName, e.getMessage());
        }
        if (!warmed) {
            // Best-effort: a miss self-heals on first read (AnalysisViewStore's lazy backfill), so
            // the stage still completes — the job must not hang on an unreachable store.
            log.warn("[ParserAnalysis] View summaries incomplete for {}; the read path will "
                    + "backfill them", folderName);
        }
        job.completeStage("sidecar-warmup");
        progress(job, broadcast, "View facets warmed (" + facetTotal + "/" + facetTotal + ")");

        // [7/8] Estate facets (views/triggers/package-globals indexes) are precomputed inside the
        // same warm-up pass; they are a first-class stage so nothing hides behind the warm-up.
        job.beginStage("estate-facets", 0);
        job.completeStage("estate-facets");

        // [8/8] Entity ingest (Mode 4) — best-effort, in-process, after the primary artifact write
        // has already succeeded; see EntityIngestTrigger's own doc comment for why a failure here
        // must never fail this job. enabled() short-circuits the (cheap but pointless) key-building
        // below in the default configuration, where this runs for every single analysis.
        if (entityIngest.enabled()) {
            job.beginStage("entity-ingest", 0);
            progress(job, broadcast, "Ingesting entities…");
            // The analysis's own root directory, NOT its api/ subfolder: MongoIngestService's own
            // mappers read paths already prefixed with api/ (StatementsMapper.ARTIFACT =
            // "api/statements/index.json"), and the CLI's own probe file, tables.json, sits at this
            // root too (a flat legacy summary, distinct from api/tables/index.json) — confirmed
            // against a real analysis's own artifact keys, not assumed from the api/ path this
            // trigger originally (wrongly) used.
            String rootKeyPrefix = com.plsqlanalyzer.web.parser.service.AnalysisPaths
                    .dirKey(folderName).toString();
            entityIngest.afterPlsqlAnalyze(dbType, project, environment, entryPoint, rootKeyPrefix);
            job.completeStage("entity-ingest");
        } else {
            job.skipStage("entity-ingest");
        }

        progress(job, broadcast, "Analysis complete: " + folderName);

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

    /**
     * The store key of this run's {@code progress.json}, or null when it cannot be derived (no
     * entry point, or an owner/type that AnalysisService resolves itself). Mirrors the
     * identity-name construction in {@code AnalysisService.runAnalysis} for the common case where
     * the caller supplied owner/objectType/dbType.
     */
    private ArtifactKey crawlProgressKey(Map<String, Object> metadata) {
        try {
            String entryPoint = str(metadata.get("entryPoint"));
            if (entryPoint == null || entryPoint.isBlank()) return null;
            String owner = str(metadata.get("owner"));
            String objectType = str(metadata.get("objectType"));
            String project = str(metadata.get("project"));
            String environment = str(metadata.get("environment"));
            String dbType = str(metadata.get("dbType"));

            boolean isStandalone = objectType != null
                    && (objectType.equalsIgnoreCase("PROCEDURE") || objectType.equalsIgnoreCase("FUNCTION")
                        || objectType.equalsIgnoreCase("VIEW") || objectType.equalsIgnoreCase("TRIGGER"));
            boolean isPackageOnly = objectType != null && objectType.equalsIgnoreCase("PACKAGE")
                    && !entryPoint.contains(".");
            String target = entryPoint;
            if (!isStandalone && !isPackageOnly && owner != null && !owner.isBlank()
                    && !entryPoint.contains(".")) {
                target = owner.toUpperCase() + "." + entryPoint;
            }
            String objectSegment = isPackageOnly ? entryPoint : target;
            String name = AnalysisPaths.name(dbType, project, environment, objectSegment);
            return AnalysisPaths.dirKey(name).resolve("progress.json");
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Reads the crawl's own progress map (through the artifact store) and advances the
     * source-fetch/parse stages by the fraction of discovered objects that are DONE. Ignored until
     * the object has been rewritten for this run (lastModified after the pre-run stamp), so a stale
     * object from a previous analysis cannot report.
     */
    private void pollCrawlProgress(QueueJob job, ArtifactKey key, long stampBefore) {
        if (key == null || store == null || !store.exists(key)) return;
        try {
            if (store.lastModified(key) < stampBefore) return;
            JsonNode root;
            try (InputStream in = store.read(key)) {
                root = MAPPER.readTree(in);
            }
            if (root == null || !root.isObject()) return;
            int total = root.size();
            if (total <= 0) return;
            int done = 0;
            var it = root.fields();
            while (it.hasNext()) {
                String v = it.next().getValue().asText("");
                if (v.startsWith("DONE") || v.startsWith("FAILED")) done++;
            }
            job.completeStage("connect-discover");
            double frac = Math.min(1.0, (double) done / total);
            // The crawl's per-object work splits roughly 35% fetch / 65% parse; distributing the
            // DONE fraction across those two stages keeps both bars moving with the real count.
            if (frac < 0.35) {
                job.advanceStage("source-fetch", Math.round(frac / 0.35 * total), total);
            } else {
                job.completeStage("source-fetch");
                job.advanceStage("parse", Math.round((frac - 0.35) / 0.65 * total), total);
            }
        } catch (Exception ignored) {
            // A partial read mid-write is expected; the next tick retries.
        }
    }

    private long lastModified(ArtifactKey key) {
        try {
            return key != null && store != null && store.exists(key) ? store.lastModified(key) : 0L;
        } catch (Exception e) {
            return 0L;
        }
    }

    private static String str(Object o) {
        return o == null ? null : o.toString();
    }

    private void progress(QueueJob job, BiConsumer<QueueJob, String> broadcast, String message) {
        job.updateProgress(message);
        broadcast.accept(job, "job-progress");
    }
}
