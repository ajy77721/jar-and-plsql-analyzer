package com.analyzer.repo;

import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.PrefetchReadStore;
import com.codeanalyzer.ingest.mongo.MongoIngestService;
import com.codeanalyzer.ingest.mongo.cli.IngestMain;
import com.codeanalyzer.ingest.mongo.verify.IntegrityChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;

/**
 * Mode 4 (entity-ingest) — runs {@link MongoIngestService} in-process right after a PL/SQL analyze
 * job completes, using {@link IngestMain#storeSource(ArtifactStore, String)} so it reads through
 * the exact same {@link ArtifactStore} the analysis was just written through, regardless of which
 * {@code app.storage.mode} that is.
 *
 * <h2>Best-effort, not blocking — deliberately</h2>
 *
 * <p>The primary artifact write already succeeded by the time this runs; that is the thing the
 * caller is waiting on. A failure here (an unreachable entity-ingest Mongo, a malformed artifact) is
 * logged loudly — the same pattern {@code MongoHousekeepingService}'s own background sweeps already
 * use — but never propagated to fail the analyze job itself. An analysis a user can see and use, with
 * a missing set of derived entity rows, is a smaller problem than an analysis silently lost because
 * a secondary indexing step threw.
 */
@Component
public class EntityIngestTrigger {

    private static final Logger log = LoggerFactory.getLogger(EntityIngestTrigger.class);

    /**
     * The PL/SQL parser feature's store — {@code afterPlsqlAnalyze} reads the analysis it just
     * wrote through this one.
     */
    private final ArtifactStore parserStore;

    /**
     * The JAR analyzer feature's store — {@code afterJarAnalyze} reads through this one. Kept
     * separate from {@link #parserStore} because the two features can be pointed at different
     * backends via {@code app.storage.parser.mode}/{@code app.storage.jar.mode}; using the parser
     * store for a JAR ingest probed the wrong backend (and found no {@code analysis.json}) whenever
     * those differed.
     */
    private final ArtifactStore jarStore;

    private final EntityIngestConfig.EntityIngestConnection connection;

    public EntityIngestTrigger(@Qualifier("parserArtifactStore") ArtifactStore parserStore,
                                @Qualifier("jarArtifactStore") ArtifactStore jarStore,
                                EntityIngestConfig.EntityIngestConnection connection) {
        this.parserStore = parserStore;
        this.jarStore = jarStore;
        this.connection = connection;
    }

    /** {@code false} when entity-ingest is off — callers can use this to skip work (e.g. logging)
     *  that would otherwise run for every analysis in the default configuration. */
    public boolean enabled() {
        return connection.enabled();
    }

    /**
     * The ingest reads the analysis tree back one artifact at a time — two S3 round trips each
     * ({@code exists} + {@code read}), sequentially, which measured ~98 s against a remote bucket.
     * Wrapping the store in a parallel prefetch cache turns those into one bulk download (bounded
     * pool, bounded heap — see {@link PrefetchReadStore}), leaving the ingest itself touching
     * little but memory. Falls through transparently on anything not cached, so a prefetch
     * failure can never break the ingest.
     */
    private ArtifactStore prefetched(ArtifactStore store, String rootKeyPrefix) throws IOException {
        PrefetchReadStore cached = new PrefetchReadStore(store);
        cached.prefetch(com.analyzer.storage.ArtifactKey.of(rootKeyPrefix.split("/")));
        return cached;
    }

    /**
     * @param dialect    e.g. {@code ORACLE}
     * @param project    e.g. {@code OracleDemo}
     * @param env        e.g. {@code local}
     * @param entryPoint e.g. {@code ERP_USP_RUN_CYCLE}
     * @param rootKeyPrefix the analysis's own root directory key (NOT its {@code api/} subfolder —
     *                      every mapper's own artifact path is already {@code api/...}-prefixed
     *                      relative to this root, and the CLI's own {@code tables.json} probe file
     *                      lives here too), e.g.
     *                      {@code plsql-parse/ORACLE/ORACLEDEMO/LOCAL/ERP_USP_RUN_CYCLE}
     */
    public void afterPlsqlAnalyze(String dialect, String project, String env, String entryPoint,
                                  String rootKeyPrefix) {
        if (!connection.enabled()) return;
        // Real diagnostic gap found live: this whole call sits inside ParserAnalysisExecutor
        // AFTER the job has already logged "[3/3] Analysis complete" — so a job stuck here for
        // several minutes (a real, observed pattern against a remote/Atlas cluster) looked
        // identical to a hung job, with nothing in the log to say entity-ingest was the reason or
        // how far it had gotten. See MongoIngestService.ingest() for the per-mapper/per-N-files
        // progress logs that fill in the middle of this span.
        long start = System.currentTimeMillis();
        log.info("[entity-ingest] {} -> starting (this runs AFTER the analysis job's own "
                + "'[3/3] Analysis complete' log line — a long gap here is entity-ingest writing to "
                + "Mongo, not the analysis itself hanging)", entryPoint);
        try {
            MongoIngestService.ArtifactSource source =
                    IngestMain.storeSource(prefetched(parserStore, rootKeyPrefix), rootKeyPrefix);
            try (InputStream probe = source.open("tables.json")) {
                if (probe == null) {
                    log.warn("[entity-ingest] skipped {}: no tables.json under {}", entryPoint,
                            rootKeyPrefix);
                    return;
                }
            }
            MongoIngestService svc = new MongoIngestService(connection.database());
            long tSchema = System.currentTimeMillis();
            svc.installSchema();
            log.info("[entity-ingest] {} -> schema install/verify took {}ms", entryPoint,
                    System.currentTimeMillis() - tSchema);
            String runId = svc.ingest(dialect, project, env, entryPoint, source).runId();
            long tIntegrity = System.currentTimeMillis();
            var findings = new IntegrityChecker(connection.database()).check(runId);
            log.info("[entity-ingest] {} -> integrity check took {}ms", entryPoint,
                    System.currentTimeMillis() - tIntegrity);
            if (findings.isEmpty()) {
                log.info("[entity-ingest] {} -> run {} (clean), total {}ms", entryPoint, runId,
                        System.currentTimeMillis() - start);
            } else {
                log.warn("[entity-ingest] {} -> run {} ({} integrity finding(s)), total {}ms: {}",
                        entryPoint, runId, findings.size(), System.currentTimeMillis() - start,
                        findings);
            }
        } catch (Exception e) {
            // Deliberately catches Exception, not a narrower type: this must never propagate into
            // the analyze job's own completion path (see class doc). Logged with the stack trace —
            // this is the only record of the failure, since nothing surfaces it to the UI.
            log.error("[entity-ingest] failed for {} ({}/{}/{}) after {}ms: {}", entryPoint, dialect,
                    project, env, System.currentTimeMillis() - start, e.toString(), e);
        }
    }

    /**
     * Jar flavour of {@link #afterPlsqlAnalyze}: same best-effort, in-process, after-the-fact
     * contract, but feeding {@link MongoIngestService#ingestJar} — the am_java_* / am_jar_*
     * mappers — instead of the PL/SQL ones.
     *
     * @param jarName       the analyzed JAR's name, as the executor already uses it
     * @param rootKeyPrefix the JAR analysis's own root directory key, e.g. {@code jar/commons-io}
     *                      (the {@code analysis.json} the jar mappers read sits directly beneath it)
     */
    public void afterJarAnalyze(String jarName, String rootKeyPrefix) {
        if (!connection.enabled()) return;
        long start = System.currentTimeMillis();
        log.info("[entity-ingest] jar {} -> starting", jarName);
        try {
            MongoIngestService.ArtifactSource source =
                    IngestMain.storeSource(prefetched(jarStore, rootKeyPrefix), rootKeyPrefix);
            try (InputStream probe = source.open("analysis.json")) {
                if (probe == null) {
                    log.warn("[entity-ingest] skipped jar {}: no analysis.json under {}", jarName,
                            rootKeyPrefix);
                    return;
                }
            }
            MongoIngestService svc = new MongoIngestService(connection.database());
            long tSchema = System.currentTimeMillis();
            svc.installSchema();
            log.info("[entity-ingest] jar {} -> schema install/verify took {}ms", jarName,
                    System.currentTimeMillis() - tSchema);
            String runId = svc.ingestJar(jarName, source).runId();
            var findings = new IntegrityChecker(connection.database()).check(runId);
            if (findings.isEmpty()) {
                log.info("[entity-ingest] jar {} -> run {} (clean), total {}ms", jarName, runId,
                        System.currentTimeMillis() - start);
            } else {
                log.warn("[entity-ingest] jar {} -> run {} ({} integrity finding(s)), total {}ms: {}",
                        jarName, runId, findings.size(), System.currentTimeMillis() - start, findings);
            }
        } catch (Exception e) {
            // Same contract as afterPlsqlAnalyze: never fails the analysis that just completed.
            log.error("[entity-ingest] failed for jar {} after {}ms: {}", jarName,
                    System.currentTimeMillis() - start, e.toString(), e);
        }
    }
}
