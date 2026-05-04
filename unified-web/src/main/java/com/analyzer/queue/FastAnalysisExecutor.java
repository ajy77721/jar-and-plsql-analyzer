package com.analyzer.queue;

import com.plsqlanalyzer.analyzer.model.AnalysisResult;
import com.plsqlanalyzer.analyzer.service.AnalysisService;
import com.plsqlanalyzer.analyzer.service.FastAnalysisService;
import com.plsqlanalyzer.config.DbUserConfig;
import com.plsqlanalyzer.config.EnvironmentConfig;
import com.plsqlanalyzer.config.PlsqlConfig;
import com.plsqlanalyzer.web.service.AnalysisJobService;
import com.plsqlanalyzer.web.service.DbSourceFetcher;
import com.plsqlanalyzer.web.service.PersistenceService;
import com.plsqlanalyzer.web.service.ProgressService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

@Component
public class FastAnalysisExecutor {

    private static final Logger log = LoggerFactory.getLogger(FastAnalysisExecutor.class);

    private final AnalysisService analysisService;
    private final PlsqlConfig config;
    private final DbSourceFetcher dbFetcher;
    private final PersistenceService persistenceService;
    private final ProgressService progressService;
    private final AnalysisJobService jobService;

    public FastAnalysisExecutor(AnalysisService analysisService,
                                 PlsqlConfig config,
                                 DbSourceFetcher dbFetcher,
                                 @Qualifier("plsqlPersistenceService") PersistenceService persistenceService,
                                 @Qualifier("plsqlProgressService") ProgressService progressService,
                                 AnalysisJobService jobService) {
        this.analysisService = analysisService;
        this.config = config;
        this.dbFetcher = dbFetcher;
        this.persistenceService = persistenceService;
        this.progressService = progressService;
        this.jobService = jobService;
    }

    public void execute(QueueJob job, BiConsumer<QueueJob, String> broadcast) throws Exception {
        String username = (String) job.metadata.get("username");
        String objectName = (String) job.metadata.get("objectName");
        String objectType = (String) job.metadata.get("objectType");
        String procedureName = (String) job.metadata.get("procedureName");
        String projectName = (String) job.metadata.get("project");
        String envName = (String) job.metadata.get("env");

        AnalysisJobService.Job legacyJob = null;
        String legacyJobId = (String) job.metadata.get("legacyJobId");
        if (legacyJobId != null) {
            legacyJob = jobService.getJob(legacyJobId);
        }
        if (legacyJob == null) {
            legacyJob = jobService.getJobByQueueId(job.id);
        }
        if (legacyJob != null) {
            jobService.markRunning(legacyJob);
        }

        String resolvedJdbcUrl = null;
        List<DbUserConfig> resolvedUsers = null;
        if (projectName != null && !projectName.isBlank() && envName != null && !envName.isBlank()) {
            EnvironmentConfig envConfig = config.resolveEnvironment(projectName, envName);
            if (envConfig != null) {
                resolvedJdbcUrl = envConfig.getJdbcUrl();
                resolvedUsers = envConfig.getUsers();
            }
        }

        List<DbUserConfig> activeUsers = resolvedUsers != null ? resolvedUsers : config.getDbUsers();
        DbUserConfig user = activeUsers.stream()
                .filter(u -> u.getUsername().equalsIgnoreCase(username))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("User not found in config: " + username));

        List<String> allSchemas = activeUsers.stream()
                .map(u -> u.getUsername().toUpperCase())
                .collect(Collectors.toList());

        final String activeJdbcUrl = resolvedJdbcUrl;
        final AnalysisJobService.Job fLegacyJob = legacyJob;

        progress(job, broadcast, "Connecting to database (fast mode)...");

        try (Connection conn = activeJdbcUrl != null
                ? dbFetcher.getConnection(user, activeJdbcUrl)
                : dbFetcher.getConnection(user)) {

            FastAnalysisService fastService = new FastAnalysisService(
                    analysisService.getDictionaryService(),
                    analysisService.getParserService());

            fastService.setConnectionSupplier(() -> {
                try {
                    return activeJdbcUrl != null
                            ? dbFetcher.getConnection(user, activeJdbcUrl)
                            : dbFetcher.getConnection(user);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to create parallel connection", e);
                }
            });

            AnalysisResult result = fastService.analyze(
                    conn, username, objectName, objectType, procedureName,
                    allSchemas, msg -> {
                        progressService.sendProgress(msg);
                        progress(job, broadcast, msg);
                        if (fLegacyJob != null) {
                            jobService.updateProgress(fLegacyJob, msg);
                        }
                    });

            if (job.status == QueueJob.Status.CANCELLED) {
                progressService.sendError("Analysis cancelled");
                if (fLegacyJob != null) {
                    fLegacyJob.status = AnalysisJobService.JobStatus.CANCELLED;
                    fLegacyJob.completedAt = java.time.Instant.now();
                    fLegacyJob.currentStep = "Cancelled";
                }
                return;
            }

            String name = persistenceService.save(result);
            job.resultName = name;
            progressService.sendComplete("Fast analysis complete: " + name
                    + " (" + result.getProcedureCount() + " procs, "
                    + result.getTableOperations().size() + " tables)");

            analysisService.setLatestResult(result);

            if (fLegacyJob != null) {
                jobService.markComplete(fLegacyJob, name);
            }
            log.info("[FAST] Analysis complete: {}", name);
        }
    }

    private void progress(QueueJob job, BiConsumer<QueueJob, String> broadcast, String msg) {
        if (broadcast != null) broadcast.accept(job, msg);
        log.info("[FAST] {}", msg);
    }
}
