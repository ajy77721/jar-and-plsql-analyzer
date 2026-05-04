package com.analyzer.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.model.EndpointInfo;
import com.jaranalyzer.service.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.BiConsumer;

@Component
public class WarAnalysisExecutor {

    private static final Logger log = LoggerFactory.getLogger(WarAnalysisExecutor.class);

    private final WarParserService warParserService;
    private final CallGraphService callGraphService;
    private final PersistenceService warPersistenceService;
    private final ProgressService progressService;
    private final ClaudeAnalysisService claudeAnalysisService;
    private final SourceEnrichmentService sourceEnrichmentService;
    private final MongoCatalogService mongoCatalogService;
    private final DomainConfigLoader domainConfigLoader;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public WarAnalysisExecutor(WarParserService warParserService,
                               CallGraphService callGraphService,
                               @Qualifier("warPersistenceService") PersistenceService warPersistenceService,
                               ProgressService progressService,
                               ClaudeAnalysisService claudeAnalysisService,
                               SourceEnrichmentService sourceEnrichmentService,
                               MongoCatalogService mongoCatalogService,
                               DomainConfigLoader domainConfigLoader) {
        this.warParserService = warParserService;
        this.callGraphService = callGraphService;
        this.warPersistenceService = warPersistenceService;
        this.progressService = progressService;
        this.claudeAnalysisService = claudeAnalysisService;
        this.sourceEnrichmentService = sourceEnrichmentService;
        this.mongoCatalogService = mongoCatalogService;
        this.domainConfigLoader = domainConfigLoader;
    }

    @SuppressWarnings("unchecked")
    public void execute(QueueJob job, BiConsumer<QueueJob, String> broadcast) throws Exception {
        String warName = (String) job.metadata.get("warName");
        String tempFilePath = (String) job.metadata.get("tempFilePath");
        String mode = (String) job.metadata.getOrDefault("mode", "static");
        String projectPath = (String) job.metadata.get("projectPath");
        String basePackage = (String) job.metadata.get("basePackage");
        String perJarConfigPath = (String) job.metadata.get("perJarConfigPath");
        long fileSize = ((Number) job.metadata.get("fileSize")).longValue();

        // Apply per-WAR domain config override before analysis begins
        if (perJarConfigPath != null) {
            Path configPath = Path.of(perJarConfigPath);
            if (Files.exists(configPath)) {
                try {
                    Map<String, Object> perJarConfig = objectMapper.readValue(configPath.toFile(), Map.class);
                    domainConfigLoader.applyPerJarOverride(perJarConfig);
                    log.info("Applied per-WAR domain config override from {}", configPath.getFileName());
                } catch (Exception e) {
                    log.warn("Failed to load per-WAR domain config {}: {}", perJarConfigPath, e.getMessage());
                }
            }
        } else {
            // Check if a stored per-WAR config exists for this WAR (re-analysis path)
            Path storedConfig = warPersistenceService.getDomainConfigPath(warName);
            if (storedConfig != null) {
                try {
                    Map<String, Object> perJarConfig = objectMapper.readValue(storedConfig.toFile(), Map.class);
                    domainConfigLoader.applyPerJarOverride(perJarConfig);
                    log.info("Applied stored per-WAR domain config for {}", warName);
                } catch (Exception e) {
                    log.warn("Failed to load stored per-WAR domain config for {}: {}", warName, e.getMessage());
                }
            }
        }

        Path tempWar = Path.of(tempFilePath);
        Path classesFile = null;

        try {
            progress(job, broadcast, "[1/5] Parsing WAR bytecode...");
            JarParserService.ParseResult parseResult = warParserService.parseWarToFile(tempWar.toFile(), basePackage);
            classesFile = parseResult.classesFile();
            checkCancelled(job);

            progress(job, broadcast, "[2/5] Extracting config files...");
            Map<String, String> configFiles = warParserService.extractConfigFiles(tempWar.toFile());
            java.util.Set<String> mongoCatalog = null;
            if (!configFiles.isEmpty()) {
                mongoCatalog = mongoCatalogService.fetchAndStore(warName, configFiles);
                if (mongoCatalog != null) {
                    progress(job, broadcast, "MongoDB catalog: " + mongoCatalog.size() + " collections/views");
                }
                Map<String, Object> connInfo = mongoCatalogService.extractAllConnectionInfo(configFiles);
                if (!connInfo.isEmpty()) {
                    warPersistenceService.storeConnections(warName, connInfo);
                }
            }
            checkCancelled(job);

            progress(job, broadcast, "[3/5] Building call-graph index...");
            callGraphService.buildIndex();
            callGraphService.setJarArtifactMap(parseResult.jarArtifactMap());
            if (mongoCatalog != null) callGraphService.setMongoCatalog(mongoCatalog);
            if (!configFiles.isEmpty()) {
                Map<String, String> props = MongoCatalogService.parseAllProperties(configFiles);
                if (!props.isEmpty()) {
                    callGraphService.setConfigProperties(props);
                }
            }
            warParserService.streamClasses(classesFile, cls -> callGraphService.addToIndex(cls));
            progress(job, broadcast, "Index built from " + parseResult.totalClasses() + " classes");
            checkCancelled(job);

            progress(job, broadcast, "[4/5] Building enriched call trees...");
            List<EndpointInfo> endpoints = callGraphService.buildEndpointsFromIndex();
            System.gc();
            checkCancelled(job);

            progress(job, broadcast, "[5/5] Writing analysis to disk...");
            String analyzedAt = LocalDateTime.now().toString();
            String mainArtifactId = parseResult.jarArtifactMap().get(null);
            String projectName = null;
            if (mainArtifactId != null) {
                projectName = java.util.Arrays.stream(mainArtifactId.split("-"))
                        .map(w -> Character.toUpperCase(w.charAt(0)) + w.substring(1).toLowerCase())
                        .collect(java.util.stream.Collectors.joining(" "));
            }

            int maxEp = claudeAnalysisService.getMaxEndpoints();
            List<EndpointInfo> targetEndpoints;
            if (maxEp >= 0 && maxEp < endpoints.size()) {
                targetEndpoints = endpoints.subList(0, maxEp);
            } else {
                targetEndpoints = endpoints;
            }

            progress(job, broadcast, "[5.5] Storing WAR copy...");
            warPersistenceService.storeJar(tempWar, warName);

            // Persist the per-WAR domain config (from temp path in metadata, if present)
            if (perJarConfigPath != null) {
                Path configPath = Path.of(perJarConfigPath);
                if (Files.exists(configPath)) {
                    try {
                        warPersistenceService.storeDomainConfig(configPath, warName);
                    } catch (Exception e) {
                        log.warn("Failed to persist per-WAR domain config: {}", e.getMessage());
                    }
                }
            }
            checkCancelled(job);

            progress(job, broadcast, "[5.6] Decompiling source code (" + targetEndpoints.size() + " endpoints)...");
            Path storedWar = warPersistenceService.getJarFilePath(warName);
            if (storedWar != null) {
                sourceEnrichmentService.enrichCallTreesWithSource(targetEndpoints, storedWar);
            }
            System.gc();
            checkCancelled(job);

            progress(job, broadcast, "[5.6.1] Writing analysis.json...");
            warPersistenceService.writeAnalysisStreaming(
                    warName, projectName, fileSize, analyzedAt,
                    parseResult.totalClasses(), endpoints.size(),
                    classesFile, endpoints, "STATIC", basePackage
            );

            progress(job, broadcast, "[5.7] Writing per-endpoint output folders...");
            claudeAnalysisService.writeEndpointOutputs(warName, targetEndpoints);

            try {
                Path warForResources = storedWar != null ? storedWar : tempWar;
                Map<String, String> resourceFiles = warParserService.extractResourceFiles(warForResources.toFile());
                warPersistenceService.storeResourceFiles(warName, resourceFiles);
            } catch (Exception e) {
                log.warn("Resource file extraction failed (non-fatal): {}", e.getMessage());
            }

            job.resultName = warName;
            progress(job, broadcast, "Static analysis complete: " + parseResult.totalClasses()
                    + " classes, " + endpoints.size() + " endpoints");

            progressService.start(warName);
            progressService.complete("Static analysis complete: " + parseResult.totalClasses()
                    + " classes, " + endpoints.size() + " endpoints");

            boolean claudeMode = "claude".equalsIgnoreCase(mode);
            if (claudeMode) {
                Map<String, Object> followUp = new LinkedHashMap<>();
                followUp.put("type", "CLAUDE_ENRICH");
                followUp.put("displayName", "Claude enrich: " + warName);
                followUp.put("jarName", warName);
                followUp.put("projectPath", projectPath);
                followUp.put("projectName", projectName);
                followUp.put("fileSize", fileSize);
                followUp.put("analyzedAt", analyzedAt);
                followUp.put("totalClasses", parseResult.totalClasses());
                if (classesFile != null) {
                    followUp.put("classesFilePath", classesFile.toString());
                    classesFile = null;
                }
                job.followUpJob = followUp;
            }

        } finally {
            domainConfigLoader.clearPerJarOverride();
            Files.deleteIfExists(tempWar);
            if (classesFile != null) Files.deleteIfExists(classesFile);
            if (perJarConfigPath != null) Files.deleteIfExists(Path.of(perJarConfigPath));
        }
    }

    private void progress(QueueJob job, BiConsumer<QueueJob, String> broadcast, String message) {
        job.updateProgress(message);
        broadcast.accept(job, "job-progress");
    }

    private void checkCancelled(QueueJob job) {
        if (job.status == QueueJob.Status.CANCELLED) {
            throw new RuntimeException("Job cancelled");
        }
    }
}
