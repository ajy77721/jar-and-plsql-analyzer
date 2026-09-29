package com.analyzer.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.model.EndpointInfo;
import com.jaranalyzer.service.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.LocalHandle;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.BiConsumer;

@Component
public class WarAnalysisExecutor {

    private static final Logger log = LoggerFactory.getLogger(WarAnalysisExecutor.class);

    private final WarParserService warParserService;
    private final JarParserService jarParserService;
    private final CallGraphService callGraphService;
    private final PersistenceService warPersistenceService;
    private final ProgressService progressService;
    private final ClaudeAnalysisService claudeAnalysisService;
    private final SourceEnrichmentService sourceEnrichmentService;
    private final MongoCatalogService mongoCatalogService;
    private final DomainConfigLoader domainConfigLoader;
    private final RepositoryStereotypeResolver repositoryStereotypeResolver;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ArtifactStore store;

    public WarAnalysisExecutor(WarParserService warParserService,
                               JarParserService jarParserService,
                               CallGraphService callGraphService,
                               @Qualifier("warPersistenceService") PersistenceService warPersistenceService,
                               ProgressService progressService,
                               ClaudeAnalysisService claudeAnalysisService,
                               SourceEnrichmentService sourceEnrichmentService,
                               MongoCatalogService mongoCatalogService,
                               DomainConfigLoader domainConfigLoader,
                               RepositoryStereotypeResolver repositoryStereotypeResolver,
                               @Qualifier("jarArtifactStore") ArtifactStore store) {
        this.store = store;
        this.warParserService = warParserService;
        this.jarParserService = jarParserService;
        this.callGraphService = callGraphService;
        this.warPersistenceService = warPersistenceService;
        this.progressService = progressService;
        this.claudeAnalysisService = claudeAnalysisService;
        this.sourceEnrichmentService = sourceEnrichmentService;
        this.mongoCatalogService = mongoCatalogService;
        this.domainConfigLoader = domainConfigLoader;
        this.repositoryStereotypeResolver = repositoryStereotypeResolver;
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

        // The uploaded per-WAR config is a LOCAL upload temp (written by the controller from the
        // multipart part, deleted in the finally below), not a stored artifact. Resolved once so
        // the path is not rebuilt from the String twice more further down.
        Path uploadedConfig = perJarConfigPath == null ? null : Path.of(perJarConfigPath);
        boolean hasUploadedConfig = uploadedConfig != null && Files.exists(uploadedConfig);
        // getPaths() is the WAR-flavoured WarDataPaths here, so the key lands under jar/war/.
        ArtifactKey storedConfigKey = warPersistenceService.getPaths().domainConfigKey(warName);

        // Apply per-WAR domain config override before analysis begins
        if (uploadedConfig != null) {
            if (hasUploadedConfig) {
                try {
                    Map<String, Object> perJarConfig = objectMapper.readValue(uploadedConfig.toFile(), Map.class);
                    domainConfigLoader.applyPerJarOverride(perJarConfig);
                    log.info("Applied per-WAR domain config override from {}", uploadedConfig.getFileName());
                } catch (Exception e) {
                    log.warn("Failed to load per-WAR domain config {}: {}", perJarConfigPath, e.getMessage());
                }
            }
        } else if (store.exists(storedConfigKey)) {
            // Re-analysis path: the stored config is an artifact, so it is read through the store.
            try (InputStream in = store.read(storedConfigKey)) {
                Map<String, Object> perJarConfig = objectMapper.readValue(in, Map.class);
                domainConfigLoader.applyPerJarOverride(perJarConfig);
                log.info("Applied stored per-WAR domain config for {}", warName);
            } catch (Exception e) {
                log.warn("Failed to load stored per-WAR domain config for {}: {}", warName, e.getMessage());
            }
        }

        Path tempWar = Path.of(tempFilePath);
        Path classesFile = null;
        // Closed in the finally below: releases the temp copy when the store is remote,
        // a no-op when the artifact was already a local file.
        LocalHandle storedWarHandle = null;

        // 3-phase timing breakdown, same shape as JarAnalysisExecutor's, so WAR analyses show
        // up in the same Performance Summary as JAR/PL/SQL.
        long crawlStart = System.nanoTime();
        long parseNanos = 0, discoverNanos = 0, saveNanos = 0;

        try {
            progress(job, broadcast, "[1/5] Parsing WAR bytecode...");
            long tParse0 = System.nanoTime();
            JarParserService.ParseResult parseResult = warParserService.parseWarToFile(tempWar.toFile(), basePackage);
            classesFile = parseResult.classesFile();
            classesFile = repositoryStereotypeResolver.resolveTransitiveRepositoryStereotypes(classesFile);
            parseNanos += System.nanoTime() - tParse0;
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
            long tDiscover0 = System.nanoTime();
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
            discoverNanos += System.nanoTime() - tDiscover0;
            System.gc();
            checkCancelled(job);

            progress(job, broadcast, "[5/5] Writing analysis to disk...");
            long tSave0 = System.nanoTime();
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
            if (hasUploadedConfig) {
                try {
                    warPersistenceService.storeDomainConfig(uploadedConfig, warName);
                } catch (Exception e) {
                    log.warn("Failed to persist per-WAR domain config: {}", e.getMessage());
                }
            }
            checkCancelled(job);

            progress(job, broadcast, "[5.6] Decompiling source code (" + targetEndpoints.size() + " endpoints)...");
            // The decompiler and the JarFile readers further down need a REAL local file —
            // they open a JarFile and seek — so this is ArtifactStore's documented escape
            // hatch rather than a stream. Materialized ONCE and shared by all four
            // consumers; a Path handed out by the persistence service would have been a
            // raw data-tree path that only resolves while storage is local.
            var storedWarKey = warPersistenceService.getPaths().storedJarKey(warName);
            if (store.exists(storedWarKey)) storedWarHandle = store.materialize(storedWarKey);
            Path storedWar = storedWarHandle == null ? null : storedWarHandle.path();
            if (store.exists(storedWarKey)) {
                // Key, not materialized Path: in remote mode that Path is a /tmp download outside
                // the data root and the decompiler would refuse every class (see JarAnalysisExecutor).
                // Pass the once-materialized local file too so every class decompiles from one open
                // JarFile; the key remains the decompile-cache identity.
                sourceEnrichmentService.enrichCallTreesWithSource(targetEndpoints, storedWarKey, storedWar);
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

            try {
                Path warForBundled = storedWar != null ? storedWar : tempWar;
                List<com.jaranalyzer.service.JarParserService.BundledJarInfo> bundledJars =
                        jarParserService.extractBundledJarInfo(warForBundled.toFile());
                warPersistenceService.storeBundledJarInfo(warName, bundledJars);
            } catch (Exception e) {
                log.warn("Bundled JAR info extraction failed (non-fatal): {}", e.getMessage());
            }

            try {
                Path warForDeps = storedWar != null ? storedWar : tempWar;
                List<com.jaranalyzer.model.ClassInfo> appClasses = new ArrayList<>();
                warParserService.streamClasses(classesFile, cls -> {
                    if (cls.getSourceJar() == null) appClasses.add(cls);
                });
                Map<String, java.util.Set<String>> depMap =
                        jarParserService.buildJarDependencyMap(warForDeps.toFile(), appClasses);
                warPersistenceService.storeJarDepsMap(warName, depMap);
            } catch (Exception e) {
                log.warn("JAR dependency map build failed (non-fatal): {}", e.getMessage());
            }

            saveNanos += System.nanoTime() - tSave0;

            job.resultName = warName;

            long totalMs = (System.nanoTime() - crawlStart) / 1_000_000;
            long parseMsVal = parseNanos / 1_000_000;
            long discoverMsVal = discoverNanos / 1_000_000;
            long saveMsVal = saveNanos / 1_000_000;
            log.info("[timing] {}: parse={}ms discover={}ms save={}ms total={}ms",
                    warName, parseMsVal, discoverMsVal, saveMsVal, totalMs);
            job.setTiming(parseMsVal, discoverMsVal, saveMsVal, totalMs);

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
            if (storedWarHandle != null) storedWarHandle.close();
            // LOCAL scratch, deliberately not store keys: tempWar is the multipart upload temp,
            // classesFile is the parser's intermediate JSONL, and uploadedConfig is the uploaded
            // config temp. All three live outside the data tree and are deleted here.
            Files.deleteIfExists(tempWar);
            if (classesFile != null) Files.deleteIfExists(classesFile);
            if (uploadedConfig != null) Files.deleteIfExists(uploadedConfig);
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
