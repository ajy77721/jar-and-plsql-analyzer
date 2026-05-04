package com.jaranalyzer.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.analyzer.queue.AnalysisQueueService;
import com.analyzer.queue.QueueJob;
import com.jaranalyzer.model.CorrectionResult;
import com.jaranalyzer.model.EndpointInfo;
import com.jaranalyzer.model.JarAnalysis;
import com.jaranalyzer.service.*;
import com.jaranalyzer.service.AnalysisDataProvider;
import com.jaranalyzer.service.AnalysisDataProviderFactory;
import com.jaranalyzer.service.AnalysisNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.*;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

@RestController("jarAnalyzerController")
@RequestMapping("/api/jar/jars")
public class AnalyzerController {

    private static final Logger log = LoggerFactory.getLogger(AnalyzerController.class);

    private final JarParserService parserService;
    private final CallGraphService callGraphService;
    private final PersistenceService persistenceService;
    private final ProgressService progressService;
    private final ObjectMapper objectMapper;
    private final ClaudeAnalysisService claudeAnalysisService;
    private final SourceEnrichmentService sourceEnrichmentService;
    private final ExcelExportService excelExportService;
    private final ClaudeEnrichmentTracker claudeTracker;
    private final ClaudeSessionManager sessionManager;
    private final MongoCatalogService mongoCatalogService;
    private final ClaudeCorrectionService correctionService;
    private final CorrectionPersistence correctionPersistence;
    private final CorrectionMerger correctionMerger;
    private final AnalysisQueueService queueService;
    private final AnalysisDataProviderFactory providerFactory;
    private final ClaudeCallLogger claudeCallLogger;

    public AnalyzerController(JarParserService parserService,
                              CallGraphService callGraphService,
                              PersistenceService persistenceService,
                              ProgressService progressService,
                              ObjectMapper objectMapper,
                              ClaudeAnalysisService claudeAnalysisService,
                              SourceEnrichmentService sourceEnrichmentService,
                              ExcelExportService excelExportService,
                              ClaudeEnrichmentTracker claudeTracker,
                              ClaudeSessionManager sessionManager,
                              MongoCatalogService mongoCatalogService,
                              ClaudeCorrectionService correctionService,
                              CorrectionPersistence correctionPersistence,
                              CorrectionMerger correctionMerger,
                              AnalysisQueueService queueService,
                              AnalysisDataProviderFactory providerFactory,
                              ClaudeCallLogger claudeCallLogger) {
        this.parserService = parserService;
        this.callGraphService = callGraphService;
        this.persistenceService = persistenceService;
        this.progressService = progressService;
        this.objectMapper = objectMapper;
        this.claudeAnalysisService = claudeAnalysisService;
        this.sourceEnrichmentService = sourceEnrichmentService;
        this.excelExportService = excelExportService;
        this.claudeTracker = claudeTracker;
        this.sessionManager = sessionManager;
        this.mongoCatalogService = mongoCatalogService;
        this.correctionService = correctionService;
        this.correctionPersistence = correctionPersistence;
        this.correctionMerger = correctionMerger;
        this.queueService = queueService;
        this.providerFactory = providerFactory;
        this.claudeCallLogger = claudeCallLogger;
    }

    @PostMapping
    public Map<String, Object> uploadAndAnalyze(
            @RequestParam("file") MultipartFile file,
            @RequestParam(value = "mode", defaultValue = "static") String mode,
            @RequestParam(value = "projectPath", required = false) String projectPath,
            @RequestParam(value = "renameSuffix", required = false) String renameSuffix,
            @RequestParam(value = "basePackage", required = false) String basePackage,
            @RequestParam(value = "configFile", required = false) MultipartFile configFile) throws IOException {

        String jarName = file.getOriginalFilename();
        if (jarName == null || (!jarName.endsWith(".jar") && !jarName.endsWith(".war"))) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "File must be a .jar or .war");
        }

        if (renameSuffix != null && !renameSuffix.isBlank()) {
            String ext = jarName.endsWith(".war") ? ".war" : ".jar";
            String base = jarName.substring(0, jarName.length() - ext.length());
            jarName = base + "_" + renameSuffix + ext;
        }

        log.info("Upload received: {} ({} MB), mode={} — submitting to queue", jarName, file.getSize() / (1024 * 1024), mode);

        // Save JAR/WAR to temp file immediately so the HTTP request can return
        Path tempJar = Files.createTempFile("jar-analyze-", jarName.endsWith(".war") ? ".war" : ".jar");
        file.transferTo(tempJar);

        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("jarName", jarName);
        metadata.put("tempFilePath", tempJar.toString());
        metadata.put("fileSize", file.getSize());
        metadata.put("mode", mode);
        metadata.put("projectPath", projectPath);
        if (basePackage != null && !basePackage.isBlank()) {
            String bp = basePackage.trim().replaceAll("\\s+", "").replaceAll("[.*]+$", "");
            if (!bp.isEmpty()) metadata.put("basePackage", bp);
        }

        // Save optional per-JAR domain config to temp file alongside the JAR
        if (configFile != null && !configFile.isEmpty()) {
            Path tempConfig = Files.createTempFile("jar-domain-config-", ".json");
            configFile.transferTo(tempConfig);
            metadata.put("perJarConfigPath", tempConfig.toString());
            log.info("Per-JAR domain config attached: {} bytes", configFile.getSize());
        }

        QueueJob job = queueService.submit(QueueJob.Type.JAR_UPLOAD, "Upload: " + jarName, metadata);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "queued");
        response.put("jobId", job.id);
        response.put("jarName", jarName);
        response.put("jarSize", file.getSize());
        response.put("mode", mode);
        response.put("hasPerJarConfig", configFile != null && !configFile.isEmpty());
        return response;
    }

    /**
     * Upload a per-JAR domain config for an already-analyzed JAR.
     * The config is stored and will be used on the next re-analysis.
     */
    @PostMapping("/{id}/domain-config")
    public Map<String, Object> uploadDomainConfig(@PathVariable String id,
                                                   @RequestParam("configFile") MultipartFile configFile) throws IOException {
        if (configFile == null || configFile.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "configFile is required");
        }

        Path tempConfig = Files.createTempFile("jar-domain-config-", ".json");
        try {
            configFile.transferTo(tempConfig);
            // Validate it's valid JSON
            new com.fasterxml.jackson.databind.ObjectMapper().readValue(tempConfig.toFile(), java.util.Map.class);
            persistenceService.storeDomainConfig(tempConfig, id);
        } finally {
            Files.deleteIfExists(tempConfig);
        }

        log.info("Per-JAR domain config stored for {}: {} bytes", id, configFile.getSize());
        return Map.of(
                "status", "stored",
                "jarName", id,
                "configSize", configFile.getSize(),
                "message", "Domain config stored. Will be applied on next re-analysis."
        );
    }

    /** Get the per-JAR domain config for a JAR if one exists. */
    @GetMapping("/{id}/domain-config")
    public ResponseEntity<String> getDomainConfig(@PathVariable String id) throws IOException {
        Path configPath = persistenceService.getDomainConfigPath(id);
        if (configPath == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "No per-JAR domain config found for: " + id);
        }
        String content = Files.readString(configPath);
        return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(content);
    }

    @PostMapping("/analyze-local")
    public Map<String, Object> analyzeLocal(@RequestBody Map<String, String> body) throws IOException {
        String localPath = body.get("path");
        String mode = body.getOrDefault("mode", "static");
        String renameSuffix = body.get("renameSuffix");

        if (localPath == null || localPath.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "path is required");
        }

        Path source = Path.of(localPath.trim());
        if (!Files.isRegularFile(source) || !source.toString().endsWith(".jar")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "File not found or not a .jar: " + localPath);
        }

        String jarName = source.getFileName().toString();
        if (renameSuffix != null && !renameSuffix.isBlank()) {
            String base = jarName.substring(0, jarName.length() - 4);
            jarName = base + "_" + renameSuffix + ".jar";
        } else {
            String id = jarName.endsWith(".jar") ? jarName.substring(0, jarName.length() - 4) : jarName;
            boolean exists = persistenceService.listJars().stream()
                    .anyMatch(j -> id.equals(j.get("id")));
            if (exists) {
                String ts = java.time.LocalDateTime.now()
                        .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
                String base = jarName.substring(0, jarName.length() - 4);
                jarName = base + "_" + ts + ".jar";
            }
        }

        long fileSize = Files.size(source);
        log.info("Local JAR analysis: {} ({} MB), mode={}", jarName, fileSize / (1024 * 1024), mode);

        Path tempJar = Files.createTempFile("jar-analyze-", ".jar");
        Files.copy(source, tempJar, java.nio.file.StandardCopyOption.REPLACE_EXISTING);

        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("jarName", jarName);
        metadata.put("tempFilePath", tempJar.toString());
        metadata.put("fileSize", fileSize);
        metadata.put("mode", mode);
        metadata.put("projectPath", body.get("projectPath"));

        QueueJob job = queueService.submit(QueueJob.Type.JAR_UPLOAD, "Upload: " + jarName, metadata);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "queued");
        response.put("jobId", job.id);
        response.put("jarName", jarName);
        response.put("jarSize", fileSize);
        response.put("mode", mode);
        return response;
    }

    @GetMapping
    public List<Map<String, Object>> listJars() {
        List<Map<String, Object>> jars = persistenceService.listJars();
        // Enrich each JAR with Claude session status
        for (Map<String, Object> jar : jars) {
            String jarName = (String) jar.get("jarName");
            if (jarName != null) {
                Map<String, Object> progress = claudeTracker.getStatus(jarName);
                jar.put("claudeStatus", progress.get("status"));
                if (progress.containsKey("completedEndpoints")) {
                    jar.put("claudeCompleted", progress.get("completedEndpoints"));
                    jar.put("claudeTotal", progress.get("totalEndpoints"));
                }
            }
        }
        return jars;
    }

    @GetMapping("/{id}")
    public void getAnalysis(@PathVariable String id,
                            @RequestParam(value = "version", required = false) String version,
                            jakarta.servlet.http.HttpServletResponse response) throws IOException {
        AnalysisDataProvider provider = resolveProvider(id, version);
        response.setContentType("application/json");
        response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
        try (var parser = objectMapper.getFactory().createParser(provider.getFilePath().toFile());
             var gen = objectMapper.getFactory().createGenerator(response.getOutputStream())) {
            provider.streamAnalysis(parser, gen);
            gen.flush();
        }
    }

    @GetMapping("/{id}/endpoints/by-index/{idx}/call-tree")
    public void getEndpointCallTree(@PathVariable String id,
                                    @PathVariable int idx,
                                    @RequestParam(value = "version", required = false) String version,
                                    jakarta.servlet.http.HttpServletResponse response) throws IOException {
        AnalysisDataProvider provider = resolveProvider(id, version);
        response.setContentType("application/json");
        response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
        try (var parser = objectMapper.getFactory().createParser(provider.getFilePath().toFile());
             var gen = objectMapper.getFactory().createGenerator(response.getOutputStream())) {
            provider.streamCallTree(parser, gen, idx);
            gen.flush();
        }
    }

    // ==================== Lazy-Loading Endpoints ====================

    @GetMapping("/{id}/summary")
    public void getSummary(@PathVariable String id,
                           @RequestParam(value = "version", required = false) String version,
                           jakarta.servlet.http.HttpServletRequest request,
                           jakarta.servlet.http.HttpServletResponse response) throws IOException {
        AnalysisDataProvider provider = resolveProvider(id, version);
        Path filePath = provider.getFilePath();
        response.setContentType("application/json");
        response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");

        Path summaryCache = provider.getSummaryCachePath();
        Path gzCache = Path.of(summaryCache + ".gz");

        // Serve from pre-gzipped cache if client accepts gzip
        if (version == null && Files.exists(gzCache)) {
            try {
                if (Files.getLastModifiedTime(gzCache).compareTo(
                        Files.getLastModifiedTime(filePath)) >= 0) {
                    String ae = request.getHeader("Accept-Encoding");
                    if (ae != null && ae.contains("gzip")) {
                        response.setHeader("Content-Encoding", "gzip");
                        response.setContentLengthLong(Files.size(gzCache));
                        Files.copy(gzCache, response.getOutputStream());
                        return;
                    }
                    if (Files.exists(summaryCache)) {
                        Files.copy(summaryCache, response.getOutputStream());
                        return;
                    }
                }
            } catch (IOException e) {
                log.debug("Summary cache read failed, regenerating: {}", e.getMessage());
            }
        }

        if (version == null) {
            try (var parser = objectMapper.getFactory().createParser(filePath.toFile())) {
                java.io.ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream(8 * 1024 * 1024);
                try (var gen = objectMapper.getFactory().createGenerator(buf)) {
                    provider.streamSummary(parser, gen);
                    gen.flush();
                }
                byte[] data = buf.toByteArray();
                try {
                    Files.write(summaryCache, data);
                    try (var gzOut = new java.util.zip.GZIPOutputStream(
                            new java.io.BufferedOutputStream(Files.newOutputStream(gzCache), 64 * 1024))) {
                        gzOut.write(data);
                    }
                } catch (IOException e) { log.debug("Failed to write summary cache: {}", e.getMessage()); }
                response.getOutputStream().write(data);
                response.getOutputStream().flush();
            }
        } else {
            try (var parser = objectMapper.getFactory().createParser(filePath.toFile());
                 var gen = objectMapper.getFactory().createGenerator(response.getOutputStream())) {
                provider.streamSummary(parser, gen);
                gen.flush();
            }
        }
    }

    @GetMapping("/{id}/summary/headers")
    public void getSummaryHeaders(@PathVariable String id,
                                  jakarta.servlet.http.HttpServletRequest request,
                                  jakarta.servlet.http.HttpServletResponse response) throws IOException {
        serveSummarySlice(id, "headers", null, request, response);
    }

    @GetMapping("/{id}/summary/external-calls")
    public void getSummaryExternalCalls(@PathVariable String id,
                                        jakarta.servlet.http.HttpServletRequest request,
                                        jakarta.servlet.http.HttpServletResponse response) throws IOException {
        serveSummarySlice(id, "slice", java.util.Set.of("externalCalls", "httpCalls"), request, response);
    }

    @GetMapping("/{id}/summary/dynamic-flows")
    public void getSummaryDynamicFlows(@PathVariable String id,
                                       jakarta.servlet.http.HttpServletRequest request,
                                       jakarta.servlet.http.HttpServletResponse response) throws IOException {
        serveSummarySlice(id, "slice", java.util.Set.of("dynamicFlows"), request, response);
    }

    @GetMapping("/{id}/summary/aggregation-flows")
    public void getSummaryAggregationFlows(@PathVariable String id,
                                            jakarta.servlet.http.HttpServletRequest request,
                                            jakarta.servlet.http.HttpServletResponse response) throws IOException {
        serveSummarySlice(id, "slice", java.util.Set.of("aggregationFlows"), request, response);
    }

    @GetMapping("/{id}/summary/beans")
    public void getSummaryBeans(@PathVariable String id,
                                jakarta.servlet.http.HttpServletRequest request,
                                jakarta.servlet.http.HttpServletResponse response) throws IOException {
        serveSummarySlice(id, "slice", java.util.Set.of("beans"), request, response);
    }

    private void serveSummarySlice(String id, String mode, java.util.Set<String> fields,
                                    jakarta.servlet.http.HttpServletRequest request,
                                    jakarta.servlet.http.HttpServletResponse response) throws IOException {
        AnalysisDataProvider provider = resolveProvider(id, null);
        response.setContentType("application/json");
        response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");

        // Ensure summary cache exists and is valid JSON (delete corrupt/partial files from prior crashes)
        Path summaryCache = provider.getSummaryCachePath();
        if (java.nio.file.Files.exists(summaryCache) && !isSummaryCacheValid(summaryCache)) {
            log.warn("Summary cache {} appears corrupt (partial write from crash) — deleting and regenerating", summaryCache.getFileName());
            java.nio.file.Files.deleteIfExists(summaryCache);
            java.nio.file.Files.deleteIfExists(java.nio.file.Path.of(summaryCache + ".gz"));
        }
        if (!java.nio.file.Files.exists(summaryCache)) {
            // Stream directly to disk — avoids loading the full summary into a ByteArrayOutputStream,
            // which causes OOM for large JARs with deep call trees.
            Path tempCache = java.nio.file.Path.of(summaryCache + ".tmp");
            try (var parser = objectMapper.getFactory().createParser(provider.getFilePath().toFile());
                 var out = new java.io.BufferedOutputStream(java.nio.file.Files.newOutputStream(tempCache), 128 * 1024);
                 var gen = objectMapper.getFactory().createGenerator(out)) {
                provider.streamSummary(parser, gen);
                gen.flush();
            }
            try {
                java.nio.file.Files.move(tempCache, summaryCache,
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                        java.nio.file.StandardCopyOption.ATOMIC_MOVE);
                // Async gzip — don't block the response on compression
                Path gzCache = java.nio.file.Path.of(summaryCache + ".gz");
                java.util.concurrent.CompletableFuture.runAsync(() -> {
                    try (var in = new java.io.BufferedInputStream(java.nio.file.Files.newInputStream(summaryCache), 128 * 1024);
                         var gzOut = new java.util.zip.GZIPOutputStream(
                                 new java.io.BufferedOutputStream(java.nio.file.Files.newOutputStream(gzCache), 64 * 1024))) {
                        in.transferTo(gzOut);
                    } catch (IOException ignored) {}
                });
            } catch (IOException e) {
                log.debug("Failed to finalize summary cache: {}", e.getMessage());
                java.nio.file.Files.deleteIfExists(tempCache);
            }
        }

        // Read from cached summary and extract the slice
        try (var parser = objectMapper.getFactory().createParser(summaryCache.toFile());
             var gen = objectMapper.getFactory().createGenerator(response.getOutputStream())) {
            provider.streamSummarySlice(parser, gen, mode, fields);
            gen.flush();
        }
    }

    @GetMapping("/{id}/classes/tree")
    public void getClassTree(@PathVariable String id,
                             @RequestParam(value = "version", required = false) String version,
                             jakarta.servlet.http.HttpServletResponse response) throws IOException {
        AnalysisDataProvider provider = resolveProvider(id, version);
        response.setContentType("application/json");
        response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
        try (var parser = objectMapper.getFactory().createParser(provider.getFilePath().toFile());
             var gen = objectMapper.getFactory().createGenerator(response.getOutputStream())) {
            provider.streamClassTree(parser, gen);
            gen.flush();
        }
    }

    @GetMapping("/{id}/classes/by-index/{idx}")
    public void getClassByIndex(@PathVariable String id,
                                @PathVariable int idx,
                                @RequestParam(value = "version", required = false) String version,
                                jakarta.servlet.http.HttpServletResponse response) throws IOException {
        AnalysisDataProvider provider = resolveProvider(id, version);
        response.setContentType("application/json");
        response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
        try (var parser = objectMapper.getFactory().createParser(provider.getFilePath().toFile());
             var gen = objectMapper.getFactory().createGenerator(response.getOutputStream())) {
            provider.streamClassByIndex(parser, gen, idx);
            gen.flush();
        }
    }

    /** Quick validation: a summary cache is valid if it ends with '}' (complete JSON object). */
    private boolean isSummaryCacheValid(Path cache) {
        try {
            long size = java.nio.file.Files.size(cache);
            if (size < 2) return false;
            try (var raf = new java.io.RandomAccessFile(cache.toFile(), "r")) {
                raf.seek(size - 1);
                return raf.read() == '}';
            }
        } catch (IOException e) {
            return false;
        }
    }

    private AnalysisDataProvider resolveProvider(String id, String version) {
        try {
            return providerFactory.resolve(id, version);
        } catch (AnalysisNotFoundException e) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, e.getMessage());
        }
    }

    // Streaming methods (streamCallTree, streamSummary, streamClassTree, etc.)
    // are now in AbstractAnalysisDataProvider / StaticAnalysisProvider / ClaudeAnalysisProvider



    @PostMapping("/export-excel")
    public ResponseEntity<byte[]> exportExcel(@RequestBody Map<String, Object> reportData) throws IOException {
        byte[] xlsx = excelExportService.generateReport(reportData);
        String jarName = String.valueOf(reportData.getOrDefault("jarName", "analysis"));
        String fileName = jarName.replace(".jar", "") + "_report.xlsx";

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + "\"")
                .contentType(MediaType.parseMediaType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"))
                .body(xlsx);
    }

    @PostMapping("/{id}/claude-enrich-single")
    public Map<String, Object> claudeEnrichSingle(@PathVariable String id,
                                                   @RequestBody Map<String, String> body) {
        String endpointName = body.get("endpointName");
        if (endpointName == null || endpointName.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "endpointName is required");
        }
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("jarName", id);
        meta.put("endpointName", endpointName);
        QueueJob job = queueService.submit(QueueJob.Type.CLAUDE_ENRICH_SINGLE,
                "Enrich: " + endpointName + " (" + id + ")", meta);
        return Map.of("status", "queued", "jobId", job.id, "endpoint", endpointName, "jarName", id);
    }

    @PostMapping("/{id}/claude-rescan")
    public Map<String, Object> claudeRescan(@PathVariable String id,
                                             @RequestParam(value = "resume", defaultValue = "true") boolean resume) {
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("jarName", id);
        meta.put("resume", resume);
        QueueJob job = queueService.submit(QueueJob.Type.CLAUDE_RESCAN,
                "Rescan: " + id + (resume ? " (resume)" : ""), meta);
        return Map.of("status", "queued", "jobId", job.id, "jarName", id);
    }

    /**
     * Manually fetch MongoDB catalog for a JAR from an explicit URI or re-extract from stored JAR.
     * Catalog is stored to disk and used for collection verification on next analysis.
     */
    @PostMapping("/{id}/fetch-catalog")
    public Map<String, Object> fetchCatalog(@PathVariable String id,
                                             @RequestBody(required = false) Map<String, String> body) {
        String mongoUri = (body != null) ? body.get("mongoUri") : null;

        java.util.Set<String> catalog;
        if (mongoUri != null && !mongoUri.isBlank()) {
            // Explicit URI provided
            catalog = mongoCatalogService.fetchAndStore(id, mongoUri);
        } else {
            // Re-extract from stored JAR
            Path jarPath = persistenceService.getJarFilePath(id);
            if (jarPath == null) {
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Stored JAR not found: " + id);
            }
            try {
                Map<String, String> configs = parserService.extractConfigFiles(jarPath.toFile());
                catalog = mongoCatalogService.fetchAndStore(id, configs);
            } catch (IOException e) {
                throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
                        "Failed to extract config: " + e.getMessage());
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("jarName", id);
        if (catalog != null) {
            result.put("status", "fetched");
            result.put("totalEntries", catalog.size());
            MongoCatalogService.MongoCatalog full = mongoCatalogService.loadCatalog(id);
            if (full != null) {
                result.put("database", full.database());
                result.put("collections", full.totalCollections());
                result.put("views", full.totalViews());
            }
        } else {
            result.put("status", "unavailable");
            result.put("message", "Could not fetch catalog — check MongoDB URI and connectivity");
        }
        return result;
    }

    /** Get the current MongoDB catalog for a JAR (if available). */
    @GetMapping("/{id}/catalog")
    public Map<String, Object> getCatalog(@PathVariable String id) {
        MongoCatalogService.MongoCatalog catalog = mongoCatalogService.loadCatalog(id);
        if (catalog == null) {
            return Map.of("jarName", id, "status", "none",
                    "message", "No catalog available. Use POST /{id}/fetch-catalog to fetch one.");
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("jarName", id);
        result.put("status", "available");
        result.put("database", catalog.database());
        result.put("maskedUri", catalog.maskedUri());
        result.put("collections", catalog.collections());
        result.put("views", catalog.views());
        result.put("totalCollections", catalog.totalCollections());
        result.put("totalViews", catalog.totalViews());
        result.put("fetchedAt", catalog.fetchedAt());
        return result;
    }

    @DeleteMapping("/{id}")
    public Map<String, Object> deleteAnalysis(@PathVariable String id) throws IOException {
        // 1. Kill any running Claude sessions for this JAR (normalizes internally)
        int killed = sessionManager.killSessionsForJar(id);

        // 2. Remove enrichment tracker state (normalizes internally)
        claudeTracker.removeTracking(id);

        // 2.5. Delete MongoDB catalog and correction data (normalizes internally)
        mongoCatalogService.deleteCatalog(id);
        correctionPersistence.deleteAll(id);

        // 3. Delete all files: analysis JSON, stored JAR, endpoint outputs, Claude fragments, corrections
        boolean deleted = persistenceService.delete(id);
        if (!deleted) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "JAR analysis not found: " + id);
        }

        log.info("Deleted JAR {}: {} Claude sessions killed", id, killed);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "deleted");
        result.put("id", id);
        result.put("sessionsKilled", killed);
        return result;
    }

    @PostMapping("/reanalyze-all")
    public Map<String, Object> reanalyzeAll() throws IOException {
        List<Map<String, Object>> jars = persistenceService.listJars();
        List<Map<String, Object>> queued = new ArrayList<>();

        for (Map<String, Object> jar : jars) {
            String jarName = (String) jar.get("jarName");
            if (jarName == null) continue;
            String id = jarName.replace(".jar", "");

            Path storedJar = persistenceService.getJarFilePath(id);
            if (storedJar == null || !Files.exists(storedJar)) continue;

            long fileSize = Files.size(storedJar);
            Path tempJar = Files.createTempFile("jar-reanalyze-", ".jar");
            Files.copy(storedJar, tempJar, java.nio.file.StandardCopyOption.REPLACE_EXISTING);

            sessionManager.killSessionsForJar(id);
            claudeTracker.removeTracking(id);
            mongoCatalogService.deleteCatalog(id);
            correctionPersistence.deleteAll(id);
            persistenceService.delete(id);

            Map<String, Object> metadata = new LinkedHashMap<>();
            metadata.put("jarName", jarName);
            metadata.put("tempFilePath", tempJar.toString());
            metadata.put("fileSize", fileSize);
            metadata.put("mode", "static");

            QueueJob job = queueService.submit(QueueJob.Type.JAR_UPLOAD, "Re-analyze: " + jarName, metadata);
            queued.add(Map.of("jarName", jarName, "jobId", job.id, "jarSize", fileSize));
        }

        log.info("Re-analyze all: queued {} JARs for fresh analysis", queued.size());
        return Map.of("status", "queued", "count", queued.size(), "jobs", queued);
    }

    @PostMapping("/{id}/reanalyze")
    public Map<String, Object> reanalyze(@PathVariable String id) throws IOException {
        Path storedJar = persistenceService.getJarFilePath(id);
        if (storedJar == null || !Files.exists(storedJar)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Stored JAR not found for: " + id);
        }

        long fileSize = Files.size(storedJar);
        String jarName = id.endsWith(".jar") ? id : id + ".jar";

        Path tempJar = Files.createTempFile("jar-reanalyze-", ".jar");
        Files.copy(storedJar, tempJar, java.nio.file.StandardCopyOption.REPLACE_EXISTING);

        sessionManager.killSessionsForJar(id);
        claudeTracker.removeTracking(id);
        mongoCatalogService.deleteCatalog(id);
        correctionPersistence.deleteAll(id);
        persistenceService.delete(id);

        log.info("Re-analyze: cleaned old data for {}, queueing fresh analysis ({} MB)", id, fileSize / (1024 * 1024));

        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("jarName", jarName);
        metadata.put("tempFilePath", tempJar.toString());
        metadata.put("fileSize", fileSize);
        metadata.put("mode", "static");

        QueueJob job = queueService.submit(QueueJob.Type.JAR_UPLOAD, "Re-analyze: " + jarName, metadata);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "queued");
        response.put("jobId", job.id);
        response.put("jarName", jarName);
        response.put("jarSize", fileSize);
        return response;
    }

    /* ---- Version management endpoints ---- */

    @GetMapping("/{id}/versions")
    public Map<String, Object> getVersions(@PathVariable String id) {
        Map<String, Object> info = persistenceService.getVersionInfo(id);
        info.put("jarName", id);
        return info;
    }

    @GetMapping("/{id}/collections")
    public List<Map<String, Object>> getCollectionReport(@PathVariable String id) throws IOException {
        JarAnalysis analysis = persistenceService.load(id);
        if (analysis == null) return List.of();

        // Aggregate: collection -> {endpoints, operations, domain, sources}
        Map<String, Map<String, Object>> collMap = new LinkedHashMap<>();

        for (EndpointInfo ep : analysis.getEndpoints()) {
            if (ep.getAggregatedCollections() == null || ep.getAggregatedCollections().isEmpty()) {
                // Fallback: compute if not yet populated (old data without aggregation)
                ep.computeAggregates();
            }
            String epLabel = ep.getControllerSimpleName() + "." + ep.getMethodName();
            String epPath = (ep.getHttpMethod() != null ? ep.getHttpMethod() + " " : "") + (ep.getFullPath() != null ? ep.getFullPath() : "");

            for (String coll : ep.getAggregatedCollections()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> entry = collMap.computeIfAbsent(coll, c -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("collection", c);
                    m.put("domain", ep.getAggregatedCollectionDomains() != null ? ep.getAggregatedCollectionDomains().getOrDefault(c, "Other") : "Other");
                    m.put("endpoints", new ArrayList<Map<String, String>>());
                    m.put("operations", new LinkedHashSet<String>());
                    m.put("endpointCount", 0);
                    return m;
                });
                @SuppressWarnings("unchecked")
                List<Map<String, String>> endpoints = (List<Map<String, String>>) entry.get("endpoints");
                String op = ep.getAggregatedOperations() != null ? ep.getAggregatedOperations().getOrDefault(coll, null) : null;
                endpoints.add(Map.of("label", epLabel, "path", epPath, "operation", op != null ? op : ""));
                @SuppressWarnings("unchecked")
                Set<String> ops = (Set<String>) entry.get("operations");
                if (op != null) ops.add(op);
                entry.put("endpointCount", endpoints.size());
            }
        }

        // Convert to list sorted by endpoint count desc
        return collMap.values().stream()
                .sorted((a, b) -> (int) b.get("endpointCount") - (int) a.get("endpointCount"))
                .peek(m -> m.put("operations", new ArrayList<>((Set<?>) m.get("operations"))))
                .toList();
    }

    @PostMapping("/{id}/revert-claude")
    public Map<String, Object> revertClaude(@PathVariable String id) throws IOException {
        persistenceService.revertCorrected(id);
        return Map.of("status", "reverted", "jarName", id);
    }

    /* ---- Full Claude scan (correct + merge + replace) ---- */

    /**
     * Full Claude correction scan: generates corrections, backs up static data,
     * merges corrections into analysis, and saves as the new analysis.
     * Uses ClaudeEnrichmentTracker for progress polling by the frontend.
     */
    @PostMapping("/{id}/claude-full-scan")
    public Map<String, Object> claudeFullScan(@PathVariable String id,
                                               @RequestParam(value = "resume", defaultValue = "true") boolean resume) {
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("jarName", id);
        meta.put("resume", resume);
        QueueJob job = queueService.submit(QueueJob.Type.CLAUDE_FULL_SCAN,
                "Full scan: " + id + (resume ? " (resume)" : ""), meta);
        return Map.of("status", "queued", "jobId", job.id, "jarName", id, "resume", resume);
    }

    /* ---- Claude correction endpoints ---- */

    @PostMapping("/{id}/claude-correct")
    public Map<String, Object> claudeCorrect(@PathVariable String id,
                                              @RequestParam(value = "resume", defaultValue = "true") boolean resume) {
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("jarName", id);
        meta.put("resume", resume);
        QueueJob job = queueService.submit(QueueJob.Type.CLAUDE_CORRECT,
                "Correct: " + id + (resume ? " (resume)" : ""), meta);
        return Map.of("status", "queued", "jobId", job.id, "jarName", id, "resume", resume);
    }

    @PostMapping("/{id}/claude-correct-single")
    public Map<String, Object> claudeCorrectSingle(@PathVariable String id,
                                                     @RequestBody Map<String, String> body) {
        String endpointName = body.get("endpointName");
        if (endpointName == null || endpointName.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "endpointName is required");
        }
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("jarName", id);
        meta.put("endpointName", endpointName);
        QueueJob job = queueService.submit(QueueJob.Type.CLAUDE_CORRECT_SINGLE,
                "Correct: " + endpointName + " (" + id + ")", meta);
        return Map.of("status", "queued", "jobId", job.id, "endpoint", endpointName, "jarName", id);
    }

    /** Get connection info discovered during analysis (MongoDB URI, Oracle JDBC URL). */
    @GetMapping("/{id}/connections")
    public ResponseEntity<?> getConnections(@PathVariable String id) {
        Map<String, Object> info = persistenceService.loadConnections(id);
        if (info == null) return ResponseEntity.ok(Map.of("available", false));
        info.put("available", true);
        return ResponseEntity.ok(info);
    }

    /** Get all correction data for a JAR. */
    @GetMapping("/{id}/corrections")
    public Map<String, Object> getCorrections(@PathVariable String id) {
        Map<String, CorrectionResult> corrections = correctionPersistence.loadAllCorrections(id);
        Map<String, Object> summary = correctionPersistence.loadSummary(id);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("jarName", id);
        result.put("totalCorrections", corrections.size());
        result.put("corrections", corrections);
        if (summary != null) result.put("summary", summary);
        return result;
    }

    /** Get correction for a single endpoint. */
    @GetMapping("/{id}/corrections/{endpoint}")
    public Map<String, Object> getCorrectionForEndpoint(@PathVariable String id,
                                                         @PathVariable String endpoint) {
        CorrectionResult correction = correctionPersistence.loadCorrection(id, endpoint);
        if (correction == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND,
                    "No correction data for endpoint: " + endpoint);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("jarName", id);
        result.put("endpoint", endpoint);
        result.put("correction", correction);
        return result;
    }

    /* ---- Correction prompt/response logs per endpoint ---- */

    /** List correction log files (prompt inputs + response outputs) for a specific endpoint. */
    @GetMapping("/{id}/correction-logs/{endpointName}")
    public List<Map<String, Object>> getCorrectionLogs(@PathVariable String id, @PathVariable String endpointName) {
        Path dir = correctionPersistence.getWorkDir(id);
        List<Map<String, Object>> logs = new ArrayList<>();
        if (!Files.isDirectory(dir)) return logs;

        // Endpoint method name is the suffix after the last dot
        String method = endpointName.contains(".") ? endpointName.substring(endpointName.lastIndexOf('.') + 1) : endpointName;

        try (var stream = Files.list(dir)) {
            stream.filter(p -> {
                String name = p.getFileName().toString();
                return name.startsWith(method + "_corr") && (name.endsWith("_input.txt") || name.endsWith("_output.json"));
            }).sorted().forEach(p -> {
                try {
                    String name = p.getFileName().toString();
                    String content = Files.readString(p);
                    String type = name.endsWith("_input.txt") ? "prompt" : "response";
                    // Determine chunk info from filename
                    String label;
                    if (name.contains("_chunk")) {
                        String chunkNum = name.replaceAll(".*_chunk(\\d+)_.*", "$1");
                        label = "Chunk " + (Integer.parseInt(chunkNum) + 1) + " " + type;
                    } else {
                        label = "Single-shot " + type;
                    }
                    logs.add(Map.of(
                            "name", name,
                            "label", label,
                            "type", type,
                            "size", content.length(),
                            "content", content
                    ));
                } catch (IOException ignored) {}
            });
        } catch (IOException e) { /* empty list */ }
        return logs;
    }

    /** List ALL correction log files (metadata only, no content) for the browse-all viewer. */
    @GetMapping("/{id}/correction-logs")
    public List<Map<String, Object>> listAllCorrectionLogs(@PathVariable String id) {
        Path dir = correctionPersistence.getWorkDir(id);
        List<Map<String, Object>> logs = new ArrayList<>();
        if (!Files.isDirectory(dir)) return logs;

        try (var stream = Files.list(dir)) {
            stream.filter(p -> {
                String name = p.getFileName().toString();
                return name.contains("_corr") && (name.endsWith("_input.txt") || name.endsWith("_output.json"));
            }).sorted().forEach(p -> {
                try {
                    String name = p.getFileName().toString();
                    long size = Files.size(p);
                    String type = name.endsWith("_input.txt") ? "prompt" : "response";
                    // Extract endpoint method name (everything before _corr)
                    String endpoint = name.contains("_corr") ? name.substring(0, name.indexOf("_corr")) : name;
                    // Build label
                    String label;
                    if (name.contains("_chunk")) {
                        String chunkNum = name.replaceAll(".*_chunk(\\d+)_.*", "$1");
                        label = endpoint + " - Chunk " + (Integer.parseInt(chunkNum) + 1) + " " + type;
                    } else {
                        label = endpoint + " - " + type;
                    }
                    logs.add(Map.of(
                            "name", name,
                            "label", label,
                            "type", type,
                            "endpoint", endpoint,
                            "size", size
                    ));
                } catch (IOException ignored) {}
            });
        } catch (IOException e) { /* empty list */ }
        return logs;
    }

    /** Fetch content of a single correction log file by name. */
    @GetMapping("/{id}/correction-logs/file/{fileName}")
    public ResponseEntity<String> getCorrectionLogFile(@PathVariable String id, @PathVariable String fileName) {
        Path dir = correctionPersistence.getWorkDir(id);
        Path file = dir.resolve(fileName);
        if (!Files.isRegularFile(file) || !file.startsWith(dir)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Log file not found: " + fileName);
        }
        try {
            String content = Files.readString(file);
            return ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body(content);
        } catch (IOException e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to read: " + e.getMessage());
        }
    }

    /* ---- Claude run logs ---- */

    /** List available run log files for a JAR. */
    @GetMapping("/{id}/run-logs")
    public List<Map<String, Object>> listRunLogs(@PathVariable String id) {
        Path logDir = correctionPersistence.getWorkDir(id);
        List<Map<String, Object>> logs = new ArrayList<>();
        if (!Files.isDirectory(logDir)) return logs;
        try (var stream = Files.list(logDir)) {
            stream.filter(p -> p.getFileName().toString().startsWith("run_") && p.getFileName().toString().endsWith(".log"))
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            logs.add(Map.of(
                                    "name", p.getFileName().toString(),
                                    "size", Files.size(p),
                                    "modified", Files.getLastModifiedTime(p).toString()
                            ));
                        } catch (IOException ignored) {}
                    });
        } catch (IOException e) { /* empty list */ }
        return logs;
    }

    /** Fetch content of a specific run log. */
    @GetMapping("/{id}/run-logs/{logName}")
    public String getRunLog(@PathVariable String id, @PathVariable String logName) {
        if (!logName.startsWith("run_") || !logName.endsWith(".log")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid log name");
        }
        Path logFile = correctionPersistence.getWorkDir(id).resolve(logName);
        if (!Files.isRegularFile(logFile)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Log not found: " + logName);
        }
        try { return Files.readString(logFile); }
        catch (IOException e) { throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to read log"); }
    }

    /* ---- Claude session management ---- */

    /** Kill all running Claude sessions for a specific JAR (keeps session history). */
    @PostMapping("/{id}/kill-sessions")
    public Map<String, Object> killJarSessions(@PathVariable String id) {
        int killed = sessionManager.killRunningForJar(id);
        if (killed > 0) {
            // Tracker normalizes keys internally — single call suffices
            claudeTracker.markFailed(id, "Cancelled by user (" + killed + " session(s) killed)");
        }
        return Map.of("status", "done", "sessionsKilled", killed, "jarName", id);
    }

    @GetMapping("/sessions")
    public List<Map<String, Object>> listSessions() {
        return sessionManager.listSessions();
    }

    @PostMapping("/sessions/{sessionId}/kill")
    public Map<String, Object> killSession(@PathVariable String sessionId) {
        boolean killed = sessionManager.kill(sessionId);
        if (!killed) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND,
                    "Session not found or not running: " + sessionId);
        }
        return Map.of("status", "killed", "sessionId", sessionId);
    }

    /* ---- Claude API call tracking ---- */

    /** Get aggregated Claude CLI call stats for a JAR. */
    @GetMapping("/{name}/claude-stats")
    public Map<String, Object> getClaudeStats(@PathVariable String name) {
        return claudeCallLogger.getStats(name);
    }

    /** Get all call entries for a specific Claude session within a JAR. */
    @GetMapping("/{name}/claude-stats/{sessionId}")
    public List<Map<String, Object>> getClaudeSessionDetail(@PathVariable String name,
                                                             @PathVariable String sessionId) {
        return claudeCallLogger.getSessionDetail(name, sessionId);
    }
}
