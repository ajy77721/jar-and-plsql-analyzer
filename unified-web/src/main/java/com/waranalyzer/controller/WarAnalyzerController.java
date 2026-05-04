package com.waranalyzer.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.analyzer.queue.AnalysisQueueService;
import com.analyzer.queue.QueueJob;
import com.jaranalyzer.model.CorrectionResult;
import com.jaranalyzer.model.EndpointInfo;
import com.jaranalyzer.model.JarAnalysis;
import com.jaranalyzer.service.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

@RestController("warAnalyzerController")
@RequestMapping("/api/war/wars")
public class WarAnalyzerController {

    private static final Logger log = LoggerFactory.getLogger(WarAnalyzerController.class);

    private final WarParserService warParserService;
    private final CallGraphService callGraphService;
    private final PersistenceService warPersistenceService;
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
    private final ClaudeCallLogger claudeCallLogger;
    private final WarDataPaths warDataPaths;

    public WarAnalyzerController(WarParserService warParserService,
                                 CallGraphService callGraphService,
                                 @Qualifier("warPersistenceService") PersistenceService warPersistenceService,
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
                                 ClaudeCallLogger claudeCallLogger) {
        this.warParserService = warParserService;
        this.callGraphService = callGraphService;
        this.warPersistenceService = warPersistenceService;
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
        this.claudeCallLogger = claudeCallLogger;
        this.warDataPaths = (WarDataPaths) warPersistenceService.getPaths();
    }

    // ==================== Upload & Analyze ====================

    @PostMapping
    public Map<String, Object> uploadAndAnalyze(
            @RequestParam("file") MultipartFile file,
            @RequestParam(value = "mode", defaultValue = "static") String mode,
            @RequestParam(value = "projectPath", required = false) String projectPath,
            @RequestParam(value = "renameSuffix", required = false) String renameSuffix,
            @RequestParam(value = "basePackage", required = false) String basePackage) throws IOException {

        String warName = file.getOriginalFilename();
        if (warName == null || !warName.endsWith(".war")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "File must be a .war");
        }

        if (renameSuffix != null && !renameSuffix.isBlank()) {
            String base = warName.substring(0, warName.length() - 4);
            warName = base + "_" + renameSuffix + ".war";
        }

        log.info("WAR upload: {} ({} MB), mode={}", warName, file.getSize() / (1024 * 1024), mode);

        Path tempWar = Files.createTempFile("war-analyze-", ".war");
        file.transferTo(tempWar);

        Map<String, Object> metadata = buildMetadata(warName, tempWar, file.getSize(), mode, projectPath);
        if (basePackage != null && !basePackage.isBlank()) {
            String bp = basePackage.trim().replaceAll("\\s+", "").replaceAll("[.*]+$", "");
            if (!bp.isEmpty()) metadata.put("basePackage", bp);
        }
        QueueJob job = queueService.submit(QueueJob.Type.WAR_UPLOAD, "Upload: " + warName, metadata);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "queued");
        response.put("jobId", job.id);
        response.put("warName", warName);
        response.put("warSize", file.getSize());
        response.put("mode", mode);
        return response;
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
        if (!Files.isRegularFile(source) || !source.toString().endsWith(".war")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "File not found or not a .war: " + localPath);
        }

        String warName = source.getFileName().toString();
        if (renameSuffix != null && !renameSuffix.isBlank()) {
            String base = warName.substring(0, warName.length() - 4);
            warName = base + "_" + renameSuffix + ".war";
        } else {
            String id = warName.endsWith(".war") ? warName.substring(0, warName.length() - 4) : warName;
            boolean exists = warPersistenceService.listJars().stream()
                    .anyMatch(j -> id.equals(j.get("id")));
            if (exists) {
                String ts = java.time.LocalDateTime.now()
                        .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
                String base = warName.substring(0, warName.length() - 4);
                warName = base + "_" + ts + ".war";
            }
        }

        long fileSize = Files.size(source);
        log.info("Local WAR analysis: {} ({} MB), mode={}", warName, fileSize / (1024 * 1024), mode);

        Path tempWar = Files.createTempFile("war-analyze-", ".war");
        Files.copy(source, tempWar, java.nio.file.StandardCopyOption.REPLACE_EXISTING);

        Map<String, Object> metadata = buildMetadata(warName, tempWar, fileSize, mode, body.get("projectPath"));
        QueueJob job = queueService.submit(QueueJob.Type.WAR_UPLOAD, "Upload: " + warName, metadata);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "queued");
        response.put("jobId", job.id);
        response.put("warName", warName);
        response.put("warSize", fileSize);
        response.put("mode", mode);
        return response;
    }

    private Map<String, Object> buildMetadata(String warName, Path tempWar, long size, String mode, String projectPath) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("warName", warName);
        m.put("tempFilePath", tempWar.toString());
        m.put("fileSize", size);
        m.put("mode", mode);
        m.put("projectPath", projectPath);
        return m;
    }

    // ==================== List & Get ====================

    @GetMapping
    public List<Map<String, Object>> listWars() {
        List<Map<String, Object>> wars = warPersistenceService.listJars();
        for (Map<String, Object> war : wars) {
            String warName = (String) war.get("jarName");
            if (warName != null) {
                Map<String, Object> progress = claudeTracker.getStatus(warName);
                war.put("claudeStatus", progress.get("status"));
                if (progress.containsKey("completedEndpoints")) {
                    war.put("claudeCompleted", progress.get("completedEndpoints"));
                    war.put("claudeTotal", progress.get("totalEndpoints"));
                }
            }
        }
        return wars;
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

    // ==================== Summary ====================

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

        Path summaryCache = provider.getSummaryCachePath();
        if (!Files.exists(summaryCache)) {
            try (var parser = objectMapper.getFactory().createParser(provider.getFilePath().toFile())) {
                java.io.ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream(8 * 1024 * 1024);
                try (var gen = objectMapper.getFactory().createGenerator(buf)) {
                    provider.streamSummary(parser, gen);
                    gen.flush();
                }
                byte[] data = buf.toByteArray();
                try {
                    Files.write(summaryCache, data);
                    Path gzCache = Path.of(summaryCache + ".gz");
                    try (var gzOut = new java.util.zip.GZIPOutputStream(
                            new java.io.BufferedOutputStream(Files.newOutputStream(gzCache), 64 * 1024))) {
                        gzOut.write(data);
                    }
                } catch (IOException e) {
                    log.debug("Failed to write summary cache for slice: {}", e.getMessage());
                }
            }
        }

        try (var parser = objectMapper.getFactory().createParser(summaryCache.toFile());
             var gen = objectMapper.getFactory().createGenerator(response.getOutputStream())) {
            provider.streamSummarySlice(parser, gen, mode, fields);
            gen.flush();
        }
    }

    // ==================== Class Tree ====================

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

    // ==================== Claude Operations ====================

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
        return Map.of("status", "queued", "jobId", job.id, "endpoint", endpointName, "warName", id);
    }

    @PostMapping("/{id}/claude-rescan")
    public Map<String, Object> claudeRescan(@PathVariable String id,
                                             @RequestParam(value = "resume", defaultValue = "true") boolean resume) {
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("jarName", id);
        meta.put("resume", resume);
        QueueJob job = queueService.submit(QueueJob.Type.CLAUDE_RESCAN,
                "Rescan: " + id + (resume ? " (resume)" : ""), meta);
        return Map.of("status", "queued", "jobId", job.id, "warName", id);
    }

    @PostMapping("/{id}/claude-full-scan")
    public Map<String, Object> claudeFullScan(@PathVariable String id,
                                               @RequestParam(value = "resume", defaultValue = "true") boolean resume) {
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("jarName", id);
        meta.put("resume", resume);
        QueueJob job = queueService.submit(QueueJob.Type.CLAUDE_FULL_SCAN,
                "Full scan: " + id + (resume ? " (resume)" : ""), meta);
        return Map.of("status", "queued", "jobId", job.id, "warName", id, "resume", resume);
    }

    @PostMapping("/{id}/claude-correct")
    public Map<String, Object> claudeCorrect(@PathVariable String id,
                                              @RequestParam(value = "resume", defaultValue = "true") boolean resume) {
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("jarName", id);
        meta.put("resume", resume);
        QueueJob job = queueService.submit(QueueJob.Type.CLAUDE_CORRECT,
                "Correct: " + id + (resume ? " (resume)" : ""), meta);
        return Map.of("status", "queued", "jobId", job.id, "warName", id, "resume", resume);
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
        return Map.of("status", "queued", "jobId", job.id, "endpoint", endpointName, "warName", id);
    }

    // ==================== MongoDB Catalog ====================

    @PostMapping("/{id}/fetch-catalog")
    public Map<String, Object> fetchCatalog(@PathVariable String id,
                                             @RequestBody(required = false) Map<String, String> body) {
        String mongoUri = (body != null) ? body.get("mongoUri") : null;

        java.util.Set<String> catalog;
        if (mongoUri != null && !mongoUri.isBlank()) {
            catalog = mongoCatalogService.fetchAndStore(id, mongoUri);
        } else {
            Path warPath = warPersistenceService.getJarFilePath(id);
            if (warPath == null) {
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Stored WAR not found: " + id);
            }
            try {
                Map<String, String> configs = warParserService.extractConfigFiles(warPath.toFile());
                catalog = mongoCatalogService.fetchAndStore(id, configs);
            } catch (IOException e) {
                throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
                        "Failed to extract config: " + e.getMessage());
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("warName", id);
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

    @GetMapping("/{id}/catalog")
    public Map<String, Object> getCatalog(@PathVariable String id) {
        MongoCatalogService.MongoCatalog catalog = mongoCatalogService.loadCatalog(id);
        if (catalog == null) {
            return Map.of("warName", id, "status", "none",
                    "message", "No catalog available. Use POST /{id}/fetch-catalog to fetch one.");
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("warName", id);
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

    // ==================== Collections Report ====================

    @GetMapping("/{id}/collections")
    public List<Map<String, Object>> getCollectionReport(@PathVariable String id) throws IOException {
        JarAnalysis analysis = warPersistenceService.load(id);
        if (analysis == null) return List.of();

        Map<String, Map<String, Object>> collMap = new LinkedHashMap<>();
        for (EndpointInfo ep : analysis.getEndpoints()) {
            if (ep.getAggregatedCollections() == null || ep.getAggregatedCollections().isEmpty()) {
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
        return collMap.values().stream()
                .sorted((a, b) -> (int) b.get("endpointCount") - (int) a.get("endpointCount"))
                .peek(m -> m.put("operations", new ArrayList<>((Set<?>) m.get("operations"))))
                .toList();
    }

    // ==================== Delete & Reanalyze ====================

    @DeleteMapping("/{id}")
    public Map<String, Object> deleteAnalysis(@PathVariable String id) throws IOException {
        int killed = sessionManager.killSessionsForJar(id);
        claudeTracker.removeTracking(id);
        mongoCatalogService.deleteCatalog(id);
        correctionPersistence.deleteAll(id);
        boolean deleted = warPersistenceService.delete(id);
        if (!deleted) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "WAR analysis not found: " + id);
        }
        log.info("Deleted WAR {}: {} Claude sessions killed", id, killed);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "deleted");
        result.put("id", id);
        result.put("sessionsKilled", killed);
        return result;
    }

    @PostMapping("/reanalyze-all")
    public Map<String, Object> reanalyzeAll() throws IOException {
        List<Map<String, Object>> wars = warPersistenceService.listJars();
        List<Map<String, Object>> queued = new ArrayList<>();

        for (Map<String, Object> war : wars) {
            String warName = (String) war.get("jarName");
            if (warName == null) continue;
            String id = warName.endsWith(".war") ? warName.substring(0, warName.length() - 4) : warName;

            Path storedWar = warPersistenceService.getJarFilePath(id);
            if (storedWar == null || !Files.exists(storedWar)) continue;

            long fileSize = Files.size(storedWar);
            Path tempWar = Files.createTempFile("war-reanalyze-", ".war");
            Files.copy(storedWar, tempWar, java.nio.file.StandardCopyOption.REPLACE_EXISTING);

            sessionManager.killSessionsForJar(id);
            claudeTracker.removeTracking(id);
            mongoCatalogService.deleteCatalog(id);
            correctionPersistence.deleteAll(id);
            warPersistenceService.delete(id);

            Map<String, Object> metadata = buildMetadata(warName, tempWar, fileSize, "static", null);
            QueueJob job = queueService.submit(QueueJob.Type.WAR_UPLOAD, "Re-analyze: " + warName, metadata);
            queued.add(Map.of("warName", warName, "jobId", job.id, "warSize", fileSize));
        }

        log.info("Re-analyze all WARs: queued {}", queued.size());
        return Map.of("status", "queued", "count", queued.size(), "jobs", queued);
    }

    @PostMapping("/{id}/reanalyze")
    public Map<String, Object> reanalyze(@PathVariable String id) throws IOException {
        Path storedWar = warPersistenceService.getJarFilePath(id);
        if (storedWar == null || !Files.exists(storedWar)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Stored WAR not found for: " + id);
        }

        long fileSize = Files.size(storedWar);
        String warName = id.endsWith(".war") ? id : id + ".war";

        Path tempWar = Files.createTempFile("war-reanalyze-", ".war");
        Files.copy(storedWar, tempWar, java.nio.file.StandardCopyOption.REPLACE_EXISTING);

        sessionManager.killSessionsForJar(id);
        claudeTracker.removeTracking(id);
        mongoCatalogService.deleteCatalog(id);
        correctionPersistence.deleteAll(id);
        warPersistenceService.delete(id);

        Map<String, Object> metadata = buildMetadata(warName, tempWar, fileSize, "static", null);
        QueueJob job = queueService.submit(QueueJob.Type.WAR_UPLOAD, "Re-analyze: " + warName, metadata);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "queued");
        response.put("jobId", job.id);
        response.put("warName", warName);
        response.put("warSize", fileSize);
        return response;
    }

    @PostMapping("/{id}/revert-claude")
    public Map<String, Object> revertClaude(@PathVariable String id) throws IOException {
        warPersistenceService.revertCorrected(id);
        return Map.of("status", "reverted", "warName", id);
    }

    // ==================== Versions & Corrections ====================

    @GetMapping("/{id}/versions")
    public Map<String, Object> getVersions(@PathVariable String id) {
        Map<String, Object> info = warPersistenceService.getVersionInfo(id);
        info.put("warName", id);
        return info;
    }

    @GetMapping("/{id}/corrections")
    public Map<String, Object> getCorrections(@PathVariable String id) {
        Map<String, CorrectionResult> corrections = correctionPersistence.loadAllCorrections(id);
        Map<String, Object> summary = correctionPersistence.loadSummary(id);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("warName", id);
        result.put("totalCorrections", corrections.size());
        result.put("corrections", corrections);
        if (summary != null) result.put("summary", summary);
        return result;
    }

    @GetMapping("/{id}/corrections/{endpoint}")
    public Map<String, Object> getCorrectionForEndpoint(@PathVariable String id,
                                                         @PathVariable String endpoint) {
        CorrectionResult correction = correctionPersistence.loadCorrection(id, endpoint);
        if (correction == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "No correction data for endpoint: " + endpoint);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("warName", id);
        result.put("endpoint", endpoint);
        result.put("correction", correction);
        return result;
    }

    @GetMapping("/{id}/correction-logs/{endpointName}")
    public List<Map<String, Object>> getCorrectionLogs(@PathVariable String id, @PathVariable String endpointName) {
        Path dir = correctionPersistence.getWorkDir(id);
        List<Map<String, Object>> logs = new ArrayList<>();
        if (!Files.isDirectory(dir)) return logs;
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
                    String label;
                    if (name.contains("_chunk")) {
                        String chunkNum = name.replaceAll(".*_chunk(\\d+)_.*", "$1");
                        label = "Chunk " + (Integer.parseInt(chunkNum) + 1) + " " + type;
                    } else {
                        label = "Single-shot " + type;
                    }
                    logs.add(Map.of("name", name, "label", label, "type", type, "size", content.length(), "content", content));
                } catch (IOException ignored) {}
            });
        } catch (IOException e) { /* empty */ }
        return logs;
    }

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
                    String endpoint = name.contains("_corr") ? name.substring(0, name.indexOf("_corr")) : name;
                    String label;
                    if (name.contains("_chunk")) {
                        String chunkNum = name.replaceAll(".*_chunk(\\d+)_.*", "$1");
                        label = endpoint + " - Chunk " + (Integer.parseInt(chunkNum) + 1) + " " + type;
                    } else {
                        label = endpoint + " - " + type;
                    }
                    logs.add(Map.of("name", name, "label", label, "type", type, "endpoint", endpoint, "size", size));
                } catch (IOException ignored) {}
            });
        } catch (IOException e) { /* empty */ }
        return logs;
    }

    @GetMapping("/{id}/correction-logs/file/{fileName}")
    public ResponseEntity<String> getCorrectionLogFile(@PathVariable String id, @PathVariable String fileName) {
        Path dir = correctionPersistence.getWorkDir(id);
        Path file = dir.resolve(fileName);
        if (!Files.isRegularFile(file) || !file.startsWith(dir)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Log file not found: " + fileName);
        }
        try {
            return ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body(Files.readString(file));
        } catch (IOException e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to read: " + e.getMessage());
        }
    }

    // ==================== Run Logs ====================

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
                            logs.add(Map.of("name", p.getFileName().toString(), "size", Files.size(p), "modified", Files.getLastModifiedTime(p).toString()));
                        } catch (IOException ignored) {}
                    });
        } catch (IOException e) { /* empty */ }
        return logs;
    }

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

    // ==================== Sessions ====================

    @PostMapping("/{id}/kill-sessions")
    public Map<String, Object> killWarSessions(@PathVariable String id) {
        int killed = sessionManager.killRunningForJar(id);
        if (killed > 0) {
            claudeTracker.markFailed(id, "Cancelled by user (" + killed + " session(s) killed)");
        }
        return Map.of("status", "done", "sessionsKilled", killed, "warName", id);
    }

    @GetMapping("/sessions")
    public List<Map<String, Object>> listSessions() {
        return sessionManager.listSessions();
    }

    @PostMapping("/sessions/{sessionId}/kill")
    public Map<String, Object> killSession(@PathVariable String sessionId) {
        boolean killed = sessionManager.kill(sessionId);
        if (!killed) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Session not found or not running: " + sessionId);
        }
        return Map.of("status", "killed", "sessionId", sessionId);
    }

    // ==================== Excel Export ====================

    @PostMapping("/export-excel")
    public ResponseEntity<byte[]> exportExcel(@RequestBody Map<String, Object> reportData) throws IOException {
        byte[] xlsx = excelExportService.generateReport(reportData);
        String warName = String.valueOf(reportData.getOrDefault("jarName", "analysis"));
        String fileName = warName.replace(".war", "") + "_report.xlsx";
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + "\"")
                .contentType(MediaType.parseMediaType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"))
                .body(xlsx);
    }

    // ==================== Claude Stats ====================

    @GetMapping("/{name}/claude-stats")
    public Map<String, Object> getClaudeStats(@PathVariable String name) {
        return claudeCallLogger.getStats(name);
    }

    @GetMapping("/{name}/claude-stats/{sessionId}")
    public List<Map<String, Object>> getClaudeSessionDetail(@PathVariable String name,
                                                             @PathVariable String sessionId) {
        return claudeCallLogger.getSessionDetail(name, sessionId);
    }

    // ==================== Internal ====================

    private AnalysisDataProvider resolveProvider(String id, String version) {
        if ("static".equals(version)) {
            Path filePath = warPersistenceService.getStaticFilePath(id);
            if (filePath == null || !Files.exists(filePath))
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Static analysis not found: " + id);
            return new StaticAnalysisProvider(filePath, objectMapper, warDataPaths, id);
        }
        if ("previous".equals(version)) {
            Path filePath = warPersistenceService.getCorrectedPrevFilePath(id);
            if (filePath == null || !Files.exists(filePath))
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Previous corrected version not found: " + id);
            return new ClaudeAnalysisProvider(filePath, objectMapper, warDataPaths, id, "previous");
        }

        Path filePath = warPersistenceService.getFilePath(id);
        if (filePath == null || !Files.exists(filePath))
            throw new ResponseStatusException(HttpStatus.NOT_FOUND,
                    version != null ? "Version '" + version + "' not found: " + id : "Analysis not found: " + id);

        Path corrected = warPersistenceService.getCorrectedFilePath(id);
        if (corrected != null && filePath.equals(corrected)) {
            return new ClaudeAnalysisProvider(filePath, objectMapper, warDataPaths, id, "claude");
        }
        return new StaticAnalysisProvider(filePath, objectMapper, warDataPaths, id);
    }
}
