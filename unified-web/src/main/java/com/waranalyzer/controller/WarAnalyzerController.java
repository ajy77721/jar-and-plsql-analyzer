package com.waranalyzer.controller;

import com.fasterxml.jackson.databind.JsonNode;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
    private final ArtifactStore store;
    private final com.plsqlanalyzer.web.page.FacetPageResponder facets;

    @Value("${app.config-dir:}")
    private String configDirProp;

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
                                 ClaudeCallLogger claudeCallLogger,
                                 @Qualifier("jarArtifactStore") ArtifactStore store,
                                 com.plsqlanalyzer.web.page.FacetPageResponder facets) {
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
        this.store = store;
        this.facets = facets;
    }

    private InputStream openRead(ArtifactKey key) throws IOException {
        return store.read(key);
    }

    private String readString(ArtifactKey key) throws IOException {
        try (InputStream in = openRead(key)) {
            return new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
    }

    private OutputStream openWrite(ArtifactKey key, long contentLength) throws IOException {
        return store.write(key, contentLength);
    }

    /** The ".gz" sibling of a cache key. */
    private static ArtifactKey gzOf(ArtifactKey key) {
        return key.parent().resolve(key.leaf() + ".gz");
    }

    /**
     * Key for the provider's summary cache.
     *
     * <p>Asked of the provider directly — it owns the static/claude choice, and the key is what
     * {@code PersistenceService.preGenerateSummaryCache} wrote. This used to go through
     * {@code keyForPath(getSummaryCachePath())}; both deprecated Path accessors are gone.
     */
    private ArtifactKey summaryCacheKey(AnalysisDataProvider provider) {
        return provider.getSummaryCacheKey();
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
        try (var parser = objectMapper.getFactory().createParser(provider.openStream());
             var gen = objectMapper.getFactory().createGenerator(response.getOutputStream())) {
            provider.streamAnalysis(parser, gen);
            gen.flush();
        }
    }

    /**
     * MID-016 companion finding, resolved differently from its JAR counterpart. This endpoint had
     * no {@code format} parameter at all — always the expanded tree — even though
     * {@code AnalysisDataProvider} (the same interface {@code AnalyzerController} uses) already
     * carries {@code streamCallTreeDag} as a default method. The fast path existed and was never
     * wired here.
     *
     * <p><b>Default stays {@code full}</b>, unlike the JAR endpoint. Checked before flipping it the
     * same way: {@code war/js/api.js} has no {@code JA.dag} module and no inflation step — flipping
     * the default would have handed the flat node dictionary to code written to render a nested
     * tree, breaking every WAR call-tree view immediately rather than fixing anything. {@code dag}
     * is added as an explicit opt-in only, so a future WAR UI update can request the fast path once
     * it can actually consume it; today's behaviour is unchanged for the client that exists.
     */
    @GetMapping("/{id}/endpoints/by-index/{idx}/call-tree")
    public void getEndpointCallTree(@PathVariable String id,
                                    @PathVariable int idx,
                                    @RequestParam(value = "version", required = false) String version,
                                    @RequestParam(value = "format", required = false) String format,
                                    jakarta.servlet.http.HttpServletResponse response) throws IOException {
        AnalysisDataProvider provider = resolveProvider(id, version);
        response.setContentType("application/json");
        response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
        boolean dag = "dag".equalsIgnoreCase(format);
        try (var parser = objectMapper.getFactory().createParser(provider.openStream());
             var gen = objectMapper.getFactory().createGenerator(response.getOutputStream())) {
            if (dag) provider.streamCallTreeDag(parser, gen, idx);
            else provider.streamCallTree(parser, gen, idx);
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
        response.setContentType("application/json");
        response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");

        ArtifactKey summaryCache = summaryCacheKey(provider);
        ArtifactKey gzCache = gzOf(summaryCache);

        if (version == null && store.exists(gzCache)) {
            try {
                if (store.lastModified(gzCache) >= provider.lastModifiedMillis()) {
                    String ae = request.getHeader("Accept-Encoding");
                    if (ae != null && ae.contains("gzip")) {
                        response.setHeader("Content-Encoding", "gzip");
                        response.setContentLengthLong(store.size(gzCache));
                        try (InputStream in = openRead(gzCache)) {
                            in.transferTo(response.getOutputStream());
                        }
                        return;
                    }
                    if (store.exists(summaryCache)) {
                        try (InputStream in = openRead(summaryCache)) {
                            in.transferTo(response.getOutputStream());
                        }
                        return;
                    }
                }
            } catch (IOException e) {
                log.debug("Summary cache read failed, regenerating: {}", e.getMessage());
            }
        }

        if (version == null) {
            try (var parser = objectMapper.getFactory().createParser(provider.openStream())) {
                java.io.ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream(8 * 1024 * 1024);
                try (var gen = objectMapper.getFactory().createGenerator(buf)) {
                    provider.streamSummary(parser, gen);
                    gen.flush();
                }
                byte[] data = buf.toByteArray();
                try {
                    try (OutputStream out = openWrite(summaryCache, data.length)) {
                        out.write(data);
                    }
                    try (var gzOut = new java.util.zip.GZIPOutputStream(
                            new java.io.BufferedOutputStream(store.write(gzCache), 64 * 1024))) {
                        gzOut.write(data);
                    }
                } catch (IOException e) { log.debug("Failed to write summary cache: {}", e.getMessage()); }
                response.getOutputStream().write(data);
                response.getOutputStream().flush();
            }
        } else {
            try (var parser = objectMapper.getFactory().createParser(provider.openStream());
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

        ArtifactKey summaryCache = summaryCacheKey(provider);
        if (!store.exists(summaryCache)) {
            try (var parser = objectMapper.getFactory().createParser(provider.openStream())) {
                java.io.ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream(8 * 1024 * 1024);
                try (var gen = objectMapper.getFactory().createGenerator(buf)) {
                    provider.streamSummary(parser, gen);
                    gen.flush();
                }
                byte[] data = buf.toByteArray();
                try {
                    try (OutputStream out = openWrite(summaryCache, data.length)) {
                        out.write(data);
                    }
                    try (var gzOut = new java.util.zip.GZIPOutputStream(
                            new java.io.BufferedOutputStream(store.write(gzOf(summaryCache)), 64 * 1024))) {
                        gzOut.write(data);
                    }
                } catch (IOException e) {
                    log.debug("Failed to write summary cache for slice: {}", e.getMessage());
                }
            }
        }

        // Streamed, not buffered: the cached summary is still large.
        try (var parser = objectMapper.getFactory().createParser(openRead(summaryCache));
             var gen = objectMapper.getFactory().createGenerator(response.getOutputStream())) {
            provider.streamSummarySlice(parser, gen, mode, fields);
            gen.flush();
        }
    }

    // ==================== Class Tree ====================

    /**
     * The class tree, without member arrays by default.
     *
     * <p>This endpoint had no {@code members} parameter at all, so the WAR page always received the
     * full tree — the 93.8% the JAR fork removed was never removed here. The WAR page reuses the
     * JAR fork's `codeTree.js`, which fetches a class's members on expand, so it was downloading
     * arrays it then ignored.
     */
    @GetMapping("/{id}/classes/tree")
    public void getClassTree(@PathVariable String id,
                             @RequestParam(value = "version", required = false) String version,
                             @RequestParam(value = "members", required = false, defaultValue = "false")
                             boolean members,
                             jakarta.servlet.http.HttpServletResponse response) throws IOException {
        AnalysisDataProvider provider = resolveProvider(id, version);
        response.setContentType("application/json");
        response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
        try (var parser = objectMapper.getFactory().createParser(provider.openStream());
             var gen = objectMapper.getFactory().createGenerator(response.getOutputStream())) {
            provider.streamClassTree(parser, gen, members);
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
        try (var parser = objectMapper.getFactory().createParser(provider.openStream());
             var gen = objectMapper.getFactory().createGenerator(response.getOutputStream())) {
            provider.streamClassByIndex(parser, gen, idx);
            gen.flush();
        }
    }

    /**
     * Global Search — the WAR-side port of {@code AnalyzerController#getGlobalSearch}. WAR ports
     * only the two facets its own {@link AnalysisDataProvider} abstraction can stream directly
     * (classes, endpoints) — unlike the JAR version, it deliberately does NOT search bundled-jars
     * ({@code AnalysisQuery}/{@code AnalysisRepository}-based), since WarAnalyzerController never
     * wired that Mongo-native query layer at all (it uses {@code AnalysisDataProvider} +
     * {@code ArtifactStore} exclusively) — reusing it here untested would risk resolving the wrong
     * storage key for a WAR id (WAR's real storage root, {@code jar/war/<id>/}, is one segment
     * deeper than a JAR's {@code jar/<id>/} — the exact class of bug already found and fixed this
     * session in {@code PersistenceService.listJarsFromStore}). Reuses {@code JarFacetSpecs
     * .SEARCH} (made public for this) so paging/sort/{@code cf} behave identically to the JAR
     * page, and {@code jar/js/search.js} can be reused (via a WAR-specific URL-prefix copy)
     * without any rendering changes.
     */
    @GetMapping("/{id}/global-search")
    public ResponseEntity<JsonNode> getGlobalSearch(
            @PathVariable String id,
            @RequestParam(value = "version", required = false) String version,
            @RequestParam(value = "q", required = false) String q,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "after", required = false) String after,
            @RequestParam(value = "sort", required = false) String sort,
            @RequestParam(value = "cf", required = false) java.util.List<String> cf,
            @RequestHeader(value = "Accept", required = false) String accept) throws IOException {
        if (q == null || q.isBlank()) {
            return com.plsqlanalyzer.web.page.FacetPageResponder.badRequest(objectMapper, "q is required");
        }
        try {
            com.fasterxml.jackson.databind.node.ArrayNode rows = buildSearchRows(id, version, q);
            return facets.respond(rows, null, com.jaranalyzer.controller.JarFacetSpecs.SEARCH,
                    new com.plsqlanalyzer.web.page.FacetPageResponder.Params(
                            limit, after, sort, null, null, cf, accept),
                    null);
        } catch (IllegalArgumentException | com.analyzer.storage.page.Cursor.InvalidCursorException e) {
            return com.plsqlanalyzer.web.page.FacetPageResponder.badRequest(objectMapper, e.getMessage());
        }
    }

    /** Column-filter value lists for {@link #getGlobalSearch}. */
    @GetMapping("/{id}/global-search/summary")
    public ResponseEntity<JsonNode> getGlobalSearchSummary(
            @PathVariable String id,
            @RequestParam(value = "version", required = false) String version,
            @RequestParam(value = "q", required = false) String q) throws IOException {
        if (q == null || q.isBlank()) {
            return com.plsqlanalyzer.web.page.FacetPageResponder.badRequest(objectMapper, "q is required");
        }
        com.fasterxml.jackson.databind.node.ArrayNode rows = buildSearchRows(id, version, q);
        return facets.summary(rows, null, com.jaranalyzer.controller.JarFacetSpecs.SEARCH, null);
    }

    private com.fasterxml.jackson.databind.node.ArrayNode buildSearchRows(String id, String version, String q)
            throws IOException {
        String needle = q.toLowerCase(java.util.Locale.ROOT);
        com.fasterxml.jackson.databind.node.ArrayNode out = objectMapper.createArrayNode();
        int[] idx = {0};

        try {
            AnalysisDataProvider provider = resolveProvider(id, version);
            java.io.ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream(64 * 1024);
            try (var parser = objectMapper.getFactory().createParser(provider.openStream());
                 var gen = objectMapper.getFactory().createGenerator(buf)) {
                provider.streamClassTree(parser, gen, false);
                gen.flush();
            }
            JsonNode arr = objectMapper.readTree(buf.toByteArray());
            if (arr.isArray()) {
                for (JsonNode row : arr) {
                    String fqn = row.path("fullyQualifiedName").asText(row.path("simpleName").asText(""));
                    String simple = row.path("simpleName").asText("");
                    String pkg = row.path("packageName").asText("");
                    boolean nameMatch = fqn.toLowerCase(java.util.Locale.ROOT).contains(needle)
                            || simple.toLowerCase(java.util.Locale.ROOT).contains(needle);
                    boolean contentMatch = pkg.toLowerCase(java.util.Locale.ROOT).contains(needle);
                    if (nameMatch || contentMatch) {
                        var o = out.addObject();
                        o.put("type", "CLASS");
                        o.put("name", fqn);
                        o.put("schema", pkg);
                        o.put("matchedField", nameMatch ? "name" : "content");
                        o.put("_idx", idx[0]++);
                        o.set("raw", row);
                    }
                }
            }
        } catch (Exception ignored) {
            // No class tree for this analysis/version — that facet is simply absent from results.
        }

        try {
            AnalysisDataProvider provider = resolveProvider(id, version);
            java.io.ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream(64 * 1024);
            try (var parser = objectMapper.getFactory().createParser(provider.openStream());
                 var gen = objectMapper.getFactory().createGenerator(buf)) {
                provider.streamSummaryEndpointPage(parser, gen, null, Integer.MAX_VALUE, q);
                gen.flush();
            }
            JsonNode arr = objectMapper.readTree(buf.toByteArray());
            if (arr.isArray()) {
                for (JsonNode ep : arr) {
                    String path = ep.path("fullPath").asText(ep.path("methodName").asText(""));
                    String method = ep.path("httpMethod").asText("");
                    String controller = ep.path("controllerSimpleName").asText("");
                    boolean nameMatch = path.toLowerCase(java.util.Locale.ROOT).contains(needle)
                            || method.toLowerCase(java.util.Locale.ROOT).contains(needle);
                    var o = out.addObject();
                    o.put("type", "ENDPOINT");
                    o.put("name", (method.isBlank() ? "" : method + " ") + path);
                    o.put("schema", controller);
                    o.put("matchedField", nameMatch ? "name" : "content");
                    o.put("_idx", idx[0]++);
                    o.set("raw", ep);
                }
            }
        } catch (Exception ignored) {
            // Endpoint scan already pre-filters by q server-side; any failure just drops this facet.
        }

        return out;
    }

    /**
     * Real gap, found live: {@code WarAnalysisExecutor} already calls {@code
     * warPersistenceService.storeConnections(warName, connInfo)} during every WAR upload — the
     * data has existed the whole time — but there was never a route to read it back, and
     * {@code war/js/api.js} inherited JAR's {@code getConnections} hardcoded to
     * {@code /api/jar/jars/}. Trivial port: {@code loadConnections} lives on the same
     * {@code PersistenceService} class both analyzers share, and is already keyRoot-aware (fixed
     * earlier this session for {@code listJarsFromStore}), so no storage-layer risk here.
     */
    @GetMapping("/{id}/connections")
    public ResponseEntity<?> getConnections(@PathVariable String id) {
        Map<String, Object> info = warPersistenceService.loadConnections(id);
        if (info == null) return ResponseEntity.ok(Map.of("available", false));
        info.put("available", true);
        return ResponseEntity.ok(info);
    }

    /**
     * Real gap, found live: same shared, already keyRoot-aware storage layer as {@link
     * #getConnections} — {@code storeDomainConfig}/{@code domainConfigKey} live on
     * {@code PersistenceService}/{@code JarDataPaths}, used by both analyzers identically.
     * Falls back to the SAME global {@code static/jar/domain-config.json} classpath default the
     * JAR page uses — WAR doesn't need its own separate default, matching how WAR already reuses
     * JAR's UI/rendering code throughout.
     */
    @PostMapping("/{id}/domain-config")
    public Map<String, Object> uploadDomainConfig(@PathVariable String id,
                                                   @RequestParam("configFile") MultipartFile configFile) throws IOException {
        if (configFile == null || configFile.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "configFile is required");
        }
        Path tempConfig = Files.createTempFile("war-domain-config-", ".json");
        try {
            configFile.transferTo(tempConfig);
            new ObjectMapper().readValue(tempConfig.toFile(), java.util.Map.class);
            warPersistenceService.storeDomainConfig(tempConfig, id);
        } finally {
            Files.deleteIfExists(tempConfig);
        }
        log.info("Per-WAR domain config stored for {}: {} bytes", id, configFile.getSize());
        return Map.of(
                "status", "stored",
                "jarName", id,
                "configSize", configFile.getSize(),
                "message", "Domain config stored. Will be applied on next re-analysis."
        );
    }

    @GetMapping("/{id}/domain-config")
    public ResponseEntity<String> getDomainConfig(@PathVariable String id) throws IOException {
        ArtifactKey configKey = warDataPaths.domainConfigKey(id);
        if (store.exists(configKey)) {
            return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(readString(configKey));
        }
        String globalContent = readGlobalDomainConfig();
        if (globalContent != null) {
            return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(globalContent);
        }
        throw new ResponseStatusException(HttpStatus.NOT_FOUND, "No domain config found for: " + id);
    }

    private String readGlobalDomainConfig() {
        try {
            if (configDirProp != null && !configDirProp.isBlank()) {
                Path dir = Path.of(configDirProp).toAbsolutePath();
                Path file = dir.resolve("domain-config.json");
                if (Files.exists(file)) return Files.readString(file);
                Path parent = dir.getParent();
                if (parent != null) {
                    Path up = parent.getParent();
                    if (up != null) {
                        Path upFile = up.resolve(configDirProp).resolve("domain-config.json");
                        if (Files.exists(upFile)) return Files.readString(upFile);
                    }
                }
            }
            ClassPathResource res = new ClassPathResource("static/jar/domain-config.json");
            if (res.exists()) {
                try (InputStream is = res.getInputStream()) {
                    return new String(is.readAllBytes());
                }
            }
        } catch (Exception e) {
            log.warn("Could not read global domain-config.json: {}", e.getMessage());
        }
        return null;
    }

    /**
     * Real gap, found live: {@code WarAnalysisExecutor} already calls {@code
     * warPersistenceService.storeBundledJarInfo(warName, bundledJars)} on every upload — the data
     * exists — but there was no route to read it back. Ports only the SIMPLE, non-paginated
     * fallback path ({@code persistenceService.loadBundledJarInfo}, same shared, keyRoot-aware
     * class as {@link #getConnections}/domain-config) — deliberately NOT the {@code
     * AnalysisQuery}/{@code AnalysisRepository}-based streaming/paging path JAR's version also
     * has, since that query layer was never wired for WAR and reusing it untested risks resolving
     * the wrong storage key (WAR's real root is one segment deeper than JAR's — the exact bug
     * already found and fixed this session in {@code PersistenceService.listJarsFromStore}). A
     * WAR's WEB-INF/lib is realistically tens of jars, not thousands, so the whole-list response
     * this returns is not a real size concern the way it would be for a huge multi-module JAR.
     */
    @GetMapping("/{id}/bundled-jars")
    public ResponseEntity<?> getBundledJars(@PathVariable String id) {
        return ResponseEntity.ok(warPersistenceService.loadBundledJarInfo(id));
    }

    /** Port of {@code AnalyzerController#getBundledJarContent} — identical logic, WAR's own
     *  keyRoot-aware {@code warDataPaths}/{@code openRead} in place of JAR's. */
    @GetMapping("/{id}/bundled-jars/content")
    public ResponseEntity<String> getBundledJarContent(@PathVariable String id,
                                                       @RequestParam("jar") String jarName,
                                                       @RequestParam("file") String fileName) {
        ArtifactKey key = warDataPaths.bundledJarsKey(id);
        if (!store.exists(key)) return ResponseEntity.notFound().build();
        boolean manifest = "MANIFEST.MF".equals(fileName);
        try (var parser = objectMapper.getFactory().createParser(openRead(key));
             var rows = com.plsqlanalyzer.web.page.JsonArrayRows.of(parser, objectMapper, null,
                     row -> jarName.equals(row.path("name").asText("")) ? row : null)) {
            if (!rows.hasNext()) return ResponseEntity.notFound().build();
            var row = rows.next();
            var content = manifest ? row.path("manifest") : row.path("resources").path(fileName);
            if (content.isMissingNode() || content.isNull()) {
                return ResponseEntity.notFound().build();
            }
            return ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN)
                    .body(content.asText(""));
        } catch (IOException e) {
            return ResponseEntity.internalServerError().body("Error reading bundled jar content: " + e.getMessage());
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
            ArtifactKey warKey = warDataPaths.storedJarKey(id);
            if (!store.exists(warKey)) {
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Stored WAR not found: " + id);
            }
            // materialize(): WarParserService opens the archive as a JarFile, which needs a real
            // seekable file. This is the documented escape hatch; locally it is zero-copy.
            try (com.analyzer.storage.LocalHandle handle = store.materialize(warKey)) {
                Map<String, String> configs = warParserService.extractConfigFiles(handle.path().toFile());
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

    /**
     * The collection report, streamed — the WAR twin of MID-013.
     *
     * <p>Was `warPersistenceService.load(id)`: the whole analysis in heap to build an aggregate
     * measured at 3.8 KB on the JAR side (14 collections, 155 endpoint/collection pairs) against a
     * 49.4 MB file. Same shape here, same fix, and deliberately the *same* aggregator — two copies
     * of this aggregation is how the two forks came to differ over `members` in the first place.
     */
    @GetMapping("/{id}/collections")
    public ResponseEntity<JsonNode> getCollectionReport(
            @PathVariable String id,
            @RequestParam(value = "version", required = false) String version,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "after", required = false) String after,
            @RequestHeader(value = "Accept", required = false) String accept) throws IOException {
        AnalysisDataProvider provider = resolveProvider(id, version);
        List<com.jaranalyzer.report.CollectionReportAggregator.CollectionRow> rows;
        try (var parser = objectMapper.getFactory().createParser(provider.openStream())) {
            rows = com.jaranalyzer.report.CollectionReportAggregator.aggregate(parser, objectMapper);
        }
        return ResponseEntity.ok(com.jaranalyzer.report.CollectionReportPage.respond(
                objectMapper, rows, limit, after, accept));
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

            ArtifactKey storedWar = warDataPaths.storedJarKey(id);
            if (!store.exists(storedWar)) continue;

            long fileSize = store.size(storedWar);
            // Temp scratch consumed by the queue worker: stays local by design, but its CONTENT
            // now comes from the store rather than assuming a local stored.war exists.
            Path tempWar = Files.createTempFile("war-reanalyze-", ".war");
            try (InputStream in = openRead(storedWar);
                 OutputStream out = Files.newOutputStream(tempWar)) {
                in.transferTo(out);
            }

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
        ArtifactKey storedWar = warDataPaths.storedJarKey(id);
        if (!store.exists(storedWar)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Stored WAR not found for: " + id);
        }

        long fileSize = store.size(storedWar);
        String warName = id.endsWith(".war") ? id : id + ".war";

        Path tempWar = Files.createTempFile("war-reanalyze-", ".war");
        try (InputStream in = openRead(storedWar);
             OutputStream out = Files.newOutputStream(tempWar)) {
            in.transferTo(out);
        }

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
        ArtifactKey dir = warDataPaths.correctionsDirKey(id);
        List<Map<String, Object>> logs = new ArrayList<>();
        String method = endpointName.contains(".") ? endpointName.substring(endpointName.lastIndexOf('.') + 1) : endpointName;
        try (var stream = store.list(dir, false)) {
            stream.filter(java.util.function.Predicate.not(com.analyzer.storage.ArtifactEntry::directory)).map(com.analyzer.storage.ArtifactEntry::key).filter(k -> {
                String name = k.leaf();
                return name.startsWith(method + "_corr") && (name.endsWith("_input.txt") || name.endsWith("_output.json"));
            }).sorted(Comparator.comparing(ArtifactKey::leaf)).forEach(p -> {
                try {
                    String name = p.leaf();
                    String content = readString(p);
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
        ArtifactKey dir = warDataPaths.correctionsDirKey(id);
        List<Map<String, Object>> logs = new ArrayList<>();
        try (var stream = store.list(dir, false)) {
            stream.filter(java.util.function.Predicate.not(com.analyzer.storage.ArtifactEntry::directory)).filter(e -> {
                String name = e.key().leaf();
                return name.contains("_corr") && (name.endsWith("_input.txt") || name.endsWith("_output.json"));
            }).sorted(Comparator.comparing(e -> e.key().leaf())).forEach(e -> {
                // Size comes from the listing entry now, so there is no per-file stat that can
                // fail here — the old inner IOException catch has nothing left to catch.
                String name = e.key().leaf();
                long size = e.size();
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
            });
        } catch (IOException e) { /* empty */ }
        return logs;
    }

    @GetMapping("/{id}/correction-logs/file/{fileName}")
    public ResponseEntity<String> getCorrectionLogFile(@PathVariable String id, @PathVariable String fileName) {
        // No base dir left to compare against, so the startsWith(dir) containment check becomes an
        // explicit rejection of anything that is not a single plain file name.
        if (fileName.contains("..") || fileName.contains("/") || fileName.contains("\\")) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Log file not found: " + fileName);
        }
        ArtifactKey file = warDataPaths.correctionsDirKey(id).resolve(fileName);
        if (!store.exists(file)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Log file not found: " + fileName);
        }
        try {
            return ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body(readString(file));
        } catch (IOException e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to read: " + e.getMessage());
        }
    }

    // ==================== Run Logs ====================

    @GetMapping("/{id}/run-logs")
    public List<Map<String, Object>> listRunLogs(@PathVariable String id) {
        ArtifactKey logDir = warDataPaths.correctionsDirKey(id);
        List<Map<String, Object>> logs = new ArrayList<>();
        try (var stream = store.list(logDir, false)) {
            stream.filter(java.util.function.Predicate.not(com.analyzer.storage.ArtifactEntry::directory))
                    .filter(e -> e.key().leaf().startsWith("run_") && e.key().leaf().endsWith(".log"))
                    .sorted(Comparator.comparing((com.analyzer.storage.ArtifactEntry e) -> e.key().leaf()).reversed())
                    .forEach(e -> logs.add(Map.of(
                            "name", e.key().leaf(),
                            "size", e.size(),
                            "modified", java.time.Instant.ofEpochMilli(e.lastModified()).toString())));
        } catch (IOException e) { /* empty */ }
        return logs;
    }

    @GetMapping("/{id}/run-logs/{logName}")
    public String getRunLog(@PathVariable String id, @PathVariable String logName) {
        if (!logName.startsWith("run_") || !logName.endsWith(".log")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid log name");
        }
        if (logName.contains("..") || logName.contains("/") || logName.contains("\\")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid log name");
        }
        ArtifactKey logFile = warDataPaths.correctionsDirKey(id).resolve(logName);
        if (!store.exists(logFile)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Log not found: " + logName);
        }
        try { return readString(logFile); }
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
        // No Files.exists() here: the getXxxFilePath() accessors are store-backed and already
        // return null when the artifact is absent. Re-checking the local filesystem would report
        // "not found" for an analysis that exists in the store but not on this disk.
        if ("static".equals(version)) {
            ArtifactKey key = warPersistenceService.getStaticDocumentKey(id);
            if (key == null)
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Static analysis not found: " + id);
            return new StaticAnalysisProvider(key, objectMapper, warDataPaths, id);
        }
        if ("previous".equals(version)) {
            ArtifactKey key = warPersistenceService.getCorrectedPrevDocumentKey(id);
            if (key == null)
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Previous corrected version not found: " + id);
            return new ClaudeAnalysisProvider(key, objectMapper, warDataPaths, id, "previous");
        }

        ArtifactKey key = warPersistenceService.getDocumentKey(id);
        if (key == null)
            throw new ResponseStatusException(HttpStatus.NOT_FOUND,
                    version != null ? "Version '" + version + "' not found: " + id : "Analysis not found: " + id);

        ArtifactKey corrected = warPersistenceService.getCorrectedDocumentKey(id);
        if (corrected != null && key.equals(corrected)) {
            return new ClaudeAnalysisProvider(key, objectMapper, warDataPaths, id, "claude");
        }
        return new StaticAnalysisProvider(key, objectMapper, warDataPaths, id);
    }
}
