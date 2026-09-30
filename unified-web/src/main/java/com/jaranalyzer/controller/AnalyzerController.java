package com.jaranalyzer.controller;

import com.fasterxml.jackson.databind.JsonNode;
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

import org.springframework.beans.factory.annotation.Value;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.RequestScopedArtifactStore;
import com.analyzer.storage.page.Cursor;
import com.analyzer.storage.page.PageRequest;
import com.jaranalyzer.service.AbstractAnalysisDataProvider;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.springframework.core.io.ClassPathResource;
import java.time.LocalDateTime;
import java.util.*;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

@RestController("jarAnalyzerController")
@RequestMapping("/api/jar/jars")
public class AnalyzerController {

    private static final Logger log = LoggerFactory.getLogger(AnalyzerController.class);

    @Value("${app.config-dir:config}")
    private String configDirProp;

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
    private final ArtifactStore store;
    private final JarDataPaths paths;
    private final com.plsqlanalyzer.web.page.FacetPageResponder facets;
    private final com.jaranalyzer.service.DomainConfigLoader domainConfig;
    /** Paged list queries — the class tree and the bundled-JAR list (PLAN-09 Increment C). */
    private final com.analyzer.storage.AnalysisRepository repository;

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
                              ClaudeCallLogger claudeCallLogger,
                              @org.springframework.beans.factory.annotation.Qualifier("jarArtifactStore") ArtifactStore store,
                              com.plsqlanalyzer.web.page.FacetPageResponder facets,
                              com.jaranalyzer.service.DomainConfigLoader domainConfig,
                              com.analyzer.storage.AnalysisRepository repository) {
        this.repository = repository;
        this.facets = facets;
        this.domainConfig = domainConfig;
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
        this.store = store;
        this.paths = persistenceService.getPaths();
    }

    private InputStream openRead(ArtifactKey key) throws IOException {
        return store.read(key);
    }

    private OutputStream openWrite(ArtifactKey key, long contentLength) throws IOException {
        return store.write(key, contentLength);
    }

    private String readString(ArtifactKey key) throws IOException {
        try (InputStream in = openRead(key)) {
            return new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
    }

    /** The ".gz" sibling of a cache key. */
    private static ArtifactKey gzOf(ArtifactKey key) {
        return key.parent().resolve(key.leaf() + ".gz");
    }

    /**
     * Key for the provider's summary cache.
     *
     * <p>Taken straight from the provider rather than reimplemented here: the static/claude choice
     * stays owned by the provider, and this reads exactly what
     * {@code PersistenceService.preGenerateSummaryCache} wrote.
     */
    private ArtifactKey summaryCacheKey(AnalysisDataProvider provider) {
        ArtifactKey key = provider.getSummaryCacheKey();
        if (key == null) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
                    "Provider has no summary cache key");
        }
        return key;
    }

    /* ---- precomputed view summaries (see JarDataPaths#viewsDirKey) ---- */

    /**
     * Reads a precomputed view body (the summary header, the collections aggregate) when it exists
     * and is at least as new as the analysis document it summarises. Returns {@code null} on any
     * miss — absent, stale, or unreadable — so the caller falls back to the full computation. One
     * {@code exists}/{@code lastModified} pair is the whole cost, mirroring the {@code
     * _summary_stats.json} sidecar's own mtime guard.
     */
    private byte[] readJarView(AnalysisDataProvider provider, ArtifactKey key) {
        try {
            if (!store.exists(key) || store.lastModified(key) < provider.lastModifiedMillis()) return null;
            try (InputStream in = store.read(key)) {
                return in.readAllBytes();
            }
        } catch (Exception e) {
            log.debug("view read {} failed: {}", key, e.getMessage());
            return null;
        }
    }

    /** Best-effort write of a view body; a failure only means the next request recomputes. */
    private void writeJarView(ArtifactKey key, byte[] body) {
        if (body == null) return;
        try (OutputStream out = store.write(key, body.length)) {
            out.write(body);
        } catch (Exception e) {
            log.warn("view write {} failed: {}", key, e.getMessage());
        }
    }

    @PostMapping
    @SuppressWarnings("unchecked")
    public Map<String, Object> uploadAndAnalyze(
            @RequestParam("file") MultipartFile file,
            @RequestParam(value = "mode", defaultValue = "static") String mode,
            @RequestParam(value = "projectPath", required = false) String projectPath,
            @RequestParam(value = "renameSuffix", required = false) String renameSuffix,
            @RequestParam(value = "basePackage", required = false) String basePackage,
            @RequestParam(value = "configFile", required = false) MultipartFile configFile,
            @RequestParam(value = "entries", required = false) String entriesJson,
            @RequestParam(value = "targetedName", required = false) String targetedName) throws IOException {

        String jarName = file.getOriginalFilename();
        if (jarName == null || (!jarName.endsWith(".jar") && !jarName.endsWith(".war"))) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "File must be a .jar or .war");
        }

        if (renameSuffix != null && !renameSuffix.isBlank()) {
            String ext = jarName.endsWith(".war") ? ".war" : ".jar";
            String base = jarName.substring(0, jarName.length() - ext.length());
            jarName = base + "_" + renameSuffix + ext;
        }

        // Optional targeted entry points supplied alongside the upload — when present, this
        // upload runs as a targeted (entry-point-only) analysis of the freshly uploaded file
        // instead of a whole-project analysis, so the user never has to know/retype the jar's
        // path on the server's filesystem (see analyze-targeted, which requires exactly that).
        List<Map<String, Object>> entries = null;
        if (entriesJson != null && !entriesJson.isBlank()) {
            Object parsed;
            try {
                parsed = objectMapper.readValue(entriesJson, List.class);
            } catch (Exception e) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "entries must be a valid JSON array: " + e.getMessage());
            }
            if (!(parsed instanceof List<?> entriesList) || entriesList.isEmpty()) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "entries must be a non-empty array");
            }
            entries = new ArrayList<>();
            for (Object o : entriesList) {
                // methodName may be blank — a bare class or package name expands to every
                // method on that class (or every class under that package); see
                // CallGraphService.expandRoots.
                if (!(o instanceof Map<?, ?> m) || !(m.get("className") instanceof String cn) || cn.isBlank()) {
                    throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "each entry requires className");
                }
                entries.add((Map<String, Object>) m);
            }
        }

        log.info("Upload received: {} ({} MB), mode={}{} — submitting to queue", jarName, file.getSize() / (1024 * 1024), mode,
                entries != null ? ", targeted=" + entries.size() + " entries" : "");

        // Save JAR/WAR to temp file immediately so the HTTP request can return
        Path tempJar = Files.createTempFile("jar-analyze-", jarName.endsWith(".war") ? ".war" : ".jar");
        file.transferTo(tempJar);

        // Save optional per-JAR domain config to temp file alongside the JAR
        Path tempConfig = null;
        if (configFile != null && !configFile.isEmpty()) {
            tempConfig = Files.createTempFile("jar-domain-config-", ".json");
            configFile.transferTo(tempConfig);
            log.info("Per-JAR domain config attached: {} bytes", configFile.getSize());
        }

        if (entries != null) {
            String jarBase = jarName.endsWith(".war") ? jarName.substring(0, jarName.length() - 4)
                    : jarName.endsWith(".jar") ? jarName.substring(0, jarName.length() - 4) : jarName;
            String targetedJarName = (targetedName != null && !targetedName.isBlank() ? targetedName.trim() : jarBase) + "_targeted.jar";
            String id = JarNameUtil.normalizeKey(targetedJarName);

            Map<String, Object> metadata = new LinkedHashMap<>();
            metadata.put("jarName", targetedJarName);
            metadata.put("tempFilePath", tempJar.toString());
            metadata.put("fileSize", file.getSize());
            metadata.put("mode", mode);
            metadata.put("entries", entries);

            // JAR_TARGETED has no perJarConfigPath support (see JarTargetedAnalysisExecutor —
            // only JarAnalysisExecutor reads that key). Without this cleanup the uploaded config
            // would be written to temp, silently ignored, and never deleted: one orphaned file
            // per targeted upload.
            if (tempConfig != null) {
                Files.deleteIfExists(tempConfig);
                log.warn("Per-JAR domain config ignored for targeted analysis (not supported) — discarded {}",
                        tempConfig);
            }

            QueueJob job = queueService.submit(QueueJob.Type.JAR_TARGETED, "Targeted: " + targetedJarName, metadata);

            Map<String, Object> response = new LinkedHashMap<>();
            response.put("status", "queued");
            response.put("jobId", job.id);
            response.put("jarName", targetedJarName);
            response.put("id", id);
            response.put("entryCount", entries.size());
            return response;
        }

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
        if (tempConfig != null) {
            metadata.put("perJarConfigPath", tempConfig.toString());
        }

        QueueJob job = queueService.submit(QueueJob.Type.JAR_UPLOAD, "Upload: " + jarName, metadata);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "queued");
        response.put("jobId", job.id);
        response.put("jarName", jarName);
        response.put("jarSize", file.getSize());
        response.put("mode", mode);
        response.put("hasPerJarConfig", tempConfig != null);
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

    /** Get the per-JAR domain config snapshot, falling back to the global config if no snapshot exists. */
    @GetMapping("/{id}/domain-config")
    public ResponseEntity<String> getDomainConfig(@PathVariable String id) throws IOException {
        ArtifactKey configKey = paths.domainConfigKey(id);
        if (store.exists(configKey)) {
            return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(readString(configKey));
        }
        // Fall back to the global config from the config directory or classpath
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
                // Try parent paths (mirror DomainConfigLoader resolution)
                Path parent = dir.getParent();
                if (parent != null) {
                    Path up = parent.getParent();
                    if (up != null) {
                        Path upFile = up.resolve(configDirProp).resolve("domain-config.json");
                        if (Files.exists(upFile)) return Files.readString(upFile);
                    }
                }
            }
            // Classpath fallback
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

    /**
     * Targeted "developer analysis": build COMPLETE call trees for ONLY the supplied entry
     * methods (not the whole project). Persists under a separate "_targeted" id so it lists
     * alongside — never overwrites — a whole-project analysis of the same JAR.
     */
    @SuppressWarnings("unchecked")
    @PostMapping("/analyze-targeted")
    public Map<String, Object> analyzeTargeted(@RequestBody Map<String, Object> body) throws IOException {
        String localPath = (String) body.get("path");
        String mode = (String) body.getOrDefault("mode", "static");
        String name = (String) body.get("name");

        if (localPath == null || localPath.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "path is required");
        }
        Path source = Path.of(localPath.trim());
        if (!Files.isRegularFile(source) || !source.toString().endsWith(".jar")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "File not found or not a .jar: " + localPath);
        }

        Object rawEntries = body.get("entries");
        if (!(rawEntries instanceof List<?> entriesList) || entriesList.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "entries must be a non-empty array");
        }
        List<Map<String, Object>> entries = new ArrayList<>();
        for (Object o : entriesList) {
            // methodName may be blank — a bare class or package name expands to every method
            // on that class (or every class under that package) once the JAR is indexed; see
            // CallGraphService.expandRoots.
            if (!(o instanceof Map<?, ?> m) || !(m.get("className") instanceof String cn) || cn.isBlank()) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "each entry requires className");
            }
            entries.add((Map<String, Object>) m);
        }

        String fileName = source.getFileName().toString();
        String jarBase = fileName.endsWith(".jar") ? fileName.substring(0, fileName.length() - 4) : fileName;
        // "_targeted" suffix keeps the id distinct from a whole-project analysis of the same JAR.
        String targetedJarName = (name != null && !name.isBlank() ? name.trim() : jarBase) + "_targeted.jar";
        String id = JarNameUtil.normalizeKey(targetedJarName);

        long fileSize = Files.size(source);
        Path tempJar = Files.createTempFile("jar-targeted-", ".jar");
        Files.copy(source, tempJar, java.nio.file.StandardCopyOption.REPLACE_EXISTING);

        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("jarName", targetedJarName);
        metadata.put("tempFilePath", tempJar.toString());
        metadata.put("fileSize", fileSize);
        metadata.put("mode", mode);
        metadata.put("entries", entries);

        QueueJob job = queueService.submit(QueueJob.Type.JAR_TARGETED, "Targeted: " + targetedJarName, metadata);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "queued");
        response.put("jobId", job.id);
        response.put("jarName", targetedJarName);
        response.put("id", id);
        response.put("entryCount", entries.size());
        return response;
    }

    @GetMapping
    public List<Map<String, Object>> listJars() {
        // Strict: a store failure must surface as an error (mapped by ApiExceptionAdvice), never as
        // a 200 [] that the screen renders as "no analyses".
        List<Map<String, Object>> jars = persistenceService.listJarsStrict();
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

    /**
     * The whole analysis document — MID-015.
     *
     * <p>Even streamed, this still passes through {@code classes} and {@code endpoints}, which is
     * most of the document; a real {@code analysis.json} reaches 912 MB. Checked across every
     * client script in this repository: <b>nothing calls this endpoint</b>. Every screen has moved
     * to a narrower one — {@code /summary/header} for the header, {@code /summary/endpoints} and
     * friends for the rest — and no test exercises it either.
     *
     * <p>Not deleted, because an operator's own script outside this repository may still call it.
     * Gated instead: the body is served only with {@code full=true}, which is loud in a way a
     * silent 912 MB response is not. Anyone hitting the default path gets a 410 naming the
     * replacement, the same pattern MID-004 used for {@code /call-graph}.
     */
    @GetMapping("/{id}")
    public void getAnalysis(@PathVariable String id,
                            @RequestParam(value = "version", required = false) String version,
                            @RequestParam(value = "full", required = false) Boolean full,
                            jakarta.servlet.http.HttpServletResponse response) throws IOException {
        if (full == null || !full) {
            response.setStatus(jakarta.servlet.http.HttpServletResponse.SC_GONE);
            response.setContentType("application/json");
            objectMapper.writeValue(response.getOutputStream(), Map.of(
                    "error", "GET /{id} no longer serves the whole analysis body by default.",
                    "replacements", Map.of(
                            "header", "/api/jar/jars/" + id + "/summary/header",
                            "endpoints", "/api/jar/jars/" + id + "/summary/endpoints",
                            "domainConfig", "/api/jar/jars/" + id + "/domain-config"),
                    "escapeHatch", "add ?full=true to get the previous, unbounded response"));
            return;
        }
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
     * One endpoint's call tree — MID-016: {@code dag} is now the default.
     *
     * <p>Measured on endpoint 271 of this very jar: the expanded tree is <b>300,167,238</b>
     * nodes — 3.5 minutes and 120 MB even with {@code CallTreeInflater}'s always-on 200,000-node
     * cap, which keeps 0.07% of the answer. The DAG for the same endpoint is <b>2,282</b> nodes
     * and 7.4 MB, proportional to distinct methods rather than to paths through them.
     *
     * <p>The default used to be the expanded tree, kept for backward compatibility. Checked before
     * flipping it: the client ({@code jar/js/api.js}) already requests {@code format=dag} on every
     * call and only falls back to a second, unqualified request for documents written before the
     * flat format existed. Flipping the bare default without changing anything else would have
     * broken exactly that fallback — a legacy document would get the (now-default) DAG stream,
     * which {@code streamCallTreeDag} answers with {@code null} for any document with no node
     * dictionary, and the client had no third request to fall back to. The fallback call was
     * updated to ask for {@code format=full} explicitly, in the same change, so it keeps meaning
     * what it always meant.
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
        boolean dag = !"full".equalsIgnoreCase(format);
        try (var parser = objectMapper.getFactory().createParser(provider.openStream());
             var gen = objectMapper.getFactory().createGenerator(response.getOutputStream())) {
            if (dag) provider.streamCallTreeDag(parser, gen, idx);
            else provider.streamCallTree(parser, gen, idx);
            gen.flush();
        }
    }

    /**
     * Q43 / L4 — the Collection popup's call paths, from one pass over {@code endpoints[]}.
     *
     * <p>The popup used to fetch {@code /endpoints/by-index/{idx}/call-tree} once per endpoint
     * that touches the collection: 15 requests and 114 411 B on the worst measured cell
     * ({@code ENTERPRISE.ORDERS} / {@code READ} on {@code web-1.0.0-SNAPSHOT}), to draw 15 rows.
     * Worse than the bytes, {@code streamCallTree} seeks the array to one index, so those were 15
     * separate parses of a 3.9 MB document. This is one parse and one response.
     *
     * <p>{@code collection} is a query parameter rather than a path variable because collection
     * names are qualified with a dot ({@code ENTERPRISE.ORDERS}) and a schema-qualified name in a
     * path segment is exactly the shape that a suffix-pattern or an encoded-slash rule silently
     * truncates.
     *
     * <p>Absent {@code limit}/{@code after}/{@code sort}/{@code q} and the envelope media type,
     * the response is the bare row array — §3.5 rule 1, so the endpoint is adoptable and
     * revertible on its own. No {@code endpoints} array at all is a 204: an empty array would be
     * indistinguishable from "this collection has no call paths", which is a different sentence
     * for the popup to write.
     *
     * @param op the operation the popup was opened on. Not a closed enum — the values come from
     *           {@code domain-config.json}'s {@code operation_types}, which is per-deployment
     *           config — but blank is a 400 rather than "every operation", because a popup that
     *           forgot its parameter would otherwise show a plausible, wrong table.
     */
    @GetMapping("/{id}/collections/call-paths")
    public void getCollectionCallPaths(@PathVariable String id,
                                       @RequestParam("collection") String collection,
                                       @RequestParam("op") String op,
                                       @RequestParam(value = "version", required = false) String version,
                                       @RequestParam(value = "limit", required = false) Integer limit,
                                       @RequestParam(value = "after", required = false) String after,
                                       @RequestParam(value = "sort", required = false) String sort,
                                       @RequestParam(value = "q", required = false) String q,
                                       @RequestHeader(value = "Accept", required = false) String accept,
                                       jakarta.servlet.http.HttpServletResponse response)
            throws IOException {
        AnalysisDataProvider provider = resolveProvider(id, version);
        response.setContentType("application/json");
        response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");

        var params = new com.plsqlanalyzer.web.page.FacetPageResponder.Params(
                limit, after, sort, q, null, null, accept);
        ResponseEntity<com.fasterxml.jackson.databind.JsonNode> entity;
        if (collection == null || collection.isBlank() || op == null || op.isBlank()) {
            entity = com.plsqlanalyzer.web.page.FacetPageResponder.badRequest(objectMapper,
                    "Both 'collection' and 'op' are required");
        } else {
            try (var parser = objectMapper.getFactory().createParser(provider.openStream())) {
                CollectionPathRows rows = CollectionPathRows.open(parser, objectMapper,
                        collection, op, domainConfig::inferOperationType);
                if (!rows.present()) {
                    entity = ResponseEntity.noContent().build();
                } else {
                    // MID-021 (§102): the unpaged `wantsEnvelope()==false` branch this used to
                    // have is gone — its only real caller (`JA.api.getCollectionCallPaths`) always
                    // sends the `application/vnd.analyzer.page+json` Accept header, so that branch
                    // was never reachable in practice. Confirmed by grepping every client caller,
                    // not assumed.
                    entity = facets.respondStreaming(rows, JarFacetSpecs.COLLECTION_PATHS,
                            params, null);
                }
            } catch (IllegalArgumentException | Cursor.InvalidCursorException e) {
                entity = com.plsqlanalyzer.web.page.FacetPageResponder.badRequest(objectMapper,
                        e.getMessage());
            }
        }
        response.setStatus(entity.getStatusCode().value());
        if (entity.getBody() != null) {
            objectMapper.writeValue(response.getOutputStream(), entity.getBody());
        }
    }

    // ==================== Lazy-Loading Endpoints ====================

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

        // Serve from pre-gzipped cache if client accepts gzip
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

    /**
     * Q21 + Q22 — the header bar, in one request of a few hundred bytes.
     *
     * <p>PLAN-03 §1.2 I2: opening a JAR fetched {@code /summary} (429 782 B measured on
     * {@code web-1.0.0-SNAPSHOT}) — or {@code /summary/headers} (120 272 B) after the earlier
     * mitigation — because the eleven scalars in the title bar were fields of the same object as
     * the endpoint list and the class index. The version flags are folded in rather than left to
     * {@code /versions}, because the bar cannot be drawn without both and two requests to draw one
     * widget is the shape PLAN-08 Q22 asks to remove: "one request, not two".
     *
     * <p><b>ETag.</b> The summary cache's last-modified time. A monotonic value already in the
     * store, so deciding not to send the body costs one metadata call and no reading — never a hash
     * of the body, which would mean producing the body in order to decide not to send it.
     */
    @GetMapping("/{id}/summary/header")
    public void getSummaryHeader(@PathVariable String id,
                                 @RequestParam(value = "version", required = false) String version,
                                 jakarta.servlet.http.HttpServletRequest request,
                                 jakarta.servlet.http.HttpServletResponse response)
            throws IOException {
        AnalysisDataProvider provider = resolveProvider(id, version);

        // "h2": the ETag generation is part of the response contract. The header gained `stats`
        // (PLAN-08 §6.1 #7) after "h1" ETags were already in browser caches, and a client holding
        // one would have been answered 304 and rendered a header with no stats in it — a
        // conditional request is only safe if the tag changes when the shape does.
        String etag = "\"h2" + provider.lastModifiedMillis() + '-'
                + (version == null ? "" : version) + "\"";
        response.setHeader("ETag", etag);
        response.setHeader("Cache-Control", "no-cache");
        if (etag.equals(request.getHeader("If-None-Match"))) {
            response.setStatus(jakarta.servlet.http.HttpServletResponse.SC_NOT_MODIFIED);
            return;
        }
        response.setContentType("application/json");

        // The best-available header (no explicit version) is precomputed into
        // views/summary-header.json at the first request that builds it, so a warm header costs a
        // few hundred bytes instead of a full pass over the (up to hundreds of MB) document. A
        // version-explicit request keeps streaming the provider, since each version's header
        // differs. Absent/stale falls straight through to the legacy build below.
        boolean cacheable = version == null;
        if (cacheable) {
            byte[] cached = readJarView(provider, paths.summaryHeaderViewKey(id));
            if (cached != null) {
                response.getOutputStream().write(cached);
                return;
            }
        }

        // Buffered, unlike the streamed endpoints below, because the version flags have to be
        // merged into the same object and the whole thing is a few hundred bytes by construction.
        java.io.ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream(1024);
        try (var parser = objectMapper.getFactory().createParser(provider.openStream());
             var gen = objectMapper.getFactory().createGenerator(buf)) {
            provider.streamSummaryHeader(parser, gen);
            gen.flush();
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> header = objectMapper.readValue(buf.toByteArray(), Map.class);
        // Q22: hasCorrected / hasCorrectedPrev / claudeIteration, from key existence — never a
        // scan, and never a second round trip for the client.
        header.putAll(persistenceService.getVersionInfo(id));
        // PLAN-08 §6.1 #7: the ten Summary chips, so the browser stops deriving them from the
        // whole endpoint list. Absent (not zero) when they cannot be computed — the client falls
        // back to computing them, which is the only answer that cannot show a wrong number.
        Map<String, Long> stats = summaryStats(provider);
        if (stats != null) header.put("stats", stats);
        byte[] payload = objectMapper.writeValueAsBytes(header);
        if (cacheable) {
            writeJarView(paths.summaryHeaderViewKey(id), payload);
        }
        response.getOutputStream().write(payload);
    }

    /**
     * Q21b — the endpoint list, paged, searched and sorted server-side.
     *
     * <p><b>Absent parameters return the whole array</b>, byte-identical to the {@code endpoints}
     * field of {@code /summary/headers}. That is PLAN-08 §3.5 rule 1, and it is what makes this
     * endpoint adoptable one screen at a time: a caller that sends nothing new sees nothing new.
     *
     * <p>Any of {@code limit}, {@code after} or {@code sort} switches on the {@code {items, page}}
     * envelope. {@code q} moves the search to the server in the same commit as the paging, because
     * once only one page is in memory a browser-side filter silently searches that page alone
     * (PLAN-08 §4.1).
     *
     * <p>Items carry {@code endpointId} — the stable natural key the summary slices are merged on.
     * A page of endpoints joined to a slice by array position would land the external calls of one
     * endpoint under another; see {@code AbstractAnalysisDataProvider.endpointId}.
     *
     * @param sort only {@code ORDINAL} — the analysis order the endpoint list has always used. A
     *             closed set, not a column name: an unknown value is a 400, never a silently
     *             different ordering.
     */
    @GetMapping("/{id}/summary/endpoints")
    public void getSummaryEndpoints(@PathVariable String id,
                                    @RequestParam(value = "version", required = false) String version,
                                    @RequestParam(required = false) Integer limit,
                                    @RequestParam(required = false) String after,
                                    @RequestParam(required = false) String sort,
                                    @RequestParam(required = false) String q,
                                    jakarta.servlet.http.HttpServletRequest request,
                                    jakarta.servlet.http.HttpServletResponse response)
            throws IOException {
        AnalysisDataProvider provider = resolveProvider(id, version);
        response.setContentType("application/json");
        response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");

        PageRequest req;
        try {
            req = PageRequest.of(limit, after, sort, q, ENDPOINT_SORTS, SORT_ORDINAL, 25);
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage());
        } catch (Cursor.InvalidCursorException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage());
        }

        String afterId = req.after() == null ? null : req.after().tiebreaker();

        // MID-021 (§102): the unpaged `legacy` branch this used to have is gone — its only real
        // caller (`JA.api.endpointListPages`, via `PA.pageSource`) always sends `limit` on every
        // request (`fetchPage` sets it unconditionally), so `req.isLegacy()` was never true in
        // practice. Confirmed by grepping every client caller, not assumed; `getSummaryEndpoints`
        // itself (the bare-call API wrapper this endpoint was named for) has no callers left at all.

        // Envelope. The items are streamed into a buffer only because `page` must follow them and
        // its `nextCursor` is not known until the last item has been written — and a page is
        // bounded by `limit`, so this cannot become the whole-document allocation the endpoint
        // exists to avoid.
        java.io.ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream(64 * 1024);
        AbstractAnalysisDataProvider.EndpointPageMark mark;
        try (var parser = objectMapper.getFactory().createParser(provider.openStream());
             var gen = objectMapper.getFactory().createGenerator(buf)) {
            mark = provider.streamSummaryEndpointPage(parser, gen, afterId, req.limit(),
                                                      req.query());
            gen.flush();
        }

        if (!mark.cursorResolved()) {
            // The endpoint the cursor pointed at is gone: the analysis was re-run under the reader.
            // Offset paging would have answered from the new run at the old position and silently
            // skipped or duplicated rows. PLAN-08 §3.4 — the client resets to page 1 with a toast.
            response.setStatus(HttpStatus.CONFLICT.value());
            objectMapper.writeValue(response.getOutputStream(),
                    Map.of("reason", "RESULT_SET_REPLACED"));
            return;
        }

        try (var gen = objectMapper.getFactory().createGenerator(response.getOutputStream())) {
            gen.writeStartObject();
            gen.writeFieldName("items");
            gen.writeRawValue(buf.toString(java.nio.charset.StandardCharsets.UTF_8));
            gen.writeFieldName("page");
            gen.writeStartObject();
            gen.writeStringField("nextCursor", mark.hasMore()
                    ? Cursor.of(req.sort(), List.of(), mark.lastId()).encode() : null);
            gen.writeBooleanField("hasMore", mark.hasMore());
            // Exact, and free: the pass over the endpoint array counted the matches on its way
            // through. This is the case PLAN-07 §2.2 allows -1 for and does not need it.
            gen.writeNumberField("total", mark.matched());
            gen.writeBooleanField("totalIsExact", true);
            gen.writeNumberField("limit", req.limit());
            gen.writeStringField("sort", req.sort());
            gen.writeEndObject();
            gen.writeEndObject();
            gen.flush();
        }
    }

    /**
     * UI-026f — every endpoint's data-access report, plus the five cross-endpoint aggregations,
     * precomputed server-side. Replaces {@code jar/js/summary.js}'s {@code _render}/{@code
     * processBatch} loop, which called {@code getProcDetail} once per procedure from the browser
     * behind a progress bar to build the same shape ({@code epReports}/{@code vertReport}/
     * {@code viewsReport}/{@code distReport}/{@code batchReport}/{@code extReport}) that
     * {@link EndpointReportBuilder}/{@link EndpointCrossReportBuilder} now build here in one
     * streaming pass over the summary cache.
     *
     * <p><b>Requires the summary cache to already exist</b> (built by an earlier {@code /summary}
     * or {@code /summary/header} call) rather than generating it inline — the same reasoning
     * {@link #summaryStats} already documents for the header chips, sharpened: this endpoint does
     * a full per-endpoint analysis pass, not ten aggregate counts, so building the cache as a side
     * effect here would hide a 912 MB-scale cost inside what looks like a report fetch. {@code 425}
     * (not 404 — the analysis itself is known, just not yet summarised) tells the client to call
     * {@code /summary} first, which every screen that would call this endpoint already does on
     * JAR-open.
     */
    @GetMapping("/{id}/summary/reports")
    public void getSummaryReports(@PathVariable String id,
                                  @RequestParam(value = "version", required = false) String version,
                                  jakarta.servlet.http.HttpServletResponse response) throws IOException {
        AnalysisDataProvider provider = resolveProvider(id, version);
        ArtifactKey summaryCache = summaryCacheKey(provider);
        if (!store.exists(summaryCache) || !isSummaryCacheValid(summaryCache)) {
            response.sendError(425, "Summary cache not built yet — call /summary first");
            return;
        }
        response.setContentType("application/json");
        response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");

        SummaryStats.Config cfg = domainConfig.summaryStatsConfig();
        Map<String, Object> rawConfig = domainConfig.rawConfig();
        var complexityRules = com.jaranalyzer.service.EndpointCrossReportBuilder.ComplexityRules
                .fromDomainConfig(rawConfig);
        @SuppressWarnings("unchecked")
        Map<String, Object> sizeThresholds = rawConfig.get("size_thresholds") instanceof Map<?, ?> m
                ? (Map<String, Object>) m : Map.of();
        @SuppressWarnings("unchecked")
        List<Object> perfThresholds = rawConfig.get("performance_thresholds") instanceof List<?> l
                ? (List<Object>) l : List.of();

        com.fasterxml.jackson.databind.node.ObjectNode all;
        try (InputStream in = openRead(summaryCache);
             var parser = objectMapper.getFactory().createParser(in)) {
            all = com.jaranalyzer.service.EndpointReportsService.buildAll(
                    parser, objectMapper, cfg, complexityRules, sizeThresholds, perfThresholds);
        }
        objectMapper.writeValue(response.getOutputStream(), all);
    }

    /**
     * PLAN-08 §6.1 #7 — the ten Summary headline aggregates, memoised beside the summary cache.
     *
     * <p><b>Why it reads the summary cache and not {@code analysis.json}.</b> The numbers have to
     * be identical to the ones the browser computes, and the browser computes them from the
     * document {@code /summary/headers} returns — which is the summary cache, with
     * {@code callTree} stripped and {@code scheduledCount} present. {@code analysis.json} has
     * neither property: it would select the other branch of {@code _analyzeEndpoint} and report
     * {@code scheduled: 0} for every analysis. Same fields, different answer.
     *
     * <p><b>Why it is memoised.</b> Computing them is one streaming pass over a 430 KB document
     * (measured, {@code web-1.0.0-SNAPSHOT}), which is cheap but not free, and this endpoint is on
     * the JAR-click path. The sidecar is keyed by nothing and validated by mtime: it is stale the
     * moment the summary cache is newer, and a stale or unreadable sidecar is recomputed rather
     * than trusted. A wrong-but-fast chip is worse than a slow one.
     *
     * <p><b>Why it can return null.</b> If the summary cache does not exist yet, this endpoint does
     * not build it — that is a full pass over the analysis document (up to 912 MB in a live run)
     * and the header exists to be the small, fast request. The client keeps its own computation as
     * the fallback, so an absent {@code stats} degrades to today's behaviour exactly.
     */
    private Map<String, Long> summaryStats(AnalysisDataProvider provider) {
        ArtifactKey summaryCache;
        try {
            summaryCache = summaryCacheKey(provider);
        } catch (ResponseStatusException e) {
            return null;
        }
        if (!store.exists(summaryCache) || !isSummaryCacheValid(summaryCache)) return null;

        ArtifactKey sidecar = summaryCache.parent().resolve("_summary_stats.json");
        long cacheStamp = store.lastModified(summaryCache);
        if (store.exists(sidecar) && store.lastModified(sidecar) >= cacheStamp) {
            try (InputStream in = openRead(sidecar)) {
                Map<String, Long> cached = toLongMap(objectMapper.readValue(in, Map.class));
                if (!cached.isEmpty()) return cached;
            } catch (IOException | IllegalArgumentException | ClassCastException e) {
                log.debug("Summary stats sidecar unreadable, recomputing: {}", e.getMessage());
            }
        }

        Map<String, Long> chips;
        try (var parser = objectMapper.getFactory().createParser(openRead(summaryCache))) {
            chips = com.jaranalyzer.service.SummaryStats
                    .compute(parser, domainConfig.summaryStatsConfig(), objectMapper)
                    .asChipMap();
        } catch (IOException e) {
            log.debug("Summary stats computation failed for {}: {}", summaryCache, e.getMessage());
            return null;
        }
        try {
            byte[] body = objectMapper.writeValueAsBytes(chips);
            try (OutputStream out = openWrite(sidecar, body.length)) {
                out.write(body);
            }
        } catch (IOException e) {
            log.debug("Failed to write summary stats sidecar: {}", e.getMessage());
        }
        return chips;
    }

    /** Jackson gives Integer for small values; the chips are counts and LOC, so widen them all. */
    private static Map<String, Long> toLongMap(Map<?, ?> raw) {
        Map<String, Long> out = new java.util.LinkedHashMap<>();
        for (Map.Entry<?, ?> e : raw.entrySet()) {
            if (!(e.getValue() instanceof Number n)) throw new IllegalArgumentException("not a number");
            out.put(String.valueOf(e.getKey()), n.longValue());
        }
        return out;
    }

    /** The only ordering the endpoint list has ever had: the order the analysis wrote them in. */
    private static final String SORT_ORDINAL = "ORDINAL";
    private static final java.util.Set<String> ENDPOINT_SORTS = java.util.Set.of(SORT_ORDINAL);

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
        ArtifactKey summaryCache = summaryCacheKey(provider);
        if (store.exists(summaryCache) && !isSummaryCacheValid(summaryCache)) {
            log.warn("Summary cache {} appears corrupt (partial write from crash) — deleting and regenerating", summaryCache.leaf());
            store.delete(summaryCache);
            store.delete(gzOf(summaryCache));
        }
        if (!store.exists(summaryCache)) {
            // Streamed, never buffered — the summary is far too large for a ByteArrayOutputStream.
            // The old write-to-.tmp-then-ATOMIC_MOVE dance is gone: ArtifactStore.write() is
            // already atomic on close, so a reader sees either the old object or the complete new
            // one, and an abandoned stream leaves the previous object intact.
            try (var parser = objectMapper.getFactory().createParser(provider.openStream());
                 var out = new java.io.BufferedOutputStream(store.write(summaryCache), 128 * 1024);
                 var gen = objectMapper.getFactory().createGenerator(out)) {
                provider.streamSummary(parser, gen);
                gen.flush();
            }
            // Async gzip — don't block the response on compression. The gzip runs on the common
            // pool, which has no request context, so capture the routing scope HERE (on the request
            // thread, before submitting) and re-install it inside the task — otherwise the summary
            // write routes to the shared default target (a silent wrong-bucket write).
            ArtifactKey gzCache = gzOf(summaryCache);
            RequestScopedArtifactStore.Scope scope = store instanceof RequestScopedArtifactStore routed
                    ? routed.captureScope()
                    : RequestScopedArtifactStore.Scope.NONE;
            java.util.concurrent.CompletableFuture.runAsync(() -> {
                try (AutoCloseable routed = scope.install();
                     var in = new java.io.BufferedInputStream(openRead(summaryCache), 128 * 1024);
                     var gzOut = new java.util.zip.GZIPOutputStream(
                             new java.io.BufferedOutputStream(store.write(gzCache), 64 * 1024))) {
                    in.transferTo(gzOut);
                } catch (Exception e) {
                    // Was silently swallowed; log at WARN with the cache key so a failed gzip
                    // regeneration is visible (the summary itself is already written).
                    log.warn("Async gzip of summary cache {} failed: {}", gzCache.leaf(), e.getMessage());
                }
            });
        }

        // Read from cached summary and extract the slice
        try (var parser = objectMapper.getFactory().createParser(openRead(summaryCache));
             var gen = objectMapper.getFactory().createGenerator(response.getOutputStream())) {
            provider.streamSummarySlice(parser, gen, mode, fields);
            gen.flush();
        }
    }

    /**
     * Code Structure tree — L1 / Q40.
     *
     * <p>Legacy path unchanged: no paging parameter, and this streams the whole projected tree out
     * of {@code analysis.json} exactly as before. With a paging parameter it answers a page,
     * ordered server-side by source JAR then package then class name — the order the tree already
     * groups by, so a page boundary falls between packages rather than through one — with the
     * filter box's text moved server-side in the same request.
     *
     * <p>The paged rows also drop {@code methodSummaries}, which is 475 KB of the 656 KB measured
     * for 119 classes and which <b>no reader consumes</b>: the tree renderer looks for
     * {@code cls.methods}, a field this response has never had. So the page is 72% smaller and
     * renders identically.
     *
     * <p>Pages are served from {@code _class-tree.json}, written at save time (plan 2b) and split
     * into {@code _class-tree/<n>.json} chunks (plan 2d) which the serving path prefers. Without it,
     * page 2 would re-stream the whole multi-megabyte {@code analysis.json} to reach the cursor. The
     * lazy build remains the fallback for analyses written before save-time precompute. The cache is
     * invalidated on re-analysis by {@code PersistenceService.invalidateSummaryCache}; a cursor
     * survives the rebuild anyway because it is keyed on the class's package and name rather than on
     * an array position.
     */
    /**
     * UI-003 (§134) — the Code Structure tab's package view no longer needs to fetch every class
     * to render its own top level. §118 found the tree itself has nothing to aggregate (a folder
     * tree of every class is a whole-corpus read by definition) — but the *package list* (name,
     * source JAR, class count) genuinely is an aggregate, one row per package rather than one per
     * class, and this streams it the same way `repository.rows` already streams the full tree:
     * one pass, tallying counts, never holding more than the distinct-package count in memory
     * (always far smaller than the class count it replaces at the tree's default, most-used view).
     *
     * <p>A package's own classes are then fetched only when it is expanded, via the existing
     * {@code /classes/tree} endpoint's new {@code cf=0:<packageName>|1:<sourceJar>} filter — no new
     * per-class endpoint needed.
     */
    @GetMapping("/{id}/classes/packages")
    public ResponseEntity<JsonNode> getClassPackages(
            @PathVariable String id,
            @RequestParam(value = "version", required = false) String version) throws IOException {
        // LinkedHashMap: package/JAR groups render in the order they are first seen, which is the
        // same PACKAGE-sorted order the underlying stream already delivers rows in — no re-sort
        // needed to reproduce today's grouping.
        Map<String, long[]> counts = new LinkedHashMap<>();
        Map<String, String[]> keys = new LinkedHashMap<>();
        try (var rows = repository.rows(
                com.analyzer.storage.repo.AnalysisQuery.ofJar(
                        com.analyzer.storage.repo.AnalysisQuery.Dataset.JAR_CLASS_TREE, id, version),
                row -> row)) {
            while (rows.hasNext()) {
                JsonNode row = rows.next();
                String pkg = row.path("packageName").asText("");
                String jar = row.path("sourceJar").asText("");
                String key = jar + ' ' + pkg;
                counts.computeIfAbsent(key, k -> new long[1])[0]++;
                keys.putIfAbsent(key, new String[]{pkg, jar});
            }
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
        com.fasterxml.jackson.databind.node.ArrayNode out = objectMapper.createArrayNode();
        for (var e : counts.entrySet()) {
            String[] k = keys.get(e.getKey());
            com.fasterxml.jackson.databind.node.ObjectNode row = objectMapper.createObjectNode();
            row.put("packageName", k[0]);
            row.put("sourceJar", k[1]);
            row.put("classCount", e.getValue()[0]);
            out.add(row);
        }
        return ResponseEntity.ok(out);
    }

    @GetMapping("/{id}/classes/tree")
    public void getClassTree(@PathVariable String id,
                             @RequestParam(value = "version", required = false) String version,
                             @RequestParam(value = "limit", required = false) Integer limit,
                             @RequestParam(value = "after", required = false) String after,
                             @RequestParam(value = "sort", required = false) String sort,
                             @RequestParam(value = "q", required = false) String q,
                             /**
                              * Column filters — {@code cf=0:<packageName>|1:<sourceJar>} — the
                              * per-package lazy-expand path (`codeTree.js`'s `_fetchPackageClasses`)
                              * depends on entirely: without this parameter, every package request
                              * silently returned the same unfiltered first page (found 2026-09-11,
                              * TASK-REGISTRY §194/§195 — the filter columns themselves
                              * ({@code JarFacetSpecs.CLASS_TREE}) were fully built and wired for
                              * §134, but this endpoint never declared the parameter to receive them,
                              * so every request built its {@code Params} with {@code cf} hardcoded
                              * to {@code null} regardless of what the client sent).
                              */
                             @RequestParam(value = "cf", required = false) java.util.List<String> cf,
                             /**
                              * Opt-in lean tree. members=false omits the methodSummaries and fields
                              * arrays — 93.8% of this response (8.2 MB + 3.6 MB of 11.8 MB for 1,623
                              * classes) — while keeping methodCount/fieldCount so the rows still show
                              * correct numbers. The tree paints class rows only; members are fetched
                              * per class from /classes/by-index/{idx} on expand.
                              *
                              * Defaults to FALSE.
                              *
                              * <p>It defaulted to true so existing callers were byte-identical. That
                              * protected callers at the cost of making the expensive answer the one
                              * you get by forgetting — and two callers did forget:
                              * `jar/js/api.js:classTreePages`, the paged path the Code Structure tab
                              * actually uses, and the entire WAR fork. The `members=false` fix had
                              * been applied only to the unpaged fallback.
                              *
                              * <p>Safe because the tree paints class rows only: `_renderClassBody`
                              * renders whatever members are present and `_loadClassMembers` fetches
                              * them per class on expand when they are not.
                              */
                             @RequestParam(value = "members", required = false, defaultValue = "false")
                             boolean members,
                             @RequestHeader(value = "Accept", required = false) String accept,
                             jakarta.servlet.http.HttpServletResponse response) throws IOException {
        AnalysisDataProvider provider = resolveProvider(id, version);
        response.setContentType("application/json");
        response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");

        var params = new com.plsqlanalyzer.web.page.FacetPageResponder.Params(
                limit, after, sort, q, null, cf, accept);
        if (!params.wantsEnvelope()) {
            try (var parser = objectMapper.getFactory().createParser(provider.openStream());
                 var gen = objectMapper.getFactory().createGenerator(response.getOutputStream())) {
                provider.streamClassTree(parser, gen, members);
                gen.flush();
            }
            return;
        }

        // The cache build and its validity check moved into ArtifactScanRepository: they are how
        // *that* implementation avoids re-streaming analysis.json to reach page 2, and an indexed
        // implementation needs neither. What is left here is the request-shaped part.
        ResponseEntity<com.fasterxml.jackson.databind.JsonNode> entity;
        try (var rows = repository.rows(
                com.analyzer.storage.repo.AnalysisQuery.ofJar(
                        com.analyzer.storage.repo.AnalysisQuery.Dataset.JAR_CLASS_TREE, id, version),
                JarFacetSpecs::classRow)) {
            entity = facets.respondStreaming(rows, JarFacetSpecs.CLASS_TREE, params, null);
        } catch (IllegalArgumentException | Cursor.InvalidCursorException e) {
            entity = com.plsqlanalyzer.web.page.FacetPageResponder.badRequest(objectMapper,
                    e.getMessage());
        }
        response.setStatus(entity.getStatusCode().value());
        objectMapper.writeValue(response.getOutputStream(), entity.getBody());
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
     * Global Search — the JAR-side mirror of {@code ParserAnalysisController.getSearch}. Named
     * {@code global-search} rather than {@code search} for the same reason PL/SQL's is: it does
     * not collide with anything today, but the name is kept in lockstep across both analyzers so
     * {@code jar/js/search.js} and {@code plsql/js/search.js} can share one mental model.
     *
     * <p>Normalizes three real, independently-sourced facets into one {@code {type, name, schema,
     * matchedField, _idx, raw}} shape — classes ({@link com.analyzer.storage.repo.AnalysisQuery.Dataset#JAR_CLASS_TREE}),
     * dependency JARs ({@link com.analyzer.storage.repo.AnalysisQuery.Dataset#JAR_BUNDLED_JARS}),
     * and REST endpoints (via the existing {@code streamSummaryEndpointPage} scan) — then hands the
     * merged row array to the same {@link com.plsqlanalyzer.web.page.FacetPageResponder}/{@link
     * JarFacetSpecs#SEARCH} paging contract every other JAR facet already uses, so {@code cf}
     * (checkbox/contains/regex) and pagination come for free rather than being reimplemented.
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
            return facets.respond(rows, null, JarFacetSpecs.SEARCH,
                    new com.plsqlanalyzer.web.page.FacetPageResponder.Params(
                            limit, after, sort, null, null, cf, accept),
                    null);
        } catch (IllegalArgumentException | Cursor.InvalidCursorException e) {
            return com.plsqlanalyzer.web.page.FacetPageResponder.badRequest(objectMapper, e.getMessage());
        }
    }

    /** Column-filter value lists for {@link #getGlobalSearch} — the same {@code /summary} sibling
     * PL/SQL's search has, feeding {@code jar/js/search.js}'s type/schema checkbox filters. */
    @GetMapping("/{id}/global-search/summary")
    public ResponseEntity<JsonNode> getGlobalSearchSummary(
            @PathVariable String id,
            @RequestParam(value = "version", required = false) String version,
            @RequestParam(value = "q", required = false) String q) throws IOException {
        if (q == null || q.isBlank()) {
            return com.plsqlanalyzer.web.page.FacetPageResponder.badRequest(objectMapper, "q is required");
        }
        com.fasterxml.jackson.databind.node.ArrayNode rows = buildSearchRows(id, version, q);
        return facets.summary(rows, null, JarFacetSpecs.SEARCH, null);
    }

    /**
     * Builds the normalized row set every Global Search request re-derives (never cached — the
     * three sources are cheap to scan and a cache would be one more place a re-analysis could go
     * stale). Any one facet failing (e.g. no bundled-jars.json for an older analysis) is swallowed
     * so the other facets still answer, mirroring {@code ParserAnalysisController.buildSearchRows}'s
     * own "skip, don't fail" behaviour per facet.
     */
    private com.fasterxml.jackson.databind.node.ArrayNode buildSearchRows(String id, String version, String q)
            throws IOException {
        String needle = q.toLowerCase(java.util.Locale.ROOT);
        com.fasterxml.jackson.databind.node.ArrayNode out = objectMapper.createArrayNode();
        int[] idx = {0};

        try (var rowsIt = repository.rows(
                com.analyzer.storage.repo.AnalysisQuery.ofJar(
                        com.analyzer.storage.repo.AnalysisQuery.Dataset.JAR_CLASS_TREE, id, version),
                row -> row)) {
            while (rowsIt.hasNext()) {
                JsonNode row = rowsIt.next();
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
        } catch (Exception ignored) {
            // No class tree for this analysis/version — that facet is simply absent from results.
        }

        try (var rowsIt = repository.rows(
                com.analyzer.storage.repo.AnalysisQuery.ofJar(
                        com.analyzer.storage.repo.AnalysisQuery.Dataset.JAR_BUNDLED_JARS, id, version),
                row -> row)) {
            while (rowsIt.hasNext()) {
                JsonNode row = rowsIt.next();
                String name = row.path("name").asText("");
                if (name.toLowerCase(java.util.Locale.ROOT).contains(needle)) {
                    var o = out.addObject();
                    o.put("type", "DEPENDENCY");
                    o.put("name", name);
                    o.put("schema", "");
                    o.put("matchedField", "name");
                    o.put("_idx", idx[0]++);
                    o.set("raw", row);
                }
            }
        } catch (Exception ignored) {
            // No bundled-jars.json for this analysis/version.
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

    /** Quick validation: a summary cache is valid if it ends with '}' (complete JSON object). */
    private boolean isSummaryCacheValid(ArtifactKey cache) {
        try {
            long size = store.size(cache);
            if (size < 2) return false;
            // Ranged read instead of RandomAccessFile: one byte transferred either way, and it
            // works against an object store (an HTTP Range request) as well as a local file.
            try (InputStream in = store.read(cache, size - 1, 1)) {
                return in.read() == '}';
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
            ArtifactKey jarKey = paths.storedJarKey(id);
            if (!store.exists(jarKey)) {
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Stored JAR not found: " + id);
            }
            // materialize(): JarParserService opens the archive as a JarFile, which needs a real
            // seekable file. Documented escape hatch; zero-copy in local mode.
            try (com.analyzer.storage.LocalHandle handle = store.materialize(jarKey)) {
                Map<String, String> configs = parserService.extractConfigFiles(handle.path().toFile());
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

            ArtifactKey storedJar = paths.storedJarKey(id);
            if (!store.exists(storedJar)) continue;

            long fileSize = store.size(storedJar);
            // Temp scratch handed to the queue worker: stays local by design, but its CONTENT now
            // comes from the store rather than assuming a local stored.jar exists.
            Path tempJar = Files.createTempFile("jar-reanalyze-", ".jar");
            try (InputStream in = openRead(storedJar);
                 OutputStream out = Files.newOutputStream(tempJar)) {
                in.transferTo(out);
            }

            // Preserve the per-JAR domain config before the delete removes it — see the
            // single-JAR reanalyze below for why.
            Path preservedConfig = null;
            ArtifactKey storedConfig = paths.domainConfigKey(jarName);
            if (store.exists(storedConfig)) {
                preservedConfig = Files.createTempFile("jar-domain-config-", ".json");
                try (InputStream in = openRead(storedConfig);
                     OutputStream out = Files.newOutputStream(preservedConfig)) {
                    in.transferTo(out);
                }
            }

            Map<String, Object> metadata = new LinkedHashMap<>();
            metadata.put("jarName", jarName);
            metadata.put("tempFilePath", tempJar.toString());
            metadata.put("fileSize", fileSize);
            metadata.put("mode", "static");
            if (preservedConfig != null) metadata.put("perJarConfigPath", preservedConfig.toString());
            String priorBasePackage = readBasePackage(id);
            if (priorBasePackage != null && !priorBasePackage.isBlank()) {
                metadata.put("basePackage", priorBasePackage);
            }

            sessionManager.killSessionsForJar(id);
            claudeTracker.removeTracking(id);
            mongoCatalogService.deleteCatalog(id);
            correctionPersistence.deleteAll(id);
            persistenceService.delete(id);

            QueueJob job = queueService.submit(QueueJob.Type.JAR_UPLOAD, "Re-analyze: " + jarName, metadata);
            queued.add(Map.of("jarName", jarName, "jobId", job.id, "jarSize", fileSize));
        }

        log.info("Re-analyze all: queued {} JARs for fresh analysis", queued.size());
        return Map.of("status", "queued", "count", queued.size(), "jobs", queued);
    }

    /**
     * Reads {@code basePackage} out of an existing analysis header so a re-analysis preserves it.
     * Returns null when there is no header or it carries no basePackage — both normal.
     */
    private String readBasePackage(String id) {
        for (Map<String, Object> jar : persistenceService.listJars()) {
            Object jid = jar.get("id");
            if (jid != null && jid.equals(id)) {
                Object bp = jar.get("basePackage");
                return bp instanceof String s ? s : null;
            }
        }
        return null;
    }

    @PostMapping("/{id}/reanalyze")
    public Map<String, Object> reanalyze(@PathVariable String id) throws IOException {
        ArtifactKey storedJar = paths.storedJarKey(id);
        if (!store.exists(storedJar)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Stored JAR not found for: " + id);
        }

        long fileSize = store.size(storedJar);
        String jarName = id.endsWith(".jar") ? id : id + ".jar";

        Path tempJar = Files.createTempFile("jar-reanalyze-", ".jar");
        try (InputStream in = openRead(storedJar);
             OutputStream out = Files.newOutputStream(tempJar)) {
            in.transferTo(out);
        }

        // Preserve the stored per-JAR domain config BEFORE the delete below wipes it. The
        // executor's re-analysis branch reads it back via getDomainConfigPath(), which returns
        // nothing once the JAR directory is gone — so without this copy, re-analysing silently
        // drops the JAR's domain config and produces differently-classified output.
        Path preservedConfig = null;
        ArtifactKey storedConfig = paths.domainConfigKey(jarName);
        if (store.exists(storedConfig)) {
            preservedConfig = Files.createTempFile("jar-domain-config-", ".json");
            try (InputStream in = openRead(storedConfig);
                 OutputStream out = Files.newOutputStream(preservedConfig)) {
                in.transferTo(out);
            }
        }

        // Carry the basePackage forward. It is supplied at upload time and stored only in the
        // analysis header, so without reading it back here a re-analysis silently reverts to
        // "no base package" and reclassifies in-scope vs external calls across the whole tree.
        String priorBasePackage = readBasePackage(id);

        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("jarName", jarName);
        metadata.put("tempFilePath", tempJar.toString());
        metadata.put("fileSize", fileSize);
        metadata.put("mode", "static");
        if (preservedConfig != null) metadata.put("perJarConfigPath", preservedConfig.toString());
        if (priorBasePackage != null && !priorBasePackage.isBlank()) {
            metadata.put("basePackage", priorBasePackage);
        }

        // Everything above is non-destructive. Do the irreversible cleanup as late as possible,
        // immediately before submit, so any earlier failure leaves the existing analysis intact.
        sessionManager.killSessionsForJar(id);
        claudeTracker.removeTracking(id);
        mongoCatalogService.deleteCatalog(id);
        correctionPersistence.deleteAll(id);
        persistenceService.delete(id);

        log.info("Re-analyze: cleaned old data for {}, queueing fresh analysis ({} MB)", id, fileSize / (1024 * 1024));

        QueueJob job;
        try {
            job = queueService.submit(QueueJob.Type.JAR_UPLOAD, "Re-analyze: " + jarName, metadata);
        } catch (RuntimeException e) {
            // The old analysis is already gone at this point. Make the recovery path obvious
            // rather than leaving the user with a deleted JAR and a generic 500.
            log.error("Re-analyze FAILED TO QUEUE for {} after cleanup — previous analysis is gone. "
                    + "Source JAR preserved at {}; re-upload it to recover.", id, tempJar, e);
            throw e;
        }

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

    /**
     * The collection report, streamed.
     *
     * <p>Was `persistenceService.load(id)` — all of `analysis.json`, measured at **49.4 MB** — to
     * build an aggregate measured at **3.8 KB** (14 collections, 155 endpoint/collection pairs).
     * Roughly 13,000× more input than output, in heap, per request. Only the `endpoints` array is
     * read now; `classes` and `callTreeNodes` are skipped as tokens.
     *
     * <p>`limit`/`after` are accepted so the response is bounded by contract rather than by the
     * happening-to-be-small size of the aggregate, but the load was the defect.
     */
    @GetMapping("/{id}/collections")
    public ResponseEntity<JsonNode> getCollectionReport(
            @PathVariable String id,
            @RequestParam(value = "version", required = false) String version,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "after", required = false) String after,
            @RequestHeader(value = "Accept", required = false) String accept) throws IOException {
        AnalysisDataProvider provider = resolveProvider(id, version);
        // The default request (no version, no paging) is the one that reads the whole document;
        // its aggregate is precomputed into views/collections-count.json. A version-explicit or
        // paged request is served the old way — the cached body is specifically the default shape.
        boolean cacheable = version == null && limit == null && (after == null || after.isBlank());
        if (cacheable) {
            byte[] cached = readJarView(provider, paths.collectionsViewKey(id));
            if (cached != null) {
                try {
                    return ResponseEntity.ok(objectMapper.readTree(cached));
                } catch (IOException e) {
                    log.debug("collections view unreadable for {}: {}", id, e.getMessage());
                }
            }
        }
        List<com.jaranalyzer.report.CollectionReportAggregator.CollectionRow> rows;
        try (var parser = objectMapper.getFactory().createParser(provider.openStream())) {
            rows = com.jaranalyzer.report.CollectionReportAggregator.aggregate(parser, objectMapper);
        }
        JsonNode body = com.jaranalyzer.report.CollectionReportPage.respond(
                objectMapper, rows, limit, after, accept);
        if (cacheable && body != null) {
            writeJarView(paths.collectionsViewKey(id), objectMapper.writeValueAsBytes(body));
        }
        return ResponseEntity.ok(body);
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
        ArtifactKey dir = paths.correctionsDirKey(id);
        List<Map<String, Object>> logs = new ArrayList<>();

        // Endpoint method name is the suffix after the last dot
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
    /**
     * {@code /correction-logs}, paged — MID-017. No client calls the unpaged route above (checked
     * across the whole static tree; the corrections screen calls the per-endpoint
     * {@code /correction-logs/{endpointName}} route instead, which is bounded already). Same
     * light in-memory pattern MID-005 used for {@code /resolver}: convert the existing
     * {@code List<Map<String,Object>>} to rows and hand them to {@code FacetPageResponder}
     * directly, skipping the repository abstraction this endpoint has no other reason to need.
     */
    @GetMapping("/{id}/correction-logs/page")
    public ResponseEntity<JsonNode> getCorrectionLogsPage(
            @PathVariable String id,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "after", required = false) String after,
            @RequestParam(value = "sort", required = false) String sort,
            @RequestParam(value = "q", required = false) String q,
            @RequestParam(value = "types", required = false) String types,
            @RequestParam(value = "cf", required = false) List<String> cf,
            @RequestHeader(value = "Accept", required = false) String accept) {
        List<JsonNode> rows = LogListFacets.rows(objectMapper, listAllCorrectionLogs(id));
        try {
            return facets.respondStreaming(rows.iterator(), LogListFacets.CORRECTION_LOGS,
                    new com.plsqlanalyzer.web.page.FacetPageResponder.Params(
                            limit, after, sort, q, types, cf, accept),
                    null);
        } catch (IllegalArgumentException | com.analyzer.storage.page.Cursor.InvalidCursorException e) {
            return com.plsqlanalyzer.web.page.FacetPageResponder.badRequest(objectMapper, e.getMessage());
        }
    }

    @GetMapping("/{id}/correction-logs")
    public List<Map<String, Object>> listAllCorrectionLogs(@PathVariable String id) {
        ArtifactKey dir = paths.correctionsDirKey(id);
        List<Map<String, Object>> logs = new ArrayList<>();

        try (var stream = store.list(dir, false)) {
            stream.filter(java.util.function.Predicate.not(com.analyzer.storage.ArtifactEntry::directory)).filter(e -> {
                String name = e.key().leaf();
                return name.contains("_corr") && (name.endsWith("_input.txt") || name.endsWith("_output.json"));
            }).sorted(Comparator.comparing(e -> e.key().leaf())).forEach(e -> {
                // Size comes from the listing entry, so there is no longer a per-file stat that
                // could throw here.
                String name = e.key().leaf();
                long size = e.size();
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
            });
        } catch (IOException e) { /* empty list */ }
        return logs;
    }

    /** Fetch content of a single correction log file by name. */
    @GetMapping("/{id}/correction-logs/file/{fileName}")
    public ResponseEntity<String> getCorrectionLogFile(@PathVariable String id, @PathVariable String fileName) {
        // With no base directory left to compare against, the startsWith(dir) containment check
        // becomes an explicit rejection of anything that is not a single plain file name.
        if (fileName.contains("..") || fileName.contains("/") || fileName.contains("\\")) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Log file not found: " + fileName);
        }
        ArtifactKey file = paths.correctionsDirKey(id).resolve(fileName);
        if (!store.exists(file)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Log file not found: " + fileName);
        }
        try {
            return ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body(readString(file));
        } catch (IOException e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to read: " + e.getMessage());
        }
    }

    /* ---- Claude run logs ---- */

    /** List available run log files for a JAR. */
    /** {@code /run-logs}, paged — MID-017. Same reasoning as {@link #getCorrectionLogsPage}: no
     * client calls the unpaged route. */
    @GetMapping("/{id}/run-logs/page")
    public ResponseEntity<JsonNode> getRunLogsPage(
            @PathVariable String id,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "after", required = false) String after,
            @RequestParam(value = "sort", required = false) String sort,
            @RequestParam(value = "q", required = false) String q,
            @RequestHeader(value = "Accept", required = false) String accept) {
        List<JsonNode> rows = LogListFacets.rows(objectMapper, listRunLogs(id));
        try {
            return facets.respondStreaming(rows.iterator(), LogListFacets.RUN_LOGS,
                    new com.plsqlanalyzer.web.page.FacetPageResponder.Params(
                            limit, after, sort, q, null, null, accept),
                    null);
        } catch (IllegalArgumentException | com.analyzer.storage.page.Cursor.InvalidCursorException e) {
            return com.plsqlanalyzer.web.page.FacetPageResponder.badRequest(objectMapper, e.getMessage());
        }
    }

    @GetMapping("/{id}/run-logs")
    public List<Map<String, Object>> listRunLogs(@PathVariable String id) {
        ArtifactKey logDir = paths.correctionsDirKey(id);
        List<Map<String, Object>> logs = new ArrayList<>();
        try (var stream = store.list(logDir, false)) {
            stream.filter(java.util.function.Predicate.not(com.analyzer.storage.ArtifactEntry::directory))
                    .filter(e -> e.key().leaf().startsWith("run_") && e.key().leaf().endsWith(".log"))
                    .sorted(Comparator.comparing((com.analyzer.storage.ArtifactEntry e) -> e.key().leaf()).reversed())
                    .forEach(e -> logs.add(Map.of(
                            "name", e.key().leaf(),
                            "size", e.size(),
                            "modified", java.time.Instant.ofEpochMilli(e.lastModified()).toString()
                    )));
        } catch (IOException e) { /* empty list */ }
        return logs;
    }

    /** Fetch content of a specific run log. */
    @GetMapping("/{id}/run-logs/{logName}")
    public String getRunLog(@PathVariable String id, @PathVariable String logName) {
        if (!logName.startsWith("run_") || !logName.endsWith(".log")
                || logName.contains("..") || logName.contains("/") || logName.contains("\\")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid log name");
        }
        ArtifactKey logFile = paths.correctionsDirKey(id).resolve(logName);
        if (!store.exists(logFile)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Log not found: " + logName);
        }
        try { return readString(logFile); }
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

    @GetMapping("/{id}/resources")
    public List<Map<String, Object>> listResources(@PathVariable String id) {
        List<String> files = persistenceService.listResourceFiles(id);
        List<Map<String, Object>> result = new ArrayList<>();
        for (String filename : files) {
            try {
                String content = persistenceService.loadResourceFile(id, filename);
                long size = content != null ? content.length() : 0;
                result.add(Map.of("filename", filename, "size", size));
            } catch (IOException e) {
                result.add(Map.of("filename", filename, "size", 0));
            }
        }
        return result;
    }

    @GetMapping("/{id}/resources/{filename:.+}")
    public ResponseEntity<String> getResourceFile(@PathVariable String id,
                                                   @PathVariable String filename) {
        try {
            String content = persistenceService.loadResourceFile(id, filename);
            if (content == null) {
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Resource not found: " + filename);
            }
            return ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body(content);
        } catch (ResponseStatusException e) {
            throw e;
        } catch (IOException e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to read resource: " + e.getMessage());
        }
    }

    /**
     * Bundled JAR list — L2 / Q41.
     *
     * <p>Two responses from one URL, decided by the request alone (PLAN-08 §3.5). Without a paging
     * parameter this is byte-for-byte what it has always been: the whole 3.03 MB array, manifest
     * text and resource bodies included. With one, it is a page of
     * {@code {name, size, hasManifest, resourceCount, resourceNames}} — 15.9 KB for the same 94
     * rows, because 2.84 MB of that document is file content the tree does not render until a leaf
     * is clicked. Those bodies now come from {@link #getBundledJarContent} on click.
     *
     * <p>Streamed either way: the paged path never holds more than one row plus a bounded heap, so
     * a JAR with ten thousand bundled libraries costs the same as this one.
     */
    @GetMapping("/{id}/bundled-jars")
    public ResponseEntity<?> getBundledJars(
            @PathVariable String id,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "after", required = false) String after,
            @RequestParam(value = "sort", required = false) String sort,
            @RequestParam(value = "q", required = false) String q,
            @RequestHeader(value = "Accept", required = false) String accept) {
        var params = new com.plsqlanalyzer.web.page.FacetPageResponder.Params(
                limit, after, sort, q, null, null, accept);
        if (!params.wantsEnvelope()) {
            return ResponseEntity.ok(persistenceService.loadBundledJarInfo(id));
        }
        var query = com.analyzer.storage.repo.AnalysisQuery.of(
                com.analyzer.storage.repo.AnalysisQuery.Dataset.JAR_BUNDLED_JARS, id);
        if (!repository.exists(query)) {
            // 204, not an empty page: "this analysis recorded no bundled JARs" and "the search
            // matched nothing" are different answers and the tree renders them differently.
            return ResponseEntity.noContent().build();
        }
        try (var rows = repository.rows(query, JarFacetSpecs::bundledJarRow)) {
            return facets.respondStreaming(rows, JarFacetSpecs.BUNDLED_JARS, params, null);
        } catch (IllegalArgumentException | Cursor.InvalidCursorException e) {
            return com.plsqlanalyzer.web.page.FacetPageResponder.badRequest(objectMapper,
                    e.getMessage());
        } catch (IOException e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
                    "Failed to read bundled JAR list: " + e.getMessage());
        }
    }

    /**
     * One file out of one bundled JAR — the body the list projection dropped.
     *
     * <p>Found by scanning, not by index: the client asks by JAR name and file name, so a
     * re-analysis that reorders {@code bundled-jars.json} cannot make a click open the wrong file.
     * The old client cached every body in a browser-side array and opened them by array position,
     * which is the same positional coupling PLAN-08 §1.5 #4 names.
     */
    @GetMapping("/{id}/bundled-jars/content")
    public ResponseEntity<String> getBundledJarContent(@PathVariable String id,
                                                       @RequestParam("jar") String jarName,
                                                       @RequestParam("file") String fileName) {
        ArtifactKey key = paths.bundledJarsKey(id);
        if (!store.exists(key)) return ResponseEntity.notFound().build();
        boolean manifest = "MANIFEST.MF".equals(fileName);
        try (var parser = objectMapper.getFactory().createParser(openRead(key));
             var rows = com.plsqlanalyzer.web.page.JsonArrayRows.of(parser, objectMapper, null,
                     // Only the requested JAR is ever built into a node; every other row is parsed
                     // and dropped, so the scan costs one row of memory rather than 3 MB.
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
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
                    "Failed to read bundled JAR content: " + e.getMessage());
        }
    }

    @GetMapping("/{id}/jar-dep-map")
    public Map<String, java.util.Set<String>> getJarDepMap(@PathVariable String id) {
        return persistenceService.loadJarDepsMap(id);
    }
}
