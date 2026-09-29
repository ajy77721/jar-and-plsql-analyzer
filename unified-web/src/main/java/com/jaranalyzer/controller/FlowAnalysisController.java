package com.jaranalyzer.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowpoc.ClaudeInvoker;
import com.flowpoc.ProgressCallback;
import com.flowpoc.PocRunner;
import com.flowpoc.config.PocConfig;
import com.flowpoc.model.FlowResult;
import com.jaranalyzer.controller.FlowRunStore.RunMeta;
import com.jaranalyzer.service.JarDataPaths;
import com.jaranalyzer.service.JarParserService;
import com.jaranalyzer.service.MongoCatalogService;
import com.jaranalyzer.service.PersistenceService;
import com.plsqlanalyzer.web.service.ClaudeProcessRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.analyzer.auth.ConnectionGuard;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.LocalHandle;
import com.analyzer.storage.RequestScopedArtifactStore;
import com.analyzer.storage.sso.CapturedSsoContext;
import com.analyzer.storage.sso.SsoCredentialsResolver;

import java.io.ByteArrayOutputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Flow Analysis controller — test-data prep &amp; optimization report.
 *
 * Routes:
 *   POST /{id}/flow/run           — start async run, returns { runId, status }
 *   GET  /{id}/flow/status/{rid}  — live progress (phase, counts, log, partial results)
 *   GET  /{id}/flow/result/{rid}  — full result when done (kept for backwards compat)
 *   GET  /{id}/flow/report/{rid}  — download .md report
 *
 * <p>All heavyweight data (full JSON report, markdown report, log lines) is written
 * to disk under {@code data/flowanalysis/{jarId}/{runId}/}.  Only a minimal
 * {@link RunMeta} is kept in the heap per active run.</p>
 */
@RestController
@RequestMapping("/api/jar/jars")
public class FlowAnalysisController {

    private static final Logger log = LoggerFactory.getLogger(FlowAnalysisController.class);

    private final JarDataPaths paths;
    private final PersistenceService persistenceService;
    private final ObjectMapper objectMapper;
    private final ClaudeProcessRunner claudeProcessRunner;
    private final JarParserService jarParserService;
    private final MongoCatalogService mongoCatalogService;
    private final FlowRunStore store;

    /** Fails a run fast when the caller is logged in but has not connected a bucket. */
    private final ConnectionGuard connectionGuard;

    /** Carries the caller's role credentials onto the bare run thread (defense in depth). */
    private final SsoCredentialsResolver ssoCredentialsResolver;

    /**
     * Required, not optional — an optional store would leave a silent filesystem fallback.
     *
     * <p>Named {@code artifacts} because {@code store} is already taken by {@link FlowRunStore},
     * which is still on the Path API and is being migrated separately. Its paths are all under
     * {@code JarDataPaths.getBaseDir()}, so {@link JarDataPaths#keyForPath} maps them to keys and
     * this controller can ask the real store about them without waiting for that migration.
     */
    private final ArtifactStore artifacts;

    /** Minimal in-memory state — large payloads live on disk. */
    private final ConcurrentHashMap<String, RunMeta> runs = new ConcurrentHashMap<>();
    private final AtomicLong counter = new AtomicLong(1);

    public FlowAnalysisController(JarDataPaths paths,
                                  PersistenceService persistenceService,
                                  ObjectMapper objectMapper,
                                  @Qualifier("plsqlClaudeProcessRunner") ClaudeProcessRunner claudeProcessRunner,
                                  JarParserService jarParserService,
                                  MongoCatalogService mongoCatalogService,
                                  @Qualifier("jarArtifactStore") ArtifactStore artifacts,
                                  ConnectionGuard connectionGuard,
                                  SsoCredentialsResolver ssoCredentialsResolver) {
        this.paths               = paths;
        this.persistenceService  = persistenceService;
        this.objectMapper        = objectMapper;
        this.claudeProcessRunner = claudeProcessRunner;
        this.jarParserService    = jarParserService;
        this.mongoCatalogService = mongoCatalogService;
        this.artifacts           = artifacts;
        this.connectionGuard     = connectionGuard;
        this.ssoCredentialsResolver = ssoCredentialsResolver;
        this.store               = new FlowRunStore(paths.keyForPath(paths.getBaseDir()), artifacts, objectMapper);
    }

    /** Does the artifact at this legacy Path exist, asked of the store rather than the disk? */
    private boolean existsAt(Path underDataRoot) {
        ArtifactKey key = paths.keyForPath(underDataRoot);
        return key != null && artifacts.exists(key);
    }

    private boolean existsAt(ArtifactKey key) {
        return key != null && artifacts.exists(key);
    }

    // ── Claude CLI status ─────────────────────────────────────────────────────

    @GetMapping("/claude-status")
    public Map<String, Object> claudeStatus() {
        boolean available = claudeProcessRunner.isAvailable();
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("available", available);
        r.put("message", available ? "Claude CLI ready — analysis will run automatically" : "Claude CLI not found — run `claude login` to enable AI insights");
        return r;
    }

    // ── Trigger ───────────────────────────────────────────────────────────────

    @PostMapping("/{id}/flow/run")
    public Map<String, Object> run(@PathVariable String id, @RequestBody FlowRunRequest req) {
        // Fail fast when the caller is logged in but has not connected a bucket, rather than start
        // a run whose artifact writes would fall through to the shared default target.
        connectionGuard.requireJarConnected();
        String runId = String.valueOf(counter.getAndIncrement());

        // Create run directory and minimal in-memory state
        ArtifactKey runDir;
        try {
            runDir = store.createRunDir(id, runId);
        } catch (Exception e) {
            return Map.of("error", "Failed to create run directory: " + e.getMessage());
        }

        RunMeta meta = new RunMeta(runId, runDir);
        // Record the owner now, on the request thread, so a poll by another session can be refused.
        meta.owner     = connectionGuard.currentIdentityKey();
        meta.mode      = req.mode();
        meta.endpoints = req.endpointPaths() != null ? req.endpointPaths() : List.of();
        runs.put(runId, meta);
        store.writeStatus(meta);

        // The bare worker below has no request context, so every store call it makes (writeStatus,
        // appendLog, report writes, artifacts.exists) would fall through to the shared default
        // target. Capture the request thread's routing scope and any installed job context HERE,
        // before the thread starts, and re-install them around the whole run. Either capture may
        // carry nothing (install is then a no-op), preserving the default-chain path exactly.
        final RequestScopedArtifactStore.Scope routingScope = artifacts instanceof RequestScopedArtifactStore routed
                ? routed.captureScope() : RequestScopedArtifactStore.Scope.NONE;
        final CapturedSsoContext jobContext = CapturedSsoContext.capture(ssoCredentialsResolver);

        new Thread(() -> {
          try (AutoCloseable routed = routingScope.install();
               AutoCloseable ctx = jobContext == null ? null : jobContext.install()) {
            try {
                ArtifactKey analysisKey = paths.analysisKey(id);
                if (!artifacts.exists(analysisKey)) {
                    meta.fail("analysis.json not found — please analyze the JAR first");
                    store.writeStatus(meta);
                    return;
                }

                Map<String, Object> conns = persistenceService.loadConnections(id);
                String mongoUri  = resolveMongoUri(req.mongoUri(), conns);
                String mongoDb   = resolveMongoDb(req.mongoDb(), conns);

                // CLI invoker — no API key needed; falls back to direct API key only if explicitly set
                ClaudeInvoker claudeInvoker = claudeProcessRunner.isAvailable()
                        ? prompt -> claudeProcessRunner.run(prompt, null, 120)
                        : null;
                // If CLI not available, fall back to HTTP API key if provided
                String claudeKey = (claudeInvoker == null)
                        ? ((req.claudeApiKey() != null && !req.claudeApiKey().isBlank())
                                ? req.claudeApiKey() : System.getenv("ANTHROPIC_API_KEY"))
                        : null;

                // Build progress callback — writes to disk in real time
                ProgressCallback cb = new ProgressCallback() {
                    @Override
                    public void onEndpointStart(int idx, int total, String ep) {
                        meta.totalEndpoints = total;
                        meta.currentEndpoint = ep;
                        meta.phase = "Static analysis · endpoint " + idx + " / " + total;
                        store.appendLog(runDir, "⏳ [" + idx + "/" + total + "] " + ep);
                        store.writeStatus(meta);
                    }
                    @Override
                    public void onEndpointDone(int idx, int total, String ep, FlowResult result) {
                        int findings = result.getOptimizations().size();
                        meta.doneEndpoints.incrementAndGet();
                        store.replaceLastLog(runDir,
                                "⏳ [" + idx + "/" + total + "] " + ep,
                                "✓ [" + idx + "/" + total + "] " + ep
                                        + " — " + findings + " finding" + (findings != 1 ? "s" : ""));
                        store.writeStatus(meta);
                    }
                    @Override
                    public void onClaudePlanReady(String ep, int totalNodes) {
                        meta.totalClaudeNodes.addAndGet(totalNodes);
                        meta.phase = "Claude AI enrichment · " + ep;
                        store.writeStatus(meta);
                    }
                    @Override
                    public void onClaudeNodeStart(String ep, String node, int nodeIdx, int totalNodes) {
                        meta.currentClaudeNode = node;
                        meta.phase = "Claude AI · " + shortClass(node) + " (" + nodeIdx + "/" + totalNodes + ")";
                        store.appendLog(runDir, "🤖 Claude → " + shortClass(node));
                        store.writeStatus(meta);
                    }
                    @Override
                    public void onClaudeNodeDone(String ep, String node, int enriched) {
                        meta.doneClaudeNodes.incrementAndGet();
                        store.replaceLastLog(runDir,
                                "🤖 Claude → " + shortClass(node),
                                "🤖 ✓ " + shortClass(node)
                                        + (enriched > 0 ? " — " + enriched + " insight" + (enriched != 1 ? "s" : "") : ""));
                        store.writeStatus(meta);
                    }
                    private String shortClass(String location) {
                        // "com.foo.bar.OrderService.processOrder" → "OrderService.processOrder"
                        if (location == null) return "?";
                        int dot = location.lastIndexOf('.', location.lastIndexOf('.') - 1);
                        return dot >= 0 ? location.substring(dot + 1) : location;
                    }
                };

                PocConfig.Builder builder = PocConfig.builder()
                        .analysis(artifacts, analysisKey)
                        .sampleSize(10)
                        .maxEndpoints(0)
                        .progressCallback(cb);

                if (mongoUri != null && mongoDb != null && !mongoDb.isBlank())
                    builder.mongo(mongoUri, mongoDb);

                if ("optimize".equals(req.mode())) {
                    ArtifactKey storedJar = paths.storedJarKey(id);
                    if (artifacts.exists(storedJar))
                        builder.layer2(storedJar);
                }

                // Wire Claude: prefer CLI invoker, fall back to HTTP API key
                if (claudeInvoker != null)
                    builder.claudeInvoker(claudeInvoker);
                else if (claudeKey != null && !claudeKey.isBlank())
                    builder.claude(claudeKey);

                // Pass selected endpoints into config so FlowWalker skips non-matching at parse time
                if (req.endpointPaths() != null && !req.endpointPaths().isEmpty()) {
                    builder.selectedEndpoints(req.endpointPaths());
                    meta.phase = "Locating " + req.endpointPaths().size() + " endpoint(s) in analysis.json…";
                } else {
                    meta.phase = "Walking call trees…";
                }

                PocConfig config = builder.build();
                List<FlowResult> results = new PocRunner(config).runAndReturn();

                meta.phase = "Generating reports…";
                store.writeStatus(meta);

                // Write reports directly to disk — never stored in heap
                ByteArrayOutputStream jsonOut = new ByteArrayOutputStream();
                new com.flowpoc.report.JsonReporter().write(results, jsonOut);
                String jsonReport = jsonOut.toString(StandardCharsets.UTF_8);
                store.writeResults(runDir, jsonReport);

                ByteArrayOutputStream mdOut = new ByteArrayOutputStream();
                new com.flowpoc.report.MarkdownReporter(req.mode()).write(results, mdOut);
                store.writeReport(runDir, mdOut.toString(StandardCharsets.UTF_8));

                // Compute and persist history summary counters into status.json
                computeAndPersistHistorySummary(meta, jsonReport);

                meta.complete();
                store.writeStatus(meta);
                store.appendLog(runDir, "Analysis complete — " + results.size() + " endpoint(s) processed.");

            } catch (Exception e) {
                meta.fail(e.getMessage());
                store.writeStatus(meta);
            }
          } catch (Exception e) {
            log.warn("Flow run {} failed to run under captured context: {}", runId, e.getMessage());
          }
        }).start();

        return Map.of("runId", runId, "status", "running");
    }

    // ── Live status (progress) ────────────────────────────────────────────────

    @GetMapping("/{id}/flow/status/{runId}")
    public ResponseEntity<Map<String, Object>> status(@PathVariable String id,
                                                       @PathVariable String runId) {
        RunMeta meta = runs.get(runId);

        // If not in memory, reconstruct a minimal view from disk
        if (meta == null) {
            ArtifactKey runDir = store.runDir(id, runId);
            Map<String, Object> diskStatus = store.readStatus(runDir);
            if (diskStatus == null) return ResponseEntity.notFound().build();
            // 404 rather than 403 for a non-owner: a poll for someone else's (or a nonexistent)
            // runId is indistinguishable, so this cannot be used to probe which ids exist.
            if (!connectionGuard.owns(str(diskStatus.get("owner"))))
                return ResponseEntity.notFound().build();

            Map<String, Object> resp = new LinkedHashMap<>(diskStatus);
            resp.put("log", store.readLogTail(runDir));
            if ("done".equals(diskStatus.get("status"))) {
                Object parsed = store.readResults(runDir);
                if (parsed != null) resp.put("results", parsed);
                if (existsAt(store.reportFile(runDir))) resp.put("mdAvailable", true);
            }
            return ResponseEntity.ok(resp);
        }

        if (!connectionGuard.owns(meta.owner)) return ResponseEntity.notFound().build();

        // In-memory run (active or recently completed)
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("status",           meta.status);
        resp.put("phase",            meta.phase);
        resp.put("totalEndpoints",   meta.totalEndpoints);
        resp.put("doneEndpoints",    meta.doneEndpoints.get());
        resp.put("currentEndpoint",  meta.currentEndpoint);
        resp.put("totalClaudeNodes", meta.totalClaudeNodes.get());
        resp.put("doneClaudeNodes",  meta.doneClaudeNodes.get());
        resp.put("currentClaudeNode",meta.currentClaudeNode);
        resp.put("log",              store.readLogTail(meta.runDir));

        if (meta.error != null) resp.put("error", meta.error);

        if ("done".equals(meta.status)) {
            Object parsed = store.readResults(meta.runDir);
            if (parsed != null) resp.put("results", parsed);
            if (existsAt(store.reportFile(meta.runDir))) resp.put("mdAvailable", true);

            // Evict from memory after results have been served once — disk has everything
            if (meta.resultsServed) {
                runs.remove(runId);
            } else {
                meta.resultsServed = true;
            }
        }

        return ResponseEntity.ok(resp);
    }

    // ── Poll (backwards compat) ───────────────────────────────────────────────

    @GetMapping("/{id}/flow/result/{runId}")
    public ResponseEntity<Map<String, Object>> result(@PathVariable String id,
                                                       @PathVariable String runId) {
        return status(id, runId);
    }

    // ── Download .md ──────────────────────────────────────────────────────────

    @GetMapping("/{id}/flow/report/{runId}")
    public ResponseEntity<byte[]> report(@PathVariable String id,
                                          @PathVariable String runId) {
        ArtifactKey runDir = resolveRunDir(id, runId);
        if (runDir == null) return ResponseEntity.notFound().build();
        // The status.json beside the report carries the owner; a non-owner gets the same 404 as a
        // run whose report does not exist.
        Map<String, Object> runStatus = store.readStatus(runDir);
        if (runStatus != null && !connectionGuard.owns(str(runStatus.get("owner"))))
            return ResponseEntity.notFound().build();

        byte[] bytes = store.readReport(runDir);
        if (bytes == null) return ResponseEntity.notFound().build();

        String filename = id.replaceAll("[^a-zA-Z0-9._-]", "_") + "-flow-analysis.md";
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
                .contentType(MediaType.parseMediaType("text/markdown; charset=UTF-8"))
                .body(bytes);
    }

    // ── History ───────────────────────────────────────────────────────────────

    /**
     * Returns history for this JAR by scanning {@code data/flowanalysis/{jarId}/}
     * and reading each run's {@code status.json}. Lazy — never loads results.json.
     */
    @GetMapping("/{id}/flow/history")
    public List<HistoryEntry> getHistory(@PathVariable String id) {
        List<Map<String, Object>> statuses = store.listRunStatuses(id);
        List<HistoryEntry> result = new ArrayList<>(statuses.size());
        for (Map<String, Object> s : statuses) {
            String  rId      = str(s.get("runId"));
            String  mode     = str(s.get("mode"));
            Object  epsRaw   = s.get("endpoints");
            long    ts       = toLong(s.get("timestamp"));
            int     high     = toInt(s.get("highCount"));
            int     med      = toInt(s.get("medCount"));
            int     low      = toInt(s.get("lowCount"));
            int     datasets = toInt(s.get("datasetCount"));

            List<String> eps = new ArrayList<>();
            if (epsRaw instanceof List<?> list) {
                for (Object o : list) if (o != null) eps.add(o.toString());
            }
            result.add(new HistoryEntry(rId, mode != null ? mode : "optimize",
                    eps, ts, high, med, low, datasets));
        }
        return result;
    }

    /**
     * Computes per-severity finding counts and dataset count from the JSON report,
     * then persists them as extra fields in status.json so history can read them
     * without loading results.json.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void computeAndPersistHistorySummary(RunMeta meta, String jsonReport) {
        int high = 0, med = 0, low = 0, datasets = 0;
        try {
            List<?> resultsList = objectMapper.readValue(jsonReport, List.class);
            for (Object res : resultsList) {
                if (!(res instanceof Map<?,?> resMap)) continue;
                List<?> opts = (List<?>) resMap.get("optimizations");
                if (opts != null) {
                    for (Object o : opts) {
                        if (o instanceof Map<?,?> f) {
                            String sev = (String) f.get("severity");
                            if      ("HIGH".equals(sev))   high++;
                            else if ("MEDIUM".equals(sev)) med++;
                            else if ("LOW".equals(sev))    low++;
                        }
                    }
                }
                List<?> tds = (List<?>) resMap.get("testDataSets");
                if (tds != null) datasets += tds.size();
            }
        } catch (Exception ignored) {}

        // Persist summary counters into status.json via extra fields on RunMeta
        meta.highCount    = high;
        meta.medCount     = med;
        meta.lowCount     = low;
        meta.datasetCount = datasets;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Resolves the run directory: checks in-memory map first, then falls back to
     * constructing the disk path directly.
     */
    private ArtifactKey resolveRunDir(String jarId, String runId) {
        RunMeta meta = runs.get(runId);
        if (meta != null) return meta.runDir;
        ArtifactKey dir = store.runDir(jarId, runId);
        // A run exists iff its status.json does. The old Files.isDirectory could not be asked of
        // an object store, which has no directories — only keys.
        return existsAt(store.statusFile(dir)) ? dir : null;
    }

    private static String str(Object v)  { return v != null ? v.toString() : null; }
    private static int    toInt(Object v) {
        if (v instanceof Number n) return n.intValue();
        if (v instanceof String s) { try { return Integer.parseInt(s); } catch (NumberFormatException ignored) {} }
        return 0;
    }
    private static long   toLong(Object v) {
        if (v instanceof Number n) return n.longValue();
        if (v instanceof String s) { try { return Long.parseLong(s); } catch (NumberFormatException ignored) {} }
        return 0L;
    }

    @SuppressWarnings("unchecked")
    private static String resolveMongoUri(String explicit, Map<String, Object> conns) {
        if (explicit != null && !explicit.isBlank()) return explicit;
        if (conns == null) return null;
        Object mongo = conns.get("mongodb");
        if (mongo instanceof Map<?,?> m) {
            Object uri = m.get("uri");
            if (uri instanceof String s && !s.isBlank() && !s.contains("***")) return s;
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static String resolveMongoDb(String explicit, Map<String, Object> conns) {
        if (explicit != null && !explicit.isBlank()) return explicit;
        if (conns == null) return null;
        Object mongo = conns.get("mongodb");
        if (mongo instanceof Map<?,?> m) {
            Object db = m.get("database");
            if (db instanceof String s && !s.isBlank()) return s;
        }
        return null;
    }

    // ── Env profiles (MongoDB connection discovery from JAR config) ───────────

    /**
     * Returns all environment profiles found in the uploaded JAR's config files,
     * each with:
     *   - profileName  : "dev1", "local", "default", etc.
     *   - uriTemplate  : the raw URI string (may contain ${VAR} placeholders)
     *   - database     : resolved database name (if available)
     *   - requiredVars : list of ${...} placeholder names the user must supply
     *   - resolvedUri  : non-null only when URI had NO placeholders (ready to use)
     */
    @GetMapping("/{id}/flow/env-profiles")
    public ResponseEntity<List<Map<String, Object>>> envProfiles(@PathVariable String id) {
        ArtifactKey storedJar = paths.storedJarKey(id);
        if (!artifacts.exists(storedJar)) {
            return ResponseEntity.ok(List.of());
        }
        // materialize(): JarParserService opens the archive as a JarFile, which needs a real
        // seekable file. Documented escape hatch; zero-copy in local mode.
        try (LocalHandle jar = artifacts.materialize(storedJar)) {
            Map<String, String> configFiles = jarParserService.extractConfigFiles(jar.path().toFile());
            List<Map<String, Object>> profiles = extractEnvProfiles(configFiles);
            return ResponseEntity.ok(profiles);
        } catch (Exception e) {
            return ResponseEntity.ok(List.of());
        }
    }

    /**
     * Test a MongoDB connection by attempting a ping against the given URI.
     * Body: { "mongoUri": "mongodb+srv://user:pass@host/db?..." }
     * Returns: { "success": true/false, "message": "...", "database": "...", "latencyMs": N }
     */
    @PostMapping("/{id}/flow/test-connection")
    public Map<String, Object> testConnection(@PathVariable String id,
                                               @RequestBody Map<String, String> body) {
        String uri = body != null ? body.get("mongoUri") : null;
        Map<String, Object> result = new LinkedHashMap<>();
        if (uri == null || uri.isBlank()) {
            result.put("success", false);
            result.put("message", "No URI provided");
            return result;
        }
        long start = System.currentTimeMillis();
        try (com.mongodb.client.MongoClient client = com.mongodb.client.MongoClients.create(
                com.mongodb.MongoClientSettings.builder()
                    .applyConnectionString(new com.mongodb.ConnectionString(uri))
                    .applyToSocketSettings(b -> b.connectTimeout(8, java.util.concurrent.TimeUnit.SECONDS)
                                                  .readTimeout(8, java.util.concurrent.TimeUnit.SECONDS))
                    .applyToClusterSettings(b -> b.serverSelectionTimeout(8, java.util.concurrent.TimeUnit.SECONDS))
                    .build())) {

            // Extract database name from URI or use the JAR's resolved database
            String dbName = extractDbFromUri(uri);
            if (dbName == null || dbName.isBlank()) dbName = "admin";

            // Ping the database
            client.getDatabase(dbName)
                  .runCommand(new org.bson.Document("ping", 1));

            long latency = System.currentTimeMillis() - start;

            // List collections to confirm real access
            List<String> collections = new ArrayList<>();
            client.getDatabase(dbName).listCollectionNames()
                  .into(collections);

            result.put("success", true);
            result.put("message", "Connected successfully");
            result.put("database", dbName);
            result.put("latencyMs", latency);
            result.put("collectionCount", collections.size());
            if (!collections.isEmpty()) {
                result.put("sampleCollections",
                    collections.subList(0, Math.min(5, collections.size())));
            }
        } catch (Exception e) {
            long latency = System.currentTimeMillis() - start;
            result.put("success", false);
            result.put("message", rootCause(e));
            result.put("latencyMs", latency);
        }
        return result;
    }

    private static String rootCause(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null) cause = cause.getCause();
        String msg = cause.getMessage();
        if (msg == null) msg = cause.getClass().getSimpleName();
        // Trim excessively long MongoDB error messages
        return msg.length() > 200 ? msg.substring(0, 200) + "…" : msg;
    }

    private List<Map<String, Object>> extractEnvProfiles(Map<String, String> configFiles) {
        List<Map<String, Object>> result = new ArrayList<>();

        // Config file names that carry profile-specific MongoDB config
        List<String> profileFiles = List.of(
            "application-local.properties", "application-local.yml",
            "application-dev.properties",   "application-dev.yml",
            "application-dev1.properties",  "application-dev2.properties",
            "application-qa1.properties",   "application-qa.properties",
            "application-uprj.properties",  "application-uat.properties",
            "application-uat.yml",          "application-it.properties",
            "application-sldev.properties", "application-prod.properties",
            "application-prod.yml",         "application.properties",
            "application.yml"
        );

        java.util.Set<String> seen = new java.util.LinkedHashSet<>();
        for (String fileName : profileFiles) {
            String content = configFiles.get(fileName);
            if (content == null) continue;

            // Derive a human-readable profile label
            String profileName = fileName
                .replace("application-", "")
                .replaceAll("\\.(properties|yml|yaml)$", "");
            if (profileName.isEmpty() || profileName.equals("application")) profileName = "default";

            // For multi-profile .properties files (spring.config.activate.on-profile),
            // also extract each embedded profile section
            if (fileName.endsWith(".properties")) {
                List<Map<String, Object>> sections = extractProfileSections(content, fileName);
                for (Map<String, Object> s : sections) {
                    String key = s.get("profileName") + "|" + s.get("uriTemplate");
                    if (seen.add(key)) result.add(s);
                }
            } else {
                // Single profile from dedicated file
                String rawUri = extractRawUri(content, fileName.endsWith(".properties"));
                if (rawUri == null) continue;
                List<String> vars = extractPlaceholders(rawUri);
                String db = extractDbFromUri(rawUri);

                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("profileName",  profileName);
                entry.put("uriTemplate",  rawUri);
                entry.put("database",     db != null ? db : "");
                entry.put("requiredVars", vars);
                entry.put("resolvedUri",  vars.isEmpty() ? rawUri : null);

                String key = profileName + "|" + rawUri;
                if (seen.add(key)) result.add(entry);
            }
        }
        return result;
    }

    /** Extract profile sections from a multi-profile .properties file. */
    private List<Map<String, Object>> extractProfileSections(String content, String fileName) {
        List<Map<String, Object>> sections = new ArrayList<>();
        // Split on #--- section markers (Spring Boot multi-doc properties)
        String[] parts = content.split("(?m)^#---\\s*$");

        // Base profile name from file name (e.g. "local" from application-local.properties)
        String baseProfile = fileName.replace("application-", "")
                .replaceAll("\\.(properties|yml|yaml)$", "");
        if (baseProfile.isEmpty() || baseProfile.equals("application")) baseProfile = "default";

        for (String part : parts) {
            try {
                Properties props = new Properties();
                props.load(new StringReader(part));

                // spring.config.activate.on-profile overrides the base profile name
                String activateOn = props.getProperty("spring.config.activate.on-profile");
                String profileName = (activateOn != null && !activateOn.isBlank())
                        ? activateOn.trim()
                        : baseProfile;

                String uri = props.getProperty("spring.data.mongodb.uri");
                String db  = props.getProperty("spring.data.mongodb.database");
                if (uri == null || uri.isBlank()) {
                    String host = props.getProperty("spring.data.mongodb.host");
                    String port = props.getProperty("spring.data.mongodb.port", "27017");
                    if (host != null && db != null) {
                        uri = "mongodb://" + host.trim() + ":" + port.trim() + "/" + db.trim();
                    }
                }
                if (uri == null || uri.isBlank()) continue;

                List<String> vars = extractPlaceholders(uri);
                // Also collect vars from the database property
                if (db != null) vars.addAll(extractPlaceholders(db));

                // Try to resolve placeholders that exist in the same section
                String resolvedUri = tryResolve(uri, props);
                String resolvedDb  = db != null ? tryResolve(db, props) : extractDbFromUri(resolvedUri);

                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("profileName",  profileName);
                entry.put("uriTemplate",  uri.trim());
                entry.put("database",     resolvedDb != null ? resolvedDb : (db != null ? db.trim() : ""));
                entry.put("requiredVars", vars.stream().distinct().collect(java.util.stream.Collectors.toList()));
                entry.put("resolvedUri",  vars.isEmpty() ? resolvedUri : null);
                sections.add(entry);
            } catch (Exception ignored) {}
        }
        return sections;
    }

    private String extractRawUri(String content, boolean isProperties) {
        try {
            if (isProperties) {
                Properties props = new Properties();
                props.load(new StringReader(content));
                return props.getProperty("spring.data.mongodb.uri");
            } else {
                // YAML — simple line-based extraction (avoids full YAML parsing complexity here)
                for (String line : content.split("\n")) {
                    String trimmed = line.trim();
                    if (trimmed.startsWith("uri:") && trimmed.contains("mongodb")) {
                        return trimmed.substring(4).trim().replaceAll("^[\"']|[\"']$", "");
                    }
                }
            }
        } catch (Exception ignored) {}
        return null;
    }

    private List<String> extractPlaceholders(String value) {
        List<String> vars = new ArrayList<>();
        if (value == null) return vars;
        Matcher m = Pattern.compile("\\$\\{([^}:]+)(?::[^}]*)?}").matcher(value);
        while (m.find()) vars.add(m.group(1));
        return vars;
    }

    private String tryResolve(String value, Properties props) {
        if (value == null || !value.contains("${")) return value;
        Matcher m = Pattern.compile("\\$\\{([^}:]+)(?::([^}]*))?}").matcher(value);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            String key = m.group(1);
            String def = m.group(2);
            String resolved = props.getProperty(key, def);
            if (resolved == null) return null; // unresolvable
            m.appendReplacement(sb, Matcher.quoteReplacement(resolved));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private String extractDbFromUri(String uri) {
        if (uri == null) return null;
        Matcher m = Pattern.compile("mongodb(?:\\+srv)?://[^/]*/([^?&#]+)").matcher(uri);
        return m.find() ? m.group(1) : null;
    }

    // ── Inner types ───────────────────────────────────────────────────────────

    public record HistoryEntry(
            String       runId,
            String       mode,
            List<String> endpoints,
            long         timestamp,
            int          highCount,
            int          medCount,
            int          lowCount,
            int          datasetCount
    ) {}

    public record FlowRunRequest(
            String       mode,
            List<String> endpointPaths,
            String       mongoUri,
            String       mongoDb,
            String       claudeApiKey   // fallback only — used when Claude CLI not available
    ) {}

}
