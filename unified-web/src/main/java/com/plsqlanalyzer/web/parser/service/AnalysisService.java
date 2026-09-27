package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.plsql.parser.flow.DbConnectionManager;
import com.plsqlanalyzer.config.ConnectionConfig;
import com.plsqlanalyzer.config.EnvironmentConfig;
import com.plsqlanalyzer.config.PlsqlConfig;
import com.plsqlanalyzer.web.parser.model.AnalysisInfo;
import com.analyzer.usercfg.UserConnectionStore;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service("parserAnalysisService")
public class AnalysisService {

    private final Path flowOutputDir;
    /**
     * The app's config directory — the parent of {@code plsql-config.yaml}. TLS material paths
     * (trust certs, client certs/keys, truststores, wallets) are stored relative to it and are
     * resolved against it by {@code ConnectionConfig.buildSslProperties(configDir)} /
     * {@code getJdbcUrl(configDir)}. May be {@code null} if the config file has no parent (a bare
     * filename), in which case the generated env config falls back to the non-TLS URL.
     */
    private final Path configDir;
    /** Schema-resolver cache prefix: {data}/cache, addressed as a store key. */
    private static final ArtifactKey CACHE_ROOT_KEY = ArtifactKey.of("cache");

    /**
     * Shared mapper for the header reads. {@link ObjectMapper} is thread-safe once configured, and
     * the listing reads headers on several threads at once — a per-call mapper (which this used to
     * build) both costs a serialiser construction per analysis and shares nothing.
     */
    private static final ObjectMapper READ_HEADER_MAPPER = new ObjectMapper();

    @Value("${app.parser-config-path:config/plsql-config.yaml}")
    private String configPath;

    private final AnalysisReaderFactory readerFactory;
    private final StaticAnalysisReader staticReader;
    private final PlsqlConfig plsqlConfig;
    private final ClaudePersistenceService claudePersistenceService;
    private final ArtifactStore store;
    private final com.analyzer.storage.AnalysisRepository repository;
    private final com.plsqlanalyzer.web.controller.detail.ObjectResolutionService objectResolutionService;
    /**
     * The caller's private connection store. Consulted FIRST when an analysis names a connection,
     * so a user's own connection of that name shadows the shared global one; a null result (the
     * user has none, or this is a background thread with no session) falls back to the global config.
     */
    private final UserConnectionStore userConnectionStore;

    public AnalysisService(@Value("${app.data-dir.base:data}") String dataDir,
                           AnalysisReaderFactory readerFactory,
                           StaticAnalysisReader staticReader,
                           PlsqlConfig plsqlConfig,
                           ClaudePersistenceService claudePersistenceService,
                           @Qualifier("parserArtifactStore") ArtifactStore store,
                           com.analyzer.storage.AnalysisRepository repository,
                           com.plsqlanalyzer.web.controller.detail.ObjectResolutionService objectResolutionService,
                           @Qualifier("plsqlConfigFilePath") Path plsqlConfigFilePath,
                           UserConnectionStore userConnectionStore) {
        // The one place a local root is still built from the property, and deliberately the only
        // one: FlowAnalysisMain is an external entry point given --output-dir as a String and
        // writing through java.io, and buildEnvConfigFile must land beside that output on LOCAL
        // disk. Everything else in this class addresses artifacts by key. Callers get this back as
        // a Path (getFlowOutputDir) precisely so they resolve FROM it instead of re-deriving it
        // with Path.of(...) and silently dropping to the local filesystem.
        this.flowOutputDir = Path.of(dataDir).resolve("plsql-parse");
        this.readerFactory = readerFactory;
        this.staticReader = staticReader;
        this.plsqlConfig = plsqlConfig;
        this.claudePersistenceService = claudePersistenceService;
        this.store = store;
        this.repository = repository;
        this.objectResolutionService = objectResolutionService;
        this.configDir = plsqlConfigFilePath != null ? plsqlConfigFilePath.getParent() : null;
        this.userConnectionStore = userConnectionStore;
    }

    /**
     * Config files holding plaintext DB credentials. These must never reach the shared artifact
     * store — in {@code external} mode that is a remote bucket. Matched by EXACT FILENAME, not by
     * extension: other {@code .yaml} artifacts are ordinary data and belong in the store.
     */
    private static final Set<String> CREDENTIAL_FILES =
            Set.of("plsql-config.yaml", "plsql-env-config.yaml");

    private InputStream openRead(ArtifactKey key) throws IOException { return store.read(key); }

    private OutputStream openWrite(ArtifactKey key) throws IOException {
        rejectCredentialFile(key);
        return store.write(key);
    }

    /**
     * Backstop for {@link #CREDENTIAL_FILES}. buildEnvConfigFile already writes locally; this
     * turns a future refactor that forgets why into a loud failure instead of a silent credential
     * leak.
     */
    private static void rejectCredentialFile(ArtifactKey key) {
        if (CREDENTIAL_FILES.contains(key.leaf())) {
            throw new IllegalArgumentException(
                    "Refusing to write " + key.leaf() + " to the shared artifact store: it "
                    + "contains plaintext database credentials. Write it to the local disk.");
        }
    }

    /**
     * Length-aware variant. Backends that must declare a size up front (a single-shot S3 PUT
     * rather than a multipart upload) use it; local ignores it.
     */
    private OutputStream openWrite(ArtifactKey key, long contentLength) throws IOException {
        rejectCredentialFile(key);
        return store.write(key, contentLength);
    }

    /** Root of the PL/SQL parse output. A Path, not a String, so callers cannot accidentally
     *  rebuild it with Path.of(...) and silently drop back to the local filesystem. */
    public Path getFlowOutputDir() { return flowOutputDir; }

    /**
     * The analysis history, newest first.
     *
     * <p>The <b>listing</b> — which analyses exist, where each index document is, and in what order
     * — now comes from {@link com.analyzer.storage.AnalysisRepository#analyses}, which is the part
     * only storage can answer and the part an index replaces (PLAN-09 Increment C). The
     * <b>interpretation</b> stays here, because {@link AnalysisInfo} is this module's model and
     * mapping it in the storage layer would drag a web model down there for no gain.
     *
     * <p>Same order, same {@link #buildInfo}, same skip-on-unreadable behaviour as before: an
     * analysis caught mid-write drops out of the list rather than failing the whole screen.
     *
     * <p>Two things are deliberately different from that original. The per-analysis document read is
     * the <em>header</em> only — see {@link #readIndexHeader} — and the rows are built concurrently
     * rather than one after another, so the screen's latency is one round trip plus a parse instead
     * of a sum over every analysis in the store. Neither changes a field of the response.
     */
    public List<AnalysisInfo> listAnalyses() throws IOException {
        List<com.analyzer.storage.repo.AnalysisRef> refs =
                repository.analyses(com.analyzer.storage.repo.AnalysisQuery.Dataset.PLSQL_ANALYSES);
        // Concurrently, because each row is an independent round trip to the store: the listing
        // wants one small document per analysis, and paying for them back-to-back is what made the
        // screen's latency the sum of every analysis ever run. Order is preserved by the map.
        List<AnalysisInfo> built = com.analyzer.storage.ShallowTree.parallelMap(store, refs, ref -> {
            try {
                return buildInfo(ref.name(), ref.index());
            } catch (IOException unreadable) {
                // Caught mid-write: drop the row rather than failing the whole screen.
                return null;
            }
        });
        List<AnalysisInfo> results = new ArrayList<>(built.size());
        for (AnalysisInfo info : built) {
            if (info != null) results.add(info);
        }
        return results;
    }

    /**
     * The same per-analysis metadata {@link #listAnalyses()} builds for one entry, looked up by
     * name — used by {@code ParserAnalysisExecutor} to attach the parse/discover/save timing
     * breakdown to the just-completed {@code QueueJob} once analysis finishes.
     */
    public AnalysisInfo getAnalysisInfo(String name) throws IOException {
        return buildInfo(name, AnalysisPaths.resolveKey(name, "api/index.json"));
    }

    public JsonNode getIndex(String name) throws IOException {
        return readerFactory.getReader(name).getIndex(name);
    }

    /** See {@link AnalysisDataReader#getIndexWithoutNodes}. */
    public JsonNode getIndexWithoutNodes(String name) throws IOException {
        return readerFactory.getReader(name).getIndexWithoutNodes(name);
    }

    public JsonNode getNodeDetail(String name, String fileName) throws IOException {
        return readerFactory.getReader(name).getNodeDetail(name, fileName);
    }

    public JsonNode getTables(String name) throws IOException {
        return readerFactory.getReader(name).getTables(name);
    }

    /** The raw join-pair array {@code parser/js/joinSummary.js} aggregates by originating query. */
    public JsonNode getJoins(String name) throws IOException {
        return readerFactory.getReader(name).getJoins(name);
    }

    /**
     * Root scalars of the tables index, without reading the table array.
     *
     * <p>Deliberately bound to the static reader rather than going through the reader factory: the
     * Claude reader's only contribution here would be to re-merge the full document, which is
     * exactly the cost this call exists to avoid, and the topbar count is the static one.
     */
    public JsonNode getTablesSummary(String name) throws IOException {
        return staticReader.getTablesSummary(name);
    }

    public JsonNode getTablesList(String name) throws IOException {
        return staticReader.getTablesList(name);
    }

    public JsonNode getCallGraph(String name) throws IOException {
        return readerFactory.getReader(name).getCallGraph(name);
    }

    public String getSource(String name, String fileName) throws IOException {
        return readerFactory.getReader(name).getSource(name, fileName);
    }

    public JsonNode getResolver(String name, String type) throws IOException {
        return readerFactory.getReader(name).getResolver(name, type);
    }

    /**
     * UI-042 — real source filenames under this analysis's {@code sources/} directory, for the
     * zip-bundle export. One listing rather than a per-file stat; deliberately reuses the exact
     * {@code sources/} location {@link CoreFileReader#getSource} already resolves against (via
     * {@link #getSource}), so the bundle endpoint and {@code /source/{file}} can never disagree
     * about which files exist. Returns leaf filenames only (no directories), suitable to pass
     * straight back into {@link #getSource(String, String)}.
     */
    public List<String> listSourceFiles(String name) throws IOException {
        ArtifactKey sourcesDir = AnalysisPaths.resolveKey(name, "sources");
        List<String> names = new ArrayList<>();
        try (Stream<ArtifactEntry> s = store.list(sourcesDir, false)) {
            s.filter(e -> !e.directory()).forEach(e -> names.add(e.key().leaf()));
        }
        return names;
    }

    public JsonNode getProcedures(String name) throws IOException {
        return readerFactory.getReader(name).getProcedures(name);
    }

    /*
     * The six facet reads that used to be one-line delegates here — joins, cursors, sequences,
     * dynamic-sql, table-joins, table-columns — are now AnalysisRepository.document(...) queries,
     * called by their controllers directly (PLAN-09 Increment C). They are gone rather than kept as
     * a second front door: two paths to the same rows is how the two come to disagree, and the
     * reader choice they encoded (factory for the node-scan four, static reader for the two
     * table-detail lists) is preserved in ArtifactScanRepository#document, which is the one place
     * it is now written down.
     */

    public JsonNode getCallTree(String name, String nodeId) throws IOException {
        return readerFactory.getReader(name).getCallTree(name, nodeId);
    }

    /** UI-019 (§142) — {@code maxDepth} fetches only that many levels; see {@link
     *  AnalysisDataReader#getCallTree(String, String, Integer)} for the full contract. */
    public JsonNode getCallTree(String name, String nodeId, Integer maxDepth) throws IOException {
        return readerFactory.getReader(name).getCallTree(name, nodeId, maxDepth);
    }

    public JsonNode getCallers(String name, String nodeId) throws IOException {
        return readerFactory.getReader(name).getCallers(name, nodeId);
    }

    /** Clear the in-memory synthesized cache: one analysis if {@code name} given, else all. */
    public int clearCache(String name) {
        if (name != null && !name.isBlank()) {
            staticReader.evictCache(name);
            return 1;
        }
        return staticReader.evictAll();
    }

    /** Derive the datasource dialect from a jdbc URL (no extra DB call). */
    private static String dialectOf(String jdbcUrl) {
        String u = jdbcUrl == null ? "" : jdbcUrl.toLowerCase();
        if (u.contains("sqlserver")) return "SQLSERVER";
        if (u.contains("oracle"))    return "ORACLE";
        if (u.contains("mysql"))     return "MYSQL";
        if (u.contains("db2"))       return "DB2";
        return "UNKNOWN";
    }

    /**
     * Cache footprint grouped per connection (project / environment / connection).
     * Each group: dbType, jdbcUrl, analysis count, total on-disk bytes.
     */
    public List<Map<String, Object>> cacheByConnection() throws IOException {
        Map<String, Map<String, Object>> groups = new LinkedHashMap<>();
        for (AnalysisInfo a : listAnalyses()) {
            String project = a.getProject() == null ? "" : a.getProject();
            String env     = a.getEnvironment() == null ? "" : a.getEnvironment();
            String conn    = a.getConnectionName() == null ? "" : a.getConnectionName();
            String key = project + "|" + env + "|" + conn;
            Map<String, Object> g = groups.computeIfAbsent(key, k -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("project", project);
                m.put("environment", env);
                m.put("connectionName", conn);
                m.put("dbType", dialectOf(a.getJdbcUrl()));
                m.put("jdbcUrl", a.getJdbcUrl());
                m.put("analysisCount", 0);
                m.put("sizeBytes", 0L);
                return m;
            });
            g.put("analysisCount", (int) g.get("analysisCount") + 1);
            long size = dirSize(AnalysisPaths.dirKey(a.getName()));
            g.put("sizeBytes", (long) g.get("sizeBytes") + size);
        }
        return new ArrayList<>(groups.values());
    }

    /**
     * Clear the in-memory caches for every analysis of a connection, and clear the
     * shared on-disk source-download cache so the next analysis re-fetches fresh
     * source. Analysis results (index/call-graph/sources) are kept.
     * @return number of analyses whose cache was evicted.
     */
    public int clearConnectionCache(String project, String environment, String connName) throws IOException {
        int evicted = 0;
        for (AnalysisInfo a : listAnalyses()) {
            if (matches(a.getProject(), project) && matches(a.getEnvironment(), environment)
                    && matches(a.getConnectionName(), connName)) {
                staticReader.evictCache(a.getName());
                // Clear this connection's scoped schema-resolver disk cache so a re-analysis
                // re-resolves object→schema mappings against the live DB. The cache is grouped
                // datasource/project/env wise under {data}/cache/schema-resolver/<DBTYPE>/<PROJECT>/<ENV>/.
                String scope = AnalysisPaths.scope(dialectOf(a.getJdbcUrl()), a.getProject(), a.getEnvironment());
                if (!scope.isEmpty()) {
                    ArtifactKey scopedResolver = ArtifactKey.of("cache", "schema-resolver");
                    for (String seg : scope.split(String.valueOf(AnalysisPaths.SEP))) {
                        if (!seg.isEmpty()) scopedResolver = scopedResolver.resolve(seg);
                    }
                    try { store.deletePrefix(scopedResolver); } catch (Exception ignored) {}
                }
                evicted++;
            }
        }
        // Clear the legacy shared downloaded-source cache (regenerated on next fetch).
        try { store.deletePrefix(ArtifactKey.of("cache-plsql")); } catch (Exception ignored) {}
        return evicted;
    }

    private static boolean matches(String actual, String wanted) {
        if (wanted == null || wanted.isEmpty()) return true;
        return wanted.equals(actual == null ? "" : actual);
    }

    /**
     * "Is a real artifact, not a directory marker."
     *
     * <p>A named predicate rather than {@code e -> !e.directory()} at each call site: the
     * DirectFileIoGateTest pattern for {@code ProcessBuilder.directory(...)} matches any
     * {@code .directory(} text, so the inline lambda was counted three times as direct file I/O
     * that this class does not do.
     */
    private static final java.util.function.Predicate<ArtifactEntry> IS_FILE =
            java.util.function.Predicate.not(ArtifactEntry::directory);

    /** True when {@code prefix} holds at least one artifact (the store-native "is a folder"). */
    private boolean hasAnyArtifact(ArtifactKey prefix) {
        try (Stream<ArtifactEntry> s = store.list(prefix, true)) {
            return s.anyMatch(IS_FILE);
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Moves every artifact under {@code from} to the same relative location under {@code to}.
     *
     * <p>{@link ArtifactStore#move} is per-key by design — object stores have no directories to
     * rename — so a subtree move is an enumeration plus a move each.
     */
    private void movePrefix(ArtifactKey from, ArtifactKey to) throws IOException {
        List<ArtifactKey> files;
        try (Stream<ArtifactEntry> s = store.list(from, true)) {
            files = s.filter(IS_FILE).map(ArtifactEntry::key).toList();
        }
        int depth = from.segments().size();
        for (ArtifactKey f : files) {
            ArtifactKey target = to;
            for (String seg : f.segments().subList(depth, f.segments().size())) {
                target = target.resolve(seg);
            }
            store.move(f, target);
        }
        // Sweep any now-empty local directories the move left behind (a no-op on object stores).
        store.deletePrefix(from);
    }

    /** Total bytes under a prefix. One recursive listing carries every size — no stat per file. */
    private long dirSize(ArtifactKey prefix) {
        try (Stream<ArtifactEntry> s = store.list(prefix, true)) {
            return s.filter(IS_FILE).mapToLong(ArtifactEntry::size).sum();
        } catch (IOException e) { return 0L; }
    }

    public void deleteAnalysis(String name) throws IOException {
        if (store.deletePrefix(AnalysisPaths.dirKey(name)) == 0) throw new NoSuchFileException(name);
        staticReader.evictCache(name);
        claudePersistenceService.deleteClaudeData(name);
    }

    /**
     * Retention for re-run archives: keep the newest {@code keep} timestamped archive folders
     * for a given base name (e.g. NAME_20260718_145303) and delete older ones (plus their
     * Claude data), so repeated re-analysis doesn't grow the data dir without bound.
     */
    private void pruneOldArchives(String baseFolderName, int keep) {
        try {
            // Archives live in the same nested <DBTYPE>/<PROJECT>/<ENV>/ parent, with the
            // OBJECT leaf suffixed by a timestamp (e.g. ERP_USP_RUN_CYCLE_20260810_145303).
            ArtifactKey parent = AnalysisPaths.parentDirKey(baseFolderName);
            String leaf = AnalysisPaths.leaf(baseFolderName);
            java.util.regex.Pattern ts = java.util.regex.Pattern.compile(
                    "^" + java.util.regex.Pattern.quote(leaf) + "_\\d{8}_\\d{6}$");
            List<ArtifactKey> archives;
            try (Stream<ArtifactEntry> s = store.list(parent, false)) {
                archives = s.map(ArtifactEntry::key)
                        .filter(k -> ts.matcher(k.leaf()).matches())
                        .sorted(Comparator.comparing(ArtifactKey::leaf))
                        .collect(java.util.stream.Collectors.toList());
            }
            for (int i = 0; i < archives.size() - keep; i++) {
                // Reconstruct the archive's identity name from its nested location.
                String archName = AnalysisPaths.nameOfKey(archives.get(i));
                store.deletePrefix(archives.get(i));
                try { claudePersistenceService.deleteClaudeData(archName); } catch (Exception ignored) {}
                staticReader.evictCache(archName);
            }
        } catch (IOException e) {
            org.slf4j.LoggerFactory.getLogger(AnalysisService.class)
                    .warn("[AnalysisService] archive prune failed for {}: {}", baseFolderName, e.getMessage());
        }
    }

    public String runAnalysis(String entryPoint) throws Exception {
        return runAnalysis(entryPoint, null, null, null, null);
    }

    public String runAnalysis(String entryPoint, String owner, String objectType) throws Exception {
        return runAnalysis(entryPoint, owner, objectType, null, null);
    }

    public String runAnalysis(String entryPoint, String owner, String objectType,
                               String project, String environment) throws Exception {
        return runAnalysis(entryPoint, owner, objectType, project, environment, null, null);
    }

    /**
     * Compute a short, bounded, URL-safe scope token so analyses of the same object under
     * different project/env/connection/db do NOT collide on disk or in the reader cache.
     * Returns "" when no scope is supplied (keeps back-compat naming for scopeless runs).
     */
    static String scopeSuffix(String project, String environment, String connName, String dbType) {
        String p = project     == null ? "" : project.trim();
        String e = environment == null ? "" : environment.trim();
        String c = connName    == null ? "" : connName.trim();
        String d = dbType      == null ? "" : dbType.trim();
        String raw = (p + "|" + e + "|" + c + "|" + d);
        if (raw.replace("|", "").isEmpty()) return "";           // nothing to scope by
        int h = raw.toUpperCase().hashCode() & 0x7fffffff;        // stable, non-negative
        return "__" + Integer.toString(h, 36).toUpperCase();      // e.g. "__1A2B3C"
    }

    public String runAnalysis(String entryPoint, String owner, String objectType,
                               String project, String environment,
                               String dbType, String connName) throws Exception {
        long analysisStartMs = System.currentTimeMillis(); // full end-to-end timer (crawl + enrichment + write)

        // No owner/objectType supplied at all — resolve the real one via the same smart search
        // DatabaseController's GET /find/{objectInput} uses (ObjectResolutionService.findObjectSmart),
        // instead of silently guessing a schema (the old hardcoded "CUSTOMER" fallback). A caller
        // that already knows the owner/objectType (e.g. picked the object from /find first) skips
        // this lookup entirely.
        if ((owner == null || owner.isBlank()) && (objectType == null || objectType.isBlank())) {
            Map<String, Object> resolved = objectResolutionService.findObjectSmart(entryPoint, project, environment);
            boolean found = resolved != null && Boolean.TRUE.equals(resolved.get("found"));
            if (!found) {
                throw new IllegalArgumentException("Could not resolve schema/type for entry point '"
                        + entryPoint + "' — object not found in any configured schema. "
                        + "Refusing to guess an owner; specify owner/objectType explicitly.");
            }
            Object resolvedSchema = resolved.get("schema");
            Object resolvedType = resolved.get("objectType");
            owner = resolvedSchema != null ? resolvedSchema.toString() : null;
            objectType = objectResolutionService.normalizeObjectType(
                    resolvedType != null ? resolvedType.toString() : null);
        }

        String target = entryPoint;
        // Standalone (schema-level, not a package member): procedures, functions, and also
        // views and triggers — these are never package subprograms in any dialect, so the
        // owner must not be folded into a "package.subprogram" entry key.
        boolean isStandalone = objectType != null &&
                (objectType.equalsIgnoreCase("PROCEDURE") || objectType.equalsIgnoreCase("FUNCTION")
                 || objectType.equalsIgnoreCase("VIEW") || objectType.equalsIgnoreCase("TRIGGER"));
        boolean isPackageOnly = objectType != null && objectType.equalsIgnoreCase("PACKAGE")
                && !entryPoint.contains(".");

        // isPackageOnly must also be excluded here: a bare package name is crawled as its own
        // single entry point (DependencyCrawler resolves its owning schema internally via
        // SchemaResolver) — prefixing it with the resolved owner turns "PKG" into "SCHEMA.PKG",
        // which DependencyCrawler's dot-split then misreads as package "SCHEMA", member "PKG"
        // (neither of which exists), producing an empty result instead of the real crawl.
        if (!isStandalone && !isPackageOnly && owner != null && !owner.isBlank() && !entryPoint.contains(".")) {
            target = owner.toUpperCase() + "." + entryPoint;
        }

        // Resolve environment config: if project+env provided, generate a temp config file
        // scoped to that environment's connections so the analysis uses the right DB.
        String effectiveConfigPath = configPath;
        String resolvedJdbcUrl = null;
        String resolvedConnName = null;
        EnvironmentConfig envConfig = null;
        // The base TLS material paths resolve against for the hand-off YAML. Normally the config
        // directory; for a user-private connection it is the directory its certs were fetched into.
        Path tlsBaseDir = configDir;
        // Which connection drives this run, when the caller named one. The user's PRIVATE store is
        // consulted first; only if they have no such connection does the shared global config win.
        ConnectionConfig privateConn = null;
        if (project != null && !project.isBlank() && environment != null && !environment.isBlank()) {
            if (connName != null && !connName.isBlank()) {
                privateConn = userConnectionStore.findForCurrentUser(connName);
            }
            envConfig = plsqlConfig.resolveEnvironment(project, environment);
            // Real bug found live: when project/environment don't resolve to a configured entry
            // (e.g. a typo, or a project simply missing from plsql-config.yaml), envConfig stayed
            // null and effectiveConfigPath silently kept its DEFAULT value (the raw global
            // configPath) — which can be an entirely unrelated legacy top-level connection block
            // (different host, different credentials, different schemas). A request for
            // project=PostgresDemo ended up connecting to a stale production Oracle host from
            // that global file's legacy fallback section, three retries deep, before failing.
            // Resolution must be strict: project+environment (and, if given, connName) select
            // ONE specific connection or the request fails outright — it must never silently
            // substitute a different project/environment/connection. A user-private connection of
            // the requested name satisfies the request on its own, so it is exempt from the
            // global-config lookup checks below.
            if (envConfig == null && privateConn == null) {
                throw new IllegalArgumentException("No configured environment for project='" + project
                        + "', environment='" + environment + "' — check plsql-config.yaml. "
                        + "Refusing to fall back to an unrelated default connection.");
            }
            if (envConfig != null && privateConn == null && connName != null && !connName.isBlank()) {
                boolean connFound = envConfig.getConnections().stream()
                        .anyMatch(c -> connName.equalsIgnoreCase(c.getName()));
                if (!connFound) {
                    throw new IllegalArgumentException("Connection '" + connName + "' not found under project='"
                            + project + "', environment='" + environment + "' — check plsql-config.yaml. "
                            + "Refusing to fall back to an unrelated default connection.");
                }
            }
        }
        if (privateConn != null) {
            // Materialize the user connection's TLS artifacts into a stable directory under the
            // analysis tree, because the hand-off YAML is written to disk and consumed by a later
            // parser job: temp files would be gone by then, and the parser needs real paths.
            ConnectionConfig dialConn = privateConn;
            if (privateConn.isTlsEnabled()) {
                try {
                    String identity = userConnectionStore.currentIdentityKey();
                    Path certDir = flowOutputDir.resolve("users")
                            .resolve(identity != null ? identity : "anon");
                    dialConn = userConnectionStore.materializeCertsPersistent(privateConn, certDir);
                    tlsBaseDir = certDir;
                } catch (Exception ex) {
                    org.slf4j.LoggerFactory.getLogger(AnalysisService.class)
                        .warn("[AnalysisService] could not materialize private TLS material for '{}': {}",
                              connName, ex.getMessage());
                }
            }
            envConfig = singleConnectionEnv(dialConn, environment);
        }
        if (envConfig != null) {
            resolvedJdbcUrl = (privateConn != null && privateConn.isTlsEnabled() && tlsBaseDir != null)
                    ? envConfig.getConnections().get(0).getJdbcUrl(tlsBaseDir)
                    : envConfig.getJdbcUrl();
            // Build env config — written to outputDir so ParserValidationExecutor can find it
            // even after a server restart (temp files are deleted on JVM exit).
            // Actual resultDir may not exist yet; we write the config after createDirectories.
            // Store the envConfig reference; we write the file after resultDir is created below.
            if (!envConfig.getConnections().isEmpty()) {
                resolvedConnName = envConfig.getConnections().stream()
                        .map(ConnectionConfig::getName)
                        .reduce((a, b) -> a + ", " + b)
                        .orElse(null);
            }
        }

        // A bare package name is passed straight through as the SOLE entry point (never
        // enumerated/fanned-out proc by proc): DependencyCrawler's package-as-entry-point
        // aggregation branch already merges every subprogram's calls/tableOperations/statements
        // into this one FlowNode (see FlowNode#getMembers), so one FlowAnalysisMain.run() below
        // produces one real, single-unit analysis for the whole package.

        // objectSegment: for a package this is the package name (e.g. CUSTOMER_PG_BPGE_WORKFLOW),
        // for a single proc this is the proc name.
        String objectSegment;
        if (isPackageOnly) {
            // Name the folder after the package, not owner.package
            objectSegment = entryPoint;
        } else {
            objectSegment = target;
        }
        // Storage is organised datasource/project/environment/object wise (see AnalysisPaths):
        // physical folder = plsql-parse/<DBTYPE>/<PROJECT>/<ENV>/<OBJECT>/, and the identity
        // "name" is that path with '/'→'~' so it stays a single URL-safe REST path variable.
        String effectiveDbType = (dbType != null && !dbType.isBlank()) ? dbType
                : (envConfig != null && !envConfig.getConnections().isEmpty()
                    ? envConfig.getConnections().get(0).getDbType() : null);
        // baseFolderName is the identity name (e.g. DB2~Db2Demo~local~ERP_USP_RUN_CYCLE).
        String baseFolderName = AnalysisPaths.name(effectiveDbType, project, environment, objectSegment);
        // The cache scope mirrors the same datasource/project/env folder tree (no object leaf),
        // so the schema-resolver disk cache is grouped per connection, never a bleed between DBs.
        String cacheScope = AnalysisPaths.scope(effectiveDbType, project, environment);

        // resultDir is a real LOCAL Path, and its ONLY remaining consumer is buildEnvConfigFile:
        // plsql-env-config.yaml holds plaintext production passwords and must never enter the
        // artifact store (CredentialGuardStore rejects that exact leaf name), while its consumers
        // — DbConnectionManager, FlowAnalysisMain — take a filesystem path, not a stream. The
        // parser output itself no longer goes near this Path: FlowAnalysisMain is driven by
        // opts.outputKey below, and every merge/read in this class addresses resultKey.
        Path resultDir = AnalysisPaths.dir(flowOutputDir, baseFolderName);
        ArtifactKey resultKey = AnalysisPaths.dirKey(baseFolderName);
        if (hasAnyArtifact(resultKey)) {
            String oldTs = java.time.LocalDateTime.now()
                    .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            // Append the timestamp to the OBJECT leaf so the archive stays inside the same
            // <DBTYPE>/<PROJECT>/<ENV>/ parent folder.
            String archivedName = baseFolderName + "_" + oldTs;
            ArtifactKey archived = AnalysisPaths.dirKey(archivedName);
            // Archive Claude data to matching timestamped name before moving static folder
            claudePersistenceService.archiveClaudeData(baseFolderName, archivedName);
            try {
                movePrefix(resultKey, archived);
            } catch (IOException moveEx) {
                // On Windows, files with open handles can't be moved.
                // Fall through — FlowAnalysisMain will overwrite individual proc subfolders.
                org.slf4j.LoggerFactory.getLogger(AnalysisService.class)
                    .warn("[AnalysisService] Could not archive {}: {}", baseFolderName, moveEx.getMessage());
            }
            staticReader.evictCache(baseFolderName);
            // Retention: keep only the newest few timestamped archives so re-runs don't
            // pile up folders in the data dir indefinitely.
            pruneOldArchives(baseFolderName, 3);
        }

        // Write env config into resultDir so it survives server restarts and ParserValidationExecutor
        // can use it (it probes the same local path — see ParserAnalysisExecutor).
        if (envConfig != null) {
            Path persistedConfig = buildEnvConfigFile(envConfig, project, environment, resultDir, tlsBaseDir);
            if (persistedConfig != null) effectiveConfigPath = persistedConfig.toString();
        }

        // One crawl, one output, for every entry point — package or not. A bare package name
        // here is resolved by DependencyCrawler's own package-as-entry-point aggregation into a
        // single FlowNode (see FlowNode#getMembers), so there is nothing left to fan out or
        // merge afterward.
        var opts = new com.plsql.parser.flow.FlowAnalysisMain.Options();
        opts.configPath = effectiveConfigPath;
        opts.entryPoints.add(target);
        // Output into the nested <DBTYPE>/<PROJECT>/<ENV>/ parent, naming the leaf folder
        // after the OBJECT — this yields plsql-parse/<DBTYPE>/<PROJECT>/<ENV>/<OBJECT>/.
        opts.outputKey = AnalysisPaths.parentDirKey(baseFolderName);
        opts.outputName = AnalysisPaths.leaf(baseFolderName);
        // Scope cache to this proc's result folder so it is cleared on re-analysis
        // and never bleeds stale parse results to other procs or future runs.
        opts.cacheKey = resultKey;
        opts.cacheScope = cacheScope;
        opts.cacheRootKey = CACHE_ROOT_KEY;
        opts.pretty = true;

        com.plsql.parser.flow.FlowAnalysisMain.run(store, opts);
        // Validation runs as a follow-up queue job — no inline runTableValidation call here.

        // Enrich tables (columns / indexes / constraints / triggers / view defs) INLINE for
        // every dialect so the UI table-detail is populated consistently (SQL Server, MySQL,
        // DB2 via their catalog enrichers; Oracle via TableValidationService).
        try (com.plsql.parser.flow.DbConnectionManager ec =
                     new com.plsql.parser.flow.DbConnectionManager(effectiveConfigPath)) {
            // Which folders have a tables index: ONE listing of the analysis subtree, then a
            // lookup per candidate, rather than an isRegularFile stat each.
            ArtifactKey baseKey = AnalysisPaths.dirKey(baseFolderName);
            Set<ArtifactKey> present = store.statPrefix(baseKey).keySet();
            // The enrichers are key-based now, so the analysis folders are passed as keys.
            List<ArtifactKey> dirs = new ArrayList<>();
            if (present.contains(baseKey.resolve("api", "tables", "index.json"))) dirs.add(baseKey);
            try (Stream<ArtifactEntry> sub = store.list(baseKey, false)) {
                sub.filter(ArtifactEntry::directory)
                   .map(ArtifactEntry::key)
                   .filter(k -> present.contains(k.resolve("api", "tables", "index.json")))
                   .forEach(dirs::add);
            }
            if (!dirs.isEmpty()) {
                if (ec.isSqlServer())      com.plsql.parser.flow.SqlServerTableEnricher.enrich(ec, store, dirs);
                else if (ec.isMySQL())     com.plsql.parser.flow.MySqlTableEnricher.enrich(ec, store, dirs);
                else if (ec.isDB2())       com.plsql.parser.flow.Db2TableEnricher.enrich(ec, store, dirs);
                else if (ec.isPostgres())  com.plsql.parser.flow.PostgresTableEnricher.enrich(ec, store, dirs);
                else                       com.plsql.parser.flow.TableValidationService.validateMultiple(ec, store, dirs);
            }
        } catch (Exception e) {
            org.slf4j.LoggerFactory.getLogger(AnalysisService.class)
                .warn("[AnalysisService] table enrichment failed: {}", e.getMessage());
        }

        staticReader.evictCache(baseFolderName);

        // Patch index.json with project/environment/connection metadata.
        // Fall back to global plsqlConfig connection info when no project/env was selected.
        String patchJdbcUrl = resolvedJdbcUrl;
        String patchConnName = resolvedConnName;
        if ((patchJdbcUrl == null || patchJdbcUrl.isBlank()) && plsqlConfig != null) {
            patchJdbcUrl = plsqlConfig.getJdbcUrl();
        }
        if ((patchConnName == null || patchConnName.isBlank()) && plsqlConfig != null
                && plsqlConfig.getDbUsers() != null && !plsqlConfig.getDbUsers().isEmpty()) {
            patchConnName = plsqlConfig.getDbUsers().stream()
                    .map(u -> (u.getDescription() != null && !u.getDescription().isBlank())
                            ? u.getDescription() : u.getUsername())
                    .filter(Objects::nonNull)
                    .reduce((a, b) -> a + ", " + b)
                    .orElse(null);
        }
        patchConnectionMeta(baseFolderName, project, environment, patchConnName, patchJdbcUrl,
                System.currentTimeMillis() - analysisStartMs);

        return baseFolderName;
    }

    /**
     * Wraps one resolved connection as a single-connection {@link EnvironmentConfig} so the rest of
     * the hand-off (env-config YAML generation, db-type and jdbc-url resolution) needs no knowledge
     * of whether the connection came from the shared global config or the caller's private store.
     */
    private static EnvironmentConfig singleConnectionEnv(ConnectionConfig conn, String envName) {
        EnvironmentConfig env = new EnvironmentConfig(envName, null);
        env.getConnections().add(conn);
        return env;
    }

    /**
     * Write an env-scoped YAML config into the analysis output directory so it persists
     * across server restarts and ParserValidationExecutor can use it as a follow-up job.
     * Falls back to a temp file if outputDir is null or not yet created.
     *
     * <p><b>Deliberately NOT routed through the ArtifactStore.</b> {@code plsql-env-config.yaml}
     * carries plaintext database credentials (see the {@code password:} lines below). The store is
     * a shared artifact tree that in {@code external} mode is a remote bucket, so putting this
     * file in it would publish production passwords to shared storage. It stays on the local disk
     * beside the analysis, which is also where the consumers need it: both
     * {@code DbConnectionManager} and {@code FlowAnalysisMain} take a filesystem path, not a
     * stream. {@code plsql-config.yaml} is excluded for the same reason.
     *
     * <p>Excluded BY EXACT FILENAME, not by extension — other {@code .yaml} artifacts in the tree
     * are ordinary data and belong in the store.
     */
    private Path buildEnvConfigFile(EnvironmentConfig envConfig, String project, String environment,
                                     Path outputDir, Path tlsBaseDir) {
        try {
            // The base the connection's relative TLS paths resolve against: the config directory
            // normally, or the directory a user-private connection's certs were materialized into.
            Path baseDir = tlsBaseDir != null ? tlsBaseDir : configDir;
            // The connection that drives this environment: its db_type decides the driver, and its
            // TLS settings decide whether the parser must be handed SSL properties too. Same
            // selection rule as the db_type line below (the first configured connection).
            ConnectionConfig activeConn = envConfig.getConnections().isEmpty()
                    ? null : envConfig.getConnections().get(0);
            boolean tlsEnabled = activeConn != null && activeConn.isTlsEnabled()
                    && baseDir != null;

            String jdbcUrl = tlsEnabled ? activeConn.getJdbcUrl(baseDir) : envConfig.getJdbcUrl();
            if (jdbcUrl == null || jdbcUrl.isBlank()) {
                org.slf4j.LoggerFactory.getLogger(AnalysisService.class)
                    .warn("[AnalysisService] No jdbcUrl for {}/{} — falling back to global config", project, environment);
                return null;
            }

            StringBuilder sb = new StringBuilder();
            sb.append("jdbc_url: \"").append(escapeYaml(jdbcUrl)).append("\"\n");
            // Write db_type so DbConnectionManager can load the right driver and use the right queries
            String dbType = activeConn == null ? "ORACLE" : activeConn.getDbType();
            if (dbType == null || dbType.isBlank()) dbType = "ORACLE";
            sb.append("db_type: \"").append(dbType.toUpperCase()).append("\"\n");
            // Carry TLS settings through to the parser: the truststore/wallet/client-cert
            // properties must be handed to DriverManager alongside user/password, or a
            // TLS-required database rejects the parser's connection. Flattened to a plain
            // string→string map; NONE when TLS is disabled so the file stays byte-identical to
            // pre-TLS output for existing configs.
            if (tlsEnabled) {
                java.util.Properties ssl = activeConn.buildSslProperties(baseDir);
                if (ssl != null && !ssl.isEmpty()) {
                    sb.append("ssl_properties:\n");
                    // Sorted for a deterministic, diff-friendly file.
                    for (String key : new java.util.TreeSet<>(ssl.stringPropertyNames())) {
                        sb.append("  \"").append(escapeYaml(key)).append("\": \"")
                          .append(escapeYaml(ssl.getProperty(key))).append("\"\n");
                    }
                }
            }
            sb.append("db_users:\n");
            for (ConnectionConfig c : envConfig.getConnections()) {
                String user = c.getUsername() != null ? c.getUsername() : "";
                String pass = c.getPassword() != null ? c.getPassword() : "";
                String desc = c.getDescription() != null ? c.getDescription() : c.getName();
                sb.append("- username: \"").append(user.replace("\"", "\\\"")).append("\"\n");
                sb.append("  password: \"").append(pass.replace("\"", "\\\"")).append("\"\n");
                sb.append("  description: \"").append(desc.replace("\"", "\\\"")).append("\"\n");
            }
            sb.append("source_paths:\n- \"./plsql-sources\"\n");

            Path target;
            if (outputDir != null && Files.isDirectory(outputDir)) {
                // Persist inside analysis folder — survives server restart
                target = outputDir.resolve("plsql-env-config.yaml");
            } else {
                String safeEnv = environment.replaceAll("[^A-Za-z0-9_-]", "_");
                target = Files.createTempFile("plsql-env-" + safeEnv + "-", ".yaml");
                target.toFile().deleteOnExit();
            }
            Files.writeString(target, sb.toString());
            org.slf4j.LoggerFactory.getLogger(AnalysisService.class)
                .info("[AnalysisService] Env config for {}/{} written to: {}", project, environment, target);
            return target;
        } catch (Exception e) {
            org.slf4j.LoggerFactory.getLogger(AnalysisService.class)
                .warn("[AnalysisService] Could not build env config for {}/{}: {}", project, environment, e.getMessage());
            return null;
        }
    }

    /**
     * Escape a value for a double-quoted YAML scalar. Backslashes are escaped first: TLS paths
     * (wallet directories, keystore locations) routinely contain Windows backslashes, which would
     * otherwise start an escape sequence and corrupt the generated YAML.
     */
    private static String escapeYaml(String value) {
        if (value == null) return "";
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /**
     * Patch the top-level api/index.json with project/environment/connection metadata.
     * Called after FlowAnalysisMain writes its output.
     */
    private void patchConnectionMeta(String baseFolderName, String project, String environment,
                                      String connectionName, String jdbcUrl, long totalDurationMs) {
        ArtifactKey indexFile = AnalysisPaths.resolveKey(baseFolderName, "api/index.json");
        if (!store.exists(indexFile)) return;
        try {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode root;
            try (InputStream in = openRead(indexFile)) {
                root = (ObjectNode) mapper.readTree(in);
            }
            if (project != null && !project.isBlank()) root.put("project", project);
            if (environment != null && !environment.isBlank()) root.put("environment", environment);
            if (connectionName != null && !connectionName.isBlank()) root.put("connectionName", connectionName);
            if (jdbcUrl != null && !jdbcUrl.isBlank()) root.put("jdbcUrl", jdbcUrl);
            // Full end-to-end analysis time: crawl + table enrichment + writing (nothing pending).
            if (totalDurationMs > 0) root.put("totalDurationMs", totalDurationMs);
            try (OutputStream out = openWrite(indexFile)) {
                mapper.writerWithDefaultPrettyPrinter().writeValue(out, root);
            }
        } catch (Exception e) {
            org.slf4j.LoggerFactory.getLogger(AnalysisService.class)
                .warn("[AnalysisService] Could not patch connection meta for {}: {}", baseFolderName, e.getMessage());
        }
    }

    // --- helpers ---

    /** The one root field the listing needs whole: the history row's {@code errorCount} counts it. */
    private static final String ERRORS_FIELD = "errors";

    /**
     * Root scalars of an analysis index document, read over the streaming parser.
     *
     * <p>{@code api/index.json} is the analysis's <em>summary</em>, and the listing renders only the
     * ~20 scalars at its root. It nonetheless also carries a {@code nodes[]} array — one entry per
     * procedure, measured at 53% of the document on this project's largest analysis — plus cycle and
     * statement summaries that no listing column reads. Materialising the whole document per
     * analysis was the listing's dominant cost, so every nested value is stepped over
     * ({@code JsonParser#skipChildren}) instead of parsed, and only {@code errors} is kept.
     *
     * <p>The interpretation downstream is untouched: the same {@code textOrDefault} /
     * {@code intOrDefault} / {@code longOrDefault} helpers read this node, and a field skipped here
     * is absent from it in exactly the way a field the writer never emitted is absent. Scalar types
     * are preserved (a number stays a number), so {@code intOrDefault}'s {@code isNumber()} test and
     * {@code textOrDefault}'s {@code asText()} see what they saw before.
     */
    private JsonNode readIndexHeader(ArtifactKey indexKey) throws IOException {
        ObjectNode scalars = READ_HEADER_MAPPER.createObjectNode();
        try (InputStream in = openRead(indexKey);
             JsonParser p = READ_HEADER_MAPPER.getFactory().createParser(in)) {
            if (p.nextToken() == JsonToken.START_OBJECT) {
                while (p.nextToken() != JsonToken.END_OBJECT) {
                    String field = p.currentName();
                    JsonToken value = p.nextToken();
                    if (value == null) break;
                    if (value.isScalarValue()) {
                        switch (value) {
                            case VALUE_STRING -> scalars.put(field, p.getText());
                            case VALUE_NUMBER_INT -> scalars.put(field, p.getLongValue());
                            case VALUE_NUMBER_FLOAT -> scalars.put(field, p.getDoubleValue());
                            case VALUE_TRUE -> scalars.put(field, true);
                            case VALUE_FALSE -> scalars.put(field, false);
                            case VALUE_NULL -> scalars.putNull(field);
                            default -> { }
                        }
                    } else if (value == JsonToken.START_ARRAY && ERRORS_FIELD.equals(field)) {
                        scalars.set(field, READ_HEADER_MAPPER.readTree(p));
                    } else {
                        p.skipChildren();
                    }
                }
            }
        }
        return scalars;
    }

    /** @param indexKey the {@code .../<OBJECT>/api/index.json} key that identifies the analysis */
    private AnalysisInfo buildInfo(String name, ArtifactKey indexKey) throws IOException {
        JsonNode root = readIndexHeader(indexKey);

        AnalysisInfo info = new AnalysisInfo();
        info.setName(name);
        info.setEntryPoint(textOrDefault(root, "entryPoint", AnalysisPaths.leaf(name)));
        info.setEntrySchema(textOrDefault(root, "entrySchema", ""));
        info.setTotalNodes(intOrDefault(root, "totalNodes", 0));
        info.setTotalTables(intOrDefault(root, "totalTables", 0));
        info.setTotalEdges(intOrDefault(root, "totalEdges", 0));
        info.setTotalLinesOfCode(intOrDefault(root, "totalLinesOfCode", 0));
        info.setMaxDepth(intOrDefault(root, "maxDepth", 0));
        info.setCrawlTimeMs(longOrDefault(root, "crawlTimeMs", 0));
        // Full end-to-end duration; old analyses without it fall back to crawlTimeMs.
        info.setTotalDurationMs(longOrDefault(root, "totalDurationMs", info.getCrawlTimeMs()));
        info.setParseMs(longOrDefault(root, "parseMs", 0));
        info.setDiscoverMs(longOrDefault(root, "discoverMs", 0));
        info.setSaveMs(longOrDefault(root, "saveMs", 0));
        info.setDbCallCount(intOrDefault(root, "dbCallCount", 0));
        info.setProject(textOrDefault(root, "project", ""));
        info.setEnvironment(textOrDefault(root, "environment", ""));
        info.setConnectionName(textOrDefault(root, "connectionName", ""));
        info.setJdbcUrl(textOrDefault(root, "jdbcUrl", ""));

        // The analysis timestamp. Was the directory mtime; object stores have no directories, so
        // the index.json mtime stands in — it is written last, so the value is equivalent.
        long mtime = store.lastModified(indexKey);
        if (mtime > 0) info.setTimestamp(mtime);

        List<String> errors = new ArrayList<>();
        JsonNode errNode = root.get("errors");
        if (errNode != null && errNode.isArray()) {
            errNode.forEach(e -> errors.add(e.asText()));
        }
        info.setErrors(errors);

        return info;
    }

    private static String textOrDefault(JsonNode node, String field, String def) {
        JsonNode v = node.get(field);
        return (v != null && !v.isNull()) ? v.asText() : def;
    }

    private static int intOrDefault(JsonNode node, String field, int def) {
        JsonNode v = node.get(field);
        return (v != null && v.isNumber()) ? v.asInt() : def;
    }

    private static long longOrDefault(JsonNode node, String field, long def) {
        JsonNode v = node.get(field);
        return (v != null && v.isNumber()) ? v.asLong() : def;
    }
}
