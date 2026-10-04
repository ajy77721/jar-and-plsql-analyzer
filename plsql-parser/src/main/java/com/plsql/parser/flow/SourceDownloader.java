package com.plsql.parser.flow;

import com.plsql.parser.multidb.Db2SourceFetchStrategy;
import com.plsql.parser.multidb.MySqlSourceFetchStrategy;
import com.plsql.parser.multidb.PostgresSourceFetchStrategy;
import com.plsql.parser.multidb.SqlServerSourceFetchStrategy;
import com.plsql.parser.multidb.SourceFetchStrategy;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Downloads PL/SQL source code from the database using ALL_SOURCE.
 * Checks local file cache first, then falls back to DB.
 */
public class SourceDownloader {

    private static final Logger log = LoggerFactory.getLogger(SourceDownloader.class);

    private final DbConnectionManager connManager;

    // Cache: "TYPE:NAME" -> source text
    private final Map<String, String> sourceCache = new ConcurrentHashMap<>();

    // Cache: "NAME" -> owner schema
    private final Map<String, String> ownerCache = new ConcurrentHashMap<>();

    private final MySqlSourceFetchStrategy mySqlStrategy = new MySqlSourceFetchStrategy();
    private final SqlServerSourceFetchStrategy sqlServerStrategy = new SqlServerSourceFetchStrategy();
    private final Db2SourceFetchStrategy db2Strategy = new Db2SourceFetchStrategy();
    private final PostgresSourceFetchStrategy postgresStrategy = new PostgresSourceFetchStrategy();

    // Source cache prefix (sources/ folder from build output) and the store it lives in.
    private ArtifactStore store;
    private ArtifactKey localCacheDir;

    // Index of cached source artifacts: "FILENAME.toUpperCase()" -> key (built lazily).
    // Written from the ≤6 "src-prefetch" pool threads inside checkLocalCache, so it is volatile:
    // a non-volatile lazy init could publish the map before its entries (or publish a torn/stale
    // reference and rebuild it forever). Each builder builds a private HashMap and assigns once —
    // the last assignment wins, and every reader sees a fully-constructed map.
    private volatile Map<String, ArtifactKey> localCacheIndex = null;

    // ── Persistent, DDL-validated source cache ────────────────────────────────────────────────
    // Lives OUTSIDE the per-analysis output folder (which AnalysisService archives/moves on every
    // re-analysis), so a re-run can reuse a source the database has not changed. Namespace:
    //   <outputKey>/.source-cache/<SANITIZED_SCHEMA>.<NAME>.<TYPE>.<hash8>.srccache
    // where <hash8> = first 8 hex of md5(connectionIdentity|schema|name|type) so two connections
    // never collide. Each file is JSON {"lastDdl":...,"source":...}. On lookup the object's current
    // LAST_DDL_TIME (primed from the catalog by the crawler) is compared with the stored one: equal
    // (or either side unknown) -> reuse; different -> treat as a miss and re-fetch. All store calls
    // run on the calling thread, so the crawler's per-task context install routes them to the
    // submitter's per-user store exactly like the src-prefetch pool already does.
    private ArtifactStore sourceCacheStore;
    private ArtifactKey sourceCacheDir;
    private String sourceCacheIdentity;
    private final ObjectMapper sourceCacheMapper = new ObjectMapper();
    // NAME (upper) -> {owner, objectType, lastDdl}; primed per BFS level by the crawler.
    private final Map<String, String[]> catalogMeta = new ConcurrentHashMap<>();

    /** On-disk entry shape for {@link #sourceCacheDir}. Public for Jackson. */
    public static final class CachedSource {
        public String lastDdl;
        public String source;
        public CachedSource() {}
        public CachedSource(String lastDdl, String source) {
            this.lastDdl = lastDdl;
            this.source = source;
        }
    }

    public SourceDownloader(DbConnectionManager connManager) {
        this.connManager = connManager;
    }

    public DbConnectionManager getConnManager() {
        return connManager;
    }

    /**
     * Point the source cache at a store prefix (the chunked writer's {@code sources/}).
     * Passing a null store or key disables the cache.
     */
    public void setLocalCacheDir(ArtifactStore store, ArtifactKey dir) {
        this.store = store;
        this.localCacheDir = dir;
        this.localCacheIndex = null; // reset index when dir changes
    }

    /**
     * Point the persistent source cache at {@code dir} (the analysis-adjacent
     * {@code <outputKey>/.source-cache/} namespace) and bind it to {@code identity} — the
     * connection's {@link DbConnectionManager#getConnectionIdentity()}. Passing a null store, key or
     * identity disables the cache. The store is expected to be the routed per-user store; callers on
     * pool threads must have installed the captured routing scope (the crawler's fan-out does).
     */
    public void setSourceCache(ArtifactStore store, ArtifactKey dir, String identity) {
        this.sourceCacheStore = store;
        this.sourceCacheDir = dir;
        this.sourceCacheIdentity = identity;
    }

    /** True when a persistent source cache is configured and usable. */
    public boolean hasSourceCache() {
        return sourceCacheStore != null && sourceCacheDir != null
                && sourceCacheIdentity != null && !sourceCacheIdentity.isEmpty();
    }

    /**
     * Whether cross-run cache READS are allowed. The fresh-default model turns them OFF for an
     * analysis unless the user explicitly chose "Fast re-run": the persistent source cache, the
     * trigger cache and the catalog snapshot are then never consulted, so every object is fetched
     * and parsed fresh. Writes still run in both modes, so a later fast run has something to read.
     *
     * <p>Default is {@code true} to preserve the downloader's own contract for direct callers
     * (tests, the trigger discoverer). The {@link DependencyCrawler} sets it explicitly from its
     * {@code fastReRun} flag, which defaults to false — i.e. an analysis is fresh unless told
     * otherwise.
     */
    private volatile boolean cacheReadEnabled = true;

    /** Enable/disable cross-run cache reads. See {@link #cacheReadEnabled}. */
    public void setCacheReadEnabled(boolean enabled) {
        this.cacheReadEnabled = enabled;
    }

    /** True when cross-run cache reads are allowed. */
    public boolean isCacheReadEnabled() {
        return cacheReadEnabled;
    }

    // ── Postgres / DB2 routine catalog + per-overload fetch (W1) ──────────────────────────────
    // A single catalog query per crawl lists every routine WITH its per-overload identity
    // (Postgres oid + pg_get_function_identity_arguments; DB2 SPECIFICNAME). The crawler uses it to
    // key each overload by its true signature and fetch that exact overload's source, instead of a
    // name-based LIMIT 1 that silently returns an arbitrary overload.
    private volatile List<SourceFetchStrategy.ObjectRef> postgresRoutineCatalog;
    private volatile List<SourceFetchStrategy.ObjectRef> db2RoutineCatalog;

    /** Cached Postgres routine catalog (empty when the connection is not Postgres or on error). */
    public List<SourceFetchStrategy.ObjectRef> listPostgresRoutines() {
        List<SourceFetchStrategy.ObjectRef> cached = postgresRoutineCatalog;
        if (cached != null) return cached;
        List<SourceFetchStrategy.ObjectRef> out = Collections.emptyList();
        if (connManager != null && connManager.isPostgres()) {
            Connection conn = null;
            try {
                conn = connManager.getAnyConnection();
                out = postgresStrategy.listObjects(conn, null);
                connManager.returnToPool(conn);
                conn = null;
            } catch (Exception e) {
                log.warn("[SourceDownloader] Postgres routine catalog list failed: {}", e.getMessage());
            } finally {
                if (conn != null) connManager.closeAndDiscard(conn);
            }
        }
        postgresRoutineCatalog = out;
        return out;
    }

    /** Cached DB2 routine catalog (empty when the connection is not DB2 or on error). */
    public List<SourceFetchStrategy.ObjectRef> listDb2Routines() {
        List<SourceFetchStrategy.ObjectRef> cached = db2RoutineCatalog;
        if (cached != null) return cached;
        List<SourceFetchStrategy.ObjectRef> out = Collections.emptyList();
        if (connManager != null && connManager.isDB2()) {
            Connection conn = null;
            try {
                conn = connManager.getAnyConnection();
                out = db2Strategy.listObjects(conn, null);
                connManager.returnToPool(conn);
                conn = null;
            } catch (Exception e) {
                log.warn("[SourceDownloader] DB2 routine catalog list failed: {}", e.getMessage());
            } finally {
                if (conn != null) connManager.closeAndDiscard(conn);
            }
        }
        db2RoutineCatalog = out;
        return out;
    }

    /**
     * Task #28: the owning module of a DB2 routine, from the cached catalog's
     * {@code ROUTINEMODULENAME} column, or null when the routine is not a module member (or the
     * connection is not DB2). Uses the same one-query, cached catalog as {@link #listDb2Routines},
     * so keying a module member costs no extra round trip.
     */
    public String db2ModuleOf(String routineName, String schema) {
        if (routineName == null || routineName.isBlank()) return null;
        if (connManager == null || !connManager.isDB2()) return null;
        for (SourceFetchStrategy.ObjectRef r : listDb2Routines()) {
            if (r.name != null && r.name.equalsIgnoreCase(routineName)
                    && (schema == null || schema.isEmpty()
                        || (r.schema != null && r.schema.equalsIgnoreCase(schema)))
                    && r.moduleName != null && !r.moduleName.isBlank()) {
                return r.moduleName;
            }
        }
        return null;
    }

    /** Fetch ONE Postgres overload's DDL by {@code pg_proc.oid} (in-memory cached per oid). */
    public String downloadPostgresSourceByOid(String oid) {
        if (oid == null || oid.isBlank()) return null;
        String key = "OID:" + oid;
        String cached = sourceCache.get(key);
        if (cached != null) return cached;
        if (connManager == null || !connManager.isPostgres()) return null;
        Connection conn = null;
        try {
            conn = connManager.getAnyConnection();
            String src = postgresStrategy.fetchSourceByOid(conn, oid);
            if (src != null) {
                sourceCache.put(key, src);
                connManager.incrementDbCalls();
            }
            connManager.returnToPool(conn);
            conn = null;
            return src;
        } catch (SQLException e) {
            log.debug("[SourceDownloader] Postgres fetch by oid {} failed: {}", oid, e.getMessage());
            return null;
        } finally {
            if (conn != null) connManager.closeAndDiscard(conn);
        }
    }

    /** Fetch ONE DB2 overload's DDL by its unique {@code SPECIFICNAME} (in-memory cached). */
    public String downloadDb2SourceBySpecificName(String specificName, String schema) {
        if (specificName == null || specificName.isBlank()) return null;
        String key = "SPECIFIC:" + specificName;
        String cached = sourceCache.get(key);
        if (cached != null) return cached;
        if (connManager == null || !connManager.isDB2()) return null;
        Connection conn = null;
        try {
            conn = connManager.getAnyConnection();
            String src = db2Strategy.fetchSourceBySpecificName(conn, specificName, schema);
            if (src != null) {
                sourceCache.put(key, src);
                connManager.incrementDbCalls();
            }
            connManager.returnToPool(conn);
            conn = null;
            return src;
        } catch (SQLException e) {
            log.debug("[SourceDownloader] DB2 fetch by specific name {} failed: {}", specificName, e.getMessage());
            return null;
        } finally {
            if (conn != null) connManager.closeAndDiscard(conn);
        }
    }

    /**
     * The object's current LAST_DDL from the catalog metadata the crawler primed for this level,
     * or null when unknown (non-Oracle dialects, or a level that was never primed). Used by the
     * analysis idempotency registry to decide whether an object changed since its last analysis.
     */
    public String getCatalogLastDdl(String objectName) {
        if (objectName == null) return null;
        return cacheLastDdl(objectName.toUpperCase());
    }

    // ── Trigger source cache ─────────────────────────────────────────────────────────────────
    // Trigger bodies were the ONE crawl path that bypassed the persistent source cache: the
    // TriggerDiscoverer fetched each trigger with its own ALL_SOURCE / DBMS_METADATA query (N+1).
    // These reuse the SAME cache dir and on-disk shape as the object source cache, keyed by
    // (identity, owner, trigger name, type=TRIGGER). A re-crawl of an unchanged trigger reads the
    // cached body instead of hitting the database again; DDL is not tracked for triggers, so an
    // entry is reused until the cache dir is cleared (the discoverer's batched pull refills it).

    /** Cached trigger body for {@code owner.triggerName}, or null on miss. */
    public String getCachedTriggerSource(String owner, String triggerName) {
        if (!cacheReadEnabled) return null; // fresh default: never read the trigger cache
        if (!hasSourceCache() || owner == null || triggerName == null) return null;
        ArtifactKey file = triggerCacheKey(owner, triggerName);
        try (InputStream in = sourceCacheStore.read(file)) {
            CachedSource entry = sourceCacheMapper.readValue(in, CachedSource.class);
            if (entry == null || entry.source == null) return null;
            log.info("[SourceDownloader] SourceCache HIT: TRIGGER {}.{}", owner, triggerName);
            return entry.source;
        } catch (IOException e) {
            return null;   // absent (the common miss) or unreadable — fall through to the DB
        }
    }

    /** Persist a freshly fetched trigger body. Best-effort. */
    public void storeTriggerSource(String owner, String triggerName, String source) {
        if (!hasSourceCache() || owner == null || triggerName == null || source == null) return;
        ArtifactKey file = triggerCacheKey(owner, triggerName);
        try (OutputStream out = sourceCacheStore.write(file)) {
            sourceCacheMapper.writeValue(out, new CachedSource(null, source));
        } catch (IOException e) {
            log.debug("[SourceDownloader] Trigger source-cache store failed for {}.{}: {}",
                    owner, triggerName, e.getMessage());
        }
    }

    private ArtifactKey triggerCacheKey(String owner, String triggerName) {
        String upOwner = owner.toUpperCase();
        String upName = triggerName.toUpperCase();
        String leaf = sanitize(upOwner) + "." + sanitize(upName) + ".TRIGGER."
                + shortHash(sourceCacheIdentity + "|" + upOwner + "|" + upName + "|TRIGGER")
                + ".srccache";
        return sourceCacheDir.resolve(leaf);
    }

    /** Prime catalog metadata for one object (owner / type / LAST_DDL) so the cache can validate. */
    public void primeCatalogMeta(String name, String owner, String type, String lastDdl) {
        if (name == null) return;
        catalogMeta.put(name.toUpperCase(), new String[]{owner, type, lastDdl});
    }

    /** Prime catalog metadata in bulk (NAME -> {owner, type, lastDdl}). */
    public void primeCatalogMeta(Map<String, String[]> meta) {
        if (meta == null) return;
        for (Map.Entry<String, String[]> e : meta.entrySet()) {
            if (e.getKey() != null && e.getValue() != null) {
                catalogMeta.put(e.getKey().toUpperCase(), e.getValue());
            }
        }
    }

    /**
     * Look up a cached source for {@code (identity, schema, name, type)} whose stored LAST_DDL
     * matches the object's current one (a missing DDL on either side counts as "cannot prove it
     * changed" and still reuses the entry). Returns null on any miss/staleness/IO error.
     */
    private String trySourceCache(String objectName, String objectType) {
        if (!cacheReadEnabled) return null; // fresh default: never read the source cache
        if (!hasSourceCache() || objectName == null || objectType == null) return null;
        String upper = objectName.toUpperCase();
        String owner = cacheOwner(upper);
        if (owner == null) return null; // no schema yet -> cannot key the cache consistently
        ArtifactKey file = sourceCacheKey(upper, objectType.toUpperCase(), owner);
        CachedSource entry;
        try (InputStream in = sourceCacheStore.read(file)) {
            entry = sourceCacheMapper.readValue(in, CachedSource.class);
        } catch (IOException e) {
            return null; // absent (the common miss) or unreadable — fall through to the DB
        }
        if (entry == null || entry.source == null) return null;
        String currentDdl = cacheLastDdl(upper);
        if (currentDdl != null && entry.lastDdl != null && !currentDdl.equals(entry.lastDdl)) {
            log.info("[SourceDownloader] SourceCache STALE for {} {} (ddl {} -> {})",
                    objectType, upper, entry.lastDdl, currentDdl);
            return null;
        }
        log.info("[SourceDownloader] SourceCache HIT: {} {} ({})", objectType, upper, owner);
        return entry.source;
    }

    /** Persist a freshly fetched source (with its catalog LAST_DDL, when known). Best-effort. */
    private void storeSourceCache(String objectName, String objectType, String source) {
        if (!hasSourceCache() || objectName == null || objectType == null || source == null) return;
        String upper = objectName.toUpperCase();
        String owner = cacheOwner(upper);
        if (owner == null) return;
        ArtifactKey file = sourceCacheKey(upper, objectType.toUpperCase(), owner);
        try (OutputStream out = sourceCacheStore.write(file)) {
            sourceCacheMapper.writeValue(out, new CachedSource(cacheLastDdl(upper), source));
        } catch (IOException e) {
            log.debug("[SourceDownloader] SourceCache store failed for {} {}: {}", objectType, upper, e.getMessage());
        }
    }

    /**
     * Persist a freshly fetched source as the per-object {@code sources/<SCHEMA>.<NAME>.<ext>}
     * artifact — the SAME artifact the per-object crawl path writes (see
     * {@code DependencyCrawler#writeNodeSource}) and the UI's {@code /source/<name>.<ext>} endpoint
     * serves, and the same naming {@link #checkLocalCache} reads back on a fast re-run.
     *
     * <p>This is the write the A2 bulk pull was missing: it returned the fetched texts to the
     * parser but never materialised them under {@code sources/}, so bulk-pulled procedures had no
     * {@code .prc} artifact and the UI's source panel 404'd. Every source-acquisition path (bulk,
     * per-object, DBMS_METADATA fallback) funnels through here so the artifact set is complete no
     * matter which path resolved the text.
     *
     * <p>Best-effort and idempotent: a missing store/dir/owner, an already-present artifact or any
     * IO error is a no-op — a failed persistence must never fail a crawl. The extension follows
     * {@link #typeToExtension} ({@code .prc}/{@code .fnc}/{@code .pkb}/{@code .pks}/{@code .sql}),
     * exactly mirroring the per-object writer's extension choice.
     */
    void persistSourceArtifact(String objectName, String objectType, String source) {
        if (store == null || localCacheDir == null || objectName == null || objectType == null
                || source == null || source.isBlank()) {
            return;
        }
        String upper = objectName.toUpperCase();
        String owner = cacheOwner(upper);
        // Mirror the per-object writer: an unknown owner is materialised as "UNKNOWN" rather than
        // dropping the artifact, so every fetched object still gets a sources/ file.
        String schema = owner != null ? owner : "UNKNOWN";
        String ext = typeToExtension(objectType.toUpperCase());
        // Defensive: the catalog should never yield a path separator, but a stray one must not let
        // a write escape the sources/ prefix.
        if (schema.indexOf('/') >= 0 || schema.indexOf('\\') >= 0
                || upper.indexOf('/') >= 0 || upper.indexOf('\\') >= 0) {
            return;
        }
        ArtifactKey file = localCacheDir.resolve(schema + "." + upper + "." + ext);
        try {
            if (store.exists(file)) return;
            byte[] bytes = source.getBytes(StandardCharsets.UTF_8);
            try (OutputStream out = store.write(file, bytes.length)) {
                out.write(bytes);
            }
            // A lazily-built local-cache index would not see the new leaf; drop it so the next
            // lookup re-lists and finds this artifact.
            localCacheIndex = null;
            log.debug("[SourceDownloader] Wrote sources/{} artifact for {} {}",
                    file.leaf(), objectType, upper);
        } catch (IOException e) {
            log.debug("[SourceDownloader] Source artifact store failed for {} {}: {}",
                    objectType, upper, e.getMessage());
        }
    }

    /** Owner to key the source cache by: the known owner, else the catalog's, else null (disabled). */
    private String cacheOwner(String upperName) {
        String owner = ownerCache.get(upperName);
        if (owner != null) return owner.toUpperCase();
        String[] meta = catalogMeta.get(upperName);
        return meta != null && meta[0] != null ? meta[0].toUpperCase() : null;
    }

    private String cacheLastDdl(String upperName) {
        String[] meta = catalogMeta.get(upperName);
        return meta != null ? meta[2] : null;
    }

    private ArtifactKey sourceCacheKey(String name, String type, String schema) {
        String leaf = sanitize(schema) + "." + sanitize(name) + "." + sanitize(type)
                + "." + shortHash(sourceCacheIdentity + "|" + schema + "|" + name + "|" + type)
                + ".srccache";
        return sourceCacheDir.resolve(leaf);
    }

    private static String sanitize(String s) {
        return s == null ? "_" : s.toUpperCase().replaceAll("[^A-Z0-9_$]", "_");
    }

    private static String shortHash(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(8);
            for (int i = 0; i < 4; i++) sb.append(String.format("%02x", digest[i]));
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            return Integer.toHexString(input.hashCode());
        }
    }

    // ══════════════════════════════════════════════════════════════════════════════════════════
    // A3 — catalog snapshot cache
    // ══════════════════════════════════════════════════════════════════════════════════════════
    //
    // The estate's object map (name -> {owner, objectType, LAST_DDL}) is captured ONCE per crawl
    // and reused for every per-level discovery lookup, so a re-crawl costs near-zero catalog
    // queries (the source cache already skips the fetches; this skips the DISCOVERY queries too).
    //
    // Correctness is guarded by a CHEAP FRESHNESS FINGERPRINT, not the TTL: at the start of every
    // crawl ONE aggregate query computes a fingerprint of the schema's catalog (row count, max
    // LAST_DDL and a hash of every (name,type,last_ddl) tuple). A different fingerprint — i.e. any
    // object added, dropped or modified — forces a FULL snapshot refresh, so a newly created
    // procedure appears on the very next crawl. The TTL is only a secondary optimization: it lets
    // a cached snapshot survive when the fingerprint probe itself cannot run (probe failure).
    // Explicit invalidation (connection change / operator refresh) is exposed via
    // {@link #invalidateCatalogSnapshot()}.

    /** Oracle catalog object types captured by the snapshot. */
    private static final String SNAPSHOT_TYPES =
            "'PACKAGE','PACKAGE BODY','PROCEDURE','FUNCTION','TABLE','VIEW','MATERIALIZED VIEW','TRIGGER'";

    /** Secondary-only TTL for the snapshot; the fingerprint probe is the real freshness guard. */
    private static final long CATALOG_SNAPSHOT_TTL_MS = 6L * 60 * 60 * 1000L; // 6 hours

    /**
     * Pluggable catalog access for the snapshot. The default implementation queries Oracle's
     * ALL_OBJECTS; tests inject a fake to exercise the fingerprint/refresh logic without a DB.
     */
    public interface CatalogSnapshotSource {
        /** One cheap aggregate query over the schema's catalog. Never returns null. */
        String fingerprint(List<String> schemas) throws SQLException;
        /** The full snapshot: rows of {owner, objectName, objectType, lastDdl}. */
        List<String[]> fetchRows(List<String> schemas) throws SQLException;
    }

    /** On-disk shape of a persisted snapshot. Public for Jackson. */
    public static final class CatalogSnapshotFile {
        public String fingerprint;
        public long loadedAtMs;
        public List<String[]> rows;
        public CatalogSnapshotFile() {}
    }

    private volatile CatalogSnapshotSource catalogSnapshotSource;
    private final Map<String, String[]> catalogSnapshot = new ConcurrentHashMap<>();
    private volatile String catalogSnapshotFingerprint;
    private volatile boolean catalogSnapshotLoaded = false;
    private volatile long catalogSnapshotLoadedAtMs = 0L;

    /** Inject a snapshot source (tests); null restores the default Oracle ALL_OBJECTS source. */
    public void setCatalogSnapshotSource(CatalogSnapshotSource source) {
        this.catalogSnapshotSource = source;
    }

    private CatalogSnapshotSource defaultSnapshotSource() {
        if (connManager == null || !connManager.isOracle()) return null;
        return new CatalogSnapshotSource() {
            @Override
            public String fingerprint(List<String> schemas) throws SQLException {
                String sql = "SELECT COUNT(*) c, "
                        + "TO_CHAR(MAX(last_ddl_time),'YYYY-MM-DD HH24:MI:SS') m, "
                        + "SUM(ORA_HASH(object_name||'|'||object_type||'|'||"
                        + "TO_CHAR(last_ddl_time,'YYYY-MM-DD HH24:MI:SS'))) h "
                        + "FROM all_objects WHERE owner IN (" + schemaList(schemas) + ") "
                        + "AND object_type IN (" + SNAPSHOT_TYPES + ")";
                Connection conn = null;
                try {
                    conn = connManager.getAnyConnection();
                    try (PreparedStatement ps = connManager.prepareStatement(conn, sql);
                         ResultSet rs = ps.executeQuery()) {
                        connManager.incrementDbCalls();
                        if (rs.next()) {
                            return rs.getString("c") + "|" + rs.getString("m") + "|" + rs.getString("h");
                        }
                    }
                    connManager.noteQuerySuccess();
                    connManager.returnToPool(conn);
                    conn = null;
                } catch (SQLException e) {
                    connManager.closeAndDiscard(conn);
                    conn = null;
                    connManager.noteQueryFailure();
                    throw e;
                } finally {
                    if (conn != null) connManager.closeAndDiscard(conn);
                }
                return "0|null|null";
            }

            @Override
            public List<String[]> fetchRows(List<String> schemas) throws SQLException {
                String sql = "SELECT owner, object_name, object_type, "
                        + "TO_CHAR(last_ddl_time,'YYYY-MM-DD HH24:MI:SS') ddl "
                        + "FROM all_objects WHERE owner IN (" + schemaList(schemas) + ") "
                        + "AND object_type IN (" + SNAPSHOT_TYPES + ")";
                List<String[]> rows = new ArrayList<>();
                Connection conn = null;
                try {
                    conn = connManager.getAnyConnection();
                    try (PreparedStatement ps = connManager.prepareStatement(conn, sql);
                         ResultSet rs = ps.executeQuery()) {
                        connManager.incrementDbCalls();
                        while (rs.next()) {
                            rows.add(new String[]{
                                    rs.getString("owner"),
                                    rs.getString("object_name"),
                                    rs.getString("object_type"),
                                    rs.getString("ddl")});
                        }
                    }
                    connManager.noteQuerySuccess();
                    connManager.returnToPool(conn);
                    conn = null;
                } catch (SQLException e) {
                    connManager.closeAndDiscard(conn);
                    conn = null;
                    connManager.noteQueryFailure();
                    throw e;
                } finally {
                    if (conn != null) connManager.closeAndDiscard(conn);
                }
                return rows;
            }
        };
    }

    private CatalogSnapshotSource snapshotSource() {
        CatalogSnapshotSource s = catalogSnapshotSource;
        return s != null ? s : defaultSnapshotSource();
    }

    private static String schemaList(List<String> schemas) {
        StringBuilder sb = new StringBuilder();
        if (schemas != null) {
            for (int i = 0; i < schemas.size(); i++) {
                if (i > 0) sb.append(",");
                sb.append("'").append(schemas.get(i).replace("'", "''")).append("'");
            }
        }
        return sb.toString();
    }

    /**
     * Ensure a fresh catalog snapshot for this crawl. Returns true when a snapshot is available.
     * The fingerprint probe runs on every call; a changed fingerprint refreshes the full snapshot.
     * On probe failure the TTL (and then the persisted snapshot) is the fallback. Best-effort:
     * any failure leaves the caller to fall back to per-object discovery.
     */
    public boolean ensureCatalogSnapshot() {
        CatalogSnapshotSource src = snapshotSource();
        if (src == null) return false;
        List<String> schemas = connManager != null ? connManager.getAvailableSchemas() : Collections.emptyList();

        String fp = null;
        try {
            fp = src.fingerprint(schemas);
        } catch (SQLException e) {
            log.debug("[SourceDownloader] Catalog fingerprint probe failed: {}", e.getMessage());
        }

        if (catalogSnapshotLoaded && fp != null && fp.equals(catalogSnapshotFingerprint)) {
            catalogSnapshotLoadedAtMs = System.currentTimeMillis();
            log.info("[SourceDownloader] Catalog snapshot FRESH (fingerprint={}, {} objects)",
                    fp, catalogSnapshot.size());
            return true;
        }
        if (fp != null) {
            try {
                List<String[]> rows = src.fetchRows(schemas);
                Map<String, String[]> fresh = new ConcurrentHashMap<>();
                for (String[] r : rows) {
                    if (r == null || r.length < 4 || r[1] == null) continue;
                    fresh.put(r[1].toUpperCase(), new String[]{r[0], r[2], r[3]});
                }
                catalogSnapshot.clear();
                catalogSnapshot.putAll(fresh);
                catalogSnapshotFingerprint = fp;
                catalogSnapshotLoaded = true;
                catalogSnapshotLoadedAtMs = System.currentTimeMillis();
                persistCatalogSnapshot();
                log.info("[SourceDownloader] Catalog snapshot REFRESHED (fingerprint={}, {} objects)",
                        fp, fresh.size());
                return true;
            } catch (SQLException e) {
                log.warn("[SourceDownloader] Catalog snapshot full refresh failed: {}", e.getMessage());
            }
        }

        // Fingerprint probe unavailable: TTL-only fallback (secondary optimization only).
        if (catalogSnapshotLoaded && !catalogSnapshotExpired()) return true;
        if (loadCatalogSnapshotFromDisk() && !catalogSnapshotExpired()) return true;
        return catalogSnapshotLoaded;
    }

    private boolean catalogSnapshotExpired() {
        return catalogSnapshotLoadedAtMs <= 0
                || (System.currentTimeMillis() - catalogSnapshotLoadedAtMs) > CATALOG_SNAPSHOT_TTL_MS;
    }

    /**
     * True when the snapshot may be READ for discovery. Like {@link #snapshotMeta}, this is
     * false under the fresh-default model ({@code cacheReadEnabled == false}) so a fresh analysis
     * always rediscovers from the catalog rather than trusting a cached snapshot; the snapshot is
     * still captured and persisted (see {@link #ensureCatalogSnapshot()}) so a later fast re-run
     * has something to read.
     */
    public boolean isCatalogSnapshotLoaded() {
        return catalogSnapshotLoaded && cacheReadEnabled;
    }

    /** The object's {owner, objectType, lastDdl} from the snapshot, or null when absent/not loaded. */
    public String[] snapshotMeta(String objectName) {
        if (!cacheReadEnabled) return null; // fresh default: snapshot is a fast-rerun-only aid
        if (objectName == null || !catalogSnapshotLoaded) return null;
        String[] v = catalogSnapshot.get(objectName.toUpperCase());
        return v == null ? null : new String[]{v[0], v[1], v[2]};
    }

    /** The fingerprint the in-memory snapshot was captured at, or null. */
    public String getCatalogSnapshotFingerprint() {
        return catalogSnapshotFingerprint;
    }

    /** Drop the in-memory AND persisted snapshot (connection change / explicit operator refresh). */
    public void invalidateCatalogSnapshot() {
        catalogSnapshot.clear();
        catalogSnapshotFingerprint = null;
        catalogSnapshotLoaded = false;
        catalogSnapshotLoadedAtMs = 0L;
        if (hasSourceCache()) {
            try {
                sourceCacheStore.delete(catalogSnapshotKey());
            } catch (Exception e) {
                log.debug("[SourceDownloader] Snapshot invalidation failed: {}", e.getMessage());
            }
        }
    }

    private ArtifactKey catalogSnapshotKey() {
        String scope = (sourceCacheIdentity == null ? "" : sourceCacheIdentity) + "|"
                + (connManager == null ? "" : String.join(",", connManager.getAvailableSchemas()));
        return sourceCacheDir.resolve(".catalog-snapshot." + shortHash(scope) + ".json");
    }

    private void persistCatalogSnapshot() {
        if (!hasSourceCache() || !catalogSnapshotLoaded) return;
        try (OutputStream out = sourceCacheStore.write(catalogSnapshotKey())) {
            CatalogSnapshotFile file = new CatalogSnapshotFile();
            file.fingerprint = catalogSnapshotFingerprint;
            file.loadedAtMs = catalogSnapshotLoadedAtMs;
            file.rows = new ArrayList<>();
            for (Map.Entry<String, String[]> e : catalogSnapshot.entrySet()) {
                String[] v = e.getValue();
                file.rows.add(new String[]{v[0], e.getKey(), v[1], v[2]});
            }
            sourceCacheMapper.writeValue(out, file);
        } catch (IOException e) {
            log.debug("[SourceDownloader] Snapshot persist failed: {}", e.getMessage());
        }
    }

    private boolean loadCatalogSnapshotFromDisk() {
        if (!hasSourceCache()) return false;
        ArtifactKey key = catalogSnapshotKey();
        try (InputStream in = sourceCacheStore.read(key)) {
            CatalogSnapshotFile file = sourceCacheMapper.readValue(in, CatalogSnapshotFile.class);
            if (file == null || file.rows == null) return false;
            Map<String, String[]> fresh = new ConcurrentHashMap<>();
            for (String[] r : file.rows) {
                if (r == null || r.length < 4 || r[1] == null) continue;
                fresh.put(r[1].toUpperCase(), new String[]{r[0], r[2], r[3]});
            }
            catalogSnapshot.clear();
            catalogSnapshot.putAll(fresh);
            catalogSnapshotFingerprint = file.fingerprint;
            catalogSnapshotLoaded = true;
            catalogSnapshotLoadedAtMs = file.loadedAtMs > 0 ? file.loadedAtMs : System.currentTimeMillis();
            log.info("[SourceDownloader] Catalog snapshot loaded from disk: {} objects", fresh.size());
            return true;
        } catch (IOException e) {
            return false; // absent (the common miss) or unreadable
        }
    }

    /**
     * Download source for a given object name and type.
     * Searches across all configured schemas.
     * Returns the source text with CREATE OR REPLACE prefix, or null if not found.
     */
    public String downloadSource(String objectName, String objectType) {
        String key = objectType.toUpperCase() + ":" + objectName.toUpperCase();
        String cached = sourceCache.get(key);
        if (cached != null) {
            return cached;
        }

        // Persistent DDL-validated source cache (survives re-analysis) — skip the DB fetch
        // entirely when the object is unchanged.
        String diskCached = trySourceCache(objectName, objectType);
        if (diskCached != null) {
            sourceCache.put(key, diskCached);
            return diskCached;
        }

        // Check local file cache first
        String localSource = checkLocalCache(objectName.toUpperCase(), objectType.toUpperCase());
        if (localSource != null) {
            sourceCache.put(key, localSource);
            log.info("[SourceDownloader] Loaded from local cache: "
                    + objectType + " " + objectName);
            return localSource;
        }

        if (connManager == null) {
            return null;
        }

        if (connManager.isCircuitOpen(objectName)) {
            log.debug("[SourceDownloader] Circuit open for {}, skipping DB attempt ({})", objectName, objectType);
            return null;
        }

        Connection conn = null;
        try {
            conn = connManager.getAnyConnection();
            long t0 = System.currentTimeMillis();
            String source = querySource(conn, objectName.toUpperCase(), objectType.toUpperCase());
            long elapsed = System.currentTimeMillis() - t0;
            if (elapsed > 500) {
                log.warn("[SourceDownloader] SLOW DB fetch for {} {} took {}ms", objectType, objectName, elapsed);
            } else {
                log.info("[SourceDownloader] DB fetch for {} {} took {}ms", objectType, objectName, elapsed);
            }
            if (source != null) {
                sourceCache.put(key, source);
                storeSourceCache(objectName, objectType, source);
                persistSourceArtifact(objectName, objectType, source);
                connManager.recordSuccess(objectName);
            }
            connManager.noteQuerySuccess();
            connManager.returnToPool(conn);
            conn = null;
            return source;
        } catch (SQLException e) {
            connManager.closeAndDiscard(conn);
            conn = null;
            log.debug("[SourceDownloader] Error downloading {} {}: {}", objectType, objectName, e.getMessage());
            connManager.recordFailure(objectName, e.getMessage());
            connManager.noteQueryFailure();
            return null;
        } finally {
            // Safety net: if an unexpected exception bypassed the explicit return/catch above
            if (conn != null) connManager.closeAndDiscard(conn);
        }
    }

    private String checkLocalCache(String objectName, String objectType) {
        if (store == null || localCacheDir == null) return null;
        // Build filename index on first use: ONE listing, then O(1) per lookup. Never a stat
        // per candidate — that is 25ms each against an object store. Read once into a local so a
        // concurrent rebuild (or setLocalCacheDir reset) cannot swap it out under the iteration.
        Map<String, ArtifactKey> idx = localCacheIndex;
        if (idx == null) {
            final Map<String, ArtifactKey> built = new HashMap<>();
            try (var stream = store.list(localCacheDir, false)) {
                stream.filter(java.util.function.Predicate.not(ArtifactEntry::directory))
                      .map(ArtifactEntry::key)
                      .forEach(k -> built.put(k.leaf().toUpperCase(), k));
            } catch (IOException e) {
                return null;
            }
            localCacheIndex = built;
            idx = built;
            log.info("[SourceDownloader] Built local cache index: {} files in {}", built.size(), localCacheDir);
        }
        String ext = typeToExtension(objectType);
        String exactSuffix = objectName + "." + ext.toUpperCase();
        // Try SCHEMA.NAME.ext first, then bare NAME.ext
        ArtifactKey match = null;
        for (Map.Entry<String, ArtifactKey> e : idx.entrySet()) {
            String fn = e.getKey();
            if (fn.equals(exactSuffix) || fn.endsWith("." + exactSuffix)) {
                match = e.getValue();
                // Extract owner from SCHEMA.NAME.ext filename
                if (fn.endsWith("." + exactSuffix) && fn.length() > exactSuffix.length() + 1) {
                    String schemaPrefix = fn.substring(0, fn.length() - exactSuffix.length() - 1);
                    if (!schemaPrefix.isEmpty()) ownerCache.put(objectName, schemaPrefix);
                }
                break;
            }
        }
        if (match == null) return null;
        try (InputStream in = store.read(match)) {
            String content = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            return !content.isBlank() ? content : null;
        } catch (IOException e) {
            return null;
        }
    }

    private String typeToExtension(String objectType) {
        switch (objectType) {
            case "PACKAGE BODY": return "pkb";
            case "PACKAGE": return "pks";
            case "PROCEDURE": return "prc";
            case "FUNCTION": return "fnc";
            default: return "sql";
        }
    }

    /**
     * Download PACKAGE BODY source.
     */
    public String downloadPackageBody(String packageName) {
        return downloadSource(packageName, "PACKAGE BODY");
    }

    /**
     * Download PACKAGE (spec) source.
     */
    public String downloadPackageSpec(String packageName) {
        return downloadSource(packageName, "PACKAGE");
    }

    /**
     * Download a standalone PROCEDURE.
     */
    public String downloadProcedure(String procName) {
        return downloadSource(procName, "PROCEDURE");
    }

    /**
     * Download a standalone FUNCTION.
     */
    public String downloadFunction(String funcName) {
        return downloadSource(funcName, "FUNCTION");
    }

    /**
     * Try to download source, trying multiple object types.
     * Tries PACKAGE BODY first, then PROCEDURE, then FUNCTION.
     * Emits a single consolidated WARN if all attempts fail (not one per type).
     */
    public String downloadAny(String objectName) {
        String[] types = {"PACKAGE BODY", "PROCEDURE", "FUNCTION", "PACKAGE"};
        // Oracle fast-path: a single ALL_SOURCE query resolves the object's type,
        // instead of probing up to 4 types (each a separate slow catalog query).
        // ~4x fewer round-trips on Oracle. Other dialects keep the probe loop.
        if (isOracleConn()) {
            return downloadAnyOracle(objectName.toUpperCase(), types);
        }
        String lastError = null;
        for (String type : types) {
            String key = type + ":" + objectName.toUpperCase();
            String cached = sourceCache.get(key);
            if (cached != null) return cached;
            String diskCached = trySourceCache(objectName, type);
            if (diskCached != null) {
                sourceCache.put(key, diskCached);
                return diskCached;
            }
            String localSource = checkLocalCache(objectName.toUpperCase(), type);
            if (localSource != null) {
                sourceCache.put(key, localSource);
                return localSource;
            }
            if (connManager == null) continue;
            if (connManager.isCircuitOpen(objectName)) {
                log.debug("[SourceDownloader] Circuit open for {}, skipping DB attempt", objectName);
                continue;
            }
            Connection conn = null;
            try {
                conn = connManager.getAnyConnection();
                long t0 = System.currentTimeMillis();
                String source = querySource(conn, objectName.toUpperCase(), type);
                long elapsed = System.currentTimeMillis() - t0;
                if (elapsed > 500) log.warn("[SourceDownloader] SLOW DB fetch for {} {} took {}ms", type, objectName, elapsed);
                else log.info("[SourceDownloader] DB fetch for {} {} took {}ms", type, objectName, elapsed);
                if (source != null) {
                    sourceCache.put(key, source);
                    storeSourceCache(objectName, type, source);
                    persistSourceArtifact(objectName, type, source);
                    connManager.recordSuccess(objectName);
                    connManager.returnToPool(conn);
                    conn = null;
                    return source;
                }
                connManager.returnToPool(conn);
                conn = null;
            } catch (SQLException e) {
                connManager.closeAndDiscard(conn);
                conn = null;
                lastError = e.getMessage();
                log.debug("[SourceDownloader] {} {} failed: {}", type, objectName, lastError);
                connManager.recordFailure(objectName, lastError);
            } finally {
                if (conn != null) connManager.closeAndDiscard(conn);
            }
        }
        if (lastError != null) {
            log.warn("[SourceDownloader] Error downloading {} (tried {} types): {}", objectName, types.length, lastError);
        }
        return null;
    }

    /** Oracle is the default dialect (no explicit MySQL/SQLServer/DB2 flag). */
    private boolean isOracleConn() {
        return connManager != null && connManager.isOracle();
    }

    /**
     * Oracle: resolve type + fetch source in ONE ALL_SOURCE query (no 4-type probe).
     */
    private String downloadAnyOracle(String objectName, String[] types) {
        // 1. caches / local cache (any known type) — same short-circuit as the probe loop
        for (String type : types) {
            String key = type + ":" + objectName;
            String cached = sourceCache.get(key);
            if (cached != null) return cached;
            String diskCached = trySourceCache(objectName, type);
            if (diskCached != null) { sourceCache.put(key, diskCached); return diskCached; }
            String local = checkLocalCache(objectName, type);
            if (local != null) { sourceCache.put(key, local); return local; }
        }
        if (connManager.isCircuitOpen(objectName)) {
            log.debug("[SourceDownloader] Circuit open for {}, skipping DB attempt", objectName);
            return null;
        }
        Connection conn = null;
        try {
            conn = connManager.getAnyConnection();
            long t0 = System.currentTimeMillis();
            String[] typeAndSource = querySourceOracleAnyType(conn, objectName);
            long elapsed = System.currentTimeMillis() - t0;
            if (elapsed > 500) log.warn("[SourceDownloader] SLOW DB fetch (1-query) for {} took {}ms", objectName, elapsed);
            else log.info("[SourceDownloader] DB fetch (1-query) for {} took {}ms", objectName, elapsed);
            connManager.returnToPool(conn);
            conn = null;
            if (typeAndSource != null) {
                sourceCache.put(typeAndSource[0] + ":" + objectName, typeAndSource[1]);
                storeSourceCache(objectName, typeAndSource[0], typeAndSource[1]);
                persistSourceArtifact(objectName, typeAndSource[0], typeAndSource[1]);
                connManager.recordSuccess(objectName);
                connManager.noteQuerySuccess();
                return typeAndSource[1];
            }
        } catch (SQLException e) {
            connManager.closeAndDiscard(conn);
            conn = null;
            connManager.recordFailure(objectName, e.getMessage());
            connManager.noteQueryFailure();
            log.warn("[SourceDownloader] Oracle fetch failed for {}: {}", objectName, e.getMessage());
        } finally {
            if (conn != null) connManager.closeAndDiscard(conn);
        }
        return null;
    }

    /**
     * One ALL_SOURCE query returning the object's source regardless of type.
     * Rows are grouped by (owner,type); the best type wins by preference
     * PACKAGE BODY > PROCEDURE > FUNCTION > PACKAGE. Returns [type, source] or null.
     * Preserves the DBMS_METADATA truncation fallback + wrapped-source detection.
     */
    private String[] querySourceOracleAnyType(Connection conn, String objectName) throws SQLException {
        String sql = "SELECT OWNER, TYPE, LINE, TEXT FROM ALL_SOURCE "
                + "WHERE NAME = ? AND TYPE IN ('PACKAGE BODY','PROCEDURE','FUNCTION','PACKAGE') "
                + "AND OWNER IN (" + buildOwnerInClause() + ") "
                + "ORDER BY OWNER, TYPE, LINE";
        java.util.Map<String, Integer> rank = new java.util.HashMap<>();
        rank.put("PACKAGE BODY", 0); rank.put("PROCEDURE", 1); rank.put("FUNCTION", 2); rank.put("PACKAGE", 3);
        try (PreparedStatement ps = connManager.prepareStatement(conn, sql)) {
            ps.setString(1, objectName);
            ps.setFetchSize(10000);
            connManager.incrementDbCalls();
            try (ResultSet rs = ps.executeQuery()) {
                java.util.Map<String, StringBuilder> bodies = new java.util.LinkedHashMap<>();
                java.util.Map<String, String[]> meta = new java.util.LinkedHashMap<>();
                while (rs.next()) {
                    String owner = rs.getString("OWNER");
                    String type = rs.getString("TYPE");
                    String k = owner + "|" + type;
                    bodies.computeIfAbsent(k, x -> new StringBuilder()).append(rs.getString("TEXT"));
                    meta.putIfAbsent(k, new String[]{owner, type});
                }
                if (bodies.isEmpty()) return null;
                String bestKey = null; int best = Integer.MAX_VALUE;
                for (String k : bodies.keySet()) {
                    int r = rank.getOrDefault(meta.get(k)[1], 9);
                    if (r < best) { best = r; bestKey = k; }
                }
                String owner = meta.get(bestKey)[0];
                String type = meta.get(bestKey)[1];
                ownerCache.put(objectName, owner);
                String source = "CREATE OR REPLACE " + bodies.get(bestKey).toString();

                if (looksLikeTruncated(source)) {
                    String full = tryDbmsMetadata(conn, owner, objectName, type);
                    if (full != null && (!looksLikeTruncated(full) || full.length() > source.length())) {
                        source = full;
                    }
                }
                if (source.contains(" wrapped") || source.contains("0000000")) {
                    String head = source.length() > 200 ? source.substring(0, 200) : source;
                    if (head.toUpperCase().contains("WRAPPED") || head.matches("(?s).*\\b[0-9a-f]{8,}\\b.*")) {
                        wrappedObjects.add(objectName);
                    }
                }
                log.info("[SourceDownloader] Downloaded {} {} from schema {} ({} chars, 1-query)",
                        type, objectName, owner, source.length());
                return new String[]{type, source};
            }
        }
    }

    // ══════════════════════════════════════════════════════════════════════════════════════════
    // A2 — bulk source pull
    // ══════════════════════════════════════════════════════════════════════════════════════════
    //
    // The closure wave fetches MANY sources at once. Where the dialect allows, they are pulled in
    // BATCHES instead of one round trip per object:
    //   • Oracle: ALL_SOURCE is multi-row (one row per source LINE), so a chunk of object tuples
    //     is fetched in ONE query — `WHERE (OWNER, NAME, TYPE) IN ((?,?,?),…)` when the catalog
    //     snapshot supplies owner+type, else `WHERE NAME IN (…) AND TYPE IN (…)`. 500 tuples per
    //     query, so a 2,000-object wave costs 4 queries instead of 2,000.
    //   • MySQL / Postgres / Db2 / SQL Server: their sources are not exposed as a multi-row text
    //     catalog in a way that batches cheaply across routines (per-routine
    //     pg_get_functiondef / SYSCAT / SHOW CREATE), so this returns an empty map and the caller
    //     falls back to the per-object parallel fan-out. That fallback is still bounded and
    //     context-correct (the existing fan-out pool).
    //
    // Bulk queries go through the SAME connManager.prepareStatement / getAnyConnection path, so the
    // W3 QPS limiter, statement timeout, read-only guard and global breaker all still apply — and
    // the DB impact is strictly better (fewer, larger queries).

    /** One (owner, name, type) tuple for the Oracle bulk ALL_SOURCE pull. */
    public static final class BulkRef {
        public final String name, owner, type;
        public BulkRef(String name, String owner, String type) {
            this.name = name; this.owner = owner; this.type = type;
        }
    }

    private static final int BULK_TUPLE_CHUNK = 500;

    private static boolean isSourceType(String t) {
        if (t == null) return false;
        String u = t.toUpperCase();
        return u.equals("PACKAGE") || u.equals("PACKAGE BODY")
                || u.equals("PROCEDURE") || u.equals("FUNCTION");
    }

    /**
     * Bulk-fetch as many of {@code names} as the dialect allows in one or few queries. Returns a
     * name(upper) -> source map for every object the bulk pull resolved; an empty map means the
     * caller must fall back to the per-object parallel fetch (non-Oracle, or nothing to do).
     */
    public Map<String, String> fetchSourcesBulk(Collection<String> names) {
        Map<String, String> out = new LinkedHashMap<>();
        if (names == null || names.isEmpty() || connManager == null || !isOracleConn()) return out;

        List<String> pending = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (String n : names) {
            if (n == null || n.isBlank()) continue;
            String upper = n.toUpperCase();
            if (!seen.add(upper)) continue;
            if (getCachedSource(upper) != null) continue; // already in memory
            pending.add(upper);
        }
        if (pending.isEmpty()) return out;

        List<String> schemas = connManager.getAvailableSchemas();
        String ownerIn = schemaList(schemas);

        // Snapshot-keyed tuples where possible; name-only for the rest.
        List<BulkRef> typed = new ArrayList<>();
        List<String> untyped = new ArrayList<>();
        for (String n : pending) {
            String[] meta = snapshotMeta(n);
            if (meta != null && meta[0] != null && isSourceType(meta[1])) {
                typed.add(new BulkRef(n, meta[0], meta[1]));
            } else {
                untyped.add(n);
            }
        }

        int queries = 0;
        for (int i = 0; i < typed.size(); i += BULK_TUPLE_CHUNK) {
            queries += bulkQueryTyped(typed.subList(i, Math.min(i + BULK_TUPLE_CHUNK, typed.size())), out);
        }
        for (int i = 0; i < untyped.size(); i += BULK_TUPLE_CHUNK) {
            queries += bulkQueryByName(untyped.subList(i, Math.min(i + BULK_TUPLE_CHUNK, untyped.size())),
                    ownerIn, out);
        }
        if (queries > 0) {
            log.info("[SourceDownloader] Bulk source pull: {} names in {} quer{} -> {} sources",
                    pending.size(), queries, queries == 1 ? "y" : "ies", out.size());
        }
        return out;
    }

    private int bulkQueryTyped(List<BulkRef> refs, Map<String, String> out) {
        StringBuilder tuples = new StringBuilder();
        for (int i = 0; i < refs.size(); i++) {
            if (i > 0) tuples.append(",");
            tuples.append("(?,?,?)");
        }
        String sql = "SELECT OWNER, NAME, TYPE, LINE, TEXT FROM ALL_SOURCE "
                + "WHERE (OWNER, NAME, TYPE) IN (" + tuples + ") "
                + "ORDER BY NAME, OWNER, TYPE, LINE";
        Connection conn = null;
        try {
            conn = connManager.getAnyConnection();
            try (PreparedStatement ps = connManager.prepareStatement(conn, sql)) {
                int p = 1;
                for (BulkRef r : refs) {
                    ps.setString(p++, r.owner);
                    ps.setString(p++, r.name);
                    ps.setString(p++, r.type);
                }
                ps.setFetchSize(10000);
                connManager.incrementDbCalls();
                try (ResultSet rs = ps.executeQuery()) {
                    assembleBulkRows(rs, out);
                }
            }
            connManager.noteQuerySuccess();
            connManager.returnToPool(conn);
            conn = null;
            return 1;
        } catch (SQLException e) {
            connManager.closeAndDiscard(conn);
            conn = null;
            connManager.noteQueryFailure();
            log.warn("[SourceDownloader] Bulk typed pull failed ({} refs): {}", refs.size(), e.getMessage());
            return 0;
        } finally {
            if (conn != null) connManager.closeAndDiscard(conn);
        }
    }

    private int bulkQueryByName(List<String> names, String ownerIn, Map<String, String> out) {
        StringBuilder ph = new StringBuilder();
        for (int i = 0; i < names.size(); i++) {
            if (i > 0) ph.append(",");
            ph.append("?");
        }
        String ownerClause = ownerIn.isEmpty() ? "" : " AND OWNER IN (" + ownerIn + ")";
        String sql = "SELECT OWNER, NAME, TYPE, LINE, TEXT FROM ALL_SOURCE "
                + "WHERE NAME IN (" + ph + ") "
                + "AND TYPE IN ('PACKAGE BODY','PROCEDURE','FUNCTION','PACKAGE')"
                + ownerClause
                + " ORDER BY NAME, OWNER, TYPE, LINE";
        Connection conn = null;
        try {
            conn = connManager.getAnyConnection();
            try (PreparedStatement ps = connManager.prepareStatement(conn, sql)) {
                for (int i = 0; i < names.size(); i++) ps.setString(i + 1, names.get(i));
                ps.setFetchSize(10000);
                connManager.incrementDbCalls();
                try (ResultSet rs = ps.executeQuery()) {
                    assembleBulkRows(rs, out);
                }
            }
            connManager.noteQuerySuccess();
            connManager.returnToPool(conn);
            conn = null;
            return 1;
        } catch (SQLException e) {
            connManager.closeAndDiscard(conn);
            conn = null;
            connManager.noteQueryFailure();
            log.warn("[SourceDownloader] Bulk name pull failed ({} names): {}", names.size(), e.getMessage());
            return 0;
        } finally {
            if (conn != null) connManager.closeAndDiscard(conn);
        }
    }

    /** Group multi-row ALL_SOURCE rows into one source per name, best type wins; cache + store. */
    private void assembleBulkRows(ResultSet rs, Map<String, String> out) throws SQLException {
        Map<String, StringBuilder> bodies = new LinkedHashMap<>();
        Map<String, String[]> keyMeta = new LinkedHashMap<>(); // key -> {owner, name, type}
        while (rs.next()) {
            String owner = rs.getString("OWNER");
            String name = rs.getString("NAME");
            String type = rs.getString("TYPE");
            if (name == null || type == null) continue;
            String key = owner + "|" + name + "|" + type;
            bodies.computeIfAbsent(key, k -> new StringBuilder()).append(rs.getString("TEXT"));
            keyMeta.putIfAbsent(key, new String[]{owner, name, type});
        }
        if (keyMeta.isEmpty()) return;

        Map<String, Integer> rank = new HashMap<>();
        rank.put("PACKAGE BODY", 0); rank.put("PROCEDURE", 1); rank.put("FUNCTION", 2); rank.put("PACKAGE", 3);
        Map<String, String> bestKeyByName = new HashMap<>();
        for (Map.Entry<String, String[]> e : keyMeta.entrySet()) {
            String key = e.getKey();
            String name = e.getValue()[1].toUpperCase();
            int r = rank.getOrDefault(e.getValue()[2].toUpperCase(), 9);
            String cur = bestKeyByName.get(name);
            if (cur == null || r < rank.getOrDefault(keyMeta.get(cur)[2].toUpperCase(), 9)) {
                bestKeyByName.put(name, key);
            }
        }
        for (Map.Entry<String, String> e : bestKeyByName.entrySet()) {
            String name = e.getKey();
            String[] m = keyMeta.get(e.getValue());
            String owner = m[0];
            String type = m[2].toUpperCase();
            String source = "CREATE OR REPLACE " + bodies.get(e.getValue()).toString();
            if (looksLikeTruncated(source)) {
                // Let the per-object path apply its DBMS_METADATA fallback rather than cache a stub.
                log.info("[SourceDownloader] Bulk pull skipped truncated {} {}", type, name);
                continue;
            }
            if (source.contains(" wrapped") || source.contains("0000000")) {
                String head = source.length() > 200 ? source.substring(0, 200) : source;
                if (head.toUpperCase().contains("WRAPPED") || head.matches("(?s).*\\b[0-9a-f]{8,}\\b.*")) {
                    wrappedObjects.add(name);
                }
            }
            sourceCache.put(type + ":" + name, source);
            if (owner != null) ownerCache.put(name, owner.toUpperCase());
            storeSourceCache(name, type, source);
            // The A2 bulk pull must materialise the SAME per-object sources/ artifact the
            // per-object path does, or the bulk-pulled objects (e.g. every .prc) have no source
            // file and the UI's source panel 404s. Persist here, from the already-fetched text —
            // no re-fetch.
            persistSourceArtifact(name, type, source);
            out.put(name, source);
        }
    }

    /**
     * Get the owner schema for a previously downloaded source.
     */
    public String getOwner(String objectName) {
        return ownerCache.get(objectName.toUpperCase());
    }

    /**
     * Query source for the given object. Delegates to MySQL or Oracle strategy.
     */
    private String querySource(Connection conn, String objectName, String objectType)
            throws SQLException {
        if (connManager != null && connManager.isMySQL()) {
            return querySourceMySQL(conn, objectName, objectType);
        }
        if (connManager != null && connManager.isSqlServer()) {
            return querySourceSqlServer(conn, objectName, objectType);
        }
        if (connManager != null && connManager.isDB2()) {
            return querySourceDB2(conn, objectName, objectType);
        }
        if (connManager != null && connManager.isPostgres()) {
            return querySourcePostgres(conn, objectName, objectType);
        }
        return querySourceOracle(conn, objectName, objectType);
    }

    private String querySourcePostgres(Connection conn, String objectName, String objectType) {
        if (objectType.toUpperCase().contains("PACKAGE")) return null; // Postgres has no packages
        // A qualified entry (schema.name) arrives as one string — split so we look up the
        // bare routine name (optionally filtered by its schema) rather than "schema.name".
        String schemaHint = null;
        String bareName = objectName;
        int dot = objectName.lastIndexOf('.');
        if (dot > 0) {
            schemaHint = objectName.substring(0, dot);
            bareName = objectName.substring(dot + 1);
        }
        try {
            // Search the hinted schema first; if none, across ALL schemas (schema=null).
            String source = postgresStrategy.fetchSource(conn, bareName, objectType, schemaHint);
            if (source == null && schemaHint != null) {
                source = postgresStrategy.fetchSource(conn, bareName, objectType, null);
            }
            if (source != null) {
                String owner = schemaHint != null ? schemaHint : connManager.getPostgresSchema();
                ownerCache.put(objectName.toUpperCase(), owner != null ? owner.toUpperCase() : "PUBLIC");
                connManager.incrementDbCalls();
                log.info("[SourceDownloader] Postgres fetched {} {} ({} chars, owner={})",
                        objectType, objectName, source.length(), owner);
            }
            return source;
        } catch (SQLException e) {
            log.debug("[SourceDownloader] Postgres fetch failed for {} {}: {}", objectType, objectName, e.getMessage());
            return null;
        }
    }

    private String querySourceDB2(Connection conn, String objectName, String objectType) {
        if (objectType.toUpperCase().contains("PACKAGE")) return null; // DB2 routines are procs/functions, no packages
        // A qualified entry (schema.name) arrives as one string — split so we look up the
        // bare routine name (optionally filtered by its schema) rather than "schema.name".
        String schemaHint = null;
        String bareName = objectName;
        int dot = objectName.lastIndexOf('.');
        if (dot > 0) {
            schemaHint = objectName.substring(0, dot);
            bareName = objectName.substring(dot + 1);
        }
        try {
            // Search the hinted schema first; if none, across ALL schemas (schema=null).
            String source = db2Strategy.fetchSource(conn, bareName, objectType, schemaHint);
            if (source == null && schemaHint != null) {
                source = db2Strategy.fetchSource(conn, bareName, objectType, null);
            }
            if (source != null) {
                String owner = schemaHint != null ? schemaHint : connManager.getDb2Schema();
                ownerCache.put(objectName.toUpperCase(), owner != null ? owner.toUpperCase() : "DB2");
                connManager.incrementDbCalls();
                log.info("[SourceDownloader] DB2 fetched {} {} ({} chars, owner={})",
                        objectType, objectName, source.length(), owner);
            }
            return source;
        } catch (SQLException e) {
            log.debug("[SourceDownloader] DB2 fetch failed for {} {}: {}", objectType, objectName, e.getMessage());
            return null;
        }
    }

    private String querySourceSqlServer(Connection conn, String objectName, String objectType) {
        if (objectType.toUpperCase().contains("PACKAGE")) return null; // SQL Server has no packages
        // A qualified entry (schema.name) arrives as one string — split so we look up the
        // bare object name (optionally filtered by its schema) rather than "schema.name".
        String schemaHint = null;
        String bareName = objectName;
        int dot = objectName.lastIndexOf('.');
        if (dot > 0) {
            schemaHint = objectName.substring(0, dot);
            bareName = objectName.substring(dot + 1);
        }
        try {
            // Search the hinted schema first; if none, across ALL schemas (dbo, sales, hr, fin, …).
            String source = sqlServerStrategy.fetchSource(conn, bareName, objectType, schemaHint);
            if (source == null && schemaHint != null) {
                source = sqlServerStrategy.fetchSource(conn, bareName, objectType, null);
            }
            if (source != null) {
                String owner = sqlServerStrategy.findSchema(conn, bareName);
                ownerCache.put(objectName.toUpperCase(), owner != null ? owner.toUpperCase() : "DBO");
                connManager.incrementDbCalls();
                log.info("[SourceDownloader] SQL Server fetched {} {} ({} chars, owner={})",
                        objectType, objectName, source.length(), owner);
            }
            return source;
        } catch (SQLException e) {
            log.debug("[SourceDownloader] SQL Server fetch failed for {} {}: {}", objectType, objectName, e.getMessage());
            return null;
        }
    }

    private String querySourceMySQL(Connection conn, String objectName, String objectType) {
        if (objectType.toUpperCase().contains("PACKAGE")) return null; // MySQL has no packages
        String schema = connManager.getMySqlSchema();
        try {
            String source = mySqlStrategy.fetchSource(conn, objectName, objectType, schema);
            if (source != null) {
                String schemaKey = schema != null ? schema.toUpperCase() : "MYSQL";
                ownerCache.put(objectName.toUpperCase(), schemaKey);
                connManager.incrementDbCalls();
                log.info("[SourceDownloader] MySQL fetched {} {} ({} chars)", objectType, objectName, source.length());
            }
            return source;
        } catch (SQLException e) {
            log.debug("[SourceDownloader] MySQL fetch failed for {} {}: {}", objectType, objectName, e.getMessage());
            return null;
        }
    }

    /**
     * Query ALL_SOURCE for the given object. Searches across all schemas.
     */
    private String querySourceOracle(Connection conn, String objectName, String objectType)
            throws SQLException {
        String sql = "SELECT OWNER, LINE, TEXT FROM ALL_SOURCE "
                + "WHERE NAME = ? AND TYPE = ? "
                + "AND OWNER IN (" + buildOwnerInClause() + ") "
                + "ORDER BY OWNER, LINE";

        try (PreparedStatement ps = connManager.prepareStatement(conn, sql)) {
            ps.setString(1, objectName);
            ps.setString(2, objectType);
            ps.setFetchSize(10000);

            connManager.incrementDbCalls();
            try (ResultSet rs = ps.executeQuery()) {
                StringBuilder sb = new StringBuilder();
                String owner = null;
                boolean hasRows = false;

                while (rs.next()) {
                    String rowOwner = rs.getString("OWNER");
                    if (!hasRows) {
                        owner = rowOwner;
                        hasRows = true;
                        sb.append("CREATE OR REPLACE ");
                    } else if (!owner.equals(rowOwner)) {
                        break;
                    }
                    sb.append(rs.getString("TEXT"));
                }

                if (!hasRows) {
                    return null;
                }

                // Cache the owner
                ownerCache.put(objectName, owner);

                String source = sb.toString();

                // If ALL_SOURCE looks truncated, attempt DBMS_METADATA fallback.
                // Prefer the DBMS_METADATA result if it is itself complete (not truncated),
                // regardless of length — stripTrailingDdl can reduce char count by removing
                // appended DDL junk, causing a spurious length-based rejection.
                if (owner != null && looksLikeTruncated(source)) {
                    String fullSource = tryDbmsMetadata(conn, owner, objectName, objectType);
                    if (fullSource != null && (!looksLikeTruncated(fullSource) || fullSource.length() > source.length())) {
                        log.info("[SourceDownloader] ALL_SOURCE truncated for {} {}, used DBMS_METADATA ({} -> {} chars, complete={})",
                                objectType, objectName, source.length(), fullSource.length(), !looksLikeTruncated(fullSource));
                        source = fullSource;
                    } else if (fullSource != null) {
                        log.warn("[SourceDownloader] DBMS_METADATA also appears truncated for {} {} ({} chars), keeping ALL_SOURCE ({} chars)",
                                objectType, objectName, fullSource.length(), source.length());
                    }
                }

                // Detect wrapped/encrypted source
                if (source.contains(" wrapped") || source.contains("0000000")) {
                    String firstLines = source.length() > 200 ? source.substring(0, 200) : source;
                    if (firstLines.toUpperCase().contains("WRAPPED") ||
                            firstLines.matches("(?s).*\\b[0-9a-f]{8,}\\b.*")) {
                        wrappedObjects.add(objectName);
                        log.info("[SourceDownloader] WRAPPED/ENCRYPTED " + objectType + " "
                                + objectName + " from schema " + owner);
                        return source;
                    }
                }

                log.info("[SourceDownloader] Downloaded " + objectType + " "
                        + objectName + " from schema " + owner
                        + " (" + sb.length() + " chars)");
                return source;
            }
        }
    }

    /**
     * Returns true if the source looks truncated — last non-empty, non-comment line is not "/" or "END...;"
     */
    private boolean looksLikeTruncated(String source) {
        if (source == null || source.isBlank()) return false;
        String[] lines = source.split("\\r?\\n");
        for (int i = lines.length - 1; i >= 0; i--) {
            String line = lines[i].trim();
            if (line.isEmpty() || line.startsWith("--")) continue;
            return !line.equals("/")
                    && !line.equalsIgnoreCase("end;")
                    && !line.matches("(?i)end\\s+[\\w\".]+\\s*;");
        }
        return false;
    }

    /**
     * Attempt to retrieve full source via DBMS_METADATA.GET_DDL as a fallback.
     * Returns null if DBMS_METADATA is unavailable or fails.
     */
    private String tryDbmsMetadata(Connection conn, String owner, String objectName, String objectType) {
        // Map objectType to DBMS_METADATA object_type parameter
        String metaType;
        switch (objectType.toUpperCase()) {
            case "PACKAGE BODY": metaType = "PACKAGE_BODY"; break;
            case "PACKAGE":      metaType = "PACKAGE";      break;
            case "PROCEDURE":    metaType = "PROCEDURE";    break;
            case "FUNCTION":     metaType = "FUNCTION";     break;
            default: return null;
        }
        // Disable DBMS_METADATA transforms that can truncate or reformat large packages:
        // PRETTY=false disables line-length reformatting that can corrupt large CLOBs.
        // SQLTERMINATOR=false removes the trailing "/" so it doesn't interfere with parsing.
        String[] setupSqls = {
            "BEGIN DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'PRETTY',FALSE); END;",
            "BEGIN DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SQLTERMINATOR',FALSE); END;"
        };
        for (String setup : setupSqls) {
            try (java.sql.CallableStatement cs = conn.prepareCall(setup)) {
                cs.execute();
            } catch (Exception e) {
                log.debug("[SourceDownloader] DBMS_METADATA transform setup skipped: {}", e.getMessage());
            }
        }
        String sql = "SELECT DBMS_METADATA.GET_DDL(?, ?, ?) FROM DUAL";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, metaType);
            ps.setString(2, objectName.toUpperCase());
            ps.setString(3, owner.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    java.sql.Clob clob = rs.getClob(1);
                    if (clob != null) {
                        long clobLen = clob.length();
                        String ddl = clob.getSubString(1, (int) Math.min(clobLen, 10_000_000));
                        clob.free();
                        log.info("[SourceDownloader] DBMS_METADATA returned {} chars for {} {}", clobLen, objectType, objectName);
                        if (ddl == null || ddl.isBlank()) return null;
                        // Normalize DBMS_METADATA header so ANTLR can parse it:
                        // Replace: CREATE OR REPLACE EDITIONABLE PACKAGE BODY "SCHEMA"."NAME"
                        // With:    CREATE OR REPLACE PACKAGE BODY NAME
                        ddl = ddl.replaceAll(
                            "(?i)CREATE\\s+OR\\s+REPLACE\\s+EDITIONABLE\\s+(PACKAGE\\s+BODY|PACKAGE|PROCEDURE|FUNCTION)\\s+\"[^\"]+\"\\.\"([^\"]+)\"",
                            "CREATE OR REPLACE $1 $2");
                        // Also handle non-schema-qualified quoted: "NAME"
                        ddl = ddl.replaceAll(
                            "(?i)CREATE\\s+OR\\s+REPLACE\\s+EDITIONABLE\\s+(PACKAGE\\s+BODY|PACKAGE|PROCEDURE|FUNCTION)\\s+\"([^\"]+)\"",
                            "CREATE OR REPLACE $1 $2");
                        // Also strip EDITIONABLE when no quotes remain
                        ddl = ddl.replaceAll(
                            "(?i)(CREATE\\s+OR\\s+REPLACE\\s+)EDITIONABLE\\s+",
                            "$1");
                        // Strip any extra DDL appended after the first complete statement.
                        // DBMS_METADATA can return "END pkgname;\n\n\nCREATE OR REPLACE ..." when
                        // multiple objects share a name. Cut at the first trailing CREATE.
                        ddl = stripTrailingDdl(ddl, objectName);
                        return ddl;
                    }
                }
            }
        } catch (Exception e) {
            log.warn("[SourceDownloader] DBMS_METADATA fallback failed for {} {}: {}", objectType, objectName, e.getMessage());
        }
        return null;
    }

    /**
     * Strip any content that appears after the first complete DDL block in the string.
     * DBMS_METADATA occasionally appends a second CREATE statement after the closing END;
     * of the first object (e.g. when the session transform state leaks between calls).
     * We find the first "END <objectName>;" and cut everything beyond the trailing "/"
     * or start of the next CREATE keyword.
     */
    private static String stripTrailingDdl(String ddl, String objectName) {
        if (ddl == null) return null;
        // Find "END <objectName>;" (case-insensitive, optional whitespace)
        java.util.regex.Matcher m = java.util.regex.Pattern.compile(
                "(?i)\\bEND\\s+" + java.util.regex.Pattern.quote(objectName) + "\\s*;")
                .matcher(ddl);
        if (m.find()) {
            int cutStart = m.end();
            // Allow trailing whitespace and a single "/" terminator
            int cutEnd = cutStart;
            while (cutEnd < ddl.length() && Character.isWhitespace(ddl.charAt(cutEnd))) cutEnd++;
            if (cutEnd < ddl.length() && ddl.charAt(cutEnd) == '/') cutEnd++;
            // If there is a second CREATE after this point, truncate there
            int nextCreate = -1;
            java.util.regex.Matcher m2 = java.util.regex.Pattern.compile(
                    "(?i)\\bCREATE\\b").matcher(ddl);
            m2.region(cutEnd, ddl.length());
            if (m2.find()) nextCreate = m2.start();
            if (nextCreate > 0) {
                log.warn("[SourceDownloader] Stripped trailing DDL after END {};  ({} extra chars removed)",
                        objectName, ddl.length() - nextCreate);
                return ddl.substring(0, nextCreate).stripTrailing();
            }
        }
        return ddl;
    }

    /**
     * Build the IN clause for the configured schemas.
     */
    private String buildOwnerInClause() {
        List<String> schemas = connManager.getAvailableSchemas();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < schemas.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append("'").append(schemas.get(i).replace("'", "''")).append("'");
        }
        return sb.toString();
    }

    /**
     * Check if a source has already been downloaded and cached.
     */
    public boolean isCached(String objectName, String objectType) {
        String key = objectType.toUpperCase() + ":" + objectName.toUpperCase();
        return sourceCache.containsKey(key);
    }

    /**
     * Get cached source for a given object, trying multiple types.
     */
    public String getCachedSource(String objectName) {
        String upper = objectName.toUpperCase();
        for (String type : new String[]{"PACKAGE BODY", "PROCEDURE", "FUNCTION", "PACKAGE"}) {
            String key = type + ":" + upper;
            String src = sourceCache.get(key);
            if (src != null) return src;
        }
        return null;
    }

    /**
     * Get the cached type for a given object.
     */
    public String getCachedType(String objectName) {
        String upper = objectName.toUpperCase();
        for (String type : new String[]{"PACKAGE BODY", "PROCEDURE", "FUNCTION", "PACKAGE"}) {
            String key = type + ":" + upper;
            if (sourceCache.containsKey(key)) return type;
        }
        return null;
    }

    private final Set<String> wrappedObjects = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public boolean isWrapped(String objectName) {
        return wrappedObjects.contains(objectName.toUpperCase());
    }

    /**
     * Clear all caches.
     */
    public void clearCache() {
        sourceCache.clear();
        ownerCache.clear();
        wrappedObjects.clear();
    }
}
