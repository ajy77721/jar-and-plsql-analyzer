package com.plsql.parser.flow;

import com.plsql.parser.multidb.Db2SourceFetchStrategy;
import com.plsql.parser.multidb.MySqlSourceFetchStrategy;
import com.plsql.parser.multidb.PostgresSourceFetchStrategy;
import com.plsql.parser.multidb.SqlServerSourceFetchStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
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

    // Index of cached source artifacts: "FILENAME.toUpperCase()" -> key (built lazily)
    private Map<String, ArtifactKey> localCacheIndex = null;

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
                connManager.recordSuccess(objectName);
            }
            connManager.returnToPool(conn);
            conn = null;
            return source;
        } catch (SQLException e) {
            connManager.closeAndDiscard(conn);
            conn = null;
            log.debug("[SourceDownloader] Error downloading {} {}: {}", objectType, objectName, e.getMessage());
            connManager.recordFailure(objectName, e.getMessage());
            return null;
        } finally {
            // Safety net: if an unexpected exception bypassed the explicit return/catch above
            if (conn != null) connManager.closeAndDiscard(conn);
        }
    }

    private String checkLocalCache(String objectName, String objectType) {
        if (store == null || localCacheDir == null) return null;
        // Build filename index on first use: ONE listing, then O(1) per lookup. Never a stat
        // per candidate — that is 25ms each against an object store.
        if (localCacheIndex == null) {
            Map<String, ArtifactKey> idx = new HashMap<>();
            try (var stream = store.list(localCacheDir, false)) {
                stream.filter(java.util.function.Predicate.not(ArtifactEntry::directory))
                      .map(ArtifactEntry::key)
                      .forEach(k -> idx.put(k.leaf().toUpperCase(), k));
            } catch (IOException e) {
                return null;
            }
            localCacheIndex = idx;
            log.info("[SourceDownloader] Built local cache index: {} files in {}", idx.size(), localCacheDir);
        }
        String ext = typeToExtension(objectType);
        String exactSuffix = objectName + "." + ext.toUpperCase();
        // Try SCHEMA.NAME.ext first, then bare NAME.ext
        ArtifactKey match = null;
        for (Map.Entry<String, ArtifactKey> e : localCacheIndex.entrySet()) {
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
        return connManager != null
                && !connManager.isMySQL()
                && !connManager.isSqlServer()
                && !connManager.isDB2()
                && !connManager.isPostgres();
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
                connManager.recordSuccess(objectName);
                return typeAndSource[1];
            }
        } catch (SQLException e) {
            connManager.closeAndDiscard(conn);
            conn = null;
            connManager.recordFailure(objectName, e.getMessage());
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
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
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

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
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
