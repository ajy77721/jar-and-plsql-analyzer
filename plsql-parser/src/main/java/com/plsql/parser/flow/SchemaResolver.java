package com.plsql.parser.flow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves owner schema for database objects using filtered queries.
 *
 * Strategy:
 *   1. Resolve seed owners (1 batched ALL_OBJECTS query)
 *   2. CONNECT BY transitive deps per seed (1 query per seed — the ONLY expensive query)
 *      → already returns schema, type, depth for ALL deps including tables/views
 *   3. Reverse deps per seed (1 simple indexed query per seed — fast)
 *   4. Direct deps for BFS validation (batched IN clause query)
 *
 * Q2 (tables/views) and Q5 (summary) are derived IN MEMORY from Q1 results — no extra DB calls.
 * NEVER runs without entry points. Results cached to disk.
 */
public class SchemaResolver {

    private static final Logger log = LoggerFactory.getLogger(SchemaResolver.class);

    private final Map<String, String> objectToSchema = new ConcurrentHashMap<>();
    private final Map<String, String> objectToType = new ConcurrentHashMap<>();
    private final Map<String, String> typeQualifiedToSchema = new ConcurrentHashMap<>();
    private final Map<String, List<SchemaTypePair>> ambiguousObjects = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> dependencyCache = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> transitiveDepCache = new ConcurrentHashMap<>();
    private final List<Map<String, String>> reverseDepResults = new ArrayList<>();

    private final List<String> configuredSchemas;
    private final DbConnectionManager connManager;
    /** Store the TSV disk cache lives in; null disables caching. */
    private final ArtifactStore store;
    private ArtifactKey cacheDir;

    /** Default cache prefix under the store root — mirrors {data}/cache. */
    private static final String CACHE_ROOT = "cache";

    /**
     * Key of the schema-resolver disk cache for a scope. Shared with FlowAnalysisMain's
     * --clear-cache so the two can never disagree about where the cache lives.
     *
     * @param cacheRoot base key, or null for {@code cache/}
     * @param cacheScope datasource/project/env token (e.g. "DB2~Db2Demo~local"); '~' becomes a
     *                   key separator so the cache tree mirrors the results tree
     */
    public static ArtifactKey resolverCacheKey(ArtifactKey cacheRoot, String cacheScope) {
        ArtifactKey base = (cacheRoot != null ? cacheRoot : ArtifactKey.of(CACHE_ROOT))
                .resolve("schema-resolver");
        if (cacheScope != null && !cacheScope.isBlank()) {
            for (String seg : cacheScope.replaceAll("[^A-Za-z0-9_~-]", "_").split("~")) {
                if (!seg.isEmpty()) base = base.resolve(seg);
            }
        }
        return base;
    }

    private static final int MAX_LEVEL = 10;
    private static final int BATCH_SIZE = 500;

    // Tracks names already attempted via tryResolveFromDb to avoid repeated DB hits
    private final Set<String> dbResolveAttempted = Collections.synchronizedSet(new HashSet<>());

    // Table/view names whose object_type has already been verified against ALL_OBJECTS by
    // batchResolveTableOwners. Prevents re-querying on every BFS node while still allowing a
    // one-time type upgrade for names first resolved (via ALL_DEPENDENCIES) to their container
    // TABLE row — e.g. an Oracle materialized view is recorded as TABLE in ALL_DEPENDENCIES.
    private final Set<String> tableTypeResolved = Collections.synchronizedSet(new HashSet<>());

    private static class SchemaTypePair {
        final String schema;
        final String objectType;
        SchemaTypePair(String s, String t) { schema = s; objectType = t; }
    }

    private static class SeedInfo {
        final String objectName, owner, objectType;
        SeedInfo(String n, String o, String t) { objectName = n; owner = o; objectType = t; }
    }

    public SchemaResolver(DbConnectionManager connManager, List<String> entryPoints) throws SQLException {
        this(connManager, entryPoints, null, null, null);
    }

    public SchemaResolver(DbConnectionManager connManager, List<String> entryPoints, String cacheScope) throws SQLException {
        this(connManager, entryPoints, cacheScope, null, null);
    }

    /**
     * @param cacheScope optional per-connection scope (e.g. "__WL2ZTC" from the analysis
     *   folder suffix, encoding project|env|conn|dbType).  When set, the disk cache lives in
     *   {@code <cacheRoot>/schema-resolver/<scope>} so different Oracle connections never read
     *   each other's object→schema / dependency mappings (names collide across databases).
     * @param store the artifact store the cache is written to; null disables the disk cache
     * @param cacheRoot optional base key for the disk cache (e.g. {@code cache}). When null,
     *   defaults to the {@code cache} prefix under the store root.
     */
    public SchemaResolver(DbConnectionManager connManager, List<String> entryPoints,
                          String cacheScope, ArtifactStore store, ArtifactKey cacheRoot) throws SQLException {
        this.connManager = connManager;
        this.store = store;
        this.configuredSchemas = connManager.getAvailableSchemas();

        if (entryPoints == null || entryPoints.isEmpty()) {
            throw new IllegalArgumentException("Entry points REQUIRED — will not load entire DB");
        }

        long totalStart = System.currentTimeMillis();
        // Scope the disk cache per connection so different Oracle databases don't share
        // object→schema mappings (contamination when object names collide across DBs).
        this.cacheDir = resolverCacheKey(cacheRoot, cacheScope);

        // MySQL has no ALL_OBJECTS / ALL_DEPENDENCIES — skip Oracle schema resolution
        if (connManager.isMySQL()) {
            String schema = connManager.getMySqlSchema();
            if (schema != null) {
                for (String ep : entryPoints) {
                    String upper = ep.toUpperCase().trim();
                    String seed = upper.contains(".") ? upper.split("\\.", 2)[0] : upper;
                    objectToSchema.put(seed, schema.toUpperCase());
                }
            }
            log.info("[SchemaResolver] MySQL mode: skipped Oracle dependency resolution in {}ms",
                    System.currentTimeMillis() - totalStart);
            return;
        }

        // SQL Server uses sys.* catalog views, not ALL_OBJECTS / ALL_DEPENDENCIES —
        // skip Oracle schema resolution and seed each entry point with the SQL Server schema.
        if (connManager.isSqlServer()) {
            String schema = connManager.getSqlServerSchema();
            if (schema != null) {
                for (String ep : entryPoints) {
                    String upper = ep.toUpperCase().trim();
                    String seed = upper.contains(".") ? upper.split("\\.", 2)[0] : upper;
                    objectToSchema.put(seed, schema.toUpperCase());
                }
            }
            log.info("[SchemaResolver] SQL Server mode: skipped Oracle dependency resolution in {}ms",
                    System.currentTimeMillis() - totalStart);
            return;
        }

        // DB2 uses SYSCAT.* catalog views, not ALL_OBJECTS / ALL_DEPENDENCIES —
        // skip Oracle schema resolution and seed each entry point with the DB2 schema.
        if (connManager.isDB2()) {
            String schema = connManager.getDb2Schema();
            if (schema != null) {
                for (String ep : entryPoints) {
                    String upper = ep.toUpperCase().trim();
                    String seed = upper.contains(".") ? upper.split("\\.", 2)[0] : upper;
                    objectToSchema.put(seed, schema.toUpperCase());
                }
            }
            log.info("[SchemaResolver] DB2 mode: skipped Oracle dependency resolution in {}ms",
                    System.currentTimeMillis() - totalStart);
            return;
        }

        // PostgreSQL uses pg_catalog / information_schema, not ALL_OBJECTS / ALL_DEPENDENCIES —
        // skip Oracle schema resolution and seed each entry point with the Postgres schema.
        if (connManager.isPostgres()) {
            String schema = connManager.getPostgresSchema();
            if (schema != null) {
                for (String ep : entryPoints) {
                    String upper = ep.toUpperCase().trim();
                    String seed = upper.contains(".") ? upper.split("\\.", 2)[0] : upper;
                    objectToSchema.put(seed, schema.toUpperCase());
                }
            }
            log.info("[SchemaResolver] Postgres mode: skipped Oracle dependency resolution in {}ms",
                    System.currentTimeMillis() - totalStart);
            return;
        }

        if (loadFromDisk()) {
            log.info("[SchemaResolver] Loaded from disk cache: " + objectToSchema.size()
                    + " objects, " + dependencyCache.size() + " deps in "
                    + (System.currentTimeMillis() - totalStart) + "ms");
            return;
        }

        Connection conn = connManager.getAnyConnection();
        try {

        // Parse entry points → seed names (PKG.PROC → PKG)
        Set<String> seedNames = new LinkedHashSet<>();
        for (String ep : entryPoints) {
            String upper = ep.toUpperCase().trim();
            seedNames.add(upper.contains(".") ? upper.split("\\.", 2)[0] : upper);
        }

        // ── Step 1: Resolve seed owners (batched ALL_OBJECTS) ──
        long t = System.currentTimeMillis();
        List<SeedInfo> seeds = resolveSeedObjects(conn, seedNames);
        log.info("[SchemaResolver] Step 1 (seed owners): " + seeds.size()
                + " seeds in " + (System.currentTimeMillis() - t) + "ms");

        if (seeds.isEmpty()) {
            log.info("[SchemaResolver] WARNING: No seeds found — nothing to resolve");
            saveToDisk();
            return;
        }

        // ── Step 2: Q1 — CONNECT BY transitive deps (1 query per seed) ──
        // This is the ONE expensive query. All type/schema info comes from here.
        t = System.currentTimeMillis();
        Set<String> allRelevantNames = new LinkedHashSet<>(seedNames);
        Map<String, Integer> typeSummary = new LinkedHashMap<>();
        int tableViewCount = 0;

        for (SeedInfo seed : seeds) {
            String sql = "SELECT DISTINCT "
                    + "  referenced_owner  AS dep_schema, "
                    + "  referenced_name   AS dep_object, "
                    + "  referenced_type   AS dep_type, "
                    + "  LEVEL             AS depth "
                    + "FROM ALL_DEPENDENCIES "
                    + "START WITH owner = ? "
                    + "  AND name  = ? "
                    + "  AND type  IN ('PACKAGE','PACKAGE BODY','PROCEDURE','FUNCTION') "
                    + "CONNECT BY NOCYCLE "
                    + "  PRIOR referenced_owner = owner "
                    + "  AND PRIOR referenced_name  = name "
                    + "  AND LEVEL <= " + MAX_LEVEL + " "
                    + "ORDER BY dep_type, dep_schema, dep_object";

            long q1Start = System.currentTimeMillis();
            log.info("[SchemaResolver] Q1 running: {}.{}", seed.owner, seed.objectName);

            int rowCount = 0;
            try (PreparedStatement ps = connManager.prepareStatement(conn, sql)) {
                ps.setString(1, seed.owner);
                ps.setString(2, seed.objectName);
                connManager.incrementDbCalls();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String depSchema = rs.getString("dep_schema");
                        String depObject = rs.getString("dep_object");
                        String depType   = rs.getString("dep_type");

                        if (depObject == null || depObject.isEmpty()) continue;

                        String upperObj    = depObject.toUpperCase();
                        String upperSchema = depSchema != null ? depSchema.toUpperCase() : "UNKNOWN";
                        String normType    = depType != null ? depType.toUpperCase().replace(" ", "_") : "UNKNOWN";

                        // Skip SYS objects
                        if ("SYS".equals(upperSchema) || "PUBLIC".equals(upperSchema)
                                || "SYSTEM".equals(upperSchema)) continue;

                        allRelevantNames.add(upperObj);

                        if (!objectToSchema.containsKey(upperObj) || isPrimaryType(normType)) {
                            objectToSchema.put(upperObj, upperSchema);
                        }
                        putType(upperObj, normType);
                        typeQualifiedToSchema.putIfAbsent(normType + "." + upperObj, upperSchema);
                        ambiguousObjects.computeIfAbsent(upperObj, k -> new ArrayList<>())
                                .add(new SchemaTypePair(upperSchema, normType));

                        // In-memory Q2 (tables/views) and Q5 (summary)
                        if ("TABLE".equals(normType) || "VIEW".equals(normType)
                                || "MATERIALIZED_VIEW".equals(normType)) {
                            tableViewCount++;
                        }
                        typeSummary.merge(normType, 1, Integer::sum);

                        rowCount++;
                    }
                }
            }
            log.info("[SchemaResolver] Q1 done: {}.{} → {} deps in {}ms",
                    seed.owner, seed.objectName, rowCount, System.currentTimeMillis() - q1Start);
        }

        // Log Q2 equivalent (from Q1 data)
        log.info("[SchemaResolver] Q2 (tables/views from Q1): " + tableViewCount);

        // Log Q5 equivalent (summary from Q1 data)
        StringBuilder summaryLog = new StringBuilder("[SchemaResolver] Q5 summary: ");
        typeSummary.entrySet().stream()
                .sorted((a, b) -> b.getValue() - a.getValue())
                .forEach(e -> summaryLog.append(e.getKey()).append("=").append(e.getValue()).append(" "));
        System.err.println(summaryLog.toString().trim());

        log.info("[SchemaResolver] Step 2 total: " + allRelevantNames.size()
                + " objects in " + (System.currentTimeMillis() - t) + "ms");

        // ── Step 3: Q3 — Reverse deps (who calls our seeds) ──
        // ONE batched query for all seeds (chunked) instead of one query per seed: the old loop
        // fired a separate ALL_DEPENDENCIES lookup for every seed object, which dominated the
        // resolver on packages with many seeds.
        t = System.currentTimeMillis();
        int reverseRows = 0;
        List<SeedInfo> q3Seeds = new ArrayList<>();
        for (SeedInfo seed : seeds) {
            String refType = seed.objectType.contains("PACKAGE") ? "PACKAGE" : seed.objectType;
            q3Seeds.add(new SeedInfo(seed.objectName, seed.owner, refType));
        }
        for (int batch = 0; batch < q3Seeds.size(); batch += 100) {
            List<SeedInfo> chunk = q3Seeds.subList(batch, Math.min(batch + 100, q3Seeds.size()));
            StringBuilder tuples = new StringBuilder();
            for (int i = 0; i < chunk.size(); i++) {
                if (i > 0) tuples.append(",");
                tuples.append("(?,?,?)");
            }
            String sql = "SELECT referenced_owner, referenced_name, referenced_type, "
                    + "owner AS caller_schema, name AS caller_name, type AS caller_type "
                    + "FROM ALL_DEPENDENCIES "
                    + "WHERE (referenced_owner, referenced_name, referenced_type) IN (" + tuples + ") "
                    + "ORDER BY referenced_owner, referenced_name, owner, type, name";
            try (PreparedStatement ps = connManager.prepareStatement(conn, sql)) {
                int p = 1;
                for (SeedInfo seed : chunk) {
                    ps.setString(p++, seed.owner);
                    ps.setString(p++, seed.objectName);
                    ps.setString(p++, seed.objectType);
                }
                connManager.incrementDbCalls();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String callerSchema = rs.getString("caller_schema");
                        String callerName   = rs.getString("caller_name");
                        String callerType   = rs.getString("caller_type");
                        if (callerName == null) continue;

                        String upper       = callerName.toUpperCase();
                        String upperSchema = callerSchema != null ? callerSchema.toUpperCase() : "UNKNOWN";
                        if ("SYS".equals(upperSchema) || "PUBLIC".equals(upperSchema)) continue;

                        String normType = callerType != null ? callerType.toUpperCase().replace(" ", "_") : "UNKNOWN";
                        objectToSchema.putIfAbsent(upper, upperSchema);
                        objectToType.putIfAbsent(upper, normType);

                        Map<String, String> row = new LinkedHashMap<>();
                        row.put("callerSchema", upperSchema);
                        row.put("callerName", upper);
                        row.put("callerType", normType);
                        row.put("referencedObject", rs.getString("referenced_name") == null
                                ? null : rs.getString("referenced_name").toUpperCase());
                        row.put("referencedSchema", rs.getString("referenced_owner") == null
                                ? null : rs.getString("referenced_owner").toUpperCase());
                        reverseDepResults.add(row);
                        reverseRows++;
                    }
                }
                connManager.noteQuerySuccess();
            } catch (SQLException e) {
                connManager.noteQueryFailure();
                log.warn("[SchemaResolver] Q3 batched reverse-dep lookup failed: " + e.getMessage());
            }
        }
        log.info("[SchemaResolver] Step 3 (reverse deps): " + reverseRows + " rows in "
                + (System.currentTimeMillis() - t) + "ms");

        // ── Step 4: Gap-fill unresolved names (batched ALL_OBJECTS) ──
        t = System.currentTimeMillis();
        resolveRemainingObjects(conn, allRelevantNames);
        log.info("[SchemaResolver] Step 4 (gap-fill): " + objectToSchema.size()
                + " objects in " + (System.currentTimeMillis() - t) + "ms");

        // ── Step 5: Q4 — Direct deps for BFS (batched) ──
        t = System.currentTimeMillis();
        loadDirectDependencies(conn, allRelevantNames);
        log.info("[SchemaResolver] Step 5 (Q4 direct deps): " + dependencyCache.size()
                + " entries in " + (System.currentTimeMillis() - t) + "ms");

        saveToDisk();

        log.info("[SchemaResolver] DONE: " + objectToSchema.size() + " objects, "
                + dependencyCache.size() + " dep entries, total "
                + (System.currentTimeMillis() - totalStart) + "ms");
        } finally {
            connManager.returnToPool(conn);
        }
    }

    public SchemaResolver(DbConnectionManager connManager) throws SQLException {
        throw new IllegalArgumentException("Entry points REQUIRED — use SchemaResolver(connManager, entryPoints)");
    }

    // ── Seed resolution (batched) ──

    private List<SeedInfo> resolveSeedObjects(Connection conn, Set<String> seedNames) throws SQLException {
        List<SeedInfo> seeds = new ArrayList<>();
        if (seedNames.isEmpty()) return seeds;

        String schemaIn = buildInClause(configuredSchemas);
        List<String> nameList = new ArrayList<>(seedNames);

        for (int batch = 0; batch < nameList.size(); batch += BATCH_SIZE) {
            int end = Math.min(batch + BATCH_SIZE, nameList.size());
            String nameIn = buildNameInClause(nameList.subList(batch, end));

            String sql = "SELECT OWNER, OBJECT_NAME, OBJECT_TYPE FROM ALL_OBJECTS "
                    + "WHERE OBJECT_NAME IN (" + nameIn + ") "
                    + "AND OWNER IN (" + schemaIn + ") "
                    + "AND OBJECT_TYPE IN ('PACKAGE','PACKAGE BODY','PROCEDURE','FUNCTION') "
                    + "ORDER BY OBJECT_NAME, "
                    + "DECODE(OBJECT_TYPE,'PACKAGE BODY',1,'PACKAGE',2,'PROCEDURE',3,'FUNCTION',4,5)";

            Map<String, SeedInfo> bestMatch = new LinkedHashMap<>();
            connManager.incrementDbCalls();
            try (Statement stmt = connManager.createStatement(conn, sql);
                 ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    String owner   = rs.getString("OWNER").toUpperCase();
                    String objName = rs.getString("OBJECT_NAME").toUpperCase();
                    String objType = rs.getString("OBJECT_TYPE").toUpperCase();
                    if (!configuredSchemas.contains(owner)) continue;
                    bestMatch.putIfAbsent(objName, new SeedInfo(objName, owner, objType));
                }
            }

            for (String seedName : nameList.subList(batch, end)) {
                SeedInfo info = bestMatch.get(seedName);
                if (info != null) {
                    seeds.add(info);
                    String normType = info.objectType.replace(" ", "_");
                    objectToSchema.put(seedName, info.owner);
                    putType(seedName, normType);
                    typeQualifiedToSchema.put(normType + "." + seedName, info.owner);
                    log.info("[SchemaResolver] Seed: " + info.owner + "."
                            + seedName + " (" + info.objectType + ")");
                } else {
                    log.info("[SchemaResolver] WARNING: " + seedName + " not found");
                }
            }
        }
        return seeds;
    }

    // ── Gap-fill ──

    private void resolveRemainingObjects(Connection conn, Set<String> allNames) throws SQLException {
        Set<String> missing = new LinkedHashSet<>();
        for (String name : allNames) {
            if (!objectToSchema.containsKey(name)) missing.add(name);
        }
        if (missing.isEmpty()) return;

        String schemaIn = buildInClause(configuredSchemas);
        List<String> nameList = new ArrayList<>(missing);

        for (int batch = 0; batch < nameList.size(); batch += BATCH_SIZE) {
            int end = Math.min(batch + BATCH_SIZE, nameList.size());
            String nameIn = buildNameInClause(nameList.subList(batch, end));

            String sql = "SELECT OWNER, OBJECT_NAME, OBJECT_TYPE FROM ALL_OBJECTS "
                    + "WHERE OWNER IN (" + schemaIn + ") AND OBJECT_NAME IN (" + nameIn + ") "
                    + "AND OBJECT_TYPE IN ("
                    + "  'TABLE','VIEW','MATERIALIZED VIEW','SEQUENCE',"
                    + "  'PACKAGE','PACKAGE BODY','PROCEDURE','FUNCTION',"
                    + "  'TRIGGER','TYPE','SYNONYM')";

            connManager.incrementDbCalls();
            try (Statement stmt = connManager.createStatement(conn, sql);
                 ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    String owner = rs.getString("OWNER").toUpperCase();
                    String name  = rs.getString("OBJECT_NAME").toUpperCase();
                    String type  = rs.getString("OBJECT_TYPE").toUpperCase().replace(" ", "_");
                    if ("SYS".equals(owner) || "PUBLIC".equals(owner)) continue;

                    typeQualifiedToSchema.put(type + "." + name, owner);
                    ambiguousObjects.computeIfAbsent(name, k -> new ArrayList<>())
                            .add(new SchemaTypePair(owner, type));
                    if (!objectToSchema.containsKey(name) || isPrimaryType(type))
                        objectToSchema.put(name, owner);
                    putType(name, type);
                }
            }
        }
    }

    // ── Q4: Direct deps (batched) ──

    private void loadDirectDependencies(Connection conn, Set<String> objectNames) throws SQLException {
        if (objectNames.isEmpty()) return;
        String schemaIn = buildInClause(configuredSchemas);
        List<String> nameList = new ArrayList<>(objectNames);

        for (int batch = 0; batch < nameList.size(); batch += BATCH_SIZE) {
            int end = Math.min(batch + BATCH_SIZE, nameList.size());
            String nameIn = buildNameInClause(nameList.subList(batch, end));

            String sql = "SELECT DISTINCT d.OWNER, d.NAME, d.REFERENCED_NAME, d.REFERENCED_TYPE "
                    + "FROM ALL_DEPENDENCIES d "
                    + "WHERE d.OWNER IN (" + schemaIn + ") AND d.NAME IN (" + nameIn + ") "
                    + "AND d.REFERENCED_TYPE IN ("
                    + "  'PACKAGE','PACKAGE BODY','PROCEDURE','FUNCTION',"
                    + "  'TABLE','VIEW','MATERIALIZED VIEW','SEQUENCE',"
                    + "  'TRIGGER','TYPE','SYNONYM') "
                    + "ORDER BY d.OWNER, d.NAME";

            connManager.incrementDbCalls();
            try (Statement stmt = connManager.createStatement(conn, sql);
                 ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    String owner   = rs.getString("OWNER").toUpperCase();
                    String name    = rs.getString("NAME").toUpperCase();
                    String refName = rs.getString("REFERENCED_NAME").toUpperCase();
                    dependencyCache.computeIfAbsent(owner + "." + name, k -> new LinkedHashSet<>())
                            .add(refName);
                }
            }
        }
    }

    // ═══════════════════════════════════════════════
    // Lookup methods (called by DependencyCrawler)
    // ═══════════════════════════════════════════════

    public List<String> getConfiguredSchemas() {
        return configuredSchemas;
    }

    /**
     * Record an owner/type for a name that was resolved by a BATCHED catalog query elsewhere
     * (e.g. DependencyCrawler's level metadata), so the resolver never needs a per-name
     * {@code tryResolveFromDb} round-trip to learn it. Additive and idempotent: an existing
     * schema mapping is never overwritten, and the type uses rank precedence so a VIEW/MV is
     * never downgraded to TABLE.
     */
    public void registerBatchResolution(String objectName, String owner, String objectType) {
        if (objectName == null || objectName.isBlank()) return;
        String name = objectName.toUpperCase().trim();
        if (name.contains(".")) name = name.split("\\.", 2)[1];
        if (owner != null && !owner.isBlank()) {
            objectToSchema.putIfAbsent(name, owner.toUpperCase());
        }
        if (objectType != null && !objectType.isBlank()) {
            String norm = objectType.toUpperCase().replace(" ", "_");
            putType(name, norm);
            if (owner != null && !owner.isBlank()) {
                typeQualifiedToSchema.putIfAbsent(norm + "." + name, owner.toUpperCase());
            }
        }
    }

    // Cache of (OWNER|OBJECT) → present? for cross-schema standalone-object probes.
    private final Map<String, Boolean> foreignOwnerProbe = new ConcurrentHashMap<>();

    /**
     * True if a standalone PROCEDURE/FUNCTION named {@code objectName} exists under owner
     * {@code owner}. Used to recognise a call written as OWNER.OBJECT where OWNER is a real
     * schema (the login user can read it) rather than a package — Oracle qualifies calls to
     * standalone routines in another schema exactly this way. On a hit, the owner is registered
     * as a discovered schema so every subsequent owner-IN clause (source fetch, existence checks)
     * can see it, and the object's schema/type are recorded. Result cached to avoid repeat DB hits.
     */
    public boolean isForeignOwnerObject(String owner, String objectName) {
        if (owner == null || objectName == null) return false;
        String o = owner.toUpperCase().trim();
        String n = objectName.toUpperCase().trim();
        if (o.isEmpty() || n.isEmpty()) return false;
        String key = o + "|" + n;
        Boolean cached = foreignOwnerProbe.get(key);
        if (cached != null) return cached;

        boolean present = false;
        Connection conn = null;
        try {
            conn = connManager.getAnyConnection();
            if (conn == null) { foreignOwnerProbe.put(key, false); return false; }
            String sql = "SELECT OBJECT_TYPE FROM ALL_OBJECTS "
                    + "WHERE OWNER = ? AND OBJECT_NAME = ? "
                    + "AND OBJECT_TYPE IN ('PROCEDURE','FUNCTION') AND ROWNUM = 1";
            connManager.incrementDbCalls();
            try (PreparedStatement ps = connManager.prepareStatement(conn, sql)) {
                ps.setString(1, o);
                ps.setString(2, n);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        present = true;
                        String type = rs.getString("OBJECT_TYPE").toUpperCase().replace(" ", "_");
                        registerDiscoveredSchema(o);
                        objectToSchema.putIfAbsent(n, o);
                        putType(n, type);
                        typeQualifiedToSchema.putIfAbsent(type + "." + n, o);
                        ambiguousObjects.computeIfAbsent(n, k -> new ArrayList<>())
                                .add(new SchemaTypePair(o, type));
                        log.info("[SchemaResolver] Cross-schema object resolved: {}.{} ({})", o, n, type);
                    }
                }
            }
            connManager.returnToPool(conn);
            conn = null;
        } catch (SQLException e) {
            connManager.closeAndDiscard(conn);
            conn = null;
            connManager.noteQueryFailure();
            log.warn("[SchemaResolver] Foreign-owner probe failed for {}.{}: {}", o, n, e.getMessage());
        } finally {
            if (conn != null) connManager.closeAndDiscard(conn);
        }
        foreignOwnerProbe.put(key, present);
        return present;
    }

    /** Register a foreign owner (discovered via a cross-schema reference) so it participates in
     *  owner-IN clauses here and in the SourceDownloader / existence-check queries. */
    public void registerDiscoveredSchema(String schema) {
        if (schema == null || schema.isBlank()) return;
        String s = schema.toUpperCase().trim();
        synchronized (configuredSchemas) {
            if (!configuredSchemas.contains(s)) configuredSchemas.add(s);
        }
        connManager.registerDiscoveredSchema(s);
    }

    public String resolveSchema(String objectName) {
        if (objectName == null) return null;
        String upper = objectName.toUpperCase().trim();
        if (upper.contains(".")) {
            String[] p = upper.split("\\.", 2);
            if (configuredSchemas.contains(p[0])) return p[0];
        }
        return objectToSchema.get(upper);
    }

    public String resolveSchema(String objectName, String objectType) {
        if (objectName == null) return null;
        String upper = objectName.toUpperCase().trim();
        if (upper.contains(".")) {
            String[] p = upper.split("\\.", 2);
            if (configuredSchemas.contains(p[0])) return p[0];
            upper = p[1];
        }
        if (objectType != null) {
            String s = typeQualifiedToSchema.get(objectType.toUpperCase().replace(" ", "_") + "." + upper);
            if (s != null) return s;
        }
        return objectToSchema.get(upper);
    }

    public String resolveSchema(String objectName, String objectType, String preferredSchema) {
        if (objectName == null) return null;
        String upper = objectName.toUpperCase().trim();
        if (upper.contains(".")) {
            String[] p = upper.split("\\.", 2);
            if (configuredSchemas.contains(p[0])) return p[0];
            upper = p[1];
        }
        if (preferredSchema != null) {
            List<SchemaTypePair> cands = ambiguousObjects.get(upper);
            if (cands != null) {
                String pref = preferredSchema.toUpperCase();
                for (SchemaTypePair c : cands) {
                    if (c.schema.equals(pref) && (objectType == null
                            || c.objectType.equalsIgnoreCase(objectType.replace(" ", "_"))))
                        return c.schema;
                }
            }
        }
        if (objectType != null) {
            String s = typeQualifiedToSchema.get(objectType.toUpperCase().replace(" ", "_") + "." + upper);
            if (s != null) return s;
        }
        return objectToSchema.get(upper);
    }

    public void batchResolveTableOwners(Collection<String> tableNames) {
        if (tableNames == null || tableNames.isEmpty()) return;
        Set<String> missing = new LinkedHashSet<>();
        for (String name : tableNames) {
            if (name == null) continue;
            String upper = name.toUpperCase().trim();
            if (upper.contains(".")) upper = upper.split("\\.", 2)[1];
            // Query when the schema is unknown, OR when the current type is (still) TABLE and this
            // name's type has not yet been verified against ALL_OBJECTS. The second case is the
            // root-cause fix for materialized views (and cross-schema views): ALL_DEPENDENCIES
            // reports an MV read as referenced_type='TABLE', so the name is already in
            // objectToSchema as TABLE and would otherwise never reach the MV-preferring DECODE
            // query below that upgrades it to MATERIALIZED_VIEW / VIEW.
            if (!objectToSchema.containsKey(upper)) {
                missing.add(upper);
            } else if (!tableTypeResolved.contains(upper)) {
                String cur = objectToType.get(upper);
                if (cur == null || "TABLE".equals(cur)) missing.add(upper);
            }
        }
        if (missing.isEmpty()) return;
        // Mark as type-verified up-front so concurrent/repeat calls don't re-queue the same names.
        tableTypeResolved.addAll(missing);

        Connection conn = null;
        try {
            conn = connManager.getAnyConnection();
            if (conn == null) return;

            String schemaIn = buildInClause(configuredSchemas);
            List<String> nameList = new ArrayList<>(missing);

            for (int batch = 0; batch < nameList.size(); batch += BATCH_SIZE) {
                int end = Math.min(batch + BATCH_SIZE, nameList.size());
                String nameIn = buildNameInClause(nameList.subList(batch, end));

                // No OWNER filter — allow cross-schema views/tables (e.g. OPUS_CORE views referenced by CUSTOMER code).
                // Configured schemas are still preferred via ORDER BY so local objects win on name collisions.
                // Dialect-aware: ALL_OBJECTS exists only in Oracle, so every other dialect gets the
                // equivalent query against ITS OWN catalog (SYSCAT.TABLES / information_schema / sys.objects).
                String sql = buildTableOwnerLookupSql(nameIn, schemaIn);
                if (sql == null) return;   // dialect has no usable catalog — nothing to resolve

                connManager.incrementDbCalls();
                try (Statement stmt = connManager.createStatement(conn, sql);
                     ResultSet rs = stmt.executeQuery(sql)) {
                    while (rs.next()) {
                        String owner = rs.getString("OWNER").toUpperCase();
                        String objName = rs.getString("OBJECT_NAME").toUpperCase();
                        String objType = rs.getString("OBJECT_TYPE").toUpperCase().replace(" ", "_");
                        if (!objectToSchema.containsKey(objName)) {
                            objectToSchema.put(objName, owner);
                            typeQualifiedToSchema.put(objType + "." + objName, owner);
                        }
                        // Type set with rank precedence regardless of the schema guard, so an MV
                        // row upgrades a previously-stored container-TABLE type for the same name.
                        putType(objName, objType);
                    }
                }
            }
            log.info("[SchemaResolver] Batch resolved " + missing.size()
                    + " table names, found " + missing.stream()
                    .filter(n -> objectToSchema.containsKey(n)).count());
            connManager.returnToPool(conn);
            conn = null;
        } catch (SQLException e) {
            connManager.closeAndDiscard(conn);
            conn = null;
            connManager.noteQueryFailure();
            log.info("[SchemaResolver] Batch table lookup failed: " + e.getMessage());
        } finally {
            if (conn != null) connManager.closeAndDiscard(conn);
        }
    }

    /**
     * Build the per-dialect "resolve these object names to owner + type" query used by
     * {@link #batchResolveTableOwners}. Every dialect returns the SAME three columns
     * (OWNER, OBJECT_NAME, OBJECT_TYPE), uppercased, with VIEW/MATERIALIZED VIEW preferred over
     * TABLE and configured schemas ordered first, so the single result-reading loop is shared.
     *
     * <p>Oracle's ALL_OBJECTS does not exist in any other dialect — running it against DB2 /
     * MySQL / PostgreSQL / SQL Server threw on every batch (DB2 SQLCODE=-204,
     * {@code relation "all_objects" does not exist}, etc.), discarding a pooled connection each
     * time and leaving table owners/types unresolved for those dialects.
     *
     * @return the dialect's SQL, or {@code null} when no catalog lookup is available
     */
    private String buildTableOwnerLookupSql(String nameIn, String schemaIn) {
        // Prefer configured schemas on name collisions; skipped when no schemas are configured
        // (an empty "IN ()" list is a syntax error in every dialect).
        boolean hasSchemas = schemaIn != null && !schemaIn.isEmpty();

        if (connManager.isDB2()) {
            // SYSCAT.TABLES.TYPE: T/P = table, V/W = view, S = MQT (materialized query table).
            return "SELECT RTRIM(TABSCHEMA) AS OWNER, RTRIM(TABNAME) AS OBJECT_NAME, "
                 + "CASE TYPE WHEN 'S' THEN 'MATERIALIZED VIEW' WHEN 'V' THEN 'VIEW' "
                 + "WHEN 'W' THEN 'VIEW' ELSE 'TABLE' END AS OBJECT_TYPE "
                 + "FROM SYSCAT.TABLES WHERE TABNAME IN (" + nameIn + ") "
                 + "AND TYPE IN ('T','P','V','W','S','N') "
                 + "ORDER BY "
                 + (hasSchemas ? "CASE WHEN TABSCHEMA IN (" + schemaIn + ") THEN 0 ELSE 1 END, " : "")
                 + "CASE TYPE WHEN 'S' THEN 1 WHEN 'V' THEN 2 WHEN 'W' THEN 2 ELSE 3 END";
        }
        if (connManager.isMySQL()) {
            // MySQL has no materialized views; TABLE_SCHEMA is the database.
            return "SELECT UPPER(TABLE_SCHEMA) AS OWNER, UPPER(TABLE_NAME) AS OBJECT_NAME, "
                 + "CASE WHEN TABLE_TYPE = 'VIEW' THEN 'VIEW' ELSE 'TABLE' END AS OBJECT_TYPE "
                 + "FROM information_schema.TABLES WHERE UPPER(TABLE_NAME) IN (" + nameIn + ") "
                 + "AND TABLE_SCHEMA NOT IN ('mysql','information_schema','performance_schema','sys') "
                 + "ORDER BY "
                 + (hasSchemas ? "CASE WHEN UPPER(TABLE_SCHEMA) IN (" + schemaIn + ") THEN 0 ELSE 1 END, " : "")
                 + "CASE WHEN TABLE_TYPE = 'VIEW' THEN 2 ELSE 3 END";
        }
        if (connManager.isPostgres()) {
            // relkind: r/p = table, v = view, m = materialized view.
            return "SELECT UPPER(ns.nspname) AS \"OWNER\", UPPER(c.relname) AS \"OBJECT_NAME\", "
                 + "CASE c.relkind WHEN 'm' THEN 'MATERIALIZED VIEW' WHEN 'v' THEN 'VIEW' "
                 + "ELSE 'TABLE' END AS \"OBJECT_TYPE\" "
                 + "FROM pg_catalog.pg_class c "
                 + "JOIN pg_catalog.pg_namespace ns ON ns.oid = c.relnamespace "
                 + "WHERE UPPER(c.relname) IN (" + nameIn + ") "
                 + "AND c.relkind IN ('r','p','v','m') "
                 + "AND ns.nspname NOT IN ('pg_catalog','information_schema') "
                 + "AND ns.nspname NOT LIKE 'pg_toast%' "
                 + "ORDER BY "
                 + (hasSchemas ? "CASE WHEN UPPER(ns.nspname) IN (" + schemaIn + ") THEN 0 ELSE 1 END, " : "")
                 + "CASE c.relkind WHEN 'm' THEN 1 WHEN 'v' THEN 2 ELSE 3 END";
        }
        if (connManager.isSqlServer()) {
            // SQL Server has no materialized views (indexed views are still type 'V').
            return "SELECT UPPER(s.name) AS OWNER, UPPER(o.name) AS OBJECT_NAME, "
                 + "CASE o.type WHEN 'V' THEN 'VIEW' ELSE 'TABLE' END AS OBJECT_TYPE "
                 + "FROM sys.objects o JOIN sys.schemas s ON s.schema_id = o.schema_id "
                 + "WHERE UPPER(o.name) IN (" + nameIn + ") AND o.type IN ('U','V') "
                 + "ORDER BY "
                 + (hasSchemas ? "CASE WHEN UPPER(s.name) IN (" + schemaIn + ") THEN 0 ELSE 1 END, " : "")
                 + "CASE o.type WHEN 'V' THEN 2 ELSE 3 END";
        }
        // Oracle (default): unchanged.
        return "SELECT OWNER, OBJECT_NAME, OBJECT_TYPE FROM ALL_OBJECTS "
             + "WHERE OBJECT_NAME IN (" + nameIn + ") "
             + "AND OBJECT_TYPE IN ('TABLE','VIEW','MATERIALIZED VIEW') "
             + (hasSchemas ? "ORDER BY CASE WHEN OWNER IN (" + schemaIn + ") THEN 0 ELSE 1 END, " : "ORDER BY ")
             // Prefer MATERIALIZED VIEW / VIEW over TABLE: an MV also has a container
             // TABLE row of the same name in ALL_OBJECTS; preferring the MV row tags it
             // correctly. Real base tables only have a TABLE row, so they stay TABLE.
             + "DECODE(OBJECT_TYPE,'MATERIALIZED VIEW',1,'VIEW',2,'TABLE',3,4)";
    }

    public String resolveObjectType(String objectName) {
        if (objectName == null) return null;
        String upper = objectName.toUpperCase().trim();
        if (upper.contains(".")) upper = upper.split("\\.", 2)[1];
        return objectToType.get(upper);
    }

    /** Precedence so a plain TABLE never overrides a VIEW/MATERIALIZED_VIEW (Oracle MVs also have
     *  a same-named container TABLE row in ALL_OBJECTS). Higher rank wins regardless of insert order. */
    private static int typeRank(String t) {
        if (t == null) return -1;
        switch (t.toUpperCase()) {
            case "MATERIALIZED_VIEW": return 4;
            case "VIEW":              return 3;
            case "PACKAGE": case "PACKAGE_BODY": case "PROCEDURE": case "FUNCTION": case "TRIGGER":
                                      return 3;
            case "TABLE":             return 1;
            default:                  return 0;
        }
    }

    /** Store an object's type using rank precedence (never downgrade a VIEW/MV to TABLE). */
    private void putType(String name, String type) {
        if (name == null || type == null) return;
        String key = name.toUpperCase().trim();
        String cur = objectToType.get(key);
        if (cur == null || typeRank(type) > typeRank(cur)) objectToType.put(key, type);
    }

    /**
     * Populate objectToType (TABLE / VIEW / MATERIALIZED_VIEW) for the given table names by
     * querying the ACTIVE dialect's catalog. Non-Oracle dialects skip the ALL_OBJECTS pass, so
     * without this every referenced object defaults to "TABLE" and views/MVs are invisible in
     * the UI. Uppercase names; results stored uppercased. Safe/no-op if connManager is null.
     */
    public void batchResolveObjectTypes(java.util.Collection<String> names) {
        // names is advisory only — the per-dialect query returns every table/view/MV in the
        // active schema, so one call fully populates objectToType for both per-node and aggregate
        // table tagging. (Oracle already resolves types via batchResolveTableOwners.)
        if (connManager == null) return;
        String sql;
        if (connManager.isMySQL()) {
            // All user schemas (databases), not just the primary — so cross-database views/MVs
            // referenced by the code are tagged correctly (not defaulted to TABLE).
            sql = "SELECT UPPER(TABLE_NAME) n, CASE WHEN TABLE_TYPE='VIEW' THEN 'VIEW' ELSE 'TABLE' END t "
                + "FROM information_schema.TABLES "
                + "WHERE TABLE_SCHEMA NOT IN ('mysql','information_schema','performance_schema','sys')";
        } else if (connManager.isPostgres()) {
            sql = "SELECT UPPER(c.relname) n, "
                + "CASE c.relkind WHEN 'm' THEN 'MATERIALIZED_VIEW' WHEN 'v' THEN 'VIEW' ELSE 'TABLE' END t "
                + "FROM pg_class c JOIN pg_namespace ns ON ns.oid=c.relnamespace "
                + "WHERE c.relkind IN ('r','v','m','p')";
        } else if (connManager.isDB2()) {
            sql = "SELECT UPPER(RTRIM(TABNAME)) n, "
                + "CASE TYPE WHEN 'V' THEN 'VIEW' WHEN 'S' THEN 'MATERIALIZED_VIEW' WHEN 'W' THEN 'VIEW' ELSE 'TABLE' END t "
                + "FROM SYSCAT.TABLES WHERE TABSCHEMA NOT LIKE 'SYS%'";   // all user schemas, incl. cross-schema refs
        } else if (connManager.isSqlServer()) {
            sql = "SELECT UPPER(name) n, CASE type WHEN 'V' THEN 'VIEW' ELSE 'TABLE' END t "
                + "FROM sys.objects WHERE type IN ('U','V')";
        } else {
            return;   // Oracle already resolves types via batchResolveTableOwners (ALL_OBJECTS)
        }
        java.sql.Connection conn = null;
        try {
            conn = connManager.getAnyConnection();
            connManager.incrementDbCalls();
            try (java.sql.Statement st = connManager.createStatement(conn, sql);
                 java.sql.ResultSet rs = st.executeQuery(sql)) {
                while (rs.next()) {
                    String n = rs.getString("n");
                    String t = rs.getString("t");
                    if (n != null && t != null) putType(n, t);
                }
            }
            connManager.returnToPool(conn);
            conn = null;
        } catch (java.sql.SQLException e) {
            connManager.closeAndDiscard(conn);
            conn = null;
            connManager.noteQueryFailure();
            log.info("[SchemaResolver] batchResolveObjectTypes failed (non-fatal): " + e.getMessage());
        } finally {
            if (conn != null) connManager.closeAndDiscard(conn);
        }
    }

    public String stripSchema(String objectName) {
        if (objectName == null) return null;
        if (objectName.contains(".")) {
            String[] p = objectName.split("\\.", 2);
            if (configuredSchemas.contains(p[0].toUpperCase())) return p[1];
        }
        return objectName;
    }

    public boolean isKnownObject(String objectName) {
        if (objectName == null) return false;
        String upper = objectName.toUpperCase().trim();
        if (upper.contains(".")) upper = upper.split("\\.", 2)[1];
        return objectToSchema.containsKey(upper);
    }

    public boolean isDependency(String src, String tgt) {
        if (src == null || tgt == null) return false;
        return getTransitiveDependencies(src.toUpperCase().trim()).contains(tgt.toUpperCase().trim());
    }

    public boolean hasDependencyData(String objectName) {
        if (objectName == null) return false;
        String upper = objectName.toUpperCase().trim();
        for (String schema : configuredSchemas) {
            if (dependencyCache.containsKey(schema.toUpperCase() + "." + upper)) return true;
        }
        return false;
    }

    /**
     * A1: the FULL reachable-object closure for an entry point, available UP FRONT from the
     * resolver's Q1 CONNECT BY transitive-dependency query (and the Q4 direct-dependency batch that
     * fills the in-memory graph). The crawler fetches exactly this set in ONE wave before the BFS
     * starts, so the BFS can scope in memory. Returns an empty set for a dialect with no closure
     * data (MySQL / SQL Server / DB2 / Postgres skip Oracle dependency resolution), which is the
     * crawler's signal to fall back to level-by-level discovery.
     *
     * <p>A qualified entry ({@code PKG.PROC}) is reduced to its owning object, matching the BFS's
     * own package resolution.
     */
    public Set<String> getReachableClosure(String entryPoint) {
        if (entryPoint == null) return Collections.emptySet();
        String upper = entryPoint.toUpperCase().trim();
        String obj = upper.contains(".") ? upper.split("\\.", 2)[0] : upper;
        return getTransitiveDependencies(obj);
    }

    public Set<String> getTransitiveDependencies(String objectName) {
        if (objectName == null) return Collections.emptySet();
        String upper = objectName.toUpperCase().trim();
        Set<String> cached = transitiveDepCache.get(upper);
        if (cached != null) return cached;

        Set<String> result = new LinkedHashSet<>();
        Queue<String> q = new LinkedList<>();
        Set<String> visited = new HashSet<>();
        q.add(upper); visited.add(upper);

        int depth = 0;
        while (!q.isEmpty() && depth < MAX_LEVEL) {
            int sz = q.size();
            for (int i = 0; i < sz; i++) {
                String cur = q.poll();
                for (String schema : configuredSchemas) {
                    Set<String> deps = dependencyCache.get(schema.toUpperCase() + "." + cur);
                    if (deps != null) {
                        for (String dep : deps) {
                            result.add(dep);
                            if (visited.add(dep)) q.add(dep);
                        }
                    }
                }
            }
            depth++;
        }
        transitiveDepCache.put(upper, result);
        return result;
    }

    public Set<String> getDependencies(String objectName) {
        if (objectName == null) return Collections.emptySet();
        String upper = objectName.toUpperCase().trim();
        Set<String> all = new LinkedHashSet<>();
        for (String schema : configuredSchemas) {
            Set<String> deps = dependencyCache.get(schema.toUpperCase() + "." + upper);
            if (deps != null) all.addAll(deps);
        }
        return all;
    }

    // ═══════════════════════════════════════════════
    // Export query results for JSON output
    // ═══════════════════════════════════════════════

    public List<Map<String, String>> exportObjectResolution() {
        List<Map<String, String>> rows = new ArrayList<>();
        for (var e : objectToSchema.entrySet()) {
            Map<String, String> row = new LinkedHashMap<>();
            row.put("objectName", e.getKey());
            row.put("schema", e.getValue());
            row.put("objectType", objectToType.getOrDefault(e.getKey(), "UNKNOWN"));
            rows.add(row);
        }
        rows.sort(Comparator.comparing((Map<String, String> m) -> m.get("schema"))
                .thenComparing(m -> m.get("objectType"))
                .thenComparing(m -> m.get("objectName")));
        return rows;
    }

    public Map<String, List<String>> exportDirectDependencies() {
        Map<String, List<String>> result = new LinkedHashMap<>();
        dependencyCache.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> result.put(e.getKey(), new ArrayList<>(e.getValue())));
        return result;
    }

    public List<Map<String, String>> exportReverseDependencies() {
        return reverseDepResults;
    }

    public List<Map<String, Object>> exportAmbiguousObjects() {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (var e : ambiguousObjects.entrySet()) {
            if (e.getValue().size() <= 1) continue;
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("objectName", e.getKey());
            List<Map<String, String>> candidates = new ArrayList<>();
            for (SchemaTypePair p : e.getValue()) {
                Map<String, String> c = new LinkedHashMap<>();
                c.put("schema", p.schema);
                c.put("objectType", p.objectType);
                candidates.add(c);
            }
            row.put("candidates", candidates);
            rows.add(row);
        }
        rows.sort(Comparator.comparing(m -> (String) m.get("objectName")));
        return rows;
    }

    // ═══════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════

    /**
     * On-demand DB lookup for a single object name that wasn't discovered during Q1.
     * Called by DependencyCrawler when ALL_DEPENDENCIES data is incomplete (e.g. only table deps returned).
     * Returns true if the object exists in a configured schema, so the caller should NOT skip it.
     */
    public boolean tryResolveFromDb(String objectName) {
        if (objectName == null) return false;
        String upper = objectName.toUpperCase().trim();
        if (objectToSchema.containsKey(upper)) return true;
        if (!dbResolveAttempted.add(upper)) return false;

        Connection conn = null;
        try {
            conn = connManager.getAnyConnection();
            if (conn == null) return false;
            String schemaIn = buildInClause(configuredSchemas);
            String sql = "SELECT OWNER, OBJECT_TYPE FROM ALL_OBJECTS "
                    + "WHERE OBJECT_NAME = ? AND OWNER IN (" + schemaIn + ") "
                    + "AND OBJECT_TYPE IN ('PACKAGE','PACKAGE BODY','PROCEDURE','FUNCTION') "
                    + "AND ROWNUM = 1";
            connManager.incrementDbCalls();
            long t0 = System.currentTimeMillis();
            try (PreparedStatement ps = connManager.prepareStatement(conn, sql)) {
                ps.setString(1, upper);
                try (ResultSet rs = ps.executeQuery()) {
                    long elapsed = System.currentTimeMillis() - t0;
                    if (rs.next()) {
                        String owner = rs.getString("OWNER").toUpperCase();
                        String type  = rs.getString("OBJECT_TYPE").toUpperCase().replace(" ", "_");
                        objectToSchema.putIfAbsent(upper, owner);
                        objectToType.putIfAbsent(upper, type);
                        typeQualifiedToSchema.putIfAbsent(type + "." + upper, owner);
                        log.info("[SchemaResolver] On-demand resolved: {}.{} ({}) in {}ms", owner, upper, type, elapsed);
                        connManager.returnToPool(conn);
                        conn = null;
                        return true;
                    } else {
                        log.info("[SchemaResolver] On-demand: {} not found in DB ({}ms)", upper, elapsed);
                    }
                }
            }
            connManager.returnToPool(conn);
            conn = null;
        } catch (SQLException e) {
            connManager.closeAndDiscard(conn);
            conn = null;
            connManager.noteQueryFailure();
            log.warn("[SchemaResolver] On-demand lookup failed for {}: {}", upper, e.getMessage());
        } finally {
            if (conn != null) connManager.closeAndDiscard(conn);
        }
        return false;
    }

    private boolean isPrimaryType(String t) {
        return "TABLE".equals(t) || "VIEW".equals(t) || "MATERIALIZED_VIEW".equals(t)
                || "PACKAGE".equals(t) || "PACKAGE_BODY".equals(t)
                || "PROCEDURE".equals(t) || "FUNCTION".equals(t);
    }

    private String buildInClause(List<String> vals) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < vals.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append("'").append(vals.get(i).toUpperCase().replace("'", "''")).append("'");
        }
        return sb.toString();
    }

    private String buildNameInClause(List<String> names) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < names.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append("'").append(names.get(i).toUpperCase().replace("'", "''")).append("'");
        }
        return sb.toString();
    }

    // ═══════════════════════════════════════════════
    // Disk cache
    // ═══════════════════════════════════════════════

    private void saveToDisk() {
        if (store == null) return;
        try {
            writeTsv(cacheDir.resolve("objects.tsv"), objectToSchema);
            writeTsv(cacheDir.resolve("object_types.tsv"), objectToType);
            writeTsv(cacheDir.resolve("type_qualified.tsv"), typeQualifiedToSchema);

            try (Writer w = openWrite(cacheDir.resolve("ambiguous.tsv"))) {
                for (var e : ambiguousObjects.entrySet()) {
                    StringBuilder sb = new StringBuilder(e.getKey()).append("\t");
                    for (int i = 0; i < e.getValue().size(); i++) {
                        if (i > 0) sb.append(",");
                        SchemaTypePair p = e.getValue().get(i);
                        sb.append(p.schema).append(":").append(p.objectType);
                    }
                    sb.append("\n");
                    w.write(sb.toString());
                }
            }
            try (Writer w = openWrite(cacheDir.resolve("dependencies.tsv"))) {
                for (var e : dependencyCache.entrySet())
                    w.write(e.getKey() + "\t" + String.join(",", e.getValue()) + "\n");
            }
            try (Writer w = openWrite(cacheDir.resolve("reverse_deps.tsv"))) {
                for (Map<String, String> row : reverseDepResults)
                    w.write(row.get("callerSchema") + "\t" + row.get("callerName") + "\t"
                            + row.get("callerType") + "\t" + row.get("referencedObject") + "\t"
                            + row.get("referencedSchema") + "\n");
            }
            log.info("[SchemaResolver] Cache saved: " + cacheDir);
        } catch (IOException e) {
            log.info("[SchemaResolver] Cache save FAILED: " + e.getMessage());
        }
    }

    /** Buffered UTF-8 writer onto a store artifact. Closing it is the commit point. */
    private Writer openWrite(ArtifactKey key) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(store.write(key), StandardCharsets.UTF_8));
    }

    /** All lines of a store artifact, or an empty list when it does not exist. */
    private List<String> readLines(ArtifactKey key) throws IOException {
        List<String> lines = new ArrayList<>();
        try (BufferedReader r = new BufferedReader(
                new InputStreamReader(store.read(key), StandardCharsets.UTF_8))) {
            String line;
            while ((line = r.readLine()) != null) lines.add(line);
        }
        return lines;
    }

    private void writeTsv(ArtifactKey file, Map<String, String> map) throws IOException {
        try (Writer w = openWrite(file)) {
            for (var e : map.entrySet()) w.write(e.getKey() + "\t" + e.getValue() + "\n");
        }
    }

    private boolean loadFromDisk() {
        if (store == null) return false;
        ArtifactKey f = cacheDir.resolve("objects.tsv");
        if (!store.exists(f)) return false;
        try {
            readTsv(f, objectToSchema);
            readTsv(cacheDir.resolve("object_types.tsv"), objectToType);
            readTsv(cacheDir.resolve("type_qualified.tsv"), typeQualifiedToSchema);

            ArtifactKey ambF = cacheDir.resolve("ambiguous.tsv");
            if (store.exists(ambF)) {
                for (String line : readLines(ambF)) {
                    String[] parts = line.split("\t", 2);
                    if (parts.length == 2) {
                        List<SchemaTypePair> pairs = new ArrayList<>();
                        for (String pair : parts[1].split(",")) {
                            String[] sp = pair.split(":", 2);
                            if (sp.length == 2) pairs.add(new SchemaTypePair(sp[0], sp[1]));
                        }
                        ambiguousObjects.put(parts[0], pairs);
                    }
                }
            }
            ArtifactKey depF = cacheDir.resolve("dependencies.tsv");
            if (store.exists(depF)) {
                for (String line : readLines(depF)) {
                    String[] parts = line.split("\t", 2);
                    if (parts.length == 2)
                        dependencyCache.put(parts[0], new LinkedHashSet<>(Arrays.asList(parts[1].split(","))));
                }
            }
            ArtifactKey revF = cacheDir.resolve("reverse_deps.tsv");
            if (store.exists(revF)) {
                for (String line : readLines(revF)) {
                    String[] parts = line.split("\t", 5);
                    if (parts.length == 5) {
                        Map<String, String> row = new LinkedHashMap<>();
                        row.put("callerSchema", parts[0]);
                        row.put("callerName", parts[1]);
                        row.put("callerType", parts[2]);
                        row.put("referencedObject", parts[3]);
                        row.put("referencedSchema", parts[4]);
                        reverseDepResults.add(row);
                    }
                }
            }
            return !objectToSchema.isEmpty();
        } catch (IOException e) {
            log.info("[SchemaResolver] Cache load FAILED: " + e.getMessage());
            objectToSchema.clear(); objectToType.clear(); typeQualifiedToSchema.clear();
            ambiguousObjects.clear(); dependencyCache.clear(); reverseDepResults.clear();
            return false;
        }
    }

    private void readTsv(ArtifactKey file, Map<String, String> target) throws IOException {
        if (!store.exists(file)) return;
        for (String line : readLines(file)) {
            String[] parts = line.split("\t", 2);
            if (parts.length == 2) target.put(parts[0], parts[1]);
        }
    }
}
