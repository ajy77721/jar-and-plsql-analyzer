package com.plsql.parser.flow;

import java.io.*;
import java.nio.file.*;
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

    private final Map<String, String> objectToSchema = new ConcurrentHashMap<>();
    private final Map<String, String> objectToType = new ConcurrentHashMap<>();
    private final Map<String, String> typeQualifiedToSchema = new ConcurrentHashMap<>();
    private final Map<String, List<SchemaTypePair>> ambiguousObjects = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> dependencyCache = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> transitiveDepCache = new ConcurrentHashMap<>();
    private final List<Map<String, String>> reverseDepResults = new ArrayList<>();

    private final List<String> configuredSchemas;
    private final DbConnectionManager connManager;
    private Path cacheDir;

    private static final int MAX_LEVEL = 10;
    private static final int BATCH_SIZE = 500;

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
        this.connManager = connManager;
        this.configuredSchemas = connManager.getAvailableSchemas();

        if (entryPoints == null || entryPoints.isEmpty()) {
            throw new IllegalArgumentException("Entry points REQUIRED — will not load entire DB");
        }

        long totalStart = System.currentTimeMillis();
        this.cacheDir = Paths.get("cache", "schema-resolver");

        if (loadFromDisk()) {
            System.err.println("[SchemaResolver] Loaded from disk cache: " + objectToSchema.size()
                    + " objects, " + dependencyCache.size() + " deps in "
                    + (System.currentTimeMillis() - totalStart) + "ms");
            return;
        }

        Connection conn = connManager.getAnyConnection();

        // Parse entry points → seed names (PKG.PROC → PKG)
        Set<String> seedNames = new LinkedHashSet<>();
        for (String ep : entryPoints) {
            String upper = ep.toUpperCase().trim();
            seedNames.add(upper.contains(".") ? upper.split("\\.", 2)[0] : upper);
        }

        // ── Step 1: Resolve seed owners (batched ALL_OBJECTS) ──
        long t = System.currentTimeMillis();
        List<SeedInfo> seeds = resolveSeedObjects(conn, seedNames);
        System.err.println("[SchemaResolver] Step 1 (seed owners): " + seeds.size()
                + " seeds in " + (System.currentTimeMillis() - t) + "ms");

        if (seeds.isEmpty()) {
            System.err.println("[SchemaResolver] WARNING: No seeds found — nothing to resolve");
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

            System.err.println("[SchemaResolver] Q1 running: " + seed.owner + "." + seed.objectName);

            int rowCount = 0;
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
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
                        if (!objectToType.containsKey(upperObj) || isPrimaryType(normType)) {
                            objectToType.put(upperObj, normType);
                        }
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
            System.err.println("[SchemaResolver] Q1 done: " + seed.owner + "." + seed.objectName
                    + " → " + rowCount + " deps in " + (System.currentTimeMillis() - t) + "ms");
        }

        // Log Q2 equivalent (from Q1 data)
        System.err.println("[SchemaResolver] Q2 (tables/views from Q1): " + tableViewCount);

        // Log Q5 equivalent (summary from Q1 data)
        StringBuilder summaryLog = new StringBuilder("[SchemaResolver] Q5 summary: ");
        typeSummary.entrySet().stream()
                .sorted((a, b) -> b.getValue() - a.getValue())
                .forEach(e -> summaryLog.append(e.getKey()).append("=").append(e.getValue()).append(" "));
        System.err.println(summaryLog.toString().trim());

        System.err.println("[SchemaResolver] Step 2 total: " + allRelevantNames.size()
                + " objects in " + (System.currentTimeMillis() - t) + "ms");

        // ── Step 3: Q3 — Reverse deps (who calls our seeds) ──
        t = System.currentTimeMillis();
        for (SeedInfo seed : seeds) {
            String refType = seed.objectType.contains("PACKAGE") ? "PACKAGE" : seed.objectType;

            String sql = "SELECT owner AS caller_schema, name AS caller_name, type AS caller_type "
                    + "FROM ALL_DEPENDENCIES "
                    + "WHERE referenced_owner = ? AND referenced_name = ? AND referenced_type = ? "
                    + "ORDER BY owner, type, name";

            int count = 0;
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, seed.owner);
                ps.setString(2, seed.objectName);
                ps.setString(3, refType);
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
                        row.put("referencedObject", seed.objectName);
                        row.put("referencedSchema", seed.owner);
                        reverseDepResults.add(row);
                        count++;
                    }
                }
            }
            System.err.println("[SchemaResolver] Q3 reverse deps for " + seed.objectName + ": " + count);
        }
        System.err.println("[SchemaResolver] Step 3 (reverse deps): "
                + (System.currentTimeMillis() - t) + "ms");

        // ── Step 4: Gap-fill unresolved names (batched ALL_OBJECTS) ──
        t = System.currentTimeMillis();
        resolveRemainingObjects(conn, allRelevantNames);
        System.err.println("[SchemaResolver] Step 4 (gap-fill): " + objectToSchema.size()
                + " objects in " + (System.currentTimeMillis() - t) + "ms");

        // ── Step 5: Q4 — Direct deps for BFS (batched) ──
        t = System.currentTimeMillis();
        loadDirectDependencies(conn, allRelevantNames);
        System.err.println("[SchemaResolver] Step 5 (Q4 direct deps): " + dependencyCache.size()
                + " entries in " + (System.currentTimeMillis() - t) + "ms");

        saveToDisk();

        System.err.println("[SchemaResolver] DONE: " + objectToSchema.size() + " objects, "
                + dependencyCache.size() + " dep entries, total "
                + (System.currentTimeMillis() - totalStart) + "ms");
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
            try (Statement stmt = conn.createStatement();
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
                    objectToType.put(seedName, normType);
                    typeQualifiedToSchema.put(normType + "." + seedName, info.owner);
                    System.err.println("[SchemaResolver] Seed: " + info.owner + "."
                            + seedName + " (" + info.objectType + ")");
                } else {
                    System.err.println("[SchemaResolver] WARNING: " + seedName + " not found");
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
            try (Statement stmt = conn.createStatement();
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
                    if (!objectToType.containsKey(name) || isPrimaryType(type))
                        objectToType.put(name, type);
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
            try (Statement stmt = conn.createStatement();
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
            if (!objectToSchema.containsKey(upper)) missing.add(upper);
        }
        if (missing.isEmpty()) return;

        try {
            Connection conn = connManager.getAnyConnection();
            if (conn == null) return;

            String schemaIn = buildInClause(configuredSchemas);
            List<String> nameList = new ArrayList<>(missing);

            for (int batch = 0; batch < nameList.size(); batch += BATCH_SIZE) {
                int end = Math.min(batch + BATCH_SIZE, nameList.size());
                String nameIn = buildNameInClause(nameList.subList(batch, end));

                String sql = "SELECT OWNER, OBJECT_NAME, OBJECT_TYPE FROM ALL_OBJECTS "
                        + "WHERE OBJECT_NAME IN (" + nameIn + ") "
                        + "AND OWNER IN (" + schemaIn + ") "
                        + "AND OBJECT_TYPE IN ('TABLE','VIEW','MATERIALIZED VIEW') "
                        + "ORDER BY DECODE(OBJECT_TYPE,'TABLE',1,'MATERIALIZED VIEW',2,'VIEW',3,4)";

                connManager.incrementDbCalls();
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(sql)) {
                    while (rs.next()) {
                        String owner = rs.getString("OWNER").toUpperCase();
                        String objName = rs.getString("OBJECT_NAME").toUpperCase();
                        String objType = rs.getString("OBJECT_TYPE").toUpperCase().replace(" ", "_");
                        if (!objectToSchema.containsKey(objName)) {
                            objectToSchema.put(objName, owner);
                            objectToType.put(objName, objType);
                            typeQualifiedToSchema.put(objType + "." + objName, owner);
                        }
                    }
                }
            }
            System.err.println("[SchemaResolver] Batch resolved " + missing.size()
                    + " table names, found " + missing.stream()
                    .filter(n -> objectToSchema.containsKey(n)).count());
        } catch (SQLException e) {
            System.err.println("[SchemaResolver] Batch table lookup failed: " + e.getMessage());
        }
    }

    public String resolveObjectType(String objectName) {
        if (objectName == null) return null;
        String upper = objectName.toUpperCase().trim();
        if (upper.contains(".")) upper = upper.split("\\.", 2)[1];
        return objectToType.get(upper);
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
        try {
            Files.createDirectories(cacheDir);
            writeTsv(cacheDir.resolve("objects.tsv"), objectToSchema);
            writeTsv(cacheDir.resolve("object_types.tsv"), objectToType);
            writeTsv(cacheDir.resolve("type_qualified.tsv"), typeQualifiedToSchema);

            try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(cacheDir.resolve("ambiguous.tsv")))) {
                for (var e : ambiguousObjects.entrySet()) {
                    StringBuilder sb = new StringBuilder(e.getKey()).append("\t");
                    for (int i = 0; i < e.getValue().size(); i++) {
                        if (i > 0) sb.append(",");
                        SchemaTypePair p = e.getValue().get(i);
                        sb.append(p.schema).append(":").append(p.objectType);
                    }
                    w.println(sb);
                }
            }
            try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(cacheDir.resolve("dependencies.tsv")))) {
                for (var e : dependencyCache.entrySet())
                    w.println(e.getKey() + "\t" + String.join(",", e.getValue()));
            }
            try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(cacheDir.resolve("reverse_deps.tsv")))) {
                for (Map<String, String> row : reverseDepResults)
                    w.println(row.get("callerSchema") + "\t" + row.get("callerName") + "\t"
                            + row.get("callerType") + "\t" + row.get("referencedObject") + "\t"
                            + row.get("referencedSchema"));
            }
            System.err.println("[SchemaResolver] Cache saved: " + cacheDir.toAbsolutePath());
        } catch (IOException e) {
            System.err.println("[SchemaResolver] Cache save FAILED: " + e.getMessage());
        }
    }

    private void writeTsv(Path file, Map<String, String> map) throws IOException {
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(file))) {
            for (var e : map.entrySet()) w.println(e.getKey() + "\t" + e.getValue());
        }
    }

    private boolean loadFromDisk() {
        Path f = cacheDir.resolve("objects.tsv");
        if (!Files.exists(f)) return false;
        try {
            readTsv(f, objectToSchema);
            readTsv(cacheDir.resolve("object_types.tsv"), objectToType);
            readTsv(cacheDir.resolve("type_qualified.tsv"), typeQualifiedToSchema);

            Path ambF = cacheDir.resolve("ambiguous.tsv");
            if (Files.exists(ambF)) {
                for (String line : Files.readAllLines(ambF)) {
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
            Path depF = cacheDir.resolve("dependencies.tsv");
            if (Files.exists(depF)) {
                for (String line : Files.readAllLines(depF)) {
                    String[] parts = line.split("\t", 2);
                    if (parts.length == 2)
                        dependencyCache.put(parts[0], new LinkedHashSet<>(Arrays.asList(parts[1].split(","))));
                }
            }
            Path revF = cacheDir.resolve("reverse_deps.tsv");
            if (Files.exists(revF)) {
                for (String line : Files.readAllLines(revF)) {
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
            System.err.println("[SchemaResolver] Cache load FAILED: " + e.getMessage());
            objectToSchema.clear(); objectToType.clear(); typeQualifiedToSchema.clear();
            ambiguousObjects.clear(); dependencyCache.clear(); reverseDepResults.clear();
            return false;
        }
    }

    private void readTsv(Path file, Map<String, String> target) throws IOException {
        if (!Files.exists(file)) return;
        for (String line : Files.readAllLines(file)) {
            String[] parts = line.split("\t", 2);
            if (parts.length == 2) target.put(parts[0], parts[1]);
        }
    }
}
