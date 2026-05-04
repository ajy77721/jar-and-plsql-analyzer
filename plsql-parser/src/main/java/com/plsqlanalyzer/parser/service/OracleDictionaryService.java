package com.plsqlanalyzer.parser.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

/**
 * Queries Oracle data dictionary views to retrieve structural dependency data,
 * trigger info, procedure listings, source code, and table metadata.
 * Complements ANTLR4 parsing with pre-computed Oracle dependency information.
 */
public class OracleDictionaryService {

    private static final Logger log = LoggerFactory.getLogger(OracleDictionaryService.class);

    // ---- Dependencies (ALL_DEPENDENCIES) ----

    /**
     * Get direct dependencies for an object (what does it depend on?).
     */
    public List<DependencyRecord> getDirectDependencies(Connection conn, String owner, String objectName) throws SQLException {
        String sql = """
            SELECT referenced_owner, referenced_name, referenced_type, referenced_link_name, dependency_type
            FROM all_dependencies
            WHERE owner = ? AND name = ?
            ORDER BY referenced_owner, referenced_name
            """;
        List<DependencyRecord> deps = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, owner.toUpperCase());
            ps.setString(2, objectName.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    deps.add(new DependencyRecord(
                            rs.getString("REFERENCED_OWNER"),
                            rs.getString("REFERENCED_NAME"),
                            rs.getString("REFERENCED_TYPE"),
                            rs.getString("REFERENCED_LINK_NAME"),
                            rs.getString("DEPENDENCY_TYPE")));
                }
            }
        }
        return deps;
    }

    /**
     * Recursively walk ALL_DEPENDENCIES via BFS to build the full dependency graph.
     * Uses batch queries per BFS level (1 query per depth level instead of per object).
     */
    public Map<String, List<DependencyRecord>> getRecursiveDependencies(
            Connection conn, String owner, String objectName, int maxDepth) throws SQLException {

        Map<String, List<DependencyRecord>> graph = new LinkedHashMap<>();
        Queue<String> queue = new LinkedList<>();
        Set<String> visited = new HashSet<>();

        String startKey = owner.toUpperCase() + "." + objectName.toUpperCase();
        queue.add(startKey);
        visited.add(startKey);
        int depth = 0;

        while (!queue.isEmpty() && depth < maxDepth) {
            // Batch: collect all keys at this BFS level and fetch deps in one query
            List<String> levelKeys = new ArrayList<>(queue);
            queue.clear();

            Map<String, List<DependencyRecord>> levelDeps = getBatchDependencies(conn, levelKeys);

            for (Map.Entry<String, List<DependencyRecord>> entry : levelDeps.entrySet()) {
                graph.put(entry.getKey(), entry.getValue());
                for (DependencyRecord dep : entry.getValue()) {
                    if (isPLSQLType(dep.referencedType())) {
                        String depKey = dep.referencedOwner() + "." + dep.referencedName();
                        if (!visited.contains(depKey)) {
                            visited.add(depKey);
                            queue.add(depKey);
                        }
                    }
                }
            }
            depth++;
        }

        return graph;
    }

    /**
     * Batch fetch dependencies for multiple objects in a single query.
     * Uses (owner, name) IN clause to fetch all at once instead of per-object queries.
     */
    public Map<String, List<DependencyRecord>> getBatchDependencies(
            Connection conn, List<String> objectKeys) throws SQLException {
        if (objectKeys.isEmpty()) return Collections.emptyMap();

        Map<String, List<DependencyRecord>> result = new LinkedHashMap<>();

        // Chunk to stay within Oracle bind variable limits (450 tuples × 2 = 900 bind vars)
        int chunkSize = 450;
        for (int i = 0; i < objectKeys.size(); i += chunkSize) {
            List<String> chunk = objectKeys.subList(i, Math.min(i + chunkSize, objectKeys.size()));

            StringBuilder sql = new StringBuilder("""
                SELECT owner, name, referenced_owner, referenced_name, referenced_type,
                       referenced_link_name, dependency_type
                FROM all_dependencies WHERE (owner, name) IN (""");
            for (int j = 0; j < chunk.size(); j++) {
                if (j > 0) sql.append(",");
                sql.append("(?,?)");
            }
            sql.append(") ORDER BY owner, name");

            try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
                int idx = 1;
                for (String key : chunk) {
                    String[] parts = key.split("\\.", 2);
                    ps.setString(idx++, parts[0]);
                    ps.setString(idx++, parts[1]);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String key = rs.getString("OWNER") + "." + rs.getString("NAME");
                        result.computeIfAbsent(key, k -> new ArrayList<>()).add(new DependencyRecord(
                                rs.getString("REFERENCED_OWNER"),
                                rs.getString("REFERENCED_NAME"),
                                rs.getString("REFERENCED_TYPE"),
                                rs.getString("REFERENCED_LINK_NAME"),
                                rs.getString("DEPENDENCY_TYPE")));
                    }
                }
            }
        }

        // Ensure all requested keys have entries (even if empty)
        for (String key : objectKeys) {
            result.putIfAbsent(key, new ArrayList<>());
        }

        return result;
    }

    /**
     * Get reverse dependencies (who depends on this object?).
     */
    public List<DependencyRecord> getReferences(Connection conn, String owner, String objectName) throws SQLException {
        String sql = """
            SELECT owner AS referenced_owner, name AS referenced_name, type AS referenced_type,
                   NULL AS referenced_link_name, dependency_type
            FROM all_dependencies
            WHERE referenced_owner = ? AND referenced_name = ?
            ORDER BY owner, name
            """;
        List<DependencyRecord> refs = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, owner.toUpperCase());
            ps.setString(2, objectName.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    refs.add(new DependencyRecord(
                            rs.getString("REFERENCED_OWNER"),
                            rs.getString("REFERENCED_NAME"),
                            rs.getString("REFERENCED_TYPE"),
                            rs.getString("REFERENCED_LINK_NAME"),
                            rs.getString("DEPENDENCY_TYPE")));
                }
            }
        }
        return refs;
    }

    // ---- Triggers (ALL_TRIGGERS) ----

    /**
     * Get triggers defined on a table.
     */
    public List<TriggerRecord> getTriggersForTable(Connection conn, String tableOwner, String tableName) throws SQLException {
        String sql = """
            SELECT owner, trigger_name, trigger_type, triggering_event,
                   table_owner, table_name, status, description
            FROM all_triggers
            WHERE table_owner = ? AND table_name = ? AND status = 'ENABLED'
            ORDER BY trigger_name
            """;
        List<TriggerRecord> triggers = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, tableOwner.toUpperCase());
            ps.setString(2, tableName.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    triggers.add(new TriggerRecord(
                            rs.getString("OWNER"),
                            rs.getString("TRIGGER_NAME"),
                            rs.getString("TRIGGER_TYPE"),
                            rs.getString("TRIGGERING_EVENT"),
                            rs.getString("TABLE_OWNER"),
                            rs.getString("TABLE_NAME"),
                            rs.getString("STATUS"),
                            rs.getString("DESCRIPTION")));
                }
            }
        }
        return triggers;
    }

    /**
     * Search triggers across all configured schemas for a given table name.
     */
    public List<TriggerRecord> getTriggersForTableAnySchema(Connection conn, String tableName,
                                                              List<String> schemas) throws SQLException {
        List<TriggerRecord> allTriggers = new ArrayList<>();
        for (String schema : schemas) {
            allTriggers.addAll(getTriggersForTable(conn, schema, tableName));
        }
        return allTriggers;
    }

    // ---- Procedures (ALL_PROCEDURES) ----

    /**
     * List procedures/functions in a package.
     */
    public List<ProcedureRecord> listProcedures(Connection conn, String owner, String packageName) throws SQLException {
        String sql = """
            SELECT object_name, procedure_name, overload, subprogram_id
            FROM all_procedures
            WHERE owner = ? AND object_name = ?
              AND procedure_name IS NOT NULL
            ORDER BY subprogram_id
            """;
        List<ProcedureRecord> procs = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, owner.toUpperCase());
            ps.setString(2, packageName.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    procs.add(new ProcedureRecord(
                            rs.getString("OBJECT_NAME"),
                            rs.getString("PROCEDURE_NAME"),
                            rs.getString("OVERLOAD"),
                            rs.getInt("SUBPROGRAM_ID")));
                }
            }
        }
        return procs;
    }

    // ---- Source (ALL_SOURCE) ----

    /**
     * Fetch source code lines for an object from ALL_SOURCE.
     */
    public List<String> fetchSource(Connection conn, String owner, String objectName, String objectType) throws SQLException {
        String sql = """
            SELECT text FROM all_source
            WHERE owner = ? AND name = ? AND type = ?
            ORDER BY line
            """;
        List<String> lines = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, owner.toUpperCase());
            ps.setString(2, objectName.toUpperCase());
            ps.setString(3, objectType.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String text = rs.getString("TEXT");
                    // ALL_SOURCE includes trailing newlines; trim just the trailing newline
                    if (text != null && text.endsWith("\n")) {
                        text = text.substring(0, text.length() - 1);
                    }
                    lines.add(text != null ? text : "");
                }
            }
        }
        return lines;
    }

    // ---- Objects (ALL_OBJECTS) ----

    /**
     * List PL/SQL objects in a schema (packages, procedures, functions, triggers).
     */
    public List<ObjectRecord> listObjects(Connection conn, String owner) throws SQLException {
        String sql = """
            SELECT object_name, object_type, status, last_ddl_time
            FROM all_objects
            WHERE owner = ?
              AND object_type IN ('PACKAGE', 'PACKAGE BODY', 'PROCEDURE', 'FUNCTION', 'TRIGGER')
            ORDER BY object_type, object_name
            """;
        List<ObjectRecord> objects = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, owner.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    objects.add(new ObjectRecord(
                            rs.getString("OBJECT_NAME"),
                            rs.getString("OBJECT_TYPE"),
                            rs.getString("STATUS"),
                            rs.getTimestamp("LAST_DDL_TIME")));
                }
            }
        }
        return objects;
    }

    // ---- Tables (ALL_TABLES + ALL_VIEWS) ----

    /**
     * Resolve which schema owns a table by checking all configured schemas.
     */
    public String resolveTableOwner(Connection conn, String tableName, List<String> schemas) throws SQLException {
        String sql = """
            SELECT owner FROM (
                SELECT owner FROM all_tables WHERE table_name = ?
                UNION
                SELECT owner FROM all_views WHERE view_name = ?
            ) WHERE owner IN (%s)
            """;
        // Build IN clause placeholders
        String placeholders = String.join(",", schemas.stream().map(s -> "?").toList());
        sql = String.format(sql, placeholders);

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            ps.setString(idx++, tableName.toUpperCase());
            ps.setString(idx++, tableName.toUpperCase());
            for (String schema : schemas) {
                ps.setString(idx++, schema.toUpperCase());
            }
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("OWNER");
                }
            }
        }
        return null;
    }

    /**
     * Check if an object exists and get its type.
     */
    public ObjectRecord getObjectInfo(Connection conn, String owner, String objectName) throws SQLException {
        String sql = """
            SELECT object_name, object_type, status, last_ddl_time
            FROM all_objects
            WHERE owner = ? AND object_name = ?
              AND object_type IN ('PACKAGE', 'PACKAGE BODY', 'PROCEDURE', 'FUNCTION', 'TRIGGER')
            ORDER BY DECODE(object_type, 'PACKAGE BODY', 1, 'PACKAGE', 2, 'PROCEDURE', 3, 'FUNCTION', 4, 5)
            FETCH FIRST 1 ROW ONLY
            """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, owner.toUpperCase());
            ps.setString(2, objectName.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new ObjectRecord(
                            rs.getString("OBJECT_NAME"),
                            rs.getString("OBJECT_TYPE"),
                            rs.getString("STATUS"),
                            rs.getTimestamp("LAST_DDL_TIME"));
                }
            }
        }
        return null;
    }

    /**
     * Resolve whether an object is a TABLE, VIEW, or MATERIALIZED VIEW.
     */
    public String resolveTableType(Connection conn, String owner, String tableName) throws SQLException {
        // Check materialized view first (it also appears in ALL_TABLES)
        String mvSql = "SELECT COUNT(*) FROM all_mviews WHERE owner = ? AND mview_name = ?";
        try (PreparedStatement ps = conn.prepareStatement(mvSql)) {
            ps.setString(1, owner.toUpperCase());
            ps.setString(2, tableName.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next() && rs.getInt(1) > 0) return "MATERIALIZED VIEW";
            }
        }
        // Check view
        String viewSql = "SELECT COUNT(*) FROM all_views WHERE owner = ? AND view_name = ?";
        try (PreparedStatement ps = conn.prepareStatement(viewSql)) {
            ps.setString(1, owner.toUpperCase());
            ps.setString(2, tableName.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next() && rs.getInt(1) > 0) return "VIEW";
            }
        }
        // Check table
        String tblSql = "SELECT COUNT(*) FROM all_tables WHERE owner = ? AND table_name = ?";
        try (PreparedStatement ps = conn.prepareStatement(tblSql)) {
            ps.setString(1, owner.toUpperCase());
            ps.setString(2, tableName.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next() && rs.getInt(1) > 0) return "TABLE";
            }
        }
        return "TABLE"; // default fallback
    }

    // ---- Table Metadata (ALL_TAB_COLUMNS + ALL_CONSTRAINTS + ALL_INDEXES) ----

    /**
     * Fetch full table metadata: columns, constraints, indexes.
     * Used during analysis to pre-cache table structure so it doesn't need live DB queries on click.
     */
    public TableMetadata fetchTableMetadata(Connection conn, String owner, String tableName) throws SQLException {
        log.debug("Fetching table metadata for {}.{}", owner, tableName);
        long start = System.currentTimeMillis();

        boolean isView = false;
        // Check if it's a table or view
        String checkSql = "SELECT COUNT(*) FROM all_tables WHERE owner = ? AND table_name = ?";
        try (PreparedStatement ps = conn.prepareStatement(checkSql)) {
            ps.setString(1, owner.toUpperCase());
            ps.setString(2, tableName.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next() && rs.getInt(1) == 0) {
                    // Check views
                    String viewSql = "SELECT COUNT(*) FROM all_views WHERE owner = ? AND view_name = ?";
                    try (PreparedStatement ps2 = conn.prepareStatement(viewSql)) {
                        ps2.setString(1, owner.toUpperCase());
                        ps2.setString(2, tableName.toUpperCase());
                        try (ResultSet rs2 = ps2.executeQuery()) {
                            if (rs2.next() && rs2.getInt(1) > 0) {
                                isView = true;
                            } else {
                                return null; // Not found
                            }
                        }
                    }
                }
            }
        }

        // Columns
        String colSql = """
            SELECT column_name, data_type, data_length, data_precision, data_scale,
                   nullable, column_id, data_default
            FROM all_tab_columns
            WHERE owner = ? AND table_name = ?
            ORDER BY column_id
            """;
        List<ColumnInfo> columns = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(colSql)) {
            ps.setString(1, owner.toUpperCase());
            ps.setString(2, tableName.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    columns.add(new ColumnInfo(
                            rs.getString("COLUMN_NAME"),
                            rs.getString("DATA_TYPE"),
                            rs.getInt("DATA_LENGTH"),
                            rs.getObject("DATA_PRECISION") != null ? rs.getInt("DATA_PRECISION") : null,
                            rs.getObject("DATA_SCALE") != null ? rs.getInt("DATA_SCALE") : null,
                            "Y".equals(rs.getString("NULLABLE")),
                            rs.getInt("COLUMN_ID"),
                            rs.getString("DATA_DEFAULT")));
                }
            }
        }

        // Constraints
        String conSql = """
            SELECT c.constraint_name, c.constraint_type, cc.column_name, cc.position,
                   c.r_constraint_name
            FROM all_constraints c
            JOIN all_cons_columns cc ON c.owner = cc.owner
                AND c.constraint_name = cc.constraint_name
            WHERE c.owner = ? AND c.table_name = ?
            ORDER BY c.constraint_type, c.constraint_name, cc.position
            """;
        List<ConstraintInfo> constraints = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(conSql)) {
            ps.setString(1, owner.toUpperCase());
            ps.setString(2, tableName.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    constraints.add(new ConstraintInfo(
                            rs.getString("CONSTRAINT_NAME"),
                            rs.getString("CONSTRAINT_TYPE"),
                            rs.getString("COLUMN_NAME"),
                            rs.getInt("POSITION"),
                            rs.getString("R_CONSTRAINT_NAME")));
                }
            }
        }

        // Indexes
        String idxSql = """
            SELECT i.index_name, i.uniqueness, ic.column_name, ic.column_position
            FROM all_indexes i
            JOIN all_ind_columns ic ON i.owner = ic.index_owner AND i.index_name = ic.index_name
            WHERE i.table_owner = ? AND i.table_name = ?
            ORDER BY i.index_name, ic.column_position
            """;
        List<IndexInfo> indexes = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(idxSql)) {
            ps.setString(1, owner.toUpperCase());
            ps.setString(2, tableName.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    indexes.add(new IndexInfo(
                            rs.getString("INDEX_NAME"),
                            rs.getString("UNIQUENESS"),
                            rs.getString("COLUMN_NAME"),
                            rs.getInt("COLUMN_POSITION")));
                }
            }
        }

        // View definition (if it's a view, fetch the SQL text)
        String viewDefinition = null;
        if (isView) {
            String viewDefSql = "SELECT text FROM all_views WHERE owner = ? AND view_name = ?";
            try (PreparedStatement ps = conn.prepareStatement(viewDefSql)) {
                ps.setString(1, owner.toUpperCase());
                ps.setString(2, tableName.toUpperCase());
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        viewDefinition = rs.getString("TEXT");
                    }
                }
            }
            log.debug("View definition for {}.{}: {} chars", owner, tableName,
                    viewDefinition != null ? viewDefinition.length() : 0);
        }

        log.debug("Table metadata for {}.{}: {} cols, {} constraints, {} indexes, isView={} in {}ms",
                owner, tableName, columns.size(), constraints.size(), indexes.size(), isView,
                System.currentTimeMillis() - start);

        return new TableMetadata(owner, tableName, isView, viewDefinition, columns, constraints, indexes);
    }

    // ---- Bulk Operations (minimize round-trips for Step 5 parallelization) ----

    /**
     * Bulk resolve table owners for many table names in a single query.
     * Returns map of TABLE_NAME -> OWNER.
     */
    public Map<String, String> bulkResolveTableOwners(Connection conn, List<String> tableNames,
                                                       List<String> schemas) throws SQLException {
        if (tableNames.isEmpty() || schemas.isEmpty()) return Collections.emptyMap();
        Map<String, String> result = new HashMap<>();

        // Chunk to avoid bind variable limits
        int chunkSize = 400;
        String schemaPlaceholders = String.join(",", schemas.stream().map(s -> "?").toList());

        for (int i = 0; i < tableNames.size(); i += chunkSize) {
            List<String> chunk = tableNames.subList(i, Math.min(i + chunkSize, tableNames.size()));
            String namePlaceholders = String.join(",", chunk.stream().map(s -> "?").toList());

            String sql = String.format("""
                SELECT table_name, owner FROM (
                    SELECT table_name, owner FROM all_tables WHERE table_name IN (%s) AND owner IN (%s)
                    UNION
                    SELECT view_name AS table_name, owner FROM all_views WHERE view_name IN (%s) AND owner IN (%s)
                )""", namePlaceholders, schemaPlaceholders, namePlaceholders, schemaPlaceholders);

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int idx = 1;
                // First UNION part: tables
                for (String name : chunk) ps.setString(idx++, name.toUpperCase());
                for (String schema : schemas) ps.setString(idx++, schema.toUpperCase());
                // Second UNION part: views
                for (String name : chunk) ps.setString(idx++, name.toUpperCase());
                for (String schema : schemas) ps.setString(idx++, schema.toUpperCase());

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        result.putIfAbsent(rs.getString("TABLE_NAME"), rs.getString("OWNER"));
                    }
                }
            }
        }
        return result;
    }

    /**
     * Bulk resolve table types (TABLE / VIEW / MATERIALIZED VIEW) for many tables.
     * Returns map of TABLE_NAME -> type string.
     */
    public Map<String, String> bulkResolveTableTypes(Connection conn, List<String> tableNames,
                                                      List<String> schemas) throws SQLException {
        if (tableNames.isEmpty()) return Collections.emptyMap();
        Map<String, String> result = new HashMap<>();

        int chunkSize = 400;
        String schemaPlaceholders = schemas.isEmpty() ? "''" :
                String.join(",", schemas.stream().map(s -> "?").toList());

        for (int i = 0; i < tableNames.size(); i += chunkSize) {
            List<String> chunk = tableNames.subList(i, Math.min(i + chunkSize, tableNames.size()));
            String namePlaceholders = String.join(",", chunk.stream().map(s -> "?").toList());

            // Check materialized views first
            String mvSql = String.format(
                    "SELECT mview_name FROM all_mviews WHERE mview_name IN (%s) AND owner IN (%s)",
                    namePlaceholders, schemaPlaceholders);
            try (PreparedStatement ps = conn.prepareStatement(mvSql)) {
                int idx = 1;
                for (String name : chunk) ps.setString(idx++, name.toUpperCase());
                for (String schema : schemas) ps.setString(idx++, schema.toUpperCase());
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) result.put(rs.getString("MVIEW_NAME"), "MATERIALIZED VIEW");
                }
            }

            // Check views (excluding those already identified as MVs)
            String viewSql = String.format(
                    "SELECT view_name FROM all_views WHERE view_name IN (%s) AND owner IN (%s)",
                    namePlaceholders, schemaPlaceholders);
            try (PreparedStatement ps = conn.prepareStatement(viewSql)) {
                int idx = 1;
                for (String name : chunk) ps.setString(idx++, name.toUpperCase());
                for (String schema : schemas) ps.setString(idx++, schema.toUpperCase());
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String vname = rs.getString("VIEW_NAME");
                        result.putIfAbsent(vname, "VIEW"); // MV takes precedence
                    }
                }
            }

            // Everything else defaults to TABLE
            for (String name : chunk) {
                result.putIfAbsent(name.toUpperCase(), "TABLE");
            }
        }
        return result;
    }

    /**
     * Bulk fetch triggers for many tables at once.
     * Returns map of TABLE_NAME -> list of TriggerRecord.
     */
    public Map<String, List<TriggerRecord>> bulkGetTriggers(Connection conn, List<String> tableNames,
                                                              List<String> schemas) throws SQLException {
        if (tableNames.isEmpty()) return Collections.emptyMap();
        Map<String, List<TriggerRecord>> result = new HashMap<>();

        int chunkSize = 400;
        String schemaPlaceholders = schemas.isEmpty() ? "''" :
                String.join(",", schemas.stream().map(s -> "?").toList());

        for (int i = 0; i < tableNames.size(); i += chunkSize) {
            List<String> chunk = tableNames.subList(i, Math.min(i + chunkSize, tableNames.size()));
            String namePlaceholders = String.join(",", chunk.stream().map(s -> "?").toList());

            String sql = String.format("""
                SELECT owner, trigger_name, trigger_type, triggering_event,
                       table_owner, table_name, status, description
                FROM all_triggers
                WHERE table_name IN (%s) AND table_owner IN (%s) AND status = 'ENABLED'
                ORDER BY table_name, trigger_name
                """, namePlaceholders, schemaPlaceholders);

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int idx = 1;
                for (String name : chunk) ps.setString(idx++, name.toUpperCase());
                for (String schema : schemas) ps.setString(idx++, schema.toUpperCase());
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String tableName = rs.getString("TABLE_NAME");
                        result.computeIfAbsent(tableName, k -> new ArrayList<>()).add(new TriggerRecord(
                                rs.getString("OWNER"),
                                rs.getString("TRIGGER_NAME"),
                                rs.getString("TRIGGER_TYPE"),
                                rs.getString("TRIGGERING_EVENT"),
                                rs.getString("TABLE_OWNER"),
                                rs.getString("TABLE_NAME"),
                                rs.getString("STATUS"),
                                rs.getString("DESCRIPTION")));
                    }
                }
            }
        }
        return result;
    }

    /**
     * Bulk fetch trigger source code for many triggers at once.
     * Returns map of "OWNER.TRIGGER_NAME" -> source lines.
     */
    public Map<String, List<String>> bulkFetchTriggerSource(Connection conn, List<String[]> ownerTriggerPairs) throws SQLException {
        if (ownerTriggerPairs.isEmpty()) return Collections.emptyMap();
        Map<String, List<String>> result = new LinkedHashMap<>();

        int chunkSize = 200;
        for (int i = 0; i < ownerTriggerPairs.size(); i += chunkSize) {
            List<String[]> chunk = ownerTriggerPairs.subList(i, Math.min(i + chunkSize, ownerTriggerPairs.size()));
            StringBuilder sql = new StringBuilder("SELECT owner, name, text FROM all_source WHERE type = 'TRIGGER' AND (");
            for (int j = 0; j < chunk.size(); j++) {
                if (j > 0) sql.append(" OR ");
                sql.append("(owner = ? AND name = ?)");
            }
            sql.append(") ORDER BY owner, name, line");

            try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
                int idx = 1;
                for (String[] pair : chunk) {
                    ps.setString(idx++, pair[0].toUpperCase());
                    ps.setString(idx++, pair[1].toUpperCase());
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String key = rs.getString("OWNER") + "." + rs.getString("NAME");
                        String text = rs.getString("TEXT");
                        if (text != null && text.endsWith("\n")) text = text.substring(0, text.length() - 1);
                        result.computeIfAbsent(key, k -> new ArrayList<>()).add(text != null ? text : "");
                    }
                }
            }
        }
        log.debug("Bulk fetched source for {} triggers", result.size());
        return result;
    }

    /**
     * Bulk fetch columns for many tables at once.
     * Returns map of "OWNER.TABLE_NAME" -> list of ColumnInfo.
     */
    public Map<String, List<ColumnInfo>> bulkFetchColumns(Connection conn, List<String[]> ownerTablePairs) throws SQLException {
        if (ownerTablePairs.isEmpty()) return Collections.emptyMap();
        Map<String, List<ColumnInfo>> result = new HashMap<>();

        int chunkSize = 400;
        for (int i = 0; i < ownerTablePairs.size(); i += chunkSize) {
            List<String[]> chunk = ownerTablePairs.subList(i, Math.min(i + chunkSize, ownerTablePairs.size()));

            // NOTE: data_default is LONG type in Oracle — cannot be used in bulk IN-clause queries
            // (same issue as ALL_VIEWS.TEXT). We omit it here and pass null for dataDefault.
            StringBuilder sql = new StringBuilder("""
                SELECT owner, table_name, column_name, data_type, data_length,
                       data_precision, data_scale, nullable, column_id
                FROM all_tab_columns WHERE (owner, table_name) IN (""");
            for (int j = 0; j < chunk.size(); j++) {
                if (j > 0) sql.append(",");
                sql.append("(?,?)");
            }
            sql.append(") ORDER BY owner, table_name, column_id");

            try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
                int idx = 1;
                for (String[] pair : chunk) {
                    ps.setString(idx++, pair[0]);
                    ps.setString(idx++, pair[1]);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String key = rs.getString("OWNER") + "." + rs.getString("TABLE_NAME");
                        result.computeIfAbsent(key, k -> new ArrayList<>()).add(new ColumnInfo(
                                rs.getString("COLUMN_NAME"),
                                rs.getString("DATA_TYPE"),
                                rs.getInt("DATA_LENGTH"),
                                rs.getObject("DATA_PRECISION") != null ? rs.getInt("DATA_PRECISION") : null,
                                rs.getObject("DATA_SCALE") != null ? rs.getInt("DATA_SCALE") : null,
                                "Y".equals(rs.getString("NULLABLE")),
                                rs.getInt("COLUMN_ID"),
                                null)); // data_default omitted — LONG type breaks bulk queries
                    }
                }
            }
        }
        return result;
    }

    /**
     * Bulk fetch constraints for many tables at once.
     */
    public Map<String, List<ConstraintInfo>> bulkFetchConstraints(Connection conn, List<String[]> ownerTablePairs) throws SQLException {
        if (ownerTablePairs.isEmpty()) return Collections.emptyMap();
        Map<String, List<ConstraintInfo>> result = new HashMap<>();

        int chunkSize = 400;
        for (int i = 0; i < ownerTablePairs.size(); i += chunkSize) {
            List<String[]> chunk = ownerTablePairs.subList(i, Math.min(i + chunkSize, ownerTablePairs.size()));

            StringBuilder sql = new StringBuilder("""
                SELECT c.owner, c.table_name, c.constraint_name, c.constraint_type,
                       cc.column_name, cc.position, c.r_constraint_name
                FROM all_constraints c
                JOIN all_cons_columns cc ON c.owner = cc.owner AND c.constraint_name = cc.constraint_name
                WHERE (c.owner, c.table_name) IN (""");
            for (int j = 0; j < chunk.size(); j++) {
                if (j > 0) sql.append(",");
                sql.append("(?,?)");
            }
            sql.append(") ORDER BY c.owner, c.table_name, c.constraint_type, c.constraint_name, cc.position");

            try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
                int idx = 1;
                for (String[] pair : chunk) {
                    ps.setString(idx++, pair[0]);
                    ps.setString(idx++, pair[1]);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String key = rs.getString("OWNER") + "." + rs.getString("TABLE_NAME");
                        result.computeIfAbsent(key, k -> new ArrayList<>()).add(new ConstraintInfo(
                                rs.getString("CONSTRAINT_NAME"),
                                rs.getString("CONSTRAINT_TYPE"),
                                rs.getString("COLUMN_NAME"),
                                rs.getInt("POSITION"),
                                rs.getString("R_CONSTRAINT_NAME")));
                    }
                }
            }
        }
        return result;
    }

    /**
     * Bulk fetch indexes for many tables at once.
     */
    public Map<String, List<IndexInfo>> bulkFetchIndexes(Connection conn, List<String[]> ownerTablePairs) throws SQLException {
        if (ownerTablePairs.isEmpty()) return Collections.emptyMap();
        Map<String, List<IndexInfo>> result = new HashMap<>();

        int chunkSize = 400;
        for (int i = 0; i < ownerTablePairs.size(); i += chunkSize) {
            List<String[]> chunk = ownerTablePairs.subList(i, Math.min(i + chunkSize, ownerTablePairs.size()));

            StringBuilder sql = new StringBuilder("""
                SELECT i.table_owner, i.table_name, i.index_name, i.uniqueness,
                       ic.column_name, ic.column_position
                FROM all_indexes i
                JOIN all_ind_columns ic ON i.owner = ic.index_owner AND i.index_name = ic.index_name
                WHERE (i.table_owner, i.table_name) IN (""");
            for (int j = 0; j < chunk.size(); j++) {
                if (j > 0) sql.append(",");
                sql.append("(?,?)");
            }
            sql.append(") ORDER BY i.table_owner, i.table_name, i.index_name, ic.column_position");

            try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
                int idx = 1;
                for (String[] pair : chunk) {
                    ps.setString(idx++, pair[0]);
                    ps.setString(idx++, pair[1]);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String key = rs.getString("TABLE_OWNER") + "." + rs.getString("TABLE_NAME");
                        result.computeIfAbsent(key, k -> new ArrayList<>()).add(new IndexInfo(
                                rs.getString("INDEX_NAME"),
                                rs.getString("UNIQUENESS"),
                                rs.getString("COLUMN_NAME"),
                                rs.getInt("COLUMN_POSITION")));
                    }
                }
            }
        }
        return result;
    }

    /**
     * Bulk fetch view definitions for many views at once.
     * NOTE: ALL_VIEWS.TEXT is type LONG — bulk IN-clause queries may return NULL.
     * Use fetchViewDefinitionsIndividual() instead for reliable results.
     */
    public Map<String, String> bulkFetchViewDefinitions(Connection conn, List<String[]> ownerViewPairs) throws SQLException {
        if (ownerViewPairs.isEmpty()) return Collections.emptyMap();
        Map<String, String> result = new HashMap<>();

        int chunkSize = 400;
        for (int i = 0; i < ownerViewPairs.size(); i += chunkSize) {
            List<String[]> chunk = ownerViewPairs.subList(i, Math.min(i + chunkSize, ownerViewPairs.size()));

            StringBuilder sql = new StringBuilder(
                    "SELECT owner, view_name, text FROM all_views WHERE (owner, view_name) IN (");
            for (int j = 0; j < chunk.size(); j++) {
                if (j > 0) sql.append(",");
                sql.append("(?,?)");
            }
            sql.append(")");

            try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
                int idx = 1;
                for (String[] pair : chunk) {
                    ps.setString(idx++, pair[0]);
                    ps.setString(idx++, pair[1]);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String key = rs.getString("OWNER") + "." + rs.getString("VIEW_NAME");
                        result.put(key, rs.getString("TEXT"));
                    }
                }
            }
        }
        return result;
    }

    /**
     * Fetch view definitions one at a time (reliable fallback for LONG column).
     * ALL_VIEWS.TEXT is Oracle type LONG which doesn't work reliably with bulk IN-clause queries.
     * Individual queries always return the TEXT correctly.
     */
    public Map<String, String> fetchViewDefinitionsIndividual(Connection conn, List<String[]> ownerViewPairs) throws SQLException {
        if (ownerViewPairs.isEmpty()) return Collections.emptyMap();
        Map<String, String> result = new HashMap<>();
        String sql = "SELECT text FROM all_views WHERE owner = ? AND view_name = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (String[] pair : ownerViewPairs) {
                ps.setString(1, pair[0]);
                ps.setString(2, pair[1]);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        String text = rs.getString("TEXT");
                        if (text != null && !text.isEmpty()) {
                            String key = pair[0] + "." + pair[1];
                            result.put(key, text);
                        }
                    }
                }
            }
        }
        log.info("Fetched {} view definitions individually out of {} views", result.size(), ownerViewPairs.size());
        return result;
    }

    // ---- Helper ----

    private boolean isPLSQLType(String objectType) {
        if (objectType == null) return false;
        return switch (objectType.toUpperCase()) {
            case "PACKAGE", "PACKAGE BODY", "PROCEDURE", "FUNCTION", "TRIGGER" -> true;
            default -> false;
        };
    }

    // ---- Record types ----

    public record DependencyRecord(
            String referencedOwner,
            String referencedName,
            String referencedType,
            String referencedLinkName,
            String dependencyType
    ) {}

    public record TriggerRecord(
            String owner,
            String triggerName,
            String triggerType,
            String triggeringEvent,
            String tableOwner,
            String tableName,
            String status,
            String description
    ) {}

    public record ProcedureRecord(
            String objectName,
            String procedureName,
            String overload,
            int subprogramId
    ) {}

    public record ObjectRecord(
            String objectName,
            String objectType,
            String status,
            Timestamp lastDdlTime
    ) {}

    // ---- Table Metadata Records ----

    public record TableMetadata(
            String owner,
            String tableName,
            boolean isView,
            String viewDefinition,
            List<ColumnInfo> columns,
            List<ConstraintInfo> constraints,
            List<IndexInfo> indexes
    ) {}

    public record ColumnInfo(
            String columnName,
            String dataType,
            int dataLength,
            Integer dataPrecision,
            Integer dataScale,
            boolean nullable,
            int columnId,
            String dataDefault
    ) {}

    public record ConstraintInfo(
            String constraintName,
            String constraintType,
            String columnName,
            int position,
            String refConstraint
    ) {}

    public record IndexInfo(
            String indexName,
            String uniqueness,
            String columnName,
            int position
    ) {}
}
