package com.plsqlanalyzer.web.browse;

import java.sql.*;
import java.util.*;

/**
 * PostgreSQL implementation of {@link DbBrowseProvider} using {@code information_schema} /
 * {@code pg_catalog} (routines, tables, columns, indexes, triggers, views, {@code pg_matviews}
 * for materialized views). SQL patterns mirror the crawler's {@code PostgresTableEnricher}.
 *
 * <p>{@link #listPackages} follows the same convention as {@link SqlServerBrowseProvider}:
 * Postgres has no packages, so standalone routines are listed as flat entries. When
 * {@code schema} is null, queries search across all visible (non-system) schemas.
 */
public class PostgresBrowseProvider implements DbBrowseProvider {

    private static final String SYSTEM_SCHEMAS = "'pg_catalog','information_schema'";

    @Override
    public List<Map<String, Object>> listObjects(Connection c, String schema) throws SQLException {
        List<Map<String, Object>> out = new ArrayList<>();

        String routineSql = "SELECT routine_name nm, routine_type rt, routine_schema sch "
                + "FROM information_schema.routines "
                + (schema != null
                    ? "WHERE upper(routine_schema) = ? ORDER BY routine_type, routine_name"
                    : "WHERE routine_schema NOT IN (" + SYSTEM_SCHEMAS + ") ORDER BY routine_schema, routine_type, routine_name");
        try (PreparedStatement ps = c.prepareStatement(routineSql)) {
            if (schema != null) ps.setString(1, schema.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> obj = new LinkedHashMap<>();
                    obj.put("owner", rs.getString("sch"));
                    obj.put("objectName", rs.getString("nm"));
                    obj.put("objectType", "PROCEDURE".equalsIgnoreCase(rs.getString("rt")) ? "PROCEDURE" : "FUNCTION");
                    out.add(obj);
                }
            }
        }

        String tableSql = "SELECT table_name nm, table_type tt, table_schema sch "
                + "FROM information_schema.tables "
                + (schema != null
                    ? "WHERE upper(table_schema) = ? ORDER BY table_type, table_name"
                    : "WHERE table_schema NOT IN (" + SYSTEM_SCHEMAS + ") ORDER BY table_schema, table_type, table_name");
        try (PreparedStatement ps = c.prepareStatement(tableSql)) {
            if (schema != null) ps.setString(1, schema.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> obj = new LinkedHashMap<>();
                    obj.put("owner", rs.getString("sch"));
                    obj.put("objectName", rs.getString("nm"));
                    obj.put("objectType", "VIEW".equalsIgnoreCase(rs.getString("tt")) ? "VIEW" : "TABLE");
                    out.add(obj);
                }
            }
        }

        String mvSql = "SELECT matviewname nm, schemaname sch FROM pg_matviews "
                + (schema != null ? "WHERE upper(schemaname) = ? ORDER BY matviewname"
                                  : "ORDER BY schemaname, matviewname");
        try (PreparedStatement ps = c.prepareStatement(mvSql)) {
            if (schema != null) ps.setString(1, schema.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> obj = new LinkedHashMap<>();
                    obj.put("owner", rs.getString("sch"));
                    obj.put("objectName", rs.getString("nm"));
                    obj.put("objectType", "MATERIALIZED VIEW");
                    out.add(obj);
                }
            }
        }
        return out;
    }

    @Override
    public String fetchSource(Connection c, String schema, String objectName) throws SQLException {
        String sql = "SELECT pg_get_functiondef(p.oid) def FROM pg_proc p "
                + "JOIN pg_namespace n ON n.oid = p.pronamespace "
                + "WHERE upper(p.proname) = ? " + (schema != null ? "AND upper(n.nspname) = ? " : "")
                + "LIMIT 1";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, objectName.toUpperCase());
            if (schema != null) ps.setString(2, schema.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String def = rs.getString("def");
                    if (def != null && !def.isBlank()) return def;
                }
            } catch (SQLException ignored) {
                // pg_get_functiondef can fail for some routine kinds — fall through to "not found".
            }
        }
        return null;
    }

    @Override
    public Map<String, Object> tableInfo(Connection c, String schema, String tableName) throws SQLException {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("tableName", tableName.toUpperCase());
        String[] resolved = PostgresTableQueries.resolveObject(c, schema, tableName);
        if (resolved == null) {
            result.put("found", false);
            return result;
        }
        String sch = resolved[0];
        boolean isView = resolved[1].contains("VIEW");
        result.put("schema", sch);
        if (isView) {
            result.put("isView", true);
            result.put("dbResolvedType", resolved[1]);
            String def = PostgresTableQueries.viewDefinition(c, sch, tableName);
            if (def != null) result.put("viewDefinition", def);
        }
        List<Map<String, Object>> columns = PostgresTableQueries.columns(c, sch, tableName);
        result.put("columns", columns);
        result.put("columnCount", columns.size());
        result.put("constraints", PostgresTableQueries.constraints(c, sch, tableName));
        result.put("indexes", PostgresTableQueries.indexes(c, sch, tableName));
        result.put("triggers", PostgresTableQueries.triggers(c, sch, tableName));
        result.put("found", true);
        return result;
    }

    @Override
    public Map<String, Object> findObject(Connection c, String input) throws SQLException {
        String in = input.toUpperCase().trim();
        String[] parts = in.split("\\.", -1);
        String schema = parts.length >= 2 ? parts[parts.length - 2] : null;
        String name = parts[parts.length - 1];

        Map<String, Object> result = new LinkedHashMap<>();

        String routineSql = "SELECT routine_schema sch, routine_name nm, routine_type rt "
                + "FROM information_schema.routines WHERE upper(routine_name) = ? "
                + (schema != null ? "AND upper(routine_schema) = ? " : "") + "LIMIT 1";
        try (PreparedStatement ps = c.prepareStatement(routineSql)) {
            ps.setString(1, name);
            if (schema != null) ps.setString(2, schema);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    result.put("found", true);
                    result.put("schema", rs.getString("sch"));
                    result.put("objectName", rs.getString("nm"));
                    result.put("objectType", "PROCEDURE".equalsIgnoreCase(rs.getString("rt")) ? "PROCEDURE" : "FUNCTION");
                    return result;
                }
            }
        }

        String mvSql = "SELECT schemaname sch, matviewname nm FROM pg_matviews WHERE upper(matviewname) = ? "
                + (schema != null ? "AND upper(schemaname) = ? " : "") + "LIMIT 1";
        try (PreparedStatement ps = c.prepareStatement(mvSql)) {
            ps.setString(1, name);
            if (schema != null) ps.setString(2, schema);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    result.put("found", true);
                    result.put("schema", rs.getString("sch"));
                    result.put("objectName", rs.getString("nm"));
                    result.put("objectType", "MATERIALIZED VIEW");
                    return result;
                }
            }
        }

        String tableSql = "SELECT table_schema sch, table_name nm, table_type tt "
                + "FROM information_schema.tables WHERE upper(table_name) = ? "
                + (schema != null ? "AND upper(table_schema) = ? " : "") + "LIMIT 1";
        try (PreparedStatement ps = c.prepareStatement(tableSql)) {
            ps.setString(1, name);
            if (schema != null) ps.setString(2, schema);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    result.put("found", true);
                    result.put("schema", rs.getString("sch"));
                    result.put("objectName", rs.getString("nm"));
                    result.put("objectType", "VIEW".equalsIgnoreCase(rs.getString("tt")) ? "VIEW" : "TABLE");
                    return result;
                }
            }
        }

        result.put("found", false);
        result.put("input", in);
        result.put("message", "Object not found in any visible schema");
        return result;
    }

    @Override
    public List<Map<String, Object>> listPackages(Connection c, String schema) throws SQLException {
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> obj : listObjects(c, schema)) {
            String type = String.valueOf(obj.get("objectType"));
            if (!"PROCEDURE".equals(type) && !"FUNCTION".equals(type)) continue;
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("objectName", obj.get("objectName"));
            m.put("objectType", type);
            m.put("status", "VALID");
            m.put("hasBody", true);
            m.put("procCount", 1);
            out.add(m);
        }
        return out;
    }
}
