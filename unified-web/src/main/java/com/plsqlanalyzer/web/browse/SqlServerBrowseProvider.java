package com.plsqlanalyzer.web.browse;

import java.sql.*;
import java.util.*;

/**
 * SQL Server implementation of {@link DbBrowseProvider} using the sys.* catalog
 * views (sys.objects / sys.sql_modules / sys.columns / ...). SQL patterns mirror
 * the crawler's SqlServerSourceFetchStrategy and SqlServerTableEnricher.
 *
 * <p>SQL Server has no packages, so {@link #listPackages} returns the standalone
 * procedures/functions as flat entries. When {@code schema} is null, queries
 * search across all visible schemas.</p>
 */
public class SqlServerBrowseProvider implements DbBrowseProvider {

    @Override
    public List<Map<String, Object>> listObjects(Connection c, String schema) throws SQLException {
        String base = "SELECT o.name nm, o.type_desc td, SCHEMA_NAME(o.schema_id) sch "
                + "FROM sys.objects o WHERE o.type IN ('P','FN','IF','TF','V','TR') ";
        String sql = schema != null
                ? base + "AND UPPER(SCHEMA_NAME(o.schema_id)) = ? ORDER BY td, nm"
                : base + "ORDER BY sch, td, nm";
        List<Map<String, Object>> out = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            if (schema != null) ps.setString(1, schema.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> obj = new LinkedHashMap<>();
                    obj.put("owner", rs.getString("sch"));
                    obj.put("objectName", rs.getString("nm"));
                    obj.put("objectType", normalizeType(rs.getString("td")));
                    out.add(obj);
                }
            }
        }
        return out;
    }

    @Override
    public String fetchSource(Connection c, String schema, String objectName) throws SQLException {
        String sql = "SELECT m.definition def FROM sys.sql_modules m "
                + "JOIN sys.objects o ON m.object_id = o.object_id "
                + "WHERE UPPER(o.name) = ? "
                + (schema != null ? "AND UPPER(SCHEMA_NAME(o.schema_id)) = ? " : "")
                + "AND o.type IN ('P','FN','IF','TF','V','TR')";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, objectName.toUpperCase());
            if (schema != null) ps.setString(2, schema.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String def = rs.getString("def");
                    return (def != null && !def.isBlank()) ? def : null;
                }
            }
        }
        return null;
    }

    @Override
    public Map<String, Object> tableInfo(Connection c, String schema, String tableName) throws SQLException {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("tableName", tableName.toUpperCase());
        String[] resolved = SqlServerTableQueries.resolveObject(c, schema, tableName);
        if (resolved == null) {
            result.put("found", false);
            return result;
        }
        String sch = resolved[0];
        boolean isView = "VIEW".equals(resolved[1]);
        result.put("schema", sch);
        if (isView) {
            result.put("isView", true);
            String def = SqlServerTableQueries.viewDefinition(c, sch, tableName);
            if (def != null) result.put("viewDefinition", def);
        }
        List<Map<String, Object>> columns = SqlServerTableQueries.columns(c, sch, tableName);
        result.put("columns", columns);
        result.put("columnCount", columns.size());
        result.put("constraints", SqlServerTableQueries.constraints(c, sch, tableName));
        result.put("indexes", SqlServerTableQueries.indexes(c, sch, tableName));
        result.put("triggers", SqlServerTableQueries.triggers(c, sch, tableName));
        result.put("found", true);
        return result;
    }

    @Override
    public Map<String, Object> findObject(Connection c, String input) throws SQLException {
        String in = input.toUpperCase().trim();
        String[] parts = in.split("\\.", -1);
        String schema = parts.length >= 2 ? parts[parts.length - 2] : null;
        String name = parts[parts.length - 1];

        String sql = "SELECT TOP 1 SCHEMA_NAME(o.schema_id) sch, o.name nm, o.type_desc td "
                + "FROM sys.objects o WHERE UPPER(o.name) = ? "
                + (schema != null ? "AND UPPER(SCHEMA_NAME(o.schema_id)) = ? " : "")
                + "AND o.type IN ('P','FN','IF','TF','V','U','TR')";
        Map<String, Object> result = new LinkedHashMap<>();
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, name);
            if (schema != null) ps.setString(2, schema);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    result.put("found", true);
                    result.put("schema", rs.getString("sch"));
                    result.put("objectName", rs.getString("nm"));
                    result.put("objectType", normalizeType(rs.getString("td")));
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

    private static String normalizeType(String typeDesc) {
        if (typeDesc == null) return "PROCEDURE";
        if (typeDesc.contains("FUNCTION")) return "FUNCTION";
        if (typeDesc.contains("VIEW")) return "VIEW";
        if (typeDesc.contains("TRIGGER")) return "TRIGGER";
        if (typeDesc.contains("USER_TABLE")) return "TABLE";
        return "PROCEDURE";
    }
}
