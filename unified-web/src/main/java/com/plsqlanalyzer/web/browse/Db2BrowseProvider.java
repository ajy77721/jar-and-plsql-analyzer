package com.plsqlanalyzer.web.browse;

import java.sql.*;
import java.util.*;

/**
 * IBM DB2 implementation of {@link DbBrowseProvider} using the {@code SYSCAT} catalog views
 * (SYSCAT.ROUTINES / SYSCAT.TABLES / SYSCAT.COLUMNS / ...). SQL patterns mirror the crawler's
 * {@code Db2SourceFetchStrategy} and {@code Db2TableEnricher}.
 *
 * <p>DB2 packages/modules ({@code SYSCAT.MODULES}) are not modeled here — this codebase's DB2
 * support (see {@code Db2ProcedureParser}/{@code Db2SourceFetchStrategy}) treats routines as
 * standalone ({@code SYSCAT.ROUTINES}, no module grouping), so {@link #listPackages} follows the
 * same convention as {@link SqlServerBrowseProvider}: standalone routines listed as flat entries.
 * When {@code schema} is null, queries search across all visible schemas.
 */
public class Db2BrowseProvider implements DbBrowseProvider {

    @Override
    public List<Map<String, Object>> listObjects(Connection c, String schema) throws SQLException {
        List<Map<String, Object>> out = new ArrayList<>();

        String routineSql = "SELECT ROUTINENAME nm, ROUTINETYPE rt, ROUTINESCHEMA sch FROM SYSCAT.ROUTINES "
                + "WHERE ROUTINETYPE IN ('P','F') "
                + (schema != null ? "AND UPPER(ROUTINESCHEMA) = ? ORDER BY ROUTINETYPE, ROUTINENAME"
                                   : "ORDER BY ROUTINESCHEMA, ROUTINETYPE, ROUTINENAME");
        try (PreparedStatement ps = c.prepareStatement(routineSql)) {
            if (schema != null) ps.setString(1, schema.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> obj = new LinkedHashMap<>();
                    obj.put("owner", rs.getString("sch").trim());
                    obj.put("objectName", rs.getString("nm").trim());
                    obj.put("objectType", "F".equals(rs.getString("rt")) ? "FUNCTION" : "PROCEDURE");
                    out.add(obj);
                }
            }
        }

        String tableSql = "SELECT TABNAME nm, TYPE tt, TABSCHEMA sch FROM SYSCAT.TABLES "
                + "WHERE TYPE IN ('T','V') "
                + (schema != null ? "AND UPPER(TABSCHEMA) = ? ORDER BY TYPE, TABNAME"
                                   : "ORDER BY TABSCHEMA, TYPE, TABNAME");
        try (PreparedStatement ps = c.prepareStatement(tableSql)) {
            if (schema != null) ps.setString(1, schema.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> obj = new LinkedHashMap<>();
                    obj.put("owner", rs.getString("sch").trim());
                    obj.put("objectName", rs.getString("nm").trim());
                    obj.put("objectType", "V".equals(rs.getString("tt")) ? "VIEW" : "TABLE");
                    out.add(obj);
                }
            }
        }
        return out;
    }

    @Override
    public String fetchSource(Connection c, String schema, String objectName) throws SQLException {
        String sql = "SELECT TEXT FROM SYSCAT.ROUTINES WHERE ROUTINENAME = ? "
                + (schema != null ? "AND ROUTINESCHEMA = ? " : "")
                + "AND ROUTINETYPE IN ('P','F') FETCH FIRST 1 ROWS ONLY";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, objectName.toUpperCase());
            if (schema != null) ps.setString(2, schema.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String text = rs.getString("TEXT");
                    return (text != null && !text.isBlank()) ? text : null;
                }
            }
        }
        return null;
    }

    @Override
    public Map<String, Object> tableInfo(Connection c, String schema, String tableName) throws SQLException {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("tableName", tableName.toUpperCase());
        String[] resolved = Db2TableQueries.resolveObject(c, schema, tableName);
        if (resolved == null) {
            result.put("found", false);
            return result;
        }
        String sch = resolved[0];
        boolean isView = "VIEW".equals(resolved[1]);
        result.put("schema", sch);
        if (isView) {
            result.put("isView", true);
            String def = Db2TableQueries.viewDefinition(c, sch, tableName);
            if (def != null) result.put("viewDefinition", def);
        }
        List<Map<String, Object>> columns = Db2TableQueries.columns(c, sch, tableName);
        result.put("columns", columns);
        result.put("columnCount", columns.size());
        result.put("constraints", Db2TableQueries.constraints(c, sch, tableName));
        result.put("indexes", Db2TableQueries.indexes(c, sch, tableName));
        result.put("triggers", Db2TableQueries.triggers(c, sch, tableName));
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

        String routineSql = "SELECT ROUTINESCHEMA sch, ROUTINENAME nm, ROUTINETYPE rt FROM SYSCAT.ROUTINES "
                + "WHERE ROUTINENAME = ? " + (schema != null ? "AND ROUTINESCHEMA = ? " : "")
                + "AND ROUTINETYPE IN ('P','F') FETCH FIRST 1 ROWS ONLY";
        try (PreparedStatement ps = c.prepareStatement(routineSql)) {
            ps.setString(1, name);
            if (schema != null) ps.setString(2, schema);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    result.put("found", true);
                    result.put("schema", rs.getString("sch").trim());
                    result.put("objectName", rs.getString("nm").trim());
                    result.put("objectType", "F".equals(rs.getString("rt")) ? "FUNCTION" : "PROCEDURE");
                    return result;
                }
            }
        }

        String tableSql = "SELECT TABSCHEMA sch, TABNAME nm, TYPE tt FROM SYSCAT.TABLES "
                + "WHERE TABNAME = ? " + (schema != null ? "AND TABSCHEMA = ? " : "")
                + "AND TYPE IN ('T','V') FETCH FIRST 1 ROWS ONLY";
        try (PreparedStatement ps = c.prepareStatement(tableSql)) {
            ps.setString(1, name);
            if (schema != null) ps.setString(2, schema);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    result.put("found", true);
                    result.put("schema", rs.getString("sch").trim());
                    result.put("objectName", rs.getString("nm").trim());
                    result.put("objectType", "V".equals(rs.getString("tt")) ? "VIEW" : "TABLE");
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
