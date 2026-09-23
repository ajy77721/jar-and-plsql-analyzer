package com.plsqlanalyzer.web.browse;

import java.sql.*;
import java.util.*;

/**
 * SQL Server catalog queries for a single table/view, returning Oracle-shaped
 * Map/List structures (columnName/dataType/..., constraintType P/U/R, etc.).
 * SQL patterns mirror {@code SqlServerTableEnricher} but target one object.
 */
final class SqlServerTableQueries {

    private SqlServerTableQueries() {
    }

    /** Resolves schema + type (U/V) for a table/view; prefers the requested schema. */
    static String[] resolveObject(Connection c, String schema, String name) throws SQLException {
        String sql = "SELECT TOP 1 SCHEMA_NAME(o.schema_id) sch, o.type_desc td "
                + "FROM sys.objects o WHERE UPPER(o.name) = ? AND o.type IN ('U','V') "
                + (schema != null ? "AND UPPER(SCHEMA_NAME(o.schema_id)) = ? " : "")
                + "ORDER BY o.type";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, name.toUpperCase());
            if (schema != null) ps.setString(2, schema.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String td = rs.getString("td");
                    String type = td != null && td.contains("VIEW") ? "VIEW" : "TABLE";
                    return new String[]{rs.getString("sch"), type};
                }
            }
        }
        return null;
    }

    static List<Map<String, Object>> columns(Connection c, String sch, String name) throws SQLException {
        String sql = "SELECT col.name col, ty.name dtype, col.max_length ml, col.precision pr, "
                + "col.scale sc, col.is_nullable nul, col.column_id cid, dc.definition ddef, col.is_computed comp "
                + "FROM sys.columns col "
                + "JOIN sys.objects o ON o.object_id = col.object_id "
                + "JOIN sys.types ty ON ty.user_type_id = col.user_type_id "
                + "LEFT JOIN sys.default_constraints dc ON dc.object_id = col.default_object_id "
                + "WHERE UPPER(SCHEMA_NAME(o.schema_id)) = ? AND UPPER(o.name) = ? ORDER BY col.column_id";
        List<Map<String, Object>> out = new ArrayList<>();
        try (PreparedStatement ps = bind(c, sql, sch, name); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Map<String, Object> col = new LinkedHashMap<>();
                col.put("columnName", rs.getString("col"));
                col.put("dataType", rs.getString("dtype"));
                col.put("dataLength", rs.getInt("ml"));
                int pr = rs.getInt("pr");
                col.put("dataPrecision", pr > 0 ? pr : null);
                int scv = rs.getInt("sc");
                col.put("dataScale", scv > 0 ? scv : null);
                col.put("nullable", rs.getBoolean("nul"));
                col.put("columnId", rs.getInt("cid"));
                col.put("dataDefault", rs.getString("ddef"));
                if (rs.getBoolean("comp")) col.put("computed", true);
                out.add(col);
            }
        }
        return out;
    }

    static List<Map<String, Object>> constraints(Connection c, String sch, String name) throws SQLException {
        List<Map<String, Object>> out = new ArrayList<>();
        String pk = "SELECT kc.name cn, kc.type_desc td, col.name col, ic.key_ordinal pos "
                + "FROM sys.key_constraints kc "
                + "JOIN sys.objects o ON o.object_id = kc.parent_object_id "
                + "JOIN sys.index_columns ic ON ic.object_id = kc.parent_object_id AND ic.index_id = kc.unique_index_id "
                + "JOIN sys.columns col ON col.object_id = ic.object_id AND col.column_id = ic.column_id "
                + "WHERE UPPER(SCHEMA_NAME(o.schema_id)) = ? AND UPPER(o.name) = ? ORDER BY kc.name, ic.key_ordinal";
        try (PreparedStatement ps = bind(c, pk, sch, name); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Map<String, Object> con = new LinkedHashMap<>();
                con.put("constraintName", rs.getString("cn"));
                con.put("constraintType", rs.getString("td").contains("PRIMARY") ? "P" : "U");
                con.put("columnName", rs.getString("col"));
                con.put("position", rs.getInt("pos"));
                con.put("refConstraint", null);
                out.add(con);
            }
        }
        String fk = "SELECT fk.name cn, col.name col, fkc.constraint_column_id pos, "
                + "SCHEMA_NAME(ro.schema_id)+'.'+ro.name reftbl "
                + "FROM sys.foreign_keys fk "
                + "JOIN sys.objects po ON po.object_id = fk.parent_object_id "
                + "JOIN sys.objects ro ON ro.object_id = fk.referenced_object_id "
                + "JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id "
                + "JOIN sys.columns col ON col.object_id = fkc.parent_object_id AND col.column_id = fkc.parent_column_id "
                + "WHERE UPPER(SCHEMA_NAME(po.schema_id)) = ? AND UPPER(po.name) = ? ORDER BY fk.name, fkc.constraint_column_id";
        try (PreparedStatement ps = bind(c, fk, sch, name); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Map<String, Object> con = new LinkedHashMap<>();
                con.put("constraintName", rs.getString("cn"));
                con.put("constraintType", "R");
                con.put("columnName", rs.getString("col"));
                con.put("position", rs.getInt("pos"));
                con.put("refConstraint", rs.getString("reftbl"));
                out.add(con);
            }
        }
        return out;
    }

    static List<Map<String, Object>> indexes(Connection c, String sch, String name) throws SQLException {
        String sql = "SELECT i.name idx, i.is_unique uniq, col.name col, ic.key_ordinal pos "
                + "FROM sys.indexes i "
                + "JOIN sys.objects o ON o.object_id = i.object_id "
                + "JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
                + "JOIN sys.columns col ON col.object_id = ic.object_id AND col.column_id = ic.column_id "
                + "WHERE i.name IS NOT NULL AND ic.key_ordinal > 0 "
                + "AND UPPER(SCHEMA_NAME(o.schema_id)) = ? AND UPPER(o.name) = ? ORDER BY i.name, ic.key_ordinal";
        List<Map<String, Object>> out = new ArrayList<>();
        try (PreparedStatement ps = bind(c, sql, sch, name); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Map<String, Object> idx = new LinkedHashMap<>();
                idx.put("indexName", rs.getString("idx"));
                idx.put("uniqueness", rs.getBoolean("uniq") ? "UNIQUE" : "NONUNIQUE");
                idx.put("columnName", rs.getString("col"));
                idx.put("position", rs.getInt("pos"));
                out.add(idx);
            }
        }
        return out;
    }

    static List<Map<String, Object>> triggers(Connection c, String sch, String name) throws SQLException {
        String sql = "SELECT tr.name trig, CASE WHEN tr.is_instead_of_trigger=1 THEN 'INSTEAD OF' ELSE 'AFTER' END timing, "
                + "tr.is_disabled dis FROM sys.triggers tr "
                + "JOIN sys.objects o ON o.object_id = tr.parent_id "
                + "WHERE tr.parent_class=1 AND UPPER(SCHEMA_NAME(o.schema_id)) = ? AND UPPER(o.name) = ?";
        List<Map<String, Object>> out = new ArrayList<>();
        try (PreparedStatement ps = bind(c, sql, sch, name); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Map<String, Object> tr = new LinkedHashMap<>();
                tr.put("triggerName", rs.getString("trig"));
                tr.put("owner", sch);
                tr.put("triggerType", rs.getString("timing"));
                tr.put("status", rs.getBoolean("dis") ? "DISABLED" : "ENABLED");
                out.add(tr);
            }
        }
        return out;
    }

    static String viewDefinition(Connection c, String sch, String name) throws SQLException {
        String sql = "SELECT mo.definition def FROM sys.objects o "
                + "JOIN sys.sql_modules mo ON mo.object_id = o.object_id "
                + "WHERE o.type='V' AND UPPER(SCHEMA_NAME(o.schema_id)) = ? AND UPPER(o.name) = ?";
        try (PreparedStatement ps = bind(c, sql, sch, name); ResultSet rs = ps.executeQuery()) {
            if (rs.next()) return rs.getString("def");
        }
        return null;
    }

    private static PreparedStatement bind(Connection c, String sql, String sch, String name) throws SQLException {
        PreparedStatement ps = c.prepareStatement(sql);
        ps.setString(1, sch.toUpperCase());
        ps.setString(2, name.toUpperCase());
        return ps;
    }
}
