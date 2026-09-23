package com.plsqlanalyzer.web.browse;

import java.sql.*;
import java.util.*;

/**
 * MySQL {@code information_schema} queries for a single table/view, returning Oracle-shaped
 * Map/List structures (columnName/dataType/..., constraintType P/U/R, etc.), mirroring
 * {@link SqlServerTableQueries} for the same {@link MySqlBrowseProvider#tableInfo}.
 *
 * <p>MySQL identifiers are matched case-insensitively here (via {@code UPPER(...)} on both sides)
 * to be lenient regardless of the server's {@code lower_case_table_names} setting.
 */
final class MySqlTableQueries {

    private MySqlTableQueries() {
    }

    /** Resolves schema + type (TABLE/VIEW) for a table/view; prefers the requested schema. */
    static String[] resolveObject(Connection c, String schema, String name) throws SQLException {
        String sql = "SELECT table_schema sch, table_type tt FROM information_schema.tables "
                + "WHERE UPPER(table_name) = ? "
                + (schema != null ? "AND UPPER(table_schema) = ? " : "")
                + "ORDER BY (table_type = 'VIEW') LIMIT 1";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, name.toUpperCase());
            if (schema != null) ps.setString(2, schema.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String tt = rs.getString("tt");
                    String type = "VIEW".equalsIgnoreCase(tt) ? "VIEW" : "TABLE";
                    return new String[]{rs.getString("sch"), type};
                }
            }
        }
        return null;
    }

    static List<Map<String, Object>> columns(Connection c, String sch, String name) throws SQLException {
        String sql = "SELECT column_name col, data_type dtype, character_maximum_length ml, "
                + "numeric_precision pr, numeric_scale sc, is_nullable nul, ordinal_position pos, "
                + "column_default ddef, generation_expression genexpr "
                + "FROM information_schema.columns "
                + "WHERE UPPER(table_schema) = ? AND UPPER(table_name) = ? ORDER BY ordinal_position";
        List<Map<String, Object>> out = new ArrayList<>();
        try (PreparedStatement ps = bind(c, sql, sch, name); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Map<String, Object> col = new LinkedHashMap<>();
                col.put("columnName", rs.getString("col"));
                col.put("dataType", rs.getString("dtype"));
                Object ml = rs.getObject("ml");
                col.put("dataLength", ml != null ? rs.getInt("ml") : null);
                Object pr = rs.getObject("pr");
                col.put("dataPrecision", pr != null ? rs.getInt("pr") : null);
                Object sc = rs.getObject("sc");
                col.put("dataScale", sc != null ? rs.getInt("sc") : null);
                col.put("nullable", "YES".equalsIgnoreCase(rs.getString("nul")));
                col.put("columnId", rs.getInt("pos"));
                col.put("dataDefault", rs.getString("ddef"));
                String genexpr = rs.getString("genexpr");
                if (genexpr != null && !genexpr.isBlank()) col.put("computed", true);
                out.add(col);
            }
        }
        return out;
    }

    static List<Map<String, Object>> constraints(Connection c, String sch, String name) throws SQLException {
        List<Map<String, Object>> out = new ArrayList<>();
        // PRIMARY KEY / UNIQUE: table_constraints joined to key_column_usage.
        String pkUnique = "SELECT tc.constraint_name cn, tc.constraint_type ct, "
                + "kcu.column_name col, kcu.ordinal_position pos "
                + "FROM information_schema.table_constraints tc "
                + "JOIN information_schema.key_column_usage kcu "
                + "  ON kcu.constraint_name = tc.constraint_name "
                + "  AND kcu.table_schema = tc.table_schema AND kcu.table_name = tc.table_name "
                + "WHERE tc.constraint_type IN ('PRIMARY KEY','UNIQUE') "
                + "AND UPPER(tc.table_schema) = ? AND UPPER(tc.table_name) = ? "
                + "ORDER BY tc.constraint_name, kcu.ordinal_position";
        try (PreparedStatement ps = bind(c, pkUnique, sch, name); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Map<String, Object> con = new LinkedHashMap<>();
                con.put("constraintName", rs.getString("cn"));
                con.put("constraintType", "PRIMARY KEY".equalsIgnoreCase(rs.getString("ct")) ? "P" : "U");
                con.put("columnName", rs.getString("col"));
                con.put("position", rs.getInt("pos"));
                con.put("refConstraint", null);
                out.add(con);
            }
        }
        // FOREIGN KEY: key_column_usage carries the referenced table/column directly for FKs.
        String fk = "SELECT kcu.constraint_name cn, kcu.column_name col, kcu.ordinal_position pos, "
                + "kcu.referenced_table_schema reftbsch, kcu.referenced_table_name reftbl "
                + "FROM information_schema.key_column_usage kcu "
                + "WHERE kcu.referenced_table_name IS NOT NULL "
                + "AND UPPER(kcu.table_schema) = ? AND UPPER(kcu.table_name) = ? "
                + "ORDER BY kcu.constraint_name, kcu.ordinal_position";
        try (PreparedStatement ps = bind(c, fk, sch, name); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Map<String, Object> con = new LinkedHashMap<>();
                con.put("constraintName", rs.getString("cn"));
                con.put("constraintType", "R");
                con.put("columnName", rs.getString("col"));
                con.put("position", rs.getInt("pos"));
                con.put("refConstraint", rs.getString("reftbsch") + "." + rs.getString("reftbl"));
                out.add(con);
            }
        }
        return out;
    }

    static List<Map<String, Object>> indexes(Connection c, String sch, String name) throws SQLException {
        String sql = "SELECT index_name idx, non_unique nu, column_name col, seq_in_index pos "
                + "FROM information_schema.statistics "
                + "WHERE UPPER(table_schema) = ? AND UPPER(table_name) = ? AND index_name <> 'PRIMARY' "
                + "ORDER BY index_name, seq_in_index";
        List<Map<String, Object>> out = new ArrayList<>();
        try (PreparedStatement ps = bind(c, sql, sch, name); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Map<String, Object> idx = new LinkedHashMap<>();
                idx.put("indexName", rs.getString("idx"));
                idx.put("uniqueness", rs.getInt("nu") == 0 ? "UNIQUE" : "NONUNIQUE");
                idx.put("columnName", rs.getString("col"));
                idx.put("position", rs.getInt("pos"));
                out.add(idx);
            }
        }
        return out;
    }

    static List<Map<String, Object>> triggers(Connection c, String sch, String name) throws SQLException {
        String sql = "SELECT trigger_name trig, action_timing timing, event_manipulation ev "
                + "FROM information_schema.triggers "
                + "WHERE UPPER(event_object_schema) = ? AND UPPER(event_object_table) = ?";
        List<Map<String, Object>> out = new ArrayList<>();
        try (PreparedStatement ps = bind(c, sql, sch, name); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Map<String, Object> tr = new LinkedHashMap<>();
                tr.put("triggerName", rs.getString("trig"));
                tr.put("owner", sch);
                tr.put("triggerType", rs.getString("timing") + " " + rs.getString("ev"));
                tr.put("status", "ENABLED");
                out.add(tr);
            }
        }
        return out;
    }

    static String viewDefinition(Connection c, String sch, String name) throws SQLException {
        String sql = "SELECT view_definition def FROM information_schema.views "
                + "WHERE UPPER(table_schema) = ? AND UPPER(table_name) = ?";
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
