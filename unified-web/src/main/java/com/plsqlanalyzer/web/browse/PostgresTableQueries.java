package com.plsqlanalyzer.web.browse;

import java.sql.*;
import java.util.*;

/**
 * PostgreSQL {@code information_schema}/{@code pg_catalog} queries for a single table/view,
 * returning Oracle-shaped Map/List structures (columnName/dataType/..., constraintType P/U/R,
 * etc.), mirroring {@link MySqlTableQueries}/{@link Db2TableQueries} for
 * {@link PostgresBrowseProvider#tableInfo}.
 *
 * <p>Postgres folds unquoted identifiers to lowercase in its catalogs, so every comparison here
 * goes through {@code upper(...)} on the catalog column rather than relying on stored case.
 */
final class PostgresTableQueries {

    private PostgresTableQueries() {
    }

    /** Resolves schema + type (TABLE/VIEW/MATERIALIZED VIEW) for a table/view/matview. */
    static String[] resolveObject(Connection c, String schema, String name) throws SQLException {
        String mvSql = "SELECT schemaname sch FROM pg_matviews WHERE upper(matviewname) = ? "
                + (schema != null ? "AND upper(schemaname) = ? " : "") + "LIMIT 1";
        try (PreparedStatement ps = bind(c, mvSql, schema, name)) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return new String[]{rs.getString("sch"), "MATERIALIZED VIEW"};
            }
        }
        String sql = "SELECT table_schema sch, table_type tt FROM information_schema.tables "
                + "WHERE upper(table_name) = ? "
                + (schema != null ? "AND upper(table_schema) = ? " : "")
                + "ORDER BY (table_type = 'VIEW') LIMIT 1";
        try (PreparedStatement ps = bind(c, sql, schema, name)) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String type = "VIEW".equalsIgnoreCase(rs.getString("tt")) ? "VIEW" : "TABLE";
                    return new String[]{rs.getString("sch"), type};
                }
            }
        }
        return null;
    }

    static List<Map<String, Object>> columns(Connection c, String sch, String name) throws SQLException {
        String sql = "SELECT column_name col, data_type dtype, character_maximum_length ml, "
                + "numeric_precision pr, numeric_scale sc, is_nullable nul, ordinal_position pos, "
                + "column_default ddef FROM information_schema.columns "
                + "WHERE upper(table_schema) = ? AND upper(table_name) = ? ORDER BY ordinal_position";
        List<Map<String, Object>> out = new ArrayList<>();
        try (PreparedStatement ps = bindExact(c, sql, sch, name); ResultSet rs = ps.executeQuery()) {
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
                out.add(col);
            }
        }
        return out;
    }

    static List<Map<String, Object>> constraints(Connection c, String sch, String name) throws SQLException {
        List<Map<String, Object>> out = new ArrayList<>();
        String pkUnique = "SELECT tc.constraint_name cn, tc.constraint_type ct, "
                + "kcu.column_name col, kcu.ordinal_position pos "
                + "FROM information_schema.table_constraints tc "
                + "JOIN information_schema.key_column_usage kcu "
                + "  ON kcu.constraint_name = tc.constraint_name "
                + "  AND kcu.table_schema = tc.table_schema AND kcu.table_name = tc.table_name "
                + "WHERE tc.constraint_type IN ('PRIMARY KEY','UNIQUE') "
                + "AND upper(tc.table_schema) = ? AND upper(tc.table_name) = ? "
                + "ORDER BY tc.constraint_name, kcu.ordinal_position";
        try (PreparedStatement ps = bindExact(c, pkUnique, sch, name); ResultSet rs = ps.executeQuery()) {
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
        String fk = "SELECT tc.constraint_name cn, kcu.column_name col, kcu.ordinal_position pos, "
                + "ccu.table_schema reftbsch, ccu.table_name reftbl "
                + "FROM information_schema.table_constraints tc "
                + "JOIN information_schema.key_column_usage kcu ON kcu.constraint_name = tc.constraint_name "
                + "  AND kcu.table_schema = tc.table_schema AND kcu.table_name = tc.table_name "
                + "JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name "
                + "WHERE tc.constraint_type = 'FOREIGN KEY' "
                + "AND upper(tc.table_schema) = ? AND upper(tc.table_name) = ? "
                + "ORDER BY tc.constraint_name, kcu.ordinal_position";
        try (PreparedStatement ps = bindExact(c, fk, sch, name); ResultSet rs = ps.executeQuery()) {
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
        String sql = "SELECT indexname idx, indexdef def FROM pg_indexes "
                + "WHERE upper(schemaname) = ? AND upper(tablename) = ? ORDER BY indexname";
        List<Map<String, Object>> out = new ArrayList<>();
        try (PreparedStatement ps = bindExact(c, sql, sch, name); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Map<String, Object> idx = new LinkedHashMap<>();
                String def = rs.getString("def");
                idx.put("indexName", rs.getString("idx"));
                idx.put("uniqueness", def != null && def.toUpperCase().contains("UNIQUE") ? "UNIQUE" : "NONUNIQUE");
                idx.put("columnName", null);
                idx.put("position", 0);
                out.add(idx);
            }
        }
        return out;
    }

    static List<Map<String, Object>> triggers(Connection c, String sch, String name) throws SQLException {
        String sql = "SELECT trigger_name trig, action_timing timing, event_manipulation ev "
                + "FROM information_schema.triggers "
                + "WHERE upper(event_object_schema) = ? AND upper(event_object_table) = ?";
        List<Map<String, Object>> out = new ArrayList<>();
        try (PreparedStatement ps = bindExact(c, sql, sch, name); ResultSet rs = ps.executeQuery()) {
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
        String mvSql = "SELECT definition def FROM pg_matviews "
                + "WHERE upper(schemaname) = ? AND upper(matviewname) = ?";
        try (PreparedStatement ps = bindExact(c, mvSql, sch, name); ResultSet rs = ps.executeQuery()) {
            if (rs.next()) return rs.getString("def");
        }
        String sql = "SELECT view_definition def FROM information_schema.views "
                + "WHERE upper(table_schema) = ? AND upper(table_name) = ?";
        try (PreparedStatement ps = bindExact(c, sql, sch, name); ResultSet rs = ps.executeQuery()) {
            if (rs.next()) return rs.getString("def");
        }
        return null;
    }

    private static PreparedStatement bind(Connection c, String sql, String schema, String name) throws SQLException {
        PreparedStatement ps = c.prepareStatement(sql);
        ps.setString(1, name.toUpperCase());
        if (schema != null) ps.setString(2, schema.toUpperCase());
        return ps;
    }

    private static PreparedStatement bindExact(Connection c, String sql, String sch, String name) throws SQLException {
        PreparedStatement ps = c.prepareStatement(sql);
        ps.setString(1, sch.toUpperCase());
        ps.setString(2, name.toUpperCase());
        return ps;
    }
}
