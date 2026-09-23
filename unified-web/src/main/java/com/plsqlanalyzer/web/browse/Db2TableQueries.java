package com.plsqlanalyzer.web.browse;

import java.sql.*;
import java.util.*;

/**
 * IBM DB2 {@code SYSCAT} queries for a single table/view, returning Oracle-shaped Map/List
 * structures (columnName/dataType/..., constraintType P/U/R, etc.), mirroring
 * {@link SqlServerTableQueries} for the same {@link Db2BrowseProvider#tableInfo}. SQL patterns
 * mirror the crawler's {@code Db2SourceFetchStrategy}/{@code Db2TableEnricher} and
 * {@code TriggerDiscoverer.discoverDb2}.
 *
 * <p><b>Not verified against a live DB2 instance</b> — {@code SYSCAT.KEYCOLUSE}/{@code
 * SYSCAT.REFERENCES}/{@code SYSCAT.INDEXCOLUSE} column names are taken from the DB2 LUW system
 * catalog reference; flagged as a follow-up to confirm against a real DB2 LUW 9.7+ instance.
 */
final class Db2TableQueries {

    private Db2TableQueries() {
    }

    /** Resolves schema + type (TABLE/VIEW) for a table/view; prefers the requested schema. */
    static String[] resolveObject(Connection c, String schema, String name) throws SQLException {
        String sql = "SELECT TABSCHEMA sch, TYPE tt FROM SYSCAT.TABLES "
                + "WHERE UPPER(TABNAME) = ? AND TYPE IN ('T','V') "
                + (schema != null ? "AND UPPER(TABSCHEMA) = ? " : "")
                + "ORDER BY TYPE FETCH FIRST 1 ROWS ONLY";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, name.toUpperCase());
            if (schema != null) ps.setString(2, schema.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String tt = rs.getString("tt");
                    String type = "V".equals(tt) ? "VIEW" : "TABLE";
                    return new String[]{rs.getString("sch").trim(), type};
                }
            }
        }
        return null;
    }

    static List<Map<String, Object>> columns(Connection c, String sch, String name) throws SQLException {
        String sql = "SELECT COLNAME col, TYPENAME dtype, LENGTH ml, SCALE sc, NULLS nul, "
                + "COLNO cid, \"DEFAULT\" ddef, GENERATED gen "
                + "FROM SYSCAT.COLUMNS WHERE UPPER(TABSCHEMA) = ? AND UPPER(TABNAME) = ? ORDER BY COLNO";
        List<Map<String, Object>> out = new ArrayList<>();
        try (PreparedStatement ps = bind(c, sql, sch, name); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Map<String, Object> col = new LinkedHashMap<>();
                col.put("columnName", rs.getString("col"));
                col.put("dataType", rs.getString("dtype"));
                col.put("dataLength", rs.getInt("ml"));
                int sc = rs.getInt("sc");
                col.put("dataScale", sc > 0 ? sc : null);
                col.put("nullable", "Y".equals(rs.getString("nul")));
                col.put("columnId", rs.getInt("cid"));
                col.put("dataDefault", rs.getString("ddef"));
                String gen = rs.getString("gen");
                if (gen != null && !gen.isBlank() && !gen.trim().isEmpty()) col.put("computed", true);
                out.add(col);
            }
        }
        return out;
    }

    static List<Map<String, Object>> constraints(Connection c, String sch, String name) throws SQLException {
        List<Map<String, Object>> out = new ArrayList<>();
        String pkUnique = "SELECT tc.CONSTNAME cn, tc.TYPE ct, kcu.COLNAME col, kcu.COLSEQ pos "
                + "FROM SYSCAT.TABCONST tc "
                + "JOIN SYSCAT.KEYCOLUSE kcu ON kcu.CONSTNAME = tc.CONSTNAME AND kcu.TABSCHEMA = tc.TABSCHEMA "
                + "WHERE tc.TYPE IN ('P','U') AND UPPER(tc.TABSCHEMA) = ? AND UPPER(tc.TABNAME) = ? "
                + "ORDER BY tc.CONSTNAME, kcu.COLSEQ";
        try (PreparedStatement ps = bind(c, pkUnique, sch, name); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Map<String, Object> con = new LinkedHashMap<>();
                con.put("constraintName", rs.getString("cn"));
                con.put("constraintType", "P".equals(rs.getString("ct")) ? "P" : "U");
                con.put("columnName", rs.getString("col"));
                con.put("position", rs.getInt("pos"));
                con.put("refConstraint", null);
                out.add(con);
            }
        }
        String fk = "SELECT r.CONSTNAME cn, kcu.COLNAME col, kcu.COLSEQ pos, "
                + "r.REFTABSCHEMA reftbsch, r.REFTABNAME reftbl "
                + "FROM SYSCAT.REFERENCES r "
                + "JOIN SYSCAT.KEYCOLUSE kcu ON kcu.CONSTNAME = r.CONSTNAME AND kcu.TABSCHEMA = r.TABSCHEMA "
                + "WHERE UPPER(r.TABSCHEMA) = ? AND UPPER(r.TABNAME) = ? ORDER BY r.CONSTNAME, kcu.COLSEQ";
        try (PreparedStatement ps = bind(c, fk, sch, name); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Map<String, Object> con = new LinkedHashMap<>();
                con.put("constraintName", rs.getString("cn"));
                con.put("constraintType", "R");
                con.put("columnName", rs.getString("col"));
                con.put("position", rs.getInt("pos"));
                con.put("refConstraint", rs.getString("reftbsch").trim() + "." + rs.getString("reftbl").trim());
                out.add(con);
            }
        }
        return out;
    }

    static List<Map<String, Object>> indexes(Connection c, String sch, String name) throws SQLException {
        String sql = "SELECT i.INDNAME idx, i.UNIQUERULE uq, icu.COLNAME col, icu.COLSEQ pos "
                + "FROM SYSCAT.INDEXES i "
                + "JOIN SYSCAT.INDEXCOLUSE icu ON icu.INDNAME = i.INDNAME AND icu.INDSCHEMA = i.INDSCHEMA "
                + "WHERE UPPER(i.TABSCHEMA) = ? AND UPPER(i.TABNAME) = ? ORDER BY i.INDNAME, icu.COLSEQ";
        List<Map<String, Object>> out = new ArrayList<>();
        try (PreparedStatement ps = bind(c, sql, sch, name); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Map<String, Object> idx = new LinkedHashMap<>();
                idx.put("indexName", rs.getString("idx"));
                idx.put("uniqueness", "D".equals(rs.getString("uq")) ? "NONUNIQUE" : "UNIQUE");
                idx.put("columnName", rs.getString("col"));
                idx.put("position", Math.abs(rs.getInt("pos")));
                out.add(idx);
            }
        }
        return out;
    }

    static List<Map<String, Object>> triggers(Connection c, String sch, String name) throws SQLException {
        String sql = "SELECT TRIGNAME trig, TRIGSCHEMA tsch, TRIGTIME tt, TRIGEVENT te "
                + "FROM SYSCAT.TRIGGERS WHERE UPPER(TABSCHEMA) = ? AND UPPER(TABNAME) = ?";
        List<Map<String, Object>> out = new ArrayList<>();
        try (PreparedStatement ps = bind(c, sql, sch, name); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Map<String, Object> tr = new LinkedHashMap<>();
                tr.put("triggerName", rs.getString("trig"));
                tr.put("owner", rs.getString("tsch") != null ? rs.getString("tsch").trim() : sch);
                String tt = rs.getString("tt");
                String timing = "B".equals(tt) ? "BEFORE" : "A".equals(tt) ? "AFTER" : "I".equals(tt) ? "INSTEAD OF" : tt;
                String te = rs.getString("te");
                String event = "I".equals(te) ? "INSERT" : "U".equals(te) ? "UPDATE" : "D".equals(te) ? "DELETE" : te;
                tr.put("triggerType", timing + " " + event);
                tr.put("status", "ENABLED");
                out.add(tr);
            }
        }
        return out;
    }

    static String viewDefinition(Connection c, String sch, String name) throws SQLException {
        String sql = "SELECT TEXT def FROM SYSCAT.VIEWS WHERE UPPER(VIEWSCHEMA) = ? AND UPPER(VIEWNAME) = ?";
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
