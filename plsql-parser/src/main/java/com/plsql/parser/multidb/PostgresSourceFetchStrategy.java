package com.plsql.parser.multidb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Fetches PostgreSQL function / procedure source via the pg_catalog.
 *
 * Source: pg_get_functiondef(oid) returns the full CREATE [OR REPLACE] FUNCTION/PROCEDURE DDL,
 * including the PL/pgSQL body between the $$ dollar-quotes.
 * prokind: 'f' = function, 'p' = procedure, 'a' = aggregate, 'w' = window (only f/p are routines).
 * PostgreSQL folds unquoted identifiers to lower-case, so matching is done case-insensitively.
 */
public class PostgresSourceFetchStrategy implements SourceFetchStrategy {

    /**
     * PostgreSQL bodies come from pg_get_functiondef wrapped in dollar-quote tags
     * ($function$ … $function$ / $$ … $$ / $tag$ … $tag$) and legitimately end in that tag
     * (not "end;" or "/"). PL/pgSQL also uses END IF/LOOP/CASE, so a BEGIN/END counter is wrong.
     * The only reliable truncation signal is an UNBALANCED dollar-quote tag (odd count).
     */
    @Override
    public boolean isTruncated(String source) {
        if (source == null) return false;
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("\\$[A-Za-z0-9_]*\\$").matcher(source);
        java.util.Map<String, Integer> counts = new java.util.HashMap<>();
        while (m.find()) counts.merge(m.group(), 1, Integer::sum);
        for (int c : counts.values()) if (c % 2 != 0) return true;   // unmatched tag → truncated
        return false;
    }

    private static final Logger log = LoggerFactory.getLogger(PostgresSourceFetchStrategy.class);

    @Override
    public String fetchSource(Connection conn, String objectName, String objectType,
                              String schema) throws SQLException {
        String sql =
                "SELECT pg_get_functiondef(p.oid) AS src " +
                "FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace " +
                "WHERE lower(p.proname) = lower(?) " +
                (schema != null ? "AND lower(n.nspname) = lower(?) " : "") +
                "AND p.prokind IN ('f','p') " +
                "ORDER BY p.oid LIMIT 1";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, objectName);
            if (schema != null) ps.setString(2, schema);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String text = rs.getString("src");
                    return (text != null && !text.isBlank()) ? text : null;
                }
                log.debug("Postgres: routine not found in pg_proc — {}.{}", schema, objectName);
                return null;
            }
        } catch (SQLException e) {
            log.warn("Postgres pg_proc fetch failed for {}.{}: {}", schema, objectName, e.getMessage());
            throw e;
        }
    }

    /**
     * Fetch ONE exact overload by its {@code pg_proc.oid}. This replaces the name-based
     * {@code LIMIT 1} for overloaded routines: each overload's DDL comes from
     * {@code pg_get_functiondef(p.oid)}, so no overload is silently dropped.
     */
    @Override
    public String fetchSourceByOid(Connection conn, String oid) throws SQLException {
        if (oid == null || oid.isBlank()) return null;
        long oidVal;
        try {
            oidVal = Long.parseLong(oid.trim());
        } catch (NumberFormatException e) {
            log.warn("Postgres: non-numeric oid '{}'", oid);
            return null;
        }
        String sql = "SELECT pg_get_functiondef(p.oid) AS src FROM pg_proc p WHERE p.oid = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, oidVal);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String text = rs.getString("src");
                    return (text != null && !text.isBlank()) ? text : null;
                }
                return null;
            }
        } catch (SQLException e) {
            log.warn("Postgres pg_proc fetch by oid {} failed: {}", oid, e.getMessage());
            throw e;
        }
    }

    @Override
    public List<ObjectRef> listObjects(Connection conn, String schema) throws SQLException {
        // oid + identity arguments come back WITH each row so the crawler can key every overload
        // by its true signature and fetch it individually (see fetchSourceByOid).
        String sql = schema != null
                ? "SELECT p.oid, p.proname, p.prokind, n.nspname, " +
                  "pg_get_function_identity_arguments(p.oid) AS idargs " +
                  "FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace " +
                  "WHERE lower(n.nspname) = lower(?) AND p.prokind IN ('f','p') " +
                  "ORDER BY n.nspname, p.proname, p.oid"
                : "SELECT p.oid, p.proname, p.prokind, n.nspname, " +
                  "pg_get_function_identity_arguments(p.oid) AS idargs " +
                  "FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace " +
                  "WHERE p.prokind IN ('f','p') AND n.nspname NOT IN ('pg_catalog','information_schema') " +
                  "ORDER BY n.nspname, p.proname, p.oid";

        List<ObjectRef> result = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            if (schema != null) ps.setString(1, schema);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String kind = rs.getString("prokind");
                    String normalizedType = "p".equalsIgnoreCase(kind) ? "PROCEDURE" : "FUNCTION";
                    long oidVal = rs.getLong("oid");
                    result.add(new ObjectRef(
                            rs.getString("proname"),
                            normalizedType,
                            rs.getString("nspname"),
                            String.valueOf(oidVal),
                            rs.getString("idargs"),
                            null));
                }
            }
        }
        return result;
    }
}
