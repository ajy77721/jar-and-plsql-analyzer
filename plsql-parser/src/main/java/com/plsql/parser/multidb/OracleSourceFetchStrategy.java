package com.plsql.parser.multidb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Fetches Oracle stored procedure source via ALL_SOURCE.
 * This is the canonical Oracle path — mirrors the logic in SourceDownloader.
 */
public class OracleSourceFetchStrategy implements SourceFetchStrategy {

    /**
     * Oracle source is assembled from ALL_SOURCE line chunks, which CAN return an incomplete
     * package body. A complete body ends (after any trailing blank/comment lines) with the
     * SQL*Plus terminator "/", "end;", or "end <name>;". Anything else means it was cut off.
     */
    @Override
    public boolean isTruncated(String source) {
        if (source == null || source.isBlank()) return false;
        String[] lines = source.split("\\r?\\n");
        for (int i = lines.length - 1; i >= 0; i--) {
            String line = lines[i].trim();
            if (line.isEmpty() || line.startsWith("--")) continue;
            return !line.equals("/") && !line.equalsIgnoreCase("end;")
                    && !line.matches("(?i)end\\s+[\\w\".]+\\s*;");
        }
        return false;
    }

    private static final Logger log = LoggerFactory.getLogger(OracleSourceFetchStrategy.class);

    @Override
    public String fetchSource(Connection conn, String objectName, String objectType,
                              String schema) throws SQLException {
        String name = objectName.toUpperCase();
        String type = objectType.toUpperCase();

        String sql = schema != null
                ? "SELECT LINE, TEXT FROM ALL_SOURCE WHERE NAME = ? AND TYPE = ? AND OWNER = ? ORDER BY LINE"
                : "SELECT LINE, TEXT FROM ALL_SOURCE WHERE NAME = ? AND TYPE = ?                ORDER BY LINE";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setFetchSize(5000);
            ps.setString(1, name);
            ps.setString(2, type);
            if (schema != null) ps.setString(3, schema.toUpperCase());

            try (ResultSet rs = ps.executeQuery()) {
                StringBuilder sb = new StringBuilder();
                sb.append("CREATE OR REPLACE ").append(type).append(" ").append(name).append("\n");
                while (rs.next()) {
                    sb.append(rs.getString("TEXT"));
                }
                String src = sb.toString().trim();
                return src.length() > ("CREATE OR REPLACE " + type + " " + name).length() + 2
                        ? src : null;
            }
        } catch (SQLException e) {
            log.warn("Oracle ALL_SOURCE fetch failed for {}/{}: {}", type, name, e.getMessage());
            throw e;
        }
    }

    @Override
    public List<ObjectRef> listObjects(Connection conn, String schema) throws SQLException {
        String sql = schema != null
                ? "SELECT OBJECT_NAME, OBJECT_TYPE FROM ALL_OBJECTS WHERE OWNER = ? AND OBJECT_TYPE IN ('PROCEDURE','FUNCTION','PACKAGE','PACKAGE BODY') ORDER BY OBJECT_TYPE, OBJECT_NAME"
                : "SELECT OBJECT_NAME, OBJECT_TYPE, OWNER FROM USER_OBJECTS WHERE OBJECT_TYPE IN ('PROCEDURE','FUNCTION','PACKAGE','PACKAGE BODY') ORDER BY OBJECT_TYPE, OBJECT_NAME";

        List<ObjectRef> result = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            if (schema != null) ps.setString(1, schema.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String owner = schema != null ? schema : safeGetString(rs, "OWNER");
                    result.add(new ObjectRef(rs.getString("OBJECT_NAME"), rs.getString("OBJECT_TYPE"), owner));
                }
            }
        }
        return result;
    }

    private String safeGetString(ResultSet rs, String col) {
        try { return rs.getString(col); } catch (SQLException e) { return null; }
    }
}
