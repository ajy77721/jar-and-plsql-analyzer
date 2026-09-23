package com.plsql.parser.multidb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Fetches SQL Server stored procedure / function source via sys.sql_modules.
 */
public class SqlServerSourceFetchStrategy implements SourceFetchStrategy {

    /**
     * SQL Server source comes from sys.sql_modules.definition (a complete NVARCHAR(MAX) /
     * OBJECT_DEFINITION), so it is never truncated. Returning false lets genuine parse errors
     * surface instead of being masked.
     */
    @Override
    public boolean isTruncated(String source) {
        return false;
    }

    private static final Logger log = LoggerFactory.getLogger(SqlServerSourceFetchStrategy.class);

    @Override
    public String fetchSource(Connection conn, String objectName, String objectType,
                              String schema) throws SQLException {
        // Build qualified name for OBJECT_ID lookup
        String qualifiedName = schema != null
                ? "[" + schema + "].[" + objectName + "]"
                : objectName;

        // OBJECT_DEFINITION is the simplest path; falls back to sys.sql_modules for large objects
        String sql =
                "SELECT m.definition " +
                "FROM sys.sql_modules m " +
                "JOIN sys.objects o ON m.object_id = o.object_id " +
                "WHERE o.name = ? " +
                (schema != null ? "AND SCHEMA_NAME(o.schema_id) = ? " : "") +
                "AND o.type IN ('P', 'FN', 'IF', 'TF', 'V', 'TR')";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, objectName);
            if (schema != null) ps.setString(2, schema);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String def = rs.getString("definition");
                    return (def != null && !def.isBlank()) ? def : null;
                }
                log.debug("SQL Server: object not found in sys.sql_modules — {}", qualifiedName);
                return null;
            }
        } catch (SQLException e) {
            log.warn("SQL Server sys.sql_modules fetch failed for {}: {}", qualifiedName, e.getMessage());
            throw e;
        }
    }

    /** Resolves the schema (owner) of a routine by name, searching all schemas. */
    public String findSchema(Connection conn, String objectName) throws SQLException {
        String sql = "SELECT TOP 1 SCHEMA_NAME(o.schema_id) AS schema_name "
                + "FROM sys.objects o WHERE o.name = ? "
                + "AND o.type IN ('P','FN','IF','TF','V','TR')";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, objectName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getString("schema_name");
            }
        }
        return null;
    }

    @Override
    public List<ObjectRef> listObjects(Connection conn, String schema) throws SQLException {
        String sql = schema != null
                ? "SELECT o.name, o.type_desc, SCHEMA_NAME(o.schema_id) AS schema_name "
                + "FROM sys.objects o "
                + "WHERE o.type IN ('P','FN','IF','TF') AND SCHEMA_NAME(o.schema_id) = ? "
                + "ORDER BY o.type_desc, o.name"
                : "SELECT o.name, o.type_desc, SCHEMA_NAME(o.schema_id) AS schema_name "
                + "FROM sys.objects o "
                + "WHERE o.type IN ('P','FN','IF','TF') "
                + "ORDER BY schema_name, o.type_desc, o.name";

        List<ObjectRef> result = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            if (schema != null) ps.setString(1, schema);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String typeDesc = rs.getString("type_desc");
                    String normalizedType = typeDesc != null && typeDesc.contains("FUNCTION")
                            ? "FUNCTION" : "PROCEDURE";
                    result.add(new ObjectRef(
                            rs.getString("name"),
                            normalizedType,
                            rs.getString("schema_name")));
                }
            }
        }
        return result;
    }
}
