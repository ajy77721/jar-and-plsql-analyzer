package com.plsqlanalyzer.web.service;

import com.plsqlanalyzer.config.DbUserConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

/**
 * Fetches PL/SQL source code directly from Oracle DB via ALL_SOURCE / DBA_SOURCE.
 * Connects per-user to each configured schema.
 */
public class DbSourceFetcher {

    private static final Logger log = LoggerFactory.getLogger(DbSourceFetcher.class);

    private final String jdbcUrl;

    static {
        try {
            Class.forName("oracle.jdbc.OracleDriver");
        } catch (ClassNotFoundException e) {
            LoggerFactory.getLogger(DbSourceFetcher.class)
                    .warn("Oracle JDBC driver not found on classpath: {}", e.getMessage());
        }
    }

    public DbSourceFetcher(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    /**
     * List all PL/SQL objects (PROCEDURE, FUNCTION, PACKAGE, PACKAGE BODY, TRIGGER)
     * visible to the given user.
     */
    public List<Map<String, String>> listObjects(DbUserConfig user) {
        try (Connection conn = getConnection(user)) {
            return listObjects(conn, user.getUsername());
        } catch (SQLException e) {
            log.error("Failed to list objects for user {}: {}", user.getUsername(), e.getMessage());
            return new ArrayList<>();
        }
    }

    /**
     * Connection-based variant of {@link #listObjects(DbUserConfig)} — same SQL,
     * caller supplies the connection and schema. Enables delegation from the
     * browse-provider abstraction without changing Oracle behaviour.
     */
    public List<Map<String, String>> listObjects(Connection conn, String schema) throws SQLException {
        List<Map<String, String>> objects = new ArrayList<>();
        String sql = """
            SELECT DISTINCT owner, object_name, object_type
            FROM all_objects
            WHERE object_type IN ('PROCEDURE','FUNCTION','PACKAGE','PACKAGE BODY','TRIGGER')
              AND owner = UPPER(?)
            ORDER BY object_type, object_name
            """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schema);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, String> obj = new LinkedHashMap<>();
                    obj.put("owner", rs.getString("owner"));
                    obj.put("objectName", rs.getString("object_name"));
                    obj.put("objectType", rs.getString("object_type"));
                    objects.add(obj);
                }
            }
        }
        return objects;
    }

    /**
     * Fetch source code for a specific object from ALL_SOURCE.
     * Lines are ordered and concatenated.
     */
    public String fetchSource(DbUserConfig user, String owner, String objectName, String objectType) {
        try (Connection conn = getConnection(user)) {
            return fetchSource(conn, owner, objectName, objectType);
        } catch (SQLException e) {
            log.error("Failed to fetch source for {}.{} ({}): {}",
                    owner, objectName, objectType, e.getMessage());
            return "";
        }
    }

    /**
     * Connection-based variant of {@link #fetchSource(DbUserConfig, String, String, String)}
     * — identical SQL and CREATE-OR-REPLACE wrapping, caller supplies the connection.
     */
    public String fetchSource(Connection conn, String owner, String objectName, String objectType)
            throws SQLException {
        String sql = """
            SELECT text
            FROM all_source
            WHERE owner = UPPER(?)
              AND name = UPPER(?)
              AND type = UPPER(?)
            ORDER BY line
            """;

        StringBuilder source = new StringBuilder();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, owner);
            ps.setString(2, objectName);
            ps.setString(3, objectType);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    source.append(rs.getString("text"));
                }
            }
        }

        // Wrap with CREATE OR REPLACE if needed
        String src = source.toString();
        if (!src.isBlank() && !src.toUpperCase().startsWith("CREATE")) {
            src = "CREATE OR REPLACE " + objectType + " " + owner + "." + objectName + " " + src;
        }
        return src;
    }

    /**
     * Fetch ALL source for a given user/schema — all packages, procedures, functions.
     * Returns map of "OWNER.OBJECT_NAME.TYPE" -> source code.
     */
    public Map<String, String> fetchAllSources(DbUserConfig user) {
        Map<String, String> sources = new LinkedHashMap<>();
        List<Map<String, String>> objects = listObjects(user);
        log.info("Fetching source for {} objects from schema {}", objects.size(), user.getUsername());

        for (Map<String, String> obj : objects) {
            String owner = obj.get("owner");
            String name = obj.get("objectName");
            String type = obj.get("objectType");
            String key = owner + "." + name + "." + type.replace(" ", "_");
            String src = fetchSource(user, owner, name, type);
            if (!src.isBlank()) {
                sources.put(key, src);
            }
        }
        return sources;
    }

    /**
     * Fetch source for a single object by name — searches across all configured users.
     */
    public String fetchSourceByName(List<DbUserConfig> users, String objectName) {
        for (DbUserConfig user : users) {
            // Try PACKAGE BODY first, then PACKAGE, PROCEDURE, FUNCTION
            for (String type : List.of("PACKAGE BODY", "PACKAGE", "PROCEDURE", "FUNCTION")) {
                String src = fetchSource(user, user.getUsername(), objectName, type);
                if (!src.isBlank()) {
                    return src;
                }
            }
        }
        return "";
    }

    public boolean testConnection(DbUserConfig user) {
        try (Connection conn = getConnection(user)) {
            return conn.isValid(5);
        } catch (SQLException e) {
            log.error("Connection test failed for {}: {}", user.getUsername(), e.getMessage());
            return false;
        }
    }

    /**
     * Get a JDBC connection for the given user config using the default JDBC URL.
     */
    public Connection getConnection(DbUserConfig user) throws SQLException {
        return DriverManager.getConnection(jdbcUrl, user.getUsername(), user.getPassword());
    }

    /**
     * Get a JDBC connection using a custom JDBC URL (for environment-specific connections).
     */
    public Connection getConnection(DbUserConfig user, String customJdbcUrl) throws SQLException {
        return DriverManager.getConnection(customJdbcUrl, user.getUsername(), user.getPassword());
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }
}
