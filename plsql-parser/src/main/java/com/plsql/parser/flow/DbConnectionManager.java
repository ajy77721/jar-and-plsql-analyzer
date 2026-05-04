package com.plsql.parser.flow;

import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages Oracle JDBC connections to all configured schemas.
 * Loads configuration from the plsql-config.yaml file.
 * Connections are created on-demand and must be closed by the caller (use try-with-resources).
 * No persistent pool is kept — each getConnection() call returns a fresh connection.
 */
public class DbConnectionManager implements AutoCloseable {

    private final String jdbcUrl;
    private final List<DbUser> dbUsers;
    private final AtomicInteger dbCallCount = new AtomicInteger(0);

    public static class DbUser {
        private final String username;
        private final String password;
        private final String description;

        public DbUser(String username, String password, String description) {
            this.username = username;
            this.password = password;
            this.description = description;
        }

        public String getUsername() { return username; }
        public String getPassword() { return password; }
        public String getDescription() { return description; }
    }

    /**
     * Create a DbConnectionManager from a YAML config file path.
     */
    public DbConnectionManager(String configPath) throws IOException {
        Yaml yaml = new Yaml();
        Map<String, Object> config;
        try (InputStream in = Files.newInputStream(Paths.get(configPath))) {
            config = yaml.load(in);
        }

        // Read JDBC URL - try top-level first, then nested
        String url = (String) config.get("jdbc_url");
        if (url == null) {
            String host = (String) config.get("db_hostname");
            int port = config.get("db_port") instanceof Integer
                    ? (Integer) config.get("db_port")
                    : Integer.parseInt(config.get("db_port").toString());
            String service = (String) config.get("db_service_name");
            url = "jdbc:oracle:thin:@//" + host + ":" + port + "/" + service;
        }
        this.jdbcUrl = url;

        // Read db_users list
        this.dbUsers = new ArrayList<>();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> usersList = (List<Map<String, Object>>) config.get("db_users");
        if (usersList != null) {
            for (Map<String, Object> u : usersList) {
                dbUsers.add(new DbUser(
                        (String) u.get("username"),
                        (String) u.get("password"),
                        (String) u.get("description")
                ));
            }
        }

        // Load Oracle JDBC driver
        try {
            Class.forName("oracle.jdbc.OracleDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Oracle JDBC driver not found on classpath", e);
        }
    }

    /**
     * Create a DbConnectionManager with explicit parameters (for testing or programmatic use).
     */
    public DbConnectionManager(String jdbcUrl, List<DbUser> dbUsers) {
        this.jdbcUrl = jdbcUrl;
        this.dbUsers = new ArrayList<>(dbUsers);
        try {
            Class.forName("oracle.jdbc.OracleDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Oracle JDBC driver not found on classpath", e);
        }
    }

    /**
     * Create a fresh connection for the given schema.
     * Caller must close the connection (use try-with-resources).
     */
    public Connection getConnection(String schema) throws SQLException {
        String upperSchema = schema.toUpperCase();
        DbUser user = null;
        for (DbUser u : dbUsers) {
            if (u.getUsername().equalsIgnoreCase(upperSchema)) {
                user = u;
                break;
            }
        }
        if (user == null) {
            throw new SQLException("No credentials configured for schema: " + upperSchema
                    + ". Available schemas: " + getAvailableSchemas());
        }
        Connection conn = DriverManager.getConnection(jdbcUrl, user.getUsername(), user.getPassword());
        conn.setAutoCommit(false);
        System.err.println("[DB] Connected to " + upperSchema + " at " + jdbcUrl);
        return conn;
    }

    /**
     * Create a fresh connection using any configured user.
     * Tries each user until one succeeds. Caller must close the connection.
     */
    public Connection getAnyConnection() throws SQLException {
        SQLException lastError = null;
        for (DbUser user : dbUsers) {
            try {
                return getConnection(user.getUsername());
            } catch (SQLException e) {
                lastError = e;
            }
        }
        throw new SQLException("Could not connect to any schema. Last error: "
                + (lastError != null ? lastError.getMessage() : "No users configured"));
    }

    /**
     * Get all available schema names from config.
     */
    public List<String> getAvailableSchemas() {
        List<String> schemas = new ArrayList<>();
        for (DbUser u : dbUsers) {
            schemas.add(u.getUsername().toUpperCase());
        }
        return schemas;
    }

    /**
     * Get the JDBC URL being used.
     */
    public String getJdbcUrl() {
        return jdbcUrl;
    }

    /** Increment the DB call counter (called before each executeQuery). */
    public void incrementDbCalls() { dbCallCount.incrementAndGet(); }

    /** Get the current DB call count. */
    public int getDbCallCount() { return dbCallCount.get(); }

    /** Reset the DB call counter to zero. */
    public void resetDbCallCount() { dbCallCount.set(0); }

    @Override
    public void close() {
        // No persistent pool — nothing to close here.
    }
}
