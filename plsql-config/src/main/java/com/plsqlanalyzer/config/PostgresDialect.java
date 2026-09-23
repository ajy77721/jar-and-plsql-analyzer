package com.plsqlanalyzer.config;

public class PostgresDialect implements DbDialect {

    @Override
    public String buildJdbcUrl(ConnectionConfig conn) {
        if (conn.getCustomUrl() != null && !conn.getCustomUrl().isBlank()) {
            return conn.getCustomUrl();
        }
        if (conn.getHostname() == null) return null;
        int port = conn.getPort() > 0 ? conn.getPort() : 5432;
        StringBuilder url = new StringBuilder("jdbc:postgresql://");
        url.append(conn.getHostname()).append(":").append(port).append("/");
        if (conn.getServiceName() != null && !conn.getServiceName().isBlank()) {
            url.append(conn.getServiceName());   // database name
        }
        return url.toString();
    }

    @Override public int    getDefaultPort() { return 5432; }
    @Override public String getDriverClass() { return "org.postgresql.Driver"; }
    @Override public String getDisplayName() { return "PostgreSQL"; }
}
