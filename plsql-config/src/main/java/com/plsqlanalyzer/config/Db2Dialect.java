package com.plsqlanalyzer.config;

import java.nio.file.Path;

public class Db2Dialect implements DbDialect {

    @Override
    public String buildJdbcUrl(ConnectionConfig conn) {
        return buildJdbcUrl(conn, null);
    }

    /**
     * DB2 has no TLS URL parameters: encryption is enabled via the driver property
     * {@code sslConnection=true} and trust material via {@code sslTrustStoreLocation}, both
     * produced by {@link ConnectionConfig#buildSslProperties(java.nio.file.Path)}. The URL is
     * therefore identical with and without TLS.
     */
    @Override
    public String buildJdbcUrl(ConnectionConfig conn, Path configDir) {
        if (conn.getCustomUrl() != null && !conn.getCustomUrl().isBlank()) {
            return conn.getCustomUrl();
        }
        if (conn.getHostname() == null) return null;
        int port = conn.getPort() > 0 ? conn.getPort() : 50000;
        StringBuilder url = new StringBuilder("jdbc:db2://");
        url.append(conn.getHostname()).append(":").append(port).append("/");
        if (conn.getServiceName() != null && !conn.getServiceName().isBlank()) {
            url.append(conn.getServiceName());
        }
        url.append(":progressiveStreaming=2;");
        return url.toString();
    }

    @Override public int    getDefaultPort() { return 50000; }
    @Override public String getDriverClass() { return "com.ibm.db2.jcc.DB2Driver"; }
    @Override public String getDisplayName() { return "IBM DB2"; }
}
