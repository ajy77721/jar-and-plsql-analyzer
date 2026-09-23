package com.plsqlanalyzer.config;

import java.nio.file.Path;

public class PostgresDialect implements DbDialect {

    @Override
    public String buildJdbcUrl(ConnectionConfig conn) {
        return buildJdbcUrl(conn, null);
    }

    /**
     * PostgreSQL takes all TLS settings as {@code sslmode} plus {@code sslrootcert} /
     * {@code sslcert} / {@code sslkey} query parameters, so the base URL is unchanged and
     * {@link #appendTls} appends the security parameters when TLS is enabled.
     */
    @Override
    public String buildJdbcUrl(ConnectionConfig conn, Path configDir) {
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
        if (configDir != null && conn.isTlsEnabled()) {
            appendTls(conn, url, configDir);
        }
        return url.toString();
    }

    /**
     * Appends {@code sslmode} (mapped to the PostgreSQL vocabulary), the root certificate when
     * a trust bundle is configured, and the client certificate/key pair when both are present.
     * Note that PostgreSQL's {@code require} mode never validates the hostname, so disabling
     * hostname validation requires no change to the {@code sslmode} value.
     */
    @Override
    public void appendTls(ConnectionConfig c, StringBuilder url, Path configDir) {
        DbDialect.appendQueryParam(url, "sslmode", postgresSslMode(c.getSslMode()));
        if (c.getTrustCertPath() != null && !c.getTrustCertPath().isBlank()) {
            Path pem = TlsKeystoreConverter.resolve(Path.of(c.getTrustCertPath()), configDir);
            DbDialect.appendQueryParam(url, "sslrootcert", pem.toString());
        }
        if (c.getClientCertPath() != null && !c.getClientCertPath().isBlank()
                && c.getClientKeyPath() != null && !c.getClientKeyPath().isBlank()) {
            Path cert = TlsKeystoreConverter.resolve(Path.of(c.getClientCertPath()), configDir);
            Path key = TlsKeystoreConverter.resolve(Path.of(c.getClientKeyPath()), configDir);
            DbDialect.appendQueryParam(url, "sslcert", cert.toString());
            DbDialect.appendQueryParam(url, "sslkey", key.toString());
        }
    }

    private static String postgresSslMode(String mode) {
        String m = mode == null ? "" : mode.trim().toUpperCase();
        return switch (m) {
            case "VERIFY_FULL" -> "verify-full";
            case "VERIFY_CA"   -> "verify-ca";
            default            -> "require";
        };
    }

    @Override public int    getDefaultPort() { return 5432; }
    @Override public String getDriverClass() { return "org.postgresql.Driver"; }
    @Override public String getDisplayName() { return "PostgreSQL"; }
}
