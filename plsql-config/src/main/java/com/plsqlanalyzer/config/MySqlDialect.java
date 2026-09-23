package com.plsqlanalyzer.config;

import java.nio.file.Path;

public class MySqlDialect implements DbDialect {

    @Override
    public String buildJdbcUrl(ConnectionConfig conn) {
        return buildJdbcUrl(conn, null);
    }

    /**
     * MySQL expresses TLS entirely through URL parameters. Without a TLS context
     * ({@code configDir == null}) the historical plaintext URL is produced verbatim —
     * {@code useSSL=false} and all. When TLS is enabled the base query keeps only the
     * connection-tuning parameters and {@link #appendTls} contributes the TLS ones, which
     * is how the previously hardcoded {@code useSSL=false} gets replaced by
     * {@code useSSL=true}.
     */
    @Override
    public String buildJdbcUrl(ConnectionConfig conn, Path configDir) {
        if (conn.getCustomUrl() != null && !conn.getCustomUrl().isBlank()) {
            return conn.getCustomUrl();
        }
        if (conn.getHostname() == null) return null;
        int port = conn.getPort() > 0 ? conn.getPort() : 3306;
        StringBuilder url = new StringBuilder("jdbc:mysql://");
        url.append(conn.getHostname()).append(":").append(port);
        if (conn.getServiceName() != null && !conn.getServiceName().isBlank()) {
            url.append("/").append(conn.getServiceName());
        }
        if (configDir != null && conn.isTlsEnabled()) {
            url.append("?serverTimezone=UTC&connectTimeout=10000");
            appendTls(conn, url, configDir);
        } else {
            url.append("?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&connectTimeout=10000");
        }
        return url.toString();
    }

    /**
     * Adds the MySQL TLS parameters. The sslMode vocabulary is mapped to the connector's own
     * values (VERIFY_FULL becomes VERIFY_IDENTITY, MySQL's name for full certificate +
     * hostname verification). When a PEM trust bundle is supplied it is referenced through
     * {@code trustCertificateKeyStoreUrl}, with {@code fallbackToSystemTrustStore=false} so
     * exactly the configured CA is trusted rather than whatever the JVM happens to know.
     */
    @Override
    public void appendTls(ConnectionConfig c, StringBuilder url, Path configDir) {
        DbDialect.appendQueryParam(url, "sslMode", mysqlSslMode(c.getSslMode()));
        DbDialect.appendQueryParam(url, "useSSL", "true");
        DbDialect.appendQueryParam(url, "allowPublicKeyRetrieval", "true");
        if (c.getTrustCertPath() != null && !c.getTrustCertPath().isBlank()) {
            Path pem = TlsKeystoreConverter.resolve(Path.of(c.getTrustCertPath()), configDir);
            DbDialect.appendQueryParam(url, "trustCertificateKeyStoreUrl", "file:" + pem);
            DbDialect.appendQueryParam(url, "fallbackToSystemTrustStore", "false");
        }
    }

    private static String mysqlSslMode(String mode) {
        String m = mode == null ? "" : mode.trim().toUpperCase();
        return switch (m) {
            case "VERIFY_FULL" -> "VERIFY_IDENTITY";
            case "VERIFY_CA"   -> "VERIFY_CA";
            default            -> "REQUIRED";
        };
    }

    @Override public int    getDefaultPort()  { return 3306; }
    @Override public String getDriverClass()  { return "com.mysql.cj.jdbc.Driver"; }
    @Override public String getDisplayName()  { return "MySQL"; }
}
