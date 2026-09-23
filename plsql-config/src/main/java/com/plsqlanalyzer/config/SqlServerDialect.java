package com.plsqlanalyzer.config;

import java.nio.file.Path;

public class SqlServerDialect implements DbDialect {

    @Override
    public String buildJdbcUrl(ConnectionConfig conn) {
        return buildJdbcUrl(conn, null);
    }

    /**
     * SQL Server's TLS knobs are semicolon-separated URL properties. With no TLS context the
     * legacy plaintext URL ({@code encrypt=false;trustServerCertificate=true}) is emitted
     * exactly as before; with TLS enabled {@link #appendTls} emits the {@code encrypt=true}
     * form instead of the hardcoded plaintext pair.
     */
    @Override
    public String buildJdbcUrl(ConnectionConfig conn, Path configDir) {
        if (conn.getCustomUrl() != null && !conn.getCustomUrl().isBlank()) {
            return conn.getCustomUrl();
        }
        if (conn.getHostname() == null) return null;
        int port = conn.getPort() > 0 ? conn.getPort() : 1433;
        StringBuilder url = new StringBuilder("jdbc:sqlserver://");
        url.append(conn.getHostname()).append(":").append(port);
        if (configDir != null && conn.isTlsEnabled()) {
            appendTls(conn, url, configDir);
        } else {
            url.append(";encrypt=false;trustServerCertificate=true");
        }
        url.append(";loginTimeout=10");
        if (conn.getServiceName() != null && !conn.getServiceName().isBlank()) {
            url.append(";databaseName=").append(conn.getServiceName());
        }
        // sid field reused for named instance (e.g. SQLEXPRESS)
        if (conn.getSid() != null && !conn.getSid().isBlank()) {
            url.append(";instanceName=").append(conn.getSid());
        }
        // role=WINDOWS triggers integrated security
        if ("WINDOWS".equalsIgnoreCase(conn.getRole())) {
            url.append(";integratedSecurity=true");
        }
        return url.toString();
    }

    /**
     * Enables SQL Server encryption. For the certificate-verifying modes (VERIFY_CA /
     * VERIFY_FULL) with a trust bundle and hostname validation on, the bundle is converted to
     * a PKCS12 keystore next to the PEM and referenced via {@code serverCertificateKeyStoreUrl}
     * with {@code trustServerCertificate=false}, i.e. the server chain is actually validated.
     * In every other case (no bundle, REQUIRED, or hostname validation disabled) the driver is
     * told {@code trustServerCertificate=<!hostnameValidation>}: validation off when the operator
     * has asked for it, on otherwise.
     */
    @Override
    public void appendTls(ConnectionConfig c, StringBuilder url, Path configDir) {
        String mode = c.getSslMode() == null ? "" : c.getSslMode().trim().toUpperCase();
        boolean verifyingMode = "VERIFY_CA".equals(mode) || "VERIFY_FULL".equals(mode);
        boolean hostnameOk = c.isHostnameValidation();
        if (verifyingMode && hostnameOk && c.getTrustCertPath() != null && !c.getTrustCertPath().isBlank()) {
            TlsKeystoreConverter.KeystoreArtifact art = TlsKeystoreConverter.ensurePkcs12StoreArtifact(
                    Path.of(c.getTrustCertPath()), configDir);
            DbDialect.appendSemiParam(url, "encrypt", "true");
            DbDialect.appendSemiParam(url, "trustServerCertificate", "false");
            DbDialect.appendSemiParam(url, "serverCertificateKeyStoreUrl", "file:" + art.path());
            DbDialect.appendSemiParam(url, "serverCertificateKeyStorePassword", art.password());
        } else {
            DbDialect.appendSemiParam(url, "encrypt", "true");
            DbDialect.appendSemiParam(url, "trustServerCertificate", String.valueOf(!hostnameOk));
        }
    }

    @Override public int    getDefaultPort()  { return 1433; }
    @Override public String getDriverClass()  { return "com.microsoft.sqlserver.jdbc.SQLServerDriver"; }
    @Override public String getDisplayName()  { return "SQL Server"; }
}
