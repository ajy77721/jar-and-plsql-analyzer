package com.plsqlanalyzer.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

public class ConnectionConfig {
    /**
     * Request-only routing hint: {@code "user"} (this caller's private connection store) or
     * {@code "shared"} (the global plsql-config.yaml). It is never persisted — null is omitted
     * from both the REST and the YAML serialisation — so it carries no state and cannot leak
     * between the two scopes. Absent means "let the endpoint decide" (new saves default to the
     * user's own store, existing behaviour for the shared/global path is preserved by scope=shared).
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String scope;
    private String name;
    private String username;
    private String password;
    private String hostname;
    private int port = 0; // 0 means "use dialect default"
    private String serviceName;
    private String sid;
    private String connectionType = "SERVICE_NAME"; // SERVICE_NAME, SID, TNS, LDAP, CUSTOM
    private String role = "DEFAULT"; // DEFAULT, SYSDBA, SYSOPER, WINDOWS (SQL Server integrated auth)
    private String tnsAlias;
    private String customUrl;
    private String description;
    private boolean tested;
    // Epoch millis of the last SUCCESSFUL connection test, or null/absent when never tested.
    // Lets the setup modal rank "recently tested" connections at the top of its picker. Persisted
    // as snake_case `last_tested_at` by ConfigLoader's naming strategy; REST binds camelCase
    // `lastTestedAt` via Spring's default mapper — the same mapper split as hostnameValidation, so
    // deliberately no explicit @JsonProperty (which would force snake_case on BOTH and drop the
    // REST field).
    private Long lastTestedAt;
    // ORACLE (default) | MYSQL | SQLSERVER
    private String dbType = "ORACLE";
    // Schemas reachable/queryable via this connection's credentials (may differ from the
    // login username — e.g. "analyzer" user with grants into "ANALYZER_REF"). Populated by
    // auto-discovery on test/save; manually editable afterward. Falls back to [username]
    // when absent/empty so existing configs (no `schemas` key) keep working unchanged.
    private java.util.List<String> schemas;

    // ── TLS / connection-security settings ──────────────────────────────────────
    // All null/absent by default so existing YAML configs (no TLS keys) continue to mean
    // "plaintext, exactly as before". "NONE" is the explicit spelling of the same thing.

    // TLS policy: NONE (default) | REQUIRED | VERIFY_CA | VERIFY_FULL. null == NONE.
    private String sslMode;
    // Whether the server certificate hostname must match the connected host. Null == true.
    private Boolean hostnameValidation;
    // Repo-relative path (under the config directory, e.g. "certs/MY_CONN/trust.pem") to a
    // PEM trust bundle (server CA / self-signed chain). Converted to JKS/PKCS12 where the
    // driver requires it — see TlsKeystoreConverter.
    private String trustCertPath;
    // Optional PEM client certificate + private key for mutual TLS. Both must be set for the
    // pair to be used (PostgreSQL sslcert/sslkey).
    private String clientCertPath;
    private String clientKeyPath;
    // Optional pre-built JKS truststore (with its password) for drivers/setups that insist on
    // a keystore rather than PEM: IBM DB2 and Oracle JSSE truststore configurations.
    private String truststorePath;
    private String truststorePassword;
    // Optional Oracle wallet directory (under certs/) used for TLS material in place of a
    // plain truststore. Rendered as an "oracle.net.wallet_location" property.
    private String walletPath;

    public ConnectionConfig() {}

    public DbType resolvedDbType() {
        return DbType.fromString(dbType);
    }

    public DbDialect dialect() {
        return DbDialectRegistry.get(resolvedDbType());
    }

    public int getEffectivePort() {
        return port > 0 ? port : dialect().getDefaultPort();
    }

    /**
     * Legacy, TLS-free URL accessor. Retained verbatim for every existing caller — it builds
     * the same plaintext URL the analyzer has always emitted (no TLS parameters), regardless
     * of any {@code sslMode} on this config.
     */
    public String getJdbcUrl() {
        return getJdbcUrl(null);
    }

    /**
     * TLS-aware URL builder. When {@code configDir} is supplied and TLS is enabled, the
     * dialect appends the appropriate protocol/parameter changes (MySQL query params,
     * SQL Server {@code encrypt=} flags, PostgreSQL {@code sslmode=}, Oracle TCPS).
     *
     * @param configDir the configuration directory relative certificate paths resolve
     *                  against, or {@code null} for the legacy plaintext URL
     */
    public String getJdbcUrl(java.nio.file.Path configDir) {
        return dialect().buildJdbcUrl(this, configDir);
    }

    /**
     * True when this connection asks for an encrypted transport. A null/blank/`NONE` sslMode
     * means plaintext (TLS is opt-in, preserving backward compatibility).
     */
    @JsonIgnore
    public boolean isTlsEnabled() {
        return sslMode != null && !sslMode.isBlank() && !"NONE".equalsIgnoreCase(sslMode.trim());
    }

    /**
     * Effective hostname-validation flag. The stored {@link Boolean} is a wrapper so an absent
     * key can mean "default"; a null value therefore resolves to {@code true} (validate), which
     * is the safe default for VERIFY_* modes.
     */
    @JsonIgnore
    public boolean isHostnameValidation() {
        return hostnameValidation == null || hostnameValidation;
    }

    /**
     * Builds driver {@link java.util.Properties} carrying TLS material that cannot be
     * expressed in the JDBC URL. Returns an empty set when TLS is disabled, so callers can
     * pass the result straight to a driver in every configuration.
     *
     * <p>Only drivers whose TLS knobs live outside the URL are handled here:
     * <ul>
     *   <li><b>ORACLE</b> — truststore (PEM converted to JKS, or a JKS/password pair, or a
     *       wallet directory) plus the DN-match flags when hostname validation is off.</li>
     *   <li><b>DB2</b> — {@code sslConnection=true} plus a JKS truststore.</li>
     *   <li><b>MYSQL / SQLSERVER / POSTGRES</b> — TLS is expressed entirely in the URL, so no
     *       properties are produced.</li>
     * </ul>
     *
     * <p>Relative certificate paths are resolved against {@code configDir} here, so the
     * returned properties always point at absolute files.
     */
    public java.util.Properties buildSslProperties(java.nio.file.Path configDir) {
        java.util.Properties props = new java.util.Properties();
        if (!isTlsEnabled()) {
            return props;
        }
        switch (resolvedDbType()) {
            case ORACLE -> applyOracleSslProperties(props, configDir);
            case DB2    -> applyDb2SslProperties(props, configDir);
            default     -> { /* MYSQL, SQLSERVER, POSTGRES: URL-side only */ }
        }
        return props;
    }

    /**
     * Oracle TLS material lives in driver/JSSE properties rather than the thin URL. Precedence
     * is wallet &gt; explicit JKS truststore &gt; PEM bundle, mirroring how operators are
     * expected to supply Oracle credentials (a wallet, when present, is the whole story).
     * The negotiated TLS version is left to the server/JVM ({@code oracle.net.ssl_version} is
     * intentionally not set). When hostname validation is disabled we relax the DN-match flags
     * so self-signed / CN-mismatched endpoints still connect.
     */
    private void applyOracleSslProperties(java.util.Properties props, java.nio.file.Path configDir) {
        java.nio.file.Path wallet = TlsKeystoreConverter.resolve(
                walletPath != null ? java.nio.file.Path.of(walletPath) : null, configDir);
        if (wallet != null) {
            props.setProperty("oracle.net.wallet_location", "file:" + wallet);
        } else if (truststorePath != null && !truststorePath.isBlank()) {
            java.nio.file.Path ts = TlsKeystoreConverter.resolve(java.nio.file.Path.of(truststorePath), configDir);
            props.setProperty("javax.net.ssl.trustStore", ts.toString());
            if (truststorePassword != null) {
                props.setProperty("javax.net.ssl.trustStorePassword", truststorePassword);
            }
        } else if (trustCertPath != null && !trustCertPath.isBlank()) {
            TlsKeystoreConverter.KeystoreArtifact art = TlsKeystoreConverter.ensureTrustStoreArtifact(
                    java.nio.file.Path.of(trustCertPath), configDir);
            props.setProperty("javax.net.ssl.trustStore", art.path().toString());
            props.setProperty("javax.net.ssl.trustStorePassword", art.password());
        }
        if (!isHostnameValidation()) {
            props.setProperty("oracle.net.ssl_server_dn_match", "false");
            props.setProperty("oracle.net.ssl_client_dn_match", "false");
        }
    }

    /**
     * DB2 takes all TLS settings as driver properties: {@code sslConnection=true} turns on
     * encryption, and trust material is supplied as a JKS truststore location + password.
     * Either a pre-built JKS ({@code truststorePath}) or a PEM bundle ({@code trustCertPath},
     * converted to a sibling JKS) is accepted.
     */
    private void applyDb2SslProperties(java.util.Properties props, java.nio.file.Path configDir) {
        props.setProperty("sslConnection", "true");
        java.nio.file.Path store = null;
        String password = null;
        if (truststorePath != null && !truststorePath.isBlank()) {
            store = TlsKeystoreConverter.resolve(java.nio.file.Path.of(truststorePath), configDir);
            password = truststorePassword;
        } else if (trustCertPath != null && !trustCertPath.isBlank()) {
            TlsKeystoreConverter.KeystoreArtifact art = TlsKeystoreConverter.ensureTrustStoreArtifact(
                    java.nio.file.Path.of(trustCertPath), configDir);
            store = art.path();
            password = art.password();
        }
        if (store != null) {
            props.setProperty("sslTrustStoreLocation", store.toString());
            if (password != null) {
                props.setProperty("sslTrustStorePassword", password);
            }
            props.setProperty("sslTrustStoreType", "JKS");
        }
    }

    public DbUserConfig toDbUserConfig() {
        return new DbUserConfig(username, password, description != null ? description : name, schemas);
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    /** Request-only "user" | "shared" routing hint; never persisted. */
    public String getScope() { return scope; }
    public void setScope(String scope) { this.scope = scope; }

    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }

    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }

    public String getHostname() { return hostname; }
    public void setHostname(String hostname) { this.hostname = hostname; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public String getServiceName() { return serviceName; }
    public void setServiceName(String serviceName) { this.serviceName = serviceName; }

    public String getSid() { return sid; }
    public void setSid(String sid) { this.sid = sid; }

    public String getConnectionType() { return connectionType; }
    public void setConnectionType(String connectionType) { this.connectionType = connectionType; }

    public String getRole() { return role; }
    public void setRole(String role) { this.role = role; }

    public String getTnsAlias() { return tnsAlias; }
    public void setTnsAlias(String tnsAlias) { this.tnsAlias = tnsAlias; }

    public String getCustomUrl() { return customUrl; }
    public void setCustomUrl(String customUrl) { this.customUrl = customUrl; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public boolean isTested() { return tested; }
    public void setTested(boolean tested) { this.tested = tested; }

    /** Epoch millis of the last successful test, or null when the connection has never been tested. */
    public Long getLastTestedAt() { return lastTestedAt; }
    public void setLastTestedAt(Long lastTestedAt) { this.lastTestedAt = lastTestedAt; }

    public String getDbType() { return dbType; }
    public void setDbType(String dbType) { this.dbType = dbType; }

    /** Declared schemas, falling back to [username] when unset (backward compat). */
    public java.util.List<String> getSchemas() {
        if (schemas == null || schemas.isEmpty()) {
            return username != null ? java.util.List.of(username.toUpperCase()) : java.util.List.of();
        }
        return schemas;
    }
    /** Raw stored value (may be null) — used by (de)serialization to avoid persisting a default. */
    public java.util.List<String> getSchemasRaw() { return schemas; }
    public void setSchemas(java.util.List<String> schemas) { this.schemas = schemas; }

    /** TLS policy: NONE | REQUIRED | VERIFY_CA | VERIFY_FULL; null == NONE. */
    public String getSslMode() { return sslMode; }
    public void setSslMode(String sslMode) { this.sslMode = sslMode; }

    /**
     * Raw hostname-validation flag (null means "use default true"); see
     * {@link #isHostnameValidation()} for the effective value. No explicit @JsonProperty here:
     * the YAML key {@code hostname_validation} comes from ConfigLoader's SNAKE_CASE strategy,
     * while REST (Spring's default camelCase mapper) binds {@code hostnameValidation}. An explicit
     * @JsonProperty would force snake_case on BOTH mappers and silently drop the REST field.
     */
    public Boolean getHostnameValidation() { return hostnameValidation; }
    public void setHostnameValidation(Boolean hostnameValidation) { this.hostnameValidation = hostnameValidation; }

    /** Repo-relative path to a PEM trust bundle (server CA / self-signed chain). */
    public String getTrustCertPath() { return trustCertPath; }
    public void setTrustCertPath(String trustCertPath) { this.trustCertPath = trustCertPath; }

    /** Repo-relative path to an optional PEM client certificate (mutual TLS). */
    public String getClientCertPath() { return clientCertPath; }
    public void setClientCertPath(String clientCertPath) { this.clientCertPath = clientCertPath; }

    /** Repo-relative path to the private key paired with {@link #getClientCertPath()}. */
    public String getClientKeyPath() { return clientKeyPath; }
    public void setClientKeyPath(String clientKeyPath) { this.clientKeyPath = clientKeyPath; }

    /** Optional pre-built JKS truststore path (for DB2 / Oracle JSSE truststore setups). */
    public String getTruststorePath() { return truststorePath; }
    public void setTruststorePath(String truststorePath) { this.truststorePath = truststorePath; }

    /** Password for {@link #getTruststorePath()}; never logged. */
    public String getTruststorePassword() { return truststorePassword; }
    public void setTruststorePassword(String truststorePassword) { this.truststorePassword = truststorePassword; }

    /** Optional Oracle wallet directory under certs/. */
    public String getWalletPath() { return walletPath; }
    public void setWalletPath(String walletPath) { this.walletPath = walletPath; }
}
