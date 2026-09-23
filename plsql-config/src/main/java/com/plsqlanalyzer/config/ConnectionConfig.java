package com.plsqlanalyzer.config;

public class ConnectionConfig {
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
    // ORACLE (default) | MYSQL | SQLSERVER
    private String dbType = "ORACLE";
    // Schemas reachable/queryable via this connection's credentials (may differ from the
    // login username — e.g. "analyzer" user with grants into "ANALYZER_REF"). Populated by
    // auto-discovery on test/save; manually editable afterward. Falls back to [username]
    // when absent/empty so existing configs (no `schemas` key) keep working unchanged.
    private java.util.List<String> schemas;

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

    public String getJdbcUrl() {
        return dialect().buildJdbcUrl(this);
    }

    public DbUserConfig toDbUserConfig() {
        return new DbUserConfig(username, password, description != null ? description : name, schemas);
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

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
}
