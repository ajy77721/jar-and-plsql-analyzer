package com.plsqlanalyzer.config;

public class DbUserConfig {
    private String username;
    private String password;
    private String description;
    // Schemas reachable via this user's login (may differ from username). Falls back to
    // [username] when absent — backward compatible with existing configs.
    private java.util.List<String> schemas;

    public DbUserConfig() {}

    public DbUserConfig(String username, String password, String description) {
        this.username = username;
        this.password = password;
        this.description = description;
    }

    public DbUserConfig(String username, String password, String description, java.util.List<String> schemas) {
        this.username = username;
        this.password = password;
        this.description = description;
        this.schemas = schemas;
    }

    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }

    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    /** Declared schemas, falling back to [username] when unset (backward compat). */
    public java.util.List<String> getSchemas() {
        if (schemas == null || schemas.isEmpty()) {
            return username != null ? java.util.List.of(username.toUpperCase()) : java.util.List.of();
        }
        return schemas;
    }
    public void setSchemas(java.util.List<String> schemas) { this.schemas = schemas; }
}
