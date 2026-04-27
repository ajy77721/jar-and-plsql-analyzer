package com.plsqlanalyzer.config;

public class ConnectionConfig {
    private String name;
    private String username;
    private String password;
    private String hostname;
    private int port = 1521;
    private String serviceName;
    private String sid;
    private String connectionType = "SERVICE_NAME"; // SERVICE_NAME, SID, TNS, LDAP, CUSTOM
    private String role = "DEFAULT"; // DEFAULT, SYSDBA, SYSOPER
    private String tnsAlias;
    private String customUrl;
    private String description;
    private boolean tested;

    public ConnectionConfig() {}

    public String getJdbcUrl() {
        switch (connectionType != null ? connectionType.toUpperCase() : "SERVICE_NAME") {
            case "SID":
                if (hostname == null || sid == null) return null;
                return "jdbc:oracle:thin:@" + hostname + ":" + port + ":" + sid;
            case "TNS":
                if (tnsAlias == null) return null;
                return "jdbc:oracle:thin:@" + tnsAlias;
            case "CUSTOM":
                return customUrl;
            case "LDAP":
                if (customUrl != null) return customUrl;
                return null;
            default: // SERVICE_NAME
                if (hostname == null || serviceName == null) return null;
                return "jdbc:oracle:thin:@//" + hostname + ":" + port + "/" + serviceName;
        }
    }

    public DbUserConfig toDbUserConfig() {
        return new DbUserConfig(username, password, description != null ? description : name);
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
}
