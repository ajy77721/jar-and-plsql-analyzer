package com.plsqlanalyzer.config;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class EnvironmentConfig {
    private String name;
    private String zone;
    private List<ConnectionConfig> connections = new ArrayList<>();

    public EnvironmentConfig() {}

    public EnvironmentConfig(String name, String zone) {
        this.name = name;
        this.zone = zone;
    }

    public ConnectionConfig findConnection(String connName) {
        if (connName == null) return null;
        for (ConnectionConfig c : connections) {
            if (connName.equalsIgnoreCase(c.getName())) return c;
        }
        return null;
    }

    public ConnectionConfig findConnectionByUsername(String username) {
        if (username == null) return null;
        for (ConnectionConfig c : connections) {
            if (username.equalsIgnoreCase(c.getUsername())) return c;
        }
        return null;
    }

    public List<DbUserConfig> getUsers() {
        return connections.stream()
                .map(ConnectionConfig::toDbUserConfig)
                .collect(Collectors.toList());
    }

    public String getJdbcUrl() {
        if (connections.isEmpty()) return null;
        return connections.get(0).getJdbcUrl();
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getZone() { return zone; }
    public void setZone(String zone) { this.zone = zone; }

    public List<ConnectionConfig> getConnections() { return connections; }
    public void setConnections(List<ConnectionConfig> connections) { this.connections = connections; }
}
