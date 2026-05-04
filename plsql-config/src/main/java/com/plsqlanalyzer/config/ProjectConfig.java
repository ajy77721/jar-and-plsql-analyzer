package com.plsqlanalyzer.config;

import java.util.ArrayList;
import java.util.List;

public class ProjectConfig {
    private String name;
    private String description;
    private List<EnvironmentConfig> environments = new ArrayList<>();

    public ProjectConfig() {}

    public ProjectConfig(String name, String description) {
        this.name = name;
        this.description = description;
    }

    public EnvironmentConfig findEnvironment(String envName) {
        if (envName == null) return null;
        for (EnvironmentConfig env : environments) {
            if (envName.equalsIgnoreCase(env.getName())) return env;
        }
        return null;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public List<EnvironmentConfig> getEnvironments() { return environments; }
    public void setEnvironments(List<EnvironmentConfig> environments) { this.environments = environments; }
}
