package com.plsqlanalyzer.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PlsqlConfig {
    private List<String> sourcePaths = new ArrayList<>();
    private Map<String, SchemaMapping> schemaMappings = new HashMap<>();
    private List<String> fileExtensions = List.of(".sql", ".pks", ".pkb", ".prc", ".fnc", ".trg");
    private List<String> excludePatterns = new ArrayList<>();

    // Oracle DB connection (legacy single-environment mode)
    private String dbHostname;
    private int dbPort = 1521;
    private String dbServiceName;
    private List<DbUserConfig> dbUsers = new ArrayList<>();

    // Project-based multi-environment mode
    private List<ProjectConfig> projects = new ArrayList<>();

    public PlsqlConfig() {}

    public List<String> getSourcePaths() { return sourcePaths; }
    public void setSourcePaths(List<String> sourcePaths) { this.sourcePaths = sourcePaths; }

    public Map<String, SchemaMapping> getSchemaMappings() { return schemaMappings; }
    public void setSchemaMappings(Map<String, SchemaMapping> schemaMappings) { this.schemaMappings = schemaMappings; }

    public List<String> getFileExtensions() { return fileExtensions; }
    public void setFileExtensions(List<String> fileExtensions) { this.fileExtensions = fileExtensions; }

    public List<String> getExcludePatterns() { return excludePatterns; }
    public void setExcludePatterns(List<String> excludePatterns) { this.excludePatterns = excludePatterns; }

    public String getDbHostname() { return dbHostname; }
    public void setDbHostname(String dbHostname) { this.dbHostname = dbHostname; }

    public int getDbPort() { return dbPort; }
    public void setDbPort(int dbPort) { this.dbPort = dbPort; }

    public String getDbServiceName() { return dbServiceName; }
    public void setDbServiceName(String dbServiceName) { this.dbServiceName = dbServiceName; }

    public List<DbUserConfig> getDbUsers() { return dbUsers; }
    public void setDbUsers(List<DbUserConfig> dbUsers) { this.dbUsers = dbUsers; }

    public String getJdbcUrl() {
        if (dbHostname == null || dbServiceName == null) return null;
        return "jdbc:oracle:thin:@//" + dbHostname + ":" + dbPort + "/" + dbServiceName;
    }

    public String resolveSchema(String alias) {
        if (alias == null) return null;
        SchemaMapping mapping = schemaMappings.get(alias.toLowerCase());
        return mapping != null ? mapping.getSchema() : alias.toUpperCase();
    }

    public List<ProjectConfig> getProjects() { return projects; }
    public void setProjects(List<ProjectConfig> projects) { this.projects = projects; }

    public boolean isLegacyMode() {
        return projects == null || projects.isEmpty();
    }

    public ProjectConfig findProject(String projectName) {
        if (projectName == null) return null;
        for (ProjectConfig p : projects) {
            if (projectName.equalsIgnoreCase(p.getName())) return p;
        }
        return null;
    }

    public EnvironmentConfig resolveEnvironment(String projectName, String envName) {
        ProjectConfig project = findProject(projectName);
        if (project == null) return null;
        return project.findEnvironment(envName);
    }
}
