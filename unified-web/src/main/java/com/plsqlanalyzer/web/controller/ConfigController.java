package com.plsqlanalyzer.web.controller;

import com.plsqlanalyzer.config.*;
import com.plsqlanalyzer.web.config.ConfigDirService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RestController("plsqlConfigController")
@RequestMapping("/api/plsql/config")
public class ConfigController {

    private static final Logger log = LoggerFactory.getLogger(ConfigController.class);

    private final PlsqlConfig config;
    private final Path configFilePath;
    private final ConfigDirService configDirService;

    public ConfigController(PlsqlConfig config,
                            @Qualifier("plsqlConfigFilePath") Path configFilePath,
                            ConfigDirService configDirService) {
        this.config = config;
        this.configFilePath = configFilePath;
        this.configDirService = configDirService;
    }

    @GetMapping
    public ResponseEntity<PlsqlConfig> getConfig() {
        return ResponseEntity.ok(config);
    }

    @GetMapping("/dir")
    public ResponseEntity<Map<String, Object>> getConfigDirInfo() {
        Map<String, Object> info = new LinkedHashMap<>();

        Path cfgDir = configDirService.getConfigDir();
        info.put("configDir", cfgDir.toAbsolutePath().toString());
        info.put("configFile", configFilePath.toAbsolutePath().toString());

        Path dataDir = configDirService.getDataDir();
        info.put("dataDir", dataDir.toAbsolutePath().toString());
        info.put("jarDataDir", configDirService.getJarDataDir().toAbsolutePath().toString());
        info.put("plsqlDataDir", configDirService.getPlsqlDataDir().toAbsolutePath().toString());

        // List config files
        List<String> files = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(cfgDir, 3)) {
            walk.filter(Files::isRegularFile)
                .forEach(f -> files.add(cfgDir.relativize(f).toString().replace('\\', '/')));
        } catch (Exception e) {
            files.add("error: " + e.getMessage());
        }
        info.put("configFiles", files);

        info.put("usage", "java -jar app.jar --app.config-dir=/path/to/config --app.data-dir=/path/to/data");
        return ResponseEntity.ok(info);
    }

    @GetMapping("/schemas")
    public ResponseEntity<Object> getSchemas() {
        return ResponseEntity.ok(config.getSchemaMappings());
    }

    // ── Projects CRUD ──

    @GetMapping("/projects")
    public ResponseEntity<List<ProjectConfig>> listProjects() {
        return ResponseEntity.ok(config.getProjects());
    }

    @PostMapping("/projects")
    public ResponseEntity<?> createProject(@RequestBody ProjectConfig project) {
        if (project.getName() == null || project.getName().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Project name is required"));
        }
        if (config.findProject(project.getName()) != null) {
            return ResponseEntity.badRequest().body(Map.of("error", "Project already exists: " + project.getName()));
        }
        config.getProjects().add(project);
        save();
        log.info("Created project: {}", project.getName());
        return ResponseEntity.ok(project);
    }

    @PutMapping("/projects/{name}")
    public ResponseEntity<?> updateProject(@PathVariable String name, @RequestBody ProjectConfig updated) {
        ProjectConfig existing = config.findProject(name);
        if (existing == null) return ResponseEntity.notFound().build();
        existing.setDescription(updated.getDescription());
        if (updated.getName() != null && !updated.getName().isBlank()) {
            existing.setName(updated.getName());
        }
        save();
        log.info("Updated project: {}", existing.getName());
        return ResponseEntity.ok(existing);
    }

    @DeleteMapping("/projects/{name}")
    public ResponseEntity<?> deleteProject(@PathVariable String name) {
        ProjectConfig existing = config.findProject(name);
        if (existing == null) return ResponseEntity.notFound().build();
        config.getProjects().remove(existing);
        save();
        log.info("Deleted project: {}", name);
        return ResponseEntity.ok(Map.of("deleted", name));
    }

    // ── Environments CRUD ──

    @GetMapping("/projects/{projectName}/environments")
    public ResponseEntity<?> listEnvironments(@PathVariable String projectName) {
        ProjectConfig project = config.findProject(projectName);
        if (project == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(project.getEnvironments());
    }

    @PostMapping("/projects/{projectName}/environments")
    public ResponseEntity<?> createEnvironment(@PathVariable String projectName,
                                               @RequestBody EnvironmentConfig env) {
        ProjectConfig project = config.findProject(projectName);
        if (project == null) return ResponseEntity.notFound().build();
        if (env.getName() == null || env.getName().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Environment name is required"));
        }
        if (project.findEnvironment(env.getName()) != null) {
            return ResponseEntity.badRequest().body(Map.of("error", "Environment already exists: " + env.getName()));
        }
        project.getEnvironments().add(env);
        save();
        log.info("Created environment {}/{}", projectName, env.getName());
        return ResponseEntity.ok(env);
    }

    @PutMapping("/projects/{projectName}/environments/{envName}")
    public ResponseEntity<?> updateEnvironment(@PathVariable String projectName,
                                               @PathVariable String envName,
                                               @RequestBody EnvironmentConfig updated) {
        ProjectConfig project = config.findProject(projectName);
        if (project == null) return ResponseEntity.notFound().build();
        EnvironmentConfig existing = project.findEnvironment(envName);
        if (existing == null) return ResponseEntity.notFound().build();
        if (updated.getName() != null && !updated.getName().isBlank()) existing.setName(updated.getName());
        if (updated.getZone() != null) existing.setZone(updated.getZone());
        save();
        log.info("Updated environment {}/{}", projectName, existing.getName());
        return ResponseEntity.ok(existing);
    }

    @DeleteMapping("/projects/{projectName}/environments/{envName}")
    public ResponseEntity<?> deleteEnvironment(@PathVariable String projectName,
                                               @PathVariable String envName) {
        ProjectConfig project = config.findProject(projectName);
        if (project == null) return ResponseEntity.notFound().build();
        EnvironmentConfig existing = project.findEnvironment(envName);
        if (existing == null) return ResponseEntity.notFound().build();
        project.getEnvironments().remove(existing);
        save();
        log.info("Deleted environment {}/{}", projectName, envName);
        return ResponseEntity.ok(Map.of("deleted", envName));
    }

    // ── Connections CRUD (SQL Developer-style, inside an environment) ──

    @GetMapping("/projects/{projectName}/environments/{envName}/connections")
    public ResponseEntity<?> listConnections(@PathVariable String projectName,
                                             @PathVariable String envName) {
        EnvironmentConfig env = resolveEnv(projectName, envName);
        if (env == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(env.getConnections());
    }

    @PostMapping("/projects/{projectName}/environments/{envName}/connections/test")
    public ResponseEntity<?> testConnection(@PathVariable String projectName,
                                            @PathVariable String envName,
                                            @RequestBody ConnectionConfig conn) {
        return doTest(conn);
    }

    @PostMapping("/projects/{projectName}/environments/{envName}/connections")
    public ResponseEntity<?> addConnection(@PathVariable String projectName,
                                           @PathVariable String envName,
                                           @RequestBody ConnectionConfig conn) {
        EnvironmentConfig env = resolveEnv(projectName, envName);
        if (env == null) return ResponseEntity.notFound().build();
        if (conn.getName() == null || conn.getName().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Connection name is required"));
        }
        if (conn.getUsername() == null || conn.getUsername().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Username is required"));
        }
        if (!conn.isTested()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Connection must be tested successfully before saving. Use the Test button first."));
        }
        if (env.findConnection(conn.getName()) != null) {
            return ResponseEntity.badRequest().body(Map.of("error", "Connection '" + conn.getName() + "' already exists"));
        }
        env.getConnections().add(conn);
        save();
        log.info("Added connection {} to {}/{}", conn.getName(), projectName, envName);
        return ResponseEntity.ok(conn);
    }

    @PutMapping("/projects/{projectName}/environments/{envName}/connections/{connName}")
    public ResponseEntity<?> updateConnection(@PathVariable String projectName,
                                              @PathVariable String envName,
                                              @PathVariable String connName,
                                              @RequestBody ConnectionConfig updated) {
        EnvironmentConfig env = resolveEnv(projectName, envName);
        if (env == null) return ResponseEntity.notFound().build();
        ConnectionConfig existing = env.findConnection(connName);
        if (existing == null) return ResponseEntity.notFound().build();
        if (!updated.isTested()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Connection must be tested successfully before saving."));
        }
        if (updated.getName() != null && !updated.getName().isBlank()) existing.setName(updated.getName());
        if (updated.getUsername() != null) existing.setUsername(updated.getUsername());
        if (updated.getPassword() != null) existing.setPassword(updated.getPassword());
        if (updated.getHostname() != null) existing.setHostname(updated.getHostname());
        if (updated.getPort() > 0) existing.setPort(updated.getPort());
        if (updated.getServiceName() != null) existing.setServiceName(updated.getServiceName());
        if (updated.getSid() != null) existing.setSid(updated.getSid());
        if (updated.getConnectionType() != null) existing.setConnectionType(updated.getConnectionType());
        if (updated.getRole() != null) existing.setRole(updated.getRole());
        if (updated.getTnsAlias() != null) existing.setTnsAlias(updated.getTnsAlias());
        if (updated.getCustomUrl() != null) existing.setCustomUrl(updated.getCustomUrl());
        if (updated.getDescription() != null) existing.setDescription(updated.getDescription());
        existing.setTested(true);
        save();
        log.info("Updated connection {} in {}/{}", existing.getName(), projectName, envName);
        return ResponseEntity.ok(existing);
    }

    @DeleteMapping("/projects/{projectName}/environments/{envName}/connections/{connName}")
    public ResponseEntity<?> deleteConnection(@PathVariable String projectName,
                                              @PathVariable String envName,
                                              @PathVariable String connName) {
        EnvironmentConfig env = resolveEnv(projectName, envName);
        if (env == null) return ResponseEntity.notFound().build();
        ConnectionConfig existing = env.findConnection(connName);
        if (existing == null) return ResponseEntity.notFound().build();
        env.getConnections().remove(existing);
        save();
        log.info("Deleted connection {} from {}/{}", connName, projectName, envName);
        return ResponseEntity.ok(Map.of("deleted", connName));
    }

    // ── Standalone test (not tied to saved connection) ──

    @PostMapping("/test-connection")
    public ResponseEntity<?> testAnyConnection(@RequestBody ConnectionConfig conn) {
        return doTest(conn);
    }

    // ── Resolve helpers for analysis ──

    @GetMapping("/resolve")
    public ResponseEntity<?> resolveEnvironment(@RequestParam String project,
                                                @RequestParam String env) {
        EnvironmentConfig resolved = config.resolveEnvironment(project, env);
        if (resolved == null) return ResponseEntity.notFound().build();
        List<Map<String, String>> connSummaries = new ArrayList<>();
        for (ConnectionConfig c : resolved.getConnections()) {
            Map<String, String> m = new LinkedHashMap<>();
            m.put("name", c.getName());
            m.put("username", c.getUsername());
            m.put("jdbcUrl", c.getJdbcUrl() != null ? c.getJdbcUrl() : "");
            m.put("description", c.getDescription() != null ? c.getDescription() : "");
            connSummaries.add(m);
        }
        return ResponseEntity.ok(Map.of(
                "connections", connSummaries,
                "users", resolved.getUsers(),
                "zone", resolved.getZone() != null ? resolved.getZone() : ""
        ));
    }

    // ── Internal helpers ──

    private EnvironmentConfig resolveEnv(String projectName, String envName) {
        ProjectConfig project = config.findProject(projectName);
        if (project == null) return null;
        return project.findEnvironment(envName);
    }

    private ResponseEntity<?> doTest(ConnectionConfig conn) {
        String jdbcUrl = conn.getJdbcUrl();
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "error", "Cannot build JDBC URL — check hostname, port, and service name/SID"));
        }
        String user = conn.getUsername();
        String pass = conn.getPassword();
        if (user == null || user.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "error", "Username is required"));
        }

        Properties props = new Properties();
        props.setProperty("user", user);
        if (pass != null) props.setProperty("password", pass);
        if (conn.getRole() != null && !"DEFAULT".equalsIgnoreCase(conn.getRole())) {
            props.setProperty("internal_logon", conn.getRole().toLowerCase());
        }

        try {
            long start = System.currentTimeMillis();
            try (Connection c = DriverManager.getConnection(jdbcUrl, props)) {
                long ms = System.currentTimeMillis() - start;
                String dbVersion = c.getMetaData().getDatabaseProductVersion();
                return ResponseEntity.ok(Map.of(
                        "success", true,
                        "message", "Connected in " + ms + "ms",
                        "version", dbVersion,
                        "url", jdbcUrl,
                        "user", user
                ));
            }
        } catch (Exception e) {
            log.warn("Connection test failed: {}", e.getMessage());
            return ResponseEntity.ok(Map.of(
                    "success", false,
                    "message", e.getMessage(),
                    "url", jdbcUrl,
                    "user", user
            ));
        }
    }

    private void save() {
        try {
            ConfigLoader.saveToFile(config, configFilePath);
        } catch (Exception e) {
            log.error("Failed to save config to {}: {}", configFilePath, e.getMessage());
        }
    }
}
