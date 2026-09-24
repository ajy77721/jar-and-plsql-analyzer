package com.plsqlanalyzer.web.controller;

import com.plsqlanalyzer.config.*;
import com.plsqlanalyzer.web.config.ConfigDirService;
import java.sql.DriverManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RestController("plsqlConfigController")
@RequestMapping("/api/plsql/config")
public class ConfigController {

    private static final Logger log = LoggerFactory.getLogger(ConfigController.class);
    // Cache quick-test results for 5 min per "proj/env/user" key — avoids Oracle SQLNET_INBOUND_CONNECT_TIMEOUT
    // and firewall rate-limit (ORA-17800) triggered by rapid repeated TCP handshakes on every page load.
    private static final long QUICK_TEST_CACHE_MS = 300_000;
    private final Map<String, Object[]> quickTestCache = new ConcurrentHashMap<>();

    // Uploaded certificates/wallets live under {configDir}/certs/{connectionName}/{filename}.
    // Only this conservative character set is accepted for BOTH the connection-name directory
    // component and the filename — it excludes '/', '\\', ':' and '..' can never survive as a
    // distinct path segment, so a crafted name cannot escape the certs directory.
    private static final Pattern SAFE_PATH_SEGMENT = Pattern.compile("[A-Za-z0-9._-]+");
    // Hard cap on a single uploaded certificate/wallet (base64 or PEM text decodes to bytes,
    // and it is the DECODED byte length that is checked).
    private static final long MAX_CERT_BYTES = 5L * 1024 * 1024;

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

    /** Serve plsql-export-config.json for the frontend Excel export. */
    @GetMapping("/export-config")
    public ResponseEntity<String> getExportConfig() {
        try {
            Path f = configDirService.getConfigDir().resolve("plsql-export-config.json");
            if (Files.exists(f)) {
                return ResponseEntity.ok()
                    .header("Content-Type", "application/json")
                    .body(Files.readString(f));
            }
            return ResponseEntity.notFound().build();
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("{}");
        }
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
        if (updated.getDbType() != null && !updated.getDbType().isBlank()) {
            existing.setDbType(updated.getDbType());
        }
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
        ResponseEntity<?> result = doTest(conn);
        // The body is a DETACHED copy, so a successful test here would otherwise leave no trace.
        // Record the test time on the STORED connection (matched by name) so the setup modal can
        // rank recently-tested connections; best-effort — an unknown name or a failed test is a no-op.
        if (result.getBody() instanceof Map<?, ?> body && Boolean.TRUE.equals(body.get("success"))) {
            EnvironmentConfig env = resolveEnv(projectName, envName);
            ConnectionConfig stored = env != null ? env.findConnection(conn.getName()) : null;
            if (stored != null) {
                stored.setLastTestedAt(System.currentTimeMillis());
                save();
            }
        }
        return result;
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
        // A save is only reachable after a passing test, so stamp "last tested" now — this is what
        // orders the setup modal's recent-connections picker even for configs that predate the field.
        conn.setLastTestedAt(System.currentTimeMillis());
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
        // Blank means "unchanged": the editor can no longer pre-fill the password (see
        // HttpSecretMaskingConfig), so an untouched form submits an empty string. An empty password
        // is not a value anyone can legitimately save, so treating it as a no-op is safe.
        if (updated.getPassword() != null && !updated.getPassword().isBlank()) {
            existing.setPassword(updated.getPassword());
        }
        if (updated.getHostname() != null) existing.setHostname(updated.getHostname());
        if (updated.getPort() > 0) existing.setPort(updated.getPort());
        if (updated.getServiceName() != null) existing.setServiceName(updated.getServiceName());
        if (updated.getSid() != null) existing.setSid(updated.getSid());
        if (updated.getConnectionType() != null) existing.setConnectionType(updated.getConnectionType());
        if (updated.getRole() != null) existing.setRole(updated.getRole());
        if (updated.getTnsAlias() != null) existing.setTnsAlias(updated.getTnsAlias());
        if (updated.getCustomUrl() != null) existing.setCustomUrl(updated.getCustomUrl());
        if (updated.getDescription() != null) existing.setDescription(updated.getDescription());
        if (updated.getSchemasRaw() != null) existing.setSchemas(updated.getSchemasRaw());
        // TLS fields must be carried across explicitly: this endpoint copies field-by-field (it
        // does NOT replace the object), so anything omitted here would silently be dropped on save.
        if (updated.getSslMode() != null) existing.setSslMode(updated.getSslMode());
        if (updated.getHostnameValidation() != null) existing.setHostnameValidation(updated.getHostnameValidation());
        if (updated.getTrustCertPath() != null) existing.setTrustCertPath(updated.getTrustCertPath());
        if (updated.getClientCertPath() != null) existing.setClientCertPath(updated.getClientCertPath());
        if (updated.getClientKeyPath() != null) existing.setClientKeyPath(updated.getClientKeyPath());
        if (updated.getTruststorePath() != null) existing.setTruststorePath(updated.getTruststorePath());
        // Blank means "unchanged", mirroring the password handling above (the editor cannot
        // pre-fill a secret, so an untouched form submits an empty string).
        if (updated.getTruststorePassword() != null && !updated.getTruststorePassword().isBlank()) {
            existing.setTruststorePassword(updated.getTruststorePassword());
        }
        if (updated.getWalletPath() != null) existing.setWalletPath(updated.getWalletPath());
        existing.setTested(true);
        // Same reasoning as addConnection: the save gate guarantees a fresh passing test, so this
        // update marks the connection as the most recently tested one.
        existing.setLastTestedAt(System.currentTimeMillis());
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

    /**
     * Standard TCP port → database type, e.g. {@code {"1521":"ORACLE","3306":"MYSQL",...}}.
     *
     * <p>The setup screen calls this to pre-select a DB type when the operator types a well-known
     * port, so the mapping has exactly one server-side definition ({@link DbType#standardPortMap()})
     * rather than a second copy hard-coded in JavaScript. The UI keeps a built-in fallback so the
     * form still works if this call fails.
     */
    @GetMapping("/db-type-suggest")
    public ResponseEntity<Map<String, String>> getDbTypeSuggestions() {
        return ResponseEntity.ok(DbType.standardPortMapByName());
    }

    // ── Certificate / wallet upload for TLS-enabled connections ──

    /**
     * Store one certificate, key material, truststore or Oracle wallet file for a connection.
     *
     * <p>The body is {@code { "filename": "trust.pem", "content": "<PEM text or base64>") }}. PEM
     * text (anything containing a {@code -----BEGIN ...-----} marker) is written verbatim; every
     * other payload is treated as base64, which is how the UI ships binary artifacts such as
     * {@code .zip} wallets or {@code .jks}/{@code .p12} keystores. The decoded payload is capped
     * at {@link #MAX_CERT_BYTES}.
     *
     * <p>The file lands at {@code {configDir}/certs/{connectionName}/{filename}} and the response
     * returns that path RELATIVE to the config dir:
     * {@code { "ok": true, "path": "certs/{connectionName}/{filename}" }}. That relative string is
     * exactly what belongs in the connection's {@code trustCertPath}/{@code walletPath}/
     * {@code clientCertPath} fields, because {@code ConnectionConfig.buildSslProperties(configDir)}
     * resolves those fields against the same config dir at connect time.
     *
     * <p>Both the connection name and the filename are restricted to {@code [A-Za-z0-9._-]} and
     * {@code ..} is rejected outright, so a crafted value cannot traverse out of the certs tree.
     * Certificate contents are never logged.
     */
    @PostMapping("/certs/{connectionName}")
    public ResponseEntity<?> uploadCertificate(@PathVariable String connectionName,
                                               @RequestBody Map<String, String> body) {
        try {
            String filename = body != null ? body.get("filename") : null;
            String content = body != null ? body.get("content") : null;
            if (filename == null || filename.isBlank()) {
                return ResponseEntity.badRequest().body(Map.of("ok", false, "error", "filename is required"));
            }
            if (content == null || content.isBlank()) {
                return ResponseEntity.badRequest().body(Map.of("ok", false, "error", "content is required"));
            }
            if (!isSafePathSegment(connectionName)) {
                return ResponseEntity.badRequest().body(Map.of("ok", false,
                        "error", "Invalid connection name — only letters, digits, dot, dash and underscore are allowed"));
            }
            if (!isSafePathSegment(filename)) {
                return ResponseEntity.badRequest().body(Map.of("ok", false,
                        "error", "Invalid filename — only letters, digits, dot, dash and underscore are allowed"));
            }

            byte[] bytes = decodeCertificateContent(filename, content);
            if (bytes.length == 0) {
                return ResponseEntity.badRequest().body(Map.of("ok", false, "error", "Certificate content is empty"));
            }
            if (bytes.length > MAX_CERT_BYTES) {
                return ResponseEntity.badRequest().body(Map.of("ok", false,
                        "error", "Certificate exceeds the " + (MAX_CERT_BYTES / (1024 * 1024)) + " MB limit"));
            }

            Path certsRoot = configDir().resolve("certs");
            Path dir = certsRoot.resolve(connectionName);
            Path target = dir.resolve(filename).normalize();
            // Defence in depth: the character whitelist already blocks separators and "..", but
            // verify the resolved path really is inside the certs tree before touching disk.
            if (!target.startsWith(certsRoot.normalize())) {
                return ResponseEntity.badRequest().body(Map.of("ok", false, "error", "Invalid certificate path"));
            }
            Files.createDirectories(dir);
            Files.write(target, bytes);

            String relativePath = "certs/" + connectionName + "/" + filename;
            // Log the name only — never the certificate/key/token contents.
            log.info("Stored certificate '{}' for connection '{}'", filename, connectionName);
            return ResponseEntity.ok(Map.of("ok", true, "path", relativePath));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("ok", false, "error", e.getMessage()));
        } catch (Exception e) {
            log.error("Failed to store certificate for connection '{}': {}", connectionName, e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of("ok", false,
                    "error", "Failed to store certificate"));
        }
    }

    /** True when a single path component matches the upload whitelist and cannot traverse. */
    private static boolean isSafePathSegment(String value) {
        return value != null && !value.isBlank()
                && !value.contains("..")
                && SAFE_PATH_SEGMENT.matcher(value).matches();
    }

    /**
     * Decode an uploaded certificate payload. PEM text is stored verbatim; anything else is
     * assumed to be base64 (binary wallets/keystores). Falling back to the raw string when base64
     * decoding fails keeps a marker-less PEM usable instead of rejecting it.
     */
    private static byte[] decodeCertificateContent(String filename, String content) {
        String trimmed = content.trim();
        String lower = filename.toLowerCase(Locale.ROOT);
        boolean binaryName = lower.endsWith(".zip") || lower.endsWith(".jks")
                || lower.endsWith(".p12") || lower.endsWith(".pfx");
        if (!binaryName && trimmed.contains("-----BEGIN")) {
            return trimmed.getBytes(StandardCharsets.UTF_8);
        }
        try {
            return Base64.getMimeDecoder().decode(trimmed);
        } catch (IllegalArgumentException notBase64) {
            return trimmed.getBytes(StandardCharsets.UTF_8);
        }
    }

    // ── Quick test for a saved project/environment (uses stored credentials) ──

    @GetMapping("/projects/{projectName}/environments/{envName}/quick-test")
    public ResponseEntity<?> quickTestEnv(@PathVariable String projectName,
                                           @PathVariable String envName) {
        EnvironmentConfig env = resolveEnv(projectName, envName);
        if (env == null) return ResponseEntity.notFound().build();
        if (env.getConnections().isEmpty()) {
            return ResponseEntity.ok(Map.of("success", false, "message", "No connections configured"));
        }
        ConnectionConfig conn = env.getConnections().get(0);
        // Return cached result if still fresh — avoids a full Oracle TCP handshake on every page load
        String cacheKey = projectName + "/" + envName + "/" + conn.getUsername();
        Object[] cached = quickTestCache.get(cacheKey);
        if (cached != null && (System.currentTimeMillis() - (Long) cached[0]) < QUICK_TEST_CACHE_MS) {
            return ResponseEntity.ok(cached[1]);
        }
        List<String> schemasBefore = conn.getSchemasRaw();
        Long testedAtBefore = conn.getLastTestedAt();
        ResponseEntity<?> result = doTest(conn);
        // doTest() mutates `conn` (a live, config-attached object here) with freshly discovered
        // schemas and a fresh lastTestedAt — persist that so it survives a restart, not just this
        // JVM's lifetime.
        if (!Objects.equals(schemasBefore, conn.getSchemasRaw())
                || !Objects.equals(testedAtBefore, conn.getLastTestedAt())) {
            save();
        }
        quickTestCache.put(cacheKey, new Object[]{System.currentTimeMillis(), result.getBody()});
        return result;
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
            // Show the TLS-aware URL — the one runtime will actually dial for this connection.
            String resolvedUrl = c.getJdbcUrl(configDir());
            m.put("jdbcUrl", resolvedUrl != null ? resolvedUrl : "");
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

    /**
     * The app's config directory — the parent of {@code plsql-config.yaml}. This is the base
     * against which {@link ConnectionConfig#buildSslProperties(java.nio.file.Path)} and
     * {@link ConnectionConfig#getJdbcUrl(java.nio.file.Path)} resolve relative certificate,
     * wallet and truststore paths, so test-connect and runtime agree on every TLS artifact.
     */
    private Path configDir() {
        Path parent = configFilePath != null ? configFilePath.getParent() : null;
        return parent != null ? parent : Path.of(".");
    }

    private ResponseEntity<?> doTest(ConnectionConfig conn) {
        Path configDir = configDir();
        // Build the URL with the config dir so TLS-only URL rewrites (e.g. Oracle TCPS / wallet
        // service URLs) are exercised by the test, not just by runtime connections.
        String jdbcUrl = conn.getJdbcUrl(configDir);
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("success", false,
                    "error", "Cannot build JDBC URL — check hostname, port, and database/service name"));
        }
        String user = conn.getUsername();
        String pass = conn.getPassword();
        DbType dbType = conn.resolvedDbType();

        // Windows integrated auth doesn't need username/password
        boolean windowsAuth = DbType.SQLSERVER == dbType && "WINDOWS".equalsIgnoreCase(conn.getRole());
        if (!windowsAuth && (user == null || user.isBlank())) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "error", "Username is required"));
        }

        // Ensure the correct JDBC driver class is loaded
        try {
            Class.forName(conn.dialect().getDriverClass());
        } catch (ClassNotFoundException e) {
            log.error("JDBC driver not found for {}: {}", dbType, conn.dialect().getDriverClass());
            return ResponseEntity.internalServerError().body(Map.of("success", false,
                    "error", "JDBC driver for " + conn.dialect().getDisplayName()
                            + " not found on classpath: " + conn.dialect().getDriverClass()));
        }

        Properties props = new Properties();
        if (!windowsAuth) {
            props.setProperty("user", user != null ? user : "");
            if (pass != null) props.setProperty("password", pass);
        }

        // DB-specific connection timeout hints
        if (dbType == DbType.ORACLE) {
            if (conn.getRole() != null && !"DEFAULT".equalsIgnoreCase(conn.getRole())) {
                props.setProperty("internal_logon", conn.getRole().toLowerCase());
            }
            props.setProperty("oracle.jdbc.loginTimeout", "10");
            props.setProperty("oracle.net.CONNECT_TIMEOUT", "10000");
        } else if (dbType == DbType.MYSQL) {
            props.setProperty("connectTimeout", "10000");
            props.setProperty("socketTimeout", "15000");
        }
        // SQL Server timeout already embedded in JDBC URL via loginTimeout parameter

        // Merge the connection's TLS settings (ssl mode, hostname validation, trust/client certs,
        // truststores, wallet) LAST so the test dials with exactly the Properties runtime will use.
        // buildSslProperties() returns an empty Properties when TLS is disabled — a no-op then.
        props.putAll(conn.buildSslProperties(configDir));

        // Cert-expiry preflight (independent of whether the socket connects): when a PEM trust or
        // client cert is configured, report how long it stays valid so a near-expiry warning
        // surfaces right in the test result line rather than as a mystery connect failure later.
        Map<String, Object> certInfo = certExpiryInfo(conn, configDir);

        try {
            long start = System.currentTimeMillis();
            try (Connection c = DriverManager.getConnection(jdbcUrl, props)) {
                long ms = System.currentTimeMillis() - start;
                String dbVersion = c.getMetaData().getDatabaseProductVersion();
                List<String> discovered = discoverSchemas(c, dbType);
                if (!discovered.isEmpty()) {
                    conn.setSchemas(discovered);
                } else if (conn.getSchemasRaw() == null && user != null) {
                    conn.setSchemas(new ArrayList<>(List.of(user.toUpperCase())));
                }
                // Stamp the successful test — the setup modal sorts its recent-connections picker on
                // this. Set on the in-memory object; callers that hold the live config persist it.
                conn.setLastTestedAt(System.currentTimeMillis());
                Map<String, Object> body = new LinkedHashMap<>();
                body.put("success", true);
                body.put("message", "Connected in " + ms + "ms");
                body.put("version", dbVersion);
                body.put("dbType", dbType.name());
                body.put("url", jdbcUrl);
                body.put("user", windowsAuth ? "(Windows auth)" : user);
                body.put("schemas", conn.getSchemas());
                body.putAll(certInfo);
                return ResponseEntity.ok(body);
            }
        } catch (Exception e) {
            String raw = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            String friendly = buildFriendlyError(dbType, jdbcUrl, raw);
            log.warn("Connection test failed [{} / {}]: {}", dbType, jdbcUrl, raw);
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("success", false);
            body.put("message", friendly);
            body.put("dbType", dbType.name());
            body.put("url", jdbcUrl);
            body.put("user", windowsAuth ? "(Windows auth)" : (user != null ? user : ""));
            // Still worth reporting an about-to-expire cert even when the connect failed — it may be
            // the very reason the handshake was rejected.
            body.putAll(certInfo);
            return ResponseEntity.ok(body);
        }
    }

    /** One {@code -----BEGIN CERTIFICATE-----}...{@code -----END CERTIFICATE-----} block (group 1 = body). */
    private static final Pattern PEM_CERT_BLOCK = Pattern.compile(
            "-----BEGIN CERTIFICATE-----(.*?)-----END CERTIFICATE-----", Pattern.DOTALL);

    /**
     * Best-effort expiry of the PEM certificates configured on a connection (trust bundle and/or
     * client certificate). Returns {@code camelCase} fields the UI renders directly:
     *
     * <ul>
     *   <li>{@code certExpiresInDays} — the soonest {@code NotAfter} across all parsed certs, in
     *       whole days (may be negative when already expired);</li>
     *   <li>{@code certExpiryWarning} — true when that soonest expiry is under 30 days.</li>
     * </ul>
     *
     * <p>Returns an empty map when no PEM cert is configured, the file is missing, or it cannot be
     * parsed — expiry reporting must never turn a working connection test into a failure. Only PEM
     * (trust/client cert) files are inspected; a JKS/PKCS12 truststore or wallet holds no readable
     * PEM here and is skipped. Certificate contents are never logged.
     */
    private Map<String, Object> certExpiryInfo(ConnectionConfig conn, Path configDir) {
        try {
            Integer soonest = null;
            for (String raw : new String[]{conn.getTrustCertPath(), conn.getClientCertPath()}) {
                if (raw == null || raw.isBlank()) continue;
                Path file = resolveTlsPath(Path.of(raw), configDir);
                if (file == null || !Files.exists(file)) continue;
                for (X509Certificate cert : readPemCertificates(file)) {
                    long days = (cert.getNotAfter().getTime() - System.currentTimeMillis()) / 86_400_000L;
                    int d = (int) days;
                    if (soonest == null || d < soonest) soonest = d;
                }
            }
            if (soonest == null) return Map.of();
            Map<String, Object> info = new LinkedHashMap<>();
            info.put("certExpiresInDays", soonest);
            info.put("certExpiryWarning", soonest < 30);
            return info;
        } catch (Exception e) {
            // Never fail the test over an unreadable/oddly-formatted cert; log the file name only.
            log.debug("Cert expiry check skipped: {}", e.getMessage());
            return Map.of();
        }
    }

    /**
     * Mirror of the (package-private) {@code TlsKeystoreConverter.resolve} for the cert-expiry
     * preflight: absolute paths pass through unchanged, relative ones are anchored at the config
     * directory and the result is absolute+normalized — the same path {@code buildSslProperties}
     * would hand the driver, so expiry is read from the file actually used at connect time.
     */
    private static Path resolveTlsPath(Path raw, Path configDir) {
        if (raw == null) return null;
        Path resolved = raw.isAbsolute() || configDir == null ? raw : configDir.resolve(raw);
        return resolved.toAbsolutePath().normalize();
    }

    /** Parse every X.509 certificate out of a PEM file (headers stripped, base64 decoded to DER). */
    private static List<X509Certificate> readPemCertificates(Path file) throws Exception {
        String pem = Files.readString(file, StandardCharsets.UTF_8);
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        List<X509Certificate> certs = new ArrayList<>();
        Matcher m = PEM_CERT_BLOCK.matcher(pem);
        while (m.find()) {
            byte[] der = Base64.getMimeDecoder().decode(m.group(1).trim());
            certs.add((X509Certificate) cf.generateCertificate(new ByteArrayInputStream(der)));
        }
        return certs;
    }

    private String buildFriendlyError(DbType dbType, String jdbcUrl, String raw) {
        switch (dbType) {
            case ORACLE:
                if (raw.contains("ORA-17800") || raw.contains("minus one from a read call"))
                    return "ORA-17800: Oracle closed the socket — SQLNET.INBOUND_CONNECT_TIMEOUT, max sessions, or firewall rate-limit. Detail: " + raw;
                if (raw.contains("ORA-12516") || raw.contains("TNS:listener could not find"))
                    return "ORA-12516: No Oracle server processes available (session limit). Detail: " + raw;
                if (raw.contains("ORA-12541") || raw.contains("TNS:no listener"))
                    return "ORA-12541: Oracle listener not running on " + jdbcUrl + ". Detail: " + raw;
                if (raw.contains("ORA-01017") || raw.contains("invalid username/password"))
                    return "ORA-01017: Invalid username or password. Detail: " + raw;
                return raw;
            case MYSQL:
                if (raw.contains("Communications link failure") || raw.contains("Connection refused"))
                    return "MySQL: Cannot reach server — check hostname and port (default 3306). Detail: " + raw;
                if (raw.contains("Access denied"))
                    return "MySQL: Access denied — check username and password. Detail: " + raw;
                if (raw.contains("Unknown database"))
                    return "MySQL: Database not found — check the database name field. Detail: " + raw;
                return raw;
            case SQLSERVER:
                if (raw.contains("Connection refused") || raw.contains("connect timed out"))
                    return "SQL Server: Cannot reach server — check hostname and port (default 1433). Detail: " + raw;
                if (raw.contains("Login failed"))
                    return "SQL Server: Login failed — check username and password (or Windows auth setting). Detail: " + raw;
                if (raw.contains("Cannot open database"))
                    return "SQL Server: Database not found — check the database name field. Detail: " + raw;
                return raw;
            case POSTGRES:
                if (raw.contains("Connection refused") || raw.contains("connect timed out"))
                    return "PostgreSQL: Cannot reach server — check hostname and port (default 5432). Detail: " + raw;
                if (raw.contains("password authentication failed") || raw.contains("role \""))
                    return "PostgreSQL: Authentication failed — check username and password. Detail: " + raw;
                if (raw.contains("database \"") && raw.contains("does not exist"))
                    return "PostgreSQL: Database not found — check the database name field. Detail: " + raw;
                return raw;
            default:
                return raw;
        }
    }

    /**
     * Discover every schema the tested connection's login can see, covering BOTH executable
     * objects (procedures/functions/packages/triggers) and data objects (tables/views/mviews/
     * sequences). Best-effort: any failure (missing catalog view, insufficient privilege) just
     * yields an empty list — discovery never fails the underlying connection test.
     */
    /**
     * Built-in/system schemas each dialect ships with, that a login's grants can incidentally
     * reach (e.g. via PUBLIC synonyms) but that no user would ever want to pick as an analysis
     * "Owner" — confirmed live: {@code oracle-free-conn}'s discovery returned {@code SYS},
     * {@code SYSTEM}, {@code CTXSYS}, {@code DVSYS}, {@code MDSYS}, {@code XDB}, {@code LBACSYS},
     * {@code GSMADMIN_INTERNAL} alongside the two real demo schemas. Excluded from the discovered
     * list shown in the UI, but NOT blocked outright — a user can still type one manually if they
     * genuinely need it (see {@code updateConnection}'s manual-override path).
     */
    private static final java.util.Set<String> SYSTEM_SCHEMAS = new java.util.HashSet<>(java.util.List.of(
            // Oracle
            "SYS", "SYSTEM", "CTXSYS", "DVSYS", "DVF", "MDSYS", "XDB", "LBACSYS",
            "GSMADMIN_INTERNAL", "OUTLN", "ORDSYS", "ORDDATA", "WMSYS", "APPQOSSYS",
            "DBSFWUSER", "GGSYS", "ANONYMOUS", "XS$NULL", "AUDSYS", "OJVMSYS", "REMOTE_SCHEDULER_AGENT",
            // MySQL (SYS shared with Oracle above; listed once)
            "MYSQL", "INFORMATION_SCHEMA", "PERFORMANCE_SCHEMA",
            // DB2
            "SYSIBM", "SYSCAT", "SYSSTAT", "SYSPUBLIC", "SYSTOOLS", "SYSIBMADM",
            // SQL Server (INFORMATION_SCHEMA shared with MySQL/Postgres above; listed once)
            "GUEST", "DB_ACCESSADMIN", "DB_BACKUPOPERATOR",
            "DB_DATAREADER", "DB_DATAWRITER", "DB_DDLADMIN", "DB_DENYDATAREADER",
            "DB_DENYDATAWRITER", "DB_OWNER", "DB_SECURITYADMIN",
            // Postgres
            "PG_CATALOG", "PG_TOAST"));

    private List<String> discoverSchemas(Connection c, DbType dbType) {
        String sql;
        switch (dbType) {
            case ORACLE:
                sql = "SELECT DISTINCT owner FROM all_objects WHERE object_type IN "
                        + "('PROCEDURE','FUNCTION','PACKAGE','PACKAGE BODY','TRIGGER','TABLE','VIEW','MATERIALIZED VIEW','SEQUENCE')";
                break;
            case MYSQL:
                sql = "SELECT DISTINCT table_schema AS s FROM information_schema.routines "
                        + "UNION SELECT DISTINCT table_schema AS s FROM information_schema.tables";
                break;
            case DB2:
                sql = "SELECT DISTINCT OWNER AS S FROM SYSCAT.ROUTINES "
                        + "UNION SELECT DISTINCT TABSCHEMA AS S FROM SYSCAT.TABLES "
                        + "UNION SELECT DISTINCT SEQSCHEMA AS S FROM SYSCAT.SEQUENCES";
                break;
            case SQLSERVER:
                sql = "SELECT DISTINCT s.name AS sch FROM sys.objects o JOIN sys.schemas s ON o.schema_id = s.schema_id "
                        + "WHERE o.type IN ('P','FN','TR','U','V') "
                        + "UNION SELECT DISTINCT s.name AS sch FROM sys.sequences q JOIN sys.schemas s ON q.schema_id = s.schema_id";
                break;
            case POSTGRES:
                sql = "SELECT DISTINCT n.nspname AS sch FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid "
                        + "UNION SELECT DISTINCT n.nspname AS sch FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relkind IN ('r','v','m') "
                        + "UNION SELECT DISTINCT schemaname AS sch FROM pg_sequences";
                break;
            default:
                return List.of();
        }
        List<String> schemas = new ArrayList<>();
        try (java.sql.Statement st = c.createStatement();
             java.sql.ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String s = rs.getString(1);
                if (s != null && !s.isBlank()) {
                    String up = s.trim().toUpperCase();
                    if (!schemas.contains(up) && !SYSTEM_SCHEMAS.contains(up)) schemas.add(up);
                }
            }
            Collections.sort(schemas);
        } catch (Exception e) {
            log.warn("Schema discovery query failed for {}: {}", dbType, e.getMessage());
            return List.of();
        }
        return schemas;
    }

    private void save() {
        try {
            ConfigLoader.saveToFile(config, configFilePath);
        } catch (Exception e) {
            log.error("Failed to save config to {}: {}", configFilePath, e.getMessage());
        }
    }
}
