package com.plsqlanalyzer.web.controller;

import com.plsqlanalyzer.analyzer.service.AnalysisService;
import com.plsqlanalyzer.config.DbUserConfig;
import com.plsqlanalyzer.config.EnvironmentConfig;
import com.plsqlanalyzer.config.PlsqlConfig;
import com.plsqlanalyzer.parser.service.OracleDictionaryService;
import com.plsqlanalyzer.parser.service.OracleDictionaryService.*;
import com.plsqlanalyzer.web.service.DbSourceFetcher;
import com.plsqlanalyzer.web.service.PersistenceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.Connection;
import java.util.*;

@RestController("plsqlDatabaseController")
@RequestMapping("/api/plsql/db")
public class DatabaseController {

    private static final Logger log = LoggerFactory.getLogger(DatabaseController.class);

    private final PlsqlConfig config;
    private final DbSourceFetcher dbFetcher;
    private final AnalysisService analysisService;
    private final PersistenceService persistenceService;

    // Cache fetched sources so source viewer can access them
    private final Map<String, String> fetchedSources = new LinkedHashMap<>();

    public DatabaseController(PlsqlConfig config, DbSourceFetcher dbFetcher,
                              AnalysisService analysisService, PersistenceService persistenceService) {
        this.config = config;
        this.dbFetcher = dbFetcher;
        this.analysisService = analysisService;
        this.persistenceService = persistenceService;
    }

    @GetMapping("/users")
    public ResponseEntity<List<Map<String, String>>> listUsers(
            @RequestParam(required = false) String project,
            @RequestParam(required = false) String env) {
        List<DbUserConfig> resolved = resolveUsers(project, env);
        List<Map<String, String>> users = new ArrayList<>();
        for (DbUserConfig u : resolved) {
            Map<String, String> m = new LinkedHashMap<>();
            m.put("username", u.getUsername());
            m.put("description", u.getDescription());
            users.add(m);
        }
        return ResponseEntity.ok(users);
    }

    @PostMapping("/test")
    public ResponseEntity<Map<String, Object>> testConnection(@RequestBody Map<String, String> body) {
        String username = body.get("username");
        DbUserConfig user = findUser(username);
        if (user == null) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "error", "User not found in config: " + username));
        }

        boolean ok = dbFetcher.testConnection(user);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("success", ok);
        result.put("username", username);
        result.put("jdbcUrl", config.getJdbcUrl());
        return ResponseEntity.ok(result);
    }

    @GetMapping("/objects/{username}")
    public ResponseEntity<List<Map<String, String>>> listObjects(@PathVariable String username) {
        DbUserConfig user = findUser(username);
        if (user == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(dbFetcher.listObjects(user));
    }

    @GetMapping("/source/{username}/{objectName}")
    public ResponseEntity<Map<String, Object>> fetchSource(
            @PathVariable String username,
            @PathVariable String objectName,
            @RequestParam(defaultValue = "PACKAGE BODY") String objectType) {

        // Find a DB connection: prefer the exact user, otherwise use any available
        // connection and query ALL_SOURCE with the target owner (cross-schema view)
        DbUserConfig user = findUser(username);
        if (user == null) {
            // Owner not in config — use any available connection to query ALL_SOURCE
            if (!config.getDbUsers().isEmpty()) {
                user = config.getDbUsers().get(0);
                log.info("Owner {} not in config, using {} connection to query ALL_SOURCE", username, user.getUsername());
            } else {
                return ResponseEntity.notFound().build();
            }
        }

        String source = dbFetcher.fetchSource(user, username.toUpperCase(), objectName.toUpperCase(), objectType);
        if (source == null || source.isBlank()) {
            return ResponseEntity.notFound().build();
        }

        // Cache source for source viewer
        String key = username.toUpperCase() + "." + objectName.toUpperCase() + "." + objectType.replace(" ", "_");
        fetchedSources.put(key, source);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("owner", username.toUpperCase());
        result.put("objectName", objectName.toUpperCase());
        result.put("objectType", objectType);
        result.put("content", source);
        result.put("lineCount", source.split("\n").length);
        result.put("sourceKey", key);
        return ResponseEntity.ok(result);
    }

    /**
     * Get cached source content for the source viewer (DB-fetched sources are in memory).
     */
    @GetMapping("/cached-source")
    public ResponseEntity<Map<String, Object>> getCachedSource(
            @RequestParam String key,
            @RequestParam(required = false) Integer line) {

        String content = fetchedSources.get(key);
        if (content == null) return ResponseEntity.notFound().build();

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("path", key);
        response.put("fileName", key);
        response.put("content", content);
        response.put("lineCount", content.split("\n").length);
        if (line != null) {
            response.put("highlightLine", line);
        }
        return ResponseEntity.ok(response);
    }

    /**
     * List packages with procedure counts for a schema.
     */
    /** Return stored connection info for a project/environment. */
    @GetMapping("/connections")
    public ResponseEntity<Map<String, Object>> getConnections(
            @RequestParam(required = false) String project,
            @RequestParam(required = false) String env) {
        Map<String, Object> result = new LinkedHashMap<>();
        String jdbcUrl = resolveJdbcUrl(project, env);
        result.put("jdbcUrl", jdbcUrl);
        List<DbUserConfig> users = resolveUsers(project, env);
        List<String> schemas = new ArrayList<>();
        for (DbUserConfig u : users) schemas.add(u.getUsername().toUpperCase());
        result.put("schemas", schemas);
        if (project != null) result.put("project", project);
        if (env != null) result.put("environment", env);
        result.put("available", !schemas.isEmpty());
        return ResponseEntity.ok(result);
    }

    @GetMapping("/packages/{username}")
    public ResponseEntity<List<Map<String, Object>>> listPackages(
            @PathVariable String username,
            @RequestParam(required = false) String project,
            @RequestParam(required = false) String env) {
        DbUserConfig user = findUser(username);
        if (user == null) return ResponseEntity.notFound().build();

        String jdbcUrl = resolveJdbcUrl(project, env);
        OracleDictionaryService dictService = analysisService.getDictionaryService();
        List<Map<String, Object>> packages = new ArrayList<>();

        try (Connection conn = jdbcUrl != null ? dbFetcher.getConnection(user, jdbcUrl) : dbFetcher.getConnection(user)) {
            List<ObjectRecord> objects = dictService.listObjects(conn, username);

            // Group by package name
            Map<String, Map<String, Object>> pkgMap = new LinkedHashMap<>();
            for (ObjectRecord obj : objects) {
                if ("PACKAGE BODY".equals(obj.objectType()) || "PACKAGE".equals(obj.objectType())) {
                    Map<String, Object> pkg = pkgMap.computeIfAbsent(obj.objectName(), k -> {
                        Map<String, Object> m = new LinkedHashMap<>();
                        m.put("objectName", obj.objectName());
                        m.put("objectType", "PACKAGE");
                        m.put("status", obj.status());
                        m.put("hasBody", false);
                        m.put("procCount", 0);
                        return m;
                    });
                    if ("PACKAGE BODY".equals(obj.objectType())) {
                        pkg.put("hasBody", true);
                    }
                }
            }

            // Get proc counts for each package
            for (Map<String, Object> pkg : pkgMap.values()) {
                try {
                    List<ProcedureRecord> procs = dictService.listProcedures(
                            conn, username, (String) pkg.get("objectName"));
                    pkg.put("procCount", procs.size());
                } catch (Exception e) {
                    log.debug("Could not list procs for {}: {}", pkg.get("objectName"), e.getMessage());
                }
            }

            packages.addAll(pkgMap.values());

            // Also add standalone procedures and functions
            for (ObjectRecord obj : objects) {
                if ("PROCEDURE".equals(obj.objectType()) || "FUNCTION".equals(obj.objectType())
                        || "TRIGGER".equals(obj.objectType())) {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("objectName", obj.objectName());
                    m.put("objectType", obj.objectType());
                    m.put("status", obj.status());
                    m.put("hasBody", true);
                    m.put("procCount", 1);
                    packages.add(m);
                }
            }
        } catch (Exception e) {
            log.error("Failed to list packages for {}: {}", username, e.getMessage());
        }

        return ResponseEntity.ok(packages);
    }

    /**
     * List procedures/functions within a specific package.
     */
    @GetMapping("/package/{username}/{packageName}")
    public ResponseEntity<List<Map<String, Object>>> listPackageProcs(
            @PathVariable String username, @PathVariable String packageName,
            @RequestParam(required = false) String project,
            @RequestParam(required = false) String env) {
        DbUserConfig user = findUser(username);
        if (user == null) return ResponseEntity.notFound().build();

        String jdbcUrl = resolveJdbcUrl(project, env);
        OracleDictionaryService dictService = analysisService.getDictionaryService();
        List<Map<String, Object>> procs = new ArrayList<>();

        try (Connection conn = jdbcUrl != null ? dbFetcher.getConnection(user, jdbcUrl) : dbFetcher.getConnection(user)) {
            List<ProcedureRecord> records = dictService.listProcedures(conn, username, packageName);
            for (ProcedureRecord rec : records) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("procedureName", rec.procedureName());
                m.put("overload", rec.overload());
                m.put("subprogramId", rec.subprogramId());
                procs.add(m);
            }
        } catch (Exception e) {
            log.error("Failed to list procs for {}.{}: {}", username, packageName, e.getMessage());
        }

        return ResponseEntity.ok(procs);
    }

    /**
     * Smart object finder: given just a name (or schema.name or pkg.proc),
     * search across ALL configured schemas to locate the object.
     */
    @GetMapping("/find/{objectInput}")
    public ResponseEntity<Map<String, Object>> findObject(
            @PathVariable String objectInput,
            @RequestParam(required = false) String project,
            @RequestParam(required = false) String env) {
        String input = objectInput.toUpperCase().trim();
        String[] parts = input.split("\\.", -1);

        String jdbcUrl = resolveJdbcUrl(project, env);
        List<DbUserConfig> users = resolveUsers(project, env);
        OracleDictionaryService dictService = analysisService.getDictionaryService();
        Map<String, Object> result = new LinkedHashMap<>();

        for (DbUserConfig user : users) {
            try (Connection conn = jdbcUrl != null ? dbFetcher.getConnection(user, jdbcUrl) : dbFetcher.getConnection(user)) {
                String schema = user.getUsername().toUpperCase();

                if (parts.length == 3) {
                    // SCHEMA.PKG.PROC
                    if (schema.equals(parts[0])) {
                        ObjectRecord obj = dictService.getObjectInfo(conn, parts[0], parts[1]);
                        if (obj != null) {
                            result.put("found", true);
                            result.put("schema", parts[0]);
                            result.put("objectName", parts[1]);
                            result.put("objectType", obj.objectType());
                            result.put("procedureName", parts[2]);
                            return ResponseEntity.ok(result);
                        }
                    }
                } else if (parts.length == 2) {
                    // Could be SCHEMA.OBJECT or PKG.PROC
                    // First check: is parts[0] a schema name?
                    if (schema.equals(parts[0])) {
                        ObjectRecord obj = dictService.getObjectInfo(conn, parts[0], parts[1]);
                        if (obj != null) {
                            result.put("found", true);
                            result.put("schema", parts[0]);
                            result.put("objectName", parts[1]);
                            result.put("objectType", obj.objectType());
                            return ResponseEntity.ok(result);
                        }
                    }
                    // Second check: is parts[0] an object in this schema? (PKG.PROC pattern)
                    ObjectRecord obj = dictService.getObjectInfo(conn, schema, parts[0]);
                    if (obj != null) {
                        result.put("found", true);
                        result.put("schema", schema);
                        result.put("objectName", parts[0]);
                        result.put("objectType", obj.objectType());
                        result.put("procedureName", parts[1]);
                        return ResponseEntity.ok(result);
                    }
                } else if (parts.length == 1) {
                    // Just a name — search all schemas
                    ObjectRecord obj = dictService.getObjectInfo(conn, schema, parts[0]);
                    if (obj != null) {
                        result.put("found", true);
                        result.put("schema", schema);
                        result.put("objectName", parts[0]);
                        result.put("objectType", obj.objectType());
                        return ResponseEntity.ok(result);
                    }
                }
            } catch (Exception e) {
                log.debug("Error searching {} for {}: {}", user.getUsername(), input, e.getMessage());
            }
        }

        result.put("found", false);
        result.put("input", input);
        result.put("message", "Object not found in any configured schema");
        return ResponseEntity.ok(result);
    }

    /**
     * Get table structure (columns, types, constraints) for a table.
     * Read-only metadata from ALL_TAB_COLUMNS.
     */
    @GetMapping("/table-info/{tableName}")
    public ResponseEntity<Map<String, Object>> getTableInfo(
            @PathVariable String tableName,
            @RequestParam(required = false) String schema,
            @RequestParam(required = false) String project,
            @RequestParam(required = false) String env) {

        String jdbcUrl = resolveJdbcUrl(project, env);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("tableName", tableName.toUpperCase());

        List<DbUserConfig> orderedUsers = new ArrayList<>(resolveUsers(project, env));
        if (schema != null && !schema.isBlank()) {
            orderedUsers.sort((a, b) -> {
                boolean aMatch = a.getUsername().equalsIgnoreCase(schema);
                boolean bMatch = b.getUsername().equalsIgnoreCase(schema);
                return Boolean.compare(bMatch, aMatch);
            });
        }
        for (DbUserConfig user : orderedUsers) {
            try (Connection conn = jdbcUrl != null ? dbFetcher.getConnection(user, jdbcUrl) : dbFetcher.getConnection(user)) {

                // Search all_tables across all visible schemas for this table
                String findSql = "SELECT owner FROM all_tables WHERE table_name = ? AND ROWNUM = 1";
                String foundOwner = null;
                boolean isView = false;
                try (var ps = conn.prepareStatement(findSql)) {
                    ps.setString(1, tableName.toUpperCase());
                    var rs = ps.executeQuery();
                    if (rs.next()) {
                        foundOwner = rs.getString("owner");
                    }
                }
                if (foundOwner == null) {
                    String findViewSql = "SELECT owner FROM all_views WHERE view_name = ? AND ROWNUM = 1";
                    try (var ps = conn.prepareStatement(findViewSql)) {
                        ps.setString(1, tableName.toUpperCase());
                        var rs = ps.executeQuery();
                        if (rs.next()) {
                            foundOwner = rs.getString("owner");
                            isView = true;
                        }
                    }
                }
                if (foundOwner == null) continue;

                String owner = foundOwner;
                if (isView) result.put("isView", true);
                result.put("schema", owner);

                // Fetch columns
                String colSql = """
                    SELECT column_name, data_type, data_length, data_precision, data_scale,
                           nullable, column_id, data_default
                    FROM all_tab_columns
                    WHERE owner = ? AND table_name = ?
                    ORDER BY column_id
                    """;
                List<Map<String, Object>> columns = new ArrayList<>();
                try (var ps = conn.prepareStatement(colSql)) {
                    ps.setString(1, owner);
                    ps.setString(2, tableName.toUpperCase());
                    var rs = ps.executeQuery();
                    while (rs.next()) {
                        Map<String, Object> col = new LinkedHashMap<>();
                        col.put("columnName", rs.getString("column_name"));
                        col.put("dataType", rs.getString("data_type"));
                        col.put("dataLength", rs.getInt("data_length"));
                        col.put("dataPrecision", rs.getObject("data_precision"));
                        col.put("dataScale", rs.getObject("data_scale"));
                        col.put("nullable", "Y".equals(rs.getString("nullable")));
                        col.put("columnId", rs.getInt("column_id"));
                        col.put("dataDefault", rs.getString("data_default"));
                        columns.add(col);
                    }
                }
                result.put("columns", columns);
                result.put("columnCount", columns.size());

                // Fetch constraints (PK, FK, UNIQUE)
                String conSql = """
                    SELECT c.constraint_name, c.constraint_type, cc.column_name, cc.position,
                           c.r_constraint_name
                    FROM all_constraints c
                    JOIN all_cons_columns cc ON c.owner = cc.owner
                        AND c.constraint_name = cc.constraint_name
                    WHERE c.owner = ? AND c.table_name = ?
                    ORDER BY c.constraint_type, c.constraint_name, cc.position
                    """;
                List<Map<String, Object>> constraints = new ArrayList<>();
                try (var ps = conn.prepareStatement(conSql)) {
                    ps.setString(1, owner);
                    ps.setString(2, tableName.toUpperCase());
                    var rs = ps.executeQuery();
                    while (rs.next()) {
                        Map<String, Object> con = new LinkedHashMap<>();
                        con.put("constraintName", rs.getString("constraint_name"));
                        con.put("constraintType", rs.getString("constraint_type"));
                        con.put("columnName", rs.getString("column_name"));
                        con.put("position", rs.getInt("position"));
                        con.put("refConstraint", rs.getString("r_constraint_name"));
                        constraints.add(con);
                    }
                }
                result.put("constraints", constraints);

                // Fetch indexes
                String idxSql = """
                    SELECT i.index_name, i.uniqueness, ic.column_name, ic.column_position
                    FROM all_indexes i
                    JOIN all_ind_columns ic ON i.owner = ic.index_owner AND i.index_name = ic.index_name
                    WHERE i.table_owner = ? AND i.table_name = ?
                    ORDER BY i.index_name, ic.column_position
                    """;
                List<Map<String, Object>> indexes = new ArrayList<>();
                try (var ps = conn.prepareStatement(idxSql)) {
                    ps.setString(1, owner);
                    ps.setString(2, tableName.toUpperCase());
                    var rs = ps.executeQuery();
                    while (rs.next()) {
                        Map<String, Object> idx = new LinkedHashMap<>();
                        idx.put("indexName", rs.getString("index_name"));
                        idx.put("uniqueness", rs.getString("uniqueness"));
                        idx.put("columnName", rs.getString("column_name"));
                        idx.put("position", rs.getInt("column_position"));
                        indexes.add(idx);
                    }
                }
                result.put("indexes", indexes);

                result.put("found", true);
                return ResponseEntity.ok(result);
            } catch (Exception e) {
                log.debug("Error fetching table info for {} in {}: {}", tableName, user.getUsername(), e.getMessage());
            }
        }

        result.put("found", false);
        return ResponseEntity.ok(result);
    }

    /**
     * Execute a read-only SELECT query against the database.
     * Only SELECT statements are allowed (no DML/DDL).
     * Query can start with SELECT or WITH (CTEs).
     */
    @PostMapping("/query")
    public ResponseEntity<Map<String, Object>> executeQuery(@RequestBody Map<String, String> body) {
        String sql = body.get("sql");
        String schema = body.get("schema");

        if (sql == null || sql.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "SQL query is required"));
        }

        // Security: only allow SELECT/WITH statements
        String trimmed = sql.trim().toUpperCase();
        if (!trimmed.startsWith("SELECT") && !trimmed.startsWith("WITH")) {
            return ResponseEntity.badRequest().body(Map.of("error", "Only SELECT queries are allowed (can start with SELECT or WITH)"));
        }

        // Block any DML/DDL keywords that might be embedded
        String[] forbidden = {"INSERT ", "UPDATE ", "DELETE ", "DROP ", "CREATE ", "ALTER ",
                "TRUNCATE ", "GRANT ", "REVOKE ", "EXECUTE ", "MERGE "};
        for (String kw : forbidden) {
            // Check if forbidden keyword appears outside of string literals (rough check)
            if (trimmed.contains(kw) && !isInsideStringLiteral(sql, trimmed.indexOf(kw))) {
                return ResponseEntity.badRequest().body(Map.of("error", "Only read-only SELECT queries allowed. Found: " + kw.trim()));
            }
        }

        // Find a user to execute the query — prefer project/environment, fallback to schema name
        String project = body.get("project");
        String environment = body.get("environment");
        DbUserConfig user = null;
        List<DbUserConfig> envUsers = resolveUsers(project, environment);
        if (schema != null && !schema.isBlank()) {
            user = envUsers.stream()
                    .filter(u -> u.getUsername().equalsIgnoreCase(schema))
                    .findFirst().orElse(null);
            if (user == null) user = findUser(schema);
        }
        if (user == null && !envUsers.isEmpty()) {
            user = envUsers.get(0);
        }
        if (user == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "No database user configured"));
        }

        String execJdbcUrl = resolveJdbcUrl(project, environment);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("sql", sql);
        result.put("schema", user.getUsername());

        try (Connection conn = execJdbcUrl != null ? dbFetcher.getConnection(user, execJdbcUrl) : dbFetcher.getConnection(user)) {
            // Set read-only and limit results
            conn.setReadOnly(true);

            try (var stmt = conn.createStatement()) {
                stmt.setMaxRows(500); // Limit to 500 rows
                stmt.setQueryTimeout(30); // 30 second timeout

                var rs = stmt.executeQuery(sql);
                var meta = rs.getMetaData();
                int colCount = meta.getColumnCount();

                // Column headers
                List<Map<String, String>> columns = new ArrayList<>();
                for (int i = 1; i <= colCount; i++) {
                    Map<String, String> col = new LinkedHashMap<>();
                    col.put("name", meta.getColumnName(i));
                    col.put("type", meta.getColumnTypeName(i));
                    columns.add(col);
                }
                result.put("columns", columns);

                // Rows
                List<List<Object>> rows = new ArrayList<>();
                int rowCount = 0;
                while (rs.next() && rowCount < 500) {
                    List<Object> row = new ArrayList<>();
                    for (int i = 1; i <= colCount; i++) {
                        Object val = rs.getObject(i);
                        row.add(val != null ? val.toString() : null);
                    }
                    rows.add(row);
                    rowCount++;
                }
                result.put("rows", rows);
                result.put("rowCount", rowCount);
                result.put("truncated", rowCount >= 500);
            }

            result.put("success", true);
        } catch (Exception e) {
            result.put("success", false);
            result.put("error", e.getMessage());
        }

        return ResponseEntity.ok(result);
    }

    /**
     * Rough check if a position in SQL is inside a string literal.
     */
    private boolean isInsideStringLiteral(String sql, int pos) {
        boolean inString = false;
        for (int i = 0; i < pos && i < sql.length(); i++) {
            if (sql.charAt(i) == '\'') inString = !inString;
        }
        return inString;
    }

    private DbUserConfig findUser(String username) {
        return config.getDbUsers().stream()
                .filter(u -> u.getUsername().equalsIgnoreCase(username))
                .findFirst()
                .orElse(null);
    }

    private List<DbUserConfig> resolveUsers(String projectName, String envName) {
        if (projectName != null && !projectName.isBlank() && envName != null && !envName.isBlank()) {
            EnvironmentConfig env = config.resolveEnvironment(projectName, envName);
            if (env != null && !env.getUsers().isEmpty()) {
                return env.getUsers();
            }
        }
        return config.getDbUsers();
    }

    private String resolveJdbcUrl(String projectName, String envName) {
        if (projectName != null && !projectName.isBlank() && envName != null && !envName.isBlank()) {
            EnvironmentConfig env = config.resolveEnvironment(projectName, envName);
            if (env != null && env.getJdbcUrl() != null) {
                return env.getJdbcUrl();
            }
        }
        return dbFetcher.getJdbcUrl();
    }
}
