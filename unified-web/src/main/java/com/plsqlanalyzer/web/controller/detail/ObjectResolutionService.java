package com.plsqlanalyzer.web.controller.detail;

import com.plsqlanalyzer.analyzer.service.AnalysisService;
import com.plsqlanalyzer.config.DbUserConfig;
import com.plsqlanalyzer.config.EnvironmentConfig;
import com.plsqlanalyzer.config.PlsqlConfig;
import com.plsqlanalyzer.parser.service.OracleDictionaryService;
import com.plsqlanalyzer.parser.service.OracleDictionaryService.ObjectRecord;
import com.plsqlanalyzer.web.browse.DbBrowseProvider;
import com.plsqlanalyzer.web.browse.DbBrowseProviderFactory;
import com.plsqlanalyzer.web.service.DbSourceFetcher;
import com.plsqlanalyzer.web.util.DbDialectSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Object/schema resolution + non-Oracle guard helpers shared by the trigger services. */
@Service
public class ObjectResolutionService {

    private static final Logger log = LoggerFactory.getLogger(ObjectResolutionService.class);

    private final AnalysisService analysisService;
    private final PlsqlConfig config;
    private final DbSourceFetcher dbFetcher;

    public ObjectResolutionService(AnalysisService analysisService, PlsqlConfig config,
                                   DbSourceFetcher dbFetcher) {
        this.analysisService = analysisService;
        this.config = config;
        this.dbFetcher = dbFetcher;
    }

    /** Search for an object across all configured schemas, returning its schema and type. */
    public ResolvedObject findObjectInAllSchemas(String objectName) {
        for (DbUserConfig user : config.getDbUsers()) {
            try (Connection conn = dbFetcher.getConnection(user)) {
                ObjectRecord obj = analysisService.getDictionaryService()
                        .getObjectInfo(conn, user.getUsername(), objectName.toUpperCase());
                if (obj != null) {
                    log.info("Found {} in schema {} as {}", objectName, user.getUsername(), obj.objectType());
                    return new ResolvedObject(user.getUsername().toUpperCase(), obj.objectType());
                }
            } catch (Exception e) {
                log.debug("Error checking {} in {}: {}", objectName, user.getUsername(), e.getMessage());
            }
        }
        return null;
    }

    /**
     * Normalize object type — convert "PACKAGE BODY" to "PACKAGE" for analysis purposes,
     * since we always want to analyze the body for call tracing.
     */
    public String normalizeObjectType(String objectType) {
        if (objectType == null) return "PACKAGE";
        return switch (objectType.toUpperCase()) {
            case "PACKAGE BODY", "PACKAGE" -> "PACKAGE";
            case "PROCEDURE" -> "PROCEDURE";
            case "FUNCTION" -> "FUNCTION";
            case "TRIGGER" -> "TRIGGER";
            case "VIEW" -> "VIEW";
            case "MATERIALIZED_VIEW" -> "MATERIALIZED_VIEW";
            case "SEQUENCE" -> "SEQUENCE";
            case "INDEX" -> "INDEX";
            case "TYPE" -> "TYPE";
            case "TYPE_BODY" -> "TYPE_BODY";
            case "DOMAIN" -> "DOMAIN";
            case "ALTER_TABLE" -> "ALTER_TABLE";
            default -> "PACKAGE";
        };
    }

    public DbUserConfig findUser(String username) {
        return findUserIn(config.getDbUsers(), username);
    }

    public DbUserConfig findUserIn(List<DbUserConfig> users, String username) {
        return users.stream()
                .filter(u -> u.getUsername().equalsIgnoreCase(username))
                .findFirst()
                .orElse(null);
    }

    /**
     * Returns a graceful guard body when the effective connection is not Oracle, else null.
     */
    public Map<String, Object> nonOracleAnalyzeGuard(String projectName, String envName) {
        String url = null;
        if (projectName != null && !projectName.isBlank() && envName != null && !envName.isBlank()) {
            EnvironmentConfig env = config.resolveEnvironment(projectName, envName);
            if (env != null && env.getJdbcUrl() != null) url = env.getJdbcUrl();
        }
        if (url == null) url = dbFetcher.getJdbcUrl();
        if (DbDialectSupport.isOracle(url)) return null;

        String dialect = DbDialectSupport.dialectFromJdbcUrl(url);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", DbDialectSupport.unsupportedMessage("Static PL/SQL analysis", dialect)
                + ". Use crawler analysis instead.");
        body.put("unsupported", true);
        body.put("dialect", dialect);
        return body;
    }

    /**
     * Smart object finder: given just a name (or schema.name / pkg.proc / schema.pkg.proc),
     * search across ALL configured schemas to locate the object — no dialect ever falls back
     * to a hardcoded/guessed owner.
     *
     * <p>Extracted from {@code DatabaseController.findObject()} (the {@code GET /find/{objectInput}}
     * endpoint) so {@code AnalysisService} can resolve an entry point's real owner/objectType
     * itself instead of defaulting one, e.g. the old hardcoded {@code "CUSTOMER"} fallback in
     * {@code discoverPackageEntries}/{@code TriggerDiscoverer}. Behaviour is unchanged from the
     * original inline controller code — same Oracle 1/2/3-part search, same non-Oracle
     * {@link DbBrowseProvider#findObject} delegation.
     *
     * @return a map with {@code found} (boolean) and, when found, {@code schema}/{@code
     *         objectName}/{@code objectType}/{@code procedureName}; when not found, {@code
     *         found=false} plus {@code input} and {@code message}/{@code unsupported} as
     *         appropriate.
     */
    public Map<String, Object> findObjectSmart(String objectInput, String projectName, String envName) {
        if (!isOracleBrowse(projectName, envName)) {
            DbBrowseProvider provider = browseProvider(projectName, envName);
            if (provider == null) {
                Map<String, Object> body = browseUnsupported(projectName, envName);
                body.put("found", false);
                body.put("input", objectInput);
                return body;
            }
            try (Connection conn = openBrowseConnection(projectName, envName)) {
                return provider.findObject(conn, objectInput);
            } catch (Exception e) {
                log.error("Browse findObject failed: {}", e.getMessage());
                Map<String, Object> nf = new LinkedHashMap<>();
                nf.put("found", false);
                nf.put("input", objectInput);
                return nf;
            }
        }

        String input = objectInput.toUpperCase().trim();
        String[] parts = input.split("\\.", -1);
        List<DbUserConfig> users = resolveUsers(projectName, envName);
        String jdbcUrl = resolveJdbcUrl(projectName, envName);
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
                            return result;
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
                            return result;
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
                        return result;
                    }
                } else if (parts.length == 1) {
                    // Just a name — search all schemas
                    ObjectRecord obj = dictService.getObjectInfo(conn, schema, parts[0]);
                    if (obj != null) {
                        result.put("found", true);
                        result.put("schema", schema);
                        result.put("objectName", parts[0]);
                        result.put("objectType", obj.objectType());
                        return result;
                    }
                }
            } catch (Exception e) {
                log.debug("Error searching {} for {}: {}", user.getUsername(), input, e.getMessage());
            }
        }

        result.put("found", false);
        result.put("input", input);
        result.put("message", "Object not found in any configured schema");
        return result;
    }

    /** True when the effective connection is Oracle (the only dialect not routed through a {@link DbBrowseProvider}). */
    public boolean isOracleBrowse(String projectName, String envName) {
        return DbDialectSupport.isOracle(effectiveJdbcUrl(projectName, envName));
    }

    /** Resolve a browse provider for the effective dialect, or null if unsupported. */
    public DbBrowseProvider browseProvider(String projectName, String envName) {
        String dialect = DbDialectSupport.dialectFromJdbcUrl(effectiveJdbcUrl(projectName, envName));
        return DbBrowseProviderFactory.get(dialect, dbFetcher);
    }

    /** Open a connection for a non-Oracle browse request using the first resolved user. */
    public Connection openBrowseConnection(String projectName, String envName) throws SQLException {
        List<DbUserConfig> users = resolveUsers(projectName, envName);
        if (users.isEmpty()) throw new SQLException("No database user configured");
        DbUserConfig user = users.get(0);
        String jdbcUrl = resolveJdbcUrl(projectName, envName);
        return jdbcUrl != null ? dbFetcher.getConnection(user, jdbcUrl) : dbFetcher.getConnection(user);
    }

    public Map<String, Object> browseUnsupported(String projectName, String envName) {
        String dialect = DbDialectSupport.dialectFromJdbcUrl(effectiveJdbcUrl(projectName, envName));
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", DbDialectSupport.unsupportedMessage("Database browse", dialect));
        body.put("unsupported", true);
        return body;
    }

    private String effectiveJdbcUrl(String projectName, String envName) {
        if (projectName != null && !projectName.isBlank() && envName != null && !envName.isBlank()) {
            EnvironmentConfig env = config.resolveEnvironment(projectName, envName);
            if (env != null && env.getJdbcUrl() != null) return env.getJdbcUrl();
        }
        return dbFetcher.getJdbcUrl();
    }

    public List<DbUserConfig> resolveUsers(String projectName, String envName) {
        if (projectName != null && !projectName.isBlank() && envName != null && !envName.isBlank()) {
            EnvironmentConfig env = config.resolveEnvironment(projectName, envName);
            if (env != null && !env.getUsers().isEmpty()) {
                return env.getUsers();
            }
        }
        return config.getDbUsers();
    }

    public String resolveJdbcUrl(String projectName, String envName) {
        if (projectName != null && !projectName.isBlank() && envName != null && !envName.isBlank()) {
            EnvironmentConfig env = config.resolveEnvironment(projectName, envName);
            if (env != null && env.getJdbcUrl() != null) {
                return env.getJdbcUrl();
            }
        }
        return dbFetcher.getJdbcUrl();
    }

    /** Holds a resolved schema + object type pair. */
    public static class ResolvedObject {
        public final String schema;
        public final String objectType;

        public ResolvedObject(String schema, String objectType) {
            this.schema = schema;
            this.objectType = objectType;
        }
    }
}
