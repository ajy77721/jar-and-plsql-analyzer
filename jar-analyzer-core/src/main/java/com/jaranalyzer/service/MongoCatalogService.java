package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts MongoDB connection info from JAR config files (application.yml/properties).
 * Stores catalogs on disk for collection verification during analysis.
 * No live MongoDB connections are made — file parsing only.
 */
@Service
public class MongoCatalogService {

    private static final Logger log = LoggerFactory.getLogger(MongoCatalogService.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    /** Profile priority: UAT first, then local, then default. */
    private static final List<String> CONFIG_PRIORITY = List.of(
            "application-uat.properties", "application-uat.yml", "application-uat.yaml",
            "application-local.properties", "application-local.yml", "application-local.yaml",
            "application.properties", "application.yml", "application.yaml",
            "application-dev.properties", "application-dev.yml",
            "application-prod.properties", "application-prod.yml"
    );

    /** Config file names to extract from JARs. */
    static final List<String> CONFIG_FILE_NAMES = List.of(
            "application.yml", "application.yaml", "application.properties",
            "application-uat.yml", "application-uat.yaml", "application-uat.properties",
            "application-local.yml", "application-local.yaml", "application-local.properties",
            "application-dev.yml", "application-dev.yaml", "application-dev.properties",
            "application-dev1.properties", "application-qa1.properties",
            "application-it.properties", "application-sldev.properties",
            "application-test.properties",
            "application-prod.yml", "application-prod.yaml", "application-prod.properties"
    );

    private final JarDataPaths jarDataPaths;

    public MongoCatalogService(JarDataPaths jarDataPaths) {
        this.jarDataPaths = jarDataPaths;
    }

    // ========== Config Properties (for @Value resolution) ==========

    /**
     * Parse ALL properties from extracted config files into a flat key→value map.
     * Merges all .properties files (profile priority order) so @Value keys resolve.
     */
    public static Map<String, String> parseAllProperties(Map<String, String> configFiles) {
        Map<String, String> result = new LinkedHashMap<>();
        // Process in reverse priority so higher-priority files overwrite lower
        List<String> reversed = new ArrayList<>(CONFIG_PRIORITY);
        Collections.reverse(reversed);
        for (String name : reversed) {
            String content = configFiles.get(name);
            if (content == null) continue;
            if (name.endsWith(".properties")) {
                try {
                    Properties props = new Properties();
                    props.load(new StringReader(content));
                    for (String key : props.stringPropertyNames()) {
                        result.put(key, props.getProperty(key));
                    }
                } catch (Exception e) {
                    log.debug("Failed to parse {}: {}", name, e.getMessage());
                }
            } else if (name.endsWith(".yml") || name.endsWith(".yaml")) {
                try {
                    Yaml yaml = new Yaml();
                    Object doc = yaml.load(content);
                    if (doc instanceof Map) {
                        flattenYaml("", (Map<?, ?>) doc, result);
                    }
                } catch (Exception e) {
                    log.debug("Failed to parse {}: {}", name, e.getMessage());
                }
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static void flattenYaml(String prefix, Map<?, ?> map, Map<String, String> result) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String key = prefix.isEmpty() ? String.valueOf(entry.getKey())
                    : prefix + "." + entry.getKey();
            Object val = entry.getValue();
            if (val instanceof Map) {
                flattenYaml(key, (Map<?, ?>) val, result);
            } else if (val != null) {
                result.put(key, String.valueOf(val));
            }
        }
    }

    // ========== MongoDB URI Extraction ==========

    /**
     * Extract MongoDB URI from config files found in the JAR.
     * Tries UAT profiles first, then default.
     */
    public String extractMongoUri(Map<String, String> configFiles) {
        if (configFiles == null || configFiles.isEmpty()) return null;

        for (String name : CONFIG_PRIORITY) {
            String content = configFiles.get(name);
            if (content == null) continue;

            String uri = name.endsWith(".properties")
                    ? extractFromProperties(content)
                    : extractFromYaml(content);

            if (uri != null && !uri.contains("${")) {
                log.info("Found MongoDB URI in {} (database: {})", name, extractDbName(uri));
                return uri;
            }
            if (uri != null) {
                log.debug("URI in {} contains unresolved placeholders — skipping", name);
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private String extractFromYaml(String content) {
        try {
            Yaml yaml = new Yaml();
            Map<String, Object> data = yaml.load(content);
            if (data == null) return null;

            // Navigate: spring.data.mongodb.uri
            Object spring = data.get("spring");
            if (spring instanceof Map<?,?> springMap) {
                Object dataObj = springMap.get("data");
                if (dataObj instanceof Map<?,?> dataMap) {
                    Object mongodb = dataMap.get("mongodb");
                    if (mongodb instanceof Map<?,?> mongoMap) {
                        Object uri = mongoMap.get("uri");
                        Object db = mongoMap.get("database");
                        if (uri instanceof String s) {
                            // URI may lack database in path — append from separate property
                            if (db instanceof String dbStr && !dbStr.isBlank()) {
                                String existingDb = extractDbName(s);
                                if (existingDb == null || existingDb.isBlank()) {
                                    int qIdx = s.indexOf('?');
                                    if (qIdx > 0) {
                                        String beforeQ = s.substring(0, qIdx);
                                        String query = s.substring(qIdx);
                                        s = (beforeQ.endsWith("/") ? beforeQ : beforeQ + "/") + dbStr + query;
                                    } else {
                                        s = (s.endsWith("/") ? s : s + "/") + dbStr;
                                    }
                                }
                            }
                            return s;
                        }
                        // Build from host/port/database
                        Object host = mongoMap.get("host");
                        Object port = mongoMap.get("port");
                        if (host != null && db != null) {
                            return "mongodb://" + host + ":" + (port != null ? port : "27017") + "/" + db;
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.debug("Failed to parse YAML: {}", e.getMessage());
        }
        return null;
    }

    private String extractFromProperties(String content) {
        try {
            Properties props = new Properties();
            props.load(new StringReader(content));

            String uri = props.getProperty("spring.data.mongodb.uri");
            String db = props.getProperty("spring.data.mongodb.database");

            // Resolve ${...} placeholders from same properties file
            if (uri != null) uri = resolvePlaceholders(uri.trim(), props);
            if (db != null) db = resolvePlaceholders(db.trim(), props);

            if (uri != null) {
                // Bug fix: URI may lack database in path (e.g. mongodb+srv://host/?params)
                // but spring.data.mongodb.database is a separate property — append it
                if (db != null && !db.isBlank()) {
                    String existingDb = extractDbName(uri);
                    if (existingDb == null || existingDb.isBlank()) {
                        // Insert database before query params: mongodb+srv://host/DATABASE?params
                        int qIdx = uri.indexOf('?');
                        if (qIdx > 0) {
                            String beforeQ = uri.substring(0, qIdx);
                            String query = uri.substring(qIdx);
                            if (beforeQ.endsWith("/")) uri = beforeQ + db + query;
                            else uri = beforeQ + "/" + db + query;
                        } else {
                            uri = uri.endsWith("/") ? uri + db : uri + "/" + db;
                        }
                        log.debug("Appended database '{}' to URI from separate property", db);
                    }
                }
                return uri;
            }
            // Build from individual properties
            String host = props.getProperty("spring.data.mongodb.host");
            String port = props.getProperty("spring.data.mongodb.port", "27017");
            if (host != null) host = resolvePlaceholders(host.trim(), props);
            if (host != null && db != null) {
                return "mongodb://" + host + ":" + port + "/" + db;
            }
        } catch (Exception e) {
            log.debug("Failed to parse properties: {}", e.getMessage());
        }
        return null;
    }

    /** Resolve ${PROP_NAME} placeholders from the same properties file. */
    private String resolvePlaceholders(String value, Properties props) {
        if (value == null || !value.contains("${")) return value;
        Matcher m = Pattern.compile("\\$\\{([^}]+)}").matcher(value);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            String key = m.group(1);
            // Handle default values: ${KEY:default}
            String defaultVal = null;
            int colonIdx = key.indexOf(':');
            if (colonIdx > 0) {
                defaultVal = key.substring(colonIdx + 1);
                key = key.substring(0, colonIdx);
            }
            String resolved = props.getProperty(key, defaultVal);
            m.appendReplacement(sb, resolved != null ? Matcher.quoteReplacement(resolved) : Matcher.quoteReplacement(m.group(0)));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    // ========== Oracle / JDBC URL Extraction ==========

    /**
     * Extract Oracle JDBC URL from config files (spring.datasource.url or similar).
     */
    public String extractOracleJdbcUrl(Map<String, String> configFiles) {
        if (configFiles == null || configFiles.isEmpty()) return null;
        for (String name : CONFIG_PRIORITY) {
            String content = configFiles.get(name);
            if (content == null) continue;
            String url = name.endsWith(".properties")
                    ? extractOracleFromProperties(content)
                    : extractOracleFromYaml(content);
            if (url != null) {
                log.info("Found Oracle JDBC URL in {}", name);
                return url;
            }
        }
        return null;
    }

    private String extractOracleFromProperties(String content) {
        try {
            Properties props = new Properties();
            props.load(new StringReader(content));
            for (String key : List.of(
                    "spring.datasource.url",
                    "spring.datasource.hikari.jdbc-url",
                    "spring.jpa.properties.hibernate.connection.url")) {
                String val = props.getProperty(key);
                if (val != null && val.contains("oracle")) {
                    return resolvePlaceholders(val.trim(), props);
                }
            }
        } catch (Exception ignored) {}
        return null;
    }

    @SuppressWarnings("unchecked")
    private String extractOracleFromYaml(String content) {
        try {
            Yaml yaml = new Yaml();
            Map<String, Object> data = yaml.load(content);
            if (data == null) return null;
            Object spring = data.get("spring");
            if (!(spring instanceof Map<?,?> sm)) return null;
            Object ds = sm.get("datasource");
            if (ds instanceof Map<?,?> dsm) {
                for (String key : List.of("url", "jdbc-url")) {
                    Object val = dsm.get(key);
                    if (val instanceof String s && s.contains("oracle")) return s;
                }
                Object hikari = dsm.get("hikari");
                if (hikari instanceof Map<?,?> hm) {
                    Object val = hm.get("jdbc-url");
                    if (val instanceof String s && s.contains("oracle")) return s;
                }
            }
        } catch (Exception ignored) {}
        return null;
    }

    // ========== PostgreSQL / JDBC URL Extraction ==========

    public String extractPostgresJdbcUrl(Map<String, String> configFiles) {
        if (configFiles == null || configFiles.isEmpty()) return null;
        for (String name : CONFIG_PRIORITY) {
            String content = configFiles.get(name);
            if (content == null) continue;
            String url = name.endsWith(".properties")
                    ? extractPostgresFromProperties(content)
                    : extractPostgresFromYaml(content);
            if (url != null) {
                log.info("Found PostgreSQL JDBC URL in {}", name);
                return url;
            }
        }
        return null;
    }

    private String extractPostgresFromProperties(String content) {
        try {
            Properties props = new Properties();
            props.load(new StringReader(content));
            for (String key : List.of(
                    "spring.datasource.url",
                    "spring.datasource.hikari.jdbc-url",
                    "spring.jpa.properties.hibernate.connection.url")) {
                String val = props.getProperty(key);
                if (val != null && (val.contains("postgresql") || val.contains("postgres"))) {
                    return resolvePlaceholders(val.trim(), props);
                }
            }
        } catch (Exception ignored) {}
        return null;
    }

    @SuppressWarnings("unchecked")
    private String extractPostgresFromYaml(String content) {
        try {
            Yaml yaml = new Yaml();
            Map<String, Object> data = yaml.load(content);
            if (data == null) return null;
            Object spring = data.get("spring");
            if (!(spring instanceof Map<?,?> sm)) return null;
            Object ds = sm.get("datasource");
            if (ds instanceof Map<?,?> dsm) {
                for (String key : List.of("url", "jdbc-url")) {
                    Object val = dsm.get(key);
                    if (val instanceof String s && (s.contains("postgresql") || s.contains("postgres"))) return s;
                }
                Object hikari = dsm.get("hikari");
                if (hikari instanceof Map<?,?> hm) {
                    Object val = hm.get("jdbc-url");
                    if (val instanceof String s && (s.contains("postgresql") || s.contains("postgres"))) return s;
                }
            }
        } catch (Exception ignored) {}
        return null;
    }

    /**
     * Extract all connection info from config files.
     * Returns a map suitable for serializing to connections.json.
     */
    public Map<String, Object> extractAllConnectionInfo(Map<String, String> configFiles) {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("extractedAt", java.time.LocalDateTime.now().toString());

        String mongoUri = extractMongoUri(configFiles);
        if (mongoUri != null) {
            Map<String, Object> mongo = new LinkedHashMap<>();
            mongo.put("uri", maskUri(mongoUri));
            String db = extractDbName(mongoUri);
            if (db != null) mongo.put("database", db);
            info.put("mongodb", mongo);
        }

        String oracleUrl = extractOracleJdbcUrl(configFiles);
        if (oracleUrl != null) {
            Map<String, Object> oracle = new LinkedHashMap<>();
            oracle.put("jdbcUrl", oracleUrl);
            info.put("oracle", oracle);
        }

        String postgresUrl = extractPostgresJdbcUrl(configFiles);
        if (postgresUrl != null) {
            Map<String, Object> postgres = new LinkedHashMap<>();
            postgres.put("jdbcUrl", postgresUrl);
            info.put("postgres", postgres);
        }

        return info;
    }

    // ========== Catalog Fetch (live connections disabled) ==========

    /**
     * Live MongoDB connections are disabled — returns null.
     * Collection verification is file-parse-only.
     */
    public MongoCatalog fetchCatalog(String mongoUri) {
        log.debug("Live MongoDB connection disabled — skipping catalog fetch");
        return null;
    }

    // ========== Persistence ==========

    public void storeCatalog(String jarName, MongoCatalog catalog) {
        try {
            Path file = jarDataPaths.mongoCatalogFile(jarName);
            Files.createDirectories(file.getParent());
            String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(catalog);
            Files.writeString(file, json, StandardCharsets.UTF_8);
            log.info("Catalog stored: {}", file.getFileName());
        } catch (IOException e) {
            log.warn("Failed to store catalog for {}: {}", jarName, e.getMessage());
        }
    }

    public Set<String> loadCollectionNames(String jarName) {
        Path file = jarDataPaths.mongoCatalogFile(jarName);
        if (!Files.exists(file)) return null;
        try {
            MongoCatalog catalog = objectMapper.readValue(
                    Files.readString(file, StandardCharsets.UTF_8), MongoCatalog.class);
            Set<String> all = new HashSet<>();
            if (catalog.collections() != null) all.addAll(catalog.collections());
            if (catalog.views() != null) all.addAll(catalog.views());
            log.info("Loaded catalog for {}: {} entries", jarName, all.size());
            return all;
        } catch (Exception e) {
            log.warn("Failed to load catalog for {}: {}", jarName, e.getMessage());
            return null;
        }
    }

    public MongoCatalog loadCatalog(String jarName) {
        Path file = jarDataPaths.mongoCatalogFile(jarName);
        if (!Files.exists(file)) return null;
        try {
            return objectMapper.readValue(Files.readString(file, StandardCharsets.UTF_8), MongoCatalog.class);
        } catch (Exception e) { return null; }
    }

    // ========== Full Flow ==========

    /**
     * Extract URI from config → fetch catalog → store to disk.
     * Returns collection names for verification, or null if unavailable.
     */
    public Set<String> fetchAndStore(String jarName, Map<String, String> configFiles) {
        String uri = extractMongoUri(configFiles);
        if (uri == null) {
            log.info("No MongoDB URI found in JAR config for {} — trying existing catalog", jarName);
            return loadCollectionNames(jarName);
        }
        return fetchAndStore(jarName, uri);
    }

    /** Fetch catalog from explicit URI → store to disk. */
    public Set<String> fetchAndStore(String jarName, String mongoUri) {
        MongoCatalog catalog = fetchCatalog(mongoUri);
        if (catalog == null) return loadCollectionNames(jarName);
        storeCatalog(jarName, catalog);
        Set<String> all = new HashSet<>();
        all.addAll(catalog.collections());
        all.addAll(catalog.views());
        return all;
    }

    public void deleteCatalog(String jarName) {
        try { Files.deleteIfExists(jarDataPaths.mongoCatalogFile(jarName)); }
        catch (IOException ignored) {}
    }

    // ========== Helpers ==========

    private String extractDbName(String uri) {
        Matcher m = Pattern.compile("mongodb(?:\\+srv)?://[^/]*/([^?]+)").matcher(uri);
        return m.find() ? m.group(1) : null;
    }

    private String maskUri(String uri) {
        if (uri == null) return null;
        return uri.replaceAll("(?<=:)[^/@:]+(?=@)", "***");
    }

    private String sanitize(String name) {
        return name.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    // ========== Catalog Record ==========

    public record MongoCatalog(
            String database,
            String maskedUri,
            List<String> collections,
            List<String> views,
            int totalCollections,
            int totalViews,
            String fetchedAt
    ) {}
}
