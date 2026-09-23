package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Loads and exposes all classification config from domain-config.json.
 * Provides domain/collection detection helpers used by CallTreeBuilder and CallGraphService.
 */
@Component
public class DomainConfigLoader {

    private static final Logger log = LoggerFactory.getLogger(DomainConfigLoader.class);

    // ALL config loaded from domain-config.json -- nothing hardcoded
    private Pattern collectionPattern;
    private List<String[]> collPrefixList;   // sorted by prefix length desc: [prefix, domain]
    private Map<String, String> sbDomainMap;
    private List<String> viewContains;
    private List<String> viewStartsWith;
    private List<String> basePackages;
    private Map<String, List<String>> operationTypes;  // e.g. "READ" -> ["find","get",...]
    private Set<String> jarNameIgnoreSegments;
    private List<String> repositorySuffixes;
    private List<String> queryAnnotations;
    private List<String[]> batchKeywords;    // from endpoint_classification
    private List<String> httpClientClasses;  // class prefixes for HTTP client detection
    private Map<String, List<Map<String, String>>> implicitCollections;  // method -> [{collection, operation, source}]
    /** The whole parsed document, retained for sections (`size_thresholds`, `performance_thresholds`,
     *  `complexity_rules`, `jar_name_ignore_segments`) that have no per-field getter of their own —
     *  {@link EndpointReportBuilder}/{@link EndpointCrossReportBuilder}'s own config needs, added
     *  after every other field here already had a dedicated accessor. Not affected by
     *  {@link #applyPerJarOverride}, which only ever touches module_types/base_packages/
     *  implicit_collections. */
    private Map<String, Object> rawConfig = Map.of();

    /**
     * One stage of a configured collection-lifecycle workflow (e.g. CREATE_TEMP, RENAME).
     * apiMethods are REAL MongoDB driver/Spring Data method names, matched against RESOLVED
     * bytecode invocations (InvocationRef.methodName(), owner-checked) — never application
     * wrapper method names, which are free to be renamed and are deliberately NOT matched here.
     * pipelineStageTokens (e.g. "$merge", "$out") are matched against already-detected
     * aggregation pipeline stages, not text-scanned.
     */
    public record StageConfig(String name, List<String> apiMethods, List<String> pipelineStageTokens, int minOccurrences) {}

    /** A named, ordered multi-stage workflow (e.g. BLUE_GREEN_COLLECTION_SWAP). */
    public record WorkflowConfig(String typeName, String description, int minStagesMatched, List<StageConfig> stages) {}

    // Collection-lifecycle workflow definitions, keyed by workflow type name (e.g.
    // "BLUE_GREEN_COLLECTION_SWAP") — consumed by CollectionLifecycleDetector to recognize
    // multi-node structural patterns (create temp -> copy -> index -> rename -> drop) that
    // no single-node classification (operationType/collectionsAccessed) can represent.
    private Map<String, WorkflowConfig> collectionLifecycleWorkflows;

    @Value("${app.config-dir:config}")
    private String configDirProp;

    @PostConstruct
    @SuppressWarnings("unchecked")
    void loadDomainConfig() {
        try {
            InputStream configStream = null;
            String source = null;

            // 1. Try external config dir first
            Path externalFile = resolveExternalConfig("domain-config.json");
            if (externalFile != null && Files.exists(externalFile)) {
                configStream = Files.newInputStream(externalFile);
                source = externalFile.toString();
            }

            // 2. Fall back to classpath
            if (configStream == null) {
                ClassPathResource res = new ClassPathResource("static/jar/domain-config.json");
                if (res.exists()) {
                    configStream = res.getInputStream();
                    source = "classpath:static/jar/domain-config.json";
                }
            }

            if (configStream != null) {
                ObjectMapper om = new ObjectMapper();
                Map<String, Object> config;
                try (InputStream is = configStream) {
                    config = om.readValue(is, Map.class);
                }
                rawConfig = config;

                // Collection name regex
                String collRegex = (String) config.getOrDefault("collection_pattern", "[A-Z][A-Z0-9_]{3,59}");
                collectionPattern = Pattern.compile(collRegex);

                // Build prefix list for collection -> domain mapping
                collPrefixList = new ArrayList<>();
                Map<String, Object> moduleTypes = (Map<String, Object>) config.getOrDefault("module_types", Map.of());
                for (Map.Entry<String, Object> entry : moduleTypes.entrySet()) {
                    String domain = entry.getKey();
                    Map<String, Object> info = (Map<String, Object>) entry.getValue();
                    List<String> prefixes = (List<String>) info.getOrDefault("prefixes", List.of());
                    for (String prefix : prefixes) {
                        collPrefixList.add(new String[]{prefix, domain});
                    }
                }
                collPrefixList.sort((a, b) -> b[0].length() - a[0].length());

                // Snapshot buffer domain map
                sbDomainMap = (Map<String, String>) config.getOrDefault("snapshot_buffer_domain_map", Map.of());

                // View detection rules
                Map<String, Object> vd = (Map<String, Object>) config.getOrDefault("view_detection", Map.of());
                viewContains = (List<String>) vd.getOrDefault("contains", List.of());
                viewStartsWith = (List<String>) vd.getOrDefault("starts_with", List.of());

                // Base packages for external call filtering
                basePackages = (List<String>) config.getOrDefault("base_packages", List.of());

                // Operation type prefixes (READ -> [find, get, ...])
                // Sort each prefix list by length descending so longer prefixes match first
                // (e.g., "saveAll" matches before "save", "findById" before "find")
                operationTypes = new LinkedHashMap<>();
                Map<String, Object> opConfig = (Map<String, Object>) config.getOrDefault("operation_types", Map.of());
                for (Map.Entry<String, Object> entry : opConfig.entrySet()) {
                    List<String> sorted = new ArrayList<>((List<String>) entry.getValue());
                    sorted.sort((a, b) -> b.length() - a.length());
                    operationTypes.put(entry.getKey(), sorted);
                }

                // JAR name segments to ignore when computing domain
                List<String> ignoreSegs = (List<String>) config.getOrDefault("jar_name_ignore_segments", List.of());
                jarNameIgnoreSegments = new HashSet<>(ignoreSegs);

                // Repository class suffixes
                repositorySuffixes = (List<String>) config.getOrDefault("repository_suffixes", List.of("Repository", "Repo"));

                // Annotation names that contain query/collection references
                queryAnnotations = (List<String>) config.getOrDefault("query_annotations", List.of("Query", "Aggregation"));

                // HTTP client class prefixes for REST call detection
                httpClientClasses = (List<String>) config.getOrDefault("http_client_classes", List.of());

                // Implicit collection mappings for known cross-JAR patterns
                implicitCollections = new LinkedHashMap<>();
                Map<String, Object> implConfig = (Map<String, Object>) config.getOrDefault("implicit_collections", Map.of());
                for (Map.Entry<String, Object> entry : implConfig.entrySet()) {
                    List<Map<String, String>> mappings = new ArrayList<>();
                    for (Object item : (List<?>) entry.getValue()) {
                        Map<String, Object> m = (Map<String, Object>) item;
                        Map<String, String> mapping = new LinkedHashMap<>();
                        mapping.put("collection", (String) m.get("collection"));
                        mapping.put("operation", (String) m.get("operation"));
                        mapping.put("source", (String) m.get("source"));
                        mappings.add(mapping);
                    }
                    implicitCollections.put(entry.getKey(), mappings);
                }

                // Endpoint classification keywords (batch detection etc.)
                batchKeywords = new ArrayList<>();
                Map<String, Object> epClass = (Map<String, Object>) config.getOrDefault("endpoint_classification", Map.of());
                for (Map.Entry<String, Object> entry : epClass.entrySet()) {
                    Map<String, Object> info = (Map<String, Object>) entry.getValue();
                    List<String> kw = (List<String>) info.getOrDefault("keywords", List.of());
                    for (String k : kw) batchKeywords.add(new String[]{k, entry.getKey()});
                }

                // Collection lifecycle workflows (e.g. blue-green collection swap). apiMethods
                // are exact (case-sensitive) real MongoDB driver/Spring Data method names —
                // matched against resolved bytecode invocations, never lowercased/substring-
                // matched against application wrapper method names.
                collectionLifecycleWorkflows = new LinkedHashMap<>();
                Map<String, Object> lifecycleConfig = (Map<String, Object>) config.getOrDefault("collection_lifecycle_workflows", Map.of());
                for (Map.Entry<String, Object> entry : lifecycleConfig.entrySet()) {
                    String typeName = entry.getKey();
                    Map<String, Object> wf = (Map<String, Object>) entry.getValue();
                    String description = (String) wf.getOrDefault("description", "");
                    int minStagesMatched = ((Number) wf.getOrDefault("minStagesMatched", 1)).intValue();
                    List<StageConfig> stages = new ArrayList<>();
                    for (Object stageObj : (List<?>) wf.getOrDefault("stages", List.of())) {
                        Map<String, Object> stageMap = (Map<String, Object>) stageObj;
                        String stageName = (String) stageMap.get("name");
                        List<String> apiMethods = new ArrayList<>((List<String>) stageMap.getOrDefault("apiMethods", List.of()));
                        List<String> pipelineStageTokens = new ArrayList<>((List<String>) stageMap.getOrDefault("pipelineStageTokens", List.of()));
                        int minOccurrences = ((Number) stageMap.getOrDefault("minOccurrences", 1)).intValue();
                        stages.add(new StageConfig(stageName, apiMethods, pipelineStageTokens, minOccurrences));
                    }
                    collectionLifecycleWorkflows.put(typeName, new WorkflowConfig(typeName, description, minStagesMatched, stages));
                }

                log.info("Loaded domain-config.json from {}: {} domain prefixes, {} SB mappings, {} view rules, {} base packages, " +
                         "{} operation types, {} JAR ignore segments, {} repo suffixes, {} query annotations",
                        source, collPrefixList.size(), sbDomainMap.size(), viewContains.size() + viewStartsWith.size(),
                        basePackages.size(), operationTypes.size(), jarNameIgnoreSegments.size(),
                        repositorySuffixes.size(), queryAnnotations.size());
            } else {
                log.warn("domain-config.json not found -- all classification disabled");
                applyDefaults();
            }
        } catch (Exception e) {
            log.error("Failed to load domain-config.json: {}", e.getMessage());
            applyDefaults();
        }
    }

    private Path resolveExternalConfig(String filename) {
        if (configDirProp == null || configDirProp.isBlank()) return null;
        Path dir = Path.of(configDirProp).toAbsolutePath();
        if (Files.isDirectory(dir)) return dir.resolve(filename);
        // Try parent directory (handles running from unified-web/ subdirectory)
        Path parent = dir.getParent();
        if (parent != null) {
            Path up = parent.getParent();
            if (up != null) {
                Path upDir = up.resolve(configDirProp);
                if (Files.isDirectory(upDir)) return upDir.resolve(filename);
            }
        }
        return dir.resolve(filename);
    }

    private void applyDefaults() {
        collectionPattern = Pattern.compile("[A-Z][A-Z0-9_]{3,59}");
        collPrefixList = List.of();
        sbDomainMap = Map.of();
        viewContains = List.of();
        viewStartsWith = List.of();
        basePackages = List.of();
        // Built-in operation type prefixes — used when no domain-config.json is present.
        // These mirror the production domain-config.json operation_types section so that
        // inferOperationType() works correctly in unit-test and offline contexts.
        operationTypes = new LinkedHashMap<>();
        operationTypes.put("READ",      List.of("find", "get", "read", "query", "search", "fetch",
                "load", "retrieve", "distinct", "stream", "scroll", "watch", "changeStream",
                "findById", "findOne", "findAll", "findAllById", "list", "select", "resolve",
                "lookup", "cursor", "toList", "iterate"));
        operationTypes.put("WRITE",     List.of("save", "insert", "persist", "create", "add",
                "store", "bulkWrite", "insertMany", "insertAll", "saveAll", "addInsert", "execute",
                "insertOne", "createCollection", "createIndex", "renameCollection", "ensureIndex",
                "putDocument", "writeDocument"));
        operationTypes.put("UPDATE",    List.of("update", "modify", "set", "patch", "edit",
                "change", "replace", "upsert", "updateMany", "updateFirst", "updateMulti",
                "findAndModify", "findAndReplace", "addUpdate", "replaceOne", "merge",
                "updateOne", "findOneAndUpdate", "findOneAndReplace", "increment", "mutate"));
        operationTypes.put("DELETE",    List.of("delete", "remove", "clear", "purge", "drop",
                "deleteMany", "deleteAll", "removeAll", "addDelete", "findAndRemove",
                "findAllAndRemove", "deleteOne", "findOneAndDelete", "dropCollection", "dropIndex"));
        operationTypes.put("AGGREGATE", List.of("aggregate", "aggregation", "graphLookup",
                "pipeline", "mapReduce", "group", "bucket", "facet", "unwind", "bucketAuto",
                "unionWith", "out"));
        operationTypes.put("COUNT",     List.of("count", "exists", "countDocuments",
                "estimatedDocumentCount"));
        jarNameIgnoreSegments = Set.of();
        repositorySuffixes = List.of("Repository", "Repo");
        queryAnnotations = List.of("Query", "Aggregation");
        httpClientClasses = List.of();
        implicitCollections = Map.of();
        batchKeywords = List.of();
        collectionLifecycleWorkflows = Map.of();
    }

    // ========== Per-JAR config override (cleared after each analysis) ==========

    /**
     * Per-JAR prefix list (module_types) — checked first in detectCollectionDomain.
     * Null when no override is active.
     */
    private List<String[]> perJarPrefixList;
    private List<String> perJarBasePackages;
    private Map<String, List<Map<String, String>>> perJarImplicitCollections;

    /**
     * Apply a per-JAR domain config override on top of the global config.
     * Supported keys: module_types, base_packages, implicit_collections.
     * Called before starting an analysis that has a JAR-specific config file.
     * The analysis pipeline is synchronized, so this is thread-safe.
     */
    @SuppressWarnings("unchecked")
    public void applyPerJarOverride(Map<String, Object> config) {
        if (config == null || config.isEmpty()) return;
        // module_types: per-JAR prefix → domain mappings (checked first in detectCollectionDomain)
        Object mt = config.get("module_types");
        if (mt instanceof Map<?, ?> moduleTypes) {
            perJarPrefixList = new ArrayList<>();
            for (Map.Entry<?, ?> e : moduleTypes.entrySet()) {
                String domain = (String) e.getKey();
                if (!(e.getValue() instanceof Map<?, ?> info)) continue;
                Object prefixObj = info.get("prefixes");
                if (!(prefixObj instanceof List<?> prefixes)) continue;
                for (Object p : prefixes) {
                    if (p instanceof String s && !s.isBlank()) {
                        perJarPrefixList.add(new String[]{s, domain});
                    }
                }
            }
            perJarPrefixList.sort((a, b) -> b[0].length() - a[0].length());
        }
        // base_packages: merged (per-JAR first, then global)
        Object bp = config.get("base_packages");
        if (bp instanceof List<?> list && !list.isEmpty()) {
            perJarBasePackages = new ArrayList<>();
            for (Object p : list) {
                if (p instanceof String s && !s.isBlank()) perJarBasePackages.add(s);
            }
        }
        // implicit_collections: per-JAR overrides merged with global
        Object ic = config.get("implicit_collections");
        if (ic instanceof Map<?, ?> icMap) {
            perJarImplicitCollections = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : icMap.entrySet()) {
                List<Map<String, String>> mappings = new ArrayList<>();
                if (e.getValue() instanceof List<?> items) {
                    for (Object item : items) {
                        if (item instanceof Map<?, ?> m) {
                            Map<String, String> mapping = new LinkedHashMap<>();
                            if (m.get("collection") instanceof String c) mapping.put("collection", c);
                            if (m.get("operation") instanceof String o) mapping.put("operation", o);
                            if (m.get("source") instanceof String s) mapping.put("source", s);
                            if (!mapping.isEmpty()) mappings.add(mapping);
                        }
                    }
                }
                perJarImplicitCollections.put((String) e.getKey(), mappings);
            }
        }
        log.info("Per-JAR config override applied: {} domain prefixes, {} base packages, {} implicit collections",
                perJarPrefixList != null ? perJarPrefixList.size() : 0,
                perJarBasePackages != null ? perJarBasePackages.size() : 0,
                perJarImplicitCollections != null ? perJarImplicitCollections.size() : 0);
    }

    /** Clear per-JAR override after analysis completes. */
    public void clearPerJarOverride() {
        perJarPrefixList = null;
        perJarBasePackages = null;
        perJarImplicitCollections = null;
    }

    // ========== Getters for config values ==========

    public Pattern getCollectionPattern() { return collectionPattern; }

    /**
     * The config slice {@link SummaryStats} needs, from the fields already loaded here.
     *
     * <p>Deliberately the <b>global</b> lists, with no per-JAR override merged in (contrast
     * {@link #getBasePackages()}). The browser computes the Summary chips against
     * {@code /jar/domain-config.json} — the classpath copy, fetched by the page — and has no way
     * to see a per-JAR override. Merging one in here would make the server's Domains chip disagree
     * with the browser's for exactly those JARs that carry an override, which is the failure mode
     * hardest to notice: a plausible number, on one JAR, in one chip.
     *
     * <p>Reading them from this loader rather than re-parsing the file keeps one parse of
     * {@code domain-config.json} in the process, and means a config reload moves both the analysis
     * and the chips together.
     */
    public SummaryStats.Config summaryStatsConfig() {
        List<String> keywords = new ArrayList<>();
        if (batchKeywords != null) {
            for (String[] pair : batchKeywords) keywords.add(pair[0]);
        }
        return new SummaryStats.Config(collPrefixList, sbDomainMap, viewContains, viewStartsWith,
                keywords);
    }

    /** The whole parsed {@code domain-config.json}, for sections with no per-field accessor. */
    public Map<String, Object> rawConfig() {
        return rawConfig;
    }

    /**
     * Returns the effective base packages: per-JAR overrides first, then global.
     * Both are included so per-JAR packages augment rather than replace the global list.
     */
    public List<String> getBasePackages() {
        if (perJarBasePackages != null && !perJarBasePackages.isEmpty()) {
            List<String> merged = new ArrayList<>(perJarBasePackages);
            for (String bp : basePackages) {
                if (!merged.contains(bp)) merged.add(bp);
            }
            return merged;
        }
        return basePackages;
    }
    public void setBasePackages(List<String> basePackages) { this.basePackages = basePackages; }
    public List<String> getRepositorySuffixes() { return repositorySuffixes; }
    public List<String> getQueryAnnotations() { return queryAnnotations; }
    public Map<String, WorkflowConfig> getCollectionLifecycleWorkflows() { return collectionLifecycleWorkflows; }

    /**
     * Returns implicit collection mappings: per-JAR entries merged with global defaults.
     * Per-JAR entries take priority (checked first by the caller).
     */
    public Map<String, List<Map<String, String>>> getImplicitCollections() {
        if (perJarImplicitCollections != null && !perJarImplicitCollections.isEmpty()) {
            Map<String, List<Map<String, String>>> merged = new LinkedHashMap<>(perJarImplicitCollections);
            for (Map.Entry<String, List<Map<String, String>>> e : implicitCollections.entrySet()) {
                merged.putIfAbsent(e.getKey(), e.getValue());
            }
            return merged;
        }
        return implicitCollections;
    }

    // ========== Collection Name Validation ==========

    /** Words that look like collection names but are actually Java/MongoDB keywords or status values.
     *  Checked case-insensitively so it covers both UPPER_CASE enterprise strings and lowercase literals. */
    private static final Set<String> COLLECTION_BLACKLIST = Set.of(
            "TRUE", "FALSE", "NULL", "EMPTY", "NONE", "TYPE", "NAME", "VALUE",
            "LIST", "ARRAY", "MAP", "SET", "HASH", "TREE",
            "STRING", "OBJECT", "CLASS", "FIELD", "INDEX",
            "DOCUMENT", "QUERY", "UPDATE", "DELETE", "INSERT",
            "ERROR", "INFO", "WARN", "DEBUG", "TRACE",
            "TEST", "TEMP", "DATA", "UTIL", "BEAN", "IMPL",
            "HTTP", "REST", "JSON", "BSON", "MONGO", "SPRING",
            "AGGREGATE", "PIPELINE", "COLLECTION", "DATABASE", "COMMAND",
            "FILTER", "ASCENDING", "DESCENDING", "DISTINCT", "COUNT",
            // Aggregation pipeline stage field keys (e.g. $merge's "into", $lookup's "from") —
            // sometimes surface as bogus candidate collection names when a nested constructor's
            // literal leaks onto the wrong call (see BytecodeClassParser fix); excluded here too
            // as defense-in-depth so they never reach collectionsAccessed even via another path.
            "INTO", "FROM",
            // Status/lifecycle values commonly used as string literals
            "ACTIVE", "INACTIVE", "PENDING", "ENABLED", "DISABLED", "ARCHIVED",
            "CONFIRMED", "REJECTED", "APPROVED", "COMPLETED", "CANCELLED", "EXPIRED",
            "CREATED", "UPDATED", "DELETED", "STARTED", "STOPPED", "RUNNING",
            "STATUS", "STATE", "FLAG", "MODE", "LEVEL", "ROLE", "SORT",
            "TEXT", "LABEL", "TITLE", "CODE", "BODY", "ITEMS", "TAGS",
            // Java class name stems that CamelCase→UPPER_SNAKE produces as false positives
            "SERVICE_IMPL", "ABSTRACT_BASE", "DEFAULT_CONFIG", "BASE_SERVICE",
            "BASE_REPOSITORY", "BASE_CONTROLLER", "ABSTRACT_REPOSITORY",
            "ABSTRACT_SERVICE", "ABSTRACT_CONTROLLER", "CUSTOM_IMPL",
            "MONGO_TEMPLATE", "MONGO_OPERATIONS", "MONGO_CONFIG",
            "APPLICATION_CONFIG", "SECURITY_CONFIG", "WEB_CONFIG",
            "ASYNC_CONFIG", "CACHE_CONFIG", "SCHEDULER_CONFIG"
    );

    /**
     * Check if a string is likely a real MongoDB collection name (not a false positive).
     *
     * Accepts two naming conventions:
     *   • Lowercase generic names (e.g. employees, orders, audit_events) — common in non-enterprise apps
     *   • Enterprise UPPER_CASE with PREFIX_NAME underscore (e.g. TYAC_MAST, CLM_DOCUMENTS)
     */
    public boolean isLikelyCollectionName(String s) {
        if (s == null || s.length() < 3 || s.length() > 60) return false;
        if (s.contains(" ") || s.contains("{") || s.contains("}")) return false;
        if (s.contains("(") || s.contains(")") || s.contains("/")) return false;
        if (s.contains("=") || s.contains(",") || s.contains(":")) return false;
        if (s.startsWith("$") || s.startsWith("_")) return false; // MongoDB operators/internal fields
        if (COLLECTION_BLACKLIST.contains(s.toUpperCase())) return false; // case-insensitive blacklist
        // Lowercase collection names — standard in generic Spring Boot / MEAN-stack apps
        if (s.matches("[a-z][a-z0-9_]*")) return true;
        // Enterprise UPPER_CASE: must contain underscore separator and match configured pattern
        if (!s.contains("_")) return false;
        if (!collectionPattern.matcher(s).matches()) return false;
        return true;
    }

    /** Same shape/blacklist guards as {@link #isLikelyCollectionName}, but without requiring an
     *  underscore separator for UPPER_CASE names. Only safe at call sites where the argument's
     *  role as a collection name is already guaranteed by API contract (e.g.
     *  MongoDatabase/MongoOperations.getCollection's own argument) — never for general heuristic
     *  string-literal scanning, where the underscore requirement is what keeps unrelated
     *  UPPER_CASE literals (status codes, constant names) from being misreported as collections. */
    public boolean isLikelyCollectionNameLenient(String s) {
        if (s == null || s.length() < 3 || s.length() > 60) return false;
        if (s.contains(" ") || s.contains("{") || s.contains("}")) return false;
        if (s.contains("(") || s.contains(")") || s.contains("/")) return false;
        if (s.contains("=") || s.contains(",") || s.contains(":")) return false;
        if (s.startsWith("$") || s.startsWith("_")) return false;
        if (COLLECTION_BLACKLIST.contains(s.toUpperCase())) return false;
        return s.matches("[A-Za-z][A-Za-z0-9_]*");
    }

    // ========== Domain / Collection Detection Helpers ==========

    /**
     * Returns the domain for a collection name based on prefix matching from domain-config.json.
     */
    public String detectCollectionDomain(String collName) {
        if (collName == null) return "Other";

        // Snapshot buffer mapping
        if (collName.startsWith("SB_") && sbDomainMap != null) {
            for (Map.Entry<String, String> entry : sbDomainMap.entrySet()) {
                if (collName.startsWith(entry.getKey())) return entry.getValue();
            }
        }

        // Normalize to uppercase for prefix matching (domain-config prefixes are all UPPER_CASE).
        // This ensures lowercase @Document names like "clm_child_locks" match "CLM_" prefix.
        String n = collName.toUpperCase();
        if (collName.startsWith("SB_") || n.startsWith("SB_")) n = n.substring(3);
        // WIP_ (Work-In-Progress) wraps a module code the same way SB_ (Snapshot Buffer) does —
        // e.g. WIP_UWPL_POLICY_FLIGHT is Underwriting's UWPL_ module, just staged. Without
        // stripping it here, every WIP_* collection falls through module_types prefix matching
        // (which only knows the bare UWPL_/UWGE_/etc. prefixes) and resolves to "Other".
        if (n.startsWith("WIP_")) n = n.substring(4);

        // Per-JAR override prefix list checked first (higher priority than global)
        if (perJarPrefixList != null) {
            for (String[] pair : perJarPrefixList) {
                if (n.startsWith(pair[0])) return pair[1];
            }
        }

        // Global prefix matching from domain-config.json
        if (collPrefixList != null) {
            for (String[] pair : collPrefixList) {
                if (n.startsWith(pair[0])) return pair[1];
            }
        }

        // Lowercase collection names (no enterprise prefix) — valid generic app collections
        if (isLikelyCollectionName(collName) && collName.matches("[a-z][a-z0-9_]*")) {
            return "Generic";
        }

        return "Other";
    }

    /**
     * Determines if an external class (not in classMap) should be kept in the call tree.
     * Rule: keep only if className starts with one of the configured base_packages.
     */
    public boolean isOrgExternalClass(String className) {
        if (className.contains("$$Lambda")) return false;
        for (String bp : basePackages) {
            if (className.startsWith(bp)) return true;
        }
        return false;
    }

    /**
     * Determines if a class is an HTTP client (RestTemplate, WebClient, FeignClient, etc.).
     */
    public boolean isHttpClientCall(String className) {
        if (className == null || httpClientClasses == null) return false;
        for (String prefix : httpClientClasses) {
            if (className.startsWith(prefix)) return true;
        }
        return false;
    }

    // Method name suffixes that indicate non-DB operations despite matching an operation prefix.
    // E.g., "addListener" starts with "add" (WRITE prefix) but is lifecycle, not database.
    private static final Set<String> NON_DB_METHOD_SUFFIXES = Set.of(
            "listener", "handler", "observer", "callback", "interceptor", "filter",
            "logger", "thread", "decorator", "factory", "builder", "validator",
            "view", "component", "panel", "frame", "dialog", "widget",
            "timer", "scheduler", "executor", "task", "runnable",
            "stream", "iterator", "comparator", "converter", "mapper",
            "savepoint", "checkpoint", "snapshot", "backup",
            "context", "scope", "session", "attribute", "header",
            "resource", "connection", "channel", "socket"
    );

    // Exact method names known to be in-memory helpers that happen to match an operation
    // prefix by coincidence (case-insensitive). Unlike NON_DB_METHOD_SUFFIXES (structural
    // suffix patterns applicable across many codebases), these are specific method names seen
    // in the wild that no generic suffix/prefix rule can safely exclude without also excluding
    // real DB operations (e.g. a suffix rule for "index" would also kill the legitimate
    // "createIndex"/"dropIndex" WRITE/DELETE operations).
    private static final Set<String> NON_DB_METHOD_NAMES = Set.of(
            "removenullfields", "getorcreatecollectionindex"
    );

    /**
     * Infers the operation type (READ, WRITE, DELETE, etc.) from method name prefixes.
     * For entity/DTO classes (no stereotype or ENTITY), ignores "set"/"get" prefixes
     * since those are in-memory bean mutations, not database operations.
     * Excludes methods whose names end with non-DB suffixes (e.g., addListener, createLogger).
     */
    public String inferOperationType(String methodName, String stereotype) {
        if (methodName == null || operationTypes == null || operationTypes.isEmpty()) return null;
        String n = methodName.toLowerCase();

        // Exclude specific known in-memory helper names (see NON_DB_METHOD_NAMES javadoc)
        if (NON_DB_METHOD_NAMES.contains(n)) return null;

        // Exclude methods whose name ends with a non-DB suffix
        for (String suffix : NON_DB_METHOD_SUFFIXES) {
            if (n.endsWith(suffix)) return null;
        }

        // Data classes: no stereotype, ENTITY (@Document/@Entity), or OTHER (plain POJOs)
        // Only known DB-interacting roles (SERVICE, REPOSITORY, etc.) should match set/get prefixes
        boolean isDataClass = stereotype == null || "ENTITY".equals(stereotype) || "OTHER".equals(stereotype);

        // Use longest-prefix-wins across all categories so that specific prefixes like
        // "findAndModify" (UPDATE) always beat generic ones like "find" (READ).
        String bestType = null;
        int bestLen = 0;
        for (Map.Entry<String, List<String>> entry : operationTypes.entrySet()) {
            for (String prefix : entry.getValue()) {
                if (n.startsWith(prefix.toLowerCase()) && prefix.length() > bestLen) {
                    // Skip bean getter/setter prefix matches on data classes
                    if (isDataClass && ("set".equals(prefix) || "get".equals(prefix))) continue;
                    bestType = entry.getKey();
                    bestLen = prefix.length();
                }
            }
        }
        return bestType;
    }

    /**
     * Extracts a human-readable project name from a JAR filename.
     * Prefers POM artifactId when available.
     */
    public String jarToProject(String jarName, String artifactId) {
        if (jarName == null) return "Main App";
        String name = artifactId != null ? artifactId : jarName.replaceAll("-\\d[\\d.]*(?:-SNAPSHOT)?\\.jar$", "").replaceAll("\\.jar$", "");
        String[] parts = name.split("-");
        StringBuilder sb = new StringBuilder();
        for (String part : parts) {
            if (!sb.isEmpty()) sb.append(' ');
            sb.append(Character.toUpperCase(part.charAt(0))).append(part.substring(1).toLowerCase());
        }
        return sb.toString();
    }

    public String jarToProject(String jarName) {
        return jarToProject(jarName, null);
    }

    /**
     * Extracts a domain label from a JAR filename, filtering out configured ignore segments.
     */
    public String jarToDomain(String jarName) {
        if (jarName == null) return "Core";
        String name = jarName.replaceAll("-\\d[\\d.]*(?:-SNAPSHOT)?\\.jar$", "").replaceAll("\\.jar$", "").toLowerCase();
        String[] parts = name.split("-");
        List<String> meaningful = new ArrayList<>();
        for (String p : parts) {
            if (jarNameIgnoreSegments == null || !jarNameIgnoreSegments.contains(p)) meaningful.add(p);
        }
        if (meaningful.isEmpty()) meaningful = Arrays.asList(parts);
        StringBuilder sb = new StringBuilder();
        for (String m : meaningful) {
            if (m.isEmpty()) continue;
            if (!sb.isEmpty()) sb.append(' ');
            sb.append(Character.toUpperCase(m.charAt(0))).append(m.substring(1));
        }
        return sb.toString();
    }

    /**
     * Extracts collection name references matching the configured collection pattern.
     */
    public Set<String> extractCollectionRefs(String str) {
        Set<String> refs = new HashSet<>();
        if (str == null || str.isBlank()) return refs;
        var matcher = collectionPattern.matcher(str);
        while (matcher.find()) {
            String match = matcher.group();
            if (match.contains("_")) refs.add(match);
        }
        return refs;
    }

    /**
     * Extracts collection names from MongoDB aggregation pipeline strings.
     * Handles: $lookup, $graphLookup, $out, $merge, $unionWith — all patterns that reference other collections.
     * Uses the configurable collection_pattern instead of hardcoded UPPER_CASE.
     */
    public Set<String> extractPipelineCollectionRefs(String str) {
        Set<String> refs = new HashSet<>();
        if (str == null || str.isBlank()) return refs;

        String cp = collectionPattern.pattern(); // e.g. "[A-Z][A-Z0-9_]{3,59}"

        // $lookup / $graphLookup: { from: "COLL" }
        findAllMatches(refs, str, "\\$(lookup|graphLookup)[^}]*[\"']from[\"']\\s*:\\s*[\"'](" + cp + ")[\"']");

        // $graphLookup also has connectFromField/connectToField but those are field names, not collections

        // $out: "COLL" or $out: { coll: "COLL" }
        findAllMatches(refs, str, "\\$out[\"']?\\s*:\\s*[\"'](" + cp + ")[\"']");
        findAllMatches(refs, str, "\\$out[^}]*[\"']coll[\"']\\s*:\\s*[\"'](" + cp + ")[\"']");

        // $merge: "COLL" or $merge: { into: "COLL" }
        findAllMatches(refs, str, "\\$merge[\"']?\\s*:\\s*[\"'](" + cp + ")[\"']");
        findAllMatches(refs, str, "\\$merge[^}]*[\"']into[\"']\\s*:\\s*[\"'](" + cp + ")[\"']");

        // $unionWith: "COLL" or $unionWith: { coll: "COLL" }
        findAllMatches(refs, str, "\\$unionWith[\"']?\\s*:\\s*[\"'](" + cp + ")[\"']");
        findAllMatches(refs, str, "\\$unionWith[^}]*[\"']coll[\"']\\s*:\\s*[\"'](" + cp + ")[\"']");

        return refs;
    }

    private void findAllMatches(Set<String> refs, String str, String regex) {
        Matcher m = Pattern.compile(regex).matcher(str);
        while (m.find()) refs.add(m.group(m.groupCount())); // last capture group is always the collection name
    }

    /**
     * Flattens annotation attribute values to a list of strings.
     * Handles String, List&lt;String&gt;, and nested lists (e.g. @Aggregation pipeline arrays).
     */
    @SuppressWarnings("unchecked")
    public List<String> flattenAnnotationValue(Object val) {
        List<String> result = new ArrayList<>();
        if (val instanceof String s) {
            result.add(s);
        } else if (val instanceof List<?> list) {
            for (Object item : list) {
                if (item instanceof String s) result.add(s);
                else if (item instanceof List<?>) result.addAll(flattenAnnotationValue(item));
            }
        }
        return result;
    }
}
