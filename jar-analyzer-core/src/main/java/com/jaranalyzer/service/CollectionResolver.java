package com.jaranalyzer.service;

import com.jaranalyzer.model.CallNode;
import com.jaranalyzer.service.CallGraphIndex.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Resolves repository-to-collection mappings and populates collection data on CallNodes.
 * Extracted from CallTreeBuilder to keep files focused and under 300 lines.
 */
@Component
class CollectionResolver {

    private static final Logger log = LoggerFactory.getLogger(CollectionResolver.class);

    private final DomainConfigLoader configLoader;
    private final MongoMethodDetector mongoMethodDetector;
    private final AggregationDetector aggregationDetector;
    private final JpaMethodDetector jpaMethodDetector;

    CollectionResolver(DomainConfigLoader configLoader, MongoMethodDetector mongoMethodDetector,
                       AggregationDetector aggregationDetector, JpaMethodDetector jpaMethodDetector) {
        this.configLoader = configLoader;
        this.mongoMethodDetector = mongoMethodDetector;
        this.aggregationDetector = aggregationDetector;
        this.jpaMethodDetector = jpaMethodDetector;
    }

    // ========== Base Package Auto-Detection ==========

    /**
     * Fallback: detect the most common top-level package (e.g. "com.acme") from indexed classes.
     * Uses the first two segments of class FQNs and picks the most frequent one.
     */
    List<String> autoDetectBasePackages(ResolutionContext ctx) {
        Map<String, Integer> pkgCount = new HashMap<>();
        for (String fqn : ctx.classMap.keySet()) {
            String[] parts = fqn.split("\\.");
            if (parts.length >= 2) {
                String base = parts[0] + "." + parts[1];
                pkgCount.merge(base, 1, Integer::sum);
            }
        }
        if (pkgCount.isEmpty()) return List.of();

        String topPkg = pkgCount.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(null);

        if (topPkg != null) {
            log.info("Auto-detected base package '{}' from {} indexed classes", topPkg, pkgCount.get(topPkg));
            return List.of(topPkg);
        }
        return List.of();
    }

    // ========== Repository -> Collection Resolution ==========

    void resolveRepositoryCollections(ResolutionContext ctx) {
        for (IndexedClass ic : ctx.classMap.values()) {
            if (!"REPOSITORY".equals(ic.stereotype) && !"SPRING_DATA".equals(ic.stereotype)) continue;

            // Priority 1: Direct entity type from generic signature (MongoRepository<Entity, ID>)
            if (ic.repositoryEntityType != null) {
                String coll = ctx.entityCollectionMap.get(ic.repositoryEntityType);
                // Also try simple name
                if (coll == null) {
                    String simpleName = ic.repositoryEntityType.contains(".")
                            ? ic.repositoryEntityType.substring(ic.repositoryEntityType.lastIndexOf('.') + 1)
                            : ic.repositoryEntityType;
                    coll = ctx.entityCollectionMap.get(simpleName);
                }
                if (coll != null) {
                    ctx.repoCollectionMap.put(ic.fqn, coll);
                    ctx.repoCollectionMap.put(ic.simpleName, coll);
                    log.debug("Repo {} → entity {} → collection {} (generic signature)",
                            ic.simpleName, ic.repositoryEntityType, coll);
                }
            }

            // Priority 1.5: Entity type known but not in entityCollectionMap —
            // try CamelCase → UPPER_SNAKE convention (e.g., ClmTransEntity → CLM_TRANS)
            if (!ctx.repoCollectionMap.containsKey(ic.simpleName) && ic.repositoryEntityType != null) {
                String entitySimple = ic.repositoryEntityType.contains(".")
                        ? ic.repositoryEntityType.substring(ic.repositoryEntityType.lastIndexOf('.') + 1)
                        : ic.repositoryEntityType;
                String entityStem = entitySimple.replaceAll("(Entity|Document|Model|Dto|DO)$", "");
                String upperSnake = entityStem.replaceAll("([a-z])([A-Z])", "$1_$2").toUpperCase();
                if (configLoader.isLikelyCollectionName(upperSnake)) {
                    ctx.repoCollectionMap.put(ic.fqn, upperSnake);
                    ctx.repoCollectionMap.put(ic.simpleName, upperSnake);
                    log.debug("Repo {} → entity {} → derived collection {} (name convention)",
                            ic.simpleName, ic.repositoryEntityType, upperSnake);
                }
            }

            // Priority 2: Stem-based matching (ClaimRepository → Claim → entityCollectionMap)
            if (!ctx.repoCollectionMap.containsKey(ic.simpleName)) {
                List<String> stems = new ArrayList<>();
                for (String suffix : configLoader.getRepositorySuffixes()) {
                    if (ic.simpleName.endsWith(suffix)) {
                        stems.add(ic.simpleName.substring(0, ic.simpleName.length() - suffix.length()));
                    }
                }
                for (String stem : stems) {
                    for (Map.Entry<String, String> entry : ctx.entityCollectionMap.entrySet()) {
                        if (entry.getKey().endsWith(stem) || entry.getKey().equals(stem)) {
                            ctx.repoCollectionMap.put(ic.fqn, entry.getValue());
                            ctx.repoCollectionMap.put(ic.simpleName, entry.getValue());
                            break;
                        }
                    }
                    if (ctx.repoCollectionMap.containsKey(ic.simpleName)) break;
                }
            }

            for (IndexedMethod m : ic.methods.values()) {
                for (AnnotationData ad : m.annotationDetailList) {
                    if (configLoader.getQueryAnnotations().contains(ad.name())) {
                        for (Object val : ad.attributes().values()) {
                            for (String s : configLoader.flattenAnnotationValue(val)) {
                                // Only use targeted pipeline extraction (from/into/coll contexts)
                                // Do NOT use extractCollectionRefs — it matches foreignField, $project aliases, etc.
                                for (String ref : configLoader.extractPipelineCollectionRefs(s)) {
                                    if (!"Other".equals(configLoader.detectCollectionDomain(ref))) {
                                        ctx.repoCollectionMap.putIfAbsent(ic.fqn, ref);
                                        ctx.repoCollectionMap.putIfAbsent(ic.simpleName, ref);
                                    }
                                }
                            }
                        }
                    }
                }
                for (String lit : m.stringLiterals) {
                    if (lit.contains("_") && !"Other".equals(configLoader.detectCollectionDomain(lit))) {
                        ctx.repoCollectionMap.putIfAbsent(ic.fqn, lit);
                        ctx.repoCollectionMap.putIfAbsent(ic.simpleName, lit);
                    }
                }
            }
            for (String fc : ic.fieldConstants) {
                if (!"Other".equals(configLoader.detectCollectionDomain(fc))) {
                    ctx.repoCollectionMap.putIfAbsent(ic.fqn, fc);
                    ctx.repoCollectionMap.putIfAbsent(ic.simpleName, fc);
                }
            }
        }

        // JPA Repository → Table resolution (parallel to MongoDB repo-collection above)
        for (IndexedClass ic : ctx.classMap.values()) {
            if (!"REPOSITORY".equals(ic.stereotype)) continue;
            if (ctx.repoTableMap.containsKey(ic.simpleName)) continue;
            if (ic.repositoryEntityType != null && !ctx.entityTableMap.isEmpty()) {
                String table = ctx.entityTableMap.get(ic.repositoryEntityType);
                if (table == null) {
                    String simpleName = ic.repositoryEntityType.contains(".")
                            ? ic.repositoryEntityType.substring(ic.repositoryEntityType.lastIndexOf('.') + 1)
                            : ic.repositoryEntityType;
                    table = ctx.entityTableMap.get(simpleName);
                }
                if (table != null) {
                    ctx.repoTableMap.put(ic.fqn, table);
                    ctx.repoTableMap.put(ic.simpleName, table);
                    log.debug("JPA Repo {} → entity {} → table {} (generic signature)",
                            ic.simpleName, ic.repositoryEntityType, table);
                }
            }
        }

        log.info("Resolved {} entity-collection, {} repo-collection, {} entity-table, {} repo-table mappings",
                ctx.entityCollectionMap.size(), ctx.repoCollectionMap.size(),
                ctx.entityTableMap.size(), ctx.repoTableMap.size());
    }

    // ========== Populate collection data on a CallNode ==========

    // Methods whose string arguments are NOT collection names (sequence IDs, log strings, etc.)
    private static final Set<String> NON_COLLECTION_METHODS = Set.of(
            // Sequence generators
            "getSequenceId", "getNextSequenceValue", "getNextVal", "nextVal",
            "getNextId", "generateSequence", "incrementAndGet",
            // Extension/API handlers
            "handleClaimAPIExtensions", "handleAPIExtensions",
            // Logging
            "info", "debug", "warn", "error", "trace", "log", "logError", "logWarning", "logInfo",
            // String formatting / IO
            "format", "printf", "println", "append", "write", "print",
            // Reflection / metadata access (not DB operations)
            "getCollectionName", "getCollectionNameFromClass",
            // In-memory utility methods
            "createBatches", "createSavepoint", "partitionList",
            // Object utilities
            "toString", "hashCode", "equals", "valueOf", "compareTo", "fromString", "parse",
            // Property/config access
            "getProperty", "setProperty", "containsKey", "containsValue",
            "getConfig", "loadProperty", "resolveProperty", "getValue", "getEnv",
            // Cache/session operations
            "put", "get", "remove", "evict", "putIfAbsent", "getIfPresent",
            // Event/messaging (string args are event names, not collections)
            "sendMessage", "publishEvent", "emit", "publish", "send", "broadcast",
            "addEventListener", "addListener", "removeListener", "removeEventListener",
            // Validation/exception
            "validate", "check", "throwException", "assertNotNull", "requireNonNull",
            // Enum/constant lookup
            "getEnumValue", "lookup", "resolve", "forName",
            // Annotation/metadata
            "annotation", "with", "metadata", "getAttribute", "setAttribute"
    );

    // Suffixes that disqualify a string literal from being a collection name.
    // E.g., "CLM_TRANS_TIMEOUT", "CLM_TRANS_ERROR" — these are config/error constants, not collections.
    private static final Set<String> NON_COLLECTION_SUFFIXES = Set.of(
            "_TIMEOUT", "_CONFIG", "_PROPERTY", "_PROPERTIES", "_MESSAGE", "_MSG",
            "_EVENT", "_EXCEPTION", "_ERROR", "_ERRORS", "_CONSTANT", "_CACHE",
            "_SESSION", "_KEY", "_KEYS", "_PREFIX", "_SUFFIX", "_FORMAT",
            "_LOG", "_LOGGER", "_HANDLER", "_LISTENER", "_FACTORY", "_BUILDER",
            "_TYPE", "_STATUS", "_STATE", "_FLAG", "_MODE", "_LEVEL",
            "_FAILED", "_SUCCESS", "_ACTIVE", "_INACTIVE", "_PENDING",
            "_COUNT", "_LIMIT", "_SIZE", "_LENGTH", "_INDEX", "_OFFSET",
            "_PATH", "_URL", "_URI", "_HOST", "_PORT", "_NAME", "_LABEL",
            "_TEMPLATE", "_PATTERN", "_REGEX", "_SEPARATOR", "_DELIMITER",
            "_QUERY", "_CONTEXT", "_CRITERIA", "_FILTER", "_SORT", "_PROJECTION",
            "_HEADER", "_FOOTER", "_BODY", "_PAYLOAD", "_RESPONSE", "_REQUEST",
            "_DESCRIPTION", "_REASON", "_DETAIL", "_DETAILS", "_REPORT"
    );

    void collectCollections(IndexedClass cls, IndexedMethod method, CallNode node, ResolutionContext ctx) {
        List<String> collections = new ArrayList<>();
        Map<String, String> collDomains = new LinkedHashMap<>();
        Map<String, String> collSources = new LinkedHashMap<>();
        Set<String> detectedOpTypes = new HashSet<>();

        // Known entity field names (from @Field/@BsonProperty) — these are NOT collections.
        Set<String> fieldNameExclusions = ctx.knownEntityFieldNames;

        // Build exclusion set: strings used as arguments to sequence/logging/utility methods
        Set<String> nonCollectionArgs = buildNonCollectionArgs(method);

        // From repository mapping
        if ("REPOSITORY".equals(cls.stereotype) || "SPRING_DATA".equals(cls.stereotype)) {
            String coll = ctx.repoCollectionMap.get(cls.fqn);
            if (coll == null) coll = ctx.repoCollectionMap.get(cls.simpleName);
            if (coll != null && !collections.contains(coll) && !fieldNameExclusions.contains(coll)) {
                collections.add(coll);
                collDomains.put(coll, configLoader.detectCollectionDomain(coll));
                collSources.put(coll, "REPOSITORY_MAPPING");
            }
        }

        // From @Document on class — always trust this (explicit annotation)
        if (cls.documentCollection != null && !collections.contains(cls.documentCollection)) {
            collections.add(cls.documentCollection);
            collDomains.put(cls.documentCollection, configLoader.detectCollectionDomain(cls.documentCollection));
            collSources.put(cls.documentCollection, "DOCUMENT_ANNOTATION");
        }

        // From method string literals -- only if they match a known domain prefix (not "Other")
        // AND are not known entity field names AND not arguments to sequence/log methods
        // AND don't end with a non-collection suffix (e.g., _TIMEOUT, _ERROR, _CONFIG)
        for (String lit : method.stringLiterals) {
            if (lit.contains("_") && !collections.contains(lit)
                    && !fieldNameExclusions.contains(lit) && !nonCollectionArgs.contains(lit)
                    && !hasNonCollectionSuffix(lit)) {
                String domain = configLoader.detectCollectionDomain(lit);
                if (!"Other".equals(domain)) {
                    collections.add(lit);
                    collDomains.put(lit, domain);
                    collSources.put(lit, "STRING_LITERAL");
                }
            }
        }

        // From class field constants — only for REPOSITORY/SPRING_DATA classes where
        // static final String constants genuinely hold collection names.
        // Skip for SERVICE/COMPONENT classes where constants may be used for other purposes
        // (e.g. field names, config keys) and leak as false positives to all methods.
        if ("REPOSITORY".equals(cls.stereotype) || "SPRING_DATA".equals(cls.stereotype)) {
            for (String fc : cls.fieldConstants) {
                if (!collections.contains(fc) && !fieldNameExclusions.contains(fc)) {
                    String domain = configLoader.detectCollectionDomain(fc);
                    if (!"Other".equals(domain)) {
                        collections.add(fc);
                        collDomains.put(fc, domain);
                        collSources.put(fc, "FIELD_CONSTANT");
                    }
                }
            }
        }

        // From annotation values (@Query, @Aggregation, etc.)
        // ONLY use extractPipelineCollectionRefs (targeted: from/into/coll contexts).
        // Do NOT use extractCollectionRefs here — it matches ANY UPPER_CASE_STRING
        // which causes false positives from foreignField, localField, $project aliases, etc.
        for (AnnotationData ad : method.annotationDetailList) {
            for (Object val : ad.attributes().values()) {
                for (String s : configLoader.flattenAnnotationValue(val)) {
                    for (String ref : configLoader.extractPipelineCollectionRefs(s)) {
                        if (!collections.contains(ref) && !fieldNameExclusions.contains(ref)) {
                            String domain = configLoader.detectCollectionDomain(ref);
                            if (!"Other".equals(domain)) {
                                collections.add(ref);
                                collDomains.put(ref, domain);
                                collSources.put(ref, "PIPELINE_ANNOTATION");
                            }
                        }
                    }
                }
            }
        }

        // From MongoMethodDetector: BulkWriteCollector, MongoTemplate, native driver, reactive
        for (MongoMethodDetector.DetectedCollection dc : mongoMethodDetector.detect(method, cls, ctx.entityCollectionMap)) {
            if (dc.operationType() != null) detectedOpTypes.add(dc.operationType());
            if (!collections.contains(dc.collectionName()) && !fieldNameExclusions.contains(dc.collectionName())) {
                String domain = configLoader.detectCollectionDomain(dc.collectionName());
                if (!"Other".equals(domain)) {
                    collections.add(dc.collectionName());
                    collDomains.put(dc.collectionName(), domain);
                    collSources.put(dc.collectionName(), dc.source());
                }
            }
        }

        // From AggregationDetector: pipeline stages ($lookup, $merge, $out), Spring Aggregation API
        AggregationDetector.PipelineDetail pipelineDetail = new AggregationDetector.PipelineDetail();
        for (AggregationDetector.DetectedPipelineCollection dc : aggregationDetector.detect(method, cls, ctx, pipelineDetail)) {
            if (dc.operationType() != null) detectedOpTypes.add(dc.operationType());
            if (!collections.contains(dc.collectionName()) && !fieldNameExclusions.contains(dc.collectionName())) {
                String domain = configLoader.detectCollectionDomain(dc.collectionName());
                if (!"Other".equals(domain)) {
                    collections.add(dc.collectionName());
                    collDomains.put(dc.collectionName(), domain);
                    collSources.put(dc.collectionName(), dc.source());
                }
            }
        }
        // Attach rich pipeline detail to CallNode if any stages were detected
        if (!pipelineDetail.stages.isEmpty() || !pipelineDetail.lookupTargets.isEmpty()
                || !pipelineDetail.pipelineSourceMethods.isEmpty()) {
            Map<String, Object> pipeline = new LinkedHashMap<>();
            pipeline.put("stages", pipelineDetail.stages);
            if (!pipelineDetail.lookupTargets.isEmpty())
                pipeline.put("lookupTargets", pipelineDetail.lookupTargets);
            if (!pipelineDetail.matchFields.isEmpty())
                pipeline.put("matchFields", new ArrayList<>(pipelineDetail.matchFields));
            if (!pipelineDetail.groupFields.isEmpty())
                pipeline.put("groupFields", new ArrayList<>(pipelineDetail.groupFields));
            if (!pipelineDetail.projectionFields.isEmpty())
                pipeline.put("projectionFields", new ArrayList<>(pipelineDetail.projectionFields));
            if (!pipelineDetail.sortFields.isEmpty())
                pipeline.put("sortFields", new ArrayList<>(pipelineDetail.sortFields));
            if (!pipelineDetail.companionUsage.isEmpty())
                pipeline.put("companionUsage", pipelineDetail.companionUsage);
            if (!pipelineDetail.detectionSources.isEmpty())
                pipeline.put("detectionSources", new ArrayList<>(pipelineDetail.detectionSources));
            if (pipelineDetail.isDynamic)
                pipeline.put("isDynamic", true);
            if (!pipelineDetail.pipelineSourceMethods.isEmpty())
                pipeline.put("pipelineSourceMethods", pipelineDetail.pipelineSourceMethods);
            node.setAggregationPipeline(pipeline);
        }

        // From JpaMethodDetector: JdbcTemplate, EntityManager, @Query, derived queries
        List<String> capturedSql = new ArrayList<>();
        for (JpaMethodDetector.DetectedTable dt : jpaMethodDetector.detect(method, cls, ctx.entityTableMap, ctx.namedQueryMap)) {
            if (dt.operationType() != null) detectedOpTypes.add(dt.operationType());
            if (!collections.contains(dt.tableName())) {
                collections.add(dt.tableName());
                String domain = configLoader.detectCollectionDomain(dt.tableName());
                collDomains.put(dt.tableName(), domain);
                collSources.put(dt.tableName(), dt.source());
            }
            // Capture raw SQL text for display in UI
            if (dt.sqlText() != null && !dt.sqlText().isBlank() && !capturedSql.contains(dt.sqlText())) {
                capturedSql.add(dt.sqlText());
            }
        }
        if (!capturedSql.isEmpty()) node.setSqlStatements(capturedSql);

        // From JPA repository mapping (parallel to MongoDB repo mapping above)
        if ("REPOSITORY".equals(cls.stereotype)) {
            String table = ctx.repoTableMap.get(cls.fqn);
            if (table == null) table = ctx.repoTableMap.get(cls.simpleName);
            if (table != null && !collections.contains(table)) {
                collections.add(table);
                collDomains.put(table, configLoader.detectCollectionDomain(table));
                collSources.put(table, "JPA_REPOSITORY_MAPPING");
            }
        }

        // From @Entity @Table on the class itself
        if (cls.isJpaEntity && cls.jpaTableName != null && !collections.contains(cls.jpaTableName)) {
            collections.add(cls.jpaTableName);
            collDomains.put(cls.jpaTableName, configLoader.detectCollectionDomain(cls.jpaTableName));
            collSources.put(cls.jpaTableName, "JPA_TABLE_ANNOTATION");
        }

        // Mark node as DB-interacting if it calls MongoTemplate/MongoOperations/native driver
        // or JPA/JDBC methods, even when the target collection couldn't be resolved.
        if (collections.isEmpty()
                && (mongoMethodDetector.hasDbInvocations(method) || jpaMethodDetector.hasJpaInvocations(method))) {
            node.setHasDbInteraction(true);
        }

        // From @Value property keys resolved via config files
        if (!cls.valuePropertyKeys.isEmpty() && ctx.configProperties != null && !ctx.configProperties.isEmpty()) {
            for (String key : cls.valuePropertyKeys) {
                String resolved = ctx.configProperties.get(key);
                if (resolved != null && !collections.contains(resolved) && !fieldNameExclusions.contains(resolved)) {
                    String domain = configLoader.detectCollectionDomain(resolved);
                    if (!"Other".equals(domain)) {
                        collections.add(resolved);
                        collDomains.put(resolved, domain);
                        collSources.put(resolved, "VALUE_PROPERTY");
                    }
                }
            }
        }

        // From Environment.getProperty() calls — resolve property key from config
        if (ctx.configProperties != null && !ctx.configProperties.isEmpty()) {
            for (InvocationRef inv : method.invocations) {
                if ("getProperty".equals(inv.methodName())
                        && inv.ownerClass() != null
                        && (inv.ownerClass().endsWith("Environment") || inv.ownerClass().endsWith("ConfigurableEnvironment"))) {
                    for (String arg : inv.stringArgs()) {
                        String resolved = ctx.configProperties.get(arg);
                        if (resolved != null && !collections.contains(resolved) && !fieldNameExclusions.contains(resolved)) {
                            String domain = configLoader.detectCollectionDomain(resolved);
                            if (!"Other".equals(domain)) {
                                collections.add(resolved);
                                collDomains.put(resolved, domain);
                                collSources.put(resolved, "ENV_PROPERTY");
                            }
                        }
                    }
                }
            }
        }

        // From enum/constants class references — resolve GETSTATIC on enum fields to their values
        if (!ctx.enumConstantsMap.isEmpty()) {
            // Check method invocation targets (calls to enum/constants class methods)
            Set<String> constantOwners = new HashSet<>();
            for (InvocationRef inv : method.invocations) {
                if (ctx.enumConstantsMap.containsKey(inv.ownerClass())) {
                    constantOwners.add(inv.ownerClass());
                }
            }
            // Check GETSTATIC field references (direct access to constants fields)
            if (method.staticFieldRefOwners != null) {
                for (String owner : method.staticFieldRefOwners) {
                    if (ctx.enumConstantsMap.containsKey(owner)) {
                        constantOwners.add(owner);
                    }
                }
            }
            for (String owner : constantOwners) {
                for (String val : ctx.enumConstantsMap.get(owner)) {
                    if (!collections.contains(val) && !fieldNameExclusions.contains(val)) {
                        String domain = configLoader.detectCollectionDomain(val);
                        if (!"Other".equals(domain)) {
                            collections.add(val);
                            collDomains.put(val, domain);
                            collSources.put(val, "ENUM_CONSTANT");
                        }
                    }
                }
            }
        }

        // From implicit collection mappings (known cross-JAR patterns like validators, sequence service)
        Map<String, List<Map<String, String>>> implicitMap = configLoader.getImplicitCollections();
        if (implicitMap != null && !implicitMap.isEmpty()) {
            // Check current node: ClassName.methodName
            String nodeKey = cls.simpleName + "." + method.name;
            addImplicitCollections(nodeKey, implicitMap, collections, collDomains, collSources, fieldNameExclusions);
            // Check invocations: if this method calls a known implicit method
            for (InvocationRef inv : method.invocations) {
                String owner = inv.ownerClass();
                if (owner == null || owner.isEmpty()) continue;
                String simpleName = owner.contains(".")
                        ? owner.substring(owner.lastIndexOf('.') + 1) : owner;
                String invKey = simpleName + "." + inv.methodName();
                addImplicitCollections(invKey, implicitMap, collections, collDomains, collSources, fieldNameExclusions);
            }
        }

        // Post-filter: when catalog is available, remove STRING_LITERAL-sourced items
        // that are NOT in the live database (sequence names, log strings, etc.)
        if (ctx.mongoCatalogCollections != null && !collections.isEmpty()) {
            List<String> toRemove = new ArrayList<>();
            for (String coll : collections) {
                String source = collSources.get(coll);
                if (!catalogContains(ctx.mongoCatalogCollections, coll)
                        && ("STRING_LITERAL".equals(source) || "FIELD_CONSTANT".equals(source)
                            || "ENUM_CONSTANT".equals(source))) {
                    toRemove.add(coll);
                }
            }
            for (String coll : toRemove) {
                collections.remove(coll);
                collDomains.remove(coll);
                collSources.remove(coll);
            }
        }

        node.setCollectionsAccessed(collections);
        node.setCollectionDomains(collDomains);
        node.setCollectionSources(collSources);

        // Always add all detected operation types to the node's set
        node.getOperationTypes().addAll(detectedOpTypes);
        // Set primary operationType if not already set
        if (node.getOperationType() == null && !detectedOpTypes.isEmpty()) {
            node.setOperationType(pickHighestPriorityOp(detectedOpTypes));
        }

        // Verify collections against live MongoDB catalog (supplementary — not a gate)
        // Uses case-insensitive matching: @Document("clm_child_locks") matches catalog "CLM_CHILD_LOCKS"
        if (!collections.isEmpty()) {
            Map<String, String> verification = new LinkedHashMap<>();
            if (ctx.mongoCatalogCollections != null) {
                for (String coll : collections) {
                    verification.put(coll, catalogContains(ctx.mongoCatalogCollections, coll) ? "VERIFIED" : "NOT_IN_DB");
                }
            } else {
                for (String coll : collections) {
                    verification.put(coll, "NO_CATALOG");
                }
            }
            node.setCollectionVerification(verification);
        }

        // Attach entity field mappings for collections that have them
        Map<String, Map<String, String>> fieldMaps = null;
        for (String coll : collections) {
            Map<String, String> mapping = ctx.collectionFieldMappings.get(coll);
            if (mapping != null && !mapping.isEmpty()) {
                if (fieldMaps == null) fieldMaps = new LinkedHashMap<>();
                fieldMaps.put(coll, mapping);
            }
        }
        if (fieldMaps != null) node.setEntityFieldMappings(fieldMaps);
    }

    /** Add implicit collections from known cross-JAR patterns (validators, sequence service, etc.). */
    private void addImplicitCollections(String key, Map<String, List<Map<String, String>>> implicitMap,
                                         List<String> collections, Map<String, String> collDomains,
                                         Map<String, String> collSources, Set<String> fieldNameExclusions) {
        List<Map<String, String>> mappings = implicitMap.get(key);
        if (mappings == null) return;
        for (Map<String, String> mapping : mappings) {
            String coll = mapping.get("collection");
            String source = mapping.get("source");
            if (coll != null && !collections.contains(coll) && !fieldNameExclusions.contains(coll)) {
                collections.add(coll);
                collDomains.put(coll, configLoader.detectCollectionDomain(coll));
                collSources.put(coll, source != null ? source : "IMPLICIT");
            }
        }
    }

    /** Check if a string literal ends with a non-collection suffix (e.g., _TIMEOUT, _ERROR). */
    private boolean hasNonCollectionSuffix(String literal) {
        String upper = literal.toUpperCase();
        for (String suffix : NON_COLLECTION_SUFFIXES) {
            if (upper.endsWith(suffix)) return true;
        }
        return false;
    }

    /** Build set of strings that are arguments to sequence/log/utility methods (not collections). */
    private Set<String> buildNonCollectionArgs(IndexedMethod method) {
        Set<String> excluded = new HashSet<>();
        for (InvocationRef inv : method.invocations) {
            if (NON_COLLECTION_METHODS.contains(inv.methodName())) {
                for (String arg : inv.stringArgs()) {
                    if (!arg.startsWith("__class:")) excluded.add(arg);
                }
            }
        }
        return excluded;
    }

    /** Case-insensitive check if catalog contains a collection name. */
    private static boolean catalogContains(Set<String> catalog, String collName) {
        if (catalog.contains(collName)) return true;
        return catalog.contains(collName.toUpperCase()) || catalog.contains(collName.toLowerCase());
    }

    /** Pick highest-priority operation type from a set of detected types. */
    private static String pickHighestPriorityOp(Set<String> ops) {
        for (String op : List.of("AGGREGATE", "DELETE", "UPDATE", "WRITE", "COUNT", "READ")) {
            if (ops.contains(op)) return op;
        }
        return null;
    }
}
