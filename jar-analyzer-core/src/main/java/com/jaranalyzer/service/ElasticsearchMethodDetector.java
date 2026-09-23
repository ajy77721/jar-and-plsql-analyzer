package com.jaranalyzer.service;

import com.jaranalyzer.model.MethodCallInfo;
import com.jaranalyzer.service.CallGraphIndex.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Detects Elasticsearch index access from method invocations in bytecode.
 * Analyzes InvocationRefs to find index names from:
 *
 *   A. ElasticsearchRestTemplate / ElasticsearchTemplate / ElasticsearchOperations / Reactive variants
 *   B. ElasticsearchClient (new Java client)
 *
 * Index name extraction:
 *   - From first string arg (direct index-name string)
 *   - From @Document(indexName="X") on the entity type via entityCollectionMap
 */
@Component
class ElasticsearchMethodDetector {

    private static final Logger log = LoggerFactory.getLogger(ElasticsearchMethodDetector.class);

    /** Result of detecting an Elasticsearch index access from a method invocation. */
    record DetectedCollection(String collectionName, String source, String operationType) {}

    /** Both resolved-literal and dynamic (variable-sourced) detections from a single pass.
     *  CALL-011: mirrors MongoMethodDetector.DetectionResult — ES's traced value (an index name)
     *  already IS the reportable value, same shape as Mongo's collection name, so this reuses the
     *  domain-agnostic CallGraphIndex.DetectedDynamicCollection capture, tagged with an "ES_"-
     *  prefixed varKind so it can never collide with Mongo's own "PARAM"/"PARAM_CLASS" entries on
     *  the same CallNode.dynamicCollectionRefs list. */
    record DetectionResult(List<DetectedCollection> resolved, List<DetectedDynamicCollection> dynamic) {}

    /** CALL-011: call-site resolution result for an Elasticsearch index-name argument — mirrors
     *  MongoMethodDetector.CallSiteCollection. */
    record CallSiteIndex(String indexName, MethodCallInfo.DynamicArgSource dynamicArg) {}

    // Owner class name patterns for Template / Operations types
    private static final List<String> TEMPLATE_OWNERS = List.of(
            "ElasticsearchRestTemplate", "ElasticsearchTemplate",
            "ElasticsearchOperations", "ReactiveElasticsearchOperations"
    );

    // Owner class name patterns for the new Java client
    private static final List<String> CLIENT_OWNERS = List.of(
            "ElasticsearchClient"
    );

    // Method → operation type mapping
    private static final Map<String, String> METHOD_OP_MAP = Map.ofEntries(
            // WRITE
            Map.entry("index",                "WRITE"),
            Map.entry("save",                 "WRITE"),
            Map.entry("bulkIndex",            "WRITE"),
            // READ
            Map.entry("get",                  "READ"),
            Map.entry("search",               "READ"),
            Map.entry("searchForStream",      "READ"),
            Map.entry("multiGet",             "READ"),
            Map.entry("searchScrollContinue", "READ"),
            Map.entry("count",                "READ"),
            // DELETE
            Map.entry("delete",               "DELETE"),
            Map.entry("bulkDelete",           "DELETE"),
            // UPDATE
            Map.entry("update",               "UPDATE"),
            Map.entry("bulkUpdate",           "UPDATE")
    );

    private static final Set<String> ALL_METHODS = METHOD_OP_MAP.keySet();

    /**
     * Scan all invocations in a method for Elasticsearch index access patterns.
     *
     * @param entityCollectionMap  entity FQN / simple name → index name
     *                             (populated from @Document(indexName="X") on entity classes)
     */
    List<DetectedCollection> detect(IndexedClass cls, IndexedMethod method,
                                    List<InvocationRef> invocations,
                                    Map<String, String> entityCollectionMap) {
        return detectWithDynamic(cls, method, invocations, entityCollectionMap, Map.of()).resolved();
    }

    /** CALL-011: same scan as {@link #detect}, but also captures an unresolved dynamic
     *  (variable/field-sourced) index-name argument instead of silently falling back to
     *  "es:unknown-index" — the callee-body half of cross-method value tracing (mirrors
     *  MongoMethodDetector's section-B unresolved-template-call handling).
     *
     * @param entityCollectionMap  legacy/shared entity map (Mongo's {@code @Document(collection=)}
     *                             attribute) — kept as a fallback for the rare case a project reuses
     *                             it for ES entities too, but CALL-017's own {@code entityIndexMap}
     *                             is always checked FIRST.
     * @param entityIndexMap       CALL-017: entity FQN/simpleName → index name, populated
     *                             specifically from {@code @Document(indexName = "...")} — the
     *                             CONFIRMED ground truth for Elasticsearch, distinct from Mongo's
     *                             {@code collection} attribute even though both share the same
     *                             annotation simple name.
     */
    DetectionResult detectWithDynamic(IndexedClass cls, IndexedMethod method,
                                       List<InvocationRef> invocations,
                                       Map<String, String> entityCollectionMap,
                                       Map<String, String> entityIndexMap) {
        List<DetectedCollection> results = new ArrayList<>();
        List<DetectedDynamicCollection> dynamicResults = new ArrayList<>();
        Map<String, String> indexMap = entityIndexMap != null ? entityIndexMap : Map.of();

        for (InvocationRef inv : invocations) {
            String owner = inv.ownerClass();
            String methodName = inv.methodName();
            List<String> stringArgs = inv.stringArgs();

            if (!ALL_METHODS.contains(methodName)) continue;

            boolean isTemplate = ownerMatchesAny(owner, TEMPLATE_OWNERS);
            boolean isClient   = ownerMatchesAny(owner, CLIENT_OWNERS);
            if (!isTemplate && !isClient) continue;

            String opType = METHOD_OP_MAP.get(methodName);
            String source = isClient ? "ELASTICSEARCH_CLIENT" : "ELASTICSEARCH_TEMPLATE";

            // Attempt 1: direct string arg (index name passed explicitly)
            String indexName = firstStringArg(stringArgs);

            // Attempt 2: entity class literal → entityIndexMap (@Document(indexName=...)) first,
            // falling back to entityCollectionMap (Mongo's collection attribute, shared corpus
            // convenience) only when no ES-specific ground truth exists for that class.
            if (indexName == null && stringArgs != null) {
                for (String arg : stringArgs) {
                    if (arg != null && arg.startsWith("__class:")) {
                        String fqn = arg.substring(8);
                        indexName = resolveIndex(fqn, indexMap);
                        if (indexName == null && !entityCollectionMap.isEmpty()) {
                            indexName = resolveIndex(fqn, entityCollectionMap);
                        }
                        if (indexName != null) break;
                    }
                }
            }

            // Attempt 3: entity class from the enclosing class @Document (for save(entity) patterns)
            if (indexName == null && cls.documentCollection != null) {
                indexName = cls.documentCollection;
            }

            if (indexName != null) {
                results.add(new DetectedCollection(indexName, source, opType));
                log.debug("Elasticsearch detected: index={} op={} owner={} method={}", indexName, opType, owner, methodName);
                continue;
            }

            // CALL-011: nothing resolvable at THIS call site — capture a dynamic (variable/field
            // -sourced) reference (if any) so a caller-side hop can resolve it later, same
            // contract as JpaMethodDetector's SQL_PARAM / MongoMethodDetector's PARAM entries.
            // "ES_"-prefixed varKind keeps this list disjoint from Mongo/JDBC entries on the same
            // dynamicCollectionRefs list.
            MethodCallInfo.DynamicArgSource dyn = firstDynamicArg(inv.dynamicArgSources());
            if (dyn != null) {
                dynamicResults.add(new DetectedDynamicCollection(dyn.name(), "ES_" + dyn.kind(), dyn.ownerClass(),
                        source + "_DYNAMIC", source, opType));
            } else {
                results.add(new DetectedCollection("es:unknown-index", source, opType));
            }
        }

        return new DetectionResult(results, dynamicResults);
    }

    /** CALL-011: single-hop call-site resolution of an Elasticsearch index-name argument — mirrors
     *  MongoMethodDetector.resolveGenericCallSiteArg. Tries a direct string literal, then a
     *  __class: literal via entityCollectionMap, then falls back to the first captured dynamic
     *  (variable/field) argument source. */
    CallSiteIndex resolveGenericCallSiteArg(InvocationRef call, Map<String, String> entityCollectionMap) {
        List<String> stringArgs = call.stringArgs();
        String index = firstStringArg(stringArgs);
        if (index == null && stringArgs != null && entityCollectionMap != null && !entityCollectionMap.isEmpty()) {
            for (String arg : stringArgs) {
                if (arg != null && arg.startsWith("__class:")) {
                    index = resolveIndex(arg.substring(8), entityCollectionMap);
                    if (index != null) break;
                }
            }
        }
        if (index != null) return new CallSiteIndex(index, null);
        MethodCallInfo.DynamicArgSource dyn = firstDynamicArg(call.dynamicArgSources());
        if (dyn != null) return new CallSiteIndex(null, dyn);
        return null;
    }

    /** First captured dynamic (variable/field-sourced) argument, if any. */
    private MethodCallInfo.DynamicArgSource firstDynamicArg(List<MethodCallInfo.DynamicArgSource> args) {
        if (args == null || args.isEmpty()) return null;
        return args.get(0);
    }

    /**
     * Quick check: does this method invoke any Elasticsearch methods?
     */
    boolean hasElasticsearchInvocations(IndexedMethod method) {
        for (InvocationRef inv : method.invocations) {
            String owner = inv.ownerClass();
            String mn = inv.methodName();
            if (ALL_METHODS.contains(mn)
                    && (ownerMatchesAny(owner, TEMPLATE_OWNERS) || ownerMatchesAny(owner, CLIENT_OWNERS))) {
                return true;
            }
        }
        return false;
    }

    /** Resolve an entity FQN or simple name to an Elasticsearch index via entityCollectionMap. */
    private String resolveIndex(String fqn, Map<String, String> entityCollectionMap) {
        if (fqn == null) return null;
        String index = entityCollectionMap.get(fqn);
        if (index != null) return index;
        int dot = fqn.lastIndexOf('.');
        if (dot >= 0) index = entityCollectionMap.get(fqn.substring(dot + 1));
        return index;
    }

    /** Return first non-blank, non-class string argument. */
    private String firstStringArg(List<String> stringArgs) {
        if (stringArgs == null) return null;
        for (String arg : stringArgs) {
            if (arg != null && !arg.startsWith("__class:") && !arg.isBlank()) return arg;
        }
        return null;
    }

    /** Check if the owner class name ends with any of the given patterns. */
    private boolean ownerMatchesAny(String owner, List<String> patterns) {
        if (owner == null) return false;
        for (String pattern : patterns) {
            if (owner.endsWith(pattern) || owner.endsWith("." + pattern)) return true;
        }
        return false;
    }
}
