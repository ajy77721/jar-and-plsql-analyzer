package com.jaranalyzer.service;

import com.jaranalyzer.model.MethodCallInfo;
import com.jaranalyzer.service.CallGraphIndex.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Deep aggregation pipeline detector — covers every way a MongoDB aggregation
 * pipeline can be constructed in Java:
 *
 *   A. Document-based: new Document("$lookup", new Document("from", "COLL"))
 *   B. Spring Data Aggregation API: LookupOperation, MatchOperation, GroupOperation, ...
 *   C. Native driver Aggregates factory: Aggregates.lookup("COLL", ...)
 *   D. BsonDocument: new BsonDocument().append("$lookup", ...)
 *   E. Document.parse(jsonString) / BasicDBObject.parse(jsonString)
 *   F. MongoTemplate.aggregate() / ReactiveMongoTemplate.aggregate()
 *   G. Filters/Projections/Sorts/Accumulators companion classes
 *   H. Cross-method pipeline tracing via return types
 *
 * Produces both DetectedPipelineCollection (collection-level) and
 * PipelineDetail (rich stage-level metadata for the UI).
 */
@Component
class AggregationDetector {

    private static final Logger log = LoggerFactory.getLogger(AggregationDetector.class);

    // Pipeline stage keywords that reference other collections
    private static final Set<String> PIPELINE_STAGE_KEYWORDS = Set.of(
            "$lookup", "$graphLookup", "$out", "$merge", "$unionWith"
    );

    // All recognized pipeline stage names (for stage detection)
    private static final Set<String> ALL_PIPELINE_STAGES = Set.of(
            "$lookup", "$graphLookup", "$match", "$group", "$project", "$sort",
            "$unwind", "$limit", "$skip", "$out", "$merge", "$unionWith",
            "$count", "$addFields", "$replaceRoot", "$facet", "$bucket",
            "$bucketAuto", "$sortByCount", "$sample", "$redact", "$set",
            "$unset", "$replaceWith", "$densify", "$fill"
    );

    // Document field keys whose VALUE is a collection name
    private static final Set<String> COLLECTION_VALUE_KEYS = Set.of("from", "into", "coll");

    // Values that are never real collection names even though they satisfy isCollectionName()'s
    // generic lowercase-word shape check — these are $lookup's OWN detail keys (localField/
    // foreignField/as/let/pipeline). They can land in a collection-VALUE slot purely by position
    // when the real value at that slot wasn't a compile-time literal (e.g. `.append("from", var)`
    // drops `var`, shifting the next literal — often the next append's own key, like "let" — into
    // the adjacent slot). Excluding by exact name, not by key context, is deliberate and safe: no
    // real MongoDB collection is ever named exactly "let"/"pipeline"/"as"/"localField"/"foreignField".
    private static final Set<String> LOOKUP_NON_COLLECTION_VALUES = Set.of(
            "let", "pipeline", "as", "localField", "foreignField"
    );

    // Spring Data Aggregation classes whose constructors/methods take collection name args, plus
    // known project-specific wrapper classes (e.g. BatchAggregationPaginationBuilder) that combine
    // aggregation with batching/pagination. Their ".collection(literal)" calls are NOT resolved
    // here though — see the class-name-agnostic scan further down — this map only matters for any
    // OTHER literal-bearing method these classes might expose.
    private static final Map<String, String> AGGREGATION_CLASSES = Map.ofEntries(
            Map.entry("LookupOperation", "AGGREGATE"),
            Map.entry("GraphLookupOperation", "AGGREGATE"),
            Map.entry("OutOperation", "WRITE"),
            Map.entry("MergeOperation", "WRITE"),
            Map.entry("UnionWithOperation", "AGGREGATE"),
            Map.entry("BatchAggregationPaginationBuilder", "AGGREGATE")
    );

    // Spring Data operation classes that indicate pipeline involvement (no collection arg)
    private static final Set<String> PIPELINE_OPERATION_CLASSES = Set.of(
            "MatchOperation", "GroupOperation", "ProjectionOperation", "SortOperation",
            "UnwindOperation", "LimitOperation", "SkipOperation", "BucketOperation",
            "FacetOperation", "ReplaceRootOperation", "AddFieldsOperation", "CountOperation",
            "SetOperation", "UnsetOperation", "DensifyOperation", "FillOperation",
            "RedactOperation", "SampleOperation", "SetWindowFieldsOperation",
            "AggregationUpdate", "TypedAggregation", "Aggregation"
    );

    // Methods on Spring Data / native-driver aggregation builders that take a collection name arg.
    // Gated by an owner-class-name heuristic below since these are common English words that show
    // up on plenty of unrelated fluent APIs. Deliberately excludes "collection" — see the
    // class-name-agnostic ".collection(literal)" scan further down, which exists precisely because
    // project-specific wrapper builders (custom batch/pagination/aggregation helpers) are named
    // differently per module and get renamed over time, so matching by class name is a losing game.
    private static final Set<String> AGGREGATION_BUILDER_METHODS = Set.of(
            "from", "into", "newLookup", "lookup", "graphLookup"
    );

    // Native MongoDB driver Aggregates factory methods
    private static final Map<String, String> AGGREGATES_FACTORY_METHODS = Map.ofEntries(
            Map.entry("lookup", "AGGREGATE"),    // Aggregates.lookup("from", "local", "foreign", "as")
            Map.entry("graphLookup", "AGGREGATE"),
            Map.entry("out", "WRITE"),           // Aggregates.out("COLL")
            Map.entry("merge", "WRITE"),         // Aggregates.merge("COLL")
            Map.entry("unionWith", "AGGREGATE"),
            Map.entry("match", "AGGREGATE"),
            Map.entry("group", "AGGREGATE"),
            Map.entry("project", "AGGREGATE"),
            Map.entry("sort", "AGGREGATE"),
            Map.entry("unwind", "AGGREGATE"),
            Map.entry("limit", "AGGREGATE"),
            Map.entry("skip", "AGGREGATE"),
            Map.entry("count", "AGGREGATE"),
            Map.entry("bucket", "AGGREGATE"),
            Map.entry("bucketAuto", "AGGREGATE"),
            Map.entry("facet", "AGGREGATE"),
            Map.entry("replaceRoot", "AGGREGATE"),
            Map.entry("addFields", "AGGREGATE"),
            Map.entry("sample", "AGGREGATE"),
            Map.entry("sortByCount", "AGGREGATE"),
            Map.entry("densify", "AGGREGATE"),
            Map.entry("fill", "AGGREGATE"),
            Map.entry("set", "AGGREGATE"),
            Map.entry("unset", "AGGREGATE")
    );

    // Companion classes whose methods enrich pipeline detail (not collection references)
    private static final Map<String, String> COMPANION_CLASSES = Map.of(
            "Filters", "MATCH",
            "Projections", "PROJECT",
            "Sorts", "SORT",
            "Accumulators", "GROUP",
            "Updates", "UPDATE"
    );

    // Regex for extracting $lookup detail from JSON-like strings
    private static final Pattern LOOKUP_FROM_PATTERN = Pattern.compile(
            "\\$(lookup|graphLookup)[^}]*?[\"']from[\"']\\s*:\\s*[\"']([A-Za-z][A-Za-z0-9_]+)[\"']");
    private static final Pattern LOOKUP_LOCAL_PATTERN = Pattern.compile(
            "[\"']localField[\"']\\s*:\\s*[\"']([^\"']+)[\"']");
    private static final Pattern LOOKUP_FOREIGN_PATTERN = Pattern.compile(
            "[\"']foreignField[\"']\\s*:\\s*[\"']([^\"']+)[\"']");
    private static final Pattern LOOKUP_AS_PATTERN = Pattern.compile(
            "[\"']as[\"']\\s*:\\s*[\"']([^\"']+)[\"']");
    private static final Pattern MATCH_FIELD_PATTERN = Pattern.compile(
            "\\$match[^}]*?[\"']([a-zA-Z][a-zA-Z0-9_.]+)[\"']\\s*:");
    private static final Pattern GROUP_ID_PATTERN = Pattern.compile(
            "\\$group[^}]*?[\"']_id[\"']\\s*:\\s*[\"']\\$?([a-zA-Z][a-zA-Z0-9_.]+)[\"']");

    private final DomainConfigLoader configLoader;

    AggregationDetector(DomainConfigLoader configLoader) {
        this.configLoader = configLoader;
    }

    record DetectedPipelineCollection(String collectionName, String source, String operationType) {}

    /**
     * Rich pipeline detail for a single aggregation method — ordered stages, lookup targets,
     * match predicates, group fields, companion usage, detection confidence.
     */
    static class PipelineDetail {
        List<Map<String, Object>> stages = new ArrayList<>();
        List<Map<String, String>> lookupTargets = new ArrayList<>();
        List<String> matchFields = new ArrayList<>();
        List<String> groupFields = new ArrayList<>();
        List<String> projectionFields = new ArrayList<>();
        List<String> sortFields = new ArrayList<>();
        Map<String, List<String>> companionUsage = new LinkedHashMap<>();
        List<String> detectionSources = new ArrayList<>();
        boolean isDynamic = false;
        List<String> pipelineSourceMethods = new ArrayList<>();
        // True when this method builds/uses an aggregation pipeline (e.g. via MatchOperation,
        // GroupOperation, SortOperation, Filters/Projections/Sorts/Accumulators companion classes)
        // even when NO stage carried a resolvable collection-name argument — e.g. a helper method
        // that only builds MatchOperation/GroupOperation stages for a pipeline assembled elsewhere.
        // Without this flag, such a method has zero collections AND zero DetectedPipelineCollection
        // results, so CollectionResolver never learns it does real aggregation work and the
        // cleanup pass nulls out its AGGREGATE operationType as a false positive.
        boolean hasPipelineInvolvement = false;
        // Dynamic (variable-sourced) collection refs — pipeline calls whose target collection
        // argument was a variable rather than a literal (e.g. Aggregates.out(targetCollection)).
        // Never merged into the resolved DetectedPipelineCollection list; see CollectionResolver.
        List<DetectedDynamicCollection> dynamicRefs = new ArrayList<>();
        // The collection this pipeline actually runs ON — as opposed to $lookup/$graphLookup
        // targets (joined in mid-pipeline) or $out/$merge targets (written to at the end). Set
        // whenever detect() can identify it unambiguously (mongoTemplate.aggregate(pipeline,
        // "coll"), a repository's own @Aggregation-annotated method, or an entity-mapped
        // Aggregation.newAggregation(Entity.class, ...) call). Null when detection only saw
        // pipeline-building calls with no resolvable base (e.g. a helper that only builds
        // MatchOperation/GroupOperation stages for a pipeline assembled elsewhere).
        String baseCollection = null;
    }

    /**
     * Detect pipeline collections AND build rich pipeline detail.
     */
    List<DetectedPipelineCollection> detect(IndexedMethod method, IndexedClass cls,
                                            CallGraphIndex.ResolutionContext ctx) {
        return detect(method, cls, ctx, null);
    }

    List<DetectedPipelineCollection> detect(IndexedMethod method, IndexedClass cls,
                                            CallGraphIndex.ResolutionContext ctx,
                                            PipelineDetail detail) {
        List<DetectedPipelineCollection> results = new ArrayList<>();
        boolean hasPipelineInvolvement = false;

        // Collect ALL string literals from this method for context scanning
        Set<String> allStagesFound = new LinkedHashSet<>();
        Set<String> allMatchFields = new LinkedHashSet<>();
        Set<String> allGroupFields = new LinkedHashSet<>();
        List<Map<String, String>> allLookups = new ArrayList<>();

        // Pre-scan all string literals for pipeline stage keywords
        for (String lit : method.stringLiterals) {
            for (String stage : ALL_PIPELINE_STAGES) {
                if (lit.contains(stage)) allStagesFound.add(stage);
            }
            extractLookupDetail(lit, allLookups);
            extractMatchFields(lit, allMatchFields);
            extractGroupFields(lit, allGroupFields);
        }

        // A. Document-based pipeline detection — key-value extraction
        for (InvocationRef inv : method.invocations) {
            List<String> args = inv.stringArgs();
            // new Document("into", targetCollectionName) where targetCollectionName is a variable
            // captures ONLY "into" as a string arg (the variable goes to dynamicArgSources instead,
            // never into stringArgs) — so stringArgs has size 1, not the >=2 a literal key+value
            // pair would produce. Handle it directly here rather than falling through to the
            // size<2 case, which has no key/value pairs to scan at all.
            if (args != null && args.size() == 1
                    && (COLLECTION_VALUE_KEYS.contains(args.get(0)) || PIPELINE_STAGE_KEYWORDS.contains(args.get(0)))) {
                String key = args.get(0);
                if (PIPELINE_STAGE_KEYWORDS.contains(key)) {
                    allStagesFound.add(key);
                    hasPipelineInvolvement = true;
                }
                if (detail != null) {
                    MethodCallInfo.DynamicArgSource dyn = firstDynamicArg(inv.dynamicArgSources());
                    if (dyn != null) {
                        boolean isWrite = "$out".equals(key) || "$merge".equals(key);
                        detail.dynamicRefs.add(new DetectedDynamicCollection(dyn.name(), dyn.kind(), dyn.ownerClass(),
                                "PIPELINE_RUNTIME_DYNAMIC", "PIPELINE_RUNTIME", isWrite ? "WRITE" : "AGGREGATE"));
                    }
                }
                continue;
            }
            if (args == null || args.size() < 2) continue;
            for (int i = 0; i < args.size() - 1; i++) {
                String key = args.get(i);
                String val = args.get(i + 1);

                // Record the stage token itself as soon as it's recognized, independent of
                // whether its VALUE resolves to a likely collection name. This is what lets
                // CollectionLifecycleDetector.matchesPipelineStageToken catch the "stage present
                // but target unresolvable" case (e.g. $merge/into where the target collection
                // arg is a dynamic variable, not a literal).
                if (PIPELINE_STAGE_KEYWORDS.contains(key)) {
                    allStagesFound.add(key);
                    hasPipelineInvolvement = true;
                }

                if (!isCollectionName(val)) continue;

                if (COLLECTION_VALUE_KEYS.contains(key)) {
                    boolean isWrite = args.stream().anyMatch(a -> "$out".equals(a) || "$merge".equals(a));
                    results.add(new DetectedPipelineCollection(val, "PIPELINE_RUNTIME", isWrite ? "WRITE" : "AGGREGATE"));
                    hasPipelineInvolvement = true;
                    // "from" names a $lookup/$graphLookup JOIN target, never this pipeline's own
                    // base collection — tag it the same way Section D (native Aggregates.lookup())
                    // and extractLookupDetail() already do, so the base-collection exclusion below
                    // and the downstream report's lookup-target filter both recognize it correctly.
                    if ("from".equals(key)) {
                        Map<String, String> lk = new LinkedHashMap<>();
                        lk.put("from", val);
                        lk.put("source", "PIPELINE_RUNTIME");
                        allLookups.add(lk);
                    }
                }
                if (PIPELINE_STAGE_KEYWORDS.contains(key)) {
                    String opType = "$out".equals(key) || "$merge".equals(key) ? "WRITE" : "AGGREGATE";
                    results.add(new DetectedPipelineCollection(val, "PIPELINE_RUNTIME", opType));
                    hasPipelineInvolvement = true;
                }
            }

            // Dynamic fallback: when a COLLECTION_VALUE_KEYS/PIPELINE_STAGE_KEYWORDS key (from/
            // into/coll) was found above but its adjacent VALUE never resolved to a literal
            // collection name (e.g. new Document("into", targetCollectionName) where the value
            // is a variable), record a dynamic ref instead of silently dropping it.
            if (detail != null) {
                boolean sawUnresolvedKey = false;
                for (int i = 0; i < args.size() - 1; i++) {
                    String key = args.get(i);
                    String val = args.get(i + 1);
                    if ((COLLECTION_VALUE_KEYS.contains(key) || PIPELINE_STAGE_KEYWORDS.contains(key))
                            && !isCollectionName(val)) {
                        sawUnresolvedKey = true;
                        break;
                    }
                }
                if (sawUnresolvedKey) {
                    MethodCallInfo.DynamicArgSource dyn = firstDynamicArg(inv.dynamicArgSources());
                    if (dyn != null) {
                        boolean isWrite = args.stream().anyMatch(a -> "$out".equals(a) || "$merge".equals(a));
                        detail.dynamicRefs.add(new DetectedDynamicCollection(dyn.name(), dyn.kind(), dyn.ownerClass(),
                                "PIPELINE_RUNTIME_DYNAMIC", "PIPELINE_RUNTIME", isWrite ? "WRITE" : "AGGREGATE"));
                    }
                }
            }
        }

        // B. Spring Data Aggregation API classes (with collection arg)
        for (InvocationRef inv : method.invocations) {
            String owner = inv.ownerClass();
            String methodName = inv.methodName();
            String simpleOwner = owner.contains(".") ? owner.substring(owner.lastIndexOf('.') + 1) : owner;

            // Spring Data static factory calls: Aggregation.out("COLL") / Aggregation.merge("COLL")
            // (and the TypedAggregation<T> variant). Owner here is "Aggregation"/"TypedAggregation"
            // itself, not "OutOperation"/"MergeOperation" — the static factory method just RETURNS
            // an OutOperation/MergeOperation instance, but the actual bytecode invocation we see is
            // the static call on Aggregation. Treat identically to the OutOperation/MergeOperation
            // "WRITE" handling above, and also add the $out/$merge token to allStagesFound so
            // CollectionLifecycleDetector's pipelineStageTokens matching picks this up too.
            if (("Aggregation".equals(simpleOwner) || "TypedAggregation".equals(simpleOwner))
                    && ("out".equals(methodName) || "merge".equals(methodName))) {
                String coll = firstCollectionArg(inv.stringArgs());
                if (coll != null) {
                    results.add(new DetectedPipelineCollection(coll, "AGGREGATION_API", "WRITE"));
                } else if (detail != null) {
                    MethodCallInfo.DynamicArgSource dyn = firstDynamicArg(inv.dynamicArgSources());
                    if (dyn != null) {
                        detail.dynamicRefs.add(new DetectedDynamicCollection(dyn.name(), dyn.kind(), dyn.ownerClass(),
                                "AGGREGATION_API_DYNAMIC", "AGGREGATION_API", "WRITE"));
                    }
                }
                allStagesFound.add("out".equals(methodName) ? "$out" : "$merge");
                hasPipelineInvolvement = true;
                continue;
            }

            // "collection" is deliberately excluded here even for known classes like
            // BatchAggregationPaginationBuilder — the class-name-agnostic scan (section I) is the
            // single source of truth for that method name, so baseCollection resolution always
            // sees a consistent "BUILDER_METHOD_COLLECTION" source instead of racing against
            // whichever section happened to run first for the same collection literal.
            String opType = "collection".equals(methodName) ? null : AGGREGATION_CLASSES.get(simpleOwner);
            if (opType != null) {
                String coll = firstCollectionArg(inv.stringArgs());
                if (coll != null) {
                    results.add(new DetectedPipelineCollection(coll, "AGGREGATION_API", opType));
                    // LookupOperation/GraphLookupOperation's own collection arg is a JOIN target,
                    // never a base collection — same reasoning as Section A's "from" tagging above.
                    if ("LookupOperation".equals(simpleOwner) || "GraphLookupOperation".equals(simpleOwner)) {
                        Map<String, String> lk = new LinkedHashMap<>();
                        lk.put("from", coll);
                        lk.put("source", "AGGREGATION_API");
                        allLookups.add(lk);
                    }
                } else if (detail != null) {
                    MethodCallInfo.DynamicArgSource dyn = firstDynamicArg(inv.dynamicArgSources());
                    if (dyn != null) {
                        detail.dynamicRefs.add(new DetectedDynamicCollection(dyn.name(), dyn.kind(), dyn.ownerClass(),
                                "AGGREGATION_API_DYNAMIC", "AGGREGATION_API", opType));
                    }
                }
                hasPipelineInvolvement = true;
                continue;
            }

            // Pipeline operation classes (no collection arg, but mark as pipeline-involved)
            if (PIPELINE_OPERATION_CLASSES.contains(simpleOwner)) {
                hasPipelineInvolvement = true;
                // Aggregation.newAggregation(Entity.class, ops...) / TypedAggregation<Entity> — the
                // target collection comes from the entity's @Document mapping via a __class:
                // literal, not a plain collection-name string, so it's invisible to the generic
                // field-name extraction below unless resolved explicitly here.
                if (("Aggregation".equals(simpleOwner) || "TypedAggregation".equals(simpleOwner))
                        && ctx != null && ctx.entityCollectionMap != null
                        && !ctx.entityCollectionMap.isEmpty() && inv.stringArgs() != null) {
                    String coll = resolveClassLiteralCollection(inv.stringArgs(), ctx.entityCollectionMap);
                    if (coll != null) {
                        results.add(new DetectedPipelineCollection(coll, "AGGREGATION_API", "AGGREGATE"));
                    }
                }
                // Extract field names from builder methods
                if (inv.stringArgs() != null) {
                    for (String arg : inv.stringArgs()) {
                        if (arg.length() > 1 && arg.length() < 60 && !arg.contains(" ")
                                && !arg.startsWith("$") && !arg.startsWith("{")) {
                            if ("MatchOperation".equals(simpleOwner) || "Criteria".equals(simpleOwner))
                                allMatchFields.add(arg);
                            else if ("GroupOperation".equals(simpleOwner))
                                allGroupFields.add(arg);
                        }
                    }
                }
            }

            // Builder methods: from, into, lookup, graphLookup — class-name-gated, see comment on
            // AGGREGATION_BUILDER_METHODS above.
            if (AGGREGATION_BUILDER_METHODS.contains(methodName)) {
                if (simpleOwner.contains("Lookup") || simpleOwner.contains("GraphLookup")
                        || simpleOwner.contains("Aggregation") || simpleOwner.contains("Merge")
                        || simpleOwner.contains("Out") || simpleOwner.contains("UnionWith")
                        || simpleOwner.contains("Batch")) {
                    String coll = firstCollectionArg(inv.stringArgs());
                    if (coll != null) {
                        results.add(new DetectedPipelineCollection(coll, "AGGREGATION_API", "AGGREGATE"));
                        // Only "from"/"lookup"/"graphLookup"/"newLookup" on a Lookup/GraphLookup-ish
                        // builder name a JOIN target — "into" (MergeOperation.into(...) etc.) is a
                        // genuine WRITE base/target and must NOT be tagged as a lookup here.
                        if (!"into".equals(methodName)
                                && (simpleOwner.contains("Lookup") || simpleOwner.contains("GraphLookup"))) {
                            Map<String, String> lk = new LinkedHashMap<>();
                            lk.put("from", coll);
                            lk.put("source", "AGGREGATION_API");
                            allLookups.add(lk);
                        }
                    } else if (detail != null) {
                        MethodCallInfo.DynamicArgSource dyn = firstDynamicArg(inv.dynamicArgSources());
                        if (dyn != null) {
                            detail.dynamicRefs.add(new DetectedDynamicCollection(dyn.name(), dyn.kind(), dyn.ownerClass(),
                                    "AGGREGATION_API_DYNAMIC", "AGGREGATION_API", "AGGREGATE"));
                        }
                    }
                    hasPipelineInvolvement = true;
                }
            }

            // Criteria.where() — capture match field names
            if ("where".equals(methodName) && simpleOwner.endsWith("Criteria")) {
                if (inv.stringArgs() != null) {
                    for (String arg : inv.stringArgs()) {
                        if (arg.length() > 1 && arg.length() < 60 && !arg.contains(" ")
                                && !arg.startsWith("$") && !arg.startsWith("{")) {
                            allMatchFields.add(arg);
                        }
                    }
                }
            }

            // C. mongoTemplate.aggregate()
            if ("aggregate".equals(methodName) && ownerMatchesTemplate(owner)) {
                String coll = firstCollectionArg(inv.stringArgs());
                // The most common overload is aggregate(TypedAggregation/Aggregation, Entity.class),
                // where the target collection is implied by the entity's @Document mapping rather
                // than an explicit collection-name string. Without resolving __class: literals here
                // (the same way MongoMethodDetector already does for updateMulti/remove/etc.), this
                // extremely common call form never resolves a collection at all.
                if (coll == null && ctx != null && ctx.entityCollectionMap != null
                        && !ctx.entityCollectionMap.isEmpty() && inv.stringArgs() != null) {
                    coll = resolveClassLiteralCollection(inv.stringArgs(), ctx.entityCollectionMap);
                }
                if (coll != null) {
                    results.add(new DetectedPipelineCollection(coll, "TEMPLATE_AGGREGATE", "AGGREGATE"));
                } else if (detail != null) {
                    MethodCallInfo.DynamicArgSource dyn = firstDynamicArg(inv.dynamicArgSources());
                    if (dyn != null) {
                        detail.dynamicRefs.add(new DetectedDynamicCollection(dyn.name(), dyn.kind(), dyn.ownerClass(),
                                "TEMPLATE_AGGREGATE_DYNAMIC", "TEMPLATE_AGGREGATE", "AGGREGATE"));
                    }
                }
                hasPipelineInvolvement = true;
            }
        }

        // D. Native driver Aggregates factory class
        for (InvocationRef inv : method.invocations) {
            String owner = inv.ownerClass();
            String simpleOwner = owner != null && owner.contains(".")
                    ? owner.substring(owner.lastIndexOf('.') + 1) : (owner != null ? owner : "");

            if ("Aggregates".equals(simpleOwner) || owner.endsWith(".model.Aggregates")) {
                String facMethod = inv.methodName();
                String facOp = AGGREGATES_FACTORY_METHODS.get(facMethod);
                if (facOp != null) {
                    hasPipelineInvolvement = true;
                    allStagesFound.add("$" + facMethod);
                    // lookup/out/merge/unionWith/graphLookup take collection name as first arg
                    if ("lookup".equals(facMethod) || "out".equals(facMethod) || "merge".equals(facMethod)
                            || "unionWith".equals(facMethod) || "graphLookup".equals(facMethod)) {
                        String coll = firstCollectionArg(inv.stringArgs());
                        if (coll != null) {
                            results.add(new DetectedPipelineCollection(coll, "NATIVE_AGGREGATES", facOp));
                        } else if (detail != null) {
                            MethodCallInfo.DynamicArgSource dyn = firstDynamicArg(inv.dynamicArgSources());
                            if (dyn != null) {
                                detail.dynamicRefs.add(new DetectedDynamicCollection(dyn.name(), dyn.kind(), dyn.ownerClass(),
                                        "NATIVE_AGGREGATES_DYNAMIC", "NATIVE_AGGREGATES", facOp));
                            }
                        }
                        // For lookup, capture all 4 args: from, localField, foreignField, as
                        if ("lookup".equals(facMethod) && inv.stringArgs() != null && inv.stringArgs().size() >= 4) {
                            Map<String, String> lk = new LinkedHashMap<>();
                            List<String> sa = inv.stringArgs();
                            lk.put("from", sa.get(0));
                            lk.put("localField", sa.size() > 1 ? sa.get(1) : "");
                            lk.put("foreignField", sa.size() > 2 ? sa.get(2) : "");
                            lk.put("as", sa.size() > 3 ? sa.get(3) : "");
                            lk.put("source", "NATIVE_AGGREGATES");
                            allLookups.add(lk);
                        }
                    }
                    // match() — capture filter fields
                    if ("match".equals(facMethod) && inv.stringArgs() != null) {
                        for (String arg : inv.stringArgs()) {
                            if (arg.length() > 1 && arg.length() < 60 && !arg.contains(" ")
                                    && !arg.startsWith("$") && !arg.startsWith("{")) {
                                allMatchFields.add(arg);
                            }
                        }
                    }
                    // group() — capture group fields
                    if ("group".equals(facMethod) && inv.stringArgs() != null) {
                        for (String arg : inv.stringArgs()) {
                            if (arg.startsWith("$")) allGroupFields.add(arg.substring(1));
                            else if (arg.length() > 1 && arg.length() < 60 && !arg.contains(" "))
                                allGroupFields.add(arg);
                        }
                    }
                }
            }
        }

        // E. BsonDocument construction with pipeline stage keys
        for (InvocationRef inv : method.invocations) {
            String owner = inv.ownerClass();
            String simpleOwner = owner != null && owner.contains(".")
                    ? owner.substring(owner.lastIndexOf('.') + 1) : (owner != null ? owner : "");

            if ("BsonDocument".equals(simpleOwner) || "BasicDBObject".equals(simpleOwner)) {
                if (inv.stringArgs() != null && inv.stringArgs().size() >= 2) {
                    List<String> args = inv.stringArgs();
                    for (int i = 0; i < args.size() - 1; i++) {
                        String key = args.get(i);
                        String val = args.get(i + 1);
                        if (ALL_PIPELINE_STAGES.contains(key)) {
                            allStagesFound.add(key);
                            hasPipelineInvolvement = true;
                        }
                        if (COLLECTION_VALUE_KEYS.contains(key) && isCollectionName(val)) {
                            results.add(new DetectedPipelineCollection(val, "BSON_DOCUMENT", "AGGREGATE"));
                            hasPipelineInvolvement = true;
                        }
                        if (PIPELINE_STAGE_KEYWORDS.contains(key) && isCollectionName(val)) {
                            String op = "$out".equals(key) || "$merge".equals(key) ? "WRITE" : "AGGREGATE";
                            results.add(new DetectedPipelineCollection(val, "BSON_DOCUMENT", op));
                            hasPipelineInvolvement = true;
                        }
                    }
                }
            }
        }

        // F. Document.parse() / BasicDBObject.parse() with JSON string
        for (InvocationRef inv : method.invocations) {
            if (!"parse".equals(inv.methodName())) continue;
            String owner = inv.ownerClass();
            String simpleOwner = owner != null && owner.contains(".")
                    ? owner.substring(owner.lastIndexOf('.') + 1) : (owner != null ? owner : "");
            if (!"Document".equals(simpleOwner) && !"BasicDBObject".equals(simpleOwner)
                    && !"BsonDocument".equals(simpleOwner)) continue;

            if (inv.stringArgs() != null) {
                for (String jsonStr : inv.stringArgs()) {
                    if (jsonStr.length() < 5) continue;
                    // Check if JSON contains pipeline stages
                    for (String stage : ALL_PIPELINE_STAGES) {
                        if (jsonStr.contains(stage)) {
                            allStagesFound.add(stage);
                            hasPipelineInvolvement = true;
                        }
                    }
                    // Extract collection refs from pipeline JSON
                    for (String ref : configLoader.extractPipelineCollectionRefs(jsonStr)) {
                        if (isCollectionName(ref)) {
                            results.add(new DetectedPipelineCollection(ref, "DOCUMENT_PARSE", "AGGREGATE"));
                        }
                    }
                    // Extract lookup detail from the JSON string
                    extractLookupDetail(jsonStr, allLookups);
                    extractMatchFields(jsonStr, allMatchFields);
                    extractGroupFields(jsonStr, allGroupFields);
                }
            }
        }

        // G. Companion classes: Filters, Projections, Sorts, Accumulators
        for (InvocationRef inv : method.invocations) {
            String owner = inv.ownerClass();
            String simpleOwner = owner != null && owner.contains(".")
                    ? owner.substring(owner.lastIndexOf('.') + 1) : (owner != null ? owner : "");

            String companionType = COMPANION_CLASSES.get(simpleOwner);
            if (companionType != null && inv.stringArgs() != null) {
                for (String arg : inv.stringArgs()) {
                    if (arg.length() > 1 && arg.length() < 60 && !arg.contains(" ")
                            && !arg.startsWith("{") && !arg.startsWith("[")) {
                        if (detail != null) {
                            detail.companionUsage
                                    .computeIfAbsent(companionType, k -> new ArrayList<>())
                                    .add(simpleOwner + "." + inv.methodName() + "(" + arg + ")");
                        }
                        if ("MATCH".equals(companionType)) allMatchFields.add(arg);
                        if ("SORT".equals(companionType)) {
                            if (detail != null) detail.sortFields.add(arg);
                        }
                    }
                }
            }
        }

        // H. Cross-method pipeline tracing — detect methods that return pipeline types
        for (InvocationRef inv : method.invocations) {
            String rt = inv.returnType();
            // Real bug, found via a live audit: `rt.contains("Aggregation")`/`rt.contains("Pipeline")`
            // used to be a bare substring test against ANY invoked method's fully-qualified return
            // type, with no check that the type actually belongs to a Mongo/Spring-Data aggregation
            // API. That matched e.g. an ANTLR-generated `Pipelined_using_clauseContext` (Oracle's
            // "PIPELINED" function grammar rule — nothing to do with MongoDB) purely because the FQN
            // contains the substring "Pipeline", and similarly any project-local class that happens
            // to be named `*Pipeline*`/`*Aggregation*` for unrelated reasons. Requiring the return
            // type to actually be IN a real Mongo/Spring-Data aggregation package before treating it
            // as pipeline involvement keeps the "List" + builder-method-name heuristic below (which
            // is deliberately name-based, for project-specific wrappers) but stops random unrelated
            // classes from silently faking a MongoDB aggregation flow.
            boolean realAggregationType = rt != null && (
                    rt.startsWith("org.springframework.data.mongodb.core.aggregation.")
                    || rt.startsWith("com.mongodb.client.model.Aggregates")
                    || rt.startsWith("com.mongodb.client.AggregateIterable")
                    || rt.startsWith("com.mongodb.client.model.Field"));
            if (realAggregationType || (rt != null && rt.equals("List") && isPipelineBuilderMethod(inv))) {
                if (detail != null) {
                    String source = (inv.ownerClass() != null ? inv.ownerClass() : "") + "." + inv.methodName() + "()";
                    detail.pipelineSourceMethods.add(source);
                    detail.isDynamic = true;
                }
                hasPipelineInvolvement = true;
            }
        }

        // I. Generic ".collection(literal)" builder-method call — deliberately NOT gated by
        // owner class name at all, unlike section B's builder-methods check above. Project-
        // specific fluent wrappers that combine aggregation with batching/pagination (e.g. a
        // custom "...PaginationBuilder"-style helper) are named differently per module and get
        // renamed/replaced over time; matching on today's class name would silently stop working
        // the moment a differently-named equivalent shows up elsewhere. A method literally named
        // "collection" taking a single resolvable Mongo-collection-name-pattern string is, on its
        // own, an unambiguous "this is the base collection" declaration — no class-name gate
        // needed, and it earns top-tier confidence in the baseCollection resolution below.
        for (InvocationRef inv : method.invocations) {
            if (!"collection".equals(inv.methodName())) continue;
            String coll = firstCollectionArg(inv.stringArgs());
            if (coll != null) {
                results.add(new DetectedPipelineCollection(coll, "BUILDER_METHOD_COLLECTION", "AGGREGATE"));
                hasPipelineInvolvement = true;
            } else if (detail != null) {
                MethodCallInfo.DynamicArgSource dyn = firstDynamicArg(inv.dynamicArgSources());
                if (dyn != null) {
                    detail.dynamicRefs.add(new DetectedDynamicCollection(dyn.name(), dyn.kind(), dyn.ownerClass(),
                            "BUILDER_METHOD_COLLECTION_DYNAMIC", "BUILDER_METHOD_COLLECTION", "AGGREGATE"));
                    hasPipelineInvolvement = true;
                }
            }
        }

        // J. Spring Data MongoDB @Aggregation(pipeline = {...}) annotated repository methods —
        // the pipeline is declared on the method annotation itself, not built via method calls,
        // so none of A-H (which all scan method.invocations/stringLiterals) can see it. Stage
        // JSON strings still carry $lookup/$out/$merge targets like any other pipeline; the base
        // collection is the repository interface's own mapped entity collection, resolved via
        // ctx.repoCollectionMap the same way CollectionResolver resolves plain (non-aggregation)
        // repository method calls.
        for (AnnotationData ann : method.annotationDetailList) {
            if (!"Aggregation".equals(ann.name())) continue;
            List<String> stageJsons = toStringList(ann.attributes().get("pipeline"));
            if (stageJsons.isEmpty()) continue;
            hasPipelineInvolvement = true;
            for (String stageJson : stageJsons) {
                for (String stage : ALL_PIPELINE_STAGES) {
                    if (stageJson.contains(stage)) allStagesFound.add(stage);
                }
                extractLookupDetail(stageJson, allLookups);
                extractMatchFields(stageJson, allMatchFields);
                extractGroupFields(stageJson, allGroupFields);
                // extractPipelineCollectionRefs only ever returns $lookup/$out/$merge/$unionWith
                // TARGET collections — never the base — so these must NOT share the
                // "REPOSITORY_ANNOTATION" source tag used below for the actual base collection,
                // or the baseCollection race in the dedup pass below could pick a lookup target.
                for (String ref : configLoader.extractPipelineCollectionRefs(stageJson)) {
                    if (isCollectionName(ref)) {
                        results.add(new DetectedPipelineCollection(ref, "REPOSITORY_ANNOTATION_PIPELINE", "AGGREGATE"));
                    }
                }
            }
            if (cls != null && ctx != null) {
                String repoColl = ctx.repoCollectionMap.get(cls.fqn);
                if (repoColl == null) repoColl = ctx.repoCollectionMap.get(cls.simpleName);
                if (repoColl != null) {
                    results.add(new DetectedPipelineCollection(repoColl, "REPOSITORY_ANNOTATION", "AGGREGATE"));
                }
            }
        }

        // Deduplicate
        Map<String, DetectedPipelineCollection> deduped = new LinkedHashMap<>();
        for (DetectedPipelineCollection dc : results) {
            deduped.putIfAbsent(dc.collectionName(), dc);
        }

        // Determine the base collection — prefer an explicit, unambiguous source (the literal
        // collection arg to mongoTemplate.aggregate(pipeline, "coll"), a repository's own
        // @Aggregation method, or a fluent ".collection(literal)" builder call) over an
        // entity-mapped Aggregation.newAggregation(Entity.class, ...) collection, since the latter
        // shares its detection source ("AGGREGATION_API"/"AGGREGATE") with LookupOperation's
        // collection arg — disambiguate by excluding anything that also shows up as a $lookup
        // "from" target.
        String baseCollection = null;
        for (DetectedPipelineCollection dc : deduped.values()) {
            if ("TEMPLATE_AGGREGATE".equals(dc.source()) || "REPOSITORY_ANNOTATION".equals(dc.source())
                    || "BUILDER_METHOD_COLLECTION".equals(dc.source())) {
                baseCollection = dc.collectionName();
                break;
            }
        }
        if (baseCollection == null) {
            for (DetectedPipelineCollection dc : deduped.values()) {
                if (!"AGGREGATE".equals(dc.operationType())) continue;
                boolean isLookupTarget = allLookups.stream()
                        .anyMatch(lk -> dc.collectionName().equals(lk.get("from")));
                if (!isLookupTarget) {
                    baseCollection = dc.collectionName();
                    break;
                }
            }
        }

        // Build rich detail if requested and pipeline involvement detected
        if (detail != null && hasPipelineInvolvement) {
            detail.hasPipelineInvolvement = true;
            detail.baseCollection = baseCollection;
            for (String stage : allStagesFound) {
                Map<String, Object> stageEntry = new LinkedHashMap<>();
                stageEntry.put("stage", stage);
                detail.stages.add(stageEntry);
            }
            detail.lookupTargets.addAll(allLookups);
            detail.matchFields.addAll(allMatchFields);
            detail.groupFields.addAll(allGroupFields);
            for (DetectedPipelineCollection dc : deduped.values()) {
                detail.detectionSources.add(dc.source());
            }
        }

        return new ArrayList<>(deduped.values());
    }

    /** Check if an invocation looks like a pipeline builder method based on context. */
    private boolean isPipelineBuilderMethod(InvocationRef inv) {
        String mn = inv.methodName();
        if (mn == null) return false;
        String mnLower = mn.toLowerCase();
        return mnLower.contains("pipeline") || mnLower.contains("aggregat")
                || mnLower.contains("stages") || mnLower.contains("lookup");
    }

    /** Extract $lookup detail (from, localField, foreignField, as) from a JSON-like string. */
    private void extractLookupDetail(String str, List<Map<String, String>> lookups) {
        Matcher fromMatcher = LOOKUP_FROM_PATTERN.matcher(str);
        while (fromMatcher.find()) {
            Map<String, String> lk = new LinkedHashMap<>();
            lk.put("type", "$" + fromMatcher.group(1));
            lk.put("from", fromMatcher.group(2));
            lk.put("source", "STRING_LITERAL");

            // Try to find localField/foreignField/as near this lookup
            Matcher lf = LOOKUP_LOCAL_PATTERN.matcher(str);
            if (lf.find()) lk.put("localField", lf.group(1));
            Matcher ff = LOOKUP_FOREIGN_PATTERN.matcher(str);
            if (ff.find()) lk.put("foreignField", ff.group(1));
            Matcher asM = LOOKUP_AS_PATTERN.matcher(str);
            if (asM.find()) lk.put("as", asM.group(1));

            lookups.add(lk);
        }
    }

    /** Extract field names from $match patterns in a string. */
    private void extractMatchFields(String str, Set<String> fields) {
        Matcher m = MATCH_FIELD_PATTERN.matcher(str);
        while (m.find()) fields.add(m.group(1));
    }

    /** Extract group-by fields from $group patterns in a string. */
    private void extractGroupFields(String str, Set<String> fields) {
        Matcher m = GROUP_ID_PATTERN.matcher(str);
        while (m.find()) fields.add(m.group(1));
    }

    /** Flattens a String or String[]/List-valued annotation attribute (e.g. @Aggregation's
     *  "pipeline") into its non-blank string entries, in declared order. */
    private List<String> toStringList(Object attr) {
        List<String> out = new ArrayList<>();
        if (attr instanceof String s) {
            if (!s.isBlank()) out.add(s);
        } else if (attr instanceof List<?> list) {
            for (Object item : list) {
                if (item instanceof String s && !s.isBlank()) out.add(s);
            }
        }
        return out;
    }

    private String firstCollectionArg(List<String> stringArgs) {
        if (stringArgs == null) return null;
        for (String arg : stringArgs) {
            if (isCollectionName(arg)) return arg;
        }
        return null;
    }

    /** First captured dynamic (variable/field-sourced) argument, if any — see
     *  MongoMethodDetector.firstDynamicArg for the same "first match, no content filter" reasoning. */
    private MethodCallInfo.DynamicArgSource firstDynamicArg(List<MethodCallInfo.DynamicArgSource> args) {
        if (args == null || args.isEmpty()) return null;
        return args.get(0);
    }

    /** Resolve a __class:FQN literal argument to its @Document collection name, trying FQN then
     *  simple name — mirrors MongoMethodDetector.resolveEntityCollection's __class: handling. */
    private String resolveClassLiteralCollection(List<String> stringArgs, Map<String, String> entityCollectionMap) {
        for (String arg : stringArgs) {
            if (!arg.startsWith("__class:")) continue;
            String beanFqn = arg.substring(8);
            String coll = entityCollectionMap.get(beanFqn);
            if (coll == null) {
                int dot = beanFqn.lastIndexOf('.');
                String simple = dot > 0 ? beanFqn.substring(dot + 1) : beanFqn;
                coll = entityCollectionMap.get(simple);
            }
            if (coll != null) return coll;
        }
        return null;
    }

    private boolean isCollectionName(String s) {
        return configLoader.isLikelyCollectionName(s) && !LOOKUP_NON_COLLECTION_VALUES.contains(s);
    }

    private boolean ownerMatchesTemplate(String owner) {
        return owner != null && (owner.endsWith("MongoTemplate") || owner.endsWith("MongoOperations")
                || owner.endsWith("ReactiveMongoTemplate") || owner.endsWith("ReactiveMongoOperations"));
    }
}
