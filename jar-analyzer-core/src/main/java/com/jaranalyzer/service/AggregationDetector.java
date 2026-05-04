package com.jaranalyzer.service;

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

    // Lookup detail field keys
    private static final Set<String> LOOKUP_DETAIL_KEYS = Set.of(
            "from", "localField", "foreignField", "as", "let", "pipeline"
    );

    // Spring Data Aggregation classes whose constructors/methods take collection name args
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

    // Methods on aggregation builders that take collection name as arg
    private static final Set<String> AGGREGATION_BUILDER_METHODS = Set.of(
            "from", "into", "newLookup", "lookup", "graphLookup", "collection"
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
            if (inv.stringArgs() == null || inv.stringArgs().size() < 2) continue;
            List<String> args = inv.stringArgs();
            for (int i = 0; i < args.size() - 1; i++) {
                String key = args.get(i);
                String val = args.get(i + 1);
                if (!isCollectionName(val)) continue;

                if (COLLECTION_VALUE_KEYS.contains(key)) {
                    boolean isWrite = args.stream().anyMatch(a -> "$out".equals(a) || "$merge".equals(a));
                    results.add(new DetectedPipelineCollection(val, "PIPELINE_RUNTIME", isWrite ? "WRITE" : "AGGREGATE"));
                    hasPipelineInvolvement = true;
                }
                if (PIPELINE_STAGE_KEYWORDS.contains(key)) {
                    String opType = "$out".equals(key) || "$merge".equals(key) ? "WRITE" : "AGGREGATE";
                    results.add(new DetectedPipelineCollection(val, "PIPELINE_RUNTIME", opType));
                    hasPipelineInvolvement = true;
                }
            }
        }

        // B. Spring Data Aggregation API classes (with collection arg)
        for (InvocationRef inv : method.invocations) {
            String owner = inv.ownerClass();
            String methodName = inv.methodName();
            String simpleOwner = owner.contains(".") ? owner.substring(owner.lastIndexOf('.') + 1) : owner;

            String opType = AGGREGATION_CLASSES.get(simpleOwner);
            if (opType != null) {
                String coll = firstCollectionArg(inv.stringArgs());
                if (coll != null) {
                    results.add(new DetectedPipelineCollection(coll, "AGGREGATION_API", opType));
                }
                hasPipelineInvolvement = true;
                continue;
            }

            // Pipeline operation classes (no collection arg, but mark as pipeline-involved)
            if (PIPELINE_OPERATION_CLASSES.contains(simpleOwner)) {
                hasPipelineInvolvement = true;
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

            // Builder methods: from, into, lookup, graphLookup
            if (AGGREGATION_BUILDER_METHODS.contains(methodName)) {
                if (simpleOwner.contains("Lookup") || simpleOwner.contains("GraphLookup")
                        || simpleOwner.contains("Aggregation") || simpleOwner.contains("Merge")
                        || simpleOwner.contains("Out") || simpleOwner.contains("UnionWith")
                        || simpleOwner.contains("Batch")) {
                    String coll = firstCollectionArg(inv.stringArgs());
                    if (coll != null) {
                        results.add(new DetectedPipelineCollection(coll, "AGGREGATION_API", "AGGREGATE"));
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
                if (coll != null) {
                    results.add(new DetectedPipelineCollection(coll, "TEMPLATE_AGGREGATE", "AGGREGATE"));
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
            if (rt != null && (rt.contains("Aggregation") || rt.contains("Pipeline")
                    || (rt.equals("List") && isPipelineBuilderMethod(inv)))) {
                if (detail != null) {
                    String source = (inv.ownerClass() != null ? inv.ownerClass() : "") + "." + inv.methodName() + "()";
                    detail.pipelineSourceMethods.add(source);
                    detail.isDynamic = true;
                }
                hasPipelineInvolvement = true;
            }
        }

        // Deduplicate
        Map<String, DetectedPipelineCollection> deduped = new LinkedHashMap<>();
        for (DetectedPipelineCollection dc : results) {
            deduped.putIfAbsent(dc.collectionName(), dc);
        }

        // Build rich detail if requested and pipeline involvement detected
        if (detail != null && hasPipelineInvolvement) {
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

    private String firstCollectionArg(List<String> stringArgs) {
        if (stringArgs == null) return null;
        for (String arg : stringArgs) {
            if (isCollectionName(arg)) return arg;
        }
        return null;
    }

    private boolean isCollectionName(String s) {
        return configLoader.isLikelyCollectionName(s);
    }

    private boolean ownerMatchesTemplate(String owner) {
        return owner != null && (owner.endsWith("MongoTemplate") || owner.endsWith("MongoOperations")
                || owner.endsWith("ReactiveMongoTemplate") || owner.endsWith("ReactiveMongoOperations"));
    }
}
