package com.jaranalyzer.service;

import com.jaranalyzer.model.MethodCallInfo;
import com.jaranalyzer.service.CallGraphIndex.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Detects MongoDB collection access from method invocations in bytecode.
 * Analyzes InvocationRefs and their associated string arguments to find
 * collection names from patterns not covered by annotation/entity-based detection.
 *
 * Detection categories:
 *   A. BulkWriteCollector (custom pattern): addInsert/addUpdate/addDelete with collection name as first arg
 *   B. MongoTemplate / MongoOperations: find, insert, upsert, remove, aggregate, etc. with collection arg
 *      B1. replace() — Spring Data MongoDB 3.x full document replacement (GAP-MONGO-REPLACE)
 *   C. Native MongoDB driver: MongoDatabase.getCollection("COLL"), MongoCollection methods
 *   D. Reactive: ReactiveMongoTemplate, ReactiveMongoOperations (same methods as B)
 */
@Component
class MongoMethodDetector {

    private static final Logger log = LoggerFactory.getLogger(MongoMethodDetector.class);

    // BulkWriteCollector methods where first string arg is collection name
    private static final Set<String> BULK_COLLECTOR_METHODS = Set.of(
            "addInsert", "addUpdate", "addDelete"
    );

    // MongoTemplate/MongoOperations methods that accept a collection name string arg
    private static final Set<String> TEMPLATE_METHODS = Set.of(
            "find", "findOne", "findById", "findAll", "findDistinct",
            "findAndModify", "findAndReplace", "findAndRemove", "findAllAndRemove",
            "insert", "insertAll", "save", "saveAll", "upsert",
            "updateFirst", "updateMulti",
            // GAP-MONGO-REPLACE: Spring Data MongoDB 3.x full-document replacement
            "replace",
            "remove", "removeWithChildren", "aggregate", "mapReduce",
            "count", "countDocuments", "estimatedDocumentCount", "exists", "stream", "scroll",
            "createCollection", "dropCollection", "getCollection",
            "execute", "executeCommand"
    );

    // Owner class name patterns for MongoTemplate/MongoOperations and custom wrappers
    private static final List<String> TEMPLATE_OWNERS = List.of(
            "MongoTemplate", "MongoOperations",
            "ReactiveMongoTemplate", "ReactiveMongoOperations",
            "OpusCommandTemplate", "CommandTemplate"
    );

    // GridFS methods that reference collections (fs.files, fs.chunks)
    private static final Set<String> GRIDFS_METHODS = Set.of(
            "store", "find", "delete", "getResource", "getResources"
    );
    private static final List<String> GRIDFS_OWNERS = List.of(
            "GridFsTemplate", "GridFsOperations", "ReactiveGridFsTemplate", "ReactiveGridFsOperations"
    );

    // Native driver methods on MongoDatabase
    private static final Set<String> DATABASE_METHODS = Set.of(
            "getCollection", "runCommand", "createCollection", "drop"
    );

    // Native driver owner patterns
    private static final List<String> DATABASE_OWNERS = List.of(
            "MongoDatabase", "InterceptableMongoDatabase"
    );

    // Native driver MongoCollection methods (collection is the receiver, not an arg)
    private static final Set<String> COLLECTION_METHODS = Set.of(
            "find", "insertOne", "insertMany", "deleteOne", "deleteMany",
            "replaceOne", "updateOne", "updateMany", "bulkWrite",
            "aggregate", "countDocuments", "estimatedDocumentCount", "distinct",
            "findOneAndUpdate", "findOneAndReplace", "findOneAndDelete",
            "createIndex", "dropIndex", "watch", "renameCollection"
    );

    /** Result of detecting a collection from a method invocation. */
    record DetectedCollection(String collectionName, String source, String operationType) {}

    /** Both resolved-literal and dynamic (variable-sourced) detections from a single pass. */
    record DetectionResult(List<DetectedCollection> resolved, List<DetectedDynamicCollection> dynamic) {}

    /**
     * Static fallback operation-type map for well-known MongoDB method names.
     * Used when DomainConfigLoader has no operation_types config loaded (unit tests,
     * offline mode). These are unambiguous MongoDB semantics that don't vary by config.
     */
    private static final Map<String, String> MONGO_OP_TYPE_FALLBACK = Map.ofEntries(
            Map.entry("find",                  "READ"),
            Map.entry("findOne",               "READ"),
            Map.entry("findById",              "READ"),
            Map.entry("findAll",               "READ"),
            Map.entry("findDistinct",          "READ"),
            Map.entry("findAndModify",         "UPDATE"),
            Map.entry("findAndReplace",        "UPDATE"),
            Map.entry("findAndRemove",         "DELETE"),
            Map.entry("findAllAndRemove",      "DELETE"),
            Map.entry("insert",                "WRITE"),
            Map.entry("insertAll",             "WRITE"),
            Map.entry("save",                  "WRITE"),
            Map.entry("saveAll",               "WRITE"),
            Map.entry("upsert",                "WRITE"),
            Map.entry("updateFirst",           "UPDATE"),
            Map.entry("updateMulti",           "UPDATE"),
            // GAP-MONGO-REPLACE: replace() is a full-document replacement → UPDATE semantics
            Map.entry("replace",               "UPDATE"),
            Map.entry("remove",                "DELETE"),
            Map.entry("removeWithChildren",    "DELETE"),
            Map.entry("aggregate",             "AGGREGATE"),
            Map.entry("mapReduce",             "AGGREGATE"),
            Map.entry("count",                 "READ"),
            Map.entry("countDocuments",        "READ"),
            Map.entry("estimatedDocumentCount","READ"),
            Map.entry("exists",                "READ"),
            Map.entry("stream",                "READ"),
            Map.entry("scroll",                "READ"),
            Map.entry("createCollection",      "WRITE"),
            Map.entry("dropCollection",        "DELETE"),
            Map.entry("getCollection",         "READ"),
            Map.entry("store",                 "WRITE"),
            Map.entry("delete",                "DELETE")
    );

    private final DomainConfigLoader configLoader;

    MongoMethodDetector(DomainConfigLoader configLoader) {
        this.configLoader = configLoader;
    }

    /**
     * Infer MongoDB operation type for a method name.
     * Tries DomainConfigLoader first (config-driven), falls back to static map for
     * known MongoDB methods when config is not loaded (unit tests, offline mode).
     */
    private String inferMongoOpType(String methodName) {
        String opType = configLoader.inferOperationType(methodName, "REPOSITORY");
        if (opType != null) return opType;
        return MONGO_OP_TYPE_FALLBACK.get(methodName);
    }

    /**
     * Scan all invocations in a method for MongoDB collection access patterns.
     * Returns detected collections with their source and operation type.
     */
    DetectionResult detect(IndexedMethod method, IndexedClass cls) {
        return detect(method, cls, Map.of());
    }

    /**
     * Scan with entity→collection mapping for resolving bean class references.
     * e.g. mongoOperations.updateMulti(query, update, Bean.class)
     */
    DetectionResult detect(IndexedMethod method, IndexedClass cls,
                            Map<String, String> entityCollectionMap) {
        List<DetectedCollection> results = new ArrayList<>();
        List<DetectedDynamicCollection> dynamicResults = new ArrayList<>();
        // Track unresolved template calls for post-processing context resolution.
        // Each entry also carries the dynamic arg source captured at that same call site (if
        // any), so if context-resolution below finds nothing, we can still report a dynamic
        // reference instead of dropping the call silently.
        List<Object[]> unresolvedTemplateCalls = new ArrayList<>(); // [source, opType, DynamicArgSource-or-null]

        for (InvocationRef inv : method.invocations) {
            String owner = inv.ownerClass();
            String methodName = inv.methodName();
            List<String> stringArgs = inv.stringArgs();

            // A. BulkWriteCollector pattern
            if (BULK_COLLECTOR_METHODS.contains(methodName) && ownerContains(owner, "BulkWriteCollector")) {
                String coll = firstCollectionArg(stringArgs);
                String opType = switch (methodName) {
                    case "addInsert" -> "WRITE";
                    case "addUpdate" -> "UPDATE";
                    case "addDelete" -> "DELETE";
                    default -> "WRITE";
                };
                if (coll != null) {
                    results.add(new DetectedCollection(coll, "BULK_WRITE_COLLECTOR", opType));
                } else {
                    MethodCallInfo.DynamicArgSource dyn = firstDynamicArg(inv.dynamicArgSources());
                    if (dyn != null) {
                        dynamicResults.add(new DetectedDynamicCollection(dyn.name(), dyn.kind(), dyn.ownerClass(),
                                "BULK_WRITE_COLLECTOR_DYNAMIC", "BULK_WRITE_COLLECTOR", opType));
                    }
                }
                continue;
            }

            // B. MongoTemplate / MongoOperations / Reactive variants
            if (TEMPLATE_METHODS.contains(methodName) && ownerMatchesAny(owner, TEMPLATE_OWNERS)) {
                String coll = firstCollectionArg(stringArgs);
                // getCollection's own argument is a collection name by API contract — fall back to
                // the lenient (no-underscore-required) check before giving up, same reasoning as
                // the native MongoDatabase.getCollection case in Section C below. Restricted to
                // getCollection specifically since other TEMPLATE_METHODS args are query/update
                // documents etc., not guaranteed to be a bare collection-name string.
                if (coll == null && "getCollection".equals(methodName) && stringArgs != null) {
                    for (String arg : stringArgs) {
                        if (configLoader.isLikelyCollectionNameLenient(arg)) { coll = arg; break; }
                    }
                }
                // If no collection name string, try resolving from bean class literal
                // e.g. mongoOperations.updateMulti(query, update, Bean.class) → __class:com.x.Bean
                if (coll == null && stringArgs != null && !entityCollectionMap.isEmpty()) {
                    for (String arg : stringArgs) {
                        if (arg.startsWith("__class:")) {
                            String beanFqn = arg.substring(8);
                            coll = entityCollectionMap.get(beanFqn);
                            if (coll == null) {
                                // Try simple name
                                int dot = beanFqn.lastIndexOf('.');
                                String simple = dot > 0 ? beanFqn.substring(dot + 1) : beanFqn;
                                coll = entityCollectionMap.get(simple);
                            }
                            if (coll != null) break;
                        }
                    }
                }
                String source = owner.contains("Reactive") ? "REACTIVE_MONGO_TEMPLATE" : "MONGO_TEMPLATE";
                String opType = inferMongoOpType(methodName);
                if (coll != null) {
                    results.add(new DetectedCollection(coll, source, opType != null ? opType : "READ"));
                } else {
                    // Track unresolved call for context-based resolution below — carry the
                    // dynamic arg source (if any) so it can still be reported if context
                    // resolution (section G) finds nothing either.
                    MethodCallInfo.DynamicArgSource dyn = firstDynamicArg(inv.dynamicArgSources());
                    unresolvedTemplateCalls.add(new Object[]{source, opType != null ? opType : "READ", dyn});
                }
                continue;
            }

            // C. Native MongoDatabase methods
            if (DATABASE_METHODS.contains(methodName) && ownerMatchesAny(owner, DATABASE_OWNERS)) {
                if ("getCollection".equals(methodName)) {
                    // getCollection's own argument is a collection name by API contract — fall back
                    // to the lenient (no-underscore-required) check before giving up, so a
                    // same-class constant like MOVEMENTS ("MOVEMENTS", no underscore) isn't silently
                    // dropped just because it fails the general enterprise-naming underscore rule.
                    String coll = firstCollectionArg(stringArgs);
                    if (coll == null && stringArgs != null) {
                        for (String arg : stringArgs) {
                            if (configLoader.isLikelyCollectionNameLenient(arg)) { coll = arg; break; }
                        }
                    }
                    if (coll != null) {
                        results.add(new DetectedCollection(coll, "NATIVE_DRIVER", "READ"));
                    } else {
                        MethodCallInfo.DynamicArgSource dyn = firstDynamicArg(inv.dynamicArgSources());
                        if (dyn != null) {
                            dynamicResults.add(new DetectedDynamicCollection(dyn.name(), dyn.kind(), dyn.ownerClass(),
                                    "NATIVE_DRIVER_DYNAMIC", "NATIVE_DRIVER", "READ"));
                        }
                    }
                } else if ("runCommand".equals(methodName)) {
                    // runCommand can contain bulkWrite with collection refs in the command doc
                    // Flag as NATIVE_COMMAND — the actual collection is in the command structure
                    boolean sawResolvableCollection = false;
                    if (stringArgs != null) {
                        for (String arg : stringArgs) {
                            if (isCollectionName(arg)) {
                                results.add(new DetectedCollection(arg, "NATIVE_COMMAND", "WRITE"));
                                sawResolvableCollection = true;
                            }
                        }
                    }
                    if (!sawResolvableCollection) {
                        // Administrative/stats commands — e.g. runCommand(new Document("collStats",
                        // collectionName)) or runCommand((Bson) opaquePrebuiltCommand) — carry no
                        // recognizable collection-name string in their args, so the loop above never
                        // classifies them. Default to READ here (admin/stats commands are read-only
                        // in the general case) rather than leaving it unclassified. Uses a null
                        // collectionName purely to carry the operationType signal through this same
                        // DetectedCollection channel — CollectionResolver skips adding a null
                        // collectionName to collectionsAccessed.
                        results.add(new DetectedCollection(null, "NATIVE_COMMAND_DEFAULT", "READ"));
                    }
                } else if ("createCollection".equals(methodName)) {
                    String coll = firstCollectionArg(stringArgs);
                    if (coll != null) {
                        results.add(new DetectedCollection(coll, "NATIVE_DRIVER", "WRITE"));
                    } else {
                        MethodCallInfo.DynamicArgSource dyn = firstDynamicArg(inv.dynamicArgSources());
                        if (dyn != null) {
                            dynamicResults.add(new DetectedDynamicCollection(dyn.name(), dyn.kind(), dyn.ownerClass(),
                                    "NATIVE_DRIVER_DYNAMIC", "NATIVE_DRIVER", "WRITE"));
                        }
                    }
                }
                continue;
            }

            // D. Native MongoCollection methods — collection was obtained earlier via getCollection
            // We detect these to flag the operation type, even though collection name was captured above
            if (COLLECTION_METHODS.contains(methodName) && ownerContains(owner, "MongoCollection")) {
                // Collection name is in the stringArgs if the code chains getCollection("X").aggregate(...)
                String coll = firstCollectionArg(stringArgs);
                String opType = inferMongoOpType(methodName);
                if (coll != null) {
                    results.add(new DetectedCollection(coll, "NATIVE_DRIVER", opType != null ? opType : "READ"));
                } else {
                    // Chained getCollection(variable).aggregate(...)/insertOne(...)/etc. — the
                    // dynamic source propagated from getCollection's own (unresolved) argument.
                    MethodCallInfo.DynamicArgSource dyn = firstDynamicArg(inv.dynamicArgSources());
                    if (dyn != null) {
                        dynamicResults.add(new DetectedDynamicCollection(dyn.name(), dyn.kind(), dyn.ownerClass(),
                                "NATIVE_DRIVER_CHAINED_DYNAMIC", "NATIVE_DRIVER", opType != null ? opType : "READ"));
                    }
                }
                continue;
            }

            // E. GridFS operations (store, find, delete on GridFsTemplate/ReactiveGridFsTemplate)
            if (GRIDFS_METHODS.contains(methodName) && ownerMatchesAny(owner, GRIDFS_OWNERS)) {
                String coll = firstCollectionArg(stringArgs);
                String source = owner.contains("Reactive") ? "REACTIVE_GRIDFS" : "GRIDFS";
                String opType = inferMongoOpType(methodName);
                if (coll != null) {
                    results.add(new DetectedCollection(coll, source, opType != null ? opType : "READ"));
                } else {
                    MethodCallInfo.DynamicArgSource dyn = firstDynamicArg(inv.dynamicArgSources());
                    if (dyn != null) {
                        dynamicResults.add(new DetectedDynamicCollection(dyn.name(), dyn.kind(), dyn.ownerClass(),
                                source + "_DYNAMIC", source, opType != null ? opType : "READ"));
                    }
                }
                continue;
            }

            // F. ChangeStream operations
            if ("changeStream".equals(methodName) && ownerMatchesAny(owner, TEMPLATE_OWNERS)) {
                String coll = firstCollectionArg(stringArgs);
                String source = owner.contains("Reactive") ? "REACTIVE_CHANGE_STREAM" : "CHANGE_STREAM";
                if (coll != null) {
                    results.add(new DetectedCollection(coll, source, "READ"));
                } else {
                    MethodCallInfo.DynamicArgSource dyn = firstDynamicArg(inv.dynamicArgSources());
                    if (dyn != null) {
                        dynamicResults.add(new DetectedDynamicCollection(dyn.name(), dyn.kind(), dyn.ownerClass(),
                                source + "_DYNAMIC", source, "READ"));
                    }
                }
            }
        }

        // G. Context-based resolution for unresolved template calls.
        // When save/remove/update was detected but collection couldn't be resolved from string args
        // or __class: literals, infer from other invocations in the same method that reference
        // @Document bean classes (setters, constructors, field access on entity classes).
        //
        // IMPORTANT: only apply this to calls that have NO dynamic arg source of their own (dyn ==
        // null in the unresolved[2] slot). A call site whose actual argument was already captured
        // as a variable/field has a precise, call-site-specific signal already — the context scan
        // below is a much weaker heuristic that just looks for ANY invocation on an @Document
        // entity type ANYWHERE in the method, so letting it override an already-captured dynamic
        // arg means the wrong collection gets reported.
        List<Object[]> unresolvedWithoutDyn = new ArrayList<>();
        for (Object[] unresolved : unresolvedTemplateCalls) {
            MethodCallInfo.DynamicArgSource dyn = (MethodCallInfo.DynamicArgSource) unresolved[2];
            if (dyn != null) {
                dynamicResults.add(new DetectedDynamicCollection(dyn.name(), dyn.kind(), dyn.ownerClass(),
                        unresolved[0] + "_DYNAMIC", (String) unresolved[0], (String) unresolved[1]));
            } else {
                unresolvedWithoutDyn.add(unresolved);
            }
        }
        if (!unresolvedWithoutDyn.isEmpty()) {
            Set<String> contextCollections = new LinkedHashSet<>();
            if (!entityCollectionMap.isEmpty()) {
                for (InvocationRef inv : method.invocations) {
                    // Check if invocation target is an @Document entity class
                    String coll = resolveEntityCollection(inv.ownerClass(), entityCollectionMap);
                    if (coll != null) contextCollections.add(coll);
                    // Also check __class: args across all invocations
                    if (inv.stringArgs() != null) {
                        for (String arg : inv.stringArgs()) {
                            if (arg.startsWith("__class:")) {
                                coll = resolveEntityCollection(arg.substring(8), entityCollectionMap);
                                if (coll != null) contextCollections.add(coll);
                            }
                        }
                    }
                }
                // Also check documentCollection on the enclosing class itself
                if (cls.documentCollection != null) {
                    contextCollections.add(cls.documentCollection);
                }
                // Check method parameter types — handles thin wrappers: create(Department dept) { template.insert(dept); }
                for (String paramType : method.paramTypes) {
                    String coll = resolveEntityCollection(paramType, entityCollectionMap);
                    if (coll != null) contextCollections.add(coll);
                }
            }
            if (contextCollections.size() == 1) {
                String coll = contextCollections.iterator().next();
                for (Object[] unresolved : unresolvedWithoutDyn) {
                    results.add(new DetectedCollection(coll, unresolved[0] + "_CONTEXT", (String) unresolved[1]));
                }
            } else if (contextCollections.size() > 1) {
                for (String coll : contextCollections) {
                    for (Object[] unresolved : unresolvedWithoutDyn) {
                        results.add(new DetectedCollection(coll, unresolved[0] + "_AMBIGUOUS_CONTEXT", (String) unresolved[1]));
                    }
                }
            }
            // Note: call sites with NO context match AND no dynamic arg source are silently
            // dropped here (as before) — nothing resolvable was found for them at all.
        }

        return new DetectionResult(results, dynamicResults);
    }

    /**
     * Resolve the collection a SPECIFIC call site passes to a BulkWriteCollector.addInsert/
     * addUpdate/addDelete invocation — used to tag the CHILD node built for that call (the
     * addInsert/addUpdate method's own node) directly, since that method's own body never sees
     * the collection name (it only receives it as a parameter; the literal/constant lives at the
     * CALLER's call site, one level up). Returns null when this call isn't a BulkWriteCollector
     * buffering call, or when the collection name/dynamic source can't be resolved at all.
     */
    record CallSiteCollection(String collectionName, MethodCallInfo.DynamicArgSource dynamicArg, String operationType) {}

    CallSiteCollection resolveBulkCollectorCallSite(InvocationRef inv) {
        String methodName = inv.methodName();
        if (!BULK_COLLECTOR_METHODS.contains(methodName) || !ownerContains(inv.ownerClass(), "BulkWriteCollector")) {
            return null;
        }
        String opType = switch (methodName) {
            case "addInsert" -> "WRITE";
            case "addUpdate" -> "UPDATE";
            case "addDelete" -> "DELETE";
            default -> "WRITE";
        };
        String coll = firstCollectionArg(inv.stringArgs());
        if (coll != null) return new CallSiteCollection(coll, null, opType);
        MethodCallInfo.DynamicArgSource dyn = firstDynamicArg(inv.dynamicArgSources());
        if (dyn != null) return new CallSiteCollection(null, dyn, opType);
        return null;
    }

    /**
     * CALL-001: generic (any callee, not just BulkWriteCollector) single-hop call-site argument
     * resolution — used when the CALLEE's own PARAMETER is later used as a collection-name
     * argument inside the callee's body (i.e. the callee node already carries an unresolved
     * PARAM-kind dynamicCollectionRefs entry), so the actual value passed at THIS call site needs
     * resolving in the CALLER's context. Unlike resolveBulkCollectorCallSite above, this has no
     * method-name/owner-class gate — CollectionResolver only calls it after confirming the callee
     * actually has a PARAM dynamic ref waiting to be resolved, so any call site can qualify.
     * Reuses the exact same literal-then-dynamic resolution order (first collection-shaped string
     * literal wins, else first dynamic/variable source) as the BulkWriteCollector-specific path.
     */
    CallSiteCollection resolveGenericCallSiteArg(InvocationRef call) {
        String coll = firstCollectionArg(call.stringArgs());
        if (coll != null) return new CallSiteCollection(coll, null, null);
        MethodCallInfo.DynamicArgSource dyn = firstDynamicArg(call.dynamicArgSources());
        if (dyn != null) return new CallSiteCollection(null, dyn, null);
        return null;
    }

    /** First captured dynamic (variable/field-sourced) argument, if any — mirrors firstCollectionArg's
     *  "first match wins" semantics, but with no content filter possible (there's no resolved
     *  string to test against isLikelyCollectionName). Safe because this is only consulted on
     *  call sites already known by method-name+owner to expect a collection-name argument. */
    private MethodCallInfo.DynamicArgSource firstDynamicArg(List<MethodCallInfo.DynamicArgSource> args) {
        if (args == null || args.isEmpty()) return null;
        return args.get(0);
    }

    /** Find the first string argument that looks like a MongoDB collection name. */
    private String firstCollectionArg(List<String> stringArgs) {
        if (stringArgs == null) return null;
        for (String arg : stringArgs) {
            if (isCollectionName(arg)) return arg;
        }
        return null;
    }

    /** Check if a string is likely a real MongoDB collection name. */
    private boolean isCollectionName(String s) {
        return configLoader.isLikelyCollectionName(s);
    }

    /** Resolve a class FQN to its @Document collection name via entity collection map. */
    private String resolveEntityCollection(String classFqn, Map<String, String> entityCollectionMap) {
        if (classFqn == null) return null;
        String coll = entityCollectionMap.get(classFqn);
        if (coll != null) return coll;
        int dot = classFqn.lastIndexOf('.');
        if (dot > 0) coll = entityCollectionMap.get(classFqn.substring(dot + 1));
        if (coll != null) return coll;

        // Fallback: CamelCase → UPPER_SNAKE convention (ClmTransEntity → CLM_TRANS).
        // Only attempted when the class name itself ends with a known entity-style suffix —
        // without this guard, ANY class reached in this context-resolution loop (ArrayList,
        // JsonNode, RuntimeException, the enclosing handler class itself, etc.) would CamelCase-
        // convert into something that happens to match isLikelyCollectionName's generic
        // UPPER_SNAKE pattern (e.g. ArrayList -> ARRAY_LIST) and get reported as a real
        // collection. Requiring the suffix restricts the guess to classes that actually look
        // like domain entities.
        String simple = dot > 0 ? classFqn.substring(dot + 1) : classFqn;
        if (!simple.matches(".*(Entity|Document|Model|Dto|DO)$")) return null;
        String stem = simple.replaceAll("(Entity|Document|Model|Dto|DO)$", "");
        // Reject known MongoDB driver builder class stems — e.g. "UpdateOneModel" ends in
        // "Model" (a real entity-style suffix) but its stem "UpdateOne" is a native driver
        // WriteModel builder (com.mongodb.client.model.UpdateOneModel), not a domain entity.
        // CamelCase-converting it produces "UPDATE_ONE", which happens to pass
        // isLikelyCollectionName's generic UPPER_SNAKE pattern and gets falsely reported as a
        // collection (leaking from a `new UpdateOneModel(filter, update)` call built inside the
        // same method that's being resolved). Reject any stem matching a known driver builder
        // name (case-insensitive) before attempting the CamelCase guess.
        if (DRIVER_BUILDER_STEMS.contains(stem.toUpperCase())) return null;
        String upperSnake = stem.replaceAll("([a-z])([A-Z])", "$1_$2").toUpperCase();
        if (configLoader.isLikelyCollectionName(upperSnake)) return upperSnake;
        return null;
    }

    // MongoDB driver builder/model class stems (after stripping the Entity/Document/Model/Dto/DO
    // suffix in resolveEntityCollection above) that must never be guessed as domain entity names.
    // e.g. UpdateOneModel, InsertOneModel, DeleteOneModel, ReplaceOneModel (com.mongodb.client.model),
    // Aggregates/Filters/Updates/Projections/Sorts (aggregation/query builder companions),
    // BulkWriteOptions/BulkOperations (bulk write builders).
    private static final Set<String> DRIVER_BUILDER_STEMS = Set.of(
            "UPDATEONE", "INSERTONE", "DELETEONE", "REPLACEONE",
            "UPDATEMANY", "INSERTMANY", "DELETEMANY",
            "AGGREGATES", "FILTERS", "UPDATES", "PROJECTIONS", "SORTS", "ACCUMULATORS",
            "BULK", "BULKWRITE", "BULKOPERATIONS", "BULKWRITEOPTIONS", "WRITEMODEL"
    );

    /** Check if the owner class name contains a given substring (case-insensitive). */
    private boolean ownerContains(String owner, String substring) {
        return owner != null && owner.toLowerCase().contains(substring.toLowerCase());
    }

    /**
     * True when the method itself IS one of the BulkWriteCollector buffering methods
     * (addInsert/addUpdate/addDelete DEFINED on a BulkWriteCollector-pattern class), as opposed
     * to hasDbInvocations() below which checks whether a method CALLS into one.
     *
     * Needed because these buffer methods only stage an operation in an in-memory list — the
     * actual Mongo write happens later, in a different class's execute()/runCommand(), by which
     * point the collection name is no longer visible as a string literal. Without this check,
     * CollectionResolver.collectCollections finds no resolvable collection and no other
     * DB-invocation signal for addInsert/addUpdate/addDelete themselves, so the post-build cleanup
     * pass in CallTreeBuilder treats them as false-positive leaves/non-leaves and either nulls or
     * re-derives their operationType from unrelated child calls.
     */
    boolean isBulkCollectorOwnMethod(IndexedClass cls, IndexedMethod method) {
        return cls != null && method != null
                && BULK_COLLECTOR_METHODS.contains(method.name)
                && ownerContains(cls.fqn, "BulkWriteCollector");
    }

    /**
     * Quick check: does this method invoke any MongoTemplate/MongoOperations/native driver methods?
     * Used to set hasDbInteraction on CallNode even when collection can't be resolved.
     */
    boolean hasDbInvocations(IndexedMethod method) {
        for (InvocationRef inv : method.invocations) {
            String owner = inv.ownerClass();
            String mn = inv.methodName();
            if (TEMPLATE_METHODS.contains(mn) && ownerMatchesAny(owner, TEMPLATE_OWNERS)) return true;
            if (COLLECTION_METHODS.contains(mn) && ownerContains(owner, "MongoCollection")) return true;
            if (DATABASE_METHODS.contains(mn) && ownerMatchesAny(owner, DATABASE_OWNERS)) return true;
            if (BULK_COLLECTOR_METHODS.contains(mn) && ownerContains(owner, "BulkWriteCollector")) return true;
            if (GRIDFS_METHODS.contains(mn) && ownerMatchesAny(owner, GRIDFS_OWNERS)) return true;
        }
        return false;
    }

    /** Check if the owner class name ends with any of the given suffixes.
     *  Also checks interface hierarchy from the resolution context if provided. */
    private boolean ownerMatchesAny(String owner, List<String> patterns) {
        if (owner == null) return false;
        for (String pattern : patterns) {
            if (owner.endsWith(pattern) || owner.endsWith("." + pattern)) return true;
        }
        return false;
    }
}
