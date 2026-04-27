package com.jaranalyzer.service;

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

    private final DomainConfigLoader configLoader;

    MongoMethodDetector(DomainConfigLoader configLoader) {
        this.configLoader = configLoader;
    }

    /**
     * Scan all invocations in a method for MongoDB collection access patterns.
     * Returns detected collections with their source and operation type.
     */
    List<DetectedCollection> detect(IndexedMethod method, IndexedClass cls) {
        return detect(method, cls, Map.of());
    }

    /**
     * Scan with entity→collection mapping for resolving bean class references.
     * e.g. mongoOperations.updateMulti(query, update, Bean.class)
     */
    List<DetectedCollection> detect(IndexedMethod method, IndexedClass cls,
                                     Map<String, String> entityCollectionMap) {
        List<DetectedCollection> results = new ArrayList<>();
        // Track unresolved template calls for post-processing context resolution
        List<String[]> unresolvedTemplateCalls = new ArrayList<>(); // [source, opType]

        for (InvocationRef inv : method.invocations) {
            String owner = inv.ownerClass();
            String methodName = inv.methodName();
            List<String> stringArgs = inv.stringArgs();

            // A. BulkWriteCollector pattern
            if (BULK_COLLECTOR_METHODS.contains(methodName) && ownerContains(owner, "BulkWriteCollector")) {
                String coll = firstCollectionArg(stringArgs);
                if (coll != null) {
                    String opType = switch (methodName) {
                        case "addInsert" -> "WRITE";
                        case "addUpdate" -> "UPDATE";
                        case "addDelete" -> "DELETE";
                        default -> "WRITE";
                    };
                    results.add(new DetectedCollection(coll, "BULK_WRITE_COLLECTOR", opType));
                }
                continue;
            }

            // B. MongoTemplate / MongoOperations / Reactive variants
            if (TEMPLATE_METHODS.contains(methodName) && ownerMatchesAny(owner, TEMPLATE_OWNERS)) {
                String coll = firstCollectionArg(stringArgs);
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
                if (coll != null) {
                    String source = owner.contains("Reactive") ? "REACTIVE_MONGO_TEMPLATE" : "MONGO_TEMPLATE";
                    String opType = configLoader.inferOperationType(methodName, "REPOSITORY");
                    results.add(new DetectedCollection(coll, source, opType != null ? opType : "READ"));
                } else {
                    // Track unresolved call for context-based resolution below
                    String source = owner.contains("Reactive") ? "REACTIVE_MONGO_TEMPLATE" : "MONGO_TEMPLATE";
                    String opType = configLoader.inferOperationType(methodName, "REPOSITORY");
                    unresolvedTemplateCalls.add(new String[]{source, opType != null ? opType : "READ"});
                }
                continue;
            }

            // C. Native MongoDatabase methods
            if (DATABASE_METHODS.contains(methodName) && ownerMatchesAny(owner, DATABASE_OWNERS)) {
                if ("getCollection".equals(methodName)) {
                    String coll = firstCollectionArg(stringArgs);
                    if (coll != null) {
                        results.add(new DetectedCollection(coll, "NATIVE_DRIVER", "READ"));
                    }
                } else if ("runCommand".equals(methodName)) {
                    // runCommand can contain bulkWrite with collection refs in the command doc
                    // Flag as NATIVE_COMMAND — the actual collection is in the command structure
                    for (String arg : stringArgs) {
                        if (isCollectionName(arg)) {
                            results.add(new DetectedCollection(arg, "NATIVE_COMMAND", "WRITE"));
                        }
                    }
                } else if ("createCollection".equals(methodName)) {
                    String coll = firstCollectionArg(stringArgs);
                    if (coll != null) {
                        results.add(new DetectedCollection(coll, "NATIVE_DRIVER", "WRITE"));
                    }
                }
                continue;
            }

            // D. Native MongoCollection methods — collection was obtained earlier via getCollection
            // We detect these to flag the operation type, even though collection name was captured above
            if (COLLECTION_METHODS.contains(methodName) && ownerContains(owner, "MongoCollection")) {
                // Collection name is in the stringArgs if the code chains getCollection("X").aggregate(...)
                String coll = firstCollectionArg(stringArgs);
                if (coll != null) {
                    String opType = configLoader.inferOperationType(methodName, "REPOSITORY");
                    results.add(new DetectedCollection(coll, "NATIVE_DRIVER", opType != null ? opType : "READ"));
                }
                continue;
            }

            // E. GridFS operations (store, find, delete on GridFsTemplate/ReactiveGridFsTemplate)
            if (GRIDFS_METHODS.contains(methodName) && ownerMatchesAny(owner, GRIDFS_OWNERS)) {
                String coll = firstCollectionArg(stringArgs);
                if (coll != null) {
                    String source = owner.contains("Reactive") ? "REACTIVE_GRIDFS" : "GRIDFS";
                    String opType = configLoader.inferOperationType(methodName, "REPOSITORY");
                    results.add(new DetectedCollection(coll, source, opType != null ? opType : "READ"));
                }
                continue;
            }

            // F. ChangeStream operations
            if ("changeStream".equals(methodName) && ownerMatchesAny(owner, TEMPLATE_OWNERS)) {
                String coll = firstCollectionArg(stringArgs);
                if (coll != null) {
                    String source = owner.contains("Reactive") ? "REACTIVE_CHANGE_STREAM" : "CHANGE_STREAM";
                    results.add(new DetectedCollection(coll, source, "READ"));
                }
            }
        }

        // G. Context-based resolution for unresolved template calls.
        // When save/remove/update was detected but collection couldn't be resolved from string args
        // or __class: literals, infer from other invocations in the same method that reference
        // @Document bean classes (setters, constructors, field access on entity classes).
        if (!unresolvedTemplateCalls.isEmpty() && !entityCollectionMap.isEmpty()) {
            Set<String> contextCollections = new LinkedHashSet<>();
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
            if (contextCollections.size() == 1) {
                String coll = contextCollections.iterator().next();
                for (String[] unresolved : unresolvedTemplateCalls) {
                    results.add(new DetectedCollection(coll, unresolved[0] + "_CONTEXT", unresolved[1]));
                }
            } else if (contextCollections.size() > 1) {
                for (String coll : contextCollections) {
                    for (String[] unresolved : unresolvedTemplateCalls) {
                        results.add(new DetectedCollection(coll, unresolved[0] + "_AMBIGUOUS_CONTEXT", unresolved[1]));
                    }
                }
            }
        }

        return results;
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

        // Fallback: CamelCase → UPPER_SNAKE convention (ClmTransEntity → CLM_TRANS)
        String simple = dot > 0 ? classFqn.substring(dot + 1) : classFqn;
        String stem = simple.replaceAll("(Entity|Document|Model|Dto|DO)$", "");
        String upperSnake = stem.replaceAll("([a-z])([A-Z])", "$1_$2").toUpperCase();
        if (configLoader.isLikelyCollectionName(upperSnake)) return upperSnake;
        return null;
    }

    /** Check if the owner class name contains a given substring (case-insensitive). */
    private boolean ownerContains(String owner, String substring) {
        return owner != null && owner.toLowerCase().contains(substring.toLowerCase());
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
