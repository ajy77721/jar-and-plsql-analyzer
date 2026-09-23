package com.jaranalyzer.service;

import com.jaranalyzer.service.CallGraphIndex.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Detects Redis key access from method invocations in bytecode.
 * Analyzes InvocationRefs to find Redis key patterns from:
 *
 *   A. RedisTemplate / StringRedisTemplate / Reactive variants
 *   B. Operations: ValueOperations, ListOperations, HashOperations, SetOperations, ZSetOperations
 *   C. Bound operations: BoundValueOperations, BoundListOperations, BoundHashOperations
 */
@Component
class RedisMethodDetector {

    private static final Logger log = LoggerFactory.getLogger(RedisMethodDetector.class);

    /** Result of detecting a Redis key access from a method invocation. */
    record DetectedCollection(String collectionName, String source, String operationType) {}

    // Owner class name patterns for RedisTemplate and Operations types
    private static final List<String> TEMPLATE_OWNERS = List.of(
            "RedisTemplate", "StringRedisTemplate",
            "ReactiveRedisTemplate", "ReactiveStringRedisTemplate"
    );

    private static final List<String> OPERATIONS_OWNERS = List.of(
            "ValueOperations", "ListOperations", "HashOperations",
            "SetOperations", "ZSetOperations",
            "BoundValueOperations", "BoundListOperations", "BoundHashOperations"
    );

    // Method → operation type mapping
    private static final Map<String, String> METHOD_OP_MAP = Map.ofEntries(
            // WRITE operations
            Map.entry("set",       "WRITE"),
            Map.entry("add",       "WRITE"),
            Map.entry("put",       "WRITE"),
            Map.entry("putAll",    "WRITE"),
            Map.entry("leftPush",  "WRITE"),
            Map.entry("rightPush", "WRITE"),
            Map.entry("insert",    "WRITE"),
            Map.entry("rename",    "WRITE"),
            // READ operations
            Map.entry("get",       "READ"),
            Map.entry("range",     "READ"),
            Map.entry("entries",   "READ"),
            Map.entry("members",   "READ"),
            Map.entry("score",     "READ"),
            Map.entry("rank",      "READ"),
            Map.entry("size",      "READ"),
            Map.entry("hasKey",    "READ"),
            Map.entry("keys",      "READ"),
            Map.entry("scan",      "READ"),
            Map.entry("exists",    "READ"),
            // DELETE operations
            Map.entry("delete",    "DELETE"),
            Map.entry("remove",    "DELETE"),
            Map.entry("pop",       "DELETE"),
            // UPDATE operations (TTL management and increment/decrement)
            Map.entry("expire",    "UPDATE"),
            Map.entry("expireAt",  "UPDATE"),
            Map.entry("persist",   "UPDATE"),
            Map.entry("increment", "UPDATE"),
            Map.entry("decrement", "UPDATE"),
            Map.entry("incrBy",    "UPDATE"),
            Map.entry("decrBy",    "UPDATE")
    );

    // All detected method names (union of the above)
    private static final Set<String> ALL_METHODS = METHOD_OP_MAP.keySet();

    /**
     * Scan all invocations in a method for Redis key access patterns.
     * Returns detected keys with their source and operation type.
     */
    List<DetectedCollection> detect(IndexedClass cls, IndexedMethod method, List<InvocationRef> invocations) {
        List<DetectedCollection> results = new ArrayList<>();

        for (InvocationRef inv : invocations) {
            String owner = inv.ownerClass();
            String methodName = inv.methodName();
            List<String> stringArgs = inv.stringArgs();

            if (!ALL_METHODS.contains(methodName)) continue;

            boolean isTemplate = ownerMatchesAny(owner, TEMPLATE_OWNERS);
            boolean isOperations = ownerMatchesAny(owner, OPERATIONS_OWNERS);
            if (!isTemplate && !isOperations) continue;

            String opType = METHOD_OP_MAP.get(methodName);
            String keyPattern = firstStringArg(stringArgs);
            if (keyPattern == null) {
                keyPattern = "redis:unknown-key";
            }

            String source = "REDIS_TEMPLATE";
            results.add(new DetectedCollection(keyPattern, source, opType));
            log.debug("Redis detected: key={} op={} owner={} method={}", keyPattern, opType, owner, methodName);
        }

        return results;
    }

    /**
     * Quick check: does this method invoke any Redis template/operations methods?
     */
    boolean hasRedisInvocations(IndexedMethod method) {
        for (InvocationRef inv : method.invocations) {
            String owner = inv.ownerClass();
            String mn = inv.methodName();
            if (ALL_METHODS.contains(mn)
                    && (ownerMatchesAny(owner, TEMPLATE_OWNERS) || ownerMatchesAny(owner, OPERATIONS_OWNERS))) {
                return true;
            }
        }
        return false;
    }

    /** Return first string argument from the list (skips __class: markers). */
    private String firstStringArg(List<String> stringArgs) {
        if (stringArgs == null) return null;
        for (String arg : stringArgs) {
            if (arg != null && !arg.startsWith("__class:") && !arg.isBlank()) {
                return arg;
            }
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
