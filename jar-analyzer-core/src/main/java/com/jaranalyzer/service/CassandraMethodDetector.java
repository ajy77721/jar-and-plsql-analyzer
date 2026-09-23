package com.jaranalyzer.service;

import com.jaranalyzer.service.CallGraphIndex.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Detects Cassandra table access from method invocations in bytecode.
 * Analyzes InvocationRefs to find table names from:
 *
 *   A. CqlSession: execute / executeAsync with CQL string args
 *   B. CassandraTemplate / ReactiveCassandraTemplate: insert, select, delete, update, count, exists
 *   C. CassandraOperations / ReactiveCassandraOperations: same method set as B
 */
@Component
class CassandraMethodDetector {

    private static final Logger log = LoggerFactory.getLogger(CassandraMethodDetector.class);

    /** Result of detecting a Cassandra table access from a method invocation. */
    record DetectedCollection(String collectionName, String source, String operationType) {}

    // Owner class name patterns for CqlSession
    private static final List<String> SESSION_OWNERS = List.of(
            "CqlSession"
    );

    // Owner class name patterns for Template / Operations
    private static final List<String> TEMPLATE_OWNERS = List.of(
            "CassandraTemplate", "ReactiveCassandraTemplate",
            "CassandraOperations", "ReactiveCassandraOperations"
    );

    // CqlSession execute methods (CQL string passed as arg)
    private static final Set<String> SESSION_EXEC_METHODS = Set.of(
            "execute", "executeAsync"
    );

    // Template/Operations explicit op methods
    private static final Map<String, String> TEMPLATE_METHOD_OPS = Map.ofEntries(
            Map.entry("insert",        "WRITE"),
            Map.entry("save",          "WRITE"),
            Map.entry("batchOps",      "WRITE"),
            Map.entry("select",        "READ"),
            Map.entry("selectOne",     "READ"),
            Map.entry("selectOneById", "READ"),
            Map.entry("count",         "READ"),
            Map.entry("exists",        "READ"),
            Map.entry("delete",        "DELETE"),
            Map.entry("deleteById",    "DELETE"),
            Map.entry("update",        "UPDATE")
    );

    // CQL keyword → operation type for execute/executeAsync
    private static final Map<String, String> CQL_KEYWORD_OPS = Map.of(
            "SELECT", "READ",
            "INSERT", "WRITE",
            "UPDATE", "UPDATE",
            "DELETE", "DELETE"
    );

    // Patterns to extract table name from CQL strings
    // Handles: FROM x, INTO x, UPDATE x, TABLE x (CREATE TABLE / DROP TABLE)
    private static final List<Pattern> TABLE_EXTRACTION_PATTERNS = List.of(
            Pattern.compile("(?i)\\bFROM\\s+([\\w.\"]+)"),
            Pattern.compile("(?i)\\bINTO\\s+([\\w.\"]+)"),
            Pattern.compile("(?i)^\\s*UPDATE\\s+([\\w.\"]+)"),
            Pattern.compile("(?i)\\bTABLE\\s+([\\w.\"]+)")
    );

    /**
     * Scan all invocations in a method for Cassandra table access patterns.
     * Returns detected tables with their source and operation type.
     */
    List<DetectedCollection> detect(IndexedClass cls, IndexedMethod method, List<InvocationRef> invocations) {
        List<DetectedCollection> results = new ArrayList<>();

        for (InvocationRef inv : invocations) {
            String owner = inv.ownerClass();
            String methodName = inv.methodName();
            List<String> stringArgs = inv.stringArgs();

            // A. CqlSession.execute / executeAsync — parse CQL string
            if (SESSION_EXEC_METHODS.contains(methodName) && ownerMatchesAny(owner, SESSION_OWNERS)) {
                String cql = firstStringArg(stringArgs);
                if (cql != null) {
                    String opType = inferCqlOpType(cql);
                    String table = extractTableFromCql(cql);
                    if (table == null) table = "cassandra:unknown-table";
                    results.add(new DetectedCollection(table, "CASSANDRA_SESSION", opType != null ? opType : "READ"));
                    log.debug("Cassandra CQL detected: table={} op={}", table, opType);
                }
                continue;
            }

            // B/C. CassandraTemplate / Operations explicit methods
            if (TEMPLATE_METHOD_OPS.containsKey(methodName) && ownerMatchesAny(owner, TEMPLATE_OWNERS)) {
                String opType = TEMPLATE_METHOD_OPS.get(methodName);
                // Try to extract table name from string args (e.g. select(query, SomeEntity.class))
                String table = extractTableFromArgs(stringArgs);
                if (table == null) table = "cassandra:unknown-table";
                String source = owner != null && owner.contains("Reactive") ? "REACTIVE_CASSANDRA_TEMPLATE" : "CASSANDRA_TEMPLATE";
                results.add(new DetectedCollection(table, source, opType));
                log.debug("Cassandra template detected: table={} op={} method={}", table, opType, methodName);
                continue;
            }
        }

        return results;
    }

    /**
     * Quick check: does this method invoke any Cassandra methods?
     */
    boolean hasCassandraInvocations(IndexedMethod method) {
        for (InvocationRef inv : method.invocations) {
            String owner = inv.ownerClass();
            String mn = inv.methodName();
            if (SESSION_EXEC_METHODS.contains(mn) && ownerMatchesAny(owner, SESSION_OWNERS)) return true;
            if (TEMPLATE_METHOD_OPS.containsKey(mn) && ownerMatchesAny(owner, TEMPLATE_OWNERS)) return true;
        }
        return false;
    }

    /** Infer operation type from CQL text by looking at first keyword. */
    private String inferCqlOpType(String cql) {
        if (cql == null || cql.isBlank()) return null;
        String upper = cql.trim().toUpperCase();
        for (Map.Entry<String, String> entry : CQL_KEYWORD_OPS.entrySet()) {
            if (upper.startsWith(entry.getKey())) return entry.getValue();
        }
        return null;
    }

    /** Extract the primary table name from a CQL string using regex patterns. */
    private String extractTableFromCql(String cql) {
        if (cql == null || cql.isBlank()) return null;
        for (Pattern p : TABLE_EXTRACTION_PATTERNS) {
            Matcher m = p.matcher(cql);
            if (m.find()) {
                String raw = m.group(1);
                if (raw != null) {
                    // Strip quotes and keyspace prefix (keyspace.table → table)
                    raw = raw.replace("\"", "").trim();
                    int dot = raw.lastIndexOf('.');
                    return dot >= 0 ? raw.substring(dot + 1) : raw;
                }
            }
        }
        return null;
    }

    /** Try to find a table/entity name from string args of a template call. */
    private String extractTableFromArgs(List<String> stringArgs) {
        if (stringArgs == null) return null;
        for (String arg : stringArgs) {
            if (arg == null || arg.startsWith("__class:") || arg.isBlank()) continue;
            // If arg looks like a CQL string, extract from it
            String upper = arg.trim().toUpperCase();
            boolean looksCql = upper.startsWith("SELECT") || upper.startsWith("INSERT")
                    || upper.startsWith("UPDATE") || upper.startsWith("DELETE")
                    || upper.startsWith("FROM");
            if (looksCql) {
                String table = extractTableFromCql(arg);
                if (table != null) return table;
            }
            // Otherwise treat it as a direct table name
            if (arg.matches("[A-Za-z_][A-Za-z0-9_]*")) {
                return arg;
            }
        }
        return null;
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
