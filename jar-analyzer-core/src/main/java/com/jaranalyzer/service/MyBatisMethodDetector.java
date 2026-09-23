package com.jaranalyzer.service;

import com.jaranalyzer.service.CallGraphIndex.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Detects MyBatis table access from method invocations in bytecode.
 *
 * Detection categories:
 *   A. SqlSession / SqlSessionTemplate: selectOne, selectList, selectMap, selectCursor,
 *      insert, update, delete — mapper statement ID passed as first string arg,
 *      resolved via namedQueryMap when available
 *   B. @Mapper interface methods annotated with @Select, @Insert, @Update, @Delete,
 *      @SelectProvider, @InsertProvider, @UpdateProvider, @DeleteProvider —
 *      SQL (or provider hint) extracted from annotation "value" attribute
 */
@Component
class MyBatisMethodDetector {

    private static final Logger log = LoggerFactory.getLogger(MyBatisMethodDetector.class);

    private static final Set<String> SESSION_OWNERS = Set.of(
            "SqlSession", "DefaultSqlSession", "SqlSessionTemplate"
    );

    private static final Set<String> SESSION_READ_METHODS = Set.of(
            "selectOne", "selectList", "selectMap", "selectCursor"
    );

    private static final Set<String> SESSION_WRITE_METHODS = Set.of(
            "insert", "update", "delete"
    );

    /** SQL annotation simple names on @Mapper interface methods. */
    private static final Set<String> SQL_ANNOTATIONS = Set.of(
            "Select", "Insert", "Update", "Delete",
            "SelectProvider", "InsertProvider", "UpdateProvider", "DeleteProvider"
    );

    /** Result of detecting a table from a MyBatis invocation or annotation. */
    record DetectedTable(String tableName, String source, String operationType) {}

    /**
     * Scan all invocations in a method for MyBatis table access patterns.
     *
     * @param method        the method being analysed
     * @param cls           the enclosing class
     * @param namedQueryMap mapper statement ID / named query → SQL text
     * @return list of detected table accesses (may be empty, never null)
     */
    List<DetectedTable> detect(IndexedMethod method, IndexedClass cls,
                               Map<String, String> namedQueryMap) {
        List<DetectedTable> results = new ArrayList<>();

        // A. SqlSession / SqlSessionTemplate invocations
        for (InvocationRef call : method.invocations) {
            String owner = simpleOwner(call.ownerClass());
            String methodName = call.methodName();

            if (!SESSION_OWNERS.contains(owner)) continue;

            if (SESSION_READ_METHODS.contains(methodName)) {
                String table = extractTableFromMapperId(call, namedQueryMap);
                results.add(new DetectedTable(
                        table != null ? table : "?", "MYBATIS_SESSION", "READ"));
            } else if (SESSION_WRITE_METHODS.contains(methodName)) {
                String table = extractTableFromMapperId(call, namedQueryMap);
                String op = "delete".equals(methodName) ? "DELETE" : "WRITE";
                results.add(new DetectedTable(
                        table != null ? table : "?", "MYBATIS_SESSION", op));
            }
        }

        // B. @Mapper interface method annotations (@Select, @Insert, @Update, @Delete, *Provider)
        for (AnnotationData ann : method.annotationDetailList) {
            String annSimple = simpleName(ann.name());
            if (!SQL_ANNOTATIONS.contains(annSimple)) continue;

            String sql = extractSqlFromAnnotation(ann);
            String op  = inferOpFromAnnotation(annSimple);
            if (sql != null) {
                String table = extractTableFromSql(sql);
                String tableName = table != null ? table : "?";
                results.add(new DetectedTable(tableName, "MYBATIS_ANNOTATION", op));
            } else {
                // Provider annotation with no resolvable SQL — record placeholder
                results.add(new DetectedTable("?", "MYBATIS_ANNOTATION", op));
            }
        }

        // Deduplicate by (tableName, operationType)
        Map<String, DetectedTable> deduped = new LinkedHashMap<>();
        for (DetectedTable dt : results) {
            deduped.putIfAbsent(dt.tableName() + "|" + dt.operationType(), dt);
        }
        return new ArrayList<>(deduped.values());
    }

    /**
     * Quick check: does this method make any SqlSession calls?
     * Used for hasDbInteraction flagging.
     */
    boolean hasMyBatisInvocations(IndexedMethod method) {
        for (InvocationRef call : method.invocations) {
            String owner = simpleOwner(call.ownerClass());
            if (SESSION_OWNERS.contains(owner)
                    && (SESSION_READ_METHODS.contains(call.methodName())
                        || SESSION_WRITE_METHODS.contains(call.methodName()))) {
                return true;
            }
        }
        // Also check for @Select / @Insert etc. on the method itself
        for (AnnotationData ann : method.annotationDetailList) {
            if (SQL_ANNOTATIONS.contains(simpleName(ann.name()))) return true;
        }
        return false;
    }

    // ---------------------------------------------------------------- private helpers

    /**
     * Resolve the first string arg of a SqlSession call (mapper statement ID) to a table name.
     * Statement IDs are typically "com.example.OrderMapper.findAll".
     * If the namedQueryMap contains an entry for the ID, extract the table from the SQL.
     */
    private String extractTableFromMapperId(InvocationRef call, Map<String, String> namedQueryMap) {
        if (call.stringArgs() == null || call.stringArgs().isEmpty()) return null;
        String stmtId = call.stringArgs().get(0);
        if (stmtId == null || stmtId.startsWith("__class:")) return null;
        if (namedQueryMap != null && namedQueryMap.containsKey(stmtId)) {
            return extractTableFromSql(namedQueryMap.get(stmtId));
        }
        return null;
    }

    /** Extract the first table/object name from a SQL fragment using FROM/INTO/UPDATE/JOIN. */
    private String extractTableFromSql(String sql) {
        if (sql == null || sql.isBlank()) return null;
        Matcher m = Pattern.compile(
                "(?i)\\b(?:FROM|INTO|UPDATE|JOIN)\\s+([A-Za-z_][A-Za-z0-9_.]*)"
        ).matcher(sql);
        return m.find() ? m.group(1).toUpperCase() : null;
    }

    /** Extract the SQL (or provider hint) from a MyBatis annotation value attribute. */
    private String extractSqlFromAnnotation(AnnotationData ann) {
        if (ann.attributes() == null) return null;
        Object val = ann.attributes().get("value");
        if (val instanceof String s) return s.isBlank() ? null : s;
        if (val instanceof String[] arr) {
            String joined = String.join(" ", arr);
            return joined.isBlank() ? null : joined;
        }
        return null;
    }

    /** Map annotation simple name to an operation type string. */
    private String inferOpFromAnnotation(String annSimple) {
        String n = annSimple.toLowerCase();
        if (n.contains("select")) return "READ";
        if (n.contains("insert")) return "WRITE";
        if (n.contains("update")) return "WRITE";
        if (n.contains("delete")) return "DELETE";
        return "READ";
    }

    /** Return the simple (unqualified) class name from a possibly FQN string. */
    private String simpleOwner(String fqn) {
        if (fqn == null) return "";
        int dot = fqn.lastIndexOf('.');
        return dot >= 0 ? fqn.substring(dot + 1) : fqn;
    }

    /** Return the simple name from a possibly FQN annotation name. */
    private String simpleName(String fqn) {
        if (fqn == null) return "";
        int dot = fqn.lastIndexOf('.');
        return dot >= 0 ? fqn.substring(dot + 1) : fqn;
    }
}
