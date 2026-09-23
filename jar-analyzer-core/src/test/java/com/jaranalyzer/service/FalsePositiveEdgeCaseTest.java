package com.jaranalyzer.service;

import com.jaranalyzer.model.ClassInfo;
import com.jaranalyzer.model.MethodInfo;
import com.jaranalyzer.service.CallGraphIndex.IndexedClass;
import com.jaranalyzer.service.CallGraphIndex.IndexedMethod;
import com.jaranalyzer.service.CallGraphIndex.InvocationRef;
import com.jaranalyzer.service.JpaMethodDetector.DetectedTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Edge-case tests for false-positive prevention in JpaMethodDetector.
 *
 * These tests verify that common non-DB API calls — logger methods,
 * collection utilities, DTO getters, framework factories, and string-building
 * operations — are never tagged as database operations.
 *
 * All 10 tests use the same programmatic stub approach as FalsePositiveDetectionTest:
 *   – No Spring context, no bytecode parsing, no DB connection.
 *   – InvocationRef built with ownerClass / methodName / stringArgs matching production patterns.
 *   – Assertion: detect() returns an empty list (no false DB operation).
 */
@DisplayName("False-positive prevention — additional edge cases")
class FalsePositiveEdgeCaseTest {

    // ─── infrastructure ──────────────────────────────────────────────────────

    private JpaMethodDetector detector;

    @BeforeEach
    void setUp() {
        DomainConfigLoader loader = new DomainConfigLoader();
        detector = new JpaMethodDetector(loader);
    }

    // ─── stub helpers ────────────────────────────────────────────────────────

    private static final Pattern NEVER_MATCH = Pattern.compile("(?!x)x");

    private static IndexedClass serviceClass(String fqn, String simpleName) {
        ClassInfo ci = new ClassInfo();
        ci.setFullyQualifiedName(fqn);
        ci.setSimpleName(simpleName);
        ci.setStereotype("SERVICE");
        return new IndexedClass(ci, NEVER_MATCH);
    }

    /** Build an IndexedMethod with a single InvocationRef. */
    private static IndexedMethod methodWithOneCall(String callerName,
                                                    String ownerClass,
                                                    String calledMethod,
                                                    String receiverField,
                                                    String... stringArgs) {
        MethodInfo mi = new MethodInfo();
        mi.setName(callerName);
        mi.setDescriptor("()V");
        mi.setReturnType("void");
        IndexedMethod im = new IndexedMethod(mi);
        im.invocations.add(new InvocationRef(
                ownerClass, calledMethod, "()V", "void",
                List.of(stringArgs), 182, receiverField));
        return im;
    }

    private List<DetectedTable> detect(IndexedMethod im, IndexedClass cls) {
        return detector.detect(im, cls, Map.of(), Map.of()).resolved();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // FP-EC1: log.info("SELECT * FROM orders") — SQL string as log arg
    // ─────────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("FP-EC1 — Logger calls with SQL-like strings are NOT DB operations")
    class LoggerWithSqlStrings {

        @Test
        @DisplayName("log.info() with SELECT string → no DB detection")
        void logInfoWithSelectString() {
            IndexedClass cls = serviceClass("com.example.OrderService", "OrderService");
            IndexedMethod im = methodWithOneCall(
                    "fetchOrders",
                    "org.slf4j.Logger",
                    "info",
                    "log",  // receiver field name = logger field
                    "SELECT * FROM orders WHERE status = 'ACTIVE'");

            List<DetectedTable> results = detect(im, cls);

            assertTrue(results.isEmpty(),
                    "log.info() with SQL string arg must NOT produce a DB detection; got: " + results);
        }

        @Test
        @DisplayName("logger.debug() with SQL string → no DB detection")
        void loggerDebugWithSqlString() {
            IndexedClass cls = serviceClass("com.example.OrderService", "OrderService");
            IndexedMethod im = methodWithOneCall(
                    "loadOrders",
                    "org.slf4j.Logger",
                    "debug",
                    "logger",  // receiver field name
                    "Querying: SELECT id FROM orders");

            List<DetectedTable> results = detect(im, cls);

            assertTrue(results.isEmpty(),
                    "logger.debug() with SQL string must NOT produce a DB detection; got: " + results);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // FP-EC2: List.remove(0) — index-based list remove → NOT a DELETE
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("FP-EC2 — List.remove(int index) → NOT a DB DELETE operation")
    void listRemoveByIndexNotDbDelete() {
        IndexedClass cls = serviceClass("com.example.BatchService", "BatchService");
        IndexedMethod im = methodWithOneCall(
                "removeFirst",
                "java.util.List",
                "remove",
                null);  // no receiver field

        List<DetectedTable> results = detect(im, cls);

        assertTrue(results.isEmpty(),
                "java.util.List.remove() must NOT be tagged as a DB DELETE; got: " + results);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // FP-EC3: Map.get("key") — map get → NOT a READ DB op
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("FP-EC3 — Map.get(key) → NOT a DB READ operation")
    void mapGetNotDbRead() {
        IndexedClass cls = serviceClass("com.example.CacheService", "CacheService");
        IndexedMethod im = methodWithOneCall(
                "lookup",
                "java.util.Map",
                "get",
                null,
                "ORDERS");  // string arg looks like a collection name but it's a Map key

        List<DetectedTable> results = detect(im, cls);

        assertTrue(results.isEmpty(),
                "java.util.Map.get() must NOT be tagged as a DB READ; got: " + results);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // FP-EC4: Optional.get() → NOT a DB op
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("FP-EC4 — Optional.get() → NOT a DB operation")
    void optionalGetNotDbOp() {
        IndexedClass cls = serviceClass("com.example.OrderService", "OrderService");
        IndexedMethod im = methodWithOneCall(
                "unwrap",
                "java.util.Optional",
                "get",
                null);

        List<DetectedTable> results = detect(im, cls);

        assertTrue(results.isEmpty(),
                "java.util.Optional.get() must NOT be tagged as a DB operation; got: " + results);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // FP-EC5: entity.getId() — DTO getter → NOT a DB op
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("FP-EC5 — DTO/entity getter getId() → NOT a DB operation")
    void dtoGetterNotDbOp() {
        IndexedClass cls = serviceClass("com.example.OrderService", "OrderService");
        IndexedMethod im = methodWithOneCall(
                "processOrder",
                "com.example.Order",
                "getId",
                null);

        List<DetectedTable> results = detect(im, cls);

        // JpaMethodDetector detects invocations on known DB owners; plain DTO getter
        // on a domain class is not in any JDBC/JPA owner list → must produce no detection.
        assertTrue(results.isEmpty(),
                "Plain DTO getter getId() must NOT be tagged as a DB operation; got: " + results);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // FP-EC6: ResponseEntity.ok(data) → NOT a DB op
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("FP-EC6 — ResponseEntity.ok() factory method → NOT a DB operation")
    void responseEntityOkNotDbOp() {
        IndexedClass cls = serviceClass("com.example.OrderController", "OrderController");
        // INVOKESTATIC on a Spring framework class
        IndexedMethod im = new IndexedMethod(new MethodInfo() {{
            setName("getOrder");
            setDescriptor("()V");
            setReturnType("void");
        }});
        im.invocations.add(new InvocationRef(
                "org.springframework.http.ResponseEntity",
                "ok",
                "()V", "void",
                List.of(),
                184,   // INVOKESTATIC
                null));

        List<DetectedTable> results = detect(im, cls);

        assertTrue(results.isEmpty(),
                "ResponseEntity.ok() must NOT be tagged as a DB operation; got: " + results);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // FP-EC7: StringBuilder.append("SELECT") → NOT a DB op
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("FP-EC7 — StringBuilder.append('SELECT...') → NOT a DB operation")
    void stringBuilderAppendSelectNotDbOp() {
        IndexedClass cls = serviceClass("com.example.QueryBuilder", "QueryBuilder");
        IndexedMethod im = methodWithOneCall(
                "buildQuery",
                "java.lang.StringBuilder",
                "append",
                null,
                "SELECT * FROM orders WHERE ");

        List<DetectedTable> results = detect(im, cls);

        assertTrue(results.isEmpty(),
                "StringBuilder.append() must NOT be tagged as a DB operation; got: " + results);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // FP-EC8: String.format("SELECT * FROM %s", tableName) → NOT a DB op (dynamic)
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("FP-EC8 — String.format() with SQL template → NOT a DB operation (dynamic table name)")
    void stringFormatSqlTemplateNotDbOp() {
        IndexedClass cls = serviceClass("com.example.DynamicQueryService", "DynamicQueryService");
        IndexedMethod im = methodWithOneCall(
                "buildDynamic",
                "java.lang.String",
                "format",
                null,
                "SELECT * FROM %s");  // template — table name not resolvable

        List<DetectedTable> results = detect(im, cls);

        assertTrue(results.isEmpty(),
                "String.format() with SQL template must NOT be tagged as a DB operation; got: " + results);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // FP-EC9: Arrays.asList("ORDERS", "ITEMS") → NOT extracted as table names
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("FP-EC9 — Arrays.asList() with string literals → NOT extracted as table names")
    void arraysAsListStringLiteralsNotTableNames() {
        IndexedClass cls = serviceClass("com.example.ConfigService", "ConfigService");
        IndexedMethod im = methodWithOneCall(
                "getTableNames",
                "java.util.Arrays",
                "asList",
                null,
                "ORDERS", "ITEMS", "CUSTOMERS");

        List<DetectedTable> results = detect(im, cls);

        assertTrue(results.isEmpty(),
                "Arrays.asList() with table-name-like strings must NOT produce DB detections; got: " + results);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // FP-EC10: Regression — SKIP_METHODS blocks false 'contains' detection
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("FP-EC10 – 'contains' method on any owner → blocked by SKIP_METHODS, no DB detection")
    void containsMethodInSkipMethodsNoDetection() {
        // 'contains' is in SKIP_METHODS — must never be tagged as DB op
        // regardless of owner class
        IndexedClass cls = serviceClass("com.example.OrderService", "OrderService");
        IndexedMethod im = methodWithOneCall(
                "checkExists",
                "java.util.HashSet",   // owner would normally be java.util.*
                "contains",
                null,
                "ORDERS");

        List<DetectedTable> results = detect(im, cls);

        assertTrue(results.isEmpty(),
                "'contains' in SKIP_METHODS must never produce a DB detection; got: " + results);
    }
}
