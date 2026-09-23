package com.jaranalyzer.service;

import com.jaranalyzer.model.ClassInfo;
import com.jaranalyzer.model.MethodInfo;
import com.jaranalyzer.service.CallGraphIndex.IndexedClass;
import com.jaranalyzer.service.CallGraphIndex.IndexedMethod;
import com.jaranalyzer.service.CallGraphIndex.InvocationRef;
import com.jaranalyzer.service.JpaMethodDetector.DetectedTable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Smoke tests for false-positive detection guards in JpaMethodDetector.
 *
 * All tests use unit-level stubs — no bytecode parsing, no Spring context.
 *
 * Coverage:
 *   FP1 — isRepositoryClass helper correctly classifies owner classes
 *   FP2 — SKIP_METHODS set contains universal no-op method names
 *   FP3 — builder.build() already excluded (NOISE_METHODS in CallTreeBuilder)
 *   FP4 — LOGGER_FIELD_NAMES set contains standard logger field name conventions
 *   FP5 — inferDerivedQueryOp: set* without "By" no longer tagged UPDATE (regression guard)
 *   R1  — real JDBC call (JdbcTemplate.query with SQL) is still detected as READ
 *   R2  — SKIP_METHODS guard does not block real EntityManager.find() (different method)
 */
@DisplayName("JpaMethodDetector — false-positive detection guards")
class FalsePositiveDetectionTest {

    // ───────────────────────────────────────────────────────────────────────────
    // Helpers: reflection-access to private statics
    // ───────────────────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private static Set<String> getPrivateStaticSet(String fieldName) throws Exception {
        Field f = JpaMethodDetector.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        return (Set<String>) f.get(null);
    }

    // ───────────────────────────────────────────────────────────────────────────
    // Helpers: build bare-minimum IndexedClass / IndexedMethod stubs
    // ───────────────────────────────────────────────────────────────────────────

    private static IndexedClass minimalClass(String fqn, String simpleName, String stereotype) {
        ClassInfo ci = new ClassInfo();
        ci.setFullyQualifiedName(fqn);
        ci.setSimpleName(simpleName);
        ci.setStereotype(stereotype);
        // Use a Pattern that never matches so no fieldConstants are collected
        return new IndexedClass(ci, Pattern.compile("(?!x)x"));
    }

    private static IndexedMethod minimalMethod(String name, List<InvocationRef> invocations) {
        MethodInfo mi = new MethodInfo();
        mi.setName(name);
        mi.setDescriptor("()V");
        mi.setReturnType("void");
        // IndexedMethod is in the same package; its `invocations` field is a package-private
        // final ArrayList. We populate it directly after construction (MethodInfo.getInvocations
        // returns an empty list, so im.invocations starts empty).
        IndexedMethod im = new IndexedMethod(mi);
        im.invocations.clear();
        im.invocations.addAll(invocations);
        return im;
    }

    /**
     * Build an InvocationRef that simulates an instance call to ownerClass.methodName
     * with an optional receiver field name (for logger guard).
     */
    private static InvocationRef invRef(String ownerClass, String methodName,
                                        List<String> stringArgs, String receiverFieldName) {
        return new InvocationRef(ownerClass, methodName, "()V", "void",
                stringArgs, 182 /* INVOKEVIRTUAL */, receiverFieldName);
    }

    // ───────────────────────────────────────────────────────────────────────────
    // FP1 — isRepositoryClass helper
    // ───────────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("FP1 — isRepositoryClass identifies repository-role classes")
    class RepositoryClassDetection {

        @Test
        @DisplayName("class ending in Repository is a repository class")
        void repositorySuffix() {
            assertTrue(JpaMethodDetector.isRepositoryClass("com.example.OrderRepository"));
            assertTrue(JpaMethodDetector.isRepositoryClass("OrderRepository"));
        }

        @Test
        @DisplayName("class ending in Dao / DAO is a repository class")
        void daoSuffix() {
            assertTrue(JpaMethodDetector.isRepositoryClass("com.example.UserDao"));
            assertTrue(JpaMethodDetector.isRepositoryClass("com.example.ClaimDAO"));
        }

        @Test
        @DisplayName("class ending in Mapper / Store / Persistence is a repository class")
        void otherRepoSuffixes() {
            assertTrue(JpaMethodDetector.isRepositoryClass("com.example.OrderMapper"));
            assertTrue(JpaMethodDetector.isRepositoryClass("com.example.OrderStore"));
            assertTrue(JpaMethodDetector.isRepositoryClass("com.example.ClaimPersistence"));
        }

        @Test
        @DisplayName("plain entity / service / DTO classes are NOT repository classes")
        void nonRepositoryClasses() {
            assertFalse(JpaMethodDetector.isRepositoryClass("com.example.OrderEntity"));
            assertFalse(JpaMethodDetector.isRepositoryClass("com.example.OrderService"));
            assertFalse(JpaMethodDetector.isRepositoryClass("com.example.OrderDto"));
            assertFalse(JpaMethodDetector.isRepositoryClass("com.example.OrderController"));
            assertFalse(JpaMethodDetector.isRepositoryClass(null));
        }
    }

    // ───────────────────────────────────────────────────────────────────────────
    // FP2 — SKIP_METHODS set content
    // ───────────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("FP2 — SKIP_METHODS contains universal no-op method names")
    class SkipMethodsSet {

        @Test
        @DisplayName("Object utility methods are in SKIP_METHODS")
        void objectUtilityMethods() throws Exception {
            Set<String> skipMethods = getPrivateStaticSet("SKIP_METHODS");
            assertTrue(skipMethods.contains("toString"), "toString must be skipped");
            assertTrue(skipMethods.contains("hashCode"), "hashCode must be skipped");
            assertTrue(skipMethods.contains("equals"),   "equals must be skipped");
            assertTrue(skipMethods.contains("compareTo"), "compareTo must be skipped");
            assertTrue(skipMethods.contains("clone"),    "clone must be skipped");
            assertTrue(skipMethods.contains("getClass"), "getClass must be skipped");
            assertTrue(skipMethods.contains("wait"),     "wait must be skipped");
            assertTrue(skipMethods.contains("notify"),   "notify must be skipped");
            assertTrue(skipMethods.contains("notifyAll"),"notifyAll must be skipped");
        }

        @Test
        @DisplayName("Collection utility methods are in SKIP_METHODS")
        void collectionUtilityMethods() throws Exception {
            Set<String> skipMethods = getPrivateStaticSet("SKIP_METHODS");
            assertTrue(skipMethods.contains("size"),     "size must be skipped");
            assertTrue(skipMethods.contains("isEmpty"),  "isEmpty must be skipped");
            assertTrue(skipMethods.contains("contains"), "contains must be skipped");
            assertTrue(skipMethods.contains("iterator"), "iterator must be skipped");
            assertTrue(skipMethods.contains("stream"),   "stream must be skipped");
        }
    }

    // ───────────────────────────────────────────────────────────────────────────
    // FP4 — LOGGER_FIELD_NAMES set content
    // ───────────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("FP4 — LOGGER_FIELD_NAMES covers all standard logger field conventions")
    class LoggerFieldNames {

        @Test
        @DisplayName("all standard logger field name variants are present")
        void allLoggerVariantsPresent() throws Exception {
            Set<String> loggerNames = getPrivateStaticSet("LOGGER_FIELD_NAMES");
            assertTrue(loggerNames.contains("log"),    "lowercase 'log' must be excluded");
            assertTrue(loggerNames.contains("logger"), "lowercase 'logger' must be excluded");
            assertTrue(loggerNames.contains("LOG"),    "uppercase 'LOG' must be excluded");
            assertTrue(loggerNames.contains("LOGGER"), "uppercase 'LOGGER' must be excluded");
        }
    }

    // ───────────────────────────────────────────────────────────────────────────
    // FP5 — inferDerivedQueryOp: set* without "By" must NOT return UPDATE
    // ───────────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("FP5 — inferDerivedQueryOp does not tag set* methods without By as UPDATE")
    class DerivedQueryOpGuard {

        /**
         * Call the package-private inferDerivedQueryOp via reflection.
         * It is an instance method but uses no instance state.
         */
        private String callInferDerivedQueryOp(String methodName) throws Exception {
            var method = JpaMethodDetector.class
                    .getDeclaredMethod("inferDerivedQueryOp", String.class);
            method.setAccessible(true);
            // Need a JpaMethodDetector instance — it has a package-private constructor
            DomainConfigLoader loader = new DomainConfigLoader();
            JpaMethodDetector detector = new JpaMethodDetector(loader);
            return (String) method.invoke(detector, methodName);
        }

        @Test
        @DisplayName("setFetchSize() — no By — must return null (not UPDATE)")
        void setFetchSizeNoBy() throws Exception {
            assertNull(callInferDerivedQueryOp("setFetchSize"),
                    "setFetchSize() without 'By' must not be tagged as UPDATE");
        }

        @Test
        @DisplayName("setMaxResults() — no By — must return null (not UPDATE)")
        void setMaxResultsNoBy() throws Exception {
            assertNull(callInferDerivedQueryOp("setMaxResults"),
                    "setMaxResults() without 'By' must not be tagged as UPDATE");
        }

        @Test
        @DisplayName("setPageSize() — no By — must return null (not UPDATE)")
        void setPageSizeNoBy() throws Exception {
            assertNull(callInferDerivedQueryOp("setPageSize"),
                    "setPageSize() without 'By' must not be tagged as UPDATE");
        }

        @Test
        @DisplayName("updateByStatus — has By — must return UPDATE (real derived query)")
        void updateByStatus() throws Exception {
            assertEquals("UPDATE", callInferDerivedQueryOp("updateByStatus"),
                    "updateByStatus is a valid Spring Data @Modifying derived query");
        }

        @Test
        @DisplayName("findByOrderId — has By — must return READ (regression guard)")
        void findByOrderId() throws Exception {
            assertEquals("READ", callInferDerivedQueryOp("findByOrderId"),
                    "findByOrderId is a standard Spring Data derived read query");
        }

        @Test
        @DisplayName("deleteByExpiry — has By — must return DELETE (regression guard)")
        void deleteByExpiry() throws Exception {
            assertEquals("DELETE", callInferDerivedQueryOp("deleteByExpiry"),
                    "deleteByExpiry is a standard Spring Data derived delete query");
        }

        @Test
        @DisplayName("saveAll — no By — must return WRITE (regression: save* is safe)")
        void saveAll() throws Exception {
            assertEquals("WRITE", callInferDerivedQueryOp("saveAll"),
                    "saveAll must still be tagged as WRITE");
        }

        @Test
        @DisplayName("findAll — no By — must return READ (regression: find* is safe)")
        void findAll() throws Exception {
            assertEquals("READ", callInferDerivedQueryOp("findAll"),
                    "findAll must still be tagged as READ");
        }
    }

    // ───────────────────────────────────────────────────────────────────────────
    // R1 — Regression: real JDBC call still detected
    // ───────────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("R1 — real JdbcTemplate.query() with SQL is still detected as READ")
    class RealJdbcDetection {

        @Test
        @DisplayName("JdbcTemplate.query with SELECT SQL produces a READ detection")
        void jdbcTemplateQueryProducesRead() {
            DomainConfigLoader loader = new DomainConfigLoader();
            JpaMethodDetector detector = new JpaMethodDetector(loader);

            IndexedClass cls = minimalClass("com.example.OrderService", "OrderService", "SERVICE");

            // Simulate: jdbcTemplate.query("SELECT * FROM ORDERS WHERE id = ?", ...)
            InvocationRef jdbcCall = invRef(
                    "org.springframework.jdbc.core.JdbcTemplate",
                    "query",
                    List.of("SELECT * FROM ORDERS WHERE id = ?"),
                    "jdbcTemplate"
            );

            IndexedMethod method = minimalMethod("getOrder", List.of(jdbcCall));

            List<DetectedTable> results = detector.detect(method, cls, Map.of(), Map.of()).resolved();

            assertFalse(results.isEmpty(), "JdbcTemplate.query() with SQL must produce at least one detection");
            assertTrue(results.stream().anyMatch(dt -> "READ".equals(dt.operationType())),
                    "Detected operation type must be READ for a SELECT statement");
            assertTrue(results.stream().anyMatch(dt -> "ORDERS".equalsIgnoreCase(dt.tableName())),
                    "Table name ORDERS must be extracted from the SQL");
        }
    }

    // ───────────────────────────────────────────────────────────────────────────
    // R2 — Regression: SKIP_METHODS does NOT block real EntityManager.find()
    // ───────────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("R2 — SKIP_METHODS does not block EntityManager.find() (different method name)")
    class SkipMethodsDoesNotBlockFind {

        @Test
        @DisplayName("'find' is NOT in SKIP_METHODS — EntityManager.find() remains detectable")
        void findNotInSkipMethods() throws Exception {
            Set<String> skipMethods = getPrivateStaticSet("SKIP_METHODS");
            assertFalse(skipMethods.contains("find"),
                    "'find' must NOT be in SKIP_METHODS — EntityManager.find() is a real DB op");
        }

        @Test
        @DisplayName("'save' is NOT in SKIP_METHODS — repository.save() remains detectable")
        void saveNotInSkipMethods() throws Exception {
            Set<String> skipMethods = getPrivateStaticSet("SKIP_METHODS");
            assertFalse(skipMethods.contains("save"),
                    "'save' must NOT be in SKIP_METHODS — repository.save() is a real DB op");
        }

        @Test
        @DisplayName("'execute' is NOT in SKIP_METHODS — CallableStatement.execute() remains detectable")
        void executeNotInSkipMethods() throws Exception {
            Set<String> skipMethods = getPrivateStaticSet("SKIP_METHODS");
            assertFalse(skipMethods.contains("execute"),
                    "'execute' must NOT be in SKIP_METHODS — CallableStatement.execute() is a real DB op");
        }
    }

    // ───────────────────────────────────────────────────────────────────────────
    // FP-logger: logger call with known field name is skipped
    // ───────────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("FP4-behavior — logger.info() is not tagged as a DB operation")
    class LoggerCallBehavior {

        @Test
        @DisplayName("invocation on field named 'log' produces no detection even with DB-like method name")
        void logFieldProducesNoDetection() {
            DomainConfigLoader loader = new DomainConfigLoader();
            JpaMethodDetector detector = new JpaMethodDetector(loader);

            IndexedClass cls = minimalClass("com.example.OrderService", "OrderService", "SERVICE");

            // Simulate: log.info("Order saved") — field name is "log", method is "info"
            // Even if "info" somehow matched an owner list, the receiver guard stops it.
            InvocationRef logCall = invRef(
                    "org.slf4j.Logger", // FQN — blocked by SLF4J owner check too, belt-and-suspenders
                    "info",
                    List.of("Order saved"),
                    "log"  // ← receiverFieldName
            );

            IndexedMethod method = minimalMethod("placeOrder", List.of(logCall));

            List<DetectedTable> results = detector.detect(method, cls, Map.of(), Map.of()).resolved();

            assertTrue(results.isEmpty(),
                    "Logger call (receiverField='log') must produce no DB detections");
        }

        @Test
        @DisplayName("invocation on field named 'LOGGER' produces no detection")
        void LOGGERFieldProducesNoDetection() {
            DomainConfigLoader loader = new DomainConfigLoader();
            JpaMethodDetector detector = new JpaMethodDetector(loader);

            IndexedClass cls = minimalClass("com.example.OrderService", "OrderService", "SERVICE");

            InvocationRef logCall = invRef(
                    "org.slf4j.Logger",
                    "error",
                    List.of("Unexpected exception"),
                    "LOGGER"  // ← receiverFieldName
            );

            IndexedMethod method = minimalMethod("processPayment", List.of(logCall));

            List<DetectedTable> results = detector.detect(method, cls, Map.of(), Map.of()).resolved();

            assertTrue(results.isEmpty(),
                    "Logger call (receiverField='LOGGER') must produce no DB detections");
        }
    }
}
