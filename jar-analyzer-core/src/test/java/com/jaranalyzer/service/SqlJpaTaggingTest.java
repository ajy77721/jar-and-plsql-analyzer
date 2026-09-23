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

import static org.junit.jupiter.api.Assertions.*;

/**
 * Category B — SQL/JPA operation detection tests.
 *
 * All tests use the same unit-level stub approach as FalsePositiveDetectionTest:
 *   • No Spring context, no bytecode parsing, no DB connection.
 *   • IndexedClass / IndexedMethod / InvocationRef built programmatically.
 *   • Assertions are on JpaMethodDetector.detect() return values.
 *
 * Patterns covered:
 *   B1–B9   : Spring Data JPA built-in + derived repository methods (REPO_METHOD / DERIVED_QUERY)
 *   B10     : @Modifying derived query
 *   B11–B13 : @Query annotation (JPQL + native)
 *   B14–B19 : EntityManager direct API
 *   B20–B24 : JDBC (JdbcTemplate + Connection.prepareStatement)
 *   B25–B27 : False-positive prevention
 *   B28–B30 : Non-DB calls that must NOT produce detections
 */
@DisplayName("Category B — SQL/JPA operation tagging")
class SqlJpaTaggingTest {

    // ─── shared infrastructure ────────────────────────────────────────────────────

    private JpaMethodDetector detector;

    @BeforeEach
    void setUp() {
        DomainConfigLoader loader = new DomainConfigLoader();
        detector = new JpaMethodDetector(loader);
    }

    // ─── helpers ─────────────────────────────────────────────────────────────────

    /**
     * Build a bare-minimum IndexedClass.
     *
     * @param fqn        fully-qualified class name
     * @param simpleName simple class name
     * @param stereotype e.g. "REPOSITORY", "SERVICE"
     * @param entityType for repository classes: entity type (used by D/E detection); null otherwise
     * @param tableName  when not null, added to entityTableMap under simpleName key
     */
    private static IndexedClass makeClass(String fqn, String simpleName, String stereotype,
                                          String entityType, String tableName) {
        ClassInfo ci = new ClassInfo();
        ci.setFullyQualifiedName(fqn);
        ci.setSimpleName(simpleName);
        ci.setStereotype(stereotype);
        ci.setRepositoryEntityType(entityType);
        return new IndexedClass(ci, java.util.regex.Pattern.compile("(?!x)x"));
    }

    /**
     * Build an IndexedMethod with given name and list of invocations.
     * descriptor defaults to ()V (no parameters, void return).
     */
    private static IndexedMethod makeMethod(String name, List<InvocationRef> invocations) {
        MethodInfo mi = new MethodInfo();
        mi.setName(name);
        mi.setDescriptor("()V");
        mi.setReturnType("void");
        IndexedMethod im = new IndexedMethod(mi);
        im.invocations.clear();
        im.invocations.addAll(invocations);
        return im;
    }

    /**
     * Build an IndexedMethod that carries a @Query annotation.
     *
     * @param methodName   method name
     * @param queryValue   JPQL or SQL string from @Query value attribute
     * @param isNative     true → nativeQuery=true
     * @param isModifying  true → adds @Modifying (only affects caller interpretation)
     */
    private static IndexedMethod makeAnnotatedQueryMethod(String methodName, String queryValue,
                                                          boolean isNative, boolean isModifying) {
        MethodInfo mi = new MethodInfo();
        mi.setName(methodName);
        mi.setDescriptor("()V");
        mi.setReturnType("void");

        com.jaranalyzer.model.AnnotationInfo queryAnn = new com.jaranalyzer.model.AnnotationInfo();
        queryAnn.setName("Query");
        java.util.Map<String, Object> attrs = new java.util.LinkedHashMap<>();
        attrs.put("value", queryValue);
        if (isNative) attrs.put("nativeQuery", Boolean.TRUE);
        queryAnn.setAttributes(attrs);
        mi.setAnnotations(isModifying
                ? List.of(queryAnn, modifyingAnnotation())
                : List.of(queryAnn));

        IndexedMethod im = new IndexedMethod(mi);
        im.invocations.clear();
        return im;
    }

    private static com.jaranalyzer.model.AnnotationInfo modifyingAnnotation() {
        com.jaranalyzer.model.AnnotationInfo mod = new com.jaranalyzer.model.AnnotationInfo();
        mod.setName("Modifying");
        mod.setAttributes(Map.of());
        return mod;
    }

    /**
     * Build an InvocationRef for a non-lambda instance call.
     */
    private static InvocationRef inv(String ownerClass, String methodName,
                                     List<String> stringArgs, String receiverField) {
        return new InvocationRef(ownerClass, methodName, "()V", "void",
                stringArgs, 182 /* INVOKEVIRTUAL */, receiverField);
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // B1–B9: Spring Data JPA built-in repository methods (Section E) and
    //         derived queries (Section D)
    // ─────────────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("B1–B9 — Spring Data JPA built-in and derived query methods")
    class SpringDataRepositoryMethods {

        // For E-section (built-in) and D-section (derived) to fire, the class must have
        // stereotype=REPOSITORY and entityTableMap must contain the entity mapping.
        private static final Map<String, String> ENTITY_TABLE = Map.of(
                "Order", "ORDERS",
                "com.example.Order", "ORDERS"
        );

        private static IndexedClass repoClass(String entityType) {
            IndexedClass ic = makeClass(
                    "com.example.OrderRepository", "OrderRepository",
                    "REPOSITORY", entityType, "ORDERS");
            // repositoryEntityType is already set via ClassInfo, but IndexedClass copies it
            // from ci.getRepositoryEntityType() — we need a class whose repositoryEntityType
            // matches an entry in ENTITY_TABLE so resolveRepoEntityTable returns a table.
            return ic;
        }

        @Test
        @DisplayName("B1 — repository.findById → opType=READ (source=DERIVED_QUERY or JPA_REPOSITORY)")
        void findById() {
            IndexedClass ic = repoClass("Order");
            IndexedMethod im = makeMethod("findById", List.of());
            List<DetectedTable> results = detector.detect(im, ic, ENTITY_TABLE, Map.of()).resolved();
            assertFalse(results.isEmpty(), "findById must produce a detection");
            assertTrue(results.stream().anyMatch(dt -> "READ".equals(dt.operationType())),
                    "findById must be READ");
            // Section D (DERIVED_QUERY) runs before E (JPA_REPOSITORY); dedup keeps first hit.
            // Both are legitimate sources — accept either.
            assertTrue(results.stream().anyMatch(dt ->
                            "JPA_REPOSITORY".equals(dt.source()) || "DERIVED_QUERY".equals(dt.source())),
                    "findById source must be JPA_REPOSITORY or DERIVED_QUERY");
        }

        @Test
        @DisplayName("B2 — repository.save → opType=WRITE")
        void save() {
            IndexedClass ic = repoClass("Order");
            IndexedMethod im = makeMethod("save", List.of());
            List<DetectedTable> results = detector.detect(im, ic, ENTITY_TABLE, Map.of()).resolved();
            assertFalse(results.isEmpty(), "save must produce a detection");
            assertTrue(results.stream().anyMatch(dt -> "WRITE".equals(dt.operationType())),
                    "save must be WRITE");
        }

        @Test
        @DisplayName("B3 — repository.deleteById → opType=DELETE")
        void deleteById() {
            IndexedClass ic = repoClass("Order");
            IndexedMethod im = makeMethod("deleteById", List.of());
            List<DetectedTable> results = detector.detect(im, ic, ENTITY_TABLE, Map.of()).resolved();
            assertFalse(results.isEmpty(), "deleteById must produce a detection");
            assertTrue(results.stream().anyMatch(dt -> "DELETE".equals(dt.operationType())),
                    "deleteById must be DELETE");
        }

        @Test
        @DisplayName("B4 — repository.findAll → opType=READ")
        void findAll() {
            IndexedClass ic = repoClass("Order");
            IndexedMethod im = makeMethod("findAll", List.of());
            List<DetectedTable> results = detector.detect(im, ic, ENTITY_TABLE, Map.of()).resolved();
            assertFalse(results.isEmpty(), "findAll must produce a detection");
            assertTrue(results.stream().anyMatch(dt -> "READ".equals(dt.operationType())),
                    "findAll must be READ");
        }

        @Test
        @DisplayName("B5 — repository.count → opType=COUNT (or READ for JPA_REPOSITORY built-in)")
        void count() {
            IndexedClass ic = repoClass("Order");
            IndexedMethod im = makeMethod("count", List.of());
            List<DetectedTable> results = detector.detect(im, ic, ENTITY_TABLE, Map.of()).resolved();
            assertFalse(results.isEmpty(), "count must produce a detection");
            // REPO_METHOD_OPS maps "count" → "COUNT"
            assertTrue(results.stream().anyMatch(dt ->
                            "COUNT".equals(dt.operationType()) || "READ".equals(dt.operationType())),
                    "count must map to COUNT or READ");
        }

        @Test
        @DisplayName("B6 — derived findByNameAndStatus → opType=READ")
        void findByNameAndStatus() {
            IndexedClass ic = repoClass("Order");
            IndexedMethod im = makeMethod("findByNameAndStatus", List.of());
            List<DetectedTable> results = detector.detect(im, ic, ENTITY_TABLE, Map.of()).resolved();
            assertFalse(results.isEmpty(), "findByNameAndStatus must produce a detection");
            assertTrue(results.stream().anyMatch(dt -> "READ".equals(dt.operationType())),
                    "findByNameAndStatus must be READ");
        }

        @Test
        @DisplayName("B7 — derived deleteByCreatedAtBefore → opType=DELETE")
        void deleteByCreatedAtBefore() {
            IndexedClass ic = repoClass("Order");
            IndexedMethod im = makeMethod("deleteByCreatedAtBefore", List.of());
            List<DetectedTable> results = detector.detect(im, ic, ENTITY_TABLE, Map.of()).resolved();
            assertFalse(results.isEmpty(), "deleteByCreatedAtBefore must produce a detection");
            assertTrue(results.stream().anyMatch(dt -> "DELETE".equals(dt.operationType())),
                    "deleteByCreatedAtBefore must be DELETE");
        }

        @Test
        @DisplayName("B8 — derived countByStatus → opType=READ or COUNT")
        void countByStatus() {
            IndexedClass ic = repoClass("Order");
            IndexedMethod im = makeMethod("countByStatus", List.of());
            List<DetectedTable> results = detector.detect(im, ic, ENTITY_TABLE, Map.of()).resolved();
            assertFalse(results.isEmpty(), "countByStatus must produce a detection");
            // inferDerivedQueryOp: starts with 'count' + contains 'by' → "COUNT" or falls to "READ" via general prefix
            assertTrue(results.stream().anyMatch(dt ->
                            "COUNT".equals(dt.operationType()) || "READ".equals(dt.operationType())),
                    "countByStatus must map to COUNT or READ");
        }

        @Test
        @DisplayName("B9 — existsById → opType=READ")
        void existsById() {
            IndexedClass ic = repoClass("Order");
            IndexedMethod im = makeMethod("existsById", List.of());
            List<DetectedTable> results = detector.detect(im, ic, ENTITY_TABLE, Map.of()).resolved();
            assertFalse(results.isEmpty(), "existsById must produce a detection");
            assertTrue(results.stream().anyMatch(dt -> "READ".equals(dt.operationType())),
                    "existsById must be READ");
        }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // B10: @Modifying derived query
    // ─────────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("B10 — derived updateStatusById with @Modifying → opType=UPDATE")
    void updateStatusByIdWithModifying() {
        Map<String, String> entityTable = Map.of("Order", "ORDERS");
        IndexedClass ic = makeClass("com.example.OrderRepository", "OrderRepository",
                "REPOSITORY", "Order", "ORDERS");
        // updateStatusById → contains 'by' + starts with 'update' → UPDATE
        IndexedMethod im = makeMethod("updateStatusById", List.of());
        List<DetectedTable> results = detector.detect(im, ic, entityTable, Map.of()).resolved();
        assertFalse(results.isEmpty(), "updateStatusById must produce a detection");
        assertTrue(results.stream().anyMatch(dt -> "UPDATE".equals(dt.operationType())),
                "updateStatusById must be UPDATE");
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // B11–B13: @Query annotation
    // ─────────────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("B11–B13 — @Query annotation (JPQL + native SQL)")
    class QueryAnnotation {

        @Test
        @DisplayName("B11 — @Query JPQL SELECT → opType=READ, source=JPQL_QUERY")
        void jpqlSelectQuery() {
            IndexedClass ic = makeClass("com.example.OrderRepository", "OrderRepository",
                    "REPOSITORY", null, null);
            // JPQL entity resolution: entity 'Order' maps to 'ORDERS'
            Map<String, String> entityTable = Map.of("Order", "ORDERS", "User", "USERS");
            IndexedMethod im = makeAnnotatedQueryMethod(
                    "findActiveOrders",
                    "SELECT u FROM User u WHERE u.id=?1",
                    false, false);
            List<DetectedTable> results = detector.detect(im, ic, entityTable, Map.of()).resolved();
            assertFalse(results.isEmpty(), "@Query JPQL SELECT must produce a detection");
            assertTrue(results.stream().anyMatch(dt -> "READ".equals(dt.operationType())),
                    "@Query JPQL SELECT must be READ");
        }

        @Test
        @DisplayName("B12 — @Query UPDATE JPQL with @Modifying → opType=UPDATE, source=JPQL_QUERY")
        void jpqlUpdateWithModifying() {
            IndexedClass ic = makeClass("com.example.UserRepository", "UserRepository",
                    "REPOSITORY", null, null);
            Map<String, String> entityTable = Map.of("User", "USERS");
            IndexedMethod im = makeAnnotatedQueryMethod(
                    "updateStatus",
                    "UPDATE User u SET u.status=?1",
                    false, true);
            List<DetectedTable> results = detector.detect(im, ic, entityTable, Map.of()).resolved();
            assertFalse(results.isEmpty(), "@Query JPQL UPDATE must produce a detection");
            assertTrue(results.stream().anyMatch(dt -> "UPDATE".equals(dt.operationType())),
                    "@Query JPQL UPDATE must be UPDATE");
        }

        @Test
        @DisplayName("B13 — @Query(nativeQuery=true) SELECT * FROM users → opType=READ, table=users")
        void nativeQuerySelect() {
            IndexedClass ic = makeClass("com.example.UserRepository", "UserRepository",
                    "REPOSITORY", null, null);
            IndexedMethod im = makeAnnotatedQueryMethod(
                    "findUserById",
                    "SELECT * FROM users WHERE id=?1",
                    true, false);
            List<DetectedTable> results = detector.detect(im, ic, Map.of(), Map.of()).resolved();
            assertFalse(results.isEmpty(), "Native @Query SELECT must produce a detection");
            assertTrue(results.stream().anyMatch(dt -> "READ".equals(dt.operationType())),
                    "Native @Query SELECT must be READ");
            assertTrue(results.stream().anyMatch(dt ->
                            dt.tableName() != null && dt.tableName().equalsIgnoreCase("users")),
                    "Native @Query must extract table name 'users'");
        }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // B14–B19: EntityManager direct API
    // ─────────────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("B14–B19 — EntityManager direct API calls")
    class EntityManagerApi {

        private static IndexedClass serviceClass() {
            return makeClass("com.example.OrderService", "OrderService",
                    "SERVICE", null, null);
        }

        @Test
        @DisplayName("B14 — em.persist(entity) → opType=WRITE")
        void emPersist() {
            IndexedClass ic = serviceClass();
            InvocationRef call = inv("javax.persistence.EntityManager", "persist",
                    List.of("__class:com.example.Order"), "em");
            IndexedMethod im = makeMethod("createOrder", List.of(call));
            List<DetectedTable> results = detector.detect(im, ic, Map.of(), Map.of()).resolved();
            // Without entityTableMap, no table is resolved, but the invocation is processed.
            // With entityTableMap, table would be resolved. Test both cases below.
            // Here: empty entityTableMap → EM_ENTITY_METHODS fires but no table resolved → no result added.
            // This verifies the invocation IS processed (no false-positive blocker).
            // For the opType assertion we add the entity to the map.
            Map<String, String> entityTable = Map.of("com.example.Order", "ORDERS", "Order", "ORDERS");
            results = detector.detect(im, ic, entityTable, Map.of()).resolved();
            assertFalse(results.isEmpty(), "em.persist must produce a detection when entity is known");
            assertTrue(results.stream().anyMatch(dt -> "WRITE".equals(dt.operationType())),
                    "em.persist must be WRITE");
        }

        @Test
        @DisplayName("B15 — em.merge(entity) → opType=UPDATE")
        void emMerge() {
            IndexedClass ic = serviceClass();
            InvocationRef call = inv("javax.persistence.EntityManager", "merge",
                    List.of("__class:com.example.Order"), "em");
            IndexedMethod im = makeMethod("updateOrder", List.of(call));
            Map<String, String> entityTable = Map.of("com.example.Order", "ORDERS", "Order", "ORDERS");
            List<DetectedTable> results = detector.detect(im, ic, entityTable, Map.of()).resolved();
            assertFalse(results.isEmpty(), "em.merge must produce a detection");
            assertTrue(results.stream().anyMatch(dt -> "UPDATE".equals(dt.operationType())),
                    "em.merge must be UPDATE");
        }

        @Test
        @DisplayName("B16 — em.remove(entity) → opType=DELETE")
        void emRemove() {
            IndexedClass ic = serviceClass();
            InvocationRef call = inv("javax.persistence.EntityManager", "remove",
                    List.of("__class:com.example.Order"), "em");
            IndexedMethod im = makeMethod("deleteOrder", List.of(call));
            Map<String, String> entityTable = Map.of("com.example.Order", "ORDERS", "Order", "ORDERS");
            List<DetectedTable> results = detector.detect(im, ic, entityTable, Map.of()).resolved();
            assertFalse(results.isEmpty(), "em.remove must produce a detection");
            assertTrue(results.stream().anyMatch(dt -> "DELETE".equals(dt.operationType())),
                    "em.remove must be DELETE");
        }

        @Test
        @DisplayName("B17 — em.find(User.class, id) → opType=READ")
        void emFind() {
            IndexedClass ic = serviceClass();
            InvocationRef call = inv("javax.persistence.EntityManager", "find",
                    List.of("__class:com.example.User"), "em");
            IndexedMethod im = makeMethod("findUser", List.of(call));
            Map<String, String> entityTable = Map.of("com.example.User", "USERS", "User", "USERS");
            List<DetectedTable> results = detector.detect(im, ic, entityTable, Map.of()).resolved();
            assertFalse(results.isEmpty(), "em.find must produce a detection");
            assertTrue(results.stream().anyMatch(dt -> "READ".equals(dt.operationType())),
                    "em.find must be READ");
        }

        @Test
        @DisplayName("B18 — em.createQuery JPQL 'SELECT i FROM Invoice i WHERE i.id=?1' → opType=READ")
        void emCreateQuery() {
            // Uses entity name "Invoice" (not a SQL keyword) as a baseline JPQL detection test.
            // Keyword-collision cases (Order, Set, Index, etc.) are covered in B18b and B18c.
            IndexedClass ic = serviceClass();
            InvocationRef call = inv("javax.persistence.EntityManager", "createQuery",
                    List.of("SELECT i FROM Invoice i WHERE i.id=?1"), "em");
            IndexedMethod im = makeMethod("findInvoice", List.of(call));
            Map<String, String> entityTable = Map.of("Invoice", "INVOICES");
            List<DetectedTable> results = detector.detect(im, ic, entityTable, Map.of()).resolved();
            assertFalse(results.isEmpty(), "em.createQuery JPQL must produce a detection");
            assertTrue(results.stream().anyMatch(dt -> "READ".equals(dt.operationType())),
                    "em.createQuery JPQL SELECT must be READ");
        }

        @Test
        @DisplayName("B18b — em.createQuery JPQL with entity name 'Order' (SQL keyword collision) → fixed via JPQL-aware extraction")
        void emCreateQueryOrderEntityKeywordGap() {
            // GAP-JPQL-KEYWORD: Previously, 'Order' (normalised to 'order') collided with SQL keywords
            // and was silently filtered. Fixed by extractJpqlEntities() using extractTableNamesForJpql()
            // which bypasses keyword filtering for tokens present in entityTableMap.
            IndexedClass ic = serviceClass();
            InvocationRef call = inv("javax.persistence.EntityManager", "createQuery",
                    List.of("SELECT o FROM Order o WHERE o.id=?1"), "em");
            IndexedMethod im = makeMethod("findOrder", List.of(call));
            Map<String, String> entityTable = Map.of("Order", "ORDERS");
            List<DetectedTable> results = detector.detect(im, ic, entityTable, Map.of()).resolved();
            assertFalse(results.isEmpty(),
                    "JPQL with entity 'Order' must detect ORDERS via JPQL-aware keyword bypass");
            assertTrue(results.stream().anyMatch(dt ->
                            dt.tableName() != null && dt.tableName().equalsIgnoreCase("ORDERS")),
                    "Table ORDERS must be resolved from entity 'Order'");
        }

        @Test
        @DisplayName("B18c — em.createQuery JPQL with entity name 'Set' (SQL keyword collision) → fixed via JPQL-aware extraction")
        void emCreateQuerySetEntityKeywordGap() {
            // Additional GAP-JPQL-KEYWORD case: 'set' IS in SQL_KEYWORDS, so without the bypass
            // it would be filtered. extractTableNamesForJpql() should allow it through.
            IndexedClass ic = serviceClass();
            InvocationRef call = inv("javax.persistence.EntityManager", "createQuery",
                    List.of("SELECT s FROM Set s WHERE s.id=?1"), "em");
            IndexedMethod im = makeMethod("findSet", List.of(call));
            Map<String, String> entityTable = Map.of("Set", "SETS");
            List<DetectedTable> results = detector.detect(im, ic, entityTable, Map.of()).resolved();
            assertFalse(results.isEmpty(),
                    "JPQL with entity 'Set' must detect SETS via JPQL-aware keyword bypass");
            assertTrue(results.stream().anyMatch(dt ->
                            dt.tableName() != null && dt.tableName().equalsIgnoreCase("SETS")),
                    "Table SETS must be resolved from entity 'Set'");
        }

        @Test
        @DisplayName("B18d — GAP-JPQL-FROM: em.createQuery JPQL shorthand 'FROM Order o WHERE o.id=?1' → opType=READ, table=ORDERS")
        void emCreateQueryJpqlFromShorthand() {
            // GAP-JPQL-FROM: JPQL allows 'FROM Entity e WHERE ...' without a leading SELECT.
            // Previously looksLikeSql() did not recognise strings starting with FROM, so they
            // were not processed. Fixed by adding "FROM " to looksLikeSql() and
            // inferOperationType() returns "READ" for FROM-leading strings.
            IndexedClass ic = serviceClass();
            InvocationRef call = inv("javax.persistence.EntityManager", "createQuery",
                    List.of("FROM Order o WHERE o.id=?1"), "em");
            IndexedMethod im = makeMethod("findOrder", List.of(call));
            Map<String, String> entityTable = Map.of("Order", "ORDERS");
            List<DetectedTable> results = detector.detect(im, ic, entityTable, Map.of()).resolved();
            assertFalse(results.isEmpty(),
                    "JPQL shorthand 'FROM Order o WHERE o.id=?1' must produce a detection");
            assertTrue(results.stream().anyMatch(dt -> "READ".equals(dt.operationType())),
                    "JPQL FROM shorthand must be treated as READ");
            assertTrue(results.stream().anyMatch(dt ->
                            dt.tableName() != null && dt.tableName().equalsIgnoreCase("ORDERS")),
                    "Table ORDERS must be resolved from JPQL FROM shorthand");
        }

        @Test
        @DisplayName("B19 — em.createNativeQuery('SELECT * FROM ORDERS WHERE id=?') → table=ORDERS, opType=READ")
        void emCreateNativeQuery() {
            IndexedClass ic = serviceClass();
            InvocationRef call = inv("javax.persistence.EntityManager", "createNativeQuery",
                    List.of("SELECT * FROM ORDERS WHERE id=?"), "em");
            IndexedMethod im = makeMethod("findOrderNative", List.of(call));
            List<DetectedTable> results = detector.detect(im, ic, Map.of(), Map.of()).resolved();
            assertFalse(results.isEmpty(), "em.createNativeQuery must produce a detection");
            assertTrue(results.stream().anyMatch(dt -> "READ".equals(dt.operationType())),
                    "Native SELECT must be READ");
            assertTrue(results.stream().anyMatch(dt ->
                            dt.tableName() != null && dt.tableName().equalsIgnoreCase("ORDERS")),
                    "Table name ORDERS must be extracted from native SQL");
        }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // B20–B24: JDBC direct access
    // ─────────────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("B20–B24 — JDBC direct access (JdbcTemplate + PreparedStatement)")
    class JdbcDirectAccess {

        private static IndexedClass serviceClass() {
            return makeClass("com.example.OrderService", "OrderService",
                    "SERVICE", null, null);
        }

        @Test
        @DisplayName("B20 — jdbcTemplate.query('SELECT * FROM ORDERS WHERE id=?') → table=ORDERS, opType=READ")
        void jdbcTemplateQuery() {
            IndexedClass ic = serviceClass();
            InvocationRef call = inv(
                    "org.springframework.jdbc.core.JdbcTemplate", "query",
                    List.of("SELECT * FROM ORDERS WHERE id=?"), "jdbcTemplate");
            IndexedMethod im = makeMethod("getOrder", List.of(call));
            List<DetectedTable> results = detector.detect(im, ic, Map.of(), Map.of()).resolved();
            assertFalse(results.isEmpty(), "jdbcTemplate.query SELECT must produce a detection");
            assertTrue(results.stream().anyMatch(dt -> "READ".equals(dt.operationType())),
                    "jdbcTemplate SELECT must be READ");
            assertTrue(results.stream().anyMatch(dt ->
                            dt.tableName() != null && dt.tableName().equalsIgnoreCase("ORDERS")),
                    "Table name ORDERS must be extracted");
        }

        @Test
        @DisplayName("B21 — jdbcTemplate.update('UPDATE ORDERS SET status=?') → table=ORDERS, opType=UPDATE")
        void jdbcTemplateUpdate() {
            IndexedClass ic = serviceClass();
            InvocationRef call = inv(
                    "org.springframework.jdbc.core.JdbcTemplate", "update",
                    List.of("UPDATE ORDERS SET status=? WHERE id=?"), "jdbcTemplate");
            IndexedMethod im = makeMethod("updateOrder", List.of(call));
            List<DetectedTable> results = detector.detect(im, ic, Map.of(), Map.of()).resolved();
            assertFalse(results.isEmpty(), "jdbcTemplate.update UPDATE must produce a detection");
            assertTrue(results.stream().anyMatch(dt -> "UPDATE".equals(dt.operationType())),
                    "UPDATE SQL must be tagged UPDATE");
            assertTrue(results.stream().anyMatch(dt ->
                            dt.tableName() != null && dt.tableName().equalsIgnoreCase("ORDERS")),
                    "Table name ORDERS must be extracted");
        }

        @Test
        @DisplayName("B22 — jdbcTemplate.update('INSERT INTO ORDERS VALUES(...)') → opType=WRITE")
        void jdbcTemplateInsert() {
            IndexedClass ic = serviceClass();
            InvocationRef call = inv(
                    "org.springframework.jdbc.core.JdbcTemplate", "update",
                    List.of("INSERT INTO ORDERS VALUES(?, ?, ?)"), "jdbcTemplate");
            IndexedMethod im = makeMethod("insertOrder", List.of(call));
            List<DetectedTable> results = detector.detect(im, ic, Map.of(), Map.of()).resolved();
            assertFalse(results.isEmpty(), "jdbcTemplate INSERT must produce a detection");
            assertTrue(results.stream().anyMatch(dt -> "WRITE".equals(dt.operationType())),
                    "INSERT SQL must be tagged WRITE");
        }

        @Test
        @DisplayName("B23 — jdbcTemplate.update('DELETE FROM ORDERS WHERE id=?') → opType=DELETE")
        void jdbcTemplateDelete() {
            IndexedClass ic = serviceClass();
            InvocationRef call = inv(
                    "org.springframework.jdbc.core.JdbcTemplate", "update",
                    List.of("DELETE FROM ORDERS WHERE id=?"), "jdbcTemplate");
            IndexedMethod im = makeMethod("deleteOrder", List.of(call));
            List<DetectedTable> results = detector.detect(im, ic, Map.of(), Map.of()).resolved();
            assertFalse(results.isEmpty(), "jdbcTemplate DELETE must produce a detection");
            assertTrue(results.stream().anyMatch(dt -> "DELETE".equals(dt.operationType())),
                    "DELETE SQL must be tagged DELETE");
        }

        @Test
        @DisplayName("B24 — connection.prepareStatement('SELECT * FROM users WHERE id=?') → opType=READ, table=users")
        void connectionPrepareStatement() {
            // connection.prepareStatement(sql) → ownerClass=Connection, methodName=prepareStatement
            // Section G: CONNECTION_CALL_METHODS contains 'prepareCall' not 'prepareStatement'
            // Section H2: PREPARED_STMT_OWNERS contains 'PreparedStatement' not 'Connection'
            // → falls through without detection unless SQL is in G2 fallback
            IndexedClass ic = serviceClass();
            InvocationRef call = inv(
                    "java.sql.Connection", "prepareStatement",
                    List.of("SELECT * FROM users WHERE id=?"), "conn");
            IndexedMethod im = makeMethod("findUser", List.of(call));
            List<DetectedTable> results = detector.detect(im, ic, Map.of(), Map.of()).resolved();
            assertFalse(results.isEmpty(), "connection.prepareStatement SELECT must produce a detection");
            assertTrue(results.stream().anyMatch(dt -> "READ".equals(dt.operationType())),
                    "prepareStatement SELECT must be READ");
            assertTrue(results.stream().anyMatch(dt ->
                            dt.tableName() != null && dt.tableName().equalsIgnoreCase("users")),
                    "Table name 'users' must be extracted");
        }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // B25–B27: False-positive prevention
    // ─────────────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("B25–B27 — False-positive prevention")
    class FalsePositivePrevention {

        @Test
        @DisplayName("B25 — setFetchSize(100) on non-repository class → NOT tagged as UPDATE")
        void setFetchSizeNotUpdate() {
            IndexedClass ic = makeClass("com.example.BatchConfig", "BatchConfig",
                    "CONFIGURATION", null, null);
            // setFetchSize is in SKIP_METHODS? No — but inferDerivedQueryOp is only called for REPOSITORY.
            // setFetchSize as InvocationRef has no matching owner/method pattern → no detection.
            InvocationRef call = inv("java.sql.Statement", "setFetchSize",
                    List.of(), null);
            IndexedMethod im = makeMethod("setFetchSize", List.of(call));
            List<DetectedTable> results = detector.detect(im, ic, Map.of(), Map.of()).resolved();
            assertTrue(results.isEmpty() ||
                            results.stream().noneMatch(dt -> "UPDATE".equals(dt.operationType())),
                    "setFetchSize must NOT be tagged as UPDATE");
        }

        @Test
        @DisplayName("B26 — setMaxResults(50) on non-repository → NOT tagged as UPDATE")
        void setMaxResultsNotUpdate() {
            IndexedClass ic = makeClass("com.example.QueryHelper", "QueryHelper",
                    "SERVICE", null, null);
            InvocationRef call = inv("javax.persistence.Query", "setMaxResults",
                    List.of(), null);
            IndexedMethod im = makeMethod("setMaxResults", List.of(call));
            List<DetectedTable> results = detector.detect(im, ic, Map.of(), Map.of()).resolved();
            assertTrue(results.isEmpty() ||
                            results.stream().noneMatch(dt -> "UPDATE".equals(dt.operationType())),
                    "setMaxResults must NOT be tagged as UPDATE");
        }

        @Test
        @DisplayName("B27 — log.info('processing') → NO DB operation detected")
        void logInfoNoDetection() {
            IndexedClass ic = makeClass("com.example.OrderService", "OrderService",
                    "SERVICE", null, null);
            InvocationRef call = inv("org.slf4j.Logger", "info",
                    List.of("processing order"), "log");
            IndexedMethod im = makeMethod("processOrder", List.of(call));
            List<DetectedTable> results = detector.detect(im, ic, Map.of(), Map.of()).resolved();
            assertTrue(results.isEmpty(),
                    "log.info() must produce no DB detections (logger field guard)");
        }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // B28–B29: Non-DB calls that must NOT produce detections
    // ─────────────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("B28–B29 — Non-DB calls must produce no DB detections")
    class NonDbCalls {

        @Test
        @DisplayName("B28 — list.remove(item) on java.util.List → NO DELETE detected")
        void listRemoveNotDetected() {
            IndexedClass ic = makeClass("com.example.OrderService", "OrderService",
                    "SERVICE", null, null);
            // java.util.List.remove is neither in EM_ENTITY_METHODS owners nor JDBC_TEMPLATE owners
            InvocationRef call = inv("java.util.ArrayList", "remove",
                    List.of(), null);
            IndexedMethod im = makeMethod("removeFromList", List.of(call));
            List<DetectedTable> results = detector.detect(im, ic, Map.of(), Map.of()).resolved();
            assertTrue(results.isEmpty() ||
                            results.stream().noneMatch(dt -> "DELETE".equals(dt.operationType())),
                    "java.util.List.remove() must NOT be tagged as a DB DELETE");
        }

        @Test
        @DisplayName("B29 — entity.toString() → NO DB operation (toString is in SKIP_METHODS)")
        void toStringSkipped() {
            IndexedClass ic = makeClass("com.example.OrderService", "OrderService",
                    "SERVICE", null, null);
            InvocationRef call = inv("com.example.Order", "toString",
                    List.of(), null);
            IndexedMethod im = makeMethod("logOrder", List.of(call));
            List<DetectedTable> results = detector.detect(im, ic, Map.of(), Map.of()).resolved();
            assertTrue(results.isEmpty(),
                    "toString() is in SKIP_METHODS and must produce no DB detection");
        }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // B30: SQL with JOIN — multiple tables extracted
    // ─────────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("B30 — SQL JOIN: 'SELECT o.*, i.* FROM ORDERS o JOIN ITEMS i ON o.id=i.order_id' → tables ORDERS + ITEMS")
    void sqlJoinMultipleTables() {
        IndexedClass ic = makeClass("com.example.ReportService", "ReportService",
                "SERVICE", null, null);
        String sql = "SELECT o.*, i.* FROM ORDERS o JOIN ITEMS i ON o.id=i.order_id";
        InvocationRef call = inv(
                "org.springframework.jdbc.core.JdbcTemplate", "query",
                List.of(sql), "jdbcTemplate");
        IndexedMethod im = makeMethod("getOrderItems", List.of(call));
        List<DetectedTable> results = detector.detect(im, ic, Map.of(), Map.of()).resolved();
        assertFalse(results.isEmpty(), "SQL JOIN must produce at least one detection");
        boolean hasOrders = results.stream().anyMatch(dt ->
                dt.tableName() != null && dt.tableName().equalsIgnoreCase("ORDERS"));
        boolean hasItems = results.stream().anyMatch(dt ->
                dt.tableName() != null && dt.tableName().equalsIgnoreCase("ITEMS"));
        assertTrue(hasOrders, "Table ORDERS must be extracted from JOIN query");
        assertTrue(hasItems, "Table ITEMS must be extracted from JOIN query");
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // B31–B35: Gap fixes — accuracy improvement round
    // ─────────────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("B31–B35 — Gap fixes: JdbcClient, em.contains, hasJpaInvocations")
    class GapFixes {

        private static IndexedClass serviceClass() {
            return makeClass("com.example.OrderService", "OrderService",
                    "SERVICE", null, null);
        }

        @Test
        @DisplayName("B31 — GAP-JDBC-CLIENT: JdbcClient.sql('SELECT * FROM USERS WHERE id=?').query() → READ, table=USERS")
        void jdbcClientSqlQuery() {
            // Spring 6.1 fluent API: JdbcClient.sql(sql).query() — sql() is the entry point.
            // Section A fires on JdbcClient.sql() because JdbcClient is now in JDBC_SQL_OWNERS.
            IndexedClass ic = serviceClass();
            InvocationRef call = inv(
                    "org.springframework.jdbc.core.simple.JdbcClient", "sql",
                    List.of("SELECT * FROM USERS WHERE id=?"), "jdbcClient");
            IndexedMethod im = makeMethod("findUser", List.of(call));
            List<DetectedTable> results = detector.detect(im, ic, Map.of(), Map.of()).resolved();
            assertFalse(results.isEmpty(), "JdbcClient.sql() SELECT must produce a detection");
            assertTrue(results.stream().anyMatch(dt -> "READ".equals(dt.operationType())),
                    "JdbcClient.sql() SELECT must be READ");
            assertTrue(results.stream().anyMatch(dt ->
                            dt.tableName() != null && dt.tableName().equalsIgnoreCase("USERS")),
                    "JdbcClient.sql() must extract table name 'USERS'");
        }

        @Test
        @DisplayName("B32 — GAP-JDBC-CLIENT: JdbcClient.sql('UPDATE ORDERS SET status=?').update() → UPDATE, table=ORDERS")
        void jdbcClientSqlUpdate() {
            IndexedClass ic = serviceClass();
            // The terminal update() call on StatementSpec is also recognised via JDBC_SQL_OWNERS.
            InvocationRef sqlCall = inv(
                    "org.springframework.jdbc.core.simple.JdbcClient", "sql",
                    List.of("UPDATE ORDERS SET status=? WHERE id=?"), "jdbcClient");
            IndexedMethod im = makeMethod("updateOrder", List.of(sqlCall));
            List<DetectedTable> results = detector.detect(im, ic, Map.of(), Map.of()).resolved();
            assertFalse(results.isEmpty(), "JdbcClient.sql() UPDATE must produce a detection");
            assertTrue(results.stream().anyMatch(dt -> "UPDATE".equals(dt.operationType())),
                    "JdbcClient.sql() UPDATE SQL must be UPDATE");
        }

        @Test
        @DisplayName("B33 — GAP-EM-CONTAINS: em.contains(entity) → opType=READ (was silently dropped by old SKIP_METHODS)")
        void emContainsIsRead() {
            // Previously "contains" was in SKIP_METHODS, so em.contains(entity) was silently
            // skipped before reaching the EM_ENTITY_METHODS branch. Now "contains" is removed
            // from SKIP_METHODS; owner guard (EM_OWNERS) prevents false positives from
            // java.util.Collection.contains() which has a non-EM owner class.
            IndexedClass ic = serviceClass();
            InvocationRef call = inv("javax.persistence.EntityManager", "contains",
                    List.of("__class:com.example.Order"), "em");
            IndexedMethod im = makeMethod("checkContains", List.of(call));
            Map<String, String> entityTable = Map.of("com.example.Order", "ORDERS", "Order", "ORDERS");
            List<DetectedTable> results = detector.detect(im, ic, entityTable, Map.of()).resolved();
            assertFalse(results.isEmpty(),
                    "em.contains(entity) must produce a detection (was blocked by SKIP_METHODS bug)");
            assertTrue(results.stream().anyMatch(dt -> "READ".equals(dt.operationType())),
                    "em.contains() must be READ");
        }

        @Test
        @DisplayName("B34 — GAP-CONN-PREPARE: hasJpaInvocations returns true for Connection.prepareStatement")
        void hasJpaInvocationsForPrepareStatement() {
            // Connection.prepareStatement(sql) is handled in section G1 of detect(),
            // but hasJpaInvocations() previously did not recognise it — causing
            // hasDbInteraction to be false when only prepareStatement was used.
            IndexedClass ic = serviceClass();
            InvocationRef call = inv("java.sql.Connection", "prepareStatement",
                    List.of("SELECT * FROM INVOICES WHERE id=?"), "conn");
            IndexedMethod im = makeMethod("findInvoice", List.of(call));
            // hasJpaInvocations must now return true for this pattern
            assertTrue(detector.hasJpaInvocations(im),
                    "hasJpaInvocations must return true for Connection.prepareStatement (GAP-CONN-PREPARE)");
        }

        @Test
        @DisplayName("B35 — GAP-COLLECTION-CONTAINS-FP: java.util.List.contains() must NOT be tagged as READ")
        void collectionContainsNoFalsePositive() {
            // Belt-and-suspenders: verifies that removing "contains" from SKIP_METHODS
            // does NOT create a false-positive for java.util.List.contains().
            // The owner class (ArrayList) is not in EM_OWNERS → no detection.
            IndexedClass ic = serviceClass();
            InvocationRef call = inv("java.util.ArrayList", "contains",
                    List.of("__class:com.example.Order"), null);
            IndexedMethod im = makeMethod("checkList", List.of(call));
            List<DetectedTable> results = detector.detect(im, ic,
                    Map.of("com.example.Order", "ORDERS"), Map.of()).resolved();
            assertTrue(results.isEmpty(),
                    "java.util.ArrayList.contains() must NOT be tagged as a DB READ (owner guard)");
        }
    }
}
