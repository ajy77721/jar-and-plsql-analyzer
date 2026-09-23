package com.jaranalyzer.service;

import com.jaranalyzer.model.AnnotationInfo;
import com.jaranalyzer.model.CallNode;
import com.jaranalyzer.model.ClassInfo;
import com.jaranalyzer.model.MethodInfo;
import com.jaranalyzer.service.CallGraphIndex.AnnotationData;
import com.jaranalyzer.service.CallGraphIndex.IndexedClass;
import com.jaranalyzer.service.CallGraphIndex.IndexedMethod;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Category H — Transaction Boundary Tests (15 tests).
 *
 * Tests verify that @Transactional metadata is correctly extracted into
 * {@link CallNode#isTransactionalBoundary()}, {@link CallNode#getTransactionalPropagation()},
 * and {@link CallNode#isTransactionalReadOnly()} by
 * {@code CallTreeBuilder.createEnrichedNode()}.
 *
 * Strategy: drive {@code createEnrichedNode} via reflection, exactly as
 * AsyncBoundaryTest and CacheTaggingTest do, feeding in stubs built from
 * {@link AnnotationInfo} / {@link AnnotationData} in the method's annotationDetailList.
 */
@DisplayName("Category H — @Transactional boundary detection")
class TransactionBoundaryTest {

    // ── Builder factory ───────────────────────────────────────────────────────

    private CallTreeBuilder builder;

    @BeforeEach
    void setUp() throws Exception {
        DomainConfigLoader loader = new DomainConfigLoader();
        // applyDefaults() populates the DomainConfigLoader with default patterns
        Method applyDefaults = DomainConfigLoader.class.getDeclaredMethod("applyDefaults");
        applyDefaults.setAccessible(true);
        applyDefaults.invoke(loader);

        MongoMethodDetector mongoDetector = new MongoMethodDetector(loader);
        AggregationDetector aggregationDetector = new AggregationDetector(loader);
        JpaMethodDetector jpaDetector = new JpaMethodDetector(loader);
        MyBatisMethodDetector myBatisDetector = new MyBatisMethodDetector();
        RedisMethodDetector redisDetector = new RedisMethodDetector();
        CassandraMethodDetector cassandraDetector = new CassandraMethodDetector();
        ElasticsearchMethodDetector esDetector = new ElasticsearchMethodDetector();
        CollectionResolver resolver = new CollectionResolver(loader, mongoDetector,
                aggregationDetector, jpaDetector, myBatisDetector,
                redisDetector, cassandraDetector, esDetector,
                new CollectionLifecycleDetector(loader));
        builder = new CallTreeBuilder(loader, resolver, 30, 30, 2000, 100000);
        builder.createContext();
    }

    // ── Reflection helper ─────────────────────────────────────────────────────

    private CallNode invokeCreateEnrichedNode(IndexedClass cls, IndexedMethod method) throws Exception {
        Method m = CallTreeBuilder.class.getDeclaredMethod(
                "createEnrichedNode",
                IndexedClass.class, IndexedMethod.class,
                String.class, String.class, String.class);
        m.setAccessible(true);
        String displayId = cls.fqn + "#" + method.name;
        String nodeKey   = cls.fqn + "#" + method.name + method.descriptor;
        return (CallNode) m.invoke(builder, cls, method, displayId, nodeKey, null);
    }

    // ── Stub builders ─────────────────────────────────────────────────────────

    private static IndexedClass minimalClass(String fqn, String simpleName) {
        ClassInfo ci = new ClassInfo();
        ci.setFullyQualifiedName(fqn);
        ci.setSimpleName(simpleName);
        ci.setStereotype("SERVICE");
        return new IndexedClass(ci, Pattern.compile("(?!x)x"));
    }

    /**
     * Build an IndexedClass that carries a class-level @Transactional annotation with the
     * supplied attributes.  Used by H8 to verify class-level annotation propagation.
     */
    private static IndexedClass classWithTransactional(String fqn, String simpleName,
                                                        Map<String, Object> txAttrs) {
        ClassInfo ci = new ClassInfo();
        ci.setFullyQualifiedName(fqn);
        ci.setSimpleName(simpleName);
        ci.setStereotype("SERVICE");

        AnnotationInfo txAnn = new AnnotationInfo();
        txAnn.setName("Transactional");
        txAnn.setAttributes(txAttrs);
        ci.getAnnotations().add(txAnn);

        return new IndexedClass(ci, Pattern.compile("(?!x)x"));
    }

    /**
     * Build an IndexedMethod that carries an @Transactional annotation with the
     * supplied attributes map.  Pass an empty map for bare @Transactional with no attrs.
     *
     * @param annotationName  e.g. "Transactional" or the full simple-name variant
     * @param attrs           annotation attribute map (may be empty)
     */
    private static IndexedMethod methodWithTransactional(String methodName,
                                                          String annotationName,
                                                          Map<String, Object> attrs) {
        MethodInfo mi = new MethodInfo();
        mi.setName(methodName);
        mi.setDescriptor("()V");
        mi.setReturnType("void");

        AnnotationInfo ann = new AnnotationInfo();
        ann.setName(annotationName);
        ann.setAttributes(attrs);
        mi.getAnnotations().add(ann);

        return new IndexedMethod(mi);
    }

    /** Build an IndexedMethod with no annotations at all. */
    private static IndexedMethod plainMethod(String methodName) {
        MethodInfo mi = new MethodInfo();
        mi.setName(methodName);
        mi.setDescriptor("()V");
        mi.setReturnType("void");
        return new IndexedMethod(mi);
    }

    /** Build a method with both @Transactional and @Async annotations. */
    private static IndexedMethod methodWithTransactionalAndAsync(String methodName,
                                                                  Map<String, Object> txAttrs) {
        MethodInfo mi = new MethodInfo();
        mi.setName(methodName);
        mi.setDescriptor("()V");
        mi.setReturnType("void");

        AnnotationInfo txAnn = new AnnotationInfo();
        txAnn.setName("Transactional");
        txAnn.setAttributes(txAttrs);
        mi.getAnnotations().add(txAnn);

        AnnotationInfo asyncAnn = new AnnotationInfo();
        asyncAnn.setName("Async");
        asyncAnn.setAttributes(Map.of());
        mi.getAnnotations().add(asyncAnn);

        return new IndexedMethod(mi);
    }

    // ── H1: bare @Transactional → transactionalBoundary=true ─────────────────

    @Test
    @DisplayName("H1 Method with @Transactional → transactionalBoundary=true")
    void h1_transactionalAnnotation_setsBoundaryTrue() throws Exception {
        IndexedClass cls = minimalClass("com.example.OrderService", "OrderService");
        IndexedMethod method = methodWithTransactional("placeOrder", "Transactional", Map.of());

        CallNode node = invokeCreateEnrichedNode(cls, method);

        assertTrue(node.isTransactionalBoundary(),
                "@Transactional method must set transactionalBoundary=true on the CallNode");
    }

    // ── H2: @Transactional(readOnly=true) → transactionalReadOnly=true ───────

    @Test
    @DisplayName("H2 @Transactional(readOnly=true) → transactionalReadOnly=true")
    void h2_readOnlyTrue_setsReadOnlyFlag() throws Exception {
        IndexedClass cls = minimalClass("com.example.OrderService", "OrderService");
        Map<String, Object> attrs = new LinkedHashMap<>();
        attrs.put("readOnly", Boolean.TRUE);
        IndexedMethod method = methodWithTransactional("getOrder", "Transactional", attrs);

        CallNode node = invokeCreateEnrichedNode(cls, method);

        assertTrue(node.isTransactionalBoundary(),
                "readOnly=true must still set transactionalBoundary=true");
        assertTrue(node.isTransactionalReadOnly(),
                "@Transactional(readOnly=true) must set transactionalReadOnly=true");
    }

    // ── H3: propagation=REQUIRES_NEW ─────────────────────────────────────────

    @Test
    @DisplayName("H3 @Transactional(propagation=REQUIRES_NEW) → transactionalPropagation contains REQUIRES_NEW")
    void h3_propagationRequiresNew() throws Exception {
        IndexedClass cls = minimalClass("com.example.PaymentService", "PaymentService");
        Map<String, Object> attrs = new LinkedHashMap<>();
        attrs.put("propagation", "REQUIRES_NEW");
        IndexedMethod method = methodWithTransactional("chargeCard", "Transactional", attrs);

        CallNode node = invokeCreateEnrichedNode(cls, method);

        assertTrue(node.isTransactionalBoundary(),
                "REQUIRES_NEW must set transactionalBoundary=true");
        assertNotNull(node.getTransactionalPropagation(),
                "transactionalPropagation must not be null when propagation is specified");
        assertTrue(node.getTransactionalPropagation().contains("REQUIRES_NEW"),
                "transactionalPropagation must contain 'REQUIRES_NEW'. Actual: "
                        + node.getTransactionalPropagation());
    }

    // ── H4: propagation=SUPPORTS ─────────────────────────────────────────────

    @Test
    @DisplayName("H4 @Transactional(propagation=SUPPORTS) → transactionalPropagation=SUPPORTS")
    void h4_propagationSupports() throws Exception {
        IndexedClass cls = minimalClass("com.example.ReportService", "ReportService");
        Map<String, Object> attrs = new LinkedHashMap<>();
        attrs.put("propagation", "SUPPORTS");
        IndexedMethod method = methodWithTransactional("generateReport", "Transactional", attrs);

        CallNode node = invokeCreateEnrichedNode(cls, method);

        assertTrue(node.isTransactionalBoundary());
        assertNotNull(node.getTransactionalPropagation());
        assertTrue(node.getTransactionalPropagation().contains("SUPPORTS"),
                "transactionalPropagation must contain 'SUPPORTS'. Actual: "
                        + node.getTransactionalPropagation());
    }

    // ── H5: propagation=MANDATORY ─────────────────────────────────────────────

    @Test
    @DisplayName("H5 @Transactional(propagation=MANDATORY) → transactionalPropagation=MANDATORY")
    void h5_propagationMandatory() throws Exception {
        IndexedClass cls = minimalClass("com.example.AuditService", "AuditService");
        Map<String, Object> attrs = new LinkedHashMap<>();
        attrs.put("propagation", "MANDATORY");
        IndexedMethod method = methodWithTransactional("recordAudit", "Transactional", attrs);

        CallNode node = invokeCreateEnrichedNode(cls, method);

        assertTrue(node.isTransactionalBoundary());
        assertNotNull(node.getTransactionalPropagation());
        assertTrue(node.getTransactionalPropagation().contains("MANDATORY"),
                "transactionalPropagation must contain 'MANDATORY'. Actual: "
                        + node.getTransactionalPropagation());
    }

    // ── H6: readOnly=true + propagation=REQUIRES_NEW (both flags) ────────────

    @Test
    @DisplayName("H6 @Transactional(readOnly=true, propagation=REQUIRES_NEW) → both flags set")
    void h6_readOnlyAndPropagationBothSet() throws Exception {
        IndexedClass cls = minimalClass("com.example.InventoryService", "InventoryService");
        Map<String, Object> attrs = new LinkedHashMap<>();
        attrs.put("readOnly", Boolean.TRUE);
        attrs.put("propagation", "REQUIRES_NEW");
        IndexedMethod method = methodWithTransactional("checkStock", "Transactional", attrs);

        CallNode node = invokeCreateEnrichedNode(cls, method);

        assertTrue(node.isTransactionalBoundary(),
                "transactionalBoundary must be true");
        assertTrue(node.isTransactionalReadOnly(),
                "transactionalReadOnly must be true");
        assertNotNull(node.getTransactionalPropagation(),
                "transactionalPropagation must not be null");
        assertTrue(node.getTransactionalPropagation().contains("REQUIRES_NEW"),
                "transactionalPropagation must contain REQUIRES_NEW. Actual: "
                        + node.getTransactionalPropagation());
    }

    // ── H7: no @Transactional → transactionalBoundary=false ──────────────────

    @Test
    @DisplayName("H7 Method with no @Transactional → transactionalBoundary=false")
    void h7_noTransactionalAnnotation_boundaryFalse() throws Exception {
        IndexedClass cls = minimalClass("com.example.HelperService", "HelperService");
        IndexedMethod method = plainMethod("doSomething");

        CallNode node = invokeCreateEnrichedNode(cls, method);

        assertFalse(node.isTransactionalBoundary(),
                "Method without @Transactional must have transactionalBoundary=false");
    }

    // ── H8: class-level @Transactional ───────────────────────────────────────

    @Test
    @DisplayName("H8 @Transactional at class level → method node marked transactionalBoundary=true")
    void h8_classLevelTransactional_methodNodeMarked() throws Exception {
        // The class carries @Transactional but the method does not.
        // createEnrichedNode now falls back to cls.annotationDetailList when the method
        // carries no @Transactional of its own (GAP-H8 fix).
        IndexedClass cls = classWithTransactional(
                "com.example.TransactionalService", "TransactionalService", Map.of());
        // Method has no @Transactional of its own
        IndexedMethod method = plainMethod("doWork");

        CallNode node = invokeCreateEnrichedNode(cls, method);

        assertTrue(node.isTransactionalBoundary(),
                "Class-level @Transactional must propagate to method node");
    }

    // ── H9: annotationDetails contains @Transactional entry ──────────────────

    @Test
    @DisplayName("H9 @Transactional annotation present in annotationDetails list on the node")
    void h9_transactionalInAnnotationDetails() throws Exception {
        IndexedClass cls = minimalClass("com.example.OrderService", "OrderService");
        IndexedMethod method = methodWithTransactional("save", "Transactional", Map.of());

        CallNode node = invokeCreateEnrichedNode(cls, method);

        assertNotNull(node.getAnnotationDetails(),
                "annotationDetails must not be null");
        boolean hasTransactional = node.getAnnotationDetails().stream()
                .anyMatch(d -> {
                    Object name = d.get("name");
                    return name != null && name.toString().contains("Transactional");
                });
        assertTrue(hasTransactional,
                "annotationDetails must contain an entry with name containing 'Transactional'. Actual: "
                        + node.getAnnotationDetails());
    }

    // ── H10: full Spring descriptor annotation name ───────────────────────────

    @Test
    @DisplayName("H10 @org.springframework.transaction.annotation.Transactional (full simple name variant) → recognized")
    void h10_fullDescriptorAnnotationName_recognized() throws Exception {
        // Bytecode parsers typically produce the simple name "Transactional"; however
        // we test that any annotation whose name *contains* "Transactional" is matched,
        // consistent with the production code's `ad.name().contains("Transactional")` guard.
        IndexedClass cls = minimalClass("com.example.ClaimService", "ClaimService");
        // Use a fully-qualified-style simple name as the parser might emit
        IndexedMethod method = methodWithTransactional("processClaim",
                "org.springframework.transaction.annotation.Transactional", Map.of());

        CallNode node = invokeCreateEnrichedNode(cls, method);

        assertTrue(node.isTransactionalBoundary(),
                "Annotation name containing 'Transactional' (any prefix) must be recognized");
    }

    // ── H11: no propagation attribute → transactionalPropagation is null ─────

    @Test
    @DisplayName("H11 @Transactional with no propagation attribute → transactionalPropagation is null")
    void h11_noPropagationAttribute_propagationIsNull() throws Exception {
        IndexedClass cls = minimalClass("com.example.OrderService", "OrderService");
        IndexedMethod method = methodWithTransactional("updateOrder", "Transactional", Map.of());

        CallNode node = invokeCreateEnrichedNode(cls, method);

        assertTrue(node.isTransactionalBoundary(),
                "Bare @Transactional must still set transactionalBoundary=true");
        assertNull(node.getTransactionalPropagation(),
                "transactionalPropagation must be null when propagation attribute is absent. Actual: "
                        + node.getTransactionalPropagation());
    }

    // ── H12: transactionalReadOnly=false by default ───────────────────────────

    @Test
    @DisplayName("H12 @Transactional without readOnly attribute → transactionalReadOnly=false (default)")
    void h12_readOnlyFalseByDefault() throws Exception {
        IndexedClass cls = minimalClass("com.example.OrderService", "OrderService");
        IndexedMethod method = methodWithTransactional("deleteOrder", "Transactional", Map.of());

        CallNode node = invokeCreateEnrichedNode(cls, method);

        assertTrue(node.isTransactionalBoundary());
        assertFalse(node.isTransactionalReadOnly(),
                "transactionalReadOnly must be false when readOnly attribute is absent");
    }

    // ── H13: @Transactional with rollbackFor → transactionalBoundary=true still

    @Test
    @DisplayName("H13 @Transactional(rollbackFor=Exception.class) → transactionalBoundary=true")
    void h13_transactionalWithRollbackFor_boundaryTrue() throws Exception {
        IndexedClass cls = minimalClass("com.example.PaymentService", "PaymentService");
        Map<String, Object> attrs = new LinkedHashMap<>();
        attrs.put("rollbackFor", "java.lang.Exception");
        IndexedMethod method = methodWithTransactional("processPayment", "Transactional", attrs);

        CallNode node = invokeCreateEnrichedNode(cls, method);

        assertTrue(node.isTransactionalBoundary(),
                "@Transactional(rollbackFor=...) must still set transactionalBoundary=true");
        // readOnly and propagation remain defaults
        assertFalse(node.isTransactionalReadOnly(),
                "transactionalReadOnly must still be false");
        assertNull(node.getTransactionalPropagation(),
                "transactionalPropagation must be null (no propagation attr)");
    }

    // ── H14: @TransactionalEventListener ─────────────────────────────────────

    @Test
    @DisplayName("H14 @TransactionalEventListener → transactionalBoundary=true")
    void h14_transactionalEventListener_boundaryTrue() throws Exception {
        // @TransactionalEventListener annotation contains "Transactional" in its name,
        // so the production code's contains("Transactional") guard would match it.
        // However, in practice the flow for @TransactionalEventListener methods
        // goes through connectEventListeners → child callType becomes "ASYNC_EVENT",
        // and the node would need separate evaluation.
        IndexedClass cls = minimalClass("com.example.OrderEventHandler", "OrderEventHandler");
        IndexedMethod method = methodWithTransactional("onOrderPlaced",
                "TransactionalEventListener", Map.of());

        CallNode node = invokeCreateEnrichedNode(cls, method);

        // The production `ad.name().contains("Transactional")` guard IS satisfied by
        // "TransactionalEventListener", so this actually passes in isolation.
        // The gap is in live wiring (event listener nodes bypass createEnrichedNode normally).
        assertTrue(node.isTransactionalBoundary(),
                "@TransactionalEventListener name contains 'Transactional' — should set boundary true");
    }

    // ── H15: @Transactional + @Async → both flags set ─────────────────────────

    @Test
    @DisplayName("H15 @Transactional + @Async on same method → transactionalBoundary=true AND asyncBoundary=true")
    void h15_transactionalAndAsync_bothFlagsSet() throws Exception {
        IndexedClass cls = minimalClass("com.example.EventService", "EventService");
        IndexedMethod method = methodWithTransactionalAndAsync("publishEventAsync", Map.of());

        CallNode node = invokeCreateEnrichedNode(cls, method);

        assertTrue(node.isTransactionalBoundary(),
                "@Transactional must set transactionalBoundary=true");
        assertTrue(node.isAsyncBoundary(),
                "@Async must set asyncBoundary=true");
    }
}
