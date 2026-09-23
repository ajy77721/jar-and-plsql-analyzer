package com.jaranalyzer.service;

import com.jaranalyzer.model.AnnotationInfo;
import com.jaranalyzer.model.CallNode;
import com.jaranalyzer.model.ClassInfo;
import com.jaranalyzer.model.MethodInfo;
import com.jaranalyzer.service.CallGraphIndex.IndexedClass;
import com.jaranalyzer.service.CallGraphIndex.IndexedMethod;
import com.jaranalyzer.service.CallGraphIndex.InvocationRef;
import com.jaranalyzer.service.CallGraphIndex.ResolutionContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Category G — Async Boundary Tests (15 tests)
 *
 * Tests verify:
 *   G1-G5   : asyncBoundary=true is set for @Async, CF, Mono, Flux return types
 *   G6, G15 : asyncBoundary=false for plain/synchronous methods
 *   G7-G13  : ASYNC_CHAIN leaf nodes created for CF/Reactor chain method calls
 *   G14     : @Async annotation surfaces in node's annotations list
 *
 * Strategy: drive createEnrichedNode and createAsyncChainLeaf via reflection
 * (same pattern as FalsePositiveDetectionTest), plus constants checks against
 * the static sets in CallTreeBuilder.
 */
@DisplayName("Category G — Async boundary detection")
class AsyncBoundaryTest {

    // ── Reflection helpers ────────────────────────────────────────────────────

    /**
     * Invoke the package-private createEnrichedNode to inspect asyncBoundary tagging.
     */
    private CallNode invokeCreateEnrichedNode(CallTreeBuilder builder,
                                               IndexedClass cls,
                                               IndexedMethod method) throws Exception {
        // createEnrichedNode(IndexedClass, IndexedMethod, String displayId, String nodeKey, String controllerJar)
        Method m = CallTreeBuilder.class.getDeclaredMethod(
                "createEnrichedNode",
                IndexedClass.class, IndexedMethod.class, String.class, String.class, String.class);
        m.setAccessible(true);
        String displayId = cls.fqn + "#" + method.name;
        String nodeKey   = cls.fqn + "#" + method.name + method.descriptor;
        return (CallNode) m.invoke(builder, cls, method, displayId, nodeKey, null /*controllerJar*/);
    }

    /**
     * Invoke the private createAsyncChainLeaf(InvocationRef) method.
     */
    private CallNode invokeCreateAsyncChainLeaf(CallTreeBuilder builder,
                                                 InvocationRef call) throws Exception {
        Method m = CallTreeBuilder.class.getDeclaredMethod(
                "createAsyncChainLeaf", InvocationRef.class);
        m.setAccessible(true);
        return (CallNode) m.invoke(builder, call);
    }

    /**
     * Access a private static Set<String> on CallTreeBuilder via reflection.
     */
    @SuppressWarnings("unchecked")
    private static Set<String> getStaticSet(String fieldName) throws Exception {
        Field f = CallTreeBuilder.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        return (Set<String>) f.get(null);
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
     * Build an IndexedMethod with a specific return type and optional @Async annotation.
     */
    private static IndexedMethod methodWithReturnType(String name, String returnType, boolean hasAsync) {
        MethodInfo mi = new MethodInfo();
        mi.setName(name);
        mi.setDescriptor("()V");
        mi.setReturnType(returnType);
        if (hasAsync) {
            AnnotationInfo asyncAnn = new AnnotationInfo();
            asyncAnn.setName("Async");
            mi.getAnnotations().add(asyncAnn);
        }
        return new IndexedMethod(mi);
    }

    private static IndexedMethod methodWithReturnType(String name, String returnType) {
        return methodWithReturnType(name, returnType, false);
    }

    private static InvocationRef chainInvRef(String ownerClass, String methodName) {
        return new InvocationRef(ownerClass, methodName, "()Ljava/lang/Object;", "java.lang.Object",
                List.of(), 182, null);
    }

    // ── CallTreeBuilder factory ───────────────────────────────────────────────

    private CallTreeBuilder builder;

    @BeforeEach
    void setUp() {
        DomainConfigLoader loader = new DomainConfigLoader();
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
        ResolutionContext ctx = builder.createContext();
        // Replace Map.of() (immutable, rejects null keys) with a mutable map so that
        // createEnrichedNode's ctx.jarArtifactMap.getOrDefault(null, null) doesn't NPE.
        ctx.jarArtifactMap = new HashMap<>();
    }

    // ── G1: @Async annotation ─────────────────────────────────────────────────

    @Test
    @DisplayName("G1 Method with @Async → asyncBoundary=true on CallNode")
    void asyncAnnotation_setsAsyncBoundaryTrue() throws Exception {
        IndexedClass cls = minimalClass("com.example.OrderService", "OrderService");
        IndexedMethod method = methodWithReturnType("processOrder", "void", /*hasAsync=*/true);

        CallNode node = invokeCreateEnrichedNode(builder, cls, method);

        assertTrue(node.isAsyncBoundary(),
                "@Async method must set asyncBoundary=true on the resulting CallNode");
    }

    // ── G2: CompletableFuture return type ────────────────────────────────────

    @Test
    @DisplayName("G2 Method returning CompletableFuture<X> → asyncBoundary=true")
    void completableFutureReturn_setsAsyncBoundaryTrue() throws Exception {
        IndexedClass cls = minimalClass("com.example.OrderService", "OrderService");
        IndexedMethod method = methodWithReturnType("fetchAsync",
                "java.util.concurrent.CompletableFuture<com.example.Order>");

        CallNode node = invokeCreateEnrichedNode(builder, cls, method);

        assertTrue(node.isAsyncBoundary(),
                "CompletableFuture return type must trigger asyncBoundary=true");
    }

    // ── G3: Mono return type ──────────────────────────────────────────────────

    @Test
    @DisplayName("G3 Method returning Mono<X> → asyncBoundary=true")
    void monoReturn_setsAsyncBoundaryTrue() throws Exception {
        IndexedClass cls = minimalClass("com.example.ReactiveService", "ReactiveService");
        IndexedMethod method = methodWithReturnType("findUser",
                "reactor.core.publisher.Mono<com.example.User>");

        CallNode node = invokeCreateEnrichedNode(builder, cls, method);

        assertTrue(node.isAsyncBoundary(),
                "Mono return type must trigger asyncBoundary=true");
    }

    // ── G4: Flux return type ──────────────────────────────────────────────────

    @Test
    @DisplayName("G4 Method returning Flux<X> → asyncBoundary=true")
    void fluxReturn_setsAsyncBoundaryTrue() throws Exception {
        IndexedClass cls = minimalClass("com.example.ReactiveService", "ReactiveService");
        IndexedMethod method = methodWithReturnType("listOrders",
                "reactor.core.publisher.Flux<com.example.Order>");

        CallNode node = invokeCreateEnrichedNode(builder, cls, method);

        assertTrue(node.isAsyncBoundary(),
                "Flux return type must trigger asyncBoundary=true");
    }

    // ── G5: @Async + CompletableFuture ───────────────────────────────────────

    @Test
    @DisplayName("G5 @Async + CompletableFuture return type → asyncBoundary=true (both conditions)")
    void asyncAnnotationAndCFReturn_setsAsyncBoundaryTrue() throws Exception {
        IndexedClass cls = minimalClass("com.example.OrderService", "OrderService");
        IndexedMethod method = methodWithReturnType("processAsync",
                "java.util.concurrent.CompletableFuture<java.lang.Void>", true);

        CallNode node = invokeCreateEnrichedNode(builder, cls, method);

        assertTrue(node.isAsyncBoundary(),
                "@Async + CompletableFuture return must still set asyncBoundary=true");
    }

    // ── G6: Plain void, no @Async ────────────────────────────────────────────

    @Test
    @DisplayName("G6 Plain void method without @Async → asyncBoundary=false")
    void plainVoidMethod_asyncBoundaryFalse() throws Exception {
        IndexedClass cls = minimalClass("com.example.OrderService", "OrderService");
        IndexedMethod method = methodWithReturnType("placeOrder", "void");

        CallNode node = invokeCreateEnrichedNode(builder, cls, method);

        assertFalse(node.isAsyncBoundary(),
                "Plain synchronous void method must NOT set asyncBoundary");
    }

    // ── G7: CompletableFuture.supplyAsync → ASYNC_CHAIN leaf ─────────────────

    @Test
    @DisplayName("G7 CompletableFuture.supplyAsync() call → ASYNC_CHAIN leaf node in children")
    void supplyAsync_createsAsyncChainLeaf() throws Exception {
        InvocationRef call = chainInvRef(
                "java.util.concurrent.CompletableFuture", "supplyAsync");
        CallNode chainNode = invokeCreateAsyncChainLeaf(builder, call);

        assertEquals("ASYNC_CHAIN", chainNode.getCallType(),
                "supplyAsync must produce callType=ASYNC_CHAIN");
    }

    // ── G8: CompletableFuture.thenApply → ASYNC_CHAIN leaf ───────────────────

    @Test
    @DisplayName("G8 CompletableFuture.thenApply() call → ASYNC_CHAIN leaf")
    void thenApply_createsAsyncChainLeaf() throws Exception {
        InvocationRef call = chainInvRef(
                "java.util.concurrent.CompletableFuture", "thenApply");
        CallNode chainNode = invokeCreateAsyncChainLeaf(builder, call);

        assertEquals("ASYNC_CHAIN", chainNode.getCallType(),
                "thenApply must produce callType=ASYNC_CHAIN");
    }

    // ── G9: CompletableFuture.thenCompose → ASYNC_CHAIN leaf ─────────────────

    @Test
    @DisplayName("G9 CompletableFuture.thenCompose() → ASYNC_CHAIN leaf")
    void thenCompose_createsAsyncChainLeaf() throws Exception {
        InvocationRef call = chainInvRef(
                "java.util.concurrent.CompletableFuture", "thenCompose");
        CallNode chainNode = invokeCreateAsyncChainLeaf(builder, call);

        assertEquals("ASYNC_CHAIN", chainNode.getCallType());
    }

    // ── G10: Mono.flatMap → ASYNC_CHAIN leaf ─────────────────────────────────

    @Test
    @DisplayName("G10 Mono.flatMap() → ASYNC_CHAIN leaf")
    void monoFlatMap_createsAsyncChainLeaf() throws Exception {
        InvocationRef call = chainInvRef(
                "reactor.core.publisher.Mono", "flatMap");
        CallNode chainNode = invokeCreateAsyncChainLeaf(builder, call);

        assertEquals("ASYNC_CHAIN", chainNode.getCallType(),
                "Reactor Mono.flatMap must produce ASYNC_CHAIN leaf");
    }

    // ── G11: Flux.filter → ASYNC_CHAIN leaf ──────────────────────────────────

    @Test
    @DisplayName("G11 Flux.filter() → ASYNC_CHAIN leaf")
    void fluxFilter_createsAsyncChainLeaf() throws Exception {
        InvocationRef call = chainInvRef(
                "reactor.core.publisher.Flux", "filter");
        CallNode chainNode = invokeCreateAsyncChainLeaf(builder, call);

        assertEquals("ASYNC_CHAIN", chainNode.getCallType(),
                "Reactor Flux.filter must produce ASYNC_CHAIN leaf");
    }

    // ── G12: ASYNC_CHAIN node has asyncChainOp=true ───────────────────────────

    @Test
    @DisplayName("G12 ASYNC_CHAIN leaf node has asyncChainOp=true")
    void asyncChainLeaf_hasAsyncChainOpTrue() throws Exception {
        InvocationRef call = chainInvRef(
                "java.util.concurrent.CompletableFuture", "thenApply");
        CallNode chainNode = invokeCreateAsyncChainLeaf(builder, call);

        assertTrue(chainNode.isAsyncChainOp(),
                "ASYNC_CHAIN leaf must have asyncChainOp=true");
    }

    // ── G13: ASYNC_CHAIN node has callType="ASYNC_CHAIN" ─────────────────────

    @Test
    @DisplayName("G13 ASYNC_CHAIN leaf node has callType='ASYNC_CHAIN'")
    void asyncChainLeaf_hasCorrectCallType() throws Exception {
        InvocationRef call = chainInvRef(
                "reactor.core.publisher.Mono", "map");
        CallNode chainNode = invokeCreateAsyncChainLeaf(builder, call);

        assertEquals("ASYNC_CHAIN", chainNode.getCallType(),
                "asyncChainOp leaf must have callType=ASYNC_CHAIN");
    }

    // ── G14: @Async annotation surfaces in node's annotations list ────────────

    @Test
    @DisplayName("G14 @Async annotation shows up in node's annotations list as '@Async'")
    void asyncAnnotation_surfacesInAnnotationsList() throws Exception {
        IndexedClass cls = minimalClass("com.example.NotificationService", "NotificationService");
        IndexedMethod method = methodWithReturnType("sendEmailAsync", "void", /*hasAsync=*/true);

        CallNode node = invokeCreateEnrichedNode(builder, cls, method);

        assertNotNull(node.getAnnotations(), "annotations list must not be null");
        assertTrue(node.getAnnotations().contains("@Async"),
                "@Async must appear in the node's annotations list as '@Async'");
    }

    // ── G15: Plain String return type → asyncBoundary=false ──────────────────

    @Test
    @DisplayName("G15 Method returning non-reactive, non-CF type → asyncBoundary=false")
    void nonReactiveReturn_asyncBoundaryFalse() throws Exception {
        IndexedClass cls = minimalClass("com.example.OrderService", "OrderService");
        IndexedMethod method = methodWithReturnType("getOrderStatus", "java.lang.String");

        CallNode node = invokeCreateEnrichedNode(builder, cls, method);

        assertFalse(node.isAsyncBoundary(),
                "Non-reactive, non-CF return type must NOT set asyncBoundary");
    }

    // ── Bonus: verify CF_CHAIN_METHODS and REACTOR_CHAIN_METHODS sets ─────────

    @Test
    @DisplayName("Bonus: CF_CHAIN_METHODS contains expected CompletableFuture operations")
    void cfChainMethods_containsExpectedOps() throws Exception {
        Set<String> cfMethods = getStaticSet("CF_CHAIN_METHODS");
        assertTrue(cfMethods.contains("supplyAsync"), "supplyAsync must be in CF_CHAIN_METHODS");
        assertTrue(cfMethods.contains("thenApply"),   "thenApply must be in CF_CHAIN_METHODS");
        assertTrue(cfMethods.contains("thenCompose"), "thenCompose must be in CF_CHAIN_METHODS");
        assertTrue(cfMethods.contains("thenAccept"),  "thenAccept must be in CF_CHAIN_METHODS");
        assertTrue(cfMethods.contains("exceptionally"), "exceptionally must be in CF_CHAIN_METHODS");
    }

    @Test
    @DisplayName("Bonus: REACTOR_CHAIN_METHODS contains expected Mono/Flux operations")
    void reactorChainMethods_containsExpectedOps() throws Exception {
        Set<String> reactorMethods = getStaticSet("REACTOR_CHAIN_METHODS");
        assertTrue(reactorMethods.contains("flatMap"),     "flatMap must be in REACTOR_CHAIN_METHODS");
        assertTrue(reactorMethods.contains("map"),         "map must be in REACTOR_CHAIN_METHODS");
        assertTrue(reactorMethods.contains("filter"),      "filter must be in REACTOR_CHAIN_METHODS");
        assertTrue(reactorMethods.contains("subscribe"),   "subscribe must be in REACTOR_CHAIN_METHODS");
        assertTrue(reactorMethods.contains("doOnNext"),    "doOnNext must be in REACTOR_CHAIN_METHODS");
    }

    @Test
    @DisplayName("Bonus: ASYNC_OWNER_SIMPLE_NAMES covers CompletableFuture, Mono, Flux")
    void asyncOwnerSimpleNames_coversExpectedClasses() throws Exception {
        Set<String> ownerNames = getStaticSet("ASYNC_OWNER_SIMPLE_NAMES");
        assertTrue(ownerNames.contains("CompletableFuture"), "CompletableFuture must be in ASYNC_OWNER_SIMPLE_NAMES");
        assertTrue(ownerNames.contains("Mono"),              "Mono must be in ASYNC_OWNER_SIMPLE_NAMES");
        assertTrue(ownerNames.contains("Flux"),              "Flux must be in ASYNC_OWNER_SIMPLE_NAMES");
    }
}
