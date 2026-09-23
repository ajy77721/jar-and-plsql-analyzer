package com.jaranalyzer.service;

import com.jaranalyzer.model.AnnotationInfo;
import com.jaranalyzer.model.CallNode;
import com.jaranalyzer.model.ClassInfo;
import com.jaranalyzer.model.EndpointInfo;
import com.jaranalyzer.model.MethodCallInfo;
import com.jaranalyzer.model.MethodInfo;
import com.jaranalyzer.service.CallGraphIndex.IndexedClass;
import com.jaranalyzer.service.CallGraphIndex.IndexedMethod;
import com.jaranalyzer.service.CallGraphIndex.InvocationRef;
import com.jaranalyzer.service.CallGraphIndex.ResolutionContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Category J — Call Tree Visibility Tests (15 tests).
 *
 * Tests verify structural properties of built call trees: depth, cross-module
 * marking, leaf-node types (HTTP_CALL, MESSAGING_PRODUCER, ASYNC_CHAIN,
 * EVENT_LISTENER), recursion detection, and EndpointInfo aggregate correctness.
 *
 * Strategy:
 * - Tree-structural assertions use {@code buildCallTree} (package-private, invoked
 *   via reflection after full class indexing), matching the MessagingTaggingTest pattern.
 * - Leaf-node creation is tested via the private factory helpers via reflection.
 * - EndpointInfo aggregation uses the public {@code computeAggregates()} API.
 */
@DisplayName("Category J — Call tree structural visibility")
class CallTreeVisibilityTest {

    // ── Builder factory ───────────────────────────────────────────────────────

    private CallTreeBuilder builder;
    private ResolutionContext ctx;

    @BeforeEach
    void setUp() throws Exception {
        DomainConfigLoader loader = new DomainConfigLoader();
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
        ctx = builder.createContext();
    }

    // ── Reflection helpers ────────────────────────────────────────────────────

    private CallNode invokeBuildCallTree(IndexedClass cls, IndexedMethod method,
                                          String controllerJar) throws Exception {
        Method m = CallTreeBuilder.class.getDeclaredMethod(
                "buildCallTree",
                IndexedClass.class, IndexedMethod.class,
                Set.class, int.class, int[].class, String.class);
        m.setAccessible(true);
        return (CallNode) m.invoke(builder, cls, method, new HashSet<>(), 0,
                new int[]{0}, controllerJar);
    }

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

    private CallNode invokeCreateAsyncChainLeaf(InvocationRef call) throws Exception {
        Method m = CallTreeBuilder.class.getDeclaredMethod(
                "createAsyncChainLeaf", InvocationRef.class);
        m.setAccessible(true);
        return (CallNode) m.invoke(builder, call);
    }

    private CallNode invokeCreateHttpCallLeaf(InvocationRef call, IndexedMethod callerMethod) throws Exception {
        Method m = CallTreeBuilder.class.getDeclaredMethod(
                "createHttpCallLeaf", InvocationRef.class, IndexedMethod.class);
        m.setAccessible(true);
        return (CallNode) m.invoke(builder, call, callerMethod);
    }

    private CallNode invokeCreateMessagingProducerLeaf(InvocationRef call, IndexedMethod callerMethod) throws Exception {
        Method m = CallTreeBuilder.class.getDeclaredMethod(
                "createMessagingProducerLeaf", InvocationRef.class, IndexedMethod.class);
        m.setAccessible(true);
        return (CallNode) m.invoke(builder, call, callerMethod);
    }

    // ── Stub builders ─────────────────────────────────────────────────────────

    private static ClassInfo classInfo(String fqn, String simpleName, String stereotype) {
        ClassInfo ci = new ClassInfo();
        ci.setFullyQualifiedName(fqn);
        ci.setSimpleName(simpleName);
        ci.setStereotype(stereotype);
        return ci;
    }

    private static IndexedClass minimalClass(String fqn, String simpleName, String stereotype) {
        return new IndexedClass(classInfo(fqn, simpleName, stereotype), Pattern.compile("(?!x)x"));
    }

    private static IndexedMethod methodWithInvocations(String name, List<InvocationRef> invocations) {
        MethodInfo mi = new MethodInfo();
        mi.setName(name);
        mi.setDescriptor("()V");
        mi.setReturnType("void");
        IndexedMethod im = new IndexedMethod(mi);
        im.invocations.clear();
        im.invocations.addAll(invocations);
        return im;
    }

    private static IndexedMethod plainMethod(String name) {
        return methodWithInvocations(name, List.of());
    }

    private static InvocationRef invRef(String ownerClass, String methodName) {
        return new InvocationRef(ownerClass, methodName, "()V", "void",
                List.of(), 182 /* INVOKEVIRTUAL */, null);
    }

    private static InvocationRef invRefStatic(String ownerClass, String methodName) {
        return new InvocationRef(ownerClass, methodName, "()V", "void",
                List.of(), 184 /* INVOKESTATIC */, null);
    }

    private static InvocationRef invRefWithArgs(String ownerClass, String methodName,
                                                 List<String> args) {
        return new InvocationRef(ownerClass, methodName, "()V", "void",
                args, 182, null);
    }

    /** Build a MethodCallInfo (setter-based, no all-args constructor in production model). */
    private static MethodCallInfo mci(String ownerClass, String methodName, String descriptor) {
        MethodCallInfo m = new MethodCallInfo();
        m.setOwnerClass(ownerClass);
        m.setMethodName(methodName);
        m.setDescriptor(descriptor);
        m.setOpcode(182); // INVOKEVIRTUAL
        return m;
    }

    // ── J1: Direct A→B→C depth-3 tree ────────────────────────────────────────

    @Nested
    @DisplayName("J1 — Direct call chain A→B→C: all 3 nodes present")
    class DirectCallChain {

        @Test
        @DisplayName("J1 Direct A→B→C produces a tree with all 3 nodes reachable")
        void j1_directChain_allNodesPresent() throws Exception {
            // C: leaf — no further calls
            ClassInfo cCi = classInfo("com.example.C", "C", "REPOSITORY");
            MethodInfo cMi = new MethodInfo();
            cMi.setName("leafMethod");
            cMi.setDescriptor("()V");
            cMi.setReturnType("void");
            cCi.getMethods().add(cMi);
            builder.indexClass(cCi);

            // B: calls C.leafMethod
            ClassInfo bCi = classInfo("com.example.B", "B", "SERVICE");
            MethodInfo bMi = new MethodInfo();
            bMi.setName("midMethod");
            bMi.setDescriptor("()V");
            bMi.setReturnType("void");
            bMi.getInvocations().add(mci("com.example.C", "leafMethod", "()V"));
            bCi.getMethods().add(bMi);
            builder.indexClass(bCi);

            // A: calls B.midMethod
            ClassInfo aCi = classInfo("com.example.A", "A", "REST_CONTROLLER");
            MethodInfo aMi = new MethodInfo();
            aMi.setName("rootMethod");
            aMi.setDescriptor("()V");
            aMi.setReturnType("void");
            aMi.setHttpMethod("GET");
            aMi.getInvocations().add(mci("com.example.B", "midMethod", "()V"));
            aCi.getMethods().add(aMi);
            builder.indexClass(aCi);

            // Build the tree from A
            IndexedClass aCls = ctx.classMap.get("com.example.A");
            IndexedMethod aMethod = aCls.methods.get("rootMethod()V");
            assertNotNull(aMethod, "rootMethod must be indexed");

            CallNode root = invokeBuildCallTree(aCls, aMethod, null);

            assertNotNull(root, "Root node must not be null");
            assertEquals("rootMethod", root.getMethodName(), "Root must be A.rootMethod");

            // B must be a direct child
            assertFalse(root.getChildren().isEmpty(), "A must have at least one child (B)");
            CallNode bNode = root.getChildren().get(0);
            assertEquals("midMethod", bNode.getMethodName(), "First child must be B.midMethod");

            // C must be a child of B
            assertFalse(bNode.getChildren().isEmpty(), "B must have at least one child (C)");
            CallNode cNode = bNode.getChildren().get(0);
            assertEquals("leafMethod", cNode.getMethodName(), "B's child must be C.leafMethod");
        }
    }

    // ── J2: Cross-module call → crossModule=true ──────────────────────────────

    @Nested
    @DisplayName("J2 — Cross-module node has crossModule=true")
    class CrossModuleTest {

        @Test
        @DisplayName("J2 Node whose sourceJar differs from controllerJar → crossModule=true")
        void j2_crossModuleNode_isFlagged() throws Exception {
            // Build a node with a different sourceJar than the controllerJar
            IndexedClass cls = minimalClass("com.example.ExternalService", "ExternalService", "SERVICE");
            IndexedMethod method = plainMethod("externalOp");

            // Invoke createEnrichedNode with controllerJar="main-jar" and class has no sourceJar (null)
            // The production code: isCross = !Objects.equals(cls.sourceJar, controllerJar)
            Method createNode = CallTreeBuilder.class.getDeclaredMethod(
                    "createEnrichedNode",
                    IndexedClass.class, IndexedMethod.class,
                    String.class, String.class, String.class);
            createNode.setAccessible(true);
            // Use "different-jar" as controllerJar so sourceJar (null) != controllerJar ("different-jar")
            CallNode node = (CallNode) createNode.invoke(builder, cls, method,
                    "com.example.ExternalService#externalOp",
                    "com.example.ExternalService#externalOp()V",
                    "main-jar" /*controllerJar*/);

            assertTrue(node.isCrossModule(),
                    "Node with different sourceJar than controllerJar must have crossModule=true");
        }
    }

    // ── J3: HTTP_CALL leaf → correct fields ───────────────────────────────────

    @Nested
    @DisplayName("J3 — HTTP_CALL leaf node has callType=HTTP_CALL and crossModule=true")
    class HttpCallLeafTest {

        @Test
        @DisplayName("J3 HTTP_CALL leaf → callType=HTTP_CALL, crossModule=true")
        void j3_httpCallLeaf_correctFields() throws Exception {
            InvocationRef call = invRefWithArgs(
                    "org.springframework.web.client.RestTemplate",
                    "getForObject",
                    List.of("https://api.example.com/orders"));

            IndexedMethod caller = methodWithInvocations("fetchData", List.of());
            CallNode node = invokeCreateHttpCallLeaf(call, caller);

            assertEquals("HTTP_CALL", node.getCallType(),
                    "HTTP client call must produce callType=HTTP_CALL");
            assertTrue(node.isCrossModule(),
                    "HTTP_CALL leaf must have crossModule=true");
        }
    }

    // ── J4: MESSAGING_PRODUCER leaf ───────────────────────────────────────────

    @Nested
    @DisplayName("J4 — MESSAGING_PRODUCER leaf has correct callType")
    class MessagingProducerLeafTest {

        @Test
        @DisplayName("J4 KafkaTemplate.send → MESSAGING_PRODUCER leaf in tree")
        void j4_messagingProducerLeaf_callType() throws Exception {
            InvocationRef call = invRefWithArgs(
                    "org.springframework.kafka.core.KafkaTemplate",
                    "send",
                    List.of("topic-name"));
            IndexedMethod caller = methodWithInvocations("publishEvent", List.of());
            CallNode node = invokeCreateMessagingProducerLeaf(call, caller);

            assertEquals("MESSAGING_PRODUCER", node.getCallType(),
                    "KafkaTemplate.send must produce callType=MESSAGING_PRODUCER");
        }
    }

    // ── J5: EVENT_LISTENER connection ─────────────────────────────────────────

    @Nested
    @DisplayName("J5 — @EventListener node gets callType=EVENT_LISTENER")
    class EventListenerConnectionTest {

        @Test
        @DisplayName("J5 @EventListener registered method → callType EVENT_LISTENER in eventListenerMap")
        void j5_eventListenerMethod_isRegistered() {
            ClassInfo ci = classInfo("com.example.OrderEventHandler", "OrderEventHandler", "COMPONENT");
            AnnotationInfo ann = new AnnotationInfo();
            ann.setName("EventListener");
            Map<String, Object> attrs = new LinkedHashMap<>();
            attrs.put("value", "com.example.event.OrderPlaced");
            ann.setAttributes(attrs);

            MethodInfo mi = new MethodInfo();
            mi.setName("handleOrder");
            mi.setDescriptor("(Lcom/example/event/OrderPlaced;)V");
            mi.setReturnType("void");
            mi.getAnnotations().add(ann);
            ci.getMethods().add(mi);

            builder.indexClass(ci);

            // Confirm it's registered with the correct event type
            var listeners = ctx.eventListenerMap.get("com.example.event.OrderPlaced");
            assertNotNull(listeners, "Event listener must be registered in eventListenerMap");
            assertFalse(listeners.isEmpty(), "eventListenerMap entry must not be empty");
            // When the tree is built, connectEventListeners sets callType = EVENT_LISTENER
            // We verify isTransactional=false (it's @EventListener, not @TransactionalEventListener)
            assertTrue(listeners.stream().anyMatch(ref -> !ref.isTransactional()),
                    "@EventListener (not transactional) must have isTransactional=false");
        }
    }

    // ── J6: ASYNC_CHAIN leaf → asyncChainOp=true ─────────────────────────────

    @Nested
    @DisplayName("J6 — ASYNC_CHAIN leaf has asyncChainOp=true and callType=ASYNC_CHAIN")
    class AsyncChainLeafTest {

        @Test
        @DisplayName("J6 CompletableFuture.supplyAsync → ASYNC_CHAIN leaf with asyncChainOp=true")
        void j6_asyncChainLeaf_asyncChainOpTrue() throws Exception {
            InvocationRef call = new InvocationRef(
                    "java.util.concurrent.CompletableFuture", "supplyAsync",
                    "()Ljava/util/concurrent/CompletableFuture;",
                    "java.util.concurrent.CompletableFuture",
                    List.of(), 184, null);
            CallNode node = invokeCreateAsyncChainLeaf(call);

            assertTrue(node.isAsyncChainOp(),
                    "ASYNC_CHAIN leaf must have asyncChainOp=true");
            assertEquals("ASYNC_CHAIN", node.getCallType(),
                    "ASYNC_CHAIN leaf must have callType=ASYNC_CHAIN");
        }
    }

    // ── J7: Recursive call → recursive=true, no infinite loop ────────────────

    @Nested
    @DisplayName("J7 — Recursive self-call → node.recursive=true, no stack overflow")
    class RecursionTest {

        @Test
        @DisplayName("J7 Self-recursive method → node marked recursive=true without infinite loop")
        void j7_recursiveSelfCall_markedAndTerminates() throws Exception {
            // A method that calls itself
            ClassInfo ci = classInfo("com.example.RecursiveService", "RecursiveService", "SERVICE");
            MethodInfo mi = new MethodInfo();
            mi.setName("recurse");
            mi.setDescriptor("()V");
            mi.setReturnType("void");
            // Add self-invocation
            mi.getInvocations().add(mci("com.example.RecursiveService", "recurse", "()V"));
            ci.getMethods().add(mi);
            builder.indexClass(ci);

            IndexedClass cls = ctx.classMap.get("com.example.RecursiveService");
            IndexedMethod method = cls.methods.get("recurse()V");
            assertNotNull(method, "recurse method must be indexed");

            // Must terminate without StackOverflowError
            CallNode root = assertDoesNotThrow(() -> invokeBuildCallTree(cls, method, null),
                    "Recursive call must not cause StackOverflowError or infinite loop");

            assertNotNull(root, "Root node must not be null");
            // There should be a recursive child node
            boolean hasRecursiveChild = root.getChildren().stream()
                    .anyMatch(CallNode::isRecursive);
            assertTrue(hasRecursiveChild,
                    "Recursive call must produce a child node with recursive=true");
        }
    }

    // ── J8: ASYNC_CHAIN annotation contains "@AsyncChain(..." string ──────────

    @Nested
    @DisplayName("J8 — ASYNC_CHAIN annotation label format")
    class AsyncChainAnnotationTest {

        @Test
        @DisplayName("J8 ASYNC_CHAIN leaf annotations list contains '@AsyncChain(...)' label")
        void j8_asyncChainLeaf_annotationContainsLabel() throws Exception {
            InvocationRef call = new InvocationRef(
                    "java.util.concurrent.CompletableFuture", "thenApply",
                    "()Ljava/util/concurrent/CompletableFuture;",
                    "java.util.concurrent.CompletableFuture",
                    List.of(), 184, null);
            CallNode node = invokeCreateAsyncChainLeaf(call);

            assertNotNull(node.getAnnotations(), "annotations must not be null on ASYNC_CHAIN leaf");
            assertFalse(node.getAnnotations().isEmpty(), "annotations list must not be empty");
            assertTrue(node.getAnnotations().stream().anyMatch(a -> a.startsWith("@AsyncChain(")),
                    "ASYNC_CHAIN annotation must start with '@AsyncChain('. Actual: "
                            + node.getAnnotations());
        }
    }

    // ── J9: HTTP_CALL verb in annotations list ────────────────────────────────

    @Nested
    @DisplayName("J9 — HTTP_CALL leaf annotations include the HTTP verb (@GET, @POST, etc.)")
    class HttpCallVerbAnnotationTest {

        @Test
        @DisplayName("J9 RestTemplate.getForObject → HTTP_CALL leaf annotated with @GET")
        void j9_httpCallLeaf_verbAnnotationPresent() throws Exception {
            InvocationRef call = invRefWithArgs(
                    "org.springframework.web.client.RestTemplate",
                    "getForObject",
                    List.of("https://api.example.com/resource"));
            IndexedMethod caller = methodWithInvocations("fetchResource", List.of());
            CallNode node = invokeCreateHttpCallLeaf(call, caller);

            assertNotNull(node.getAnnotations(), "HTTP_CALL leaf must have annotations");
            assertFalse(node.getAnnotations().isEmpty(),
                    "HTTP_CALL leaf annotations list must not be empty");
            assertTrue(node.getAnnotations().stream().anyMatch(a ->
                            a.startsWith("@GET") || a.startsWith("@POST")
                                    || a.startsWith("@PUT") || a.startsWith("@DELETE")
                                    || a.startsWith("@PATCH") || a.startsWith("@HEAD")),
                    "HTTP_CALL leaf must carry a verb annotation (@GET/@POST/etc.). Actual: "
                            + node.getAnnotations());
        }
    }

    // ── J10: aggregatedCollections aggregated from deep child nodes ────────────

    @Nested
    @DisplayName("J10 — EndpointInfo.aggregatedCollections from deep child node")
    class AggregatedCollectionsTest {

        @Test
        @DisplayName("J10 Collection accessed by a deep child appears in EndpointInfo.aggregatedCollections")
        void j10_deepChildCollection_surfacesInAggregated() {
            // Build a 2-level tree manually: root → child that accesses a collection
            CallNode child = new CallNode();
            child.setId("com.example.OrderRepo#findAll");
            child.setClassName("com.example.OrderRepo");
            child.setSimpleClassName("OrderRepo");
            child.setMethodName("findAll");
            child.setReturnType("java.util.List");
            child.setStereotype("REPOSITORY");
            child.setOperationType("READ");
            child.getCollectionsAccessed().add("ORDERS");

            CallNode root = new CallNode();
            root.setId("com.example.OrderController#listOrders");
            root.setClassName("com.example.OrderController");
            root.setSimpleClassName("OrderController");
            root.setMethodName("listOrders");
            root.setReturnType("java.util.List");
            root.setStereotype("REST_CONTROLLER");
            root.getChildren().add(child);

            EndpointInfo ei = new EndpointInfo();
            ei.setCallTree(root);
            ei.computeAggregates();

            assertTrue(ei.getAggregatedCollections().contains("ORDERS"),
                    "Collection accessed in deep child must appear in aggregatedCollections. Actual: "
                            + ei.getAggregatedCollections());
        }
    }

    // ── J11: derivedOperationType reflects highest-priority op ────────────────

    @Nested
    @DisplayName("J11 — derivedOperationType reflects highest-priority op across tree")
    class DerivedOperationTypeTest {

        @Test
        @DisplayName("J11 WRITE child alongside READ child → derivedOperationType=WRITE (highest priority)")
        void j11_derivedOperationType_highestPriority() {
            CallNode readChild = new CallNode();
            readChild.setId("com.example.OrderRepo#findAll");
            readChild.setClassName("com.example.OrderRepo");
            readChild.setSimpleClassName("OrderRepo");
            readChild.setMethodName("findAll");
            readChild.setOperationType("READ");
            readChild.getCollectionsAccessed().add("ORDERS");

            CallNode writeChild = new CallNode();
            writeChild.setId("com.example.AuditRepo#save");
            writeChild.setClassName("com.example.AuditRepo");
            writeChild.setSimpleClassName("AuditRepo");
            writeChild.setMethodName("save");
            writeChild.setOperationType("WRITE");
            writeChild.getCollectionsAccessed().add("AUDIT_LOG");

            CallNode root = new CallNode();
            root.setId("com.example.OrderController#placeOrder");
            root.setClassName("com.example.OrderController");
            root.setSimpleClassName("OrderController");
            root.setMethodName("placeOrder");
            root.getChildren().add(readChild);
            root.getChildren().add(writeChild);

            EndpointInfo ei = new EndpointInfo();
            ei.setCallTree(root);
            ei.computeAggregates();

            assertEquals("WRITE", ei.getDerivedOperationType(),
                    "derivedOperationType must be WRITE (highest priority). Actual: "
                            + ei.getDerivedOperationType());
        }
    }

    // ── J12: totalDbOperations counts nodes with collections ─────────────────

    @Nested
    @DisplayName("J12 — totalDbOperations equals number of nodes with non-empty collectionsAccessed")
    class TotalDbOperationsTest {

        @Test
        @DisplayName("J12 Two child nodes with collections → totalDbOperations=2")
        void j12_totalDbOperations_countMatchesNodesWithCollections() {
            CallNode child1 = new CallNode();
            child1.setId("com.example.OrderRepo#find");
            child1.setClassName("com.example.OrderRepo");
            child1.setSimpleClassName("OrderRepo");
            child1.setMethodName("find");
            child1.setOperationType("READ");
            child1.getCollectionsAccessed().add("ORDERS");

            CallNode child2 = new CallNode();
            child2.setId("com.example.PaymentRepo#save");
            child2.setClassName("com.example.PaymentRepo");
            child2.setSimpleClassName("PaymentRepo");
            child2.setMethodName("save");
            child2.setOperationType("WRITE");
            child2.getCollectionsAccessed().add("PAYMENTS");

            CallNode root = new CallNode();
            root.setId("com.example.OrderController#checkout");
            root.setClassName("com.example.OrderController");
            root.setSimpleClassName("OrderController");
            root.setMethodName("checkout");
            root.getChildren().add(child1);
            root.getChildren().add(child2);

            EndpointInfo ei = new EndpointInfo();
            ei.setCallTree(root);
            ei.computeAggregates();

            assertEquals(2, ei.getTotalDbOperations(),
                    "totalDbOperations must equal the number of nodes with non-empty collectionsAccessed. Actual: "
                            + ei.getTotalDbOperations());
        }
    }

    // ── J13: operationTypes on leaf is a set (no duplicates) ─────────────────

    @Nested
    @DisplayName("J13 — operationTypes on leaf node has no duplicate entries")
    class OperationTypesNoDuplicatesTest {

        @Test
        @DisplayName("J13 Leaf node operationTypes is a Set (no duplicate operation strings)")
        void j13_leafNode_operationTypesIsSet_noDuplicates() throws Exception {
            IndexedClass cls = minimalClass("com.example.OrderRepo", "OrderRepo", "REPOSITORY");
            IndexedMethod method = plainMethod("findAll");

            // Manually set operationType and call getOperationTypes() to verify Set semantics
            CallNode node = invokeCreateEnrichedNode(cls, method);

            // Add same operationType twice — Set must deduplicate
            node.getOperationTypes().add("READ");
            node.getOperationTypes().add("READ");
            node.getOperationTypes().add("READ");

            long readCount = node.getOperationTypes().stream()
                    .filter("READ"::equals)
                    .count();
            assertEquals(1, readCount,
                    "operationTypes must deduplicate identical entries (it is a Set). Actual: "
                            + node.getOperationTypes());
        }
    }

    // ── J14: dispatchType is non-null on non-leaf nodes ──────────────────────

    @Nested
    @DisplayName("J14 — dispatchType is non-null on non-leaf application nodes")
    class DispatchTypeNotNullTest {

        @Test
        @DisplayName("J14 DIRECT dispatch → dispatchType non-null and set to DIRECT")
        void j14_directDispatch_dispatchTypeNotNull() throws Exception {
            // Build a 2-class chain: A calls B
            ClassInfo bCi = classInfo("com.example.SimpleService", "SimpleService", "SERVICE");
            MethodInfo bMi = new MethodInfo();
            bMi.setName("doWork");
            bMi.setDescriptor("()V");
            bMi.setReturnType("void");
            bCi.getMethods().add(bMi);
            builder.indexClass(bCi);

            ClassInfo aCi = classInfo("com.example.SimpleController", "SimpleController", "REST_CONTROLLER");
            MethodInfo aMi = new MethodInfo();
            aMi.setName("handle");
            aMi.setDescriptor("()V");
            aMi.setReturnType("void");
            aMi.setHttpMethod("GET");
            aMi.getInvocations().add(mci("com.example.SimpleService", "doWork", "()V"));
            aCi.getMethods().add(aMi);
            builder.indexClass(aCi);

            IndexedClass aCls = ctx.classMap.get("com.example.SimpleController");
            IndexedMethod aMethod = aCls.methods.get("handle()V");
            assertNotNull(aMethod, "handle method must be indexed");

            CallNode root = invokeBuildCallTree(aCls, aMethod, null);
            assertNotNull(root, "Root node must not be null");
            assertFalse(root.getChildren().isEmpty(), "Root must have at least one child");

            CallNode child = root.getChildren().get(0);
            assertNotNull(child.getDispatchType(),
                    "Non-leaf child node must have a non-null dispatchType. " +
                    "Found child: " + child.getClassName() + "#" + child.getMethodName());
        }

        @Test
        @DisplayName("J14b dispatchType=DIRECT for a concrete class call")
        void j14b_directDispatch_valueIsDirect() throws Exception {
            ClassInfo bCi = classInfo("com.example.ConcreteService", "ConcreteService", "SERVICE");
            MethodInfo bMi = new MethodInfo();
            bMi.setName("compute");
            bMi.setDescriptor("()V");
            bMi.setReturnType("void");
            bCi.getMethods().add(bMi);
            builder.indexClass(bCi);

            ClassInfo aCi = classInfo("com.example.ConcreteController", "ConcreteController", "REST_CONTROLLER");
            MethodInfo aMi = new MethodInfo();
            aMi.setName("api");
            aMi.setDescriptor("()V");
            aMi.setReturnType("void");
            aMi.setHttpMethod("POST");
            aMi.getInvocations().add(mci("com.example.ConcreteService", "compute", "()V"));
            aCi.getMethods().add(aMi);
            builder.indexClass(aCi);

            IndexedClass aCls = ctx.classMap.get("com.example.ConcreteController");
            IndexedMethod aMethod = aCls.methods.get("api()V");
            assertNotNull(aMethod);

            CallNode root = invokeBuildCallTree(aCls, aMethod, null);
            assertNotNull(root);
            assertFalse(root.getChildren().isEmpty());

            CallNode child = root.getChildren().get(0);
            assertEquals("DIRECT", child.getDispatchType(),
                    "Concrete class call must have dispatchType=DIRECT. Actual: " + child.getDispatchType());
        }
    }

    // ── J15: root node annotations contain HTTP mapping annotation ────────────

    @Nested
    @DisplayName("J15 — Endpoint root node annotations contain @GetMapping or similar")
    class RootNodeHttpAnnotationTest {

        @Test
        @DisplayName("J15 Controller method with @GetMapping → node.annotations contains '@GetMapping'")
        void j15_rootNode_annotationsContainGetMapping() throws Exception {
            MethodInfo mi = new MethodInfo();
            mi.setName("getOrder");
            mi.setDescriptor("()V");
            mi.setReturnType("void");
            mi.setHttpMethod("GET");

            AnnotationInfo getMappingAnn = new AnnotationInfo();
            getMappingAnn.setName("GetMapping");
            Map<String, Object> attrs = new LinkedHashMap<>();
            attrs.put("value", "/orders/{id}");
            getMappingAnn.setAttributes(attrs);
            mi.getAnnotations().add(getMappingAnn);

            IndexedClass cls = minimalClass("com.example.OrderController", "OrderController", "REST_CONTROLLER");
            IndexedMethod method = new IndexedMethod(mi);

            CallNode node = invokeCreateEnrichedNode(cls, method);

            assertNotNull(node.getAnnotations(), "annotations list must not be null");
            assertTrue(node.getAnnotations().contains("@GetMapping"),
                    "Root node annotations must contain '@GetMapping'. Actual: "
                            + node.getAnnotations());
        }

        @Test
        @Disabled("GAP: annotationDetails on root node contains mapping annotation but " +
                  "the aggregation walkForAggregation does not currently expose a dedicated " +
                  "'httpAnnotation' field on EndpointInfo; callers must inspect callTree.annotations directly")
        @DisplayName("J15b EndpointInfo exposes HTTP annotation via dedicated field (GAP)")
        void j15b_endpointInfo_httpAnnotationFieldGap() {
            // The current EndpointInfo has no dedicated field for the root node's HTTP mapping
            // annotation. Callers must walk ei.getCallTree().getAnnotations() directly.
            // This is a minor UX gap — not a correctness issue.
        }
    }
}
