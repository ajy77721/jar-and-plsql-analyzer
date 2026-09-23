package com.jaranalyzer.service;

import com.jaranalyzer.model.CallNode;
import com.jaranalyzer.model.EndpointInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Edge-case tests for EndpointInfo.computeAggregates() — the collection/operation
 * aggregation pass that walks the call tree and populates:
 *   • aggregatedCollections
 *   • aggregatedOperations  (collection → highest-priority op)
 *   • totalDbOperations
 *   • derivedOperationType
 *
 * All tests use hand-built CallNode trees (no Spring, no bytecode, no DB).
 * Priority order (ascending): COUNT < AGGREGATE < READ < DELETE < UPDATE < WRITE
 */
@DisplayName("EndpointInfo.computeAggregates — collection aggregation edge cases")
class CollectionAggregationEdgeCaseTest {

    // ─── helpers ─────────────────────────────────────────────────────────────

    /** Minimal root node (controller method) with no collections. */
    private static CallNode rootNode(String className, String method) {
        CallNode n = new CallNode();
        n.setId(className + "#" + method);
        n.setClassName(className);
        n.setSimpleClassName(simpleName(className));
        n.setMethodName(method);
        n.setCallType("APPLICATION");
        return n;
    }

    /** Node with a single collection and operation type. */
    private static CallNode dbNode(String className, String method, String collection, String opType) {
        CallNode n = new CallNode();
        n.setId(className + "#" + method);
        n.setClassName(className);
        n.setSimpleClassName(simpleName(className));
        n.setMethodName(method);
        n.setStereotype("REPOSITORY");
        n.setCallType("APPLICATION");
        n.setOperationType(opType);
        n.getOperationTypes().add(opType);
        n.getCollectionsAccessed().add(collection);
        return n;
    }

    /** Node with no collections (service/utility layer). */
    private static CallNode serviceNode(String className, String method) {
        CallNode n = new CallNode();
        n.setId(className + "#" + method);
        n.setClassName(className);
        n.setSimpleClassName(simpleName(className));
        n.setMethodName(method);
        n.setCallType("APPLICATION");
        return n;
    }

    /** HTTP_CALL leaf node (external HTTP call — should not count as DB op). */
    private static CallNode httpNode(String className, String method) {
        CallNode n = new CallNode();
        n.setId("http:" + className + "#" + method);
        n.setClassName(className);
        n.setSimpleClassName(simpleName(className));
        n.setMethodName(method);
        n.setCallType("HTTP_CALL");
        n.setCrossModule(true);
        return n;
    }

    /** ASYNC_CHAIN leaf node. */
    private static CallNode asyncChainNode(String ownerClass, String method) {
        CallNode n = new CallNode();
        n.setId("async-chain:" + simpleName(ownerClass) + "#" + method);
        n.setClassName(ownerClass);
        n.setSimpleClassName(simpleName(ownerClass));
        n.setMethodName(method);
        n.setCallType("ASYNC_CHAIN");
        // setAsyncChainOp was removed from CallNode along with the rest of the async-boundary
        // feature (nothing in any main source references async any more). The callType label is
        // kept because this test's actual subject survives it: a leaf carrying no collection must
        // not increment totalDbOperations, whatever kind of leaf it is.
        n.setCrossModule(true);
        return n;
    }

    /** Cross-module node with a collection. */
    private static CallNode crossModuleDbNode(String className, String method,
                                               String collection, String opType, String jar) {
        CallNode n = dbNode(className, method, collection, opType);
        n.setSourceJar(jar);
        n.setCrossModule(true);
        n.setCallType("CROSS_MODULE");
        return n;
    }

    private static EndpointInfo endpointWith(CallNode root) {
        EndpointInfo ep = new EndpointInfo();
        ep.setHttpMethod("GET");
        ep.setFullPath("/test");
        ep.setCallTree(root);
        ep.computeAggregates();
        return ep;
    }

    private static String simpleName(String fqn) {
        int dot = fqn.lastIndexOf('.');
        return dot >= 0 ? fqn.substring(dot + 1) : fqn;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 1: DB call at depth 5 → aggregatedCollections populated
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("T1 – DB call at depth 5 → collection appears in aggregatedCollections")
    void dbCallAtDepth5Aggregated() {
        CallNode root = rootNode("com.example.OrderController", "getOrder");
        CallNode d1   = serviceNode("com.example.OrderService",    "getOrder");
        CallNode d2   = serviceNode("com.example.OrderManager",    "load");
        CallNode d3   = serviceNode("com.example.OrderValidator",  "validate");
        CallNode d4   = serviceNode("com.example.OrderHelper",     "fetch");
        CallNode d5   = dbNode("com.example.OrderRepository", "findById", "ORDER_COL", "READ");

        root.getChildren().add(d1);
        d1.getChildren().add(d2);
        d2.getChildren().add(d3);
        d3.getChildren().add(d4);
        d4.getChildren().add(d5);

        EndpointInfo ep = endpointWith(root);

        assertTrue(ep.getAggregatedCollections().contains("ORDER_COL"),
                "Collection at depth-5 node must appear in aggregatedCollections");
        assertTrue(ep.getTotalDbOperations() >= 1,
                "totalDbOperations must count the depth-5 DB node");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 2: Same collection READ and WRITE → aggregatedOperations shows WRITE
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("T2 – Same collection READ + WRITE → aggregatedOperations shows highest priority (WRITE)")
    void sameCollectionReadAndWriteHighestPriority() {
        CallNode root  = rootNode("com.example.OrderController", "processOrder");
        CallNode read  = dbNode("com.example.OrderRepo",  "findById", "ORDER_COL", "READ");
        CallNode write = dbNode("com.example.OrderRepo",  "save",     "ORDER_COL", "WRITE");

        root.getChildren().add(read);
        root.getChildren().add(write);

        EndpointInfo ep = endpointWith(root);

        assertEquals("WRITE", ep.getAggregatedOperations().get("ORDER_COL"),
                "WRITE must win over READ for the same collection (higher priority)");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 3: WRITE > UPDATE > DELETE > READ priority ordering
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("T3 – Priority: WRITE > UPDATE > DELETE > READ on a single collection")
    void operationPriorityOrdering() {
        // Four sibling nodes on the same collection — WRITE must win
        CallNode root   = rootNode("com.example.Controller", "combo");
        CallNode read   = dbNode("com.example.Repo", "findAll",     "TEST_COL", "READ");
        CallNode delete = dbNode("com.example.Repo", "deleteById",  "TEST_COL", "DELETE");
        CallNode update = dbNode("com.example.Repo", "updateStatus","TEST_COL", "UPDATE");
        CallNode write  = dbNode("com.example.Repo", "save",        "TEST_COL", "WRITE");

        // Use distinct identity objects — visited uses IdentityHashMap
        root.getChildren().addAll(List.of(read, delete, update, write));

        EndpointInfo ep = endpointWith(root);

        assertEquals("WRITE", ep.getAggregatedOperations().get("TEST_COL"),
                "WRITE must be selected as highest priority operation");
        assertEquals("WRITE", ep.getDerivedOperationType(),
                "derivedOperationType must also be WRITE");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 4: 50 different collections → all appear in aggregatedCollections
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("T4 – 50 different collections in one tree → all appear in aggregatedCollections")
    void fiftyCollectionsAllAggregated() {
        CallNode root = rootNode("com.example.BulkController", "processAll");

        List<String> expected = new ArrayList<>();
        for (int i = 1; i <= 50; i++) {
            String coll = "COLL_" + String.format("%02d", i);
            expected.add(coll);
            // Each on its own sibling node (unique identity objects for IdentityHashMap)
            CallNode node = dbNode("com.example.Repo" + i, "findAll", coll, "READ");
            root.getChildren().add(node);
        }

        EndpointInfo ep = endpointWith(root);

        assertEquals(50, ep.getAggregatedCollections().size(),
                "All 50 collections must appear in aggregatedCollections");
        for (String coll : expected) {
            assertTrue(ep.getAggregatedCollections().contains(coll),
                    coll + " must be in aggregatedCollections");
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 5: Same collection accessed 3 times → deduplicated to one entry
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("T5 – Same collection accessed 3 times (different nodes) → one entry in aggregatedCollections")
    void sameCollectionDeduplicatedInAggregated() {
        CallNode root = rootNode("com.example.Controller", "doWork");

        // Three distinct nodes that all touch ORDER_COL
        root.getChildren().add(dbNode("com.example.RepoA", "findAll", "ORDER_COL", "READ"));
        root.getChildren().add(dbNode("com.example.RepoB", "findOne", "ORDER_COL", "READ"));
        root.getChildren().add(dbNode("com.example.RepoC", "count",   "ORDER_COL", "COUNT"));

        EndpointInfo ep = endpointWith(root);

        long orderColCount = ep.getAggregatedCollections().stream()
                .filter("ORDER_COL"::equals).count();
        assertEquals(1, orderColCount,
                "ORDER_COL must appear exactly once in aggregatedCollections despite 3 accesses");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 6: HTTP_CALL leaf → not counted in totalDbOperations
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("T6 – HTTP_CALL leaf → not counted in totalDbOperations")
    void httpCallLeafNotCountedAsDbOp() {
        CallNode root = rootNode("com.example.OrderController", "placeOrder");
        CallNode http = httpNode("com.example.PaymentClient", "chargeCard");

        root.getChildren().add(http);

        EndpointInfo ep = endpointWith(root);

        assertEquals(0, ep.getTotalDbOperations(),
                "HTTP_CALL leaf must not increment totalDbOperations");
        assertTrue(ep.getAggregatedCollections().isEmpty(),
                "No collection must be aggregated from HTTP_CALL leaf");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 7: ASYNC_CHAIN leaf → not counted in totalDbOperations
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("T7 – ASYNC_CHAIN leaf → not counted in totalDbOperations")
    void asyncChainLeafNotCountedAsDbOp() {
        CallNode root  = rootNode("com.example.OrderService", "process");
        CallNode async = asyncChainNode("java.util.concurrent.CompletableFuture", "thenApply");

        root.getChildren().add(async);

        EndpointInfo ep = endpointWith(root);

        assertEquals(0, ep.getTotalDbOperations(),
                "ASYNC_CHAIN leaf must not increment totalDbOperations");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 8: Recursive node → not double-counted
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("T8 – Recursive node (self-reference) → visited guard prevents double-counting")
    void recursiveNodeNotDoubleCounted() {
        CallNode root = rootNode("com.example.TreeController", "get");
        CallNode db   = dbNode("com.example.TreeRepo", "findByParent", "TREE_COL", "READ");

        // Simulate a recursive child that points back to root (same object identity)
        CallNode recursive = new CallNode();
        recursive.setId(root.getId());
        recursive.setClassName(root.getClassName());
        recursive.setMethodName(root.getMethodName());
        recursive.setRecursive(true);
        recursive.setCallType("RECURSIVE");
        // Recursive node has same collection — would cause double-count if visited guard fails
        recursive.getCollectionsAccessed().add("TREE_COL");

        root.getChildren().add(db);
        root.getChildren().add(recursive);

        EndpointInfo ep = endpointWith(root);

        // TREE_COL must appear exactly once in aggregatedCollections
        long count = ep.getAggregatedCollections().stream()
                .filter("TREE_COL"::equals).count();
        assertEquals(1, count, "TREE_COL must appear exactly once despite recursive node");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 9: Cross-module node with collection → included in aggregation
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("T9 – Cross-module node with collection → included in aggregatedCollections")
    void crossModuleNodeCollectionAggregated() {
        CallNode root   = rootNode("com.main.Controller", "submitOrder");
        CallNode cross  = crossModuleDbNode("com.shared.OrderRepo", "save",
                "SHARED_COL", "WRITE", "shared-lib.jar");

        root.getChildren().add(cross);

        EndpointInfo ep = endpointWith(root);

        assertTrue(ep.getAggregatedCollections().contains("SHARED_COL"),
                "Cross-module node's collection must be included in aggregatedCollections");
        assertEquals("WRITE", ep.getAggregatedOperations().get("SHARED_COL"),
                "WRITE operation from cross-module node must appear in aggregatedOperations");
        assertTrue(ep.getTotalDbOperations() >= 1,
                "Cross-module DB node must increment totalDbOperations");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 10: Empty tree (no DB calls) → totalDbOperations=0, aggregatedCollections empty
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("T10 – Empty tree (no DB calls) → totalDbOperations=0, aggregatedCollections empty")
    void emptyTreeZeroDbOps() {
        CallNode root = rootNode("com.example.HealthController", "ping");
        // No children, no collections

        EndpointInfo ep = endpointWith(root);

        assertEquals(0, ep.getTotalDbOperations(),
                "Empty tree must have totalDbOperations=0");
        assertTrue(ep.getAggregatedCollections().isEmpty(),
                "Empty tree must have empty aggregatedCollections");
        assertNull(ep.getDerivedOperationType(),
                "Empty tree must have null derivedOperationType");
    }
}
