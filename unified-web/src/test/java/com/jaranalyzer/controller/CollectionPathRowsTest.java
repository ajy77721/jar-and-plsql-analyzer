package com.jaranalyzer.controller;

import com.analyzer.storage.page.Page;
import com.analyzer.storage.page.PageRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.plsqlanalyzer.web.page.JsonFacetPager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The Collection popup's single query — PLAN-08 step 11 / Q43 / L4.
 *
 * <p>The failures worth a test are all silent ones. A row set that differs from the browser's
 * fan-out looks like data, not like a bug. A node whose operation cannot be determined dropping
 * out of every popup looks like the analyzer missed it. And a page boundary losing a row only
 * shows up on page 2, so the cursor chain is walked in full and compared against the same rows
 * fetched in one go.
 */
class CollectionPathRowsTest {

    private static final ObjectMapper M = new ObjectMapper();

    /** The analyzer's inference, stubbed to the one behaviour these fixtures depend on. */
    private static final BiFunction<String, String, String> INFER = (method, stereotype) -> {
        if (method == null) return null;
        if ("LOGGING".equals(stereotype) || "JDK".equals(stereotype)) return null;
        String n = method.toLowerCase(Locale.ROOT);
        if (n.startsWith("find") || n.startsWith("get")) {
            boolean dataClass = stereotype == null || "ENTITY".equals(stereotype)
                    || "OTHER".equals(stereotype);
            if (dataClass && n.startsWith("get")) return null;
            return "READ";
        }
        if (n.startsWith("save")) return "WRITE";
        return null;
    };

    /* ── fixtures ────────────────────────────────────────────────────────────────────────── */

    private static ObjectNode node(String cls, String method, String stereotype,
                                   List<String> collections, List<String> ops) {
        ObjectNode n = M.createObjectNode();
        n.put("className", cls);
        n.put("simpleClassName", cls.substring(cls.lastIndexOf('.') + 1));
        n.put("methodName", method);
        if (stereotype != null) n.put("stereotype", stereotype);
        ArrayNode c = n.putArray("collectionsAccessed");
        collections.forEach(c::add);
        if (ops != null) {
            ArrayNode o = n.putArray("operationTypes");
            ops.forEach(o::add);
        }
        return n;
    }

    private static ObjectNode child(ObjectNode parent, ObjectNode kid) {
        if (!parent.has("children")) parent.putArray("children");
        ((ArrayNode) parent.get("children")).add(kid);
        return kid;
    }

    private static ObjectNode endpoint(String method, String path, String controller,
                                       String handler, ObjectNode callTree) {
        ObjectNode ep = M.createObjectNode();
        ep.put("httpMethod", method);
        ep.put("fullPath", path);
        ep.put("controllerClass", controller);
        ep.put("controllerSimpleName", controller.substring(controller.lastIndexOf('.') + 1));
        ep.put("methodName", handler);
        if (callTree != null) ep.set("callTree", callTree);
        return ep;
    }

    /** A document with the same top-level shape as {@code analysis.json}. */
    private static String document(List<ObjectNode> endpoints) {
        ObjectNode root = M.createObjectNode();
        // A large sibling field before and a nested map named "endpoints" after, because the
        // reader has to skip the first and must not mistake the second for the array it wants.
        root.putArray("classes").add(M.createObjectNode().put("padding", "x".repeat(500)));
        ArrayNode arr = root.putArray("endpoints");
        endpoints.forEach(arr::add);
        root.putObject("localVariables").putArray("endpoints").add("decoy");
        return root.toString();
    }

    private static CollectionPathRows open(String json, String collection, String op)
            throws IOException {
        var parser = M.getFactory().createParser(
                new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
        return CollectionPathRows.open(parser, M, collection, op, INFER);
    }

    private static List<JsonNode> all(String json, String collection, String op)
            throws IOException {
        List<JsonNode> out = new ArrayList<>();
        open(json, collection, op).forEachRemaining(out::add);
        return out;
    }

    /* ── row semantics ───────────────────────────────────────────────────────────────────── */

    @Test
    @DisplayName("emits one row per matching node, with the whole root-to-node breadcrumb")
    void breadcrumb() throws IOException {
        ObjectNode root = node("c.OrderController", "list", "REST_CONTROLLER", List.of(), null);
        ObjectNode svc = child(root, node("s.OrderService", "findAll", "SERVICE", List.of(), null));
        child(svc, node("r.OrderRepo", "findAll", "REPOSITORY", List.of("ORDERS"),
                List.of("READ")));
        String doc = document(List.of(
                endpoint("GET", "/orders", "c.OrderController", "list", root)));

        List<JsonNode> rows = all(doc, "ORDERS", "READ");
        assertEquals(1, rows.size());
        JsonNode row = rows.get(0);
        assertEquals("GET /orders c.OrderController#list", row.path("endpointId").asText());
        assertEquals("OrderController.list", row.path("endpointName").asText());
        assertEquals(List.of("OrderController.list", "OrderService.findAll", "OrderRepo.findAll"),
                labels(row));
        assertEquals("READ", row.path("ops").get(0).asText());
    }

    @Test
    @DisplayName("the endpointId is the same four fields, in the same order, as JA.utils.endpointKey")
    void endpointIdentityMatchesTheClientFormula() throws IOException {
        ObjectNode root = node("c.C", "h", "REST_CONTROLLER", List.of("T"), List.of("READ"));
        String doc = document(List.of(endpoint("POST", "/a/{b}", "com.x.C", "h", root)));
        // (httpMethod, fullPath, controllerClass, '#', methodName) — see
        // AbstractAnalysisDataProvider.endpointId and utils.js endpointKey.
        assertEquals("POST /a/{b} com.x.C#h",
                all(doc, "T", "READ").get(0).path("endpointId").asText());
    }

    @Test
    @DisplayName("a node whose operation cannot be determined matches every operation")
    void unknownOperationMatchesEveryOperation() throws IOException {
        // OrderItem.calculateLineTotal in web-1.0.0-SNAPSHOT is exactly this: an ENTITY whose
        // name matches no configured prefix. The client's guard is
        // `if (opsToShow.length && !opsToShow.includes(op)) return`, so an empty list falls
        // through. Filtering it out instead would drop the row from every popup silently.
        ObjectNode root = node("e.OrderItem", "calculateLineTotal", "ENTITY",
                List.of("ORDER_ITEMS"), null);
        String doc = document(List.of(endpoint("GET", "/i", "c.C", "h", root)));

        for (String op : List.of("READ", "WRITE", "DELETE", "AGGREGATE")) {
            assertEquals(1, all(doc, "ORDER_ITEMS", op).size(), op);
            assertEquals(List.of(op), ops(all(doc, "ORDER_ITEMS", op).get(0)),
                    "the badge shows the operation the popup was opened on");
        }
    }

    @Test
    @DisplayName("operationTypes wins outright; operationType and inference are the fallbacks")
    void operationPrecedence() throws IOException {
        ObjectNode many = node("r.R", "findAll", "REPOSITORY", List.of("T"), List.of("AGGREGATE"));
        ObjectNode single = node("r.S", "findAll", "REPOSITORY", List.of("T"), null);
        single.put("operationType", "DELETE");
        ObjectNode inferred = node("r.T", "findAll", "REPOSITORY", List.of("T"), null);
        String doc = document(List.of(
                endpoint("GET", "/1", "c.A", "a", many),
                endpoint("GET", "/2", "c.B", "b", single),
                endpoint("GET", "/3", "c.C", "c", inferred)));

        assertEquals(List.of("A.a"), endpointNames(all(doc, "T", "AGGREGATE")));
        assertEquals(List.of("B.b"), endpointNames(all(doc, "T", "DELETE")));
        assertEquals(List.of("C.c"), endpointNames(all(doc, "T", "READ")));
    }

    @Test
    @DisplayName("rows are de-duplicated per endpoint and target method, keeping the first path")
    void deduplicatesPerEndpointAndTargetMethod() throws IOException {
        ObjectNode root = node("c.C", "h", "REST_CONTROLLER", List.of(), null);
        ObjectNode a = child(root, node("s.A", "one", "SERVICE", List.of(), null));
        ObjectNode b = child(root, node("s.B", "two", "SERVICE", List.of(), null));
        child(a, node("r.R", "findAll", "REPOSITORY", List.of("T"), List.of("READ")));
        child(b, node("r.R", "findAll", "REPOSITORY", List.of("T"), List.of("READ")));
        String doc = document(List.of(endpoint("GET", "/x", "c.C", "h", root)));

        List<JsonNode> rows = all(doc, "T", "READ");
        assertEquals(1, rows.size(), "the same target method reached twice is one row");
        assertEquals(List.of("C.h", "A.one", "R.findAll"), labels(rows.get(0)),
                "and it keeps the first path in walk order");
    }

    @Test
    @DisplayName("a different collection or operation matches nothing")
    void filtersOnBoth() throws IOException {
        ObjectNode root = node("r.R", "findAll", "REPOSITORY", List.of("ORDERS"), List.of("READ"));
        String doc = document(List.of(endpoint("GET", "/x", "c.C", "h", root)));
        assertTrue(all(doc, "ORDERS", "WRITE").isEmpty());
        assertTrue(all(doc, "CUSTOMERS", "READ").isEmpty());
    }

    @Test
    @DisplayName("an endpoint with no call tree yields a synthetic row the client fills in")
    void syntheticRowForATreelessEndpoint() throws IOException {
        // No analysis under data/jar has such an endpoint, so this branch exists only because
        // dropping it would lose rows on one that does — and only a fixture can exercise it.
        ObjectNode ep = endpoint("GET", "/legacy", "c.Legacy", "run", null);
        ep.putArray("aggregatedCollections").add("ORDERS");
        ep.putObject("aggregatedOperations").put("ORDERS", "READ");
        String doc = document(List.of(ep));

        List<JsonNode> rows = all(doc, "ORDERS", "READ");
        assertEquals(1, rows.size());
        assertTrue(rows.get(0).path("synthetic").asBoolean());
        assertTrue(rows.get(0).path("path").isEmpty(), "no path to show; the client builds one");
        assertTrue(all(doc, "ORDERS", "WRITE").isEmpty(), "and the endpoint-level op still filters");
    }

    @Test
    @DisplayName("no endpoints array at all is reported as absent — a 204, not an empty page")
    void absentEndpointsArray() throws IOException {
        assertFalse(open("{\"classes\":[]}", "T", "READ").present());
        assertTrue(open(document(List.of()), "T", "READ").present());
    }

    /* ── paging ──────────────────────────────────────────────────────────────────────────── */

    @Test
    @DisplayName("the full cursor chain returns every row exactly once, in the unpaged order")
    void cursorChainCoversEveryRowOnce() throws IOException {
        List<ObjectNode> endpoints = new ArrayList<>();
        for (int i = 0; i < 37; i++) {
            ObjectNode root = node("c.C" + i, "h", "REST_CONTROLLER", List.of(), null);
            child(root, node("r.R" + i, "findAll", "REPOSITORY", List.of("T"), List.of("READ")));
            endpoints.add(endpoint("GET", "/e" + i, "c.C" + i, "h", root));
        }
        String doc = document(endpoints);

        List<String> unpaged = new ArrayList<>();
        for (JsonNode row : all(doc, "T", "READ")) unpaged.add(row.path("endpointId").asText());
        assertEquals(37, unpaged.size());

        List<String> chained = new ArrayList<>();
        String cursor = null;
        int pages = 0;
        while (true) {
            Page<JsonNode> page = JsonFacetPager.page(
                    open(doc, "T", "READ"),
                    PageRequest.of(5, cursor, "SCAN", null,
                            JarFacetSpecs.COLLECTION_PATHS.sorts().keySet(),
                            JarFacetSpecs.COLLECTION_PATHS.defaultSort(), 50),
                    JarFacetSpecs.COLLECTION_PATHS.sorts(),
                    JarFacetSpecs.COLLECTION_PATHS.id(),
                    JarFacetSpecs.COLLECTION_PATHS.searchText(), null);
            page.items().forEach(r -> chained.add(r.path("endpointId").asText()));
            assertEquals(37, page.page().total());
            assertTrue(page.page().totalIsExact());
            if (!page.page().hasMore()) break;
            cursor = page.page().nextCursor();
            assertTrue(++pages < 50, "cursor chain did not terminate");
        }
        assertEquals(unpaged, chained,
                "the pages, concatenated, are the unpaged rows in the unpaged order");
    }

    @Test
    @DisplayName("server-side search matches the endpoint and the breadcrumb, not just page 1")
    void searchIsServerSide() throws IOException {
        ObjectNode a = node("c.OrderController", "list", "REST_CONTROLLER", List.of("T"),
                List.of("READ"));
        ObjectNode b = node("c.CustomerController", "list", "REST_CONTROLLER", List.of("T"),
                List.of("READ"));
        String doc = document(List.of(
                endpoint("GET", "/orders", "c.OrderController", "list", a),
                endpoint("GET", "/customers", "c.CustomerController", "list", b)));

        assertEquals(List.of("CustomerController.list"), endpointNames(search(doc, "customer")));
        assertEquals(List.of("OrderController.list"), endpointNames(search(doc, "/orders")));
    }

    @Test
    @DisplayName("an undeclared sort is rejected rather than served in a different order")
    void unknownSortIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> PageRequest.of(10, null, "BY_WHATEVER",
                null, JarFacetSpecs.COLLECTION_PATHS.sorts().keySet(),
                JarFacetSpecs.COLLECTION_PATHS.defaultSort(), 50));
    }

    /* ── helpers ─────────────────────────────────────────────────────────────────────────── */

    private static List<JsonNode> search(String doc, String q) throws IOException {
        Page<JsonNode> page = JsonFacetPager.page(open(doc, "T", "READ"),
                PageRequest.of(50, null, "SCAN", q,
                        JarFacetSpecs.COLLECTION_PATHS.sorts().keySet(),
                        JarFacetSpecs.COLLECTION_PATHS.defaultSort(), 50),
                JarFacetSpecs.COLLECTION_PATHS.sorts(), JarFacetSpecs.COLLECTION_PATHS.id(),
                JarFacetSpecs.COLLECTION_PATHS.searchText(), null);
        return page.items();
    }

    private static List<String> labels(JsonNode row) {
        List<String> out = new ArrayList<>();
        row.path("path").forEach(seg -> out.add(seg.path("label").asText()));
        return out;
    }

    private static List<String> ops(JsonNode row) {
        List<String> out = new ArrayList<>();
        row.path("ops").forEach(o -> out.add(o.asText()));
        return out;
    }

    private static List<String> endpointNames(List<JsonNode> rows) {
        List<String> out = new ArrayList<>();
        rows.forEach(r -> out.add(r.path("endpointName").asText()));
        return out;
    }
}
