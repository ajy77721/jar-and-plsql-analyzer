package com.jaranalyzer.service;

import com.jaranalyzer.model.AnnotationInfo;
import com.jaranalyzer.model.ClassInfo;
import com.jaranalyzer.model.MethodInfo;
import com.jaranalyzer.model.ParameterInfo;
import com.jaranalyzer.service.CallGraphIndex.IndexedEndpoint;
import com.jaranalyzer.service.CallGraphIndex.ResolutionContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Category A — Endpoint detection / tagging tests.
 *
 * Strategy: build minimal ClassInfo + MethodInfo objects that carry the same data
 * the real BytecodeClassParser would produce after parsing bytecode, then call
 * CallTreeBuilder.indexClass() and assert on ctx.endpoints.
 *
 * Important: BytecodeClassParser already concatenates basePath + methodPath and
 * stores the combined value in MethodInfo.path, so these tests set that combined
 * value directly and verify it survives intact into IndexedEndpoint.
 *
 * Synthetic listeners (KafkaListener, RabbitListener) are registered in a separate
 * loop inside indexClass; their path comes from the annotation attribute, not
 * MethodInfo.path.
 */
@DisplayName("Category A — Endpoint detection and tagging")
class EndpointTaggingTest {

    // ─── shared infrastructure ───────────────────────────────────────────────────

    private CallTreeBuilder builder;
    private ResolutionContext ctx;

    @BeforeEach
    void setUp() {
        DomainConfigLoader loader = new DomainConfigLoader();
        // CollectionResolver is needed by CallTreeBuilder constructor but not used
        // by indexClass — instantiate with all required detectors.
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

    // ─── factory helpers ─────────────────────────────────────────────────────────

    /**
     * Build a ClassInfo with the minimum fields required for endpoint indexing.
     * sourceJar == null → isMainJar == true in indexClass().
     */
    private static ClassInfo classInfo(String fqn, String simpleName, String stereotype,
                                       List<AnnotationInfo> classAnnotations) {
        ClassInfo ci = new ClassInfo();
        ci.setFullyQualifiedName(fqn);
        ci.setSimpleName(simpleName);
        ci.setStereotype(stereotype);
        ci.setAnnotations(classAnnotations != null ? classAnnotations : List.of());
        ci.setSourceJar(null); // marks as main-JAR class → isMainJar = true
        return ci;
    }

    /**
     * Build a MethodInfo that already carries the combined httpMethod + fullPath,
     * simulating BytecodeClassParser output.
     */
    private static MethodInfo methodInfo(String name, String httpMethod, String fullPath,
                                         List<AnnotationInfo> annotations) {
        MethodInfo mi = new MethodInfo();
        mi.setName(name);
        mi.setDescriptor("()Ljava/lang/Object;");
        mi.setReturnType("java.lang.Object");
        mi.setAccessFlags(0x0001); // ACC_PUBLIC
        mi.setHttpMethod(httpMethod);
        mi.setPath(fullPath);
        mi.setAnnotations(annotations != null ? annotations : List.of());
        return mi;
    }

    /** Build a MethodInfo for a synthetic listener method (KafkaListener / RabbitListener). */
    private static MethodInfo listenerMethodInfo(String name, String annotationName,
                                                 Map<String, Object> annotationAttrs) {
        MethodInfo mi = new MethodInfo();
        mi.setName(name);
        mi.setDescriptor("(Ljava/lang/Object;)V");
        mi.setReturnType("void");
        mi.setAccessFlags(0x0001);
        mi.setHttpMethod(null); // no HTTP method — registered via synthetic listener loop
        mi.setPath(null);
        AnnotationInfo ann = new AnnotationInfo();
        ann.setName(annotationName);
        ann.setAttributes(annotationAttrs);
        mi.setAnnotations(List.of(ann));
        return mi;
    }

    /** Helper: find first endpoint matching the given httpMethod in ctx.endpoints. */
    private IndexedEndpoint findEndpoint(String httpMethod) {
        return ctx.endpoints.stream()
                .filter(ep -> httpMethod.equals(ep.httpMethod))
                .findFirst().orElse(null);
    }

    /** Helper: find first endpoint matching the given httpMethod and path prefix. */
    private IndexedEndpoint findEndpoint(String httpMethod, String path) {
        return ctx.endpoints.stream()
                .filter(ep -> httpMethod.equals(ep.httpMethod) && path.equals(ep.path))
                .findFirst().orElse(null);
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // A1–A5: Basic HTTP verb mappings
    // ─────────────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("A1–A5 — Basic HTTP verb annotation mapping")
    class BasicVerbMapping {

        @Test
        @DisplayName("A1 — @GetMapping produces httpMethod=GET and fullPath=/users/{id}")
        void getMapping() {
            ClassInfo ci = classInfo("com.example.UserController", "UserController",
                    "REST_CONTROLLER", List.of());
            ci.setMethods(List.of(
                    methodInfo("getUser", "GET", "/users/{id}", List.of())
            ));
            builder.indexClass(ci);

            IndexedEndpoint ep = findEndpoint("GET", "/users/{id}");
            assertNotNull(ep, "GET /users/{id} endpoint must be indexed");
            assertEquals("GET", ep.httpMethod);
            assertEquals("/users/{id}", ep.path);
        }

        @Test
        @DisplayName("A2 — @PostMapping produces httpMethod=POST")
        void postMapping() {
            ClassInfo ci = classInfo("com.example.UserController", "UserController",
                    "REST_CONTROLLER", List.of());
            ci.setMethods(List.of(
                    methodInfo("createUser", "POST", "/users", List.of())
            ));
            builder.indexClass(ci);

            IndexedEndpoint ep = findEndpoint("POST");
            assertNotNull(ep, "POST endpoint must be indexed");
            assertEquals("POST", ep.httpMethod);
            assertEquals("/users", ep.path);
        }

        @Test
        @DisplayName("A3 — @PutMapping produces httpMethod=PUT")
        void putMapping() {
            ClassInfo ci = classInfo("com.example.UserController", "UserController",
                    "REST_CONTROLLER", List.of());
            ci.setMethods(List.of(
                    methodInfo("updateUser", "PUT", "/users/{id}", List.of())
            ));
            builder.indexClass(ci);

            IndexedEndpoint ep = findEndpoint("PUT");
            assertNotNull(ep, "PUT endpoint must be indexed");
            assertEquals("PUT", ep.httpMethod);
            assertEquals("/users/{id}", ep.path);
        }

        @Test
        @DisplayName("A4 — @DeleteMapping produces httpMethod=DELETE")
        void deleteMapping() {
            ClassInfo ci = classInfo("com.example.UserController", "UserController",
                    "REST_CONTROLLER", List.of());
            ci.setMethods(List.of(
                    methodInfo("deleteUser", "DELETE", "/users/{id}", List.of())
            ));
            builder.indexClass(ci);

            IndexedEndpoint ep = findEndpoint("DELETE");
            assertNotNull(ep, "DELETE endpoint must be indexed");
            assertEquals("DELETE", ep.httpMethod);
        }

        @Test
        @DisplayName("A5 — @PatchMapping produces httpMethod=PATCH")
        void patchMapping() {
            ClassInfo ci = classInfo("com.example.UserController", "UserController",
                    "REST_CONTROLLER", List.of());
            ci.setMethods(List.of(
                    methodInfo("patchUser", "PATCH", "/users/{id}", List.of())
            ));
            builder.indexClass(ci);

            IndexedEndpoint ep = findEndpoint("PATCH");
            assertNotNull(ep, "PATCH endpoint must be indexed");
            assertEquals("PATCH", ep.httpMethod);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // A6–A7: Path concatenation
    // ─────────────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("A6–A7 — Class-level + method-level path concatenation")
    class PathConcatenation {

        @Test
        @DisplayName("A6 — Class @RequestMapping('/api') + method @GetMapping('/items') → /api/items")
        void classPathPlusGetMappingPath() {
            // BytecodeClassParser combines paths before storing into MethodInfo.
            // We simulate that combined output here: "/api/items".
            ClassInfo ci = classInfo("com.example.ItemController", "ItemController",
                    "REST_CONTROLLER", List.of());
            ci.setMethods(List.of(
                    methodInfo("listItems", "GET", "/api/items", List.of())
            ));
            builder.indexClass(ci);

            IndexedEndpoint ep = findEndpoint("GET", "/api/items");
            assertNotNull(ep, "GET /api/items must be indexed after class+method path concat");
            assertEquals("/api/items", ep.path);
        }

        @Test
        @DisplayName("A7 — Class @RequestMapping('/base') + method @RequestMapping('/sub') → /base/sub")
        void classPathPlusRequestMappingPath() {
            ClassInfo ci = classInfo("com.example.BaseController", "BaseController",
                    "CONTROLLER", List.of());
            ci.setMethods(List.of(
                    methodInfo("subAction", "ALL", "/base/sub", List.of())
            ));
            builder.indexClass(ci);

            IndexedEndpoint ep = findEndpoint("ALL", "/base/sub");
            assertNotNull(ep, "ALL /base/sub endpoint must be indexed");
            assertEquals("/base/sub", ep.path);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // A8–A9: Stereotype detection
    // ─────────────────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("A8–A9 — Controller stereotype detection")
    class StereotypeDetection {

        @Test
        @DisplayName("A8 — @RestController class is indexed (stereotype=REST_CONTROLLER)")
        void restControllerIndexed() {
            ClassInfo ci = classInfo("com.example.UserRestController", "UserRestController",
                    "REST_CONTROLLER", List.of());
            ci.setMethods(List.of(
                    methodInfo("getUser", "GET", "/users/{id}", List.of())
            ));
            builder.indexClass(ci);

            assertEquals(1, ctx.endpoints.size(), "REST_CONTROLLER with HTTP method must produce 1 endpoint");
            assertEquals("com.example.UserRestController", ctx.endpoints.get(0).controllerFqn);
        }

        @Test
        @DisplayName("A9 — @Controller class is indexed (stereotype=CONTROLLER)")
        void controllerIndexed() {
            ClassInfo ci = classInfo("com.example.PageController", "PageController",
                    "CONTROLLER", List.of());
            ci.setMethods(List.of(
                    methodInfo("showPage", "GET", "/page", List.of())
            ));
            builder.indexClass(ci);

            assertEquals(1, ctx.endpoints.size(), "CONTROLLER with HTTP method must produce 1 endpoint");
            assertEquals("CONTROLLER",
                    ctx.classMap.get("com.example.PageController").stereotype);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // A10: No HTTP annotation → no endpoint
    // ─────────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("A10 — Method without HTTP annotation is NOT indexed as endpoint")
    void noHttpAnnotationNotIndexed() {
        ClassInfo ci = classInfo("com.example.UserController", "UserController",
                "REST_CONTROLLER", List.of());
        ci.setMethods(List.of(
                // httpMethod == null → BytecodeClassParser found no HTTP annotation
                methodInfo("helperMethod", null, null, List.of())
        ));
        builder.indexClass(ci);

        assertTrue(ctx.endpoints.isEmpty(),
                "Method without HTTP annotation must NOT be indexed as an endpoint");
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // A11: Multi-variable path preserved
    // ─────────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("A11 — @GetMapping with multiple path variables is preserved verbatim")
    void multiplePathVariables() {
        ClassInfo ci = classInfo("com.example.OrderController", "OrderController",
                "REST_CONTROLLER", List.of());
        String path = "/orders/{orderId}/items/{itemId}";
        ci.setMethods(List.of(
                methodInfo("getOrderItem", "GET", path, List.of())
        ));
        builder.indexClass(ci);

        IndexedEndpoint ep = findEndpoint("GET", path);
        assertNotNull(ep, "Multi-path-variable endpoint must be indexed");
        assertEquals(path, ep.path,
                "Path with multiple path variables must be preserved verbatim");
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // A12: @RequestMapping with method=GET
    // ─────────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("A12 — @RequestMapping(value='/v1/orders', method=GET) → GET /v1/orders")
    void requestMappingWithGetMethod() {
        ClassInfo ci = classInfo("com.example.OrderController", "OrderController",
                "REST_CONTROLLER", List.of());
        ci.setMethods(List.of(
                methodInfo("listOrders", "GET", "/v1/orders", List.of())
        ));
        builder.indexClass(ci);

        IndexedEndpoint ep = findEndpoint("GET", "/v1/orders");
        assertNotNull(ep, "GET /v1/orders endpoint must be indexed");
        assertEquals("GET", ep.httpMethod);
        assertEquals("/v1/orders", ep.path);
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // A13: @GetMapping with multiple paths
    // ─────────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("A13 — @GetMapping({'path1','path2'}) — at least one path produces an endpoint")
    void getmappingMultiplePaths() {
        // BytecodeClassParser takes the first value from the path list; the second path
        // would require a separate MethodInfo registration. We verify at least path1 is indexed.
        ClassInfo ci = classInfo("com.example.SearchController", "SearchController",
                "REST_CONTROLLER", List.of());
        ci.setMethods(List.of(
                methodInfo("search", "GET", "/path1", List.of())
        ));
        builder.indexClass(ci);

        assertFalse(ctx.endpoints.isEmpty(), "At least one path from multi-path @GetMapping must produce an endpoint");
        assertEquals("/path1", ctx.endpoints.get(0).path);
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // A14: @Service class with @GetMapping — implementation-defined behavior
    // ─────────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("A14 — @Service class with @GetMapping IS indexed (impl decision: SERVICE stereotype is allowed)")
    void serviceClassWithGetMappingIsIndexed() {
        // CallTreeBuilder.indexClass() allows SERVICE stereotype for endpoint indexing.
        // This test documents that behavior — a @Service with HTTP-mapped methods is indexed.
        ClassInfo ci = classInfo("com.example.OrderService", "OrderService",
                "SERVICE", List.of());
        ci.setMethods(List.of(
                methodInfo("getOrders", "GET", "/orders", List.of())
        ));
        builder.indexClass(ci);

        assertEquals(1, ctx.endpoints.size(),
                "SERVICE class with HTTP-mapped method IS indexed (implementation allows SERVICE stereotype)");
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // A15: Inherited endpoint — concrete child picks up parent path
    // ─────────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("A15 — Concrete child inheriting parent path is indexed with its own methods")
    void concreteChildEndpointIndexed() {
        // A concrete child class with its own HTTP-mapped method is indexed normally.
        // (Inheritance-level path merging is done by BytecodeClassParser, not CallTreeBuilder.)
        ClassInfo ci = classInfo("com.example.ConcreteController", "ConcreteController",
                "REST_CONTROLLER", List.of());
        // Simulating: abstract parent had @GetMapping("/items"), child inherits path "/api/items"
        ci.setMethods(List.of(
                methodInfo("listItems", "GET", "/api/items", List.of())
        ));
        builder.indexClass(ci);

        IndexedEndpoint ep = findEndpoint("GET", "/api/items");
        assertNotNull(ep, "Concrete child's HTTP method must be indexed");
        assertEquals("com.example.ConcreteController", ep.controllerFqn);
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // A16: @GetMapping with produces attribute
    // ─────────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("A16 — @GetMapping with produces attribute creates endpoint normally")
    void getmappingWithProduces() {
        ClassInfo ci = classInfo("com.example.UserController", "UserController",
                "REST_CONTROLLER", List.of());
        ci.setMethods(List.of(
                methodInfo("getUser", "GET", "/users/{id}", List.of())
        ));
        builder.indexClass(ci);

        // produces attribute does not affect path/httpMethod — endpoint is created as normal
        IndexedEndpoint ep = findEndpoint("GET", "/users/{id}");
        assertNotNull(ep, "Endpoint with produces attribute must still be indexed");
        assertEquals("GET", ep.httpMethod);
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // A17: Private method access flags
    // ─────────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("A17 — @GetMapping on private method — endpoint is indexed (access flags not checked by indexClass)")
    void getmappingOnPrivateMethod() {
        ClassInfo ci = classInfo("com.example.UserController", "UserController",
                "REST_CONTROLLER", List.of());
        MethodInfo mi = methodInfo("privateEndpoint", "GET", "/private", List.of());
        mi.setAccessFlags(0x0002); // ACC_PRIVATE
        ci.setMethods(List.of(mi));
        builder.indexClass(ci);

        // indexClass does not filter by access flags — it only checks httpMethod != null
        assertFalse(ctx.endpoints.isEmpty(),
                "indexClass does not filter by access flags — private @GetMapping is indexed");
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // A18: @EventListener method — not an HTTP endpoint
    // ─────────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("A18 — @EventListener method is NOT indexed as HTTP endpoint")
    void eventListenerNotAnEndpoint() {
        ClassInfo ci = classInfo("com.example.OrderEventHandler", "OrderEventHandler",
                "SERVICE", List.of());
        // EventListener methods have no httpMethod in MethodInfo
        MethodInfo mi = new MethodInfo();
        mi.setName("handleOrderCreated");
        mi.setDescriptor("(Lcom/example/OrderCreatedEvent;)V");
        mi.setReturnType("void");
        mi.setAccessFlags(0x0001);
        mi.setHttpMethod(null); // not an HTTP endpoint
        AnnotationInfo ann = new AnnotationInfo();
        ann.setName("EventListener");
        ann.setAttributes(Map.of());
        mi.setAnnotations(List.of(ann));
        // The fixture must populate parameters, not just the descriptor.
        //
        // CallTreeBuilder resolves the event type from the annotation's value/classes and, failing
        // that, from the first parameter — which is the normal case, since a bare @EventListener
        // carries no attributes. Real bytecode indexing fills ParameterInfo in; this fixture did
        // not, so the fallback found nothing and the path came out "/event/unknown". That was the
        // fixture being unrealistic, not the resolver being wrong.
        ParameterInfo param = new ParameterInfo();
        param.setName("event");
        param.setType("com.example.OrderCreatedEvent");
        mi.setParameters(List.of(param));
        ci.setMethods(List.of(mi));
        builder.indexClass(ci);

        // The design changed, and this test had not caught up.
        //
        // It asserted that an @EventListener never reaches ctx.endpoints, with the note "(they
        // appear in ctx.eventListenerMap instead)". CallTreeBuilder now deliberately synthesises a
        // pseudo-endpoint for one, with a readable path derived from the event type, so a listener's
        // call tree can be explored the same way an HTTP endpoint's can. That is intentional — the
        // code names the variable syntheticHttp and computes "/event/<EventType>" — not an accident
        // of a null httpMethod slipping through: the real HTTP path is still guarded by
        // `if (mi.getHttpMethod() != null)`.
        //
        // So the property worth holding is not "absent" but "present and clearly marked as
        // synthetic", which is what stops it being mistaken for a real HTTP route.
        assertEquals(1, ctx.endpoints.size(),
                "an @EventListener should surface as exactly one synthetic endpoint");
        var ep = ctx.endpoints.get(0);
        assertTrue(ep.path != null && ep.path.startsWith("/event/"),
                "a synthetic listener endpoint must carry an /event/ path so it is not mistaken "
                        + "for a real HTTP route; was " + ep.path);
        assertTrue(ep.path.contains("OrderCreatedEvent"),
                "the path should name the event type, was " + ep.path);
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // A19: @KafkaListener
    // ─────────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("A19 — @KafkaListener(topics='orders') → syntheticHttp=KAFKA, path='orders'")
    void kafkaListenerIndexed() {
        ClassInfo ci = classInfo("com.example.OrderConsumer", "OrderConsumer",
                "SERVICE", List.of());
        MethodInfo mi = listenerMethodInfo("consumeOrder", "KafkaListener",
                Map.of("topics", "orders"));
        ci.setMethods(List.of(mi));
        builder.indexClass(ci);

        IndexedEndpoint ep = ctx.endpoints.stream()
                .filter(e -> "KAFKA".equals(e.httpMethod))
                .findFirst().orElse(null);
        assertNotNull(ep, "@KafkaListener must produce a KAFKA synthetic endpoint");
        assertEquals("KAFKA", ep.httpMethod);
        assertEquals("orders", ep.path);
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // A20: @RabbitListener
    // ─────────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("A20 — @RabbitListener(queues='payments') → syntheticHttp=AMQP, path='payments'")
    void rabbitListenerIndexed() {
        ClassInfo ci = classInfo("com.example.PaymentConsumer", "PaymentConsumer",
                "SERVICE", List.of());
        MethodInfo mi = listenerMethodInfo("processPayment", "RabbitListener",
                Map.of("queues", "payments"));
        ci.setMethods(List.of(mi));
        builder.indexClass(ci);

        IndexedEndpoint ep = ctx.endpoints.stream()
                .filter(e -> "AMQP".equals(e.httpMethod))
                .findFirst().orElse(null);
        assertNotNull(ep, "@RabbitListener must produce an AMQP synthetic endpoint");
        assertEquals("AMQP", ep.httpMethod);
        assertEquals("payments", ep.path);
    }
}
