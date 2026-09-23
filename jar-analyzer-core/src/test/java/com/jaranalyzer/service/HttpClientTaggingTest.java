package com.jaranalyzer.service;

import com.jaranalyzer.model.CallNode;
import com.jaranalyzer.service.CallGraphIndex.IndexedClass;
import com.jaranalyzer.service.CallGraphIndex.IndexedMethod;
import com.jaranalyzer.service.CallGraphIndex.InvocationRef;
import com.jaranalyzer.service.CallGraphIndex.ResolutionContext;
import com.jaranalyzer.model.ClassInfo;
import com.jaranalyzer.model.MethodInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
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
 * Category E — Outbound HTTP Client Tests (20 tests)
 *
 * Tests verify that HTTP client calls produce HTTP_CALL leaf nodes with correct
 * callType, operationType, crossModule, annotations, and stringLiterals.
 *
 * Strategy: drive CallTreeBuilder directly at the unit level using the same
 * InvocationRef / IndexedMethod stubs pattern established in FalsePositiveDetectionTest.
 * The builder's createHttpCallLeaf / createFeignCallLeaf paths are exercised by feeding
 * synthetic invocations through buildCallTree on a minimal root method.
 */
@DisplayName("Category E — Outbound HTTP Client tagging")
class HttpClientTaggingTest {

    // ── Reflection helpers ────────────────────────────────────────────────────

    /**
     * Invoke the private createHttpCallLeaf(InvocationRef, IndexedMethod) method.
     */
    private CallNode invokeCreateHttpCallLeaf(CallTreeBuilder builder,
                                               InvocationRef call,
                                               IndexedMethod callerMethod) throws Exception {
        Method m = CallTreeBuilder.class.getDeclaredMethod(
                "createHttpCallLeaf", InvocationRef.class, IndexedMethod.class);
        m.setAccessible(true);
        return (CallNode) m.invoke(builder, call, callerMethod);
    }

    /**
     * Invoke the private createFeignCallLeaf(IndexedClass, InvocationRef, IndexedMethod) method.
     */
    private CallNode invokeCreateFeignCallLeaf(CallTreeBuilder builder,
                                                IndexedClass feignClass,
                                                InvocationRef call,
                                                IndexedMethod callerMethod) throws Exception {
        Method m = CallTreeBuilder.class.getDeclaredMethod(
                "createFeignCallLeaf", IndexedClass.class, InvocationRef.class, IndexedMethod.class);
        m.setAccessible(true);
        return (CallNode) m.invoke(builder, feignClass, call, callerMethod);
    }

    /**
     * Invoke the private inferHttpOperation(String) method.
     */
    private String invokeInferHttpOperation(CallTreeBuilder builder, String methodName) throws Exception {
        Method m = CallTreeBuilder.class.getDeclaredMethod("inferHttpOperation", String.class);
        m.setAccessible(true);
        return (String) m.invoke(builder, methodName);
    }

    // ── Stub builders ─────────────────────────────────────────────────────────

    private static IndexedClass minimalClass(String fqn, String simpleName, String stereotype) {
        ClassInfo ci = new ClassInfo();
        ci.setFullyQualifiedName(fqn);
        ci.setSimpleName(simpleName);
        ci.setStereotype(stereotype);
        return new IndexedClass(ci, Pattern.compile("(?!x)x"));
    }

    private static IndexedMethod minimalMethod(String name, List<InvocationRef> invocations,
                                                List<String> stringLiterals) {
        MethodInfo mi = new MethodInfo();
        mi.setName(name);
        mi.setDescriptor("()V");
        mi.setReturnType("void");
        IndexedMethod im = new IndexedMethod(mi);
        im.invocations.clear();
        im.invocations.addAll(invocations);
        // String literals are final in IndexedMethod, so we patch via reflection
        try {
            Field f = IndexedMethod.class.getDeclaredField("stringLiterals");
            f.setAccessible(true);
            // The field is final — we need to replace the list reference; Java allows this
            // on non-static finals via reflection.
            f.set(im, stringLiterals);
        } catch (Exception e) {
            throw new RuntimeException("Could not set stringLiterals on IndexedMethod", e);
        }
        return im;
    }

    private static IndexedMethod minimalMethod(String name, List<InvocationRef> invocations) {
        return minimalMethod(name, invocations, List.of());
    }

    /** InvocationRef for an HTTP client call (opcode 182 = INVOKEVIRTUAL). */
    private static InvocationRef httpInvRef(String ownerClass, String methodName,
                                             List<String> stringArgs) {
        return new InvocationRef(ownerClass, methodName, "()Ljava/lang/Object;", "java.lang.Object",
                stringArgs, 182, null);
    }

    // ── CallTreeBuilder factory ───────────────────────────────────────────────

    private CallTreeBuilder builder;

    @BeforeEach
    void setUp() {
        DomainConfigLoader loader = new DomainConfigLoader();
        // CollectionResolver is needed by the builder constructor but not exercised here.
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
        // Bootstrap a ResolutionContext so createHttpCallLeaf doesn't NPE
        builder.createContext();
    }

    // ── E1: RestTemplate.getForObject ─────────────────────────────────────────

    @Nested
    @DisplayName("E1-E4 — RestTemplate CRUD methods")
    class RestTemplateCrudMethods {

        @Test
        @DisplayName("E1 getForObject → HTTP_CALL, operationType=READ")
        void getForObject_producesReadHttpCall() throws Exception {
            InvocationRef call = httpInvRef(
                    "org.springframework.web.client.RestTemplate",
                    "getForObject",
                    List.of("http://api/users/{id}")
            );
            IndexedMethod caller = minimalMethod("fetchUser", List.of(call));
            CallNode node = invokeCreateHttpCallLeaf(builder, call, caller);

            assertEquals("HTTP_CALL", node.getCallType(), "callType must be HTTP_CALL");
            assertEquals("READ", node.getOperationType(), "getForObject → READ");
        }

        @Test
        @DisplayName("E2 postForObject → HTTP_CALL, operationType=WRITE")
        void postForObject_producesWriteHttpCall() throws Exception {
            InvocationRef call = httpInvRef(
                    "org.springframework.web.client.RestTemplate",
                    "postForObject",
                    List.of("http://api/users")
            );
            IndexedMethod caller = minimalMethod("createUser", List.of(call));
            CallNode node = invokeCreateHttpCallLeaf(builder, call, caller);

            assertEquals("HTTP_CALL", node.getCallType());
            assertEquals("WRITE", node.getOperationType(), "postForObject → WRITE");
        }

        @Test
        @DisplayName("E3 put → HTTP_CALL, operationType=WRITE (impl groups put with post/save/create)")
        void put_producesWriteHttpCall() throws Exception {
            // Note: inferHttpOperation maps "put" → WRITE (grouped with post/save/create),
            // not UPDATE (reserved for "patch"/"update" method names).
            // This reflects the actual implementation in CallTreeBuilder.inferHttpOperation.
            InvocationRef call = httpInvRef(
                    "org.springframework.web.client.RestTemplate",
                    "put",
                    List.of("http://api/users/{id}")
            );
            IndexedMethod caller = minimalMethod("updateUser", List.of(call));
            CallNode node = invokeCreateHttpCallLeaf(builder, call, caller);

            assertEquals("HTTP_CALL", node.getCallType());
            // The mapping was refined, and this assertion predates it. PUT was once lumped in with
            // post/save/create as a generic WRITE; inferHttpOperationFromMethod now distinguishes
            // POST -> WRITE, PUT/PATCH -> UPDATE, DELETE -> DELETE. That is a more useful answer on
            // a screen reporting what an endpoint does to a resource, so the test follows the code.
            assertEquals("UPDATE", node.getOperationType(),
                    "PUT is an update, distinct from POST's create/write");
        }

        @Test
        @DisplayName("E4 delete → HTTP_CALL, operationType=DELETE")
        void delete_producesDeleteHttpCall() throws Exception {
            InvocationRef call = httpInvRef(
                    "org.springframework.web.client.RestTemplate",
                    "delete",
                    List.of("http://api/users/{id}")
            );
            IndexedMethod caller = minimalMethod("deleteUser", List.of(call));
            CallNode node = invokeCreateHttpCallLeaf(builder, call, caller);

            assertEquals("HTTP_CALL", node.getCallType());
            assertEquals("DELETE", node.getOperationType(), "delete → DELETE");
        }

        @Test
        @DisplayName("E5 getForEntity → HTTP_CALL")
        void getForEntity_producesHttpCall() throws Exception {
            InvocationRef call = httpInvRef(
                    "org.springframework.web.client.RestTemplate",
                    "getForEntity",
                    List.of("http://api/orders")
            );
            IndexedMethod caller = minimalMethod("listOrders", List.of(call));
            CallNode node = invokeCreateHttpCallLeaf(builder, call, caller);

            assertEquals("HTTP_CALL", node.getCallType(), "getForEntity must be HTTP_CALL");
        }
    }

    // ── E6-E8: WebClient ──────────────────────────────────────────────────────

    @Nested
    @DisplayName("E6-E8 — WebClient fluent API")
    class WebClientMethods {

        @Test
        @DisplayName("E6 WebClient.get().uri() → HTTP_CALL")
        void webClientGet_producesHttpCall() throws Exception {
            // The bytecode will show a call to the method "get" on WebClient;
            // uri is a separate call — we test the "get" verb first.
            InvocationRef call = httpInvRef(
                    "org.springframework.web.reactive.function.client.WebClient",
                    "get",
                    List.of("/users")
            );
            IndexedMethod caller = minimalMethod("fetchUsers", List.of(call));
            CallNode node = invokeCreateHttpCallLeaf(builder, call, caller);

            assertEquals("HTTP_CALL", node.getCallType());
        }

        @Test
        @DisplayName("E7 WebClient.post().uri() → HTTP_CALL, operationType=WRITE")
        void webClientPost_producesWriteHttpCall() throws Exception {
            InvocationRef call = httpInvRef(
                    "org.springframework.web.reactive.function.client.WebClient",
                    "post",
                    List.of("/users")
            );
            IndexedMethod caller = minimalMethod("createUser", List.of(call));
            CallNode node = invokeCreateHttpCallLeaf(builder, call, caller);

            assertEquals("HTTP_CALL", node.getCallType());
            assertEquals("WRITE", node.getOperationType(), "post → WRITE");
        }

        @Test
        @DisplayName("E8 WebClient.delete() → HTTP_CALL, operationType=DELETE")
        void webClientDelete_producesDeleteHttpCall() throws Exception {
            InvocationRef call = httpInvRef(
                    "org.springframework.web.reactive.function.client.WebClient",
                    "delete",
                    List.of("/users/{id}")
            );
            IndexedMethod caller = minimalMethod("deleteUser", List.of(call));
            CallNode node = invokeCreateHttpCallLeaf(builder, call, caller);

            assertEquals("HTTP_CALL", node.getCallType());
            assertEquals("DELETE", node.getOperationType(), "delete → DELETE");
        }
    }

    // ── E9-E12: Feign / HTTP Exchange ─────────────────────────────────────────

    @Nested
    @DisplayName("E9-E12 — FeignClient and Spring 6 HTTP Exchange")
    class FeignAndExchangeMethods {

        /**
         * Build a minimal IndexedClass pretending to be a @FeignClient interface.
         * We set stereotype = "HTTP_CLIENT" and register a method with httpMethod/path
         * so createFeignCallLeaf can extract the annotation and path.
         */
        private IndexedClass feignInterface(String fqn, String simpleName,
                                             String methodName, String httpVerb, String path) {
            ClassInfo ci = new ClassInfo();
            ci.setFullyQualifiedName(fqn);
            ci.setSimpleName(simpleName);
            ci.setStereotype("HTTP_CLIENT");
            ci.setIsInterface(true);

            MethodInfo mi = new MethodInfo();
            mi.setName(methodName);
            mi.setDescriptor("(Ljava/lang/Long;)Ljava/lang/Object;");
            mi.setReturnType("java.lang.Object");
            mi.setHttpMethod(httpVerb);
            mi.setPath(path);

            ci.getMethods().add(mi);

            IndexedClass ic = new IndexedClass(ci, Pattern.compile("(?!x)x"));
            ic.methods.put(methodName + "(Ljava/lang/Long;)Ljava/lang/Object;",
                    new IndexedMethod(mi));
            return ic;
        }

        @Test
        @DisplayName("E9 @FeignClient @GetMapping → HTTP_CALL, annotation @GET, path in stringLiterals")
        void feignGetMapping_producesHttpCallWithGetAnnotationAndPath() throws Exception {
            IndexedClass feign = feignInterface(
                    "com.example.client.UserClient", "UserClient",
                    "getUser", "GET", "/users/{id}");

            InvocationRef call = new InvocationRef(
                    "com.example.client.UserClient", "getUser",
                    "(Ljava/lang/Long;)Ljava/lang/Object;", "java.lang.Object",
                    List.of(), 185, null);

            IndexedMethod caller = minimalMethod("invokeFeign", List.of(call));
            CallNode node = invokeCreateFeignCallLeaf(builder, feign, call, caller);

            assertEquals("HTTP_CALL", node.getCallType(), "callType must be HTTP_CALL");
            assertTrue(node.getAnnotations() != null && node.getAnnotations().stream()
                    .anyMatch(a -> a.startsWith("@GET")),
                    "annotations must contain @GET");
            assertTrue(node.getStringLiterals() != null
                    && node.getStringLiterals().contains("/users/{id}"),
                    "stringLiterals must contain the path");
        }

        @Test
        @DisplayName("E10 @FeignClient @PostMapping → HTTP_CALL")
        void feignPostMapping_producesHttpCall() throws Exception {
            IndexedClass feign = feignInterface(
                    "com.example.client.UserClient", "UserClient",
                    "createUser", "POST", "/users");

            InvocationRef call = new InvocationRef(
                    "com.example.client.UserClient", "createUser",
                    "(Ljava/lang/Long;)Ljava/lang/Object;", "java.lang.Object",
                    List.of(), 185, null);

            IndexedMethod caller = minimalMethod("invokeFeign", List.of(call));
            CallNode node = invokeCreateFeignCallLeaf(builder, feign, call, caller);

            assertEquals("HTTP_CALL", node.getCallType());
            assertTrue(node.getAnnotations() != null && node.getAnnotations().stream()
                    .anyMatch(a -> a.startsWith("@POST")),
                    "annotations must contain @POST for postMapping");
        }

        @Test
        @DisplayName("E11 @GetExchange Spring 6.1 → HTTP_CALL (httpMethod=GET on indexed method)")
        void getExchange_producesHttpCall() throws Exception {
            // Spring 6 HTTP Interface: @GetExchange is resolved into httpMethod="GET" by the bytecode reader.
            IndexedClass httpIface = feignInterface(
                    "com.example.client.OrderClient", "OrderClient",
                    "getOrder", "GET", "/orders/{id}");

            InvocationRef call = new InvocationRef(
                    "com.example.client.OrderClient", "getOrder",
                    "(Ljava/lang/Long;)Ljava/lang/Object;", "java.lang.Object",
                    List.of(), 185, null);

            IndexedMethod caller = minimalMethod("invokeHttp", List.of(call));
            CallNode node = invokeCreateFeignCallLeaf(builder, httpIface, call, caller);

            assertEquals("HTTP_CALL", node.getCallType());
        }

        @Test
        @DisplayName("E12 @PostExchange → HTTP_CALL, operationType=WRITE")
        void postExchange_producesWriteHttpCall() throws Exception {
            IndexedClass httpIface = feignInterface(
                    "com.example.client.OrderClient", "OrderClient",
                    "createOrder", "POST", "/orders");

            InvocationRef call = new InvocationRef(
                    "com.example.client.OrderClient", "createOrder",
                    "(Ljava/lang/Long;)Ljava/lang/Object;", "java.lang.Object",
                    List.of(), 185, null);

            IndexedMethod caller = minimalMethod("invokeHttp", List.of(call));
            CallNode node = invokeCreateFeignCallLeaf(builder, httpIface, call, caller);

            assertEquals("HTTP_CALL", node.getCallType());
            // operationType is derived from method name via inferHttpOperation
            assertEquals("WRITE", node.getOperationType(), "post → WRITE");
        }
    }

    // ── E13: java.net.http.HttpClient ─────────────────────────────────────────

    @Test
    @DisplayName("E13 java.net.http.HttpClient.send() → HTTP_CALL")
    void javaNetHttpClient_send_producesHttpCall() throws Exception {
        InvocationRef call = httpInvRef(
                "java.net.http.HttpClient",
                "send",
                List.of("https://api.example.com/v1/users")
        );
        IndexedMethod caller = minimalMethod("callApi", List.of(call));
        CallNode node = invokeCreateHttpCallLeaf(builder, call, caller);

        assertEquals("HTTP_CALL", node.getCallType(), "java.net.http.HttpClient.send() must be HTTP_CALL");
    }

    // ── E14: URI captured in stringLiterals from string args ─────────────────

    @Test
    @DisplayName("E14 URL from call stringArgs is captured in stringLiterals")
    void urlFromStringArgs_capturedInStringLiterals() throws Exception {
        String expectedUrl = "http://payments.internal/process";
        InvocationRef call = httpInvRef(
                "org.springframework.web.client.RestTemplate",
                "postForObject",
                List.of(expectedUrl)
        );
        IndexedMethod caller = minimalMethod("processPayment", List.of(call));
        CallNode node = invokeCreateHttpCallLeaf(builder, call, caller);

        assertNotNull(node.getStringLiterals(), "stringLiterals must not be null");
        assertTrue(node.getStringLiterals().contains(expectedUrl),
                "URL from call args must appear in stringLiterals");
    }

    // ── E15: crossModule=true ────────────────────────────────────────────────

    @Test
    @DisplayName("E15 HTTP_CALL node has crossModule=true")
    void httpCallNode_hasCrossModuleTrue() throws Exception {
        InvocationRef call = httpInvRef(
                "org.springframework.web.client.RestTemplate",
                "getForObject",
                List.of("http://api/data")
        );
        IndexedMethod caller = minimalMethod("fetchData", List.of(call));
        CallNode node = invokeCreateHttpCallLeaf(builder, call, caller);

        assertTrue(node.isCrossModule(), "HTTP_CALL leaf must have crossModule=true");
    }

    // ── E16: HTTP verb in annotations list ───────────────────────────────────

    @Test
    @DisplayName("E16 HTTP verb present in annotations list (@GET, @POST, etc.)")
    void httpVerbAnnotation_presentInAnnotationsList() throws Exception {
        InvocationRef getCall = httpInvRef(
                "org.springframework.web.client.RestTemplate",
                "getForObject",
                List.of("http://api/items")
        );
        InvocationRef postCall = httpInvRef(
                "org.springframework.web.client.RestTemplate",
                "postForObject",
                List.of("http://api/items")
        );
        IndexedMethod caller = minimalMethod("doOps", List.of());

        CallNode getNode = invokeCreateHttpCallLeaf(builder, getCall, caller);
        CallNode postNode = invokeCreateHttpCallLeaf(builder, postCall, caller);

        // The verb moved from annotations to operationType, deliberately.
        //
        // createHttpCallLeaf sets node.setOperationType(resolvedVerb) and CallTreeBuilder carries an
        // explicit note that "HTTP_CALL leaves get their operationType from the resolved HTTP verb"
        // — with an exemption written specifically so that value is not nulled out by
        // false-positive filtering. Asserting on the old @GET annotation was testing a design that
        // had been replaced; the property that matters, that the verb is captured and reachable, is
        // unchanged.
        // Two fields, deliberately: httpTargetMethod carries the VERB, operationType the semantic
        // operation it implies. createHttpCallLeaf sets httpTargetMethod = resolvedVerb and
        // operationType = inferHttpOperationFromMethod(resolvedVerb), so GET becomes READ. Asserting
        // "GET" on operationType was my error while updating this test — the verb lives next door.
        assertEquals("GET", getNode.getHttpTargetMethod(), "a GET call must record the GET verb");
        assertEquals("POST", postNode.getHttpTargetMethod(), "a POST call must record the POST verb");
        assertEquals("READ", getNode.getOperationType(), "GET is a read");
        assertEquals("WRITE", postNode.getOperationType(), "POST is a write");
    }

    // ── E17: Feign method with no HTTP annotation → fallback HTTP_CALL ───────

    @Test
    @DisplayName("E17 Feign method with no HTTP mapping → fallback HTTP_CALL leaf")
    void feignMethod_withNoHttpAnnotation_stillProducesHttpCallLeaf() throws Exception {
        // Create a feign interface whose method has no httpMethod set (null)
        ClassInfo ci = new ClassInfo();
        ci.setFullyQualifiedName("com.example.client.LegacyClient");
        ci.setSimpleName("LegacyClient");
        ci.setStereotype("HTTP_CLIENT");
        ci.setIsInterface(true);

        MethodInfo mi = new MethodInfo();
        mi.setName("getData");
        mi.setDescriptor("()Ljava/lang/Object;");
        mi.setReturnType("java.lang.Object");
        // httpMethod intentionally left null

        ci.getMethods().add(mi);

        IndexedClass ic = new IndexedClass(ci, Pattern.compile("(?!x)x"));
        ic.methods.put("getData()Ljava/lang/Object;", new IndexedMethod(mi));

        InvocationRef call = new InvocationRef(
                "com.example.client.LegacyClient", "getData",
                "()Ljava/lang/Object;", "java.lang.Object",
                List.of(), 185, null);

        IndexedMethod caller = minimalMethod("callLegacy", List.of(call));
        CallNode node = invokeCreateFeignCallLeaf(builder, ic, call, caller);

        // Even with no mapping, the node must be HTTP_CALL
        assertEquals("HTTP_CALL", node.getCallType(),
                "Feign call with no HTTP annotation must still produce HTTP_CALL");
    }

    // ── E18: URL from method's string literals (not direct arg) ─────────────

    @Test
    @DisplayName("E18 RestTemplate call URL from caller's stringLiterals (not call arg)")
    void urlFromCallerStringLiterals_captured() throws Exception {
        String expectedUrl = "/api/v2/claims";
        InvocationRef call = httpInvRef(
                "org.springframework.web.client.RestTemplate",
                "getForObject",
                List.of() // no direct string args
        );
        // Provide the URL via the caller method's string literals
        IndexedMethod caller = minimalMethod("fetchClaims", List.of(call),
                List.of(expectedUrl));
        CallNode node = invokeCreateHttpCallLeaf(builder, call, caller);

        assertNotNull(node.getStringLiterals(), "stringLiterals must be populated from caller literals");
        assertFalse(node.getStringLiterals().isEmpty(),
                "stringLiterals must not be empty when caller has URL literals");
        assertTrue(node.getStringLiterals().contains(expectedUrl),
                "The URL from caller's stringLiterals must be captured in the node");
    }

    // ── E19: restTemplate.exchange with HttpMethod.DELETE ───────────────────

    @Test
    @DisplayName("E19 restTemplate.exchange(url, DELETE, entity, Void) → HTTP_CALL present")
    void restTemplate_exchange_delete_producesHttpCall() throws Exception {
        InvocationRef call = httpInvRef(
                "org.springframework.web.client.RestTemplate",
                "exchange",
                List.of("http://api/orders/{id}")
        );
        IndexedMethod caller = minimalMethod("cancelOrder", List.of(call));
        CallNode node = invokeCreateHttpCallLeaf(builder, call, caller);

        assertEquals("HTTP_CALL", node.getCallType(), "exchange must produce HTTP_CALL");
    }

    // ── E20: Negative test — non-HTTP class with get() ───────────────────────

    @Test
    @DisplayName("E20 NEGATIVE: Plain non-HTTP class with get() must NOT be tagged HTTP_CALL by inferHttpOperation alone")
    void nonHttpClass_getMethod_operationTypeIsRead_butNotHttpCall() throws Exception {
        // createHttpCallLeaf is only invoked by CallTreeBuilder when isHttpClientCall() is true.
        // A plain class's get() will NOT reach createHttpCallLeaf — it goes through normal resolution.
        // We verify the inferHttpOperation helper maps "get" → READ (consistent with the filter).
        String opType = invokeInferHttpOperation(builder, "get");
        assertEquals("READ", opType, "inferHttpOperation('get') must return READ for a getter-like name");

        // Also verify that a hypothetical non-HTTP leaf built via createHttpCallLeaf still
        // has HTTP_CALL type — but the guard that blocks it is in buildCallTree (isHttpClientCall).
        // We test that guard separately: a class not matching httpClientClasses prefix does NOT
        // produce a leaf (tested at integration level; here we guard the contract).
        InvocationRef call = httpInvRef(
                "com.example.internal.OrderService",
                "get",
                List.of()
        );
        // The configLoader has empty httpClientClasses (no domain-config.json in test).
        // So isHttpClientCall("com.example.internal.OrderService") returns false.
        // We verify isHttpClientCall returns false for a non-HTTP class.
        DomainConfigLoader loader = new DomainConfigLoader();
        assertFalse(loader.isHttpClientCall("com.example.internal.OrderService"),
                "Non-HTTP class must not be classified as HTTP client without config");
    }
}
