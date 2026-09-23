package com.jaranalyzer.service;

import com.jaranalyzer.model.AnnotationInfo;
import com.jaranalyzer.model.CallNode;
import com.jaranalyzer.model.ClassInfo;
import com.jaranalyzer.model.EndpointInfo;
import com.jaranalyzer.model.MethodInfo;
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
 * Category I — Security Annotation Tests (15 tests).
 *
 * Tests verify that security annotations (@PreAuthorize, @PostAuthorize,
 * @Secured, @RolesAllowed) are correctly captured into
 * {@link CallNode#getSecurityRoles()} and that
 * {@link EndpointInfo#getSecurityConstraints()} is populated for endpoint-level
 * methods.
 *
 * Strategy: drive {@code createEnrichedNode} via reflection (same pattern as
 * AsyncBoundaryTest / CacheTaggingTest).  {@code EndpointInfo.securityConstraints}
 * is set by the service layer that copies securityRoles from the root node; we
 * verify the field directly on EndpointInfo for the relevant tests.
 */
@DisplayName("Category I — Security annotation tagging")
class SecurityTaggingTest {

    // ── Builder factory ───────────────────────────────────────────────────────

    private CallTreeBuilder builder;

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

    private static IndexedClass minimalClass(String fqn, String simpleName, String stereotype) {
        ClassInfo ci = new ClassInfo();
        ci.setFullyQualifiedName(fqn);
        ci.setSimpleName(simpleName);
        ci.setStereotype(stereotype);
        return new IndexedClass(ci, Pattern.compile("(?!x)x"));
    }

    /**
     * Build an IndexedMethod carrying a single security annotation with a single
     * string "value" attribute.
     */
    private static IndexedMethod methodWithSecurityAnnotation(String methodName,
                                                               String annotationName,
                                                               String value) {
        MethodInfo mi = new MethodInfo();
        mi.setName(methodName);
        mi.setDescriptor("()V");
        mi.setReturnType("void");

        AnnotationInfo ann = new AnnotationInfo();
        ann.setName(annotationName);
        Map<String, Object> attrs = new LinkedHashMap<>();
        attrs.put("value", value);
        ann.setAttributes(attrs);
        mi.getAnnotations().add(ann);

        return new IndexedMethod(mi);
    }

    /**
     * Build an IndexedMethod carrying a @Secured annotation whose value is a
     * List of role names (the typical multi-role form).
     */
    private static IndexedMethod methodWithSecuredMultiRole(String methodName, List<String> roles) {
        MethodInfo mi = new MethodInfo();
        mi.setName(methodName);
        mi.setDescriptor("()V");
        mi.setReturnType("void");

        AnnotationInfo ann = new AnnotationInfo();
        ann.setName("Secured");
        Map<String, Object> attrs = new LinkedHashMap<>();
        attrs.put("value", roles);
        ann.setAttributes(attrs);
        mi.getAnnotations().add(ann);

        return new IndexedMethod(mi);
    }

    /** Build an IndexedMethod with no security annotations. */
    private static IndexedMethod plainMethod(String methodName) {
        MethodInfo mi = new MethodInfo();
        mi.setName(methodName);
        mi.setDescriptor("()V");
        mi.setReturnType("void");
        return new IndexedMethod(mi);
    }

    /** Build an IndexedMethod with multiple security annotations. */
    private static IndexedMethod methodWithMultipleSecurityAnnotations(String methodName) {
        MethodInfo mi = new MethodInfo();
        mi.setName(methodName);
        mi.setDescriptor("()V");
        mi.setReturnType("void");

        AnnotationInfo preAuth = new AnnotationInfo();
        preAuth.setName("PreAuthorize");
        Map<String, Object> attrs1 = new LinkedHashMap<>();
        attrs1.put("value", "hasRole('ADMIN')");
        preAuth.setAttributes(attrs1);
        mi.getAnnotations().add(preAuth);

        AnnotationInfo secured = new AnnotationInfo();
        secured.setName("Secured");
        Map<String, Object> attrs2 = new LinkedHashMap<>();
        attrs2.put("value", "ROLE_SUPERUSER");
        secured.setAttributes(attrs2);
        mi.getAnnotations().add(secured);

        return new IndexedMethod(mi);
    }

    /** Build an IndexedMethod with @Secured and an empty value array. */
    private static IndexedMethod methodWithSecuredEmptyArray(String methodName) {
        MethodInfo mi = new MethodInfo();
        mi.setName(methodName);
        mi.setDescriptor("()V");
        mi.setReturnType("void");

        AnnotationInfo ann = new AnnotationInfo();
        ann.setName("Secured");
        Map<String, Object> attrs = new LinkedHashMap<>();
        attrs.put("value", List.of()); // empty list
        ann.setAttributes(attrs);
        mi.getAnnotations().add(ann);

        return new IndexedMethod(mi);
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("I1-I2 — @PreAuthorize SpEL expressions")
    class PreAuthorizeTests {

        @Test
        @DisplayName("I1 @PreAuthorize(\"hasRole('ADMIN')\") → securityRoles contains the SpEL expression")
        void i1_preAuthorizeHasRole_capturedInSecurityRoles() throws Exception {
            IndexedClass cls = minimalClass("com.example.AdminController", "AdminController", "REST_CONTROLLER");
            IndexedMethod method = methodWithSecurityAnnotation("adminAction",
                    "PreAuthorize", "hasRole('ADMIN')");

            CallNode node = invokeCreateEnrichedNode(cls, method);

            assertNotNull(node.getSecurityRoles(), "securityRoles must not be null");
            assertFalse(node.getSecurityRoles().isEmpty(),
                    "securityRoles must not be empty for @PreAuthorize method");
            assertTrue(node.getSecurityRoles().contains("hasRole('ADMIN')"),
                    "securityRoles must contain the SpEL expression \"hasRole('ADMIN')\". Actual: "
                            + node.getSecurityRoles());
        }

        @Test
        @DisplayName("I2 @PreAuthorize(\"hasAuthority('WRITE_ORDER')\") → securityRoles contains the authority expression")
        void i2_preAuthorizeHasAuthority_capturedInSecurityRoles() throws Exception {
            IndexedClass cls = minimalClass("com.example.OrderController", "OrderController", "REST_CONTROLLER");
            IndexedMethod method = methodWithSecurityAnnotation("createOrder",
                    "PreAuthorize", "hasAuthority('WRITE_ORDER')");

            CallNode node = invokeCreateEnrichedNode(cls, method);

            assertNotNull(node.getSecurityRoles());
            assertTrue(node.getSecurityRoles().contains("hasAuthority('WRITE_ORDER')"),
                    "securityRoles must contain the authority expression. Actual: " + node.getSecurityRoles());
        }
    }

    @Nested
    @DisplayName("I3-I6 — @Secured and @RolesAllowed")
    class SecuredAndRolesAllowedTests {

        @Test
        @DisplayName("I3 @Secured({\"ROLE_USER\",\"ROLE_ADMIN\"}) → securityRoles contains both roles")
        void i3_securedMultipleRoles_bothCaptured() throws Exception {
            IndexedClass cls = minimalClass("com.example.UserController", "UserController", "REST_CONTROLLER");
            IndexedMethod method = methodWithSecuredMultiRole("listUsers",
                    List.of("ROLE_USER", "ROLE_ADMIN"));

            CallNode node = invokeCreateEnrichedNode(cls, method);

            assertNotNull(node.getSecurityRoles());
            assertTrue(node.getSecurityRoles().contains("ROLE_USER"),
                    "securityRoles must contain 'ROLE_USER'. Actual: " + node.getSecurityRoles());
            assertTrue(node.getSecurityRoles().contains("ROLE_ADMIN"),
                    "securityRoles must contain 'ROLE_ADMIN'. Actual: " + node.getSecurityRoles());
        }

        @Test
        @DisplayName("I4 @Secured(\"ROLE_MANAGER\") (single value String) → securityRoles contains ROLE_MANAGER")
        void i4_securedSingleRole_captured() throws Exception {
            IndexedClass cls = minimalClass("com.example.ManagerController", "ManagerController", "REST_CONTROLLER");
            IndexedMethod method = methodWithSecurityAnnotation("approveOrder",
                    "Secured", "ROLE_MANAGER");

            CallNode node = invokeCreateEnrichedNode(cls, method);

            assertNotNull(node.getSecurityRoles());
            assertTrue(node.getSecurityRoles().contains("ROLE_MANAGER"),
                    "securityRoles must contain 'ROLE_MANAGER'. Actual: " + node.getSecurityRoles());
        }

        @Test
        @DisplayName("I5 @RolesAllowed(\"ROLE_OPERATOR\") (javax) → securityRoles contains ROLE_OPERATOR")
        void i5_rolesAllowedJavax_captured() throws Exception {
            IndexedClass cls = minimalClass("com.example.OperationsController", "OperationsController", "REST_CONTROLLER");
            IndexedMethod method = methodWithSecurityAnnotation("startJob",
                    "RolesAllowed", "ROLE_OPERATOR");

            CallNode node = invokeCreateEnrichedNode(cls, method);

            assertNotNull(node.getSecurityRoles());
            assertTrue(node.getSecurityRoles().contains("ROLE_OPERATOR"),
                    "securityRoles must contain 'ROLE_OPERATOR' for javax @RolesAllowed. Actual: "
                            + node.getSecurityRoles());
        }

        @Test
        @DisplayName("I6 @RolesAllowed(\"ROLE_OPERATOR\") (jakarta variant) → securityRoles contains ROLE_OPERATOR")
        void i6_rolesAllowedJakarta_captured() throws Exception {
            // The production switch uses the annotation simple name "RolesAllowed"
            // regardless of whether it came from javax or jakarta — both map to the same name.
            IndexedClass cls = minimalClass("com.example.OperationsController", "OperationsController", "REST_CONTROLLER");
            IndexedMethod method = methodWithSecurityAnnotation("stopJob",
                    "RolesAllowed", "ROLE_OPERATOR");

            CallNode node = invokeCreateEnrichedNode(cls, method);

            assertNotNull(node.getSecurityRoles());
            assertTrue(node.getSecurityRoles().contains("ROLE_OPERATOR"),
                    "securityRoles must contain 'ROLE_OPERATOR' for jakarta @RolesAllowed. Actual: "
                            + node.getSecurityRoles());
        }
    }

    @Nested
    @DisplayName("I7 — No security annotation")
    class NoSecurityAnnotation {

        @Test
        @DisplayName("I7 Method with no security annotation → securityRoles null or empty")
        void i7_noSecurityAnnotation_securityRolesEmpty() throws Exception {
            IndexedClass cls = minimalClass("com.example.PublicController", "PublicController", "REST_CONTROLLER");
            IndexedMethod method = plainMethod("getPublicInfo");

            CallNode node = invokeCreateEnrichedNode(cls, method);

            // securityRoles is initialized as an empty list, so it should never be null
            // but it must be empty (no roles captured)
            assertTrue(node.getSecurityRoles() == null || node.getSecurityRoles().isEmpty(),
                    "Method without security annotation must have empty securityRoles. Actual: "
                            + node.getSecurityRoles());
        }
    }

    @Nested
    @DisplayName("I8 — @PostAuthorize")
    class PostAuthorizeTests {

        @Test
        @DisplayName("I8 @PostAuthorize(\"returnObject.owner == authentication.name\") → securityRoles contains the expression")
        void i8_postAuthorize_capturedInSecurityRoles() throws Exception {
            IndexedClass cls = minimalClass("com.example.DocumentController", "DocumentController", "REST_CONTROLLER");
            IndexedMethod method = methodWithSecurityAnnotation("getDocument",
                    "PostAuthorize", "returnObject.owner == authentication.name");

            CallNode node = invokeCreateEnrichedNode(cls, method);

            assertNotNull(node.getSecurityRoles());
            assertFalse(node.getSecurityRoles().isEmpty(),
                    "securityRoles must not be empty for @PostAuthorize method");
            assertTrue(node.getSecurityRoles().contains("returnObject.owner == authentication.name"),
                    "securityRoles must contain the SpEL expression verbatim. Actual: "
                            + node.getSecurityRoles());
        }
    }

    @Nested
    @DisplayName("I9 — annotationDetails entry for security annotation")
    class AnnotationDetailsEntry {

        @Test
        @DisplayName("I9 Security annotation shows up in annotationDetails with correct name")
        void i9_securityAnnotationInAnnotationDetails() throws Exception {
            IndexedClass cls = minimalClass("com.example.SecureService", "SecureService", "SERVICE");
            IndexedMethod method = methodWithSecurityAnnotation("sensitiveOp",
                    "PreAuthorize", "hasRole('ADMIN')");

            CallNode node = invokeCreateEnrichedNode(cls, method);

            assertNotNull(node.getAnnotationDetails(), "annotationDetails must not be null");
            boolean found = node.getAnnotationDetails().stream()
                    .anyMatch(d -> "PreAuthorize".equals(d.get("name")));
            assertTrue(found,
                    "annotationDetails must contain an entry named 'PreAuthorize'. Actual: "
                            + node.getAnnotationDetails());
        }
    }

    @Nested
    @DisplayName("I10 — SpEL expressions preserved exactly")
    class SpelPreservation {

        @Test
        @DisplayName("I10 Security annotation SpEL value preserved exactly (no modification)")
        void i10_spelExpressionPreservedExactly() throws Exception {
            String exactSpel = "hasRole('ADMIN') and #order.customerId == authentication.name";
            IndexedClass cls = minimalClass("com.example.OrderController", "OrderController", "REST_CONTROLLER");
            IndexedMethod method = methodWithSecurityAnnotation("updateOrder",
                    "PreAuthorize", exactSpel);

            CallNode node = invokeCreateEnrichedNode(cls, method);

            assertNotNull(node.getSecurityRoles());
            assertEquals(exactSpel, node.getSecurityRoles().get(0),
                    "The SpEL expression must be stored exactly as-is. Actual: " + node.getSecurityRoles());
        }
    }

    @Nested
    @DisplayName("I11-I12 — EndpointInfo.securityConstraints")
    class EndpointSecurityConstraints {

        @Test
        @DisplayName("I11 @PreAuthorize on endpoint method → EndpointInfo.securityConstraints populated")
        void i11_preAuthorizeOnEndpoint_securityConstraintsPopulated() throws Exception {
            IndexedClass cls = minimalClass("com.example.AdminController", "AdminController", "REST_CONTROLLER");
            IndexedMethod method = methodWithSecurityAnnotation("deleteUser",
                    "PreAuthorize", "hasRole('ADMIN')");

            CallNode rootNode = invokeCreateEnrichedNode(cls, method);

            // Simulate the service layer copying securityRoles to EndpointInfo.securityConstraints
            EndpointInfo ei = new EndpointInfo();
            ei.setSecurityConstraints(rootNode.getSecurityRoles());

            assertNotNull(ei.getSecurityConstraints(),
                    "securityConstraints must not be null");
            assertFalse(ei.getSecurityConstraints().isEmpty(),
                    "securityConstraints must be non-empty for secured endpoint");
            assertTrue(ei.getSecurityConstraints().contains("hasRole('ADMIN')"),
                    "securityConstraints must contain the role expression. Actual: "
                            + ei.getSecurityConstraints());
        }

        @Test
        @DisplayName("I12 Endpoint with no security annotation → EndpointInfo.securityConstraints empty")
        void i12_noSecurityAnnotation_securityConstraintsEmpty() throws Exception {
            IndexedClass cls = minimalClass("com.example.PublicController", "PublicController", "REST_CONTROLLER");
            IndexedMethod method = plainMethod("healthCheck");

            CallNode rootNode = invokeCreateEnrichedNode(cls, method);

            EndpointInfo ei = new EndpointInfo();
            ei.setSecurityConstraints(rootNode.getSecurityRoles());

            assertTrue(ei.getSecurityConstraints() == null || ei.getSecurityConstraints().isEmpty(),
                    "securityConstraints must be empty for unsecured endpoint. Actual: "
                            + ei.getSecurityConstraints());
        }
    }

    @Nested
    @DisplayName("I13 — Multiple security annotations on same method")
    class MultipleSecurityAnnotations {

        @Test
        @DisplayName("I13 Multiple security annotations on same method → all roles collected in securityRoles")
        void i13_multipleSecurityAnnotations_allRolesCollected() throws Exception {
            IndexedClass cls = minimalClass("com.example.SuperController", "SuperController", "REST_CONTROLLER");
            IndexedMethod method = methodWithMultipleSecurityAnnotations("superAction");

            CallNode node = invokeCreateEnrichedNode(cls, method);

            assertNotNull(node.getSecurityRoles());
            assertTrue(node.getSecurityRoles().size() >= 2,
                    "Multiple security annotations must contribute multiple entries to securityRoles. Actual: "
                            + node.getSecurityRoles());
            assertTrue(node.getSecurityRoles().contains("hasRole('ADMIN')"),
                    "Must contain ADMIN role from @PreAuthorize");
            assertTrue(node.getSecurityRoles().contains("ROLE_SUPERUSER"),
                    "Must contain ROLE_SUPERUSER from @Secured");
        }
    }

    @Nested
    @DisplayName("I14 — @Secured with empty value array")
    class SecuredEmptyArray {

        @Test
        @DisplayName("I14 @Secured with empty array → securityRoles empty (no NPE)")
        void i14_securedEmptyArray_noNpe() throws Exception {
            IndexedClass cls = minimalClass("com.example.EmptySecuredService", "EmptySecuredService", "SERVICE");
            IndexedMethod method = methodWithSecuredEmptyArray("doNothing");

            // Must not throw NPE or any exception
            CallNode node = assertDoesNotThrow(() -> invokeCreateEnrichedNode(cls, method),
                    "@Secured with empty value array must not throw an exception");

            // securityRoles should be empty (no roles to add)
            assertNotNull(node.getSecurityRoles(), "securityRoles must not be null");
            assertTrue(node.getSecurityRoles().isEmpty(),
                    "securityRoles must be empty for @Secured({}). Actual: " + node.getSecurityRoles());
        }
    }

    @Nested
    @DisplayName("I15 — Security annotation on @Service method (not endpoint)")
    class SecurityOnServiceMethod {

        @Test
        @DisplayName("I15 @PreAuthorize on @Service method → securityRoles on node, but EndpointInfo.securityConstraints empty")
        void i15_securityOnServiceMethod_notPropagatedToEndpoint() throws Exception {
            // The @Service method has @PreAuthorize
            IndexedClass serviceCls = minimalClass("com.example.OrderService", "OrderService", "SERVICE");
            IndexedMethod serviceMethod = methodWithSecurityAnnotation("processOrder",
                    "PreAuthorize", "hasRole('ADMIN')");

            CallNode serviceNode = invokeCreateEnrichedNode(serviceCls, serviceMethod);

            // The service node has securityRoles populated
            assertFalse(serviceNode.getSecurityRoles().isEmpty(),
                    "Service node with @PreAuthorize must have securityRoles populated");

            // But the root endpoint (controller) has no security annotation
            IndexedClass ctrlCls = minimalClass("com.example.OrderController", "OrderController", "REST_CONTROLLER");
            IndexedMethod ctrlMethod = plainMethod("placeOrder");
            CallNode ctrlNode = invokeCreateEnrichedNode(ctrlCls, ctrlMethod);

            // EndpointInfo is constructed from the controller root node
            EndpointInfo ei = new EndpointInfo();
            ei.setSecurityConstraints(ctrlNode.getSecurityRoles());

            // GAP: the service node's roles are NOT propagated to the endpoint's securityConstraints
            assertTrue(ei.getSecurityConstraints().isEmpty(),
                    "EndpointInfo.securityConstraints must be empty when only downstream service has security roles");
        }
    }
}
