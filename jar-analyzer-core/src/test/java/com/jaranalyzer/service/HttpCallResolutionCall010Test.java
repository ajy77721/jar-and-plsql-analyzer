package com.jaranalyzer.service;

import com.jaranalyzer.model.ClassInfo;
import com.jaranalyzer.model.CallNode;
import com.jaranalyzer.service.CallGraphIndex.IndexedClass;
import com.jaranalyzer.service.CallGraphIndex.IndexedEndpoint;
import com.jaranalyzer.service.CallGraphIndex.IndexedMethod;
import com.jaranalyzer.service.CallGraphIndex.ResolutionContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CALL-010 (docs/TASK-REGISTRY.md §216): real, javac-compiled .class-file proof that HTTP/
 * external-call target-path resolution now has the same cross-method value-tracing capability
 * CALL-001..CALL-009 already built for MongoDB collections / JDBC table names — single-hop
 * caller-arg resolution, bounded multi-hop (reusing {@link CollectionResolver#MAX_HOP_DEPTH}),
 * and narrow `literal + param` string-concat folding for RestTemplate-style HTTP client calls.
 *
 * Loads REAL .class files off disk through the REAL BytecodeClassParser/CallTreeBuilder/
 * CollectionResolver pipeline — no stubbed ClassInfo/MethodInfo, no mocked detectors — matching
 * this session's own established fixture style (see JdbcTableResolutionCall009Test,
 * FieldStoredMethodReferenceGapTest).
 */
class HttpCallResolutionCall010Test {

    // Fixtures are COMMITTED sources (src/test/resources/scratch/call010/<app>/*.java) compiled
        // at test time by ScratchCompiler — the original per-session /tmp scratchpad died with
        // the workspace move (44 NoSuchFileErrors). Same apps, same -g compile, reproducible anywhere.

    @Test
    @DisplayName("CALL-010 control case — direct literal URL at the call site resolves (baseline)")
    void controlCase_directLiteralUrl() throws Exception {
        CallNode root = buildTreeForFirstEndpoint(ScratchCompiler.compile("call010", "control"));
        printTree(root, 0);
        assertNotNull(root);
        CallNode leaf = root.getChildren().get(0);
        assertEquals("HTTP_CALL", leaf.getCallType());
        assertEquals("/api/orders", leaf.getHttpTargetPath());
        assertEquals("DIRECT_LITERAL", leaf.getHttpTargetPathSource());
    }

    @Test
    @DisplayName("CALL-010 single-hop — literal URL passed as a caller argument now resolves")
    void singleHopCase_paramPropagation() throws Exception {
        CallNode root = buildTreeForFirstEndpoint(ScratchCompiler.compile("call010", "singlehop"));
        printTree(root, 0);
        assertNotNull(root);
        CallNode leaf = root.getChildren().get(0).getChildren().get(0);
        assertEquals("HTTP_CALL", leaf.getCallType());
        assertEquals("/api/customers", leaf.getHttpTargetPath());
        assertEquals("CALL_SITE_PARAM_PROPAGATION", leaf.getHttpTargetPathSource());
    }

    @Test
    @DisplayName("CALL-010 bounded multi-hop — URL threaded unchanged through 2 wrapper methods now resolves")
    void multiHopCase_boundedPropagation() throws Exception {
        CallNode root = buildTreeForFirstEndpoint(ScratchCompiler.compile("call010", "multihop"));
        printTree(root, 0);
        assertNotNull(root);
        CallNode leaf = root.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(0);
        assertEquals("HTTP_CALL", leaf.getCallType());
        assertEquals("/api/invoices", leaf.getHttpTargetPath());
        assertEquals("CALL_SITE_PARAM_PROPAGATION", leaf.getHttpTargetPathSource());
    }

    @Test
    @DisplayName("CALL-010 concat-fold — literal-prefix + resolvable-param URL concatenation now resolves")
    void concatCase_literalParamFold() throws Exception {
        CallNode root = buildTreeForFirstEndpoint(ScratchCompiler.compile("call010", "concat"));
        printTree(root, 0);
        assertNotNull(root);
        CallNode leaf = root.getChildren().get(0).getChildren().get(0).getChildren().get(0);
        assertEquals("HTTP_CALL", leaf.getCallType());
        assertEquals("/api/orders/east", leaf.getHttpTargetPath());
        assertEquals("CALL_SITE_CONCAT_PROPAGATION", leaf.getHttpTargetPathSource());
    }

    // ── shared harness (mirrors JdbcTableResolutionCall009Test) ────────────────────────────

    private CallNode buildTreeForFirstEndpoint(Path classesDir) throws Exception {
        BytecodeClassParser parser = new BytecodeClassParser();
        DomainConfigLoader loader = new DomainConfigLoader();
        var m = DomainConfigLoader.class.getDeclaredMethod("applyDefaults");
        m.setAccessible(true);
        m.invoke(loader);
        // CALL-010: applyDefaults() leaves httpClientClasses empty (real config comes from an
        // external YAML the test harness doesn't load) — patch in the one prefix this fixture
        // needs so isHttpClientCall() actually routes RestTemplate calls to createHttpCallLeaf,
        // exactly matching how the real app's own config would classify RestTemplate.
        Field httpClientClassesField = DomainConfigLoader.class.getDeclaredField("httpClientClasses");
        httpClientClassesField.setAccessible(true);
        httpClientClassesField.set(loader, List.of("org.springframework.web.client.RestTemplate"));

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
        CallTreeBuilder builder = new CallTreeBuilder(loader, resolver, 30, 30, 2000, 100000);
        ResolutionContext ctx = builder.createContext();
        ctx.jarArtifactMap = new java.util.HashMap<>();

        List<ClassInfo> parsed;
        try (Stream<Path> walk = Files.walk(classesDir)) {
            parsed = walk.filter(p -> p.toString().endsWith(".class"))
                    .map(p -> {
                        try {
                            return parser.parseClass(Files.readAllBytes(p));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .filter(c -> c != null)
                    .toList();
        }

        System.out.println("Parsed " + parsed.size() + " real .class files from " + classesDir);
        for (ClassInfo ci : parsed) {
            builder.indexClass(ci);
        }

        if (ctx.endpoints.isEmpty()) {
            System.out.println("NO ENDPOINTS INDEXED — cannot build a call tree.");
            return null;
        }
        IndexedEndpoint ep = ctx.endpoints.get(0);
        IndexedClass cls = ctx.classMap.get(ep.controllerFqn);
        IndexedMethod method = cls.methods.get(ep.methodKey);
        if (method == null) {
            method = cls.methods.values().stream()
                    .filter(mm -> mm.name.equals(ep.methodName))
                    .findFirst().orElse(null);
        }
        return builder.buildCallTree(cls, method, new HashSet<>(), 0, new int[]{0}, null);
    }

    private void printTree(CallNode node, int depth) {
        if (node == null) {
            System.out.println("(null node)");
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("  ".repeat(depth));
        sb.append(node.getClassName()).append("#").append(node.getMethodName());
        sb.append(" callType=").append(node.getCallType());
        sb.append(" httpTargetPath=").append(node.getHttpTargetPath());
        sb.append(" httpTargetPathSource=").append(node.getHttpTargetPathSource());
        System.out.println(sb);
        if (node.getChildren() != null) {
            for (CallNode child : node.getChildren()) {
                printTree(child, depth + 1);
            }
        }
    }
}
