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
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * CALL-015 (docs/TASK-REGISTRY.md §216/§220): real, javac-compiled .class-file proof that the
 * `this.baseUrl + path` base-URL-instance-field concat pattern — §216's own documented residual
 * gap in {@code BytecodeClassParser.tryFoldStringConcat}/{@code ConcatSlotInfo} — now resolves,
 * while a genuinely unresolvable field (assigned from a constructor PARAMETER, the real
 * `@Value`-injected shape) correctly stays unresolved rather than being guessed at.
 *
 * Loads REAL .class files off disk through the REAL BytecodeClassParser/CallTreeBuilder/
 * CollectionResolver pipeline, matching this session's own established fixture style (see
 * HttpCallResolutionCall010Test, JdbcTableResolutionCall009Test).
 */
class FieldBaseUrlConcatResolutionCall015Test {

    // Fixtures are COMMITTED sources compiled at test time by ScratchCompiler (see call011).

    @Test
    @DisplayName("CALL-015 — this.baseUrl + path resolves when baseUrl is a single-literal-assigned instance field")
    void fieldConcatCase_resolvesToLiteralPlusParam() throws Exception {
        CallNode root = buildTreeForFirstEndpoint(ScratchCompiler.compile("call015", "fieldconcat"));
        printTree(root, 0);
        assertNotNull(root);
        CallNode leaf = root.getChildren().get(0).getChildren().get(0);
        assertEquals("HTTP_CALL", leaf.getCallType());
        assertEquals("/api/orders", leaf.getHttpTargetPath());
        assertEquals("order-svc", leaf.getHttpTargetModuleHint());
        assertEquals("CALL_SITE_CONCAT_PROPAGATION", leaf.getHttpTargetPathSource());
    }

    @Test
    @DisplayName("CALL-015 negative — this.baseUrl + path stays unresolved when baseUrl comes from a constructor parameter (external-config shape)")
    void fieldConcatCase_unresolvedWhenFieldIsConstructorInjected() throws Exception {
        CallNode root = buildTreeForFirstEndpoint(ScratchCompiler.compile("call015", "fieldconcatunresolved"));
        printTree(root, 0);
        assertNotNull(root);
        CallNode leaf = root.getChildren().get(0).getChildren().get(0);
        assertEquals("HTTP_CALL", leaf.getCallType());
        assertNull(leaf.getHttpTargetPath());
    }

    // ── shared harness (mirrors HttpCallResolutionCall010Test) ────────────────────────────

    private CallNode buildTreeForFirstEndpoint(Path classesDir) throws Exception {
        BytecodeClassParser parser = new BytecodeClassParser();
        DomainConfigLoader loader = new DomainConfigLoader();
        var m = DomainConfigLoader.class.getDeclaredMethod("applyDefaults");
        m.setAccessible(true);
        m.invoke(loader);
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
