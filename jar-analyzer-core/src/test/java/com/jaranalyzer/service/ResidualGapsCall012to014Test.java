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
 * CALL-012/013/014 (docs/TASK-REGISTRY.md §219): real, javac-compiled .class-file proof closing
 * three small, documented residual gaps left open by CALL-009 (§215) and CALL-010 (§216):
 * <ul>
 *   <li>CALL-012 — JDBC `param+param` SQL-string concat-fold ({@code CollectionResolver.
 *       foldTwoParamSqlConcatIfPossible}), porting CALL-008's Mongo
 *       {@code foldTwoParamConcatIfPossible} technique to the JDBC path.</li>
 *   <li>CALL-013 — HTTP `param+param` URL-concat-fold ({@code CallTreeBuilder.
 *       foldTwoParamHttpConcatIfPossible}), same technique ported to HTTP path resolution.</li>
 *   <li>CALL-014 — JDBC {@code Class<?>}-across-hops CONFIRMED tier: {@code tagSqlClassCallSite}
 *       now has {@code ResolutionContext} threaded to it, so a propagated Class with a REAL
 *       {@code @Entity}/{@code @Table} annotation resolves via {@code ctx.entityTableMap} first,
 *       falling back to the naming-convention heuristic only when no annotation ground truth
 *       exists — matching CALL-004's Mongo tiering behavior.</li>
 * </ul>
 *
 * Loads REAL .class files off disk through the REAL BytecodeClassParser/CallTreeBuilder/
 * CollectionResolver/JpaMethodDetector pipeline — no stubbed ClassInfo/MethodInfo, no mocked
 * detectors — matching this session's own established fixture style (see
 * JdbcTableResolutionCall009Test, HttpCallResolutionCall010Test).
 */
class ResidualGapsCall012to014Test {

    // Fixtures are COMMITTED sources (src/test/resources/scratch/call012/<app>/*.java) compiled
        // at test time by ScratchCompiler — the original per-session /tmp scratchpad died with
        // the workspace move (44 NoSuchFileErrors). Same apps, same -g compile, reproducible anywhere.

    @Test
    @DisplayName("CALL-012 — JDBC param+param SQL concat-fold resolves both operands via ancestor-chain walking")
    void call012_jdbcParamParamConcatFold() throws IOException {
        CallNode root = buildTreeForFirstEndpoint(ScratchCompiler.compile("call012", "paramparam"));
        printTree(root, 0);
        assertNotNull(root);
        CallNode leaf = root.getChildren().get(0).getChildren().get(0);
        assertTrue(leaf.isHasDbInteraction());
        assertTrue(leaf.getCollectionsAccessed().contains("customers"),
            "\"SELECT * FROM \" + tableName is concatenated from TWO of the method's own " +
            "parameters (neither a same-method literal) — both are supplied as independent " +
            "literal arguments one hop up, so the fold must resolve to table \"customers\".");
        assertEquals("CALL_SITE_SQL_CONCAT_PROPAGATION", leaf.getCollectionSources().get("customers"));
        assertTrue(leaf.getDynamicCollectionRefs() == null || leaf.getDynamicCollectionRefs().isEmpty(),
            "the now-stale SQL_ dynamic ref must be removed once resolved");
    }

    @Test
    @DisplayName("CALL-013 — HTTP param+param URL concat-fold resolves both operands via ancestor-chain walking")
    void call013_httpParamParamConcatFold() throws Exception {
        CallNode root = buildTreeForFirstEndpoint(ScratchCompiler.compile("call013", "paramparam"));
        printTree(root, 0);
        assertNotNull(root);
        CallNode leaf = root.getChildren().get(0).getChildren().get(0);
        assertEquals("HTTP_CALL", leaf.getCallType());
        assertEquals("/api/orders/east", leaf.getHttpTargetPath(),
            "\"http://order-svc/api/orders/\" + suffix is concatenated from TWO of the method's " +
            "own parameters (neither a same-method literal) — both are supplied as independent " +
            "literal arguments one hop up, so the fold must resolve to /api/orders/east.");
        assertEquals("CALL_SITE_CONCAT_PROPAGATION", leaf.getHttpTargetPathSource());
    }

    @Test
    @DisplayName("CALL-014 — JDBC Class<?>-across-hops resolves via the CONFIRMED @Entity tier, not the heuristic")
    void call014_classAcrossHopsConfirmedTier() throws IOException {
        CallNode root = buildTreeForFirstEndpoint(ScratchCompiler.compile("call014", "confirmed"));
        printTree(root, 0);
        assertNotNull(root);
        CallNode leaf = root.getChildren().get(0).getChildren().get(0);
        assertTrue(leaf.isHasDbInteraction());
        assertTrue(leaf.getCollectionsAccessed().contains("INVOICE"),
            "Invoice.class carries a REAL @Entity annotation (ctx.entityTableMap ground truth: " +
            "default JPA table name INVOICE) threaded through one wrapper method — must resolve " +
            "via the CONFIRMED tier now that ResolutionContext is threaded to tagSqlClassCallSite.");
        assertEquals("CALL_SITE_SQL_CLASS_PROPAGATION", leaf.getCollectionSources().get("INVOICE"),
            "must be tagged as the CONFIRMED annotation match, distinctly from " +
            "CALL_SITE_SQL_CLASS_NAMING_HEURISTIC (the pre-CALL-014 behavior for this exact shape).");
    }

    // ── shared harness (mirrors JdbcTableResolutionCall009Test/HttpCallResolutionCall010Test) ──

    private CallNode buildTreeForFirstEndpoint(Path classesDir) throws IOException {
        BytecodeClassParser parser = new BytecodeClassParser();
        DomainConfigLoader loader = new DomainConfigLoader();
        try {
            var m = DomainConfigLoader.class.getDeclaredMethod("applyDefaults");
            m.setAccessible(true);
            m.invoke(loader);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // CALL-010's own established pattern: applyDefaults() leaves httpClientClasses empty
        // (real config comes from an external YAML this harness doesn't load) — patch in the one
        // prefix the call013 HTTP fixture needs so isHttpClientCall() routes RestTemplate calls
        // to createHttpCallLeaf; harmless no-op for the call012/call014 JDBC fixtures.
        try {
            Field httpClientClassesField = DomainConfigLoader.class.getDeclaredField("httpClientClasses");
            httpClientClassesField.setAccessible(true);
            httpClientClassesField.set(loader, List.of("org.springframework.web.client.RestTemplate"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
                    .filter(m -> m.name.equals(ep.methodName))
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
        sb.append(" hasDbInteraction=").append(node.isHasDbInteraction());
        sb.append(" collectionsAccessed=").append(node.getCollectionsAccessed());
        sb.append(" collectionSources=").append(node.getCollectionSources());
        sb.append(" dynamicCollectionRefs=").append(node.getDynamicCollectionRefs());
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
