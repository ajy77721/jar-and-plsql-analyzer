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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CALL-011 (docs/TASK-REGISTRY.md §214/§217): real, javac-compiled .class-file proof that
 * Elasticsearch index-name resolution now has the same cross-method value-tracing capability
 * CALL-001..CALL-010 already built for MongoDB collections / JDBC tables / HTTP call targets —
 * single-hop caller-arg resolution, bounded multi-hop (reusing
 * {@link CollectionResolver#MAX_HOP_DEPTH}), and narrow literal+param index-name concat-folding.
 *
 * Loads REAL .class files off disk through the REAL BytecodeClassParser/CallTreeBuilder/
 * CollectionResolver/ElasticsearchMethodDetector — no stubbed ClassInfo/MethodInfo, no mocked
 * detectors — matching this session's own established fixture style (JdbcTableResolutionCall009Test,
 * HttpCallResolutionCall010Test).
 */
class ElasticsearchIndexResolutionCall011Test {

    // Fixtures are COMMITTED sources (src/test/resources/scratch/call011/<app>/*.java) compiled at
    // test time by ScratchCompiler — the original per-session /tmp scratchpad died with the
    // workspace move (44 NoSuchFileErrors). Same apps, same -g compile, reproducible anywhere.

    @Test
    @DisplayName("CALL-011 control case — direct literal index name at the call site resolves (baseline)")
    void controlCase_directLiteralIndex() throws IOException {
        CallNode root = buildTreeForFirstEndpoint(ScratchCompiler.compile("call011", "control"));
        printTree(root, 0);
        assertNotNull(root);
        assertTrue(root.isHasDbInteraction());
        assertTrue(root.getCollectionsAccessed().contains("catalog_index"),
            "Baseline: a literal index name right at the ElasticsearchOperations.search() call " +
            "site must resolve to \"catalog_index\" — no CALL-011 propagation logic involved.");
    }

    @Test
    @DisplayName("CALL-011 single-hop — literal index passed as a caller argument now resolves")
    void singleHopCase_paramPropagation() throws IOException {
        CallNode root = buildTreeForFirstEndpoint(ScratchCompiler.compile("call011", "singlehop"));
        printTree(root, 0);
        assertNotNull(root);
        CallNode leaf = root.getChildren().get(0);
        assertTrue(leaf.isHasDbInteraction());
        assertTrue(leaf.getCollectionsAccessed().contains("customers_index"),
            "Single-hop: the callee's own body only sees an unresolved index-name PARAMETER; " +
            "CALL-011 must resolve it from the CALLER's own literal argument.");
        assertEquals("CALL_SITE_ES_PARAM_PROPAGATION", leaf.getCollectionSources().get("customers_index"));
        assertTrue(leaf.getDynamicCollectionRefs() == null || leaf.getDynamicCollectionRefs().isEmpty(),
            "the now-stale ES_PARAM dynamic ref must be removed once resolved");
    }

    @Test
    @DisplayName("CALL-011 bounded multi-hop — index name threaded unchanged through 2 wrapper methods now resolves")
    void multiHopCase_boundedPropagation() throws IOException {
        CallNode root = buildTreeForFirstEndpoint(ScratchCompiler.compile("call011", "multihop"));
        printTree(root, 0);
        assertNotNull(root);
        CallNode leaf = root.getChildren().get(0).getChildren().get(0).getChildren().get(0);
        assertTrue(leaf.isHasDbInteraction());
        assertTrue(leaf.getCollectionsAccessed().contains("invoices_index"),
            "Multi-hop: the literal index name was supplied at the entry point and threaded " +
            "unchanged through 2 wrapper methods before being used at the bottom — bounded " +
            "walk-up (CollectionResolver.MAX_HOP_DEPTH) must still find it.");
    }

    @Test
    @DisplayName("CALL-011 concat-fold — literal-prefix + resolvable-param index concatenation now resolves")
    void concatCase_literalParamFold() throws IOException {
        CallNode root = buildTreeForFirstEndpoint(ScratchCompiler.compile("call011", "concat"));
        printTree(root, 0);
        assertNotNull(root);
        CallNode leaf = root.getChildren().get(0).getChildren().get(0);
        assertTrue(leaf.isHasDbInteraction());
        assertTrue(leaf.getCollectionsAccessed().contains("orders_east"),
            "Concat: \"orders_\" + region is built one level below the caller that supplies the " +
            "literal region — CALL-011 must fold the concatenation once the param side resolves " +
            "to a literal.");
        assertEquals("CALL_SITE_ES_CONCAT_PROPAGATION", leaf.getCollectionSources().get("orders_east"));
    }

    @Test
    @DisplayName("CALL-016 — param+param index-name concatenation (neither operand a same-method literal) now resolves")
    void paramParamConcatCase_bothOperandsResolvedFromCaller() throws IOException {
        CallNode root = buildTreeForFirstEndpoint(ScratchCompiler.compile("call011", "paramparam"));
        printTree(root, 0);
        assertNotNull(root);
        CallNode leaf = root.getChildren().get(0).getChildren().get(0);
        assertTrue(leaf.isHasDbInteraction());
        assertTrue(leaf.getCollectionsAccessed().contains("orders_east"),
            "Param+param: EsService.fetch(String prefix, String region) builds \"prefix + region\" " +
            "with NEITHER operand a same-method literal; the caller supplies both as literals " +
            "(\"orders_\", \"east\") — CALL-016 must fold this the same way CALL-008 (Mongo)/" +
            "CALL-012 (JDBC)/CALL-013 (HTTP) already do for their own param+param shape.");
        assertEquals("CALL_SITE_ES_CONCAT_PROPAGATION", leaf.getCollectionSources().get("orders_east"));
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
        System.out.println(sb);
        if (node.getChildren() != null) {
            for (CallNode child : node.getChildren()) {
                printTree(child, depth + 1);
            }
        }
    }
}
