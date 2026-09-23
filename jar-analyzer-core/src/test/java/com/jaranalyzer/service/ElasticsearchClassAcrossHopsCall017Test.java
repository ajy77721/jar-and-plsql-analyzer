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
 * CALL-017 (docs/TASK-REGISTRY.md §221/§222): real, javac-compiled .class-file proof that
 * Elasticsearch index-name resolution now has the same {@code Class<?>}-across-hops capability
 * Mongo (CALL-004/CALL-008, §210/§213) and JDBC (CALL-009/CALL-014, §215/§219) already have —
 * a CONFIRMED tier reading {@code @Document(indexName = "...")} (the Elasticsearch-specific
 * attribute, distinct from Mongo's {@code collection} attribute on the SAME annotation simple
 * name), both direct-call and propagated through 2+ hops, plus a lower-confidence
 * naming-convention HEURISTIC tier for un-annotated classes threaded across hops.
 *
 * Loads REAL .class files off disk through the REAL BytecodeClassParser/CallTreeBuilder/
 * CollectionResolver/ElasticsearchMethodDetector — no stubbed ClassInfo/MethodInfo, no mocked
 * detectors — matching this session's own established fixture style
 * (ElasticsearchIndexResolutionCall011Test, JdbcTableResolutionCall009Test).
 */
class ElasticsearchClassAcrossHopsCall017Test {

    private static final String ROOT =
        "/private/tmp/claude-502/-Users-ajaysingh-thakur-Desktop-bkup-25-jun-2026-new-proj-jar-beta-jar-plsql-analyzer" +
        "/461f7dbb-0af7-4cb4-9fba-050c5fdcca63/scratchpad/call017/out";

    @Test
    @DisplayName("CALL-017 direct — Class<?> literal at the call site for an @Document(indexName=...) entity resolves")
    void directClassLiteralWithDocumentIndexNameAnnotationResolves() throws IOException {
        CallNode root = buildTreeForFirstEndpoint(Path.of(ROOT, "direct"));
        printTree(root, 0);
        assertNotNull(root);
        assertTrue(root.isHasDbInteraction());
        assertTrue(root.getCollectionsAccessed().contains("products_index"),
            "A .class literal passed directly at the ES call site for an entity annotated " +
            "@Document(indexName = \"products_index\") must resolve — this requires " +
            "CallGraphIndex to read the ES-specific \"indexName\" attribute into a NEW " +
            "entityIndexMap distinct from Mongo's \"collection\" attribute; before CALL-017 this " +
            "resolved to nothing at all (entityCollectionMap never had an entry for it).");
    }

    @Test
    @DisplayName("CALL-017 confirmed, multi-hop — Class<?> propagated through 2 hops for an @Document(indexName=...) entity resolves via the CONFIRMED tier")
    void classPropagatedThroughTwoHopsWithDocumentIndexNameAnnotationResolvesConfirmed() throws IOException {
        CallNode root = buildTreeForFirstEndpoint(Path.of(ROOT, "multihop"));
        printTree(root, 0);
        assertNotNull(root);
        CallNode leaf = root.getChildren().get(0).getChildren().get(0);
        assertTrue(leaf.isHasDbInteraction());
        assertTrue(leaf.getCollectionsAccessed().contains("catalog_index"),
            "MultiHopEsController passes AnnotatedProductEntity.class into EsServiceMid.fetch, " +
            "which forwards it unchanged into EsRepoLeaf.execute, whose own body only ever sees " +
            "an unresolved Class<?> parameter — CALL-017's resolveEsClassAcrossHops-equivalent " +
            "walk must resolve it through ctx.entityIndexMap.");
        assertEquals("CALL_SITE_ES_CLASS_PROPAGATION", leaf.getCollectionSources().get("catalog_index"),
            "must be tagged as the CONFIRMED annotation tier, not the naming-convention heuristic");
        assertTrue(leaf.getDynamicCollectionRefs() == null || leaf.getDynamicCollectionRefs().isEmpty(),
            "the now-stale ES_PARAM_CLASS dynamic ref must be removed once resolved");
    }

    @Test
    @DisplayName("CALL-017 heuristic, multi-hop — un-annotated Class<?> propagated through 2 hops resolves via the naming-convention HEURISTIC tier, tagged distinctly")
    void classPropagatedThroughTwoHopsWithoutAnnotationResolvesViaNamingHeuristic() throws IOException {
        CallNode root = buildTreeForFirstEndpoint(Path.of(ROOT, "heuristic"));
        printTree(root, 0);
        assertNotNull(root);
        CallNode leaf = root.getChildren().get(0).getChildren().get(0);
        assertTrue(leaf.isHasDbInteraction());
        assertTrue(leaf.getCollectionsAccessed().contains("catalogs"),
            "CatalogEntity.class carries no @Document annotation at all — propagated through 2 " +
            "hops, it must still resolve via the SAME lower-confidence naming-convention heuristic " +
            "Mongo/JDBC already established (CamelCase minus a common suffix, lowercased, " +
            "pluralized): CatalogEntity -> catalogs.");
        assertEquals("CALL_SITE_ES_CLASS_NAMING_HEURISTIC", leaf.getCollectionSources().get("catalogs"),
            "must be tagged as the lower-confidence heuristic tier, distinctly from the confirmed " +
            "annotation tier the previous test asserts");
    }

    // ── shared harness (mirrors ElasticsearchIndexResolutionCall011Test/JdbcTableResolutionCall009Test) ──

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
