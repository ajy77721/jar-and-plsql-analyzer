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
 * CALL-009 (docs/TASK-REGISTRY.md §214/§215): real, javac-compiled .class-file proof that JDBC/SQL
 * table-name resolution now has the same cross-method value-tracing capability CALL-001..CALL-008
 * already built for MongoDB collection names — single-hop caller-arg resolution, bounded
 * multi-hop (reusing {@link CollectionResolver#MAX_HOP_DEPTH}), narrow literal+param SQL-string
 * concat-folding, and Class&lt;?&gt;-to-table naming-convention derivation across hops.
 *
 * Loads REAL .class files off disk through the REAL BytecodeClassParser/CallTreeBuilder/
 * CollectionResolver/JpaMethodDetector — no stubbed ClassInfo/MethodInfo, no mocked detectors —
 * matching this session's own established fixture style (see Section203MultiLevelPropagationProofTest,
 * FieldStoredMethodReferenceGapTest).
 */
class JdbcTableResolutionCall009Test {

    private static final String ROOT =
        "/private/tmp/claude-502/-Users-ajaysingh-thakur-Desktop-bkup-25-jun-2026-new-proj-jar-beta-jar-plsql-analyzer" +
        "/461f7dbb-0af7-4cb4-9fba-050c5fdcca63/scratchpad/call009/out";

    @Test
    @DisplayName("CALL-009 control case — direct literal SQL at the call site resolves (baseline)")
    void controlCase_directLiteralSql() throws IOException {
        CallNode root = buildTreeForFirstEndpoint(Path.of(ROOT, "control"));
        printTree(root, 0);
        assertNotNull(root);
        CallNode leaf = root.getChildren().get(0);
        assertTrue(leaf.isHasDbInteraction());
        assertTrue(leaf.getCollectionsAccessed().contains("orders"),
            "Baseline: a literal SQL string right at the JdbcTemplate.query() call site must " +
            "resolve to table \"orders\" — no CALL-009 propagation logic involved at all.");
    }

    @Test
    @DisplayName("CALL-009 single-hop — literal SQL passed as a caller argument now resolves")
    void singleHopCase_paramPropagation() throws IOException {
        CallNode root = buildTreeForFirstEndpoint(Path.of(ROOT, "singlehop"));
        printTree(root, 0);
        assertNotNull(root);
        CallNode leaf = root.getChildren().get(0);
        assertTrue(leaf.isHasDbInteraction());
        assertTrue(leaf.getCollectionsAccessed().contains("customers"),
            "Single-hop: the callee's own body only sees an unresolved SQL-string PARAMETER; " +
            "CALL-009 must resolve it from the CALLER's own literal argument.");
        assertEquals("CALL_SITE_SQL_PARAM_PROPAGATION", leaf.getCollectionSources().get("customers"));
        assertTrue(leaf.getDynamicCollectionRefs() == null || leaf.getDynamicCollectionRefs().isEmpty(),
            "the now-stale SQL_PARAM dynamic ref must be removed once resolved");
    }

    @Test
    @DisplayName("CALL-009 bounded multi-hop — SQL threaded unchanged through 2 wrapper methods now resolves")
    void multiHopCase_boundedPropagation() throws IOException {
        CallNode root = buildTreeForFirstEndpoint(Path.of(ROOT, "multihop"));
        printTree(root, 0);
        assertNotNull(root);
        CallNode leaf = root.getChildren().get(0).getChildren().get(0).getChildren().get(0);
        assertTrue(leaf.isHasDbInteraction());
        assertTrue(leaf.getCollectionsAccessed().contains("invoices"),
            "Multi-hop: the literal SQL was built at level 1 and threaded unchanged through 2 " +
            "wrapper methods (levels 2/3) before being used at level 4 — bounded walk-up " +
            "(CollectionResolver.MAX_HOP_DEPTH) must still find it.");
    }

    @Test
    @DisplayName("CALL-009 concat-fold — literal-prefix + resolvable-param SQL concatenation now resolves")
    void concatCase_literalParamFold() throws IOException {
        CallNode root = buildTreeForFirstEndpoint(Path.of(ROOT, "concat"));
        printTree(root, 0);
        assertNotNull(root);
        CallNode leaf = root.getChildren().get(0).getChildren().get(0);
        assertTrue(leaf.isHasDbInteraction());
        assertTrue(leaf.getCollectionsAccessed().contains("customers"),
            "Concat: \"SELECT * FROM \" + tableName is built one level below the caller that " +
            "supplies the literal tableName — CALL-009 must fold the concatenation once the " +
            "param side resolves to a literal, then parse the resulting SQL for its table name.");
        assertEquals("CALL_SITE_SQL_CONCAT_PROPAGATION", leaf.getCollectionSources().get("customers"));
    }

    @Test
    @DisplayName("CALL-009 Class<?> naming-heuristic derivation across a hop (no @Table/@Entity ground truth)")
    void classDerivationCase_namingHeuristicAcrossHop() throws IOException {
        CallNode root = buildTreeForFirstEndpoint(Path.of(ROOT, "classderiv"));
        printTree(root, 0);
        assertNotNull(root);
        CallNode leaf = root.getChildren().get(0).getChildren().get(0);
        assertTrue(leaf.isHasDbInteraction());
        assertTrue(leaf.getCollectionsAccessed().contains("invoices"),
            "InvoiceEntity.class is threaded through one wrapper method with no @Table/@Entity " +
            "ground truth reachable at the call site — must resolve via the naming-convention " +
            "heuristic (CamelCase -> snake_case, pluralized), tagged as lower-confidence.");
        assertEquals("CALL_SITE_SQL_CLASS_NAMING_HEURISTIC", leaf.getCollectionSources().get("invoices"),
            "must be tagged distinctly from a confirmed @Table/@Entity match.");
    }

    // ── shared harness (mirrors Section203MultiLevelPropagationProofTest) ──────────────────

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
