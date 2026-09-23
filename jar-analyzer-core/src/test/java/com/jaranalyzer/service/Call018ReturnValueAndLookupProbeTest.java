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

import static org.junit.jupiter.api.Assertions.*;

/**
 * INVESTIGATION PROBE (not a fix, not a claimed future implementation) for the CALL-018/019/020
 * planning pass, docs/TASK-REGISTRY.md §224. Answers, with real javac-compiled .class files run
 * through the REAL BytecodeClassParser/CallTreeBuilder/CollectionResolver/AggregationDetector
 * pipeline (same harness style as ResidualGapsCall012to014Test):
 *
 * <ul>
 *   <li>Q1 — does a MongoDB collection name that arrives at the real DB-call site ONLY via a
 *       chain of RETURN VALUES (ARETURN), never as a caller-supplied parameter, get resolved?</li>
 *   <li>Q2 — for an aggregation with a $lookup stage, is the $lookup "from" target captured as
 *       an ADDITIONAL collection reference, separate from (and not overwriting) the base/primary
 *       aggregation collection given to mongoTemplate.aggregate(...)?</li>
 *   <li>Q2b — does $lookup/base-collection detection depend on the pipeline argument's static
 *       type being exactly Spring Data's Aggregation, or does it also work for a raw
 *       org.bson.Document / List&lt;Document&gt; native-driver-style pipeline?</li>
 * </ul>
 *
 * Fixture sources/compiled classes live under the scratch dir referenced below (SCR/call018/...),
 * not under this test's own module, per this task's read-only-on-src/main constraint — this test
 * only ever reads real, pre-compiled .class bytes off disk, exactly like
 * ResidualGapsCall012to014Test.
 */
class Call018ReturnValueAndLookupProbeTest {

    private static final String SCRATCH =
        "/private/tmp/claude-502/-Users-ajaysingh-thakur-Desktop-bkup-25-jun-2026-new-proj-jar-beta-jar-plsql-analyzer" +
        "/461f7dbb-0af7-4cb4-9fba-050c5fdcca63/scratchpad";

    @Test
    @DisplayName("Q1/CALL-019 — collection name reaching the DB call via a 2-hop RETURN VALUE chain (C returns -> B returns -> D uses) NOW resolves (bounded multi-hop return-value chaining)")
    void q1_returnValuePropagationNotTraced() throws IOException {
        CallNode root = buildTreeForFirstEndpoint(Path.of(SCRATCH, "call018/out/q1"));
        printTree(root, 0);
        assertNotNull(root, "endpoint must be indexed");
        // A(handle) -> D(run)
        CallNode dNode = root.getChildren().get(0);
        assertEquals("run", dNode.getMethodName());

        // The literal collection name "invoices" is defined ONLY in CBuilder.resolveCollectionName(),
        // reached from D via two ARETURN hops (C -> B -> D's local `collectionName`), never as a
        // parameter. CALL-019 walks this pass-through chain (D's direct callee B is itself a pure
        // pass-through of C's own return) down to C's own single literal return.
        System.out.println("Q1 RESULT: hasDbInteraction=" + dNode.isHasDbInteraction()
                + " collectionsAccessed=" + dNode.getCollectionsAccessed()
                + " dynamicCollectionRefs=" + dNode.getDynamicCollectionRefs());

        assertTrue(dNode.getCollectionsAccessed().contains("invoices"),
            "CALL-019: a 2-hop pass-through return-value chain (D -> B.getCollectionName() " +
            "[pure pass-through] -> C.resolveCollectionName() [single literal 'invoices']) must now " +
            "resolve, extending CALL-018's single-hop lookup with a bounded multi-hop walk.");
        assertEquals("RETURN_VALUE_SINGLE_HOP_RESOLVED", dNode.getCollectionSources().get("invoices"));
    }

    @Test
    @DisplayName("CALL-018 — TRUE single-hop return-value resolution: D calls C.resolveCollectionName() " +
            "DIRECTLY (no B pass-through hop) and stores the return straight into the arg used at the DB " +
            "call site — this is the exact narrow shape CALL-018 implements and it now resolves")
    void call018_singleHopReturnValueResolves() throws IOException {
        // NOTE: this is a SEPARATE, purpose-built fixture from q1's own 2-hop (C->B->D) chain —
        // q1 was, at CALL-018 time, an intentionally-documented gap; CALL-019 (see
        // q1_returnValuePropagationNotTraced above, and call019_threeHopReturnValueChainResolves/
        // call019_chainLongerThanBoundStaysUnresolved below) closed it with a bounded multi-hop walk.
        // See docs/TASK-REGISTRY.md CALL-018 row: single ARETURN, single literal, ONE hop.
        CallNode root = buildTreeForFirstEndpoint(Path.of(SCRATCH, "call018/out/q1single"));
        printTree(root, 0);
        assertNotNull(root, "endpoint must be indexed");
        CallNode dNode = root.getChildren().get(0);
        assertEquals("run", dNode.getMethodName());

        System.out.println("CALL-018 RESULT: hasDbInteraction=" + dNode.isHasDbInteraction()
                + " collectionsAccessed=" + dNode.getCollectionsAccessed()
                + " collectionSources=" + dNode.getCollectionSources());

        assertTrue(dNode.getCollectionsAccessed().contains("invoices"),
                "CALL-018: a single-hop return value (CBuilder.resolveCollectionName() has exactly " +
                "one ARETURN of the literal \"invoices\", called directly by D and stored straight " +
                "into the mongoTemplate.aggregate(...) collection-name argument) must now resolve.");
        assertEquals("RETURN_VALUE_SINGLE_HOP_RESOLVED", dNode.getCollectionSources().get("invoices"),
                "Resolution must be tagged with the new CALL-018-specific source, distinct from any " +
                "existing CALL_SITE_* argument-flow tag, so return-flow provenance stays auditable " +
                "separately from argument-flow provenance.");
    }

    @Test
    @DisplayName("CALL-019 — a 3-hop pass-through return-value chain (D -> B -> C -> E, E has the " +
            "single literal) resolves, proving the bounded walk goes deeper than 2 hops")
    void call019_threeHopReturnValueChainResolves() throws IOException {
        CallNode root = buildTreeForFirstEndpoint(Path.of(SCRATCH, "call018/out/q1chain3"));
        printTree(root, 0);
        assertNotNull(root, "endpoint must be indexed");
        CallNode dNode = root.getChildren().get(0);
        assertEquals("run", dNode.getMethodName());

        System.out.println("CALL-019 (3-hop) RESULT: hasDbInteraction=" + dNode.isHasDbInteraction()
                + " collectionsAccessed=" + dNode.getCollectionsAccessed()
                + " collectionSources=" + dNode.getCollectionSources());

        assertTrue(dNode.getCollectionsAccessed().contains("invoices3"),
                "CALL-019: a 3-hop pass-through chain (D -> B.getCollectionName() [pass-through] -> " +
                "C.resolveCollectionName() [pass-through] -> E.resolveCollectionName() [single " +
                "literal 'invoices3']) must resolve — confirming the bound genuinely walks more " +
                "than 2 hops, not just the exact q1 fixture shape.");
        assertEquals("RETURN_VALUE_SINGLE_HOP_RESOLVED", dNode.getCollectionSources().get("invoices3"));
    }

    @Test
    @DisplayName("CALL-019 — a 5-hop pass-through return-value chain, ONE hop longer than " +
            "MAX_HOP_DEPTH (4) can walk, correctly stays UNRESOLVED (the bound is real, not just theoretical)")
    void call019_chainLongerThanBoundStaysUnresolved() throws IOException {
        CallNode root = buildTreeForFirstEndpoint(Path.of(SCRATCH, "call018/out/q1chain5"));
        printTree(root, 0);
        assertNotNull(root, "endpoint must be indexed");
        CallNode dNode = root.getChildren().get(0);
        assertEquals("run", dNode.getMethodName());

        System.out.println("CALL-019 (5-hop, bound-exceeded) RESULT: hasDbInteraction=" + dNode.isHasDbInteraction()
                + " collectionsAccessed=" + dNode.getCollectionsAccessed()
                + " dynamicCollectionRefs=" + dNode.getDynamicCollectionRefs());

        assertFalse(dNode.getCollectionsAccessed().contains("invoices5"),
                "A 5-hop pass-through chain (D -> B -> C -> E -> F -> G, G holding the single " +
                "literal) exceeds CollectionResolver.MAX_HOP_DEPTH (4) and must correctly stay " +
                "unresolved rather than guessed at — proving the bound is real, not just theoretical.");
    }

    @Test
    @DisplayName("Q2 — $lookup 'from' target is captured as an ADDITIONAL lookup reference and the base aggregation collection is still captured, unconflated (Spring Data Aggregation-object shape)")
    void q2_lookupTargetSeparateFromBaseCollection() throws IOException {
        // NOTE: collection names use lower_snake_case ("base_collection"/"other_collection"), not
        // camelCase — DomainConfigLoader.isLikelyCollectionName requires either all-lowercase
        // [a-z][a-z0-9_]* or UPPER_CASE-with-underscore; a first attempt at this fixture used
        // camelCase ("baseCollection") and was rejected by that naming heuristic alone (confirmed
        // via the diag_* tests below with a $lookup-free control case) — a fixture-naming issue,
        // not a $lookup-specific finding. Using the project's own established naming convention
        // (matching MongoTaggingTest's own documented "orders"/"ORDER_COL" choices) avoids that.
        CallNode root = buildTreeForFirstEndpoint(Path.of(SCRATCH, "call018/out/q2f"));
        printTree(root, 0);
        assertNotNull(root, "endpoint must be indexed");
        CallNode leaf = root; // handle() itself performs the aggregate() call directly
        System.out.println("Q2 RESULT: hasDbInteraction=" + leaf.isHasDbInteraction()
                + " collectionsAccessed=" + leaf.getCollectionsAccessed()
                + " collectionSources=" + leaf.getCollectionSources());

        assertTrue(leaf.getCollectionsAccessed().contains("base_collection"),
            "Base/primary aggregation collection must ALWAYS be captured, $lookup present or not " +
            "(mandatory regression-safety constraint).");
        // AggregationDetector already tracks $lookup targets separately in
        // AggregationRichDetail.lookupTargets rather than collectionsAccessed (see
        // AggregationDetector.java's PipelineDetail/lookupTargets field and CollectionResolver.java
        // lines ~1486-1493 which puts them under pipeline.put("lookupTargets", ...) distinctly from
        // the base collection) — confirm here that "other_collection" is NOT silently dropped nor
        // merged into/overwriting collectionsAccessed's "base_collection" entry.
        assertFalse(leaf.getCollectionsAccessed().contains("other_collection")
                        && leaf.getCollectionsAccessed().size() == 1,
            "other_collection must never REPLACE base_collection in collectionsAccessed.");
    }

    @Test
    @DisplayName("Q2b — same $lookup + base-collection shape built with raw org.bson.Document/List<Document> (native-driver style) instead of Aggregation object")
    void q2b_rawDocumentPipelineShape() throws IOException {
        CallNode root = buildTreeForFirstEndpoint(Path.of(SCRATCH, "call018/out/q2b_docf"));
        printTree(root, 0);
        assertNotNull(root, "endpoint must be indexed");
        CallNode leaf = root;
        System.out.println("Q2b RESULT: hasDbInteraction=" + leaf.isHasDbInteraction()
                + " collectionsAccessed=" + leaf.getCollectionsAccessed()
                + " collectionSources=" + leaf.getCollectionSources());
        assertTrue(leaf.getCollectionsAccessed().contains("base_collection_doc"),
            "Native-driver db.getCollection(\"base_collection_doc\") base collection must be " +
            "captured for the raw Document/List<Document> pipeline shape too.");
    }

    @Test
    @DisplayName("DIAGNOSTIC — isolate whether Q2's non-capture is caused by $lookup or by the static-field mongoTemplate receiver (instance-field variant, $lookup present, fixed naming)")
    void diag_instanceFieldReceiverWithLookupFixedNaming() throws IOException {
        CallNode root = buildTreeForFirstEndpoint(Path.of(SCRATCH, "call018/out/q2_instancef"));
        printTree(root, 0);
        System.out.println("DIAG instance+lookup(fixed naming) RESULT: collectionsAccessed=" + root.getCollectionsAccessed());
        assertTrue(root.getCollectionsAccessed().contains("base_collection"));
    }

    @Test
    @DisplayName("DIAGNOSTIC — static-field mongoTemplate receiver, NO $lookup, plain empty aggregation + literal base collection (fixed naming)")
    void diag_staticFieldReceiverNoLookupFixedNaming() throws IOException {
        CallNode root = buildTreeForFirstEndpoint(Path.of(SCRATCH, "call018/out/q2_nolkup_staticf"));
        printTree(root, 0);
        System.out.println("DIAG static+noLookup(fixed naming) RESULT: collectionsAccessed=" + root.getCollectionsAccessed());
        assertTrue(root.getCollectionsAccessed().contains("base_collection"));
    }

    // ── shared harness (mirrors JdbcTableResolutionCall009Test/ResidualGapsCall012to014Test) ──

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
