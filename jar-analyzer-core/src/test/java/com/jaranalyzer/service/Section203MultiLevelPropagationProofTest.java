package com.jaranalyzer.service;

import com.jaranalyzer.model.ClassInfo;
import com.jaranalyzer.service.CallGraphIndex.IndexedClass;
import com.jaranalyzer.service.CallGraphIndex.IndexedEndpoint;
import com.jaranalyzer.service.CallGraphIndex.IndexedMethod;
import com.jaranalyzer.service.CallGraphIndex.ResolutionContext;
import com.jaranalyzer.model.CallNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * §203/§204 real proof, NOW A PERMANENT REGRESSION TEST (§213 — CALL-008): both
 * patterns below were originally a print-only gap-demonstrating harness (see §203/§204/§209/
 * §210 in docs/TASK-REGISTRY.md for the full history); §213 closed both of their own residual
 * gaps (§209's narrower-than-expected Pattern A gap — concatenation of TWO still-unresolved
 * parameters, neither a same-method literal — and §210's non-`@Document`-annotated Pattern B
 * residual), so this file now asserts the FIXED behavior directly, matching this session's own
 * established "flip proof tests into permanent regression tests" pattern (§205/§211).
 *
 * Loads REAL, javac-compiled .class files off disk (the EXACT SAME fixtures §204 built for its
 * own investigation — see docs/TASK-REGISTRY.md §204 for the exact source) through the REAL
 * BytecodeClassParser and CallTreeBuilder — no stubbed ClassInfo/MethodInfo, no mocked
 * detectors. Compiled WITH {@code javac -g} (full local-variable debug info) — the favorable
 * case for the analyzer, since it can actually name the variables; §204's own investigation
 * separately confirmed the no-debug-info case stays a bare, empty result (a real limitation of
 * bytecode without a local-variable table, not something this mechanism can improve on).
 */
class Section203MultiLevelPropagationProofTest {

    // Fixtures are COMMITTED sources compiled at test time by ScratchCompiler (see call011).
    private Path classesA, classesB;

    private Path classesA() throws IOException {
        if (classesA == null) classesA = ScratchCompiler.compile("section203", "outA2");
        return classesA;
    }

    private Path classesB() throws IOException {
        if (classesB == null) classesB = ScratchCompiler.compile("section203", "outB2");
        return classesB;
    }

    @Test
    @DisplayName("§204 Pattern A, FIXED (§213/CALL-008) — multi-level string-concatenation propagation to a Mongo collection name")
    void patternA_stringPropagation() throws IOException {
        CallNode root = buildTreeForFirstEndpoint(classesA());
        System.out.println("=== §203 PATTERN A PROOF OUTPUT START ===");
        printTree(root, 0);
        System.out.println("=== §203 PATTERN A PROOF OUTPUT END ===");

        assertNotNull(root);
        CallNode leaf = root.getChildren().get(0).getChildren().get(0).getChildren().get(0);
        assertTrue(leaf.isHasDbInteraction());
        // "region" is a genuine @PathVariable runtime value — no literal for it exists ANYWHERE
        // up the chain — so the honestly-correct FIXED result is the partial fold "orders_" +
        // "{region}" (the resolvable "prefix" side folded, an explicit placeholder — never a
        // fabricated guess — for the genuinely-unknowable side), not the empty result §204/§209
        // originally found.
        assertTrue(leaf.getCollectionsAccessed().contains("orders_{region}"),
            "§204 Pattern A must now resolve to the partial fold \"orders_{region}\" — the " +
            "concatenation's own two-operand independent ancestor-chase fix (§213).");
    }

    @Test
    @DisplayName("§204 Pattern B, FIXED (§213/CALL-008) — Class<?> propagation down the call chain to derive a collection name")
    void patternB_classPropagation() throws IOException {
        CallNode root = buildTreeForFirstEndpoint(classesB());
        System.out.println("=== §203 PATTERN B PROOF OUTPUT START ===");
        printTree(root, 0);
        System.out.println("=== §203 PATTERN B PROOF OUTPUT END ===");

        assertNotNull(root);
        CallNode leaf = root.getChildren().get(0).getChildren().get(0);
        assertTrue(leaf.isHasDbInteraction());
        assertTrue(leaf.getCollectionsAccessed().contains("invoices"),
            "§204 Pattern B must now resolve \"InvoiceEntity\" (deliberately NOT @Document-" +
            "annotated) to \"invoices\" via the bare naming-convention heuristic (§213).");
        assertTrue("CALL_SITE_CLASS_NAMING_HEURISTIC".equals(leaf.getCollectionSources().get("invoices")),
            "resolved via the naming-convention heuristic, not a confirmed @Document match — " +
            "must be tagged with the distinct, lower-confidence source so callers/reports can " +
            "tell a guess apart from a confirmed match.");
    }

    // ── shared harness ──────────────────────────────────────────────────────────

    private CallNode buildTreeForFirstEndpoint(Path classesDir) throws IOException {
        BytecodeClassParser parser = new BytecodeClassParser();
        DomainConfigLoader loader = new DomainConfigLoader();
        // No domain-config.json on the unit-test classpath — trigger the same applyDefaults()
        // fallback MongoTaggingTest uses, so collectionPattern etc. are actually populated.
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
        // Map.of() (the default) throws NPE on getOrDefault(null,...) for our sourceJar==null
        // (main-jar) test classes — swap in a mutable, null-tolerant map for this harness only.
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
            System.out.println("  indexed: " + ci.getFullyQualifiedName()
                    + " stereotype=" + ci.getStereotype());
        }

        if (ctx.endpoints.isEmpty()) {
            System.out.println("NO ENDPOINTS INDEXED — cannot build a call tree.");
            return null;
        }
        IndexedEndpoint ep = ctx.endpoints.get(0);
        System.out.println("Endpoint: " + ep.httpMethod + " " + ep.path
                + " -> " + ep.controllerFqn + "#" + ep.methodName);

        IndexedClass cls = ctx.classMap.get(ep.controllerFqn);
        IndexedMethod method = cls.methods.get(ep.methodKey);
        if (method == null) {
            // fallback: find by simple name match if the exact key format differs
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
