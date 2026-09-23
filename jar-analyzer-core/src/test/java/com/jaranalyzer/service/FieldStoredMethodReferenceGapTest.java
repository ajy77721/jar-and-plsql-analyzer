package com.jaranalyzer.service;

import com.jaranalyzer.model.ClassInfo;
import com.jaranalyzer.model.EndpointInfo;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * §20x — REAL, RUN proof (not code-reading) for §200's case (b): a method reference stored in a
 * class field, assigned once in a constructor, and invoked later from an unrelated method.
 *
 * <p>Four real classes, compiled with {@code javac} into real bytecode (source under
 * {@code /private/tmp/.../scratchpad/fieldgap/src/com/demo}, compiled classes reused from that
 * scratch directory — same scratch-build pattern as {@code scratchpad/testjar}):
 *
 * <ul>
 *   <li>{@code OrderRepo.findAll()} — a plain Mongo-driver "DB access" in this analyzer's own
 *       detection idiom: {@code db.getCollection("orders").find()...}, the exact chained-call
 *       shape {@code BytecodeClassParser} keys off (see {@code getCollection}/chained-call
 *       propagation around line ~563) and {@code MongoMethodDetector} classifies as a
 *       {@code NATIVE_DRIVER} {@code READ} on collection {@code "orders"}.</li>
 *   <li>{@code OrderQueryHandler} — has an instance field {@code Supplier<List<Document>> loader}
 *       assigned via the method reference {@code repo::findAll} INSIDE THE CONSTRUCTOR, and a
 *       separate, unrelated public method {@code runQuery()} that reads the field and calls
 *       {@code loader.get()}. This is exactly §200 case (b).</li>
 *   <li>{@code OrderController} — a {@code @RestController} with {@code @GetMapping("/orders")}
 *       method {@code listOrders()} that calls {@code handler.runQuery()} — the ONLY path from
 *       the endpoint to {@code OrderRepo.findAll()} is through the field indirection.</li>
 * </ul>
 *
 * <p>This test feeds the real compiled {@code .class} bytes through the real
 * {@link BytecodeClassParser}, the real {@link CallGraphService}/{@link CallTreeBuilder} indexing
 * and tree-building pipeline (the same pipeline the running app uses for an uploaded JAR), and
 * asserts on the real resulting {@link EndpointInfo#getAggregatedCollections()} for the
 * {@code GET /orders} endpoint — the same field {@code EndpointReportBuilder}/the UI read.
 */
class FieldStoredMethodReferenceGapTest {

    private static final Path CLASSES_DIR = Path.of(
        "/private/tmp/claude-502/-Users-ajaysingh-thakur-Desktop-bkup-25-jun-2026-new-proj-jar-beta-jar-plsql-analyzer" +
        "/461f7dbb-0af7-4cb4-9fba-050c5fdcca63/scratchpad/fieldgap/classes");

    private ClassInfo parse(BytecodeClassParser parser, String simpleName) throws Exception {
        byte[] bytes = Files.readAllBytes(CLASSES_DIR.resolve("com/demo/" + simpleName + ".class"));
        ClassInfo ci = parser.parseClass(bytes);
        // Leave sourceJar null: CallTreeBuilder.indexClass treats a null sourceJar as "main jar"
        // (isMainJar = cls.getSourceJar() == null), which is required for REST_CONTROLLER/
        // @GetMapping endpoint discovery to register this class's methods as entry points.
        ci.setJarName("demo-fieldgap.jar");
        ci.setSourcePath("com/demo/" + simpleName + ".class");
        return ci;
    }

    @Test
    void fieldStoredMethodReferenceDbAccessIsResolvedByCall005() throws Exception {
        BytecodeClassParser parser = new BytecodeClassParser();
        ClassInfo repo = parse(parser, "OrderRepo");
        ClassInfo handler = parse(parser, "OrderQueryHandler");
        ClassInfo controller = parse(parser, "OrderController");

        assertNotNull(repo);
        assertNotNull(handler);
        assertNotNull(controller);

        ProgressService progressService = new ProgressService();
        DomainConfigLoader configLoader = new DomainConfigLoader();
        java.lang.reflect.Method applyDefaults = DomainConfigLoader.class.getDeclaredMethod("applyDefaults");
        applyDefaults.setAccessible(true);
        applyDefaults.invoke(configLoader);

        MongoMethodDetector mongoDetector = new MongoMethodDetector(configLoader);
        AggregationDetector aggregationDetector = new AggregationDetector(configLoader);
        JpaMethodDetector jpaDetector = new JpaMethodDetector(configLoader);
        MyBatisMethodDetector myBatisDetector = new MyBatisMethodDetector();
        RedisMethodDetector redisDetector = new RedisMethodDetector();
        CassandraMethodDetector cassandraDetector = new CassandraMethodDetector();
        ElasticsearchMethodDetector esDetector = new ElasticsearchMethodDetector();
        CollectionLifecycleDetector lifecycleDetector = new CollectionLifecycleDetector(configLoader);
        CollectionResolver resolver = new CollectionResolver(configLoader, mongoDetector,
                aggregationDetector, jpaDetector, myBatisDetector,
                redisDetector, cassandraDetector, esDetector, lifecycleDetector);
        CallTreeBuilder treeBuilder = new CallTreeBuilder(configLoader, resolver, 30, 30, 2000, 100000);
        CallGraphService service = new CallGraphService(progressService, configLoader, treeBuilder,
                new com.fasterxml.jackson.databind.ObjectMapper(), lifecycleDetector);

        service.buildIndex();
        service.setJarArtifactMap(new java.util.HashMap<>());
        service.addToIndex(repo);
        service.addToIndex(handler);
        service.addToIndex(controller);

        List<EndpointInfo> endpoints = service.buildEndpointsFromIndex();
        assertEquals(1, endpoints.size(), "exactly one @GetMapping endpoint should be discovered");

        EndpointInfo ep = endpoints.get(0);
        assertEquals("/orders", ep.getFullPath());
        assertEquals("GET", ep.getHttpMethod());
        assertEquals("listOrders", ep.getMethodName());

        // THE ACTUAL PROOF: does the endpoint report list the "orders" collection?
        // §200/§203 found this was missed — the invokedynamic (and therefore the DB edge) was
        // attached to OrderQueryHandler's <init>, which the call tree never reaches from the
        // endpoint (endpoint -> runQuery() -> loader.get(), a plain interface call). CALL-005
        // (§ see TASK-REGISTRY) fixed this: BytecodeClassParser now records the field->lambda-target
        // association when a PUTFIELD directly consumes an invokedynamic result, CallGraphIndex
        // builds a per-class fieldLambdaTargets map from it (mirroring injectedFields), and
        // CallTreeBuilder redirects the GETFIELD-sourced loader.get() call to the real target
        // (OrderRepo.findAll()) exactly like a normal isLambdaInvocation() method reference.
        System.out.println("=== REAL CAPTURED OUTPUT (post CALL-005 fix) ===");
        System.out.println("aggregatedCollections = " + ep.getAggregatedCollections());
        System.out.println("totalDbOperations = " + ep.getTotalDbOperations());
        System.out.println("callTree root method = " + (ep.getCallTree() != null ? ep.getCallTree().getMethodName() : "null"));
        printTree(ep.getCallTree(), 0);

        assertTrue(ep.getAggregatedCollections().contains("orders"),
            "CALL-005: a method reference stored directly into an instance field in the " +
            "constructor, then invoked later from an unrelated method, must now resolve back to " +
            "the real DB access — matching the direct-call control case below.");
    }

    /**
     * Control/sanity case: {@code DirectController.listOrdersDirect()} calls
     * {@code OrderRepo.findAll()} DIRECTLY — no field-stored method-reference indirection at all.
     * This proves the "orders" collection detection mechanism itself (bytecode parsing, chained
     * getCollection().find() detection, call-tree walk, {@code computeAggregates()}) works
     * correctly on this exact class/method pair when reached by a normal direct call edge —
     * ruling out "the harness/setup is broken" as an explanation for the empty result in
     * {@link #fieldStoredMethodReferenceDbAccessIsMissedByTheEndpointReport()} above.
     */
    @Test
    void sameDbAccessIsCorrectlyFoundWhenReachedByADirectCallNoFieldIndirection() throws Exception {
        BytecodeClassParser parser = new BytecodeClassParser();
        ClassInfo repo = parse(parser, "OrderRepo");
        ClassInfo controller = parse(parser, "DirectController");

        ProgressService progressService = new ProgressService();
        DomainConfigLoader configLoader = new DomainConfigLoader();
        java.lang.reflect.Method applyDefaults = DomainConfigLoader.class.getDeclaredMethod("applyDefaults");
        applyDefaults.setAccessible(true);
        applyDefaults.invoke(configLoader);

        MongoMethodDetector mongoDetector = new MongoMethodDetector(configLoader);
        AggregationDetector aggregationDetector = new AggregationDetector(configLoader);
        JpaMethodDetector jpaDetector = new JpaMethodDetector(configLoader);
        MyBatisMethodDetector myBatisDetector = new MyBatisMethodDetector();
        RedisMethodDetector redisDetector = new RedisMethodDetector();
        CassandraMethodDetector cassandraDetector = new CassandraMethodDetector();
        ElasticsearchMethodDetector esDetector = new ElasticsearchMethodDetector();
        CollectionLifecycleDetector lifecycleDetector = new CollectionLifecycleDetector(configLoader);
        CollectionResolver resolver = new CollectionResolver(configLoader, mongoDetector,
                aggregationDetector, jpaDetector, myBatisDetector,
                redisDetector, cassandraDetector, esDetector, lifecycleDetector);
        CallTreeBuilder treeBuilder = new CallTreeBuilder(configLoader, resolver, 30, 30, 2000, 100000);
        CallGraphService service = new CallGraphService(progressService, configLoader, treeBuilder,
                new com.fasterxml.jackson.databind.ObjectMapper(), lifecycleDetector);

        service.buildIndex();
        service.setJarArtifactMap(new java.util.HashMap<>());
        service.addToIndex(repo);
        service.addToIndex(controller);

        List<EndpointInfo> endpoints = service.buildEndpointsFromIndex();
        assertEquals(1, endpoints.size());
        EndpointInfo ep = endpoints.get(0);
        assertEquals("/orders-direct", ep.getFullPath());

        System.out.println("=== CONTROL CASE — REAL CAPTURED OUTPUT ===");
        System.out.println("aggregatedCollections = " + ep.getAggregatedCollections());
        printTree(ep.getCallTree(), 0);

        assertTrue(ep.getAggregatedCollections().contains("orders"),
            "control case: the SAME OrderRepo.findAll() DB access, reached by a normal direct " +
            "call (no field indirection), must be found — otherwise the negative result above " +
            "would be a harness bug, not a real analyzer gap");
    }

    /**
     * CALL-001 proof: a NEW one-hop-only case (no field indirection, no multi-level chain) —
     * {@code ParamController.listOrdersParam()} calls {@code ParamRepositoryChild.execute(
     * "orders_param")} directly, passing a LITERAL collection name into a parameter that the
     * callee's own body then uses as its Mongo collection-name argument. Before CALL-001, this
     * would only ever show up as {@code collectionsAccessed=[]} plus a bare PARAM-kind
     * {@code dynamicCollectionRefs} entry on the child node (mirroring §203/§204's gap shape,
     * just for an arbitrary callee rather than BulkWriteCollector). CALL-001 generalizes
     * {@code CollectionResolver.tagCallSiteCollection} to resolve this single-hop case for ANY
     * callee, so this now asserts the FIXED behavior directly (not a gap).
     */
    @Test
    void call001SingleHopAnyParameterCollectionPropagationIsResolved() throws Exception {
        BytecodeClassParser parser = new BytecodeClassParser();
        ClassInfo child = parse(parser, "ParamRepositoryChild");
        ClassInfo controller = parse(parser, "ParamController");

        ProgressService progressService = new ProgressService();
        DomainConfigLoader configLoader = new DomainConfigLoader();
        java.lang.reflect.Method applyDefaults = DomainConfigLoader.class.getDeclaredMethod("applyDefaults");
        applyDefaults.setAccessible(true);
        applyDefaults.invoke(configLoader);

        MongoMethodDetector mongoDetector = new MongoMethodDetector(configLoader);
        AggregationDetector aggregationDetector = new AggregationDetector(configLoader);
        JpaMethodDetector jpaDetector = new JpaMethodDetector(configLoader);
        MyBatisMethodDetector myBatisDetector = new MyBatisMethodDetector();
        RedisMethodDetector redisDetector = new RedisMethodDetector();
        CassandraMethodDetector cassandraDetector = new CassandraMethodDetector();
        ElasticsearchMethodDetector esDetector = new ElasticsearchMethodDetector();
        CollectionLifecycleDetector lifecycleDetector = new CollectionLifecycleDetector(configLoader);
        CollectionResolver resolver = new CollectionResolver(configLoader, mongoDetector,
                aggregationDetector, jpaDetector, myBatisDetector,
                redisDetector, cassandraDetector, esDetector, lifecycleDetector);
        CallTreeBuilder treeBuilder = new CallTreeBuilder(configLoader, resolver, 30, 30, 2000, 100000);
        CallGraphService service = new CallGraphService(progressService, configLoader, treeBuilder,
                new com.fasterxml.jackson.databind.ObjectMapper(), lifecycleDetector);

        service.buildIndex();
        service.setJarArtifactMap(new java.util.HashMap<>());
        service.addToIndex(child);
        service.addToIndex(controller);

        List<EndpointInfo> endpoints = service.buildEndpointsFromIndex();
        assertEquals(1, endpoints.size());
        EndpointInfo ep = endpoints.get(0);
        assertEquals("/orders-param", ep.getFullPath());

        System.out.println("=== CALL-001 SINGLE-HOP CASE — REAL CAPTURED OUTPUT ===");
        System.out.println("aggregatedCollections = " + ep.getAggregatedCollections());
        printTree(ep.getCallTree(), 0);

        assertTrue(ep.getAggregatedCollections().contains("orders_param"),
            "CALL-001: a literal collection name passed one hop into a callee's parameter, where " +
            "the callee's own body uses that parameter as its Mongo collection-name argument, " +
            "must now resolve — this is the whole scope of CALL-001's single-hop generalization.");
    }

    /**
     * CALL-002 proof: a pure parameter-forwarding chain, 3 hops deep — {@code HopController}
     * calls {@code HopServiceA.callA("orders_hop")} (a LITERAL, at the top), which forwards its
     * own parameter unchanged into {@code HopServiceB.callB(collectionName)}, which forwards ITS
     * own parameter unchanged into {@code HopRepoLeaf.execute(collectionName)}, whose own body
     * uses the parameter as its Mongo collection-name argument. No string concatenation and no
     * Class&lt;?&gt; derivation anywhere in this chain (those are CALL-003/CALL-004's separate
     * concerns) — this isolates CALL-002's own scope (bounded multi-hop walk-up through pure
     * pass-through parameters) from the OTHER gaps identified in the same {@code Section203...}
     * proof fixture, which additionally require concatenation/Class-derivation resolution.
     *
     * Before CALL-002: {@code tagCallSiteCollection} only resolved a SINGLE hop (CALL-001), so
     * this exact 3-hop chain would still show {@code collectionsAccessed=[]} at every node with
     * an unresolved PARAM-kind {@code dynamicCollectionRefs} entry at the leaf — this is precisely
     * §206's own re-run of {@code Section203MultiLevelPropagationProofTest}'s Pattern A, which
     * confirmed it stayed unresolved by design. CALL-002 walks the ancestor call-site chain
     * (bounded at {@code CollectionResolver.MAX_HOP_DEPTH = 4} call sites total) to find the
     * literal 3 hops up, so this now asserts the FIXED behavior directly.
     */
    @Test
    void call002ThreeHopPureParameterPassThroughChainIsResolved() throws Exception {
        BytecodeClassParser parser = new BytecodeClassParser();
        ClassInfo leaf = parse(parser, "HopRepoLeaf");
        ClassInfo serviceB = parse(parser, "HopServiceB");
        ClassInfo serviceA = parse(parser, "HopServiceA");
        ClassInfo controller = parse(parser, "HopController");

        ProgressService progressService = new ProgressService();
        DomainConfigLoader configLoader = new DomainConfigLoader();
        java.lang.reflect.Method applyDefaults = DomainConfigLoader.class.getDeclaredMethod("applyDefaults");
        applyDefaults.setAccessible(true);
        applyDefaults.invoke(configLoader);

        MongoMethodDetector mongoDetector = new MongoMethodDetector(configLoader);
        AggregationDetector aggregationDetector = new AggregationDetector(configLoader);
        JpaMethodDetector jpaDetector = new JpaMethodDetector(configLoader);
        MyBatisMethodDetector myBatisDetector = new MyBatisMethodDetector();
        RedisMethodDetector redisDetector = new RedisMethodDetector();
        CassandraMethodDetector cassandraDetector = new CassandraMethodDetector();
        ElasticsearchMethodDetector esDetector = new ElasticsearchMethodDetector();
        CollectionLifecycleDetector lifecycleDetector = new CollectionLifecycleDetector(configLoader);
        CollectionResolver resolver = new CollectionResolver(configLoader, mongoDetector,
                aggregationDetector, jpaDetector, myBatisDetector,
                redisDetector, cassandraDetector, esDetector, lifecycleDetector);
        CallTreeBuilder treeBuilder = new CallTreeBuilder(configLoader, resolver, 30, 30, 2000, 100000);
        CallGraphService service = new CallGraphService(progressService, configLoader, treeBuilder,
                new com.fasterxml.jackson.databind.ObjectMapper(), lifecycleDetector);

        service.buildIndex();
        service.setJarArtifactMap(new java.util.HashMap<>());
        service.addToIndex(leaf);
        service.addToIndex(serviceB);
        service.addToIndex(serviceA);
        service.addToIndex(controller);

        List<EndpointInfo> endpoints = service.buildEndpointsFromIndex();
        assertEquals(1, endpoints.size());
        EndpointInfo ep = endpoints.get(0);
        assertEquals("/orders-hop", ep.getFullPath());

        System.out.println("=== CALL-002 THREE-HOP PASS-THROUGH CASE — REAL CAPTURED OUTPUT ===");
        System.out.println("aggregatedCollections = " + ep.getAggregatedCollections());
        printTree(ep.getCallTree(), 0);

        assertTrue(ep.getAggregatedCollections().contains("orders_hop"),
            "CALL-002: a literal collection name supplied at the TOP of a 3-hop pure parameter " +
            "pass-through chain (no concatenation, no Class<?> derivation) must now resolve all " +
            "the way down to the leaf that actually uses it as a Mongo collection-name argument.");
    }

    /**
     * CALL-003 proof, positive case: {@code ConcatEntryController.handle()} assigns
     * {@code String region = "east";} (single-assignment literal, resolved the same way today's
     * existing mechanism already handles it) then calls {@code svc.fetchOrders(region)}.
     * {@code ConcatService.fetchOrders(String region)} builds {@code "orders_" + region} — the
     * narrow `literal + param` shape — and forwards the result into
     * {@code ConcatRepoLeaf.execute(collectionName)}, whose own body uses it as a Mongo
     * collection-name argument. Before CALL-003, {@code coll} would only ever be captured as a
     * bare, unresolved {@code LOCAL_VAR}-kind dynamic ref (exactly §204 Pattern A's own shape) —
     * CALL-003 now folds the literal prefix with the param's own (fully resolvable, one hop up)
     * value, so this asserts the end-to-end FIXED behavior.
     */
    @Test
    void call003LiteralPlusParamConcatenationIsResolved() throws Exception {
        BytecodeClassParser parser = new BytecodeClassParser();
        ClassInfo leaf = parse(parser, "ConcatRepoLeaf");
        ClassInfo svc = parse(parser, "ConcatService");
        ClassInfo controller = parse(parser, "ConcatEntryController");

        ProgressService progressService = new ProgressService();
        DomainConfigLoader configLoader = new DomainConfigLoader();
        java.lang.reflect.Method applyDefaults = DomainConfigLoader.class.getDeclaredMethod("applyDefaults");
        applyDefaults.setAccessible(true);
        applyDefaults.invoke(configLoader);

        MongoMethodDetector mongoDetector = new MongoMethodDetector(configLoader);
        AggregationDetector aggregationDetector = new AggregationDetector(configLoader);
        JpaMethodDetector jpaDetector = new JpaMethodDetector(configLoader);
        MyBatisMethodDetector myBatisDetector = new MyBatisMethodDetector();
        RedisMethodDetector redisDetector = new RedisMethodDetector();
        CassandraMethodDetector cassandraDetector = new CassandraMethodDetector();
        ElasticsearchMethodDetector esDetector = new ElasticsearchMethodDetector();
        CollectionLifecycleDetector lifecycleDetector = new CollectionLifecycleDetector(configLoader);
        CollectionResolver resolver = new CollectionResolver(configLoader, mongoDetector,
                aggregationDetector, jpaDetector, myBatisDetector,
                redisDetector, cassandraDetector, esDetector, lifecycleDetector);
        CallTreeBuilder treeBuilder = new CallTreeBuilder(configLoader, resolver, 30, 30, 2000, 100000);
        CallGraphService service = new CallGraphService(progressService, configLoader, treeBuilder,
                new com.fasterxml.jackson.databind.ObjectMapper(), lifecycleDetector);

        service.buildIndex();
        service.setJarArtifactMap(new java.util.HashMap<>());
        service.addToIndex(leaf);
        service.addToIndex(svc);
        service.addToIndex(controller);

        List<EndpointInfo> endpoints = service.buildEndpointsFromIndex();
        assertEquals(1, endpoints.size());
        EndpointInfo ep = endpoints.get(0);
        assertEquals("/orders-concat", ep.getFullPath());

        System.out.println("=== CALL-003 LITERAL+PARAM CONCAT CASE — REAL CAPTURED OUTPUT ===");
        System.out.println("aggregatedCollections = " + ep.getAggregatedCollections());
        printTree(ep.getCallTree(), 0);

        assertTrue(ep.getAggregatedCollections().contains("orders_east"),
            "CALL-003: `\"orders_\" + region` where region itself resolves to the literal \"east\" " +
            "one hop up must fold end-to-end to \"orders_east\" — the narrow literal+param " +
            "concatenation shape this task targets.");
    }

    /**
     * CALL-003 proof, trivial fully-literal case: {@code "orders_" + "east"} — javac itself
     * constant-folds this away entirely before it ever reaches an invokedynamic (confirmed via
     * javap on the real compiled fixture — see docs/TASK-REGISTRY.md), so this is really a
     * sanity/baseline check that the existing (untouched) plain-literal mechanism still handles
     * it, not a new code path CALL-003 introduces.
     */
    @Test
    void call003FullyLiteralConcatenationAlreadyResolvedByJavac() throws Exception {
        BytecodeClassParser parser = new BytecodeClassParser();
        ClassInfo controller = parse(parser, "LiteralConcatController");

        ProgressService progressService = new ProgressService();
        DomainConfigLoader configLoader = new DomainConfigLoader();
        java.lang.reflect.Method applyDefaults = DomainConfigLoader.class.getDeclaredMethod("applyDefaults");
        applyDefaults.setAccessible(true);
        applyDefaults.invoke(configLoader);

        MongoMethodDetector mongoDetector = new MongoMethodDetector(configLoader);
        AggregationDetector aggregationDetector = new AggregationDetector(configLoader);
        JpaMethodDetector jpaDetector = new JpaMethodDetector(configLoader);
        MyBatisMethodDetector myBatisDetector = new MyBatisMethodDetector();
        RedisMethodDetector redisDetector = new RedisMethodDetector();
        CassandraMethodDetector cassandraDetector = new CassandraMethodDetector();
        ElasticsearchMethodDetector esDetector = new ElasticsearchMethodDetector();
        CollectionLifecycleDetector lifecycleDetector = new CollectionLifecycleDetector(configLoader);
        CollectionResolver resolver = new CollectionResolver(configLoader, mongoDetector,
                aggregationDetector, jpaDetector, myBatisDetector,
                redisDetector, cassandraDetector, esDetector, lifecycleDetector);
        CallTreeBuilder treeBuilder = new CallTreeBuilder(configLoader, resolver, 30, 30, 2000, 100000);
        CallGraphService service = new CallGraphService(progressService, configLoader, treeBuilder,
                new com.fasterxml.jackson.databind.ObjectMapper(), lifecycleDetector);

        service.buildIndex();
        service.setJarArtifactMap(new java.util.HashMap<>());
        service.addToIndex(controller);

        List<EndpointInfo> endpoints = service.buildEndpointsFromIndex();
        assertEquals(1, endpoints.size());
        EndpointInfo ep = endpoints.get(0);
        assertEquals("/orders-literal-concat", ep.getFullPath());

        System.out.println("=== CALL-003 FULLY-LITERAL CONCAT CASE — REAL CAPTURED OUTPUT ===");
        System.out.println("aggregatedCollections = " + ep.getAggregatedCollections());
        printTree(ep.getCallTree(), 0);

        assertTrue(ep.getAggregatedCollections().contains("orders_east"),
            "a fully-literal concatenation must resolve — trivially, since javac itself folds it " +
            "into a single LDC before this analyzer ever sees an invokedynamic.");
    }

    /**
     * CALL-003 negative case: {@code prefix + region} concatenates the method's OWN TWO
     * parameters — neither operand is a literal in this method's own context, outside CALL-003's
     * narrow scope (`literal + param` / `param + literal` only). Must correctly fall back to an
     * unresolved dynamic reference, exactly as before this task — never fabricate a guessed name.
     */
    @Test
    void call003TwoNonConstantOperandsCorrectlyStaysUnresolved() throws Exception {
        BytecodeClassParser parser = new BytecodeClassParser();
        ClassInfo leaf = parse(parser, "ConcatRepoLeaf");
        ClassInfo controller = parse(parser, "NegativeConcatController");

        ProgressService progressService = new ProgressService();
        DomainConfigLoader configLoader = new DomainConfigLoader();
        java.lang.reflect.Method applyDefaults = DomainConfigLoader.class.getDeclaredMethod("applyDefaults");
        applyDefaults.setAccessible(true);
        applyDefaults.invoke(configLoader);

        MongoMethodDetector mongoDetector = new MongoMethodDetector(configLoader);
        AggregationDetector aggregationDetector = new AggregationDetector(configLoader);
        JpaMethodDetector jpaDetector = new JpaMethodDetector(configLoader);
        MyBatisMethodDetector myBatisDetector = new MyBatisMethodDetector();
        RedisMethodDetector redisDetector = new RedisMethodDetector();
        CassandraMethodDetector cassandraDetector = new CassandraMethodDetector();
        ElasticsearchMethodDetector esDetector = new ElasticsearchMethodDetector();
        CollectionLifecycleDetector lifecycleDetector = new CollectionLifecycleDetector(configLoader);
        CollectionResolver resolver = new CollectionResolver(configLoader, mongoDetector,
                aggregationDetector, jpaDetector, myBatisDetector,
                redisDetector, cassandraDetector, esDetector, lifecycleDetector);
        CallTreeBuilder treeBuilder = new CallTreeBuilder(configLoader, resolver, 30, 30, 2000, 100000);
        CallGraphService service = new CallGraphService(progressService, configLoader, treeBuilder,
                new com.fasterxml.jackson.databind.ObjectMapper(), lifecycleDetector);

        service.buildIndex();
        service.setJarArtifactMap(new java.util.HashMap<>());
        service.addToIndex(leaf);
        service.addToIndex(controller);

        List<EndpointInfo> endpoints = service.buildEndpointsFromIndex();
        assertEquals(1, endpoints.size());
        EndpointInfo ep = endpoints.get(0);
        assertEquals("/orders-negative-concat", ep.getFullPath());

        System.out.println("=== CALL-003 NEGATIVE CASE (2 non-constant operands) — REAL CAPTURED OUTPUT ===");
        System.out.println("aggregatedCollections = " + ep.getAggregatedCollections());
        printTree(ep.getCallTree(), 0);

        assertTrue(ep.getAggregatedCollections().isEmpty(),
            "concatenating two of the method's OWN non-constant parameters is outside CALL-003's " +
            "narrow scope and must stay unresolved (no fabricated collection name) — collections " +
            "must remain empty, matching the pre-CALL-003 unresolved-LOCAL_VAR behavior.");
    }

    /**
     * CALL-004 control case: {@code AnnotatedInvoiceEntity} IS {@code @Document(collection =
     * "invoices")}-annotated, and {@code DirectClassRepo.findDirect()} passes the {@code .class}
     * literal DIRECTLY at the same call site as {@code mongoTemplate.find(new Query(),
     * AnnotatedInvoiceEntity.class)} — no propagation at all. This is the pre-existing path
     * (MongoMethodDetector's own {@code __class:} inline resolution against
     * {@code entityCollectionMap}) that this task's own instructions require confirming still
     * works, so CALL-004's NEW multi-hop mechanism below can be shown as a genuine addition, not a
     * fix to something that was already broken.
     */
    @Test
    void call004DirectClassLiteralWithDocumentAnnotationResolves() throws Exception {
        BytecodeClassParser parser = new BytecodeClassParser();
        ClassInfo entity = parse(parser, "AnnotatedInvoiceEntity");
        ClassInfo repo = parse(parser, "DirectClassRepo");
        ClassInfo controller = parse(parser, "DirectClassController");

        ProgressService progressService = new ProgressService();
        DomainConfigLoader configLoader = new DomainConfigLoader();
        java.lang.reflect.Method applyDefaults = DomainConfigLoader.class.getDeclaredMethod("applyDefaults");
        applyDefaults.setAccessible(true);
        applyDefaults.invoke(configLoader);

        MongoMethodDetector mongoDetector = new MongoMethodDetector(configLoader);
        AggregationDetector aggregationDetector = new AggregationDetector(configLoader);
        JpaMethodDetector jpaDetector = new JpaMethodDetector(configLoader);
        MyBatisMethodDetector myBatisDetector = new MyBatisMethodDetector();
        RedisMethodDetector redisDetector = new RedisMethodDetector();
        CassandraMethodDetector cassandraDetector = new CassandraMethodDetector();
        ElasticsearchMethodDetector esDetector = new ElasticsearchMethodDetector();
        CollectionLifecycleDetector lifecycleDetector = new CollectionLifecycleDetector(configLoader);
        CollectionResolver resolver = new CollectionResolver(configLoader, mongoDetector,
                aggregationDetector, jpaDetector, myBatisDetector,
                redisDetector, cassandraDetector, esDetector, lifecycleDetector);
        CallTreeBuilder treeBuilder = new CallTreeBuilder(configLoader, resolver, 30, 30, 2000, 100000);
        CallGraphService service = new CallGraphService(progressService, configLoader, treeBuilder,
                new com.fasterxml.jackson.databind.ObjectMapper(), lifecycleDetector);

        service.buildIndex();
        service.setJarArtifactMap(new java.util.HashMap<>());
        service.addToIndex(entity);
        service.addToIndex(repo);
        service.addToIndex(controller);

        List<EndpointInfo> endpoints = service.buildEndpointsFromIndex();
        assertEquals(1, endpoints.size());
        EndpointInfo ep = endpoints.get(0);
        assertEquals("/invoices-direct-class", ep.getFullPath());

        System.out.println("=== CALL-004 DIRECT @Document CLASS LITERAL — REAL CAPTURED OUTPUT ===");
        System.out.println("aggregatedCollections = " + ep.getAggregatedCollections());
        printTree(ep.getCallTree(), 0);

        assertTrue(ep.getAggregatedCollections().contains("invoices"),
            "CALL-004 control: a Class<?> literal passed DIRECTLY at the Mongo call site, for an " +
            "@Document-annotated entity, must resolve via the pre-existing entityCollectionMap " +
            "lookup — confirms the direct case already works before/independent of this task's " +
            "new multi-hop mechanism.");
    }

    /**
     * CALL-004 proof, the actual gap this task closes: {@code MultiHopClassController.
     * getInvoicesMultiHop()} passes {@code AnnotatedInvoiceEntity.class} (an @Document-annotated
     * entity) into {@code ClassServiceMid.fetch(Class<?> entityClass)}, which forwards its OWN
     * parameter unchanged into {@code ClassRepoLeaf.execute(Class<?> entityClass)}, whose own body
     * uses {@code entityClass} (never a literal in ITS OWN context) directly as
     * {@code mongoTemplate.find(new Query(), entityClass)}'s entity-class argument. Before
     * CALL-004, {@code BytecodeClassParser}'s dynamic-arg ALOAD tracking only recognized
     * String-typed locals/params, so a {@code Class<?>}-typed parameter used this way produced NO
     * dynamicCollectionRefs entry at all (not even an unresolved one) — this exact shape is §204
     * Pattern B's own "never even reaches deriveCollectionName's own logic" finding, minus the
     * naming-convention derivation step (this fixture resolves via @Document annotation instead,
     * the higher-confidence sub-case this task scopes itself to). CALL-004 adds a new
     * PARAM_CLASS/LOCAL_VAR_CLASS dynamic-arg kind plus a bounded ancestor walk
     * (resolveClassAcrossHops) that finds the {@code .class} literal 2 hops up and resolves it
     * through the SAME entityCollectionMap lookup as the direct case above.
     */
    @Test
    void call004ClassPropagatedThroughTwoHopsWithDocumentAnnotationResolves() throws Exception {
        BytecodeClassParser parser = new BytecodeClassParser();
        ClassInfo entity = parse(parser, "AnnotatedInvoiceEntity");
        ClassInfo leaf = parse(parser, "ClassRepoLeaf");
        ClassInfo mid = parse(parser, "ClassServiceMid");
        ClassInfo controller = parse(parser, "MultiHopClassController");

        ProgressService progressService = new ProgressService();
        DomainConfigLoader configLoader = new DomainConfigLoader();
        java.lang.reflect.Method applyDefaults = DomainConfigLoader.class.getDeclaredMethod("applyDefaults");
        applyDefaults.setAccessible(true);
        applyDefaults.invoke(configLoader);

        MongoMethodDetector mongoDetector = new MongoMethodDetector(configLoader);
        AggregationDetector aggregationDetector = new AggregationDetector(configLoader);
        JpaMethodDetector jpaDetector = new JpaMethodDetector(configLoader);
        MyBatisMethodDetector myBatisDetector = new MyBatisMethodDetector();
        RedisMethodDetector redisDetector = new RedisMethodDetector();
        CassandraMethodDetector cassandraDetector = new CassandraMethodDetector();
        ElasticsearchMethodDetector esDetector = new ElasticsearchMethodDetector();
        CollectionLifecycleDetector lifecycleDetector = new CollectionLifecycleDetector(configLoader);
        CollectionResolver resolver = new CollectionResolver(configLoader, mongoDetector,
                aggregationDetector, jpaDetector, myBatisDetector,
                redisDetector, cassandraDetector, esDetector, lifecycleDetector);
        CallTreeBuilder treeBuilder = new CallTreeBuilder(configLoader, resolver, 30, 30, 2000, 100000);
        CallGraphService service = new CallGraphService(progressService, configLoader, treeBuilder,
                new com.fasterxml.jackson.databind.ObjectMapper(), lifecycleDetector);

        service.buildIndex();
        service.setJarArtifactMap(new java.util.HashMap<>());
        service.addToIndex(entity);
        service.addToIndex(leaf);
        service.addToIndex(mid);
        service.addToIndex(controller);

        List<EndpointInfo> endpoints = service.buildEndpointsFromIndex();
        assertEquals(1, endpoints.size());
        EndpointInfo ep = endpoints.get(0);
        assertEquals("/invoices-multihop-class", ep.getFullPath());

        System.out.println("=== CALL-004 TWO-HOP CLASS<?> PROPAGATION (@Document) — REAL CAPTURED OUTPUT ===");
        System.out.println("aggregatedCollections = " + ep.getAggregatedCollections());
        printTree(ep.getCallTree(), 0);

        assertTrue(ep.getAggregatedCollections().contains("invoices"),
            "CALL-004: a Class<?> value threaded through 2 method calls, for an @Document-" +
            "annotated entity, must now resolve to the entity's real collection name — this is " +
            "the actual gap CALL-004 closes (previously: no dynamicCollectionRefs entry at all).");
    }

    /**
     * §209/§213 (CALL-008) proof, closing §204 Pattern A's own real gap: unlike
     * {@link #call003LiteralPlusParamConcatenationIsResolved}'s `"orders_" + region` (where
     * {@code region} resolves to a literal ONE hop up), THIS fixture concatenates the method's
     * OWN two parameters — `prefix + region` inside {@code TwoParamConcatAppender.append} —
     * where {@code region} is a genuine {@code @PathVariable} runtime value with NO literal
     * ANYWHERE up the chain, and {@code prefix} only becomes a real literal TWO hops further up
     * (§204 Pattern A's own exact shape). Before this fix, {@code tryFoldStringConcat} refused
     * this `param + param` shape entirely (see {@code BytecodeClassParser}'s own "both
     * non-literal... out of scope" comment), so it never even produced a
     * {@code CONCAT_PARAM_PARAM} dynamic-arg entry — the concatenation result showed up as a
     * bare, unresolved {@code LOCAL_VAR}. The fix chases EACH operand up its own ancestor chain
     * independently and, when one side resolves to a literal but the other is a genuine
     * runtime-only value, folds to a PARTIAL result ({@code "orders_{region}"}) rather than
     * fabricating a guessed value or reporting nothing at all.
     */
    @Test
    void call006TwoParamConcatenationWithOneGenuinelyRuntimeOperandPartiallyResolves() throws Exception {
        BytecodeClassParser parser = new BytecodeClassParser();
        ClassInfo leaf = parse(parser, "TwoParamConcatRepoLeaf");
        ClassInfo appender = parse(parser, "TwoParamConcatAppender");
        ClassInfo svc = parse(parser, "TwoParamConcatService");
        ClassInfo controller = parse(parser, "TwoParamConcatController");

        ProgressService progressService = new ProgressService();
        DomainConfigLoader configLoader = new DomainConfigLoader();
        java.lang.reflect.Method applyDefaults = DomainConfigLoader.class.getDeclaredMethod("applyDefaults");
        applyDefaults.setAccessible(true);
        applyDefaults.invoke(configLoader);

        MongoMethodDetector mongoDetector = new MongoMethodDetector(configLoader);
        AggregationDetector aggregationDetector = new AggregationDetector(configLoader);
        JpaMethodDetector jpaDetector = new JpaMethodDetector(configLoader);
        MyBatisMethodDetector myBatisDetector = new MyBatisMethodDetector();
        RedisMethodDetector redisDetector = new RedisMethodDetector();
        CassandraMethodDetector cassandraDetector = new CassandraMethodDetector();
        ElasticsearchMethodDetector esDetector = new ElasticsearchMethodDetector();
        CollectionLifecycleDetector lifecycleDetector = new CollectionLifecycleDetector(configLoader);
        CollectionResolver resolver = new CollectionResolver(configLoader, mongoDetector,
                aggregationDetector, jpaDetector, myBatisDetector,
                redisDetector, cassandraDetector, esDetector, lifecycleDetector);
        CallTreeBuilder treeBuilder = new CallTreeBuilder(configLoader, resolver, 30, 30, 2000, 100000);
        CallGraphService service = new CallGraphService(progressService, configLoader, treeBuilder,
                new com.fasterxml.jackson.databind.ObjectMapper(), lifecycleDetector);

        service.buildIndex();
        service.setJarArtifactMap(new java.util.HashMap<>());
        service.addToIndex(leaf);
        service.addToIndex(appender);
        service.addToIndex(svc);
        service.addToIndex(controller);

        List<EndpointInfo> endpoints = service.buildEndpointsFromIndex();
        assertEquals(1, endpoints.size());
        EndpointInfo ep = endpoints.get(0);
        assertEquals("/orders-twoparam/{region}", ep.getFullPath());

        System.out.println("=== CALL-008 TWO-PARAM CONCAT (PARTIAL) — REAL CAPTURED OUTPUT ===");
        System.out.println("aggregatedCollections = " + ep.getAggregatedCollections());
        printTree(ep.getCallTree(), 0);

        assertTrue(ep.getAggregatedCollections().contains("orders_{region}"),
            "CALL-008: `prefix + region` where prefix resolves 2 hops up but region is " +
            "a genuine @PathVariable runtime value (no literal anywhere up the chain) must now " +
            "fold to the honestly-partial \"orders_{region}\" — the resolvable part reported, a " +
            "placeholder (never a guess) for the genuinely-unknowable part.");
    }

    /**
     * §209/§213 (CALL-008) companion case: the SAME {@code TwoParamConcatService/
     * Appender/RepoLeaf} chain, but BOTH operands terminate in a real literal up the chain (no
     * {@code @PathVariable} at all) — proves the independent per-operand ancestor chase can fold
     * BOTH sides all the way to a full, non-placeholder literal, not just the partial case above.
     */
    @Test
    void call006TwoParamConcatenationWithBothOperandsLiteralFullyResolves() throws Exception {
        BytecodeClassParser parser = new BytecodeClassParser();
        ClassInfo leaf = parse(parser, "TwoParamConcatRepoLeaf");
        ClassInfo appender = parse(parser, "TwoParamConcatAppender");
        ClassInfo svc = parse(parser, "TwoParamConcatService");
        ClassInfo controller = parse(parser, "TwoLiteralConcatController");

        ProgressService progressService = new ProgressService();
        DomainConfigLoader configLoader = new DomainConfigLoader();
        java.lang.reflect.Method applyDefaults = DomainConfigLoader.class.getDeclaredMethod("applyDefaults");
        applyDefaults.setAccessible(true);
        applyDefaults.invoke(configLoader);

        MongoMethodDetector mongoDetector = new MongoMethodDetector(configLoader);
        AggregationDetector aggregationDetector = new AggregationDetector(configLoader);
        JpaMethodDetector jpaDetector = new JpaMethodDetector(configLoader);
        MyBatisMethodDetector myBatisDetector = new MyBatisMethodDetector();
        RedisMethodDetector redisDetector = new RedisMethodDetector();
        CassandraMethodDetector cassandraDetector = new CassandraMethodDetector();
        ElasticsearchMethodDetector esDetector = new ElasticsearchMethodDetector();
        CollectionLifecycleDetector lifecycleDetector = new CollectionLifecycleDetector(configLoader);
        CollectionResolver resolver = new CollectionResolver(configLoader, mongoDetector,
                aggregationDetector, jpaDetector, myBatisDetector,
                redisDetector, cassandraDetector, esDetector, lifecycleDetector);
        CallTreeBuilder treeBuilder = new CallTreeBuilder(configLoader, resolver, 30, 30, 2000, 100000);
        CallGraphService service = new CallGraphService(progressService, configLoader, treeBuilder,
                new com.fasterxml.jackson.databind.ObjectMapper(), lifecycleDetector);

        service.buildIndex();
        service.setJarArtifactMap(new java.util.HashMap<>());
        service.addToIndex(leaf);
        service.addToIndex(appender);
        service.addToIndex(svc);
        service.addToIndex(controller);

        List<EndpointInfo> endpoints = service.buildEndpointsFromIndex();
        assertEquals(1, endpoints.size());
        EndpointInfo ep = endpoints.get(0);
        assertEquals("/orders-twoliteral", ep.getFullPath());

        System.out.println("=== CALL-008 TWO-PARAM CONCAT (FULL) — REAL CAPTURED OUTPUT ===");
        System.out.println("aggregatedCollections = " + ep.getAggregatedCollections());
        printTree(ep.getCallTree(), 0);

        assertTrue(ep.getAggregatedCollections().contains("orders_east"),
            "CALL-008: when BOTH concatenation operands terminate in a real literal up " +
            "the chain, the independent per-operand ancestor chase must fold them to a full, " +
            "non-placeholder collection name end-to-end.");
    }

    /**
     * §210/§213 (CALL-008) proof, closing §204 Pattern B's own residual gap: identical
     * shape to {@link #call004ClassPropagatedThroughTwoHopsWithDocumentAnnotationResolves} EXCEPT
     * {@code InvoiceEntity} carries NO {@code @Document} annotation — there is no ground truth to
     * look up, so this must fall back to the bare naming-convention heuristic (strip suffix,
     * lowercase, pluralize: {@code InvoiceEntity} → {@code "invoices"}), tagged with the distinct,
     * lower-confidence {@code CALL_SITE_CLASS_NAMING_HEURISTIC} source so it's never confused with
     * a confirmed {@code @Document} match.
     */
    @Test
    void call006ClassPropagatedThroughTwoHopsWithoutDocumentAnnotationResolvesViaNamingHeuristic() throws Exception {
        BytecodeClassParser parser = new BytecodeClassParser();
        ClassInfo entity = parse(parser, "InvoiceEntity");
        ClassInfo leaf = parse(parser, "UnannotatedClassRepoLeaf");
        ClassInfo mid = parse(parser, "UnannotatedClassServiceMid");
        ClassInfo controller = parse(parser, "UnannotatedMultiHopClassController");

        ProgressService progressService = new ProgressService();
        DomainConfigLoader configLoader = new DomainConfigLoader();
        java.lang.reflect.Method applyDefaults = DomainConfigLoader.class.getDeclaredMethod("applyDefaults");
        applyDefaults.setAccessible(true);
        applyDefaults.invoke(configLoader);

        MongoMethodDetector mongoDetector = new MongoMethodDetector(configLoader);
        AggregationDetector aggregationDetector = new AggregationDetector(configLoader);
        JpaMethodDetector jpaDetector = new JpaMethodDetector(configLoader);
        MyBatisMethodDetector myBatisDetector = new MyBatisMethodDetector();
        RedisMethodDetector redisDetector = new RedisMethodDetector();
        CassandraMethodDetector cassandraDetector = new CassandraMethodDetector();
        ElasticsearchMethodDetector esDetector = new ElasticsearchMethodDetector();
        CollectionLifecycleDetector lifecycleDetector = new CollectionLifecycleDetector(configLoader);
        CollectionResolver resolver = new CollectionResolver(configLoader, mongoDetector,
                aggregationDetector, jpaDetector, myBatisDetector,
                redisDetector, cassandraDetector, esDetector, lifecycleDetector);
        CallTreeBuilder treeBuilder = new CallTreeBuilder(configLoader, resolver, 30, 30, 2000, 100000);
        CallGraphService service = new CallGraphService(progressService, configLoader, treeBuilder,
                new com.fasterxml.jackson.databind.ObjectMapper(), lifecycleDetector);

        service.buildIndex();
        service.setJarArtifactMap(new java.util.HashMap<>());
        service.addToIndex(entity);
        service.addToIndex(leaf);
        service.addToIndex(mid);
        service.addToIndex(controller);

        List<EndpointInfo> endpoints = service.buildEndpointsFromIndex();
        assertEquals(1, endpoints.size());
        EndpointInfo ep = endpoints.get(0);
        assertEquals("/invoices-multihop-unannotated", ep.getFullPath());

        System.out.println("=== CALL-008 TWO-HOP CLASS<?> PROPAGATION (NO @Document) — REAL CAPTURED OUTPUT ===");
        System.out.println("aggregatedCollections = " + ep.getAggregatedCollections());
        printTree(ep.getCallTree(), 0);

        assertTrue(ep.getAggregatedCollections().contains("invoices"),
            "CALL-008: a Class<?> value threaded through 2 method calls, for a NON-" +
            "@Document-annotated entity, must now resolve via the bare naming-convention " +
            "heuristic (InvoiceEntity -> invoices) — §204 Pattern B's own residual gap.");
    }

    private void printTree(com.jaranalyzer.model.CallNode node, int depth) {
        if (node == null) return;
        System.out.println("  ".repeat(depth) + "- " + node.getClassName() + "#" + node.getMethodName()
                + " collections=" + node.getCollectionsAccessed() + " hasDbInteraction=" + node.isHasDbInteraction()
                + " dynamicRefs=" + node.getDynamicCollectionRefs());
        if (node.getChildren() != null) {
            for (var c : node.getChildren()) printTree(c, depth + 1);
        }
    }
}
