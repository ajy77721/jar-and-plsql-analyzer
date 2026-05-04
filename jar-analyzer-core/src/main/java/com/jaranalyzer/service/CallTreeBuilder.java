package com.jaranalyzer.service;

import com.jaranalyzer.model.*;
import com.jaranalyzer.service.CallGraphIndex.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Builds enriched call trees for endpoints.
 * Handles class indexing, method resolution, and the recursive tree-building logic.
 * Collection resolution is delegated to {@link CollectionResolver}.
 * Index data structures live in {@link CallGraphIndex}.
 */
@Service
public class CallTreeBuilder {

    private static final Logger log = LoggerFactory.getLogger(CallTreeBuilder.class);

    final int maxDepth;
    final int maxChildrenPerNode;
    final int maxNodesPerTree;

    private final DomainConfigLoader configLoader;
    private final CollectionResolver collectionResolver;

    public CallTreeBuilder(DomainConfigLoader configLoader, CollectionResolver collectionResolver,
                           @Value("${calltree.max-depth:30}") int maxDepth,
                           @Value("${calltree.max-children-per-node:30}") int maxChildrenPerNode,
                           @Value("${calltree.max-nodes-per-tree:2000}") int maxNodesPerTree) {
        this.configLoader = configLoader;
        this.collectionResolver = collectionResolver;
        this.maxDepth = maxDepth;
        this.maxChildrenPerNode = maxChildrenPerNode;
        this.maxNodesPerTree = maxNodesPerTree;
    }

    // ========== Context lifecycle (called by CallGraphService) ==========

    private ResolutionContext ctx;

    ResolutionContext createContext() {
        this.ctx = new ResolutionContext();
        return this.ctx;
    }

    ResolutionContext getContext() {
        return this.ctx;
    }

    void clearContext() {
        this.ctx = null;
        this.appClassCache.clear();
    }

    // ========== Delegated to CollectionResolver ==========

    List<String> autoDetectBasePackages() {
        return collectionResolver.autoDetectBasePackages(ctx);
    }

    void resolveRepositoryCollections() {
        collectionResolver.resolveRepositoryCollections(ctx);
    }

    /**
     * Expand interfaceImplMap transitively after all classes are indexed.
     *
     * Problem: interfaceImplMap only records DIRECT implements declarations.
     * If AbstractBase implements Interface, and ConcreteImpl extends AbstractBase,
     * then ConcreteImpl never appears in interfaceImplMap["Interface"] — so calls
     * through the interface never reach ConcreteImpl.
     *
     * Fix: for every direct implementor, walk childClassMap (BFS) and add all
     * concrete subclasses to the same interface entry.
     */
    void expandInterfaceImplMapTransitively() {
        Map<String, List<String>> additions = new HashMap<>();

        for (Map.Entry<String, List<String>> entry : ctx.interfaceImplMap.entrySet()) {
            String iface = entry.getKey();
            Set<String> known = new HashSet<>(entry.getValue());
            List<String> newEntries = new ArrayList<>();

            Queue<String> queue = new LinkedList<>(entry.getValue());
            while (!queue.isEmpty()) {
                String cls = queue.poll();
                List<String> children = ctx.childClassMap.get(cls);
                if (children == null) continue;
                for (String child : children) {
                    if (known.add(child)) {   // true = not already registered
                        newEntries.add(child);
                        queue.add(child);     // recurse into grandchildren
                    }
                }
            }
            if (!newEntries.isEmpty()) additions.put(iface, newEntries);
        }

        int total = 0;
        for (Map.Entry<String, List<String>> e : additions.entrySet()) {
            ctx.interfaceImplMap.computeIfAbsent(e.getKey(), k -> new ArrayList<>()).addAll(e.getValue());
            total += e.getValue().size();
        }
        if (total > 0) {
            log.info("Interface impl map expanded: +{} transitive implementors across {} interfaces",
                    total, additions.size());
        }
    }

    // ========== Indexing a single class (called by CallGraphService.addToIndex) ==========

    void indexClass(ClassInfo cls) {
        IndexedClass ic = new IndexedClass(cls, configLoader.getCollectionPattern());

        for (MethodInfo mi : cls.getMethods()) {
            boolean isStatic = (mi.getAccessFlags() & 0x0008) != 0; // ACC_STATIC
            String key = (isStatic ? "S:" : "") + mi.getName() + mi.getDescriptor();
            ic.methods.put(key, new IndexedMethod(mi));
        }

        ctx.classMap.put(ic.fqn, ic);

        // Build SimpleJdbcCall field→procedure map from constructor and init method invocations.
        // Pattern: this.callField = new SimpleJdbcCall(...).withCatalogName("PKG").withProcedureName("X")
        // withSchemaName resets catalog context; withCatalogName sets it; withProcedureName/withFunctionName captures it.
        // PUTFIELD follows the fluent chain; withProcedureName/withFunctionName precedes it.
        // OracleProcedureRepository-style classes often initialize in @PostConstruct or named init methods.
        for (MethodInfo mi : cls.getMethods()) {
            boolean isConstructor = "<init>".equals(mi.getName());
            boolean isPostConstruct = mi.getAnnotations() != null && mi.getAnnotations().stream()
                    .anyMatch(a -> "PostConstruct".equals(a.getName()));
            boolean isNamedInit = Set.of("init", "initialize", "afterPropertiesSet", "setup", "postConstruct")
                    .contains(mi.getName());
            if (!isConstructor && !isPostConstruct && !isNamedInit) continue;
            // Collect (lineNumber, isProcedure, name) for each withProcedureName/withFunctionName call
            record ProcEntry(int line, boolean isFunc, String name) {}
            List<ProcEntry> procEntries = new ArrayList<>();
            String currentCatalog = null;
            for (MethodCallInfo mci : mi.getInvocations()) {
                // Detect StoredProcedure subclass: INVOKESPECIAL to StoredProcedure.<init> carries proc name
                // Pattern: class extends StoredProcedure { Constructor() { super(ds, "PKG.PROC"); } }
                if (isConstructor && "<init>".equals(mci.getMethodName())
                        && mci.getOwnerClass() != null
                        && (mci.getOwnerClass().endsWith(".StoredProcedure")
                            || "StoredProcedure".equals(mci.getOwnerClass()))) {
                    List<String> superArgs = mci.getRecentStringArgs();
                    if (superArgs != null) {
                        for (String arg : superArgs) {
                            if (arg != null && !arg.startsWith("__class:") && !arg.isBlank()
                                    && arg.matches("[A-Za-z_][A-Za-z0-9_.]*")) {
                                ic.storedProcedureName = arg.trim();
                                break;
                            }
                        }
                    }
                    continue;
                }
                // withSchemaName signals a new SimpleJdbcCall chain — reset catalog context
                if ("withSchemaName".equals(mci.getMethodName())) {
                    currentCatalog = null;
                    continue;
                }
                // withCatalogName carries the Oracle package name (e.g. "PKG_ORDER")
                if ("withCatalogName".equals(mci.getMethodName())) {
                    List<String> catArgs = mci.getRecentStringArgs();
                    if (catArgs != null && !catArgs.isEmpty() && catArgs.get(0) != null
                            && !catArgs.get(0).isBlank()) {
                        currentCatalog = catArgs.get(0).trim();
                    }
                    continue;
                }
                boolean isProc = "withProcedureName".equals(mci.getMethodName());
                boolean isFunc = "withFunctionName".equals(mci.getMethodName());
                if (!isProc && !isFunc) continue;
                List<String> args = mci.getRecentStringArgs();
                if (args == null || args.isEmpty()) continue;
                String procName = args.get(0);
                if (procName != null && !procName.isBlank()
                        && procName.matches("[A-Za-z_][A-Za-z0-9_.]*")) {
                    // Prefix with catalog/package name if present (e.g. PKG_ORDER.PLACE_ORDER)
                    String fullName = (currentCatalog != null)
                            ? currentCatalog + "." + procName.trim()
                            : procName.trim();
                    procEntries.add(new ProcEntry(mci.getLineNumber(), isFunc, fullName));
                    currentCatalog = null; // consumed — next chain starts fresh
                }
            }
            if (procEntries.isEmpty()) continue;
            // Collect PUTFIELD operations on SimpleJdbcCall-type fields, sorted by line
            List<FieldAccessInfo> putFields = new ArrayList<>();
            if (mi.getFieldAccesses() != null) {
                for (FieldAccessInfo fa : mi.getFieldAccesses()) {
                    if ("PUT".equals(fa.getAccessType()) && fa.getFieldName() != null
                            && fa.getOwnerClass() != null && fa.getOwnerClass().equals(ic.fqn)) {
                        putFields.add(fa);
                    }
                }
            }
            putFields.sort(java.util.Comparator.comparingInt(FieldAccessInfo::getLineNumber));
            // For each proc entry, find the nearest PUTFIELD at or after its line
            for (ProcEntry pe : procEntries) {
                FieldAccessInfo match = putFields.stream()
                        .filter(f -> f.getLineNumber() >= pe.line())
                        .findFirst().orElse(null);
                if (match != null) {
                    String prefix = pe.isFunc() ? "FUNC" : "PROC";
                    ic.simpleJdbcCallProcedures.put(match.getFieldName(), prefix + ":" + pe.name());
                }
            }
        }

        // Scan @Bean methods in @Configuration classes
        if ("CONFIGURATION".equals(cls.getStereotype())) {
            for (MethodInfo mi : cls.getMethods()) {
                String beanName = null;
                List<String> beanAliases = null;
                for (AnnotationInfo ann : mi.getAnnotations()) {
                    if ("Bean".equals(ann.getName())) {
                        Object nameVal = ann.getAttributes().get("name");
                        if (nameVal == null) nameVal = ann.getAttributes().get("value");
                        List<String> allNames = new ArrayList<>();
                        if (nameVal instanceof String s && !s.isBlank()) {
                            allNames.add(s);
                        } else if (nameVal instanceof List<?> list && !list.isEmpty()) {
                            for (Object item : list) {
                                String n = item.toString();
                                if (!n.isBlank()) allNames.add(n);
                            }
                        }
                        if (allNames.isEmpty()) allNames.add(mi.getName());
                        beanName = allNames.get(0);
                        if (allNames.size() > 1) {
                            beanAliases = allNames.subList(1, allNames.size());
                        }
                    }
                }
                if (beanName != null) {
                    for (MethodCallInfo mci : mi.getInvocations()) {
                        if ("<init>".equals(mci.getMethodName())) {
                            String implFqn = mci.getOwnerClass();
                            if (implFqn != null && !implFqn.startsWith("java.")) {
                                ctx.beanNameToImplMap.put(beanName, implFqn);
                                if (beanAliases != null) {
                                    for (String alias : beanAliases) {
                                        ctx.beanNameToImplMap.put(alias, implFqn);
                                    }
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }

        for (String iface : cls.getInterfaces()) {
            ctx.interfaceImplMap.computeIfAbsent(iface, k -> new ArrayList<>()).add(ic.fqn);
        }

        // Scan @EventListener / @TransactionalEventListener annotated methods
        for (MethodInfo mi : cls.getMethods()) {
            String eventType = null;
            boolean isTransactional = false;
            boolean isEventListener = false;
            for (AnnotationInfo ann : mi.getAnnotations()) {
                String aName = ann.getName();
                if ("EventListener".equals(aName) || "TransactionalEventListener".equals(aName)) {
                    isEventListener = true;
                    isTransactional = "TransactionalEventListener".equals(aName);
                    // Check annotation value/classes attribute for explicit event type
                    Object val = ann.getAttributes().get("value");
                    if (val == null) val = ann.getAttributes().get("classes");
                    if (val instanceof String s && !s.isBlank()) {
                        eventType = s;
                    } else if (val instanceof List<?> list && !list.isEmpty()) {
                        // Multiple event types: register for each
                        for (Object item : list) {
                            String evtFqn = item.toString();
                            if (!evtFqn.isBlank()) {
                                registerEventListener(ic, mi, evtFqn, isTransactional);
                            }
                        }
                        eventType = null; // already registered above
                    }
                }
            }
            if (isEventListener) {
                // If annotation didn't specify type, infer from first parameter
                if (eventType == null) {
                    List<ParameterInfo> params = mi.getParameters();
                    if (!params.isEmpty()) {
                        eventType = params.get(0).getType();
                    }
                }
                if (eventType != null) {
                    registerEventListener(ic, mi, eventType, isTransactional);
                }
            }
        }
        if (cls.getSuperClass() != null) {
            ctx.superClassMap.put(ic.fqn, cls.getSuperClass());
            ctx.childClassMap.computeIfAbsent(cls.getSuperClass(), k -> new ArrayList<>()).add(ic.fqn);
        }

        if (ic.documentCollection != null) {
            ctx.entityCollectionMap.put(ic.fqn, ic.documentCollection);
            ctx.entityCollectionMap.put(ic.simpleName, ic.documentCollection);
            // Store entity field mappings keyed by collection name
            if (!ic.fieldMappings.isEmpty()) {
                ctx.collectionFieldMappings.put(ic.documentCollection, new LinkedHashMap<>(ic.fieldMappings));
                // Register all MongoDB field names so they aren't mistaken for collection names
                ctx.knownEntityFieldNames.addAll(ic.fieldMappings.values());
            }
        }
        // Also register field mappings from non-@Document classes (DTOs, embedded docs)
        if (ic.documentCollection == null && !ic.fieldMappings.isEmpty()) {
            ctx.knownEntityFieldNames.addAll(ic.fieldMappings.values());
        }

        // JPA @Entity/@Table → entityTableMap
        if (ic.isJpaEntity && ic.jpaTableName != null) {
            ctx.entityTableMap.put(ic.fqn, ic.jpaTableName);
            ctx.entityTableMap.put(ic.simpleName, ic.jpaTableName);
            // Store JPA column mappings keyed by table name
            if (!ic.jpaColumnMappings.isEmpty()) {
                ctx.collectionFieldMappings.put(ic.jpaTableName, new LinkedHashMap<>(ic.jpaColumnMappings));
            }
            // Index @NamedQuery / @NamedNativeQuery annotations on @Entity classes
            for (AnnotationInfo ann : cls.getAnnotations()) {
                indexNamedQuery(ann, ctx);
            }
        }

        // Index enum/constants class string values for GETSTATIC resolution
        if ((ic.isEnum || isConstantsClass(ic)) && !ic.allStringConstants.isEmpty()) {
            ctx.enumConstantsMap.put(ic.fqn, ic.allStringConstants);
        }

        // Populate stereotype bean name map and custom qualifier context
        if (ic.explicitBeanName != null) {
            ctx.stereotypeBeanNameMap.put(ic.explicitBeanName, ic.fqn);
        }
        if (!ic.customQualifierAnnotations.isEmpty()) {
            ctx.classCustomQualifiers.put(ic.fqn, ic.customQualifierAnnotations);
        }

        String st = cls.getStereotype();
        boolean isMainJar = cls.getSourceJar() == null;
        // Index REST endpoints: standard controllers + any class with HTTP-mapped methods
        // (covers @Component/@Service with @RequestMapping)
        if (isMainJar && ("REST_CONTROLLER".equals(st) || "CONTROLLER".equals(st)
                || "COMPONENT".equals(st) || "SERVICE".equals(st))) {
            for (MethodInfo mi : cls.getMethods()) {
                if (mi.getHttpMethod() != null) {
                    ctx.endpoints.add(new IndexedEndpoint(mi, cls));
                }
            }
        }

        // Register async / event-driven entry points as synthetic endpoints.
        // @RabbitListener → AMQP (queue name as path)
        // @KafkaListener → KAFKA (topic name as path)
        // @MessageMapping → WS (WebSocket path)
        // @Scheduled → SCHEDULED (cron/rate expression as path)
        if (isMainJar) {
            for (MethodInfo mi : cls.getMethods()) {
                for (AnnotationInfo ann : mi.getAnnotations()) {
                    String syntheticHttp = null;
                    String syntheticPath = null;
                    switch (ann.getName()) {
                        case "RabbitListener" -> {
                            syntheticHttp = "AMQP";
                            Object q = ann.getAttributes().get("queues");
                            if (q == null) q = ann.getAttributes().get("value");
                            syntheticPath = firstStringAttr(q);
                        }
                        case "KafkaListener" -> {
                            syntheticHttp = "KAFKA";
                            Object t = ann.getAttributes().get("topics");
                            if (t == null) t = ann.getAttributes().get("value");
                            syntheticPath = firstStringAttr(t);
                        }
                        case "MessageMapping" -> {
                            syntheticHttp = "WS";
                            Object v = ann.getAttributes().get("value");
                            syntheticPath = firstStringAttr(v);
                        }
                        case "Scheduled" -> {
                            syntheticHttp = "SCHEDULED";
                            Object cron = ann.getAttributes().get("cron");
                            if (cron instanceof String s && !s.isBlank()) {
                                syntheticPath = s;
                            } else {
                                Object rate = ann.getAttributes().get("fixedRate");
                                syntheticPath = rate != null ? "rate=" + rate : "scheduled";
                            }
                        }
                        default -> {}
                    }
                    if (syntheticHttp != null) {
                        boolean isStatic = (mi.getAccessFlags() & 0x0008) != 0;
                        String key = (isStatic ? "S:" : "") + mi.getName() + mi.getDescriptor();
                        IndexedMethod im = ic.methods.get(key);
                        if (im != null) {
                            ctx.endpoints.add(new IndexedEndpoint(syntheticHttp,
                                    syntheticPath != null ? syntheticPath : "", ic, im));
                        }
                        break; // one synthetic entry per method
                    }
                }
            }
        }
    }

    /** Extract first string value from an annotation attribute (handles String or List<?>). */
    private static String firstStringAttr(Object attr) {
        if (attr instanceof String s && !s.isBlank()) return s;
        if (attr instanceof List<?> list) {
            for (Object item : list) {
                if (item instanceof String s && !s.isBlank()) return s;
            }
        }
        return null;
    }

    /**
     * Second pass: find endpoints in nested JAR classes that belong to a detected base package.
     * Called after detectedBasePackages is populated (not available during initial indexClass).
     */
    void indexNestedJarEndpoints() {
        if (ctx.detectedBasePackages == null || ctx.detectedBasePackages.isEmpty()) return;
        Set<String> alreadyIndexed = new HashSet<>();
        for (IndexedEndpoint iep : ctx.endpoints) {
            alreadyIndexed.add(iep.controllerFqn + "#" + iep.methodKey);
        }
        int added = 0;
        for (IndexedClass ic : ctx.classMap.values()) {
            if (ic.sourceJar == null) continue;
            String st = ic.stereotype;
            if (!("REST_CONTROLLER".equals(st) || "CONTROLLER".equals(st)
                    || "COMPONENT".equals(st) || "SERVICE".equals(st))) continue;
            boolean inBasePackage = false;
            for (String pkg : ctx.detectedBasePackages) {
                if (ic.fqn.startsWith(pkg + ".")) { inBasePackage = true; break; }
            }
            if (!inBasePackage) continue;
            for (IndexedMethod im : ic.methods.values()) {
                if (im.httpMethod == null) continue;
                String key = ic.fqn + "#" + (im.isStatic ? "S:" : "") + im.name + im.descriptor;
                if (alreadyIndexed.contains(key)) continue;
                ctx.endpoints.add(new IndexedEndpoint(im.httpMethod, im.path, ic, im));
                alreadyIndexed.add(key);
                added++;
            }
        }
        if (added > 0) {
            log.info("Indexed {} additional endpoints from nested JARs in base packages", added);
        }
    }

    // ========== Call tree building ==========

    private SubtreeCache globalSubtreeCache;

    void setGlobalSubtreeCache(SubtreeCache cache) {
        this.globalSubtreeCache = cache;
    }

    CallNode buildCallTree(IndexedClass cls, IndexedMethod method,
                           Set<String> callStack, int depth, int[] nodeCount,
                           String controllerJar) {
        return buildCallTree(cls, method, callStack, depth, nodeCount, controllerJar, new HashMap<>());
    }

    private CallNode buildCallTree(IndexedClass cls, IndexedMethod method,
                           Set<String> callStack, int depth, int[] nodeCount,
                           String controllerJar, Map<String, CallNode> memo) {
        if (depth > maxDepth || nodeCount[0] >= maxNodesPerTree) return null;
        nodeCount[0]++;

        String nodeId = cls.fqn + "#" + method.name + method.descriptor;
        CallNode node = createEnrichedNode(cls, method, nodeId, controllerJar);

        if (callStack.contains(nodeId)) {
            // Back-edge detected: mark clearly so the trace UI can render it as a
            // cycle indicator rather than an incomplete subtree.
            node.setRecursive(true);
            node.setCallType("RECURSIVE");
            node.setDispatchType("RECURSIVE");
            return node;
        }

        // Memoization: if we've already built this subtree from a different path,
        // return a shallow copy to avoid exponential rebuilds
        if (memo.containsKey(nodeId)) {
            CallNode cached = memo.get(nodeId);
            node.setChildren(cached.getChildren());
            node.setOperationType(cached.getOperationType());
            node.getOperationTypes().addAll(cached.getOperationTypes());
            node.getCollectionsAccessed().addAll(cached.getCollectionsAccessed());
            return node;
        }

        // Global disk-backed cache: reuse subtrees across endpoints.
        // Reads from JSONL file (no memory bloat). Deep copy so each endpoint is independent.
        if (globalSubtreeCache != null && depth > 0) {
            int remainingDepth = maxDepth - depth;
            var cacheKey = new SubtreeCache.SubtreeCacheKey(nodeId, null, controllerJar, remainingDepth);
            CallNode cached = globalSubtreeCache.get(cacheKey);
            if (cached != null) {
                rewriteCrossModule(cached, controllerJar);
                return cached;
            }
        }

        callStack.add(nodeId);

        Set<String> seenCalls = new HashSet<>();
        int childCount = 0;

        for (InvocationRef call : method.invocations) {
            if (childCount >= maxChildrenPerNode || nodeCount[0] >= maxNodesPerTree) break;
            if ("<init>".equals(call.methodName()) || "<clinit>".equals(call.methodName())) continue;
            // Don't noise-filter lambda synthetic methods — they carry real business logic
            if (!call.methodName().startsWith("lambda$") && isNoiseMethod(call.methodName())) continue;

            boolean isStaticCall = call.opcode() == 184; // Opcodes.INVOKESTATIC
            String staticPrefix = isStaticCall ? "S:" : "";
            String callKey = call.ownerClass() + "#" + staticPrefix + call.methodName() + call.descriptor();
            if (seenCalls.contains(callKey)) continue;
            seenCalls.add(callKey);

            // Spring Event dispatch: publishEvent() → @EventListener methods
            int eventChildren = connectEventListeners(call, method, node, callStack,
                    depth, nodeCount, controllerJar, memo, childCount);
            if (eventChildren >= 0) {
                childCount += eventChildren;
                continue; // handled as event dispatch — skip normal resolution
            }

            // --- Lambda / method reference resolution ---
            // For invokedynamic-sourced calls (isLambdaInvocation), resolve the target explicitly.
            // Two patterns:
            // 1. Lambda body: target is a synthetic lambda$xxx$N method in the same (or outer) class
            //    → resolve the synthetic method, which contains the actual calls
            // 2. Method reference (SomeClass::someMethod): target points directly to the method
            //    → resolve it like a normal call to that class
            if (call.isLambdaInvocation() && call.lambdaTargetClass() != null) {
                String lambdaTargetCls = call.lambdaTargetClass();
                String lambdaTargetMtd = call.lambdaTargetMethod();
                String lambdaTargetDesc = call.lambdaTargetDescriptor();

                boolean isSyntheticLambda = lambdaTargetMtd != null && lambdaTargetMtd.startsWith("lambda$");
                IndexedClass lambdaCls = ctx.classMap.get(lambdaTargetCls);

                if (lambdaCls != null) {
                    // Try both static and instance keys — lambda bodies are typically static,
                    // but method references can be either
                    String lambdaKey = (isSyntheticLambda ? "S:" : staticPrefix) + lambdaTargetMtd + lambdaTargetDesc;
                    IndexedMethod lambdaMethod = lambdaCls.methods.get(lambdaKey);
                    // Fallback: try the opposite prefix
                    if (lambdaMethod == null) {
                        String altKey = (isSyntheticLambda ? "" : "S:") + lambdaTargetMtd + lambdaTargetDesc;
                        lambdaMethod = lambdaCls.methods.get(altKey);
                    }
                    if (lambdaMethod != null) {
                        CallNode child = buildCallTree(lambdaCls, lambdaMethod, callStack,
                                depth + 1, nodeCount, controllerJar, memo);
                        if (child != null) {
                            child.setDispatchType(isSyntheticLambda ? "LAMBDA" : "METHOD_REFERENCE");
                            node.getChildren().add(child);
                            childCount++;
                        }
                        continue; // resolved via lambda path — skip normal resolution
                    }
                }

                // Lambda target class not in classMap — for method references to interfaces/abstract,
                // fall through to try interface implementation resolution
                if (!isSyntheticLambda) {
                    boolean targetIsApp = ctx.classMap.containsKey(lambdaTargetCls)
                            || ctx.interfaceImplMap.containsKey(lambdaTargetCls)
                            || isApplicationClass(lambdaTargetCls);
                    if (targetIsApp) {
                        List<ResolvedRef> resolved = resolveAllMethods(lambdaTargetCls,
                                lambdaTargetMtd, lambdaTargetDesc, call, cls);
                        if (!resolved.isEmpty()) {
                            for (ResolvedRef ref : resolved) {
                                if (childCount >= maxChildrenPerNode || nodeCount[0] >= maxNodesPerTree) break;
                                CallNode child = buildCallTree(ref.cls, ref.method, callStack,
                                        depth + 1, nodeCount, controllerJar, memo);
                                if (child != null) {
                                    child.setDispatchType("METHOD_REFERENCE");
                                    child.setResolvedFrom(ref.resolvedFrom());
                                    child.setQualifierHint(ref.qualifierHint());
                                    node.getChildren().add(child);
                                    childCount++;
                                }
                            }
                            continue; // resolved via method-ref path
                        }
                    }
                }
                // If lambda target resolution failed, fall through to standard resolution
            }

            String targetClass = call.ownerClass();
            // Standard library and third-party framework classes must never be recursed into,
            // even when they appear in classMap (because nested JARs inside a WAR are indexed).
            // Guard here prevents Hikari, Guava, Spring internals, etc. from polluting the tree.
            boolean isAppClass = !isStandardLibraryClass(targetClass)
                    && (ctx.classMap.containsKey(targetClass)
                        || ctx.interfaceImplMap.containsKey(targetClass)
                        || isApplicationClass(targetClass));

            if (isAppClass) {
                // Skip DTO/model getter/setter calls — they add noise without insight
                // Don't filter lambda synthetic methods, they carry actual logic
                if (!call.methodName().startsWith("lambda$") && isDtoCall(targetClass, call.methodName())) continue;
                // Skip static factory methods on response-wrapper classes (e.g. ApiResponse.success()):
                // These wrap a return value but contain no business logic worth traversing.
                if (isStaticCall && isResponseWrapperFactory(targetClass, call.methodName())) continue;

                List<ResolvedRef> resolved = resolveAllMethods(targetClass, call.methodName(), call.descriptor(), call, cls);
                if (!resolved.isEmpty()) {
                    for (ResolvedRef ref : resolved) {
                        if (childCount >= maxChildrenPerNode || nodeCount[0] >= maxNodesPerTree) break;
                        // FeignClient / HTTP_CLIENT interface: create HTTP_CALL leaf instead of recursing.
                        // The interface has no method bodies — its proxy makes real HTTP calls at runtime.
                        if ("HTTP_CLIENT".equals(ref.cls().stereotype)) {
                            CallNode httpNode = createFeignCallLeaf(ref.cls(), call, method);
                            node.getChildren().add(httpNode);
                            childCount++;
                            nodeCount[0]++;
                            continue;
                        }
                        CallNode child = buildCallTree(ref.cls, ref.method, callStack, depth + 1, nodeCount, controllerJar, memo);
                        if (child != null) {
                            // Preserve RECURSIVE dispatchType set by the back-edge detection above.
                            if (!child.isRecursive()) child.setDispatchType(ref.dispatchType());
                            child.setResolvedFrom(ref.resolvedFrom());
                            child.setQualifierHint(ref.qualifierHint());
                            node.getChildren().add(child);
                            childCount++;
                        }
                    }
                } else {
                    // Method not found in index — might be a Spring Data derived query
                    // (e.g., findByStatus, deleteByExpiry, countByDomain) generated at runtime.
                    // Create a synthetic leaf with operation inferred from method name prefix.
                    IndexedClass targetCls = ctx.classMap.get(targetClass);
                    if (targetCls != null && isRepoStereotype(targetCls.stereotype)) {
                        CallNode derived = createDerivedQueryLeaf(targetCls, call);
                        if (derived != null) {
                            node.getChildren().add(derived);
                            childCount++;
                            nodeCount[0]++;
                        }
                    }
                }
            } else if (configLoader.isHttpClientCall(targetClass)) {
                // HTTP client call (RestTemplate, WebClient, FeignClient, etc.)
                CallNode httpNode = createHttpCallLeaf(call, method);
                node.getChildren().add(httpNode);
                childCount++;
                nodeCount[0]++;
            } else {
                if (configLoader.isOrgExternalClass(targetClass)) {
                    CallNode extNode = createExternalLeaf(call);
                    node.getChildren().add(extNode);
                    childCount++;
                    nodeCount[0]++;
                }
            }
        }

        callStack.remove(nodeId);
        memo.put(nodeId, node);

        // Store in global disk cache for cross-endpoint reuse (skip root node, depth=0)
        if (globalSubtreeCache != null && depth > 0) {
            int remainingDepth = maxDepth - depth;
            var cacheKey = new SubtreeCache.SubtreeCacheKey(nodeId, null, controllerJar, remainingDepth);
            globalSubtreeCache.put(cacheKey, node);
        }

        return node;
    }

    /**
     * Rewrite crossModule flag on a cached subtree for a different controller JAR.
     * The cached node was built for one endpoint's controllerJar; the new endpoint
     * may have a different one, changing which nodes are "cross-module".
     */
    private void rewriteCrossModule(CallNode node, String controllerJar) {
        if (node == null) return;
        Set<CallNode> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        rewriteCrossModuleRecursive(node, controllerJar, visited);
    }

    private void rewriteCrossModuleRecursive(CallNode node, String controllerJar, Set<CallNode> visited) {
        if (node == null || !visited.add(node)) return;
        boolean isCross = !Objects.equals(node.getSourceJar(), controllerJar);
        node.setCrossModule(isCross);
        if (node.getCallType() != null && !"HTTP_CALL".equals(node.getCallType())
                && !"EVENT_LISTENER".equals(node.getCallType())
                && !"ASYNC_EVENT".equals(node.getCallType())
                && !"RECURSIVE".equals(node.getCallType())) {
            boolean isAsync = node.getAnnotations() != null && node.getAnnotations().contains("Async");
            node.setCallType(isAsync ? "ASYNC" : (isCross ? "CROSS_MODULE" : "APPLICATION"));
        }
        if (node.getChildren() != null) {
            for (CallNode child : node.getChildren()) {
                rewriteCrossModuleRecursive(child, controllerJar, visited);
            }
        }
    }

    // ========== Post-processing: clean false operation types ==========

    /**
     * Walk the tree after it's fully built and clean up false operation types.
     * Two passes:
     * 1. Leaf nodes: if operationType set by method name matching but no collections
     *    and not a DB stereotype → clear (e.g., createSavepoint getting WRITE)
     * 2. Non-leaf nodes: if no direct collections, derive operationType from children.
     *    If no children have operations → clear.
     */
    void cleanOperationTypes(CallNode node) {
        if (node == null) return;
        Set<CallNode> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        cleanOperationTypesRecursive(node, visited);
    }

    private void cleanOperationTypesRecursive(CallNode node, Set<CallNode> visited) {
        if (node == null || !visited.add(node)) return;

        // Process children first (bottom-up)
        if (node.getChildren() != null) {
            for (CallNode child : node.getChildren()) {
                cleanOperationTypesRecursive(child, visited);
            }
        }

        boolean hasCollections = node.getCollectionsAccessed() != null
                && !node.getCollectionsAccessed().isEmpty();
        boolean isDbStereotype = "REPOSITORY".equals(node.getStereotype())
                || "SPRING_DATA".equals(node.getStereotype());
        boolean isLeaf = node.getChildren() == null || node.getChildren().isEmpty();

        // hasDbInteraction: method calls MongoTemplate/MongoOperations but collection
        // couldn't be resolved. The operation type (e.g., WRITE from save()) is still valid.
        if (!hasCollections && !isDbStereotype && !node.isHasDbInteraction()) {
            if (isLeaf) {
                // Leaf with no collections and not a DB node → false positive
                node.setOperationType(null);
                node.getOperationTypes().clear();
            } else {
                // Non-leaf: collect ALL child operations
                Set<String> childOps = deriveAllOperationsFromChildren(node);
                node.getOperationTypes().addAll(childOps);
                // Keep highest priority as the primary operationType
                String best = null;
                for (String op : childOps) {
                    if (isHigherPriority(op, best)) best = op;
                }
                node.setOperationType(best);
            }
        }
        // Ensure primary op is always in the set
        if (node.getOperationType() != null) {
            node.getOperationTypes().add(node.getOperationType());
        }
    }

    /**
     * Derive a node's operation type from its children.
     * Priority: WRITE > UPDATE > DELETE > READ > AGGREGATE > COUNT > null
     */
    private Set<String> deriveAllOperationsFromChildren(CallNode node) {
        Set<String> ops = new LinkedHashSet<>();
        if (node.getChildren() != null) {
            for (CallNode child : node.getChildren()) {
                if (child.getOperationType() != null) ops.add(child.getOperationType());
                ops.addAll(child.getOperationTypes());
            }
        }
        return ops;
    }

    private boolean isHigherPriority(String op, String current) {
        return opPriority(op) > opPriority(current);
    }

    private int opPriority(String op) {
        if (op == null) return 0;
        return switch (op) {
            case "COUNT" -> 1;
            case "AGGREGATE" -> 2;
            case "READ" -> 3;
            case "DELETE" -> 4;
            case "UPDATE" -> 5;
            case "WRITE" -> 6;
            default -> 0;
        };
    }

    // ========== Enriched Node Creation ==========

    private CallNode createEnrichedNode(IndexedClass cls, IndexedMethod method, String nodeId, String controllerJar) {
        CallNode node = new CallNode();
        node.setId(nodeId);
        node.setClassName(cls.fqn);
        node.setSimpleClassName(cls.simpleName);
        node.setMethodName(method.name);
        node.setReturnType(method.returnType);
        node.setStereotype(cls.stereotype);
        node.setParameterTypes(method.paramTypes);
        node.setAnnotations(method.annotationNames);
        node.setSourceJar(cls.sourceJar);
        node.setLineCount(method.startLine > 0 && method.endLine >= method.startLine
                ? method.endLine - method.startLine + 1 : 0);

        String artifactId = ctx.jarArtifactMap.getOrDefault(cls.sourceJar, null);
        node.setModule(configLoader.jarToProject(cls.sourceJar, artifactId));
        node.setDomain(configLoader.jarToDomain(cls.sourceJar));

        boolean isCross = !Objects.equals(cls.sourceJar, controllerJar);
        node.setCrossModule(isCross);
        boolean isAsync = method.annotationNames != null && method.annotationNames.contains("Async");
        node.setCallType(isAsync ? "ASYNC" : (isCross ? "CROSS_MODULE" : "APPLICATION"));

        String opType = configLoader.inferOperationType(method.name, cls.stereotype);
        // @Aggregation → AGGREGATE, @Query → READ (use annotationDetailList, names have no @ prefix)
        if (opType == null) {
            for (AnnotationData ad : method.annotationDetailList) {
                if ("Aggregation".equals(ad.name())) { opType = "AGGREGATE"; break; }
                else if ("Query".equals(ad.name())) { opType = "READ"; break; }
            }
        }
        node.setOperationType(opType);
        if (opType != null) node.getOperationTypes().add(opType);

        // Full annotation details (name + attributes)
        List<Map<String, Object>> annDetails = new ArrayList<>();
        for (AnnotationData ad : method.annotationDetailList) {
            Map<String, Object> detail = new LinkedHashMap<>();
            detail.put("name", ad.name());
            if (ad.attributes() != null && !ad.attributes().isEmpty()) {
                detail.put("attributes", ad.attributes());
            }
            annDetails.add(detail);
        }
        node.setAnnotationDetails(annDetails);
        node.setStringLiterals(method.stringLiterals);

        // Extract Spring Cache annotation metadata (@Cacheable, @CacheEvict, @CachePut, @Caching)
        List<Map<String, Object>> cacheOps = extractCacheOperations(method.annotationDetailList);
        if (!cacheOps.isEmpty()) {
            node.setCacheOperations(cacheOps);
        }

        // Delegate collection gathering to CollectionResolver
        collectionResolver.collectCollections(cls, method, node, ctx);

        return node;
    }

    // ========== Spring Cache Annotation Extraction ==========

    /** Annotation names that represent individual cache operations. */
    private static final Map<String, String> CACHE_ANNOTATION_TYPES = Map.of(
            "Cacheable", "CACHEABLE",
            "CacheEvict", "CACHE_EVICT",
            "CachePut", "CACHE_PUT"
    );

    /**
     * Scan annotation list for Spring Cache annotations and build cache operation maps.
     * Handles direct @Cacheable/@CacheEvict/@CachePut and composite @Caching.
     */
    private List<Map<String, Object>> extractCacheOperations(List<AnnotationData> annotations) {
        List<Map<String, Object>> ops = new ArrayList<>();
        for (AnnotationData ad : annotations) {
            String cacheType = CACHE_ANNOTATION_TYPES.get(ad.name());
            if (cacheType != null) {
                // Direct cache annotation: @Cacheable, @CacheEvict, @CachePut
                Map<String, Object> op = buildCacheOp(cacheType, ad.attributes());
                if (op != null) ops.add(op);
            } else if ("Caching".equals(ad.name()) && ad.attributes() != null) {
                // Composite @Caching — attributes are: cacheable, evict, put (each a list of nested annotations)
                extractNestedCacheOps(ops, ad.attributes(), "cacheable", "CACHEABLE");
                extractNestedCacheOps(ops, ad.attributes(), "evict", "CACHE_EVICT");
                extractNestedCacheOps(ops, ad.attributes(), "put", "CACHE_PUT");
            }
        }
        return ops;
    }

    /**
     * Extract nested cache annotations from a @Caching attribute.
     * The bytecode parser stores nested annotations as List&lt;Map&lt;String,Object&gt;&gt;
     * where each map has "@type" key and the annotation attribute keys.
     */
    @SuppressWarnings("unchecked")
    private void extractNestedCacheOps(List<Map<String, Object>> ops,
                                        Map<String, Object> cachingAttrs,
                                        String attrName, String cacheType) {
        Object nested = cachingAttrs.get(attrName);
        if (nested instanceof List<?> list) {
            for (Object item : list) {
                if (item instanceof Map<?, ?> nestedMap) {
                    Map<String, Object> op = buildCacheOp(cacheType, (Map<String, Object>) nestedMap);
                    if (op != null) ops.add(op);
                }
            }
        } else if (nested instanceof Map<?, ?> singleMap) {
            // Single nested annotation (not wrapped in array)
            Map<String, Object> op = buildCacheOp(cacheType, (Map<String, Object>) singleMap);
            if (op != null) ops.add(op);
        }
    }

    /**
     * Build a cache operation map from annotation attributes.
     * Extracts cacheNames/value, key, condition, unless, allEntries, beforeInvocation.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> buildCacheOp(String type, Map<String, Object> attrs) {
        if (attrs == null) return null;
        Map<String, Object> op = new LinkedHashMap<>();
        op.put("type", type);

        // Cache names: "cacheNames" attribute takes priority, then "value"
        List<String> names = extractStringList(attrs, "cacheNames");
        if (names.isEmpty()) names = extractStringList(attrs, "value");
        if (!names.isEmpty()) op.put("cacheNames", names);

        // Key expression (SpEL)
        String key = extractString(attrs, "key");
        if (key != null) op.put("key", key);

        // Condition expression (SpEL)
        String condition = extractString(attrs, "condition");
        if (condition != null) op.put("condition", condition);

        // Unless expression (SpEL) — @Cacheable and @CachePut
        String unless = extractString(attrs, "unless");
        if (unless != null) op.put("unless", unless);

        // @CacheEvict-specific: allEntries and beforeInvocation
        if ("CACHE_EVICT".equals(type)) {
            Object allEntries = attrs.get("allEntries");
            if (Boolean.TRUE.equals(allEntries)) op.put("allEntries", true);
            Object beforeInvocation = attrs.get("beforeInvocation");
            if (Boolean.TRUE.equals(beforeInvocation)) op.put("beforeInvocation", true);
        }

        return op;
    }

    /** Extract a single string from annotation attributes (handles String values). */
    private String extractString(Map<String, Object> attrs, String key) {
        Object val = attrs.get(key);
        if (val instanceof String s && !s.isBlank()) return s;
        return null;
    }

    /** Extract a list of strings from annotation attributes (handles String, List&lt;String&gt;). */
    @SuppressWarnings("unchecked")
    private List<String> extractStringList(Map<String, Object> attrs, String key) {
        Object val = attrs.get(key);
        if (val instanceof String s && !s.isBlank()) return List.of(s);
        if (val instanceof List<?> list) {
            List<String> result = new ArrayList<>();
            for (Object item : list) {
                if (item instanceof String s && !s.isBlank()) result.add(s);
            }
            return result;
        }
        return List.of();
    }

    private CallNode createExternalLeaf(InvocationRef call) {
        CallNode node = new CallNode();
        String className = call.ownerClass();
        node.setId("ext:" + className + "#" + call.methodName() + call.descriptor());
        node.setClassName(className);
        int lastDot = className.lastIndexOf('.');
        node.setSimpleClassName(lastDot > 0 ? className.substring(lastDot + 1) : className);
        node.setMethodName(call.methodName());
        node.setReturnType(call.returnType());
        node.setCallType("CROSS_MODULE");
        node.setCrossModule(true);
        // Only infer operation types for application/framework classes, not standard library.
        // java.util.List.add(), Map.remove(), etc. are NOT DB operations.
        if (!isStandardLibraryClass(className)) {
            node.setOperationType(configLoader.inferOperationType(call.methodName(), null));
        }
        return node;
    }

    /**
     * Returns true for JDK classes and well-known third-party library packages.
     * These are never recursed into during tree building, even when indexed from nested JARs
     * inside an analyzed WAR (which would otherwise put them in classMap and trigger false recursion).
     */
    private static boolean isStandardLibraryClass(String className) {
        if (className == null) return false;
        return className.startsWith("java.") || className.startsWith("javax.")
            || className.startsWith("jakarta.") || className.startsWith("sun.")
            || className.startsWith("jdk.") || className.startsWith("com.sun.")
            // Logging
            || className.startsWith("org.slf4j.") || className.startsWith("org.apache.logging.")
            || className.startsWith("ch.qos.logback.")
            // JDBC connection pools
            || className.startsWith("com.zaxxer.hikari.") || className.startsWith("com.mchange.v2.")
            || className.startsWith("org.apache.commons.dbcp.") || className.startsWith("com.jolbox.")
            || className.startsWith("com.alibaba.druid.")
            // Google / Guava
            || className.startsWith("com.google.common.") || className.startsWith("com.google.guava.")
            || className.startsWith("com.google.gson.") || className.startsWith("com.google.protobuf.")
            // Apache commons and HTTP
            || className.startsWith("org.apache.commons.") || className.startsWith("org.apache.http.")
            || className.startsWith("org.apache.tomcat.") || className.startsWith("org.apache.catalina.")
            || className.startsWith("org.apache.coyote.")
            // Spring Framework internals (not user code)
            || className.startsWith("org.springframework.")
            // Hibernate / JPA internals
            || className.startsWith("org.hibernate.") || className.startsWith("org.jboss.")
            || className.startsWith("net.bytebuddy.") || className.startsWith("javassist.")
            // Jackson serialization
            || className.startsWith("com.fasterxml.jackson.")
            // Reactive / Netty
            || className.startsWith("io.netty.") || className.startsWith("reactor.")
            || className.startsWith("io.reactivex.") || className.startsWith("rx.")
            // Observability
            || className.startsWith("io.micrometer.") || className.startsWith("io.opentelemetry.")
            // Security / token libraries
            || className.startsWith("org.bouncycastle.") || className.startsWith("io.jsonwebtoken.")
            // Oracle JDBC driver internals
            || className.startsWith("oracle.jdbc.driver.") || className.startsWith("oracle.jdbc.proxy.")
            // ASM / bytecode tools
            || className.startsWith("org.objectweb.asm.");
    }

    private static boolean isRepoStereotype(String stereotype) {
        return "REPOSITORY".equals(stereotype) || "SPRING_DATA".equals(stereotype);
    }

    /**
     * Create a synthetic leaf node for a Spring Data derived query method
     * (findByX, deleteByX, countByX, existsByX) that doesn't exist in bytecode.
     */
    private CallNode createDerivedQueryLeaf(IndexedClass repoClass, InvocationRef call) {
        String methodName = call.methodName();
        String opType = configLoader.inferOperationType(methodName, repoClass.stereotype);
        if (opType == null) return null; // not a recognizable operation

        CallNode node = new CallNode();
        node.setId(repoClass.fqn + "#" + methodName + call.descriptor());
        node.setClassName(repoClass.fqn);
        node.setSimpleClassName(repoClass.simpleName);
        node.setMethodName(methodName);
        node.setReturnType(call.returnType());
        node.setStereotype(repoClass.stereotype);
        node.setOperationType(opType);
        node.setDispatchType("SPRING_DATA_DERIVED");
        // Inherit collections from the repository's mapped collection
        String coll = ctx.repoCollectionMap.get(repoClass.fqn);
        if (coll == null) coll = ctx.repoCollectionMap.get(repoClass.simpleName);
        if (coll != null) {
            node.getCollectionsAccessed().add(coll);
            String domain = configLoader.detectCollectionDomain(coll);
            if (domain != null) node.getCollectionDomains().put(coll, domain);
        }
        return node;
    }

    /**
     * Create an HTTP_CALL leaf for a @FeignClient interface method.
     * The interface is a declarative REST client — each method maps to an HTTP call.
     * We extract the path from @RequestMapping/@GetMapping etc. on the interface method if available.
     */
    private CallNode createFeignCallLeaf(IndexedClass feignClass, InvocationRef call, IndexedMethod callerMethod) {
        CallNode node = new CallNode();
        node.setId("feign:" + feignClass.fqn + "#" + call.methodName() + call.descriptor());
        node.setClassName(feignClass.fqn);
        node.setSimpleClassName(feignClass.simpleName);
        node.setMethodName(call.methodName());
        node.setReturnType(call.returnType());
        node.setStereotype("HTTP_CLIENT");
        node.setCallType("HTTP_CALL");
        node.setCrossModule(true);
        node.setOperationType(inferHttpOperation(call.methodName()));
        // Try to pull HTTP method/path from the indexed interface method
        String methodKey = call.methodName() + call.descriptor();
        IndexedMethod im = feignClass.methods.get(methodKey);
        if (im == null) im = feignClass.methods.get("S:" + methodKey);
        if (im != null && im.httpMethod != null) {
            node.setAnnotations(List.of("@" + im.httpMethod));
            if (im.path != null && !im.path.isBlank()) {
                node.setStringLiterals(List.of(im.path));
            }
        }
        return node;
    }

    private CallNode createHttpCallLeaf(InvocationRef call, IndexedMethod callerMethod) {
        CallNode node = new CallNode();
        String className = call.ownerClass();
        node.setId("http:" + className + "#" + call.methodName() + call.descriptor());
        node.setClassName(className);
        int lastDot = className.lastIndexOf('.');
        node.setSimpleClassName(lastDot > 0 ? className.substring(lastDot + 1) : className);
        node.setMethodName(call.methodName());
        node.setReturnType(call.returnType());
        node.setCallType("HTTP_CALL");
        node.setCrossModule(true);
        node.setOperationType(inferHttpOperation(call.methodName()));
        // Try to extract URL from string literals in the calling method
        if (callerMethod.stringLiterals != null && !callerMethod.stringLiterals.isEmpty()) {
            List<String> urls = callerMethod.stringLiterals.stream()
                    .filter(s -> s.startsWith("http://") || s.startsWith("https://") || s.startsWith("/"))
                    .toList();
            node.setStringLiterals(urls.isEmpty() ? null : urls);
        }
        return node;
    }

    private String inferHttpOperation(String methodName) {
        if (methodName == null) return "READ";
        String n = methodName.toLowerCase();
        if (n.contains("post") || n.contains("put") || n.contains("save") || n.contains("create")) return "WRITE";
        if (n.contains("delete") || n.contains("remove")) return "DELETE";
        if (n.contains("patch") || n.contains("update")) return "UPDATE";
        return "READ";
    }

    // ========== Method resolution ==========

    private record ResolvedRef(IndexedClass cls, IndexedMethod method,
                                String dispatchType, String resolvedFrom, String qualifierHint) {}

    /**
     * Resolve ALL concrete implementations for a method call.
     * For interfaces/abstract classes, returns every implementation that has the method.
     * For concrete classes, returns the single match (or walks super/subclass hierarchy).
     */
    private List<ResolvedRef> resolveAllMethods(String className, String methodName, String descriptor,
                                                 InvocationRef call, IndexedClass callerClass) {
        boolean isStaticCall = call != null && call.opcode() == 184; // Opcodes.INVOKESTATIC
        String key = (isStaticCall ? "S:" : "") + methodName + descriptor;
        IndexedClass cls = ctx.classMap.get(className);
        if (cls != null) {
            IndexedMethod m = cls.methods.get(key);
            if (m != null) {
                if (cls.isInterface || cls.isAbstract) {
                    List<ResolvedRef> impls = resolveAllFromImplementations(className, key);
                    if (!impls.isEmpty()) {
                        // Try to narrow using @Qualifier or field name heuristic
                        ResolvedRef narrowed = tryNarrowImplementation(impls, call, callerClass, className);
                        if (narrowed != null) return List.of(narrowed);
                        if (impls.size() > 1) {
                            // List<Interface> injection context — all impls are intentional
                            if (isListInjectContext(callerClass, className)) return markListInject(impls, className);
                            return markAmbiguous(impls, className);
                        }
                        return impls;
                    }
                    // No implementations found — fallback to interface/abstract itself
                    return List.of(new ResolvedRef(cls, m, "INTERFACE_FALLBACK", className, null));
                }
                return List.of(new ResolvedRef(cls, m, "DIRECT", null, null));
            }
            // Walk superclass chain
            String superClass = ctx.superClassMap.get(className);
            if (superClass != null && ctx.classMap.containsKey(superClass)) {
                return resolveAllMethods(superClass, methodName, descriptor, call, callerClass);
            }
        }
        // Interface not in classMap — resolve from implementations
        List<ResolvedRef> impls = resolveAllFromImplementations(className, key);
        if (!impls.isEmpty()) {
            ResolvedRef narrowed = tryNarrowImplementation(impls, call, callerClass, className);
            if (narrowed != null) return List.of(narrowed);
            if (impls.size() > 1) {
                if (isListInjectContext(callerClass, className)) return markListInject(impls, className);
                return markAmbiguous(impls, className);
            }
            return impls;
        }

        // Check subclass hierarchy via BFS — handles N-level deep inheritance
        List<String> firstChildren = ctx.childClassMap.get(className);
        if (firstChildren != null) {
            Queue<String> bfsQueue = new LinkedList<>(firstChildren);
            Set<String> bfsSeen = new HashSet<>(firstChildren);
            while (!bfsQueue.isEmpty()) {
                String childFqn = bfsQueue.poll();
                IndexedClass childCls = ctx.classMap.get(childFqn);
                if (childCls != null) {
                    IndexedMethod m = childCls.methods.get(key);
                    if (m != null && !childCls.isInterface) {
                        return List.of(new ResolvedRef(childCls, m, "DIRECT", null, null));
                    }
                }
                List<String> grandchildren = ctx.childClassMap.get(childFqn);
                if (grandchildren != null) {
                    for (String gc : grandchildren) {
                        if (bfsSeen.add(gc)) bfsQueue.add(gc);
                    }
                }
            }
        }
        return List.of();
    }

    /**
     * Remap a list of candidate implementations to AMBIGUOUS_IMPL dispatch type.
     * Called when narrowing failed with more than one candidate — every child gets a
     * clear marker so the UI can show a fan-out warning instead of a misleading DIRECT.
     */
    private List<ResolvedRef> markAmbiguous(List<ResolvedRef> impls, String interfaceFqn) {
        String hint = impls.size() + " candidates";
        List<ResolvedRef> out = new ArrayList<>(impls.size());
        for (ResolvedRef r : impls) {
            out.add(new ResolvedRef(r.cls(), r.method(), "AMBIGUOUS_IMPL", interfaceFqn, hint));
        }
        return out;
    }

    /** Mark all impls as LIST_INJECT — the caller injects them all via List&lt;Interface&gt;. */
    private List<ResolvedRef> markListInject(List<ResolvedRef> impls, String interfaceFqn) {
        int lastDot = interfaceFqn.lastIndexOf('.');
        String simpleName = lastDot >= 0 ? interfaceFqn.substring(lastDot + 1) : interfaceFqn;
        String hint = "List<" + simpleName + "> injection";
        List<ResolvedRef> out = new ArrayList<>(impls.size());
        for (ResolvedRef r : impls) {
            out.add(new ResolvedRef(r.cls(), r.method(), "LIST_INJECT", interfaceFqn, hint));
        }
        return out;
    }

    /** True if the caller has a List-typed injected field whose element type is interfaceFqn. */
    private boolean isListInjectContext(IndexedClass callerClass, String interfaceFqn) {
        if (callerClass == null) return false;
        for (FieldDIInfo di : callerClass.injectedFields.values()) {
            if (interfaceFqn.equals(di.collectionElementType())) return true;
        }
        return false;
    }

    /** True if any injection-point custom qualifier matches a bean class custom qualifier by name+value. */
    private boolean customQualifierMatches(List<AnnotationData> injectionAnns, List<AnnotationData> beanAnns) {
        for (AnnotationData ia : injectionAnns) {
            for (AnnotationData ba : beanAnns) {
                if (ia.name().equals(ba.name())) {
                    Object iv = ia.attributes().get("value");
                    Object bv = ba.attributes().get("value");
                    if (iv != null && iv.equals(bv)) return true;
                    if (iv == null && bv == null) return true; // marker annotation match
                }
            }
        }
        return false;
    }

    /**
     * Return ALL concrete implementations of an interface/abstract method.
     *
     * For each registered implementor, walks the superclass chain if the method
     * is not declared directly on that class (handles inherited implementations).
     * Skips abstract classes and interfaces — only returns concrete overrides.
     * Deduplicates by the class that actually owns the method so the same inherited
     * implementation is not emitted multiple times for sibling subclasses.
     */
    private List<ResolvedRef> resolveAllFromImplementations(String interfaceName, String methodKey) {
        List<String> impls = ctx.interfaceImplMap.get(interfaceName);
        if (impls == null) return List.of();
        List<ResolvedRef> results = new ArrayList<>();
        // Track (concreteClass, definingClass) pairs already added to avoid duplicates
        // when multiple subclasses inherit the same method from one abstract base.
        Set<String> seenConcrete = new HashSet<>();
        for (String implClass : impls) {
            IndexedClass concreteCls = ctx.classMap.get(implClass);
            if (concreteCls == null || concreteCls.isInterface) continue;
            // Skip abstract classes — they have concrete subclasses that will appear
            // separately in the list (added by expandInterfaceImplMapTransitively).
            if (concreteCls.isAbstract) continue;
            if (!seenConcrete.add(concreteCls.fqn)) continue;

            // Walk superclass chain to find the method (may be inherited, not redeclared)
            IndexedClass search = concreteCls;
            IndexedMethod m = null;
            while (search != null && m == null) {
                m = search.methods.get(methodKey);
                if (m == null) {
                    String superFqn = ctx.superClassMap.get(search.fqn);
                    search = (superFqn != null) ? ctx.classMap.get(superFqn) : null;
                }
            }
            if (m != null) {
                results.add(new ResolvedRef(concreteCls, m, "DYNAMIC_DISPATCH", interfaceName, null));
            }
        }
        // Default interface method fallback: if no concrete class overrides the method, check
        // whether the interface itself declares it as a Java 8+ default method.
        if (results.isEmpty()) {
            IndexedClass ifaceCls = ctx.classMap.get(interfaceName);
            if (ifaceCls != null && ifaceCls.isInterface) {
                IndexedMethod defaultMethod = ifaceCls.methods.get(methodKey);
                if (defaultMethod != null) {
                    Set<String> addedConcrete = new HashSet<>();
                    for (String implClass : impls) {
                        IndexedClass concreteCls = ctx.classMap.get(implClass);
                        if (concreteCls == null || concreteCls.isInterface || concreteCls.isAbstract) continue;
                        if (!addedConcrete.add(concreteCls.fqn)) continue;
                        results.add(new ResolvedRef(concreteCls, defaultMethod, "DEFAULT_METHOD", interfaceName, "default"));
                    }
                }
            }
        }
        return results;
    }

    /**
     * Try to narrow multiple implementations to a single one using DI metadata.
     * Priority order:
     * 1a: @Qualifier exact bean name match (Spring convention: lcfirst(simpleName))
     * 1b: @Qualifier @Bean name mapping from @Configuration classes
     * 1c: @Qualifier substring fallback (for non-standard naming)
     * 2a: Field name heuristic (substring containment)
     * 2b: Field name @Bean name match
     * 3:  @Primary (exactly one implementation marked @Primary)
     */
    private ResolvedRef tryNarrowImplementation(List<ResolvedRef> impls, InvocationRef call,
                                                 IndexedClass callerClass, String interfaceName) {
        if (call == null || callerClass == null) return null;

        // Strategy 1 + 2: Field-based narrowing (covers field/constructor/setter injection)
        String fieldName = call.receiverFieldName();
        if (fieldName != null) {
            CallGraphIndex.FieldDIInfo di = callerClass.injectedFields.get(fieldName);
            // Strategy 1: @Qualifier-based resolution (3-tier)
            if (di != null && di.qualifierValue() != null) {
                String q = di.qualifierValue();
                String qLower = q.toLowerCase();

                // Strategy 1a: Exact bean name match (Spring convention: lcfirst(simpleName))
                for (ResolvedRef ref : impls) {
                    String beanName = Character.toLowerCase(ref.cls().simpleName.charAt(0))
                                      + ref.cls().simpleName.substring(1);
                    if (q.equals(beanName) || ref.cls().fqn.endsWith("." + q)) {
                        return new ResolvedRef(ref.cls(), ref.method(), "QUALIFIED", interfaceName, q);
                    }
                }

                // Strategy 1b: @Bean name mapping from @Configuration classes
                String beanImpl = ctx.beanNameToImplMap.get(q);
                if (beanImpl != null) {
                    for (ResolvedRef ref : impls) {
                        if (ref.cls().fqn.equals(beanImpl)) {
                            return new ResolvedRef(ref.cls(), ref.method(), "QUALIFIED", interfaceName, q);
                        }
                    }
                }

                // Strategy 1d: Explicit stereotype bean name (@Service("name"), @Component("name"), etc.)
                String stereotypeImpl = ctx.stereotypeBeanNameMap.get(q);
                if (stereotypeImpl != null) {
                    for (ResolvedRef ref : impls) {
                        if (ref.cls().fqn.equals(stereotypeImpl)) {
                            return new ResolvedRef(ref.cls(), ref.method(), "QUALIFIED", interfaceName, q);
                        }
                    }
                }

                // Strategy 1c: Substring fallback (for non-standard naming)
                for (ResolvedRef ref : impls) {
                    String implSimple = ref.cls().simpleName.toLowerCase();
                    if (implSimple.contains(qLower) || ref.cls().fqn.toLowerCase().endsWith("." + qLower)) {
                        return new ResolvedRef(ref.cls(), ref.method(), "QUALIFIED", interfaceName, q);
                    }
                }
            }
            // Strategy 2c: Custom qualifier cross-match — injection point carries @AnalysisType("statistical"),
            // bean class also carries @AnalysisType("statistical") → matched by annotation name + value.
            // Runs BEFORE field-name heuristics so annotation-based resolution takes precedence.
            if (di != null && di.customQualifiers() != null && !di.customQualifiers().isEmpty()) {
                for (ResolvedRef ref : impls) {
                    List<AnnotationData> beanAnns = ctx.classCustomQualifiers.get(ref.cls().fqn);
                    if (beanAnns != null && customQualifierMatches(di.customQualifiers(), beanAnns)) {
                        return new ResolvedRef(ref.cls(), ref.method(), "QUALIFIED", interfaceName,
                                di.customQualifiers().get(0).name());
                    }
                }
            }
            // Strategy 2a: Field name heuristic — full-name substring match
            String fieldLower = fieldName.toLowerCase();
            if (fieldLower.length() > 3) {
                for (ResolvedRef ref : impls) {
                    String implLower = ref.cls().simpleName.toLowerCase();
                    if (implLower.contains(fieldLower) || fieldLower.contains(implLower)) {
                        return new ResolvedRef(ref.cls(), ref.method(), "HEURISTIC", interfaceName, fieldName);
                    }
                }
                // Strategy 2a continued: camelCase word-segment matching.
                // Handles cases where the field name is shorter than the class name, e.g.
                // field "trendStrategy" → segments ["trend","Strategy"] → "trend" found in
                // "TrendAnalysisStrategy" even though "trendstrategy" ⊄ "trendanalysisstrategy".
                String[] segments = fieldName.split("(?<=[a-z])(?=[A-Z])");
                for (String seg : segments) {
                    String segLower = seg.toLowerCase();
                    if (segLower.length() <= 3) continue; // skip generic/short segments
                    for (ResolvedRef ref : impls) {
                        String implLower = ref.cls().simpleName.toLowerCase();
                        if (implLower.contains(segLower)) {
                            return new ResolvedRef(ref.cls(), ref.method(), "HEURISTIC", interfaceName, fieldName);
                        }
                    }
                }
            }
            // Strategy 2b: Field name matches a @Bean name
            if (fieldName != null) {
                String beanImpl = ctx.beanNameToImplMap.get(fieldName);
                if (beanImpl != null) {
                    for (ResolvedRef ref : impls) {
                        if (ref.cls().fqn.equals(beanImpl)) {
                            return new ResolvedRef(ref.cls(), ref.method(), "QUALIFIED", interfaceName, fieldName);
                        }
                    }
                }
            }
        }

        // Strategy 3: @Primary — exactly one implementation marked @Primary
        List<ResolvedRef> primaryImpls = new ArrayList<>();
        for (ResolvedRef ref : impls) {
            IndexedClass implCls = ctx.classMap.get(ref.cls().fqn);
            if (implCls != null && implCls.isPrimary) {
                primaryImpls.add(ref);
            }
        }
        if (primaryImpls.size() == 1) {
            ResolvedRef ref = primaryImpls.get(0);
            return new ResolvedRef(ref.cls(), ref.method(), "PRIMARY", interfaceName, "@Primary");
        }

        return null;
    }

    // ========== Noise Filtering ==========

    /** Common methods that never produce meaningful call tree branches. */
    private static final Set<String> NOISE_METHODS = Set.of(
            "toString", "hashCode", "equals", "compareTo", "clone",
            "valueOf", "values", "ordinal", "name",
            "builder", "build", "toBuilder"
    );

    /** Skip universally noisy methods. */
    private boolean isNoiseMethod(String methodName) {
        return NOISE_METHODS.contains(methodName);
    }

    /** Suffixes that identify data-carrying classes (DTOs, entities, request/response objects). */
    private static final Set<String> DTO_SUFFIXES = Set.of(
            "DTO", "Dto", "Request", "Response", "VO", "Model", "Bean",
            "Payload", "Command", "Event", "Message", "Result", "Data",
            "Info", "Detail", "Details", "Summary", "Item", "Entry",
            "Record", "Wrapper", "Holder", "Container", "Param", "Params",
            "Builder"  // Lombok/manual builder inner classes (e.g. Order$OrderBuilder)
    );

    /** Static factory method names commonly used on response-wrapper classes. */
    private static final Set<String> RESPONSE_FACTORY_METHODS = Set.of(
            "success", "ok", "error", "fail", "failure", "of", "from", "wrap",
            "created", "accepted", "notFound", "badRequest", "unauthorized"
    );

    /**
     * True if a static call on a DTO-suffix class is a response-wrapper factory method.
     * e.g. ApiResponse.success(data), Result.of(value), ResponseWrapper.error("msg").
     * These wrap a return value but contain no business logic worth traversing.
     */
    private boolean isResponseWrapperFactory(String className, String methodName) {
        if (!RESPONSE_FACTORY_METHODS.contains(methodName)) return false;
        IndexedClass cls = ctx.classMap.get(className);
        if (cls == null) return false;
        for (String suffix : DTO_SUFFIXES) {
            if (cls.simpleName.endsWith(suffix)) return true;
        }
        return false;
    }

    /**
     * Skip getter/setter/builder calls on DTO/model/entity classes.
     * Uses both name patterns AND method characteristics (param count, line count,
     * invocation count) to avoid filtering real business methods like setPaymentStatus().
     */
    private boolean isDtoCall(String targetClass, String methodName) {
        IndexedClass cls = ctx.classMap.get(targetClass);
        if (cls == null) return false;
        // Allow ENTITY, OTHER (plain POJOs), and null stereotype through to accessor check.
        // Only skip filtering for known DB-interacting roles (SERVICE, REPOSITORY, COMPONENT, etc.)
        if (cls.stereotype != null && !"ENTITY".equals(cls.stereotype) && !"OTHER".equals(cls.stereotype)) return false;
        if (cls.isInterface) return false;
        boolean isDto = cls.documentCollection != null;
        if (!isDto) {
            for (String suffix : DTO_SUFFIXES) {
                if (cls.simpleName.endsWith(suffix)) { isDto = true; break; }
            }
        }
        // JPA @Entity classes generate getter/setter noise identical to DTOs — filter them too
        if (!isDto && cls.isJpaEntity) isDto = true;
        if (!isDto) return false;
        // Check actual method characteristics — not just name prefix
        return isSimpleAccessor(cls, methodName);
    }

    /** Check if method is a simple accessor based on name, param count, line count, and calls. */
    private boolean isSimpleAccessor(IndexedClass cls, String methodName) {
        if (methodName.length() <= 2) return false;
        // Find the actual method in the class
        for (IndexedMethod m : cls.methods.values()) {
            if (!m.name.equals(methodName)) continue;
            int params = m.paramTypes.size();
            int lines = (m.startLine > 0 && m.endLine >= m.startLine)
                    ? m.endLine - m.startLine + 1 : 0;
            int calls = m.invocations.size();

            if (methodName.startsWith("get") || methodName.startsWith("is")) {
                // getter: 0 params, short body, no meaningful calls
                return params == 0 && lines <= 5 && calls <= 1;
            }
            if (methodName.startsWith("set")) {
                // setter: exactly 1 param, short body, no meaningful calls
                return params == 1 && lines <= 5 && calls <= 1;
            }
            if (methodName.startsWith("with") || methodName.startsWith("add") || methodName.startsWith("put")) {
                // builder: 1 param, short body
                return params <= 1 && lines <= 5 && calls <= 1;
            }
            return false;
        }
        // Method not found in class — use name-only heuristic as fallback
        return methodName.startsWith("get") || methodName.startsWith("set")
                || methodName.startsWith("is") || methodName.startsWith("with");
    }

    private final Map<String, Boolean> appClassCache = new java.util.concurrent.ConcurrentHashMap<>();

    private boolean isApplicationClass(String className) {
        Boolean cached = appClassCache.get(className);
        if (cached != null) return cached;

        boolean result = false;
        if (ctx.detectedBasePackages != null) {
            for (String pkg : ctx.detectedBasePackages) {
                if (className.startsWith(pkg + ".")) { result = true; break; }
            }
        }
        if (!result) {
            List<String> children = ctx.childClassMap.get(className);
            if (children != null) {
                for (String childFqn : children) {
                    if (ctx.classMap.containsKey(childFqn)) { result = true; break; }
                }
            }
        }
        appClassCache.put(className, result);
        return result;
    }

    /** Check if a class looks like a constants holder (CollectionNames, Constants, etc.). */
    private static boolean isConstantsClass(IndexedClass ic) {
        String name = ic.simpleName;
        return name.endsWith("Constants") || name.endsWith("Names") || name.endsWith("Collections")
                || name.endsWith("Consts") || name.endsWith("Config")
                || name.endsWith("Definitions") || name.endsWith("Keys")
                || name.endsWith("Fields") || name.endsWith("Enums");
    }

    // ========== @NamedQuery indexing ==========

    /**
     * Index @NamedQuery / @NamedNativeQuery / @NamedQueries / @NamedNativeQueries annotations
     * found on @Entity classes. Each named query's text is stored in ctx.namedQueryMap keyed
     * by the query name, so createNamedQuery("Name") calls can be resolved to their SQL/JPQL.
     */
    private void indexNamedQuery(AnnotationInfo ann, ResolutionContext ctx) {
        String aName = ann.getName();
        if ("NamedQuery".equals(aName) || "NamedNativeQuery".equals(aName)) {
            Object name = ann.getAttributes().get("name");
            Object query = ann.getAttributes().get("query");
            if (name instanceof String n && !n.isBlank() && query instanceof String q && !q.isBlank()) {
                ctx.namedQueryMap.put(n.trim(), q.trim());
            }
        } else if ("NamedQueries".equals(aName) || "NamedNativeQueries".equals(aName)) {
            Object value = ann.getAttributes().get("value");
            if (value instanceof List<?> list) {
                for (Object item : list) {
                    if (item instanceof AnnotationInfo nested) {
                        indexNamedQuery(nested, ctx);
                    } else if (item instanceof Map<?,?> map) {
                        Object n = map.get("name"), q = map.get("query");
                        if (n instanceof String ns && !ns.isBlank() && q instanceof String qs && !qs.isBlank()) {
                            ctx.namedQueryMap.put(ns.trim(), qs.trim());
                        }
                    }
                }
            }
        } else if ("NamedStoredProcedureQuery".equals(aName)) {
            // JPA @NamedStoredProcedureQuery: maps a logical name to an actual DB procedure name.
            // Stored with "PROC:" prefix so createNamedStoredProcedureQuery("name") can look it up.
            Object name = ann.getAttributes().get("name");
            Object procName = ann.getAttributes().get("procedureName");
            if (name instanceof String n && !n.isBlank() && procName instanceof String p && !p.isBlank()) {
                ctx.namedQueryMap.put(n.trim(), "PROC:" + p.trim());
            }
        } else if ("NamedStoredProcedureQueries".equals(aName)) {
            Object value = ann.getAttributes().get("value");
            if (value instanceof List<?> list) {
                for (Object item : list) {
                    if (item instanceof AnnotationInfo nested) {
                        indexNamedQuery(nested, ctx);
                    } else if (item instanceof Map<?,?> map) {
                        Object n = map.get("name"), p = map.get("procedureName");
                        if (n instanceof String ns && !ns.isBlank() && p instanceof String ps && !ps.isBlank()) {
                            ctx.namedQueryMap.put(ns.trim(), "PROC:" + ps.trim());
                        }
                    }
                }
            }
        }
    }

    // ========== Spring Event Listener support ==========

    /** Register a method as an event listener for the given event type FQN. */
    private void registerEventListener(IndexedClass ic, MethodInfo mi, String eventTypeFqn, boolean isTransactional) {
        boolean isStatic = (mi.getAccessFlags() & 0x0008) != 0;
        String key = (isStatic ? "S:" : "") + mi.getName() + mi.getDescriptor();
        IndexedMethod im = ic.methods.get(key);
        if (im == null) return;
        ctx.eventListenerMap.computeIfAbsent(eventTypeFqn, k -> new ArrayList<>())
                .add(new EventListenerRef(ic, im, isTransactional));
    }

    /** Known owner classes (or suffixes) for ApplicationEventPublisher.publishEvent(). */
    private static boolean isEventPublisherClass(String className) {
        if (className == null) return false;
        return className.equals("org.springframework.context.ApplicationEventPublisher")
                || className.equals("org.springframework.context.ApplicationContext")
                || className.equals("org.springframework.context.ConfigurableApplicationContext")
                || className.endsWith("EventPublisher");
    }

    /**
     * Detect the event type from a publishEvent invocation and create child nodes
     * for all matching @EventListener methods.
     *
     * @return number of children added, or -1 if this is not a publishEvent call
     */
    private int connectEventListeners(InvocationRef call, IndexedMethod callerMethod,
                                       CallNode parentNode, Set<String> callStack,
                                       int depth, int[] nodeCount, String controllerJar,
                                       Map<String, CallNode> memo, int childCount) {
        if (!"publishEvent".equals(call.methodName())) return -1;
        if (!isEventPublisherClass(call.ownerClass())) return -1;
        if (ctx.eventListenerMap.isEmpty()) return 0;

        // Strategy 1: Check __class: prefixed args on the publishEvent call itself
        String eventType = null;
        if (call.stringArgs() != null) {
            for (String arg : call.stringArgs()) {
                if (arg.startsWith("__class:")) {
                    String candidate = arg.substring(8);
                    if (ctx.eventListenerMap.containsKey(candidate)) {
                        eventType = candidate;
                        break;
                    }
                    // Also check if it's a subtype of a registered event type (interface/superclass)
                    eventType = resolveEventTypeViaHierarchy(candidate);
                    if (eventType == null) {
                        // Even if no listeners found yet, this is the best type we have
                        eventType = candidate;
                    }
                    break;
                }
            }
        }

        // Strategy 2: Scan preceding <init> calls in the same method's invocations
        // to find the event constructor (e.g., new SomeEvent(...) just before publishEvent)
        if (eventType == null) {
            eventType = findEventTypeFromPrecedingInit(call, callerMethod);
        }

        // Strategy 3: Parse the descriptor of publishEvent for typed overloads
        // e.g., publishEvent(Lcom/example/SomeEvent;)V
        if (eventType == null) {
            eventType = extractEventTypeFromDescriptor(call.descriptor());
        }

        if (eventType == null) return 0;

        // Look up all listeners for this event type (direct match)
        List<EventListenerRef> listeners = ctx.eventListenerMap.get(eventType);

        // Also check listeners for superclasses/interfaces of the event
        // (e.g., listener for ApplicationEvent catches all subtypes)
        List<EventListenerRef> allListeners = new ArrayList<>();
        if (listeners != null) allListeners.addAll(listeners);

        // Check if any registered event type is a superclass/interface of our event type
        for (Map.Entry<String, List<EventListenerRef>> entry : ctx.eventListenerMap.entrySet()) {
            String registeredType = entry.getKey();
            if (registeredType.equals(eventType)) continue;
            if (isSubtypeOf(eventType, registeredType)) {
                allListeners.addAll(entry.getValue());
            }
        }

        if (allListeners.isEmpty()) return 0;

        int added = 0;
        for (EventListenerRef ref : allListeners) {
            if (childCount + added >= maxChildrenPerNode || nodeCount[0] >= maxNodesPerTree) break;
            CallNode child = buildCallTree(ref.cls(), ref.method(), callStack, depth + 1,
                    nodeCount, controllerJar, memo);
            if (child != null) {
                child.setCallType(ref.isTransactional() ? "ASYNC_EVENT" : "EVENT_LISTENER");
                child.setDispatchType("EVENT_DISPATCH");
                child.setResolvedFrom(eventType);
                parentNode.getChildren().add(child);
                added++;
            }
        }
        return added;
    }

    /**
     * Walk the invocations list backward from the publishEvent call
     * and find the most recent constructor whose owner is a known event type.
     */
    private String findEventTypeFromPrecedingInit(InvocationRef publishCall, IndexedMethod callerMethod) {
        // Find the position of publishCall in the invocations list
        int publishIdx = -1;
        for (int i = 0; i < callerMethod.invocations.size(); i++) {
            if (callerMethod.invocations.get(i) == publishCall) {
                publishIdx = i;
                break;
            }
        }
        if (publishIdx < 0) return null;

        // Walk backward looking for <init> calls that match a registered event type
        for (int i = publishIdx - 1; i >= 0 && i >= publishIdx - 5; i--) {
            InvocationRef prev = callerMethod.invocations.get(i);
            if ("<init>".equals(prev.methodName())) {
                String ownerClass = prev.ownerClass();
                // Direct match
                if (ctx.eventListenerMap.containsKey(ownerClass)) {
                    return ownerClass;
                }
                // Hierarchy match: check if ownerClass extends/implements a registered event type
                String resolved = resolveEventTypeViaHierarchy(ownerClass);
                if (resolved != null) return ownerClass; // return the concrete type
                // If it looks like an event class (ends with Event, Message, etc.)
                if (ownerClass.endsWith("Event") || ownerClass.endsWith("Message")
                        || ownerClass.endsWith("Notification")) {
                    return ownerClass;
                }
            }
        }
        return null;
    }

    /**
     * Check if the publishEvent descriptor contains a specific event type
     * (not just Object or ApplicationEvent).
     */
    private String extractEventTypeFromDescriptor(String descriptor) {
        if (descriptor == null) return null;
        // Descriptor like (Lcom/example/SomeEvent;)V
        if (descriptor.startsWith("(L") && descriptor.contains(";)")) {
            String typePart = descriptor.substring(2, descriptor.indexOf(";)"));
            String fqn = typePart.replace('/', '.');
            // Skip generic Object/ApplicationEvent parameters — not useful
            if ("java.lang.Object".equals(fqn)
                    || "org.springframework.context.ApplicationEvent".equals(fqn)) {
                return null;
            }
            return fqn;
        }
        return null;
    }

    /** Check if candidateType extends/implements registeredType via the indexed hierarchy. */
    private boolean isSubtypeOf(String candidateType, String registeredType) {
        if (candidateType == null || registeredType == null) return false;
        // Check interface chain
        IndexedClass cls = ctx.classMap.get(candidateType);
        if (cls == null) return false;
        if (cls.interfaces != null && cls.interfaces.contains(registeredType)) return true;
        // Check superclass chain
        String superClass = ctx.superClassMap.get(candidateType);
        if (registeredType.equals(superClass)) return true;
        if (superClass != null) return isSubtypeOf(superClass, registeredType);
        return false;
    }

    /** Find a registered event listener type that the candidate type extends/implements. */
    private String resolveEventTypeViaHierarchy(String candidateType) {
        for (String registeredType : ctx.eventListenerMap.keySet()) {
            if (isSubtypeOf(candidateType, registeredType)) {
                return registeredType;
            }
        }
        return null;
    }
}
