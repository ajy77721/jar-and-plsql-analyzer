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

    // Spring Batch step interfaces → the single method each contract requires, keyed by the
    // interface FQN as it appears in ClassInfo.getInterfaces() (already dot-separated).
    // A class registers as a batch entry point purely by implementing one of these — no
    // annotation is involved, unlike @Scheduled/@RabbitListener/@KafkaListener below.
    private static final Map<String, String> BATCH_STEP_INTERFACE_METHODS = Map.of(
            "org.springframework.batch.core.step.tasklet.Tasklet", "execute",
            "org.springframework.batch.item.ItemReader", "read",
            "org.springframework.batch.item.ItemWriter", "write",
            "org.springframework.batch.item.ItemProcessor", "process"
    );

    final int maxDepth;
    final int maxChildrenPerNode;
    final int maxNodesPerTree;
    final int maxInvocationsPerMethod;

    private final DomainConfigLoader configLoader;
    private final CollectionResolver collectionResolver;

    // All four caps default to UNLIMITED (0 => Integer.MAX_VALUE, applied in the constructor).
    // The crawler is required to scan every reachable method to full depth: a truncated tree
    // silently under-reports collections and operations, which is worse than a slow one because
    // the result looks complete.
    //
    // Termination does NOT depend on these caps. It is guaranteed by two mechanisms below:
    //   - the callStack back-edge check, which cuts recursion at a repeated nodeId; and
    //   - the memo, which returns an already-built subtree instead of re-expanding it.
    // A cap only ever bounds MEMORY, never guarantees the walk finishes.
    //
    // If a value IS set, it must stay far above any realistic endpoint's node count — a global
    // cap close to real endpoint sizes silently starves unrelated sibling branches (e.g.
    // switch-case branches) of budget consumed earlier in the same tree.
    public CallTreeBuilder(DomainConfigLoader configLoader, CollectionResolver collectionResolver,
                           @Value("${calltree.max-depth:0}") int maxDepth,
                           @Value("${calltree.max-children-per-node:0}") int maxChildrenPerNode,
                           @Value("${calltree.max-nodes-per-tree:0}") int maxNodesPerTree,
                           @Value("${calltree.max-invocations-per-method:0}") int maxInvocationsPerMethod) {
        this.configLoader = configLoader;
        this.collectionResolver = collectionResolver;
        // A value <= 0 means UNLIMITED. That is the default: the crawler is expected to walk
        // every reachable method to full depth rather than truncating, because a truncated call
        // tree silently under-reports collections and operations — a wrong answer that looks
        // like a right one. Termination does not depend on these caps: cycles are cut by the
        // callStack back-edge check and repeated subtrees by the memo, both below.
        this.maxDepth = maxDepth <= 0 ? Integer.MAX_VALUE : maxDepth;
        this.maxChildrenPerNode = maxChildrenPerNode <= 0 ? Integer.MAX_VALUE : maxChildrenPerNode;
        this.maxNodesPerTree = maxNodesPerTree <= 0 ? Integer.MAX_VALUE : maxNodesPerTree;
        this.maxInvocationsPerMethod = maxInvocationsPerMethod <= 0 ? Integer.MAX_VALUE : maxInvocationsPerMethod;
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

    // ========== Targeted (developer) analysis root resolution ==========

    /** A resolved root method for targeted analysis (class + method to root a call tree at). */
    record RootMatch(IndexedClass cls, IndexedMethod method) {}

    /**
     * Resolve targeted-analysis roots by class + method name (+ optional param types).
     * Overload-safe (returns every matching overload) and interface/abstract tolerant
     * (falls back to implementations via interfaceImplMap, then subclasses via childClassMap,
     * finally the declaring interface/abstract type itself). Returns empty when nothing matches.
     */
    List<RootMatch> resolveRoots(String className, String methodName, List<String> paramTypes) {
        List<RootMatch> matches = new ArrayList<>();
        if (ctx == null || className == null || methodName == null) return matches;

        IndexedClass cls = ctx.classMap.get(className);
        if (cls != null && !cls.isInterface && !cls.isAbstract) {
            collectMatchingMethods(cls, methodName, paramTypes, matches);
        }
        if (!matches.isEmpty()) return matches;

        // Interface/abstract root, or class not indexed → resolve to implementations.
        List<String> impls = ctx.interfaceImplMap.get(className);
        if (impls != null) {
            for (String implFqn : impls) {
                IndexedClass impl = ctx.classMap.get(implFqn);
                if (impl != null) collectMatchingMethods(impl, methodName, paramTypes, matches);
            }
        }
        if (matches.isEmpty()) {
            List<String> children = ctx.childClassMap.get(className);
            if (children != null) {
                for (String childFqn : children) {
                    IndexedClass child = ctx.classMap.get(childFqn);
                    if (child != null && !child.isInterface && !child.isAbstract) {
                        collectMatchingMethods(child, methodName, paramTypes, matches);
                    }
                }
            }
        }
        // Last resort: root at the interface/abstract method itself so the tree still renders.
        if (matches.isEmpty() && cls != null) {
            collectMatchingMethods(cls, methodName, paramTypes, matches);
        }
        return matches;
    }

    private void collectMatchingMethods(IndexedClass cls, String methodName,
                                        List<String> paramTypes, List<RootMatch> out) {
        if ("<init>".equals(methodName) || "<clinit>".equals(methodName)) return;
        for (IndexedMethod m : cls.methods.values()) {
            if (!m.name.equals(methodName)) continue;
            if (paramTypes != null && !paramTypes.isEmpty() && !paramTypesMatch(m.paramTypes, paramTypes)) continue;
            out.add(new RootMatch(cls, m));
        }
    }

    /** Match by arity + type name — accepting either fully-qualified or simple type names.
     *  Package-private (not private) so CallGraphService's scopeTarget resolver can reuse the
     *  same tolerance rules instead of duplicating them. */
    boolean paramTypesMatch(List<String> actual, List<String> requested) {
        if (actual.size() != requested.size()) return false;
        for (int i = 0; i < actual.size(); i++) {
            String a = actual.get(i), r = requested.get(i);
            if (a.equals(r)) continue;
            String aSimple = a.substring(a.lastIndexOf('.') + 1);
            String rSimple = r.substring(r.lastIndexOf('.') + 1);
            if (!aSimple.equals(rSimple)) return false;
        }
        return true;
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

        // CALL-017: @Document(indexName=...) → entityIndexMap, the Elasticsearch-specific
        // CONFIRMED-tier ground truth, kept independent of entityCollectionMap above.
        if (ic.documentIndexName != null) {
            ctx.entityIndexMap.put(ic.fqn, ic.documentIndexName);
            ctx.entityIndexMap.put(ic.simpleName, ic.documentIndexName);
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

        // Index per-field static-final-String constant values for EVERY class (not just
        // enums/constants classes) so a cross-class STATIC_FIELD dynamic-arg reference (e.g.
        // builder.collection(SomeOtherClass.TABLE_NAME)) can resolve to the exact field's value.
        if (!ic.fieldConstantsByName.isEmpty()) {
            ctx.fieldConstantsMap.put(ic.fqn, ic.fieldConstantsByName);
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

        // Register Spring Batch step components as synthetic entry points.
        // Unlike @Scheduled/@RabbitListener/@KafkaListener (annotation-driven, detected below),
        // these are pure interface-implementation contracts with NO annotation at all — a class
        // is a batch step component purely by implementing Tasklet/ItemReader/ItemWriter/
        // ItemProcessor. Without an explicit entry point here, such a class is only reachable if
        // some OTHER already-detected endpoint happens to call it directly, which batch step
        // classes typically are not (Spring Batch's JobRepository invokes them reflectively at
        // runtime) — so their entire DB read/write footprint would otherwise never enter any
        // call tree at all.
        if (isMainJar && cls.getInterfaces() != null) {
            for (String iface : cls.getInterfaces()) {
                String syntheticMethod = BATCH_STEP_INTERFACE_METHODS.get(iface);
                if (syntheticMethod == null) continue;
                String ifaceSimple = iface.substring(iface.lastIndexOf('.') + 1);
                for (MethodInfo mi : cls.getMethods()) {
                    if (!syntheticMethod.equals(mi.getName())) continue;
                    boolean isStatic = (mi.getAccessFlags() & 0x0008) != 0;
                    String key = (isStatic ? "S:" : "") + mi.getName() + mi.getDescriptor();
                    IndexedMethod im = ic.methods.get(key);
                    if (im != null) {
                        ctx.endpoints.add(new IndexedEndpoint("BATCH", ifaceSimple + ":" + ic.simpleName, ic, im));
                    }
                }
            }
        }

        // Register async / event-driven entry points as synthetic endpoints.
        // @RabbitListener → AMQP (queue name as path)
        // @KafkaListener → KAFKA (topic name as path)
        // @MessageMapping → WS (WebSocket path)
        // @Scheduled → SCHEDULED (cron/rate expression as path)
        // GraphQL (Spring for GraphQL + Netflix DGS) → QUERY/MUTATION/SUBSCRIPTION (/graphql/<field>)
        // @EventListener / @TransactionalEventListener → EVENT (/event/<eventType>)
        if (isMainJar) {
            for (MethodInfo mi : cls.getMethods()) {
                for (AnnotationInfo ann : mi.getAnnotations()) {
                    String syntheticHttp = null;
                    String syntheticPath = null;
                    String syntheticStereotype = null;
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
                        // GraphQL (Spring for GraphQL + Netflix DGS) — GAP-JAR-033
                        case "QueryMapping", "MutationMapping", "SubscriptionMapping",
                             "SchemaMapping", "DgsQuery", "DgsMutation", "DgsSubscription" -> {
                            String gqlVerb = com.jaranalyzer.util.SpringAnnotations.httpMethodFor(ann.getName());
                            if (gqlVerb != null) {
                                syntheticHttp = gqlVerb;
                                syntheticStereotype = "GraphQL";
                                // field name from "value" or "field" attribute; fall back to method name
                                Object fieldAttr = ann.getAttributes().get("field");
                                if (fieldAttr == null) fieldAttr = ann.getAttributes().get("value");
                                String fieldName = firstStringAttr(fieldAttr);
                                if (fieldName == null || fieldName.isBlank()) fieldName = mi.getName();
                                syntheticPath = "/graphql/" + fieldName;
                            }
                        }
                        // @EventListener / @TransactionalEventListener — GAP-JAR-033
                        case "EventListener", "TransactionalEventListener" -> {
                            syntheticHttp = "EVENT";
                            syntheticStereotype = "EventListener";
                            // Resolve event type from annotation value/classes or first parameter
                            Object val = ann.getAttributes().get("value");
                            if (val == null) val = ann.getAttributes().get("classes");
                            String eventType = firstStringAttr(val);
                            if (eventType == null) {
                                List<ParameterInfo> params = mi.getParameters();
                                if (!params.isEmpty()) eventType = params.get(0).getType();
                            }
                            if (eventType != null) {
                                int lastDot = eventType.lastIndexOf('.');
                                String simpleName = lastDot >= 0 ? eventType.substring(lastDot + 1) : eventType;
                                syntheticPath = "/event/" + simpleName;
                            } else {
                                syntheticPath = "/event/unknown";
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
        return buildCallTree(cls, method, callStack, depth, nodeCount, controllerJar, new HashMap<>(), null, List.of());
    }

    /**
     * @param fragments Sink for children that overflow calltree.max-children-per-node on any
     *                   single node. Never null-drops overflow — every implementation of an
     *                   ambiguous interface call, every switch-style fan-out, etc. is always
     *                   fully built; only the JSON *shape* changes (inline vs. fragment) once a
     *                   node's own children list exceeds the per-node cap. Null disables
     *                   fragmenting (used only by call sites that don't persist output, if any).
     */
    CallNode buildCallTree(IndexedClass cls, IndexedMethod method,
                           Set<String> callStack, int depth, int[] nodeCount,
                           String controllerJar, Map<String, List<CallNode>> fragments) {
        return buildCallTree(cls, method, callStack, depth, nodeCount, controllerJar, new HashMap<>(), fragments, List.of());
    }

    /**
     * CALL-002: build the ancestor-call-chain passed to the next recursion level down — prepend
     * THIS call site (the one about to be recursed into) to the chain already accumulated for the
     * CURRENT frame, capped at {@code MAX_HOP_DEPTH - 1} entries (one fewer than the bound, since
     * the call site being tagged at resolution time itself always counts as the first hop). Bounds
     * memory/list growth for arbitrarily deep real call trees — entries beyond the cap are simply
     * dropped, never consulted, matching this task's "remain unresolved past the bound" contract.
     */
    private List<InvocationRef> extendAncestorChain(List<InvocationRef> ancestorCallChain, InvocationRef call) {
        int cap = CollectionResolver.MAX_HOP_DEPTH - 1;
        if (cap <= 0) return List.of();
        List<InvocationRef> next = new ArrayList<>(Math.min(cap, ancestorCallChain.size() + 1));
        next.add(call);
        for (int i = 0; i < ancestorCallChain.size() && next.size() < cap; i++) {
            next.add(ancestorCallChain.get(i));
        }
        return next;
    }

    /**
     * CALL-002: extends {@code ancestorCallChain} — the call sites (nearest ancestor first) that
     * produced the CURRENT stack frame, i.e. the call that invoked {@code method} itself, then the
     * call that invoked THAT caller, and so on. Threaded down through this existing recursive walk
     * so {@link CollectionResolver#tagCallSiteCollection} can, when a call site's own argument is
     * itself an unresolved parameter pass-through, resolve it by consulting progressively higher
     * ancestors instead of giving up after a single hop. Capped at
     * {@link CollectionResolver#MAX_HOP_DEPTH}{@code - 1} entries (one fewer than the bound, since
     * the call site being tagged itself counts as the first hop) — see
     * {@link #extendAncestorChain(List, InvocationRef)}.
     */
    private CallNode buildCallTree(IndexedClass cls, IndexedMethod method,
                           Set<String> callStack, int depth, int[] nodeCount,
                           String controllerJar, Map<String, CallNode> memo,
                           Map<String, List<CallNode>> fragments, List<InvocationRef> ancestorCallChain) {
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
            // Carry over the overflow marker too — the cached subtree may have had its children
            // list split into a fragment; losing that here would silently re-drop the overflow
            // for every occurrence reached via this memoized path.
            node.setFragmentRef(cached.getFragmentRef());
            node.setTruncatedChildCount(cached.getTruncatedChildCount());
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

        // Invocations cap per method (GAP-JAR-018): treat methods with more invocations than
        // calltree.max-invocations-per-method as leaf nodes to prevent OOM from Lombok/MapStruct/
        // Spring-generated code with pathologically large bodies.
        if (method.invocations.size() > maxInvocationsPerMethod) {
            log.warn("Method {}.{} has {} invocations — exceeding {} cap, treating as leaf",
                     cls.fqn, method.name, method.invocations.size(), maxInvocationsPerMethod);
            callStack.remove(nodeId);
            memo.put(nodeId, node);
            return node;
        }

        Set<String> seenCalls = new HashSet<>();
        int childCount = 0;

        for (InvocationRef call : method.invocations) {
            // Only the rare global safety backstop stops this loop early. The per-node children
            // cap (maxChildrenPerNode) is no longer enforced here — it's applied once, after the
            // full children list is built, by splitting overflow into a fragment (see below).
            // This guarantees every invocation the bytecode actually contains gets a chance to
            // resolve, so no switch-branch/interface-implementation is ever silently dropped.
            if (nodeCount[0] >= maxNodesPerTree) break;
            if ("<init>".equals(call.methodName()) || "<clinit>".equals(call.methodName())) continue;
            // Don't noise-filter lambda synthetic methods — they carry real business logic
            if (!call.methodName().startsWith("lambda$") && isNoiseMethod(call.methodName())) continue;

            // Real reflective invocation — checked BEFORE the JDK-class recursion cutoff below,
            // since java.lang.reflect.* calls never become tree children (isStandardLibraryClass
            // stops recursion into them) and would otherwise be invisible to this analysis
            // entirely, not just unresolved.
            String reflectiveCall = classifyReflectiveCall(call.ownerClass(), call.methodName());
            if (reflectiveCall != null) node.getReflectiveCalls().add(reflectiveCall);

            boolean isStaticCall = call.opcode() == 184; // Opcodes.INVOKESTATIC
            String staticPrefix = isStaticCall ? "S:" : "";
            String callKey = call.ownerClass() + "#" + staticPrefix + call.methodName() + call.descriptor();
            if (seenCalls.contains(callKey)) continue;
            seenCalls.add(callKey);

            // Spring Event dispatch: publishEvent() → @EventListener methods
            int eventChildren = connectEventListeners(call, method, node, callStack,
                    depth, nodeCount, controllerJar, memo, fragments);
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
            // CALL-005: a plain (non-invokedynamic) interface call whose receiver was loaded from
            // a field that was itself assigned a captured method reference directly (mirrors the
            // injectedFields/receiverFieldName DI-resolution pattern above, just for a functional-
            // interface-typed field instead of a DI bean field) — redirect it the same way a
            // genuine isLambdaInvocation() call site is redirected below.
            CallGraphIndex.FieldLambdaInfo fieldLambda = (!call.isLambdaInvocation() && call.receiverFieldName() != null)
                    ? cls.fieldLambdaTargets.get(call.receiverFieldName())
                    : null;

            if ((call.isLambdaInvocation() && call.lambdaTargetClass() != null) || fieldLambda != null) {
                String lambdaTargetCls = fieldLambda != null ? fieldLambda.targetClass() : call.lambdaTargetClass();
                String lambdaTargetMtd = fieldLambda != null ? fieldLambda.targetMethod() : call.lambdaTargetMethod();
                String lambdaTargetDesc = fieldLambda != null ? fieldLambda.targetDescriptor() : call.lambdaTargetDescriptor();

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
                        // If the lambda/method-ref target is an interface or abstract class, recursing
                        // directly into it dead-ends at the abstract declaration (no body, no
                        // invocations). Route through the same interface-dispatch machinery used for
                        // normal calls so it reaches the concrete implementation(s) instead.
                        if (lambdaCls.isInterface || lambdaCls.isAbstract) {
                            List<ResolvedRef> resolved = resolveAllMethods(lambdaTargetCls,
                                    lambdaTargetMtd, lambdaTargetDesc, call, cls);
                            if (!resolved.isEmpty()) {
                                for (ResolvedRef ref : resolved) {
                                    if (nodeCount[0] >= maxNodesPerTree) break;
                                    CallNode child = buildCallTree(ref.cls, ref.method, callStack,
                                            depth + 1, nodeCount, controllerJar, memo, fragments,
                                            extendAncestorChain(ancestorCallChain, call));
                                    if (child != null) {
                                        child.setDispatchType(isSyntheticLambda ? "LAMBDA" : "METHOD_REFERENCE");
                                        child.setResolvedFrom(ref.resolvedFrom());
                                        child.setQualifierHint(ref.qualifierHint());
                                        node.getChildren().add(child);
                                        childCount++;
                                    }
                                }
                                continue; // resolved via interface dispatch — skip normal resolution
                            }
                            // No implementations found — genuine dead end, fall through to direct
                            // recursion below so the interface declaration is still shown.
                        }
                        CallNode child = buildCallTree(lambdaCls, lambdaMethod, callStack,
                                depth + 1, nodeCount, controllerJar, memo, fragments,
                                extendAncestorChain(ancestorCallChain, call));
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
                                if (nodeCount[0] >= maxNodesPerTree) break;
                                CallNode child = buildCallTree(ref.cls, ref.method, callStack,
                                        depth + 1, nodeCount, controllerJar, memo, fragments,
                                        extendAncestorChain(ancestorCallChain, call));
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
            // isExcludedByConfig is checked here, at the point the walk decides whether to
            // RECURSE into a target — not at render time. An excluded class still appears as a
            // leaf node (so the call is not hidden), but its own callees are not expanded. That is
            // the distinction that matters: the edge into generated code stays visible, while the
            // combinatorial walk through the grammar beneath it does not happen.
            boolean isAppClass = !isStandardLibraryClass(targetClass)
                    && !isExcludedByConfig(targetClass)
                    && (ctx.classMap.containsKey(targetClass)
                        || ctx.interfaceImplMap.containsKey(targetClass)
                        || isApplicationClass(targetClass));

            if (!isAppClass) {
                String thirdParty = classifyThirdPartyDependency(targetClass);
                if (thirdParty != null) node.getThirdPartyDependencies().add(thirdParty);
            }

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
                        if (nodeCount[0] >= maxNodesPerTree) break;
                        // FeignClient / HTTP_CLIENT interface: create HTTP_CALL leaf instead of recursing.
                        // The interface has no method bodies — its proxy makes real HTTP calls at runtime.
                        if ("HTTP_CLIENT".equals(ref.cls().stereotype)) {
                            CallNode httpNode = createFeignCallLeaf(ref.cls(), call, method);
                            node.getChildren().add(httpNode);
                            childCount++;
                            nodeCount[0]++;
                            continue;
                        }
                        CallNode child = buildCallTree(ref.cls, ref.method, callStack, depth + 1, nodeCount, controllerJar, memo, fragments,
                                extendAncestorChain(ancestorCallChain, call));
                        if (child != null) {
                            // Preserve RECURSIVE dispatchType set by the back-edge detection above.
                            if (!child.isRecursive()) child.setDispatchType(ref.dispatchType());
                            child.setResolvedFrom(ref.resolvedFrom());
                            child.setQualifierHint(ref.qualifierHint());
                            // Tag the collection resolved at THIS call site (e.g. BulkWriteCollector.
                            // addInsert("WIP_X", doc)) directly onto the child — that method's own body
                            // never sees the collection name (only receives it as a parameter), so
                            // collectCollections inside buildCallTree found nothing for it. Returns a
                            // COPY (never null unless this isn't a BulkWriteCollector call) — child is
                            // the first-build node stored verbatim in `memo`, so mutating it in place
                            // would corrupt every other call site sharing that memo entry. CALL-002:
                            // ancestorCallChain lets this walk one hop further up (bounded) when the
                            // call site's own argument is itself an unresolved parameter pass-through.
                            CallNode tagged = collectionResolver.tagCallSiteCollection(child, call, ancestorCallChain, ctx);
                            if (tagged != null) child = tagged;
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
                        CallNode derived = createDerivedQueryLeaf(targetCls, call, controllerJar);
                        if (derived != null) {
                            node.getChildren().add(derived);
                            childCount++;
                            nodeCount[0]++;
                        }
                    }
                }
            } else if (configLoader.isHttpClientCall(targetClass)) {
                // HTTP client call (RestTemplate, WebClient, FeignClient, etc.)
                CallNode httpNode = createHttpCallLeaf(call, method, ancestorCallChain);
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

        // Enforce the per-node children cap AFTER the complete list is built, by splitting
        // overflow into a fragment rather than dropping it. Every switch-branch / ambiguous
        // interface implementation / event listener that was actually resolved above survives —
        // it just moves out of the inline tree once a node has more children than
        // maxChildrenPerNode. computeAggregates()/EndpointOutputWriter read fragments too, so
        // stats and persisted output are never missing this data.
        if (fragments != null && node.getChildren().size() > maxChildrenPerNode) {
            List<CallNode> all = node.getChildren();
            List<CallNode> inline = new ArrayList<>(all.subList(0, maxChildrenPerNode));
            List<CallNode> overflow = new ArrayList<>(all.subList(maxChildrenPerNode, all.size()));
            String fragmentId = "frag_" + String.format("%06d", fragments.size() + 1);
            fragments.put(fragmentId, overflow);
            node.setChildren(inline);
            node.setFragmentRef(fragmentId);
            node.setTruncatedChildCount(overflow.size());
        }

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
        cleanOperationTypes(node, null);
    }

    /**
     * @param fragments Overflow children keyed by fragmentRef (see buildCallTree). Passed so a
     *                   node whose full children list was split into a fragment still gets a
     *                   correctly-derived operationType — otherwise the bottom-up derivation would
     *                   silently ignore the fragment's contribution.
     */
    void cleanOperationTypes(CallNode node, Map<String, List<CallNode>> fragments) {
        if (node == null) return;
        Set<CallNode> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        cleanOperationTypesRecursive(node, visited, fragments);
    }

    private void cleanOperationTypesRecursive(CallNode node, Set<CallNode> visited,
                                               Map<String, List<CallNode>> fragments) {
        if (node == null || !visited.add(node)) return;

        // Process children first (bottom-up)
        if (node.getChildren() != null) {
            for (CallNode child : node.getChildren()) {
                cleanOperationTypesRecursive(child, visited, fragments);
            }
        }
        List<CallNode> overflow = fragmentChildren(node, fragments);
        if (overflow != null) {
            for (CallNode child : overflow) {
                cleanOperationTypesRecursive(child, visited, fragments);
            }
        }

        boolean hasCollections = node.getCollectionsAccessed() != null
                && !node.getCollectionsAccessed().isEmpty();
        boolean isDbStereotype = "REPOSITORY".equals(node.getStereotype())
                || "SPRING_DATA".equals(node.getStereotype());
        // HTTP_CALL leaves (RestTemplate/WebClient/Feign) get their operationType from the
        // resolved HTTP verb (createHttpCallLeaf/createFeignCallLeaf), not from Mongo/JPA
        // detection — they never have collectionsAccessed or a DB stereotype by design, so
        // without this exemption every one of them is wrongly treated as a false positive
        // below and has its correctly-inferred GET/POST/PUT/DELETE operationType nulled out.
        boolean isHttpCall = "HTTP_CALL".equals(node.getCallType());
        boolean isLeaf = (node.getChildren() == null || node.getChildren().isEmpty())
                && (overflow == null || overflow.isEmpty());

        // hasDbInteraction: method calls MongoTemplate/MongoOperations but collection
        // couldn't be resolved. The operation type (e.g., WRITE from save()) is still valid.
        if (!hasCollections && !isDbStereotype && !isHttpCall && !node.isHasDbInteraction()) {
            if (isLeaf) {
                // Leaf with no collections and not a DB node → false positive
                node.setOperationType(null);
                node.getOperationTypes().clear();
            } else {
                // Non-leaf with no DB evidence of its own: the operationType createEnrichedNode
                // guessed from the method-name prefix heuristic is untrustworthy (that's why we're
                // here), so REPLACE rather than merge into operationTypes. Merging (addAll without
                // clearing) would leave that stale false-positive tag sitting in the set — and once
                // this node's own parent later calls deriveAllOperationsFromChildren() on ITS
                // children, it reads this node's operationTypes and inherits the stale tag even
                // though this node's own (correct) derivation below produced something different.
                Set<String> childOps = deriveAllOperationsFromChildren(node, overflow);
                node.getOperationTypes().clear();
                node.getOperationTypes().addAll(childOps);
                // Keep highest priority as the primary operationType
                String best = null;
                for (String op : childOps) {
                    if (isHigherPriority(op, best)) best = op;
                }
                node.setOperationType(best);
            }
        } else if (!isLeaf) {
            // Non-leaf WITH its own DB evidence (hasCollections/isDbStereotype/isHttpCall/
            // hasDbInteraction true): the node's own operationType/primary op stays as-is (it's
            // a real signal, not a guess to discard) — but its operationTypes SET still needs
            // children's operations UNIONED in as additional entries, not just its own. Without
            // this, an orchestrator that makes one trivial direct call (e.g. a READ via
            // getCollection(x)) permanently freezes its own operationTypes at that one call even
            // though children do far more consequential WRITE/UPDATE/DELETE work — under-
            // reporting what the node's subtree actually does. Union only; never replace the
            // primary operationType, since that IS the node's own reliable direct signal.
            Set<String> childOps = deriveAllOperationsFromChildren(node, overflow);
            node.getOperationTypes().addAll(childOps);
        }
        // Ensure primary op is always in the set
        if (node.getOperationType() != null) {
            node.getOperationTypes().add(node.getOperationType());
        }
    }

    /** Look up a node's overflow children in the fragments map, if any. */
    private List<CallNode> fragmentChildren(CallNode node, Map<String, List<CallNode>> fragments) {
        if (fragments == null || node.getFragmentRef() == null) return null;
        return fragments.get(node.getFragmentRef());
    }

    /**
     * Derive a node's operation type from its children (inline + fragment overflow, if any).
     * Priority: WRITE > UPDATE > DELETE > READ > AGGREGATE > COUNT > null
     */
    private Set<String> deriveAllOperationsFromChildren(CallNode node, List<CallNode> overflow) {
        Set<String> ops = new LinkedHashSet<>();
        if (node.getChildren() != null) {
            for (CallNode child : node.getChildren()) {
                if (child.getOperationType() != null) ops.add(child.getOperationType());
                ops.addAll(child.getOperationTypes());
            }
        }
        if (overflow != null) {
            for (CallNode child : overflow) {
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
        node.setJarName(cls.jarName);
        node.setSourcePath(cls.sourcePath);
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

        // Resolve Oracle stored-procedure reference from bytecode-captured SimpleJdbcCall chains.
        // simpleJdbcCallProcedures: field → "PROC:PKG.NAME" or "FUNC:PKG.NAME"
        // storedProcedureName: set when class extends StoredProcedure directly.
        if (cls.storedProcedureName != null && !cls.storedProcedureName.isBlank()) {
            node.setOracleProcRef("PROC:" + cls.storedProcedureName);
        } else if (!cls.simpleJdbcCallProcedures.isEmpty()) {
            // Single proc class → take the first (and usually only) entry.
            // Multi-proc classes expose all values joined — consumed by CrossRefService for mapping.
            String combined = String.join("|", cls.simpleJdbcCallProcedures.values());
            node.setOracleProcRef(combined);
        }

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
    /**
     * Extra class-name prefixes to exclude from the walk, from
     * {@code calltree.exclude-packages} (comma-separated).
     *
     * <p>Exists for <b>generated code inside the application's own packages</b>, which no base-package
     * bound can express. A generated ANTLR parser is the case that motivated it: on
     * {@code unified-web.jar}, 1,219 of 1,623 analysed classes were {@code com.plsql.parser.*}
     * grammar output, one class had 47,585 invocations, and walking it uncapped produced a 69 GB
     * {@code analysis.json}. It is business-logic-free, so the entire cost buys nothing.
     *
     * <p>Empty by default: excluding classes silently is exactly the "wrong answer that looks
     * right" this crawler is built to avoid, so it must be an explicit deployment choice. The count
     * of excluded classes is logged at INFO on every run.
     */
    private volatile List<String> excludedPrefixes = List.of();

    @Value("${calltree.exclude-packages:}")
    void setExcludePackages(String csv) {
        this.excludedPrefixes = (csv == null || csv.isBlank())
                ? List.of()
                : java.util.Arrays.stream(csv.split(","))
                        .map(String::trim).filter(x -> !x.isEmpty()).toList();
        if (!excludedPrefixes.isEmpty()) {
            log.info("Call-tree walk excluding {} configured prefix(es): {}",
                    excludedPrefixes.size(), excludedPrefixes);
        }
    }

    /** True when the class is excluded by configuration. */
    private boolean isExcludedByConfig(String className) {
        if (className == null || excludedPrefixes.isEmpty()) return false;
        for (String p : excludedPrefixes) {
            if (className.startsWith(p)) return true;
        }
        return false;
    }

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
            || className.startsWith("org.objectweb.asm.")
            // Parser generator runtimes. A generated parser's rule methods are mutually recursive
            // with very high fan-out, so walking them produces enormous numbers of distinct paths
            // and no business insight. Measured on unified-web.jar: com.plsql.parser.PlSqlParser
            // alone has 1,245 methods and 47,585 invocations, and walking the grammar uncapped
            // took analysis.json to 69 GB.
            || className.startsWith("org.antlr.") || className.startsWith("antlr.");
    }

    private static boolean isRepoStereotype(String stereotype) {
        return "REPOSITORY".equals(stereotype) || "SPRING_DATA".equals(stereotype);
    }

    /**
     * Create a synthetic leaf node for a Spring Data derived query method
     * (findByX, deleteByX, countByX, existsByX) that doesn't exist in bytecode.
     */
    private CallNode createDerivedQueryLeaf(IndexedClass repoClass, InvocationRef call, String controllerJar) {
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

        // The method itself is inherited from a Spring Data framework interface (MongoRepository/
        // CrudRepository — never indexed, no sourceJar of its own), but the CALL is genuinely made
        // through repoClass, which IS an app-declared interface living in a real jar. Attribute the
        // node to repoClass's own jar/module/domain — same fields/lookups createEnrichedNode already
        // uses for the "method found in bytecode" path (e.g. this repo's own derived-query methods).
        node.setSourceJar(repoClass.sourceJar);
        node.setJarName(repoClass.jarName);
        node.setSourcePath(repoClass.sourcePath);
        String artifactId = ctx.jarArtifactMap.getOrDefault(repoClass.sourceJar, null);
        node.setModule(configLoader.jarToProject(repoClass.sourceJar, artifactId));
        node.setDomain(configLoader.jarToDomain(repoClass.sourceJar));
        node.setCrossModule(!Objects.equals(repoClass.sourceJar, controllerJar));

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
        node.setHttpTargetModuleHint(feignClass.feignClientHint);
        // Try to pull HTTP method/path from the indexed interface method — this is the reliable
        // source of truth for Feign (declarative client: each method's mapping annotation IS the
        // target endpoint's contract), unlike RestTemplate/WebClient where the method invoked on
        // the client is generic and the target is only known from string arguments.
        String methodKey = call.methodName() + call.descriptor();
        IndexedMethod im = feignClass.methods.get(methodKey);
        if (im == null) im = feignClass.methods.get("S:" + methodKey);
        if (im != null && im.httpMethod != null) {
            node.setAnnotations(List.of("@" + im.httpMethod));
            node.setHttpTargetMethod(im.httpMethod);
            if (im.path != null && !im.path.isBlank()) {
                node.setStringLiterals(List.of(im.path));
                node.setHttpTargetPath(im.path);
            }
        }
        node.setOperationType(node.getHttpTargetMethod() != null
                ? inferHttpOperationFromMethod(node.getHttpTargetMethod())
                : inferHttpOperation(call.methodName()));
        return node;
    }

    /**
     * RestTemplate methods and WebClient fluent-builder entry points that unambiguously imply an
     * HTTP method — the reliable source of truth for cross-JAR matching (see
     * ExternalApiLinkService), unlike guessing from the CALLING Java method's name.
     * exchange()/execute()/method() are intentionally absent: the HTTP method there is a runtime
     * argument (HttpMethod enum), not resolvable from the method name alone — left null rather
     * than guessed.
     */
    private static final Map<String, String> HTTP_CLIENT_METHOD_TO_VERB = Map.ofEntries(
            Map.entry("getForObject", "GET"), Map.entry("getForEntity", "GET"),
            Map.entry("headForHeaders", "HEAD"), Map.entry("optionsForAllow", "OPTIONS"),
            Map.entry("postForObject", "POST"), Map.entry("postForEntity", "POST"),
            Map.entry("postForLocation", "POST"),
            Map.entry("put", "PUT"), Map.entry("delete", "DELETE"),
            Map.entry("patchForObject", "PATCH"),
            // WebClient fluent-builder entry points
            Map.entry("get", "GET"), Map.entry("post", "POST"), Map.entry("head", "HEAD"),
            Map.entry("patch", "PATCH"), Map.entry("options", "OPTIONS")
    );

    /** Reflection/legacy-call convenience overload — no ancestor chain available, so this only
     *  ever resolves the same-call-site-literal / caller-method-literal-scan tiers (pre-CALL-010
     *  behavior). Kept so existing test call sites built before CALL-010 keep compiling/passing. */
    private CallNode createHttpCallLeaf(InvocationRef call, IndexedMethod callerMethod) {
        return createHttpCallLeaf(call, callerMethod, null);
    }

    /**
     * CALL-010: brings HTTP/external-call target-path resolution up to the same cross-method
     * value-tracing capability level CALL-001..009 already built for MongoDB collections / JDBC
     * table names — single-hop caller-arg resolution, bounded multi-hop (reusing
     * {@link CollectionResolver#MAX_HOP_DEPTH}), and safe `literal + param`/`param + literal`
     * string-concat folding. See {@link #resolveHttpPathAcrossHops} for the walk itself.
     */
    private CallNode createHttpCallLeaf(InvocationRef call, IndexedMethod callerMethod,
                                         List<InvocationRef> ancestorCallChain) {
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

        String resolvedVerb = HTTP_CLIENT_METHOD_TO_VERB.get(call.methodName());
        node.setHttpTargetMethod(resolvedVerb);
        node.setOperationType(resolvedVerb != null
                ? inferHttpOperationFromMethod(resolvedVerb)
                : inferHttpOperation(call.methodName()));

        // Bind the path/URL to THIS specific call site's arguments (call.stringArgs()), not any
        // string literal found anywhere in the calling method — the previous approach could pick
        // up an unrelated URL from earlier in the method and made method+path unreliable as a
        // cross-JAR match key.
        String targetPath = firstUrlLikeArg(call.stringArgs());
        String pathSource = targetPath != null ? "DIRECT_LITERAL" : null;
        if (targetPath == null) {
            // CALL-010: the direct call-site literal is missing — this call's own dynamic-arg
            // capture (from BytecodeClassParser, domain-agnostic, same infra CALL-001..009 already
            // reuse) may show the URL/path argument is an unresolved PARAM pass-through (or a
            // `literal + param`/`param + literal` concat) instead of a plain literal. Walk the
            // caller chain, bounded, exactly like Mongo/JDBC's own single-hop/multi-hop mechanism.
            HttpPathResolution resolved = resolveHttpPathAcrossHops(call, ancestorCallChain);
            if (resolved != null) {
                targetPath = resolved.path();
                pathSource = resolved.source();
            }
        }
        if (targetPath == null) {
            // Fallback for chained/dynamic call sites where the URL isn't a direct argument to
            // THIS invocation (e.g. built via a variable) — best-effort scan of the calling
            // method's literals, same as the old behavior, kept only as a last resort.
            if (callerMethod.stringLiterals != null) {
                targetPath = firstUrlLikeArg(callerMethod.stringLiterals);
                if (targetPath != null) pathSource = "CALLER_METHOD_LITERAL_SCAN";
            }
        }
        if (targetPath != null) {
            node.setStringLiterals(List.of(targetPath));
            node.setHttpTargetPath(extractPathFromUrl(targetPath));
            node.setHttpTargetModuleHint(extractHost(targetPath));
            node.setHttpTargetPathSource(pathSource);
        }
        return node;
    }

    /** CALL-010: pairs a resolved URL/path string with which mechanism resolved it, mirroring
     *  CollectionResolver's ClassResolution/CallSiteCollection provenance-pairing pattern. */
    private record HttpPathResolution(String path, String source) {}

    /** First captured String-typed dynamic (variable/field-sourced) argument at this call site, if
     *  any — same "first match wins" simplification {@code MongoMethodDetector.
     *  resolveGenericCallSiteArg}/{@code JpaMethodDetector.resolveGenericSqlCallSiteArg} already
     *  use; safe here because {@code BytecodeClassParser} only ever records a dynamic-arg entry
     *  for String-typed locals/params/fields, never a Class/enum/response-type argument. */
    private static MethodCallInfo.DynamicArgSource firstDynamicArg(List<MethodCallInfo.DynamicArgSource> args) {
        return (args == null || args.isEmpty()) ? null : args.get(0);
    }

    private static boolean isUnresolvedHttpParamPassThrough(MethodCallInfo.DynamicArgSource dyn) {
        return dyn != null && "PARAM".equals(dyn.kind());
    }

    private static boolean isHttpConcatShape(MethodCallInfo.DynamicArgSource dyn) {
        return dyn != null
                && ("CONCAT_LITERAL_PARAM".equals(dyn.kind()) || "CONCAT_PARAM_LITERAL".equals(dyn.kind())
                    || "CONCAT_PARAM_PARAM".equals(dyn.kind()));
    }

    /**
     * CALL-010: JDBC/Mongo-parallel bounded multi-hop walk for the HTTP target-path case. Hop 1 is
     * the current call site's own dynamic-arg capture; each further hop consults the next entry in
     * {@code ancestorCallChain} (nearest ancestor first — same threading CallTreeBuilder already
     * does for {@code extendAncestorChain}), up to {@link CollectionResolver#MAX_HOP_DEPTH} call
     * sites total. Stops as soon as a literal/URL-like value is found, a non-PARAM/null result is
     * hit, or the bound/chain is exhausted. Folds a single-operand `literal + param`/
     * `param + literal` concatenation once the PARAM side resolves to a literal one hop further up
     * — the narrow, safe shape {@code CollectionResolver.foldConcatIfPossible}/
     * {@code foldSqlConcatIfPossible} already use; the ambiguous `param + param` shape is
     * deliberately out of scope here too, matching CALL-009's own residual-gap decision.
     */
    private HttpPathResolution resolveHttpPathAcrossHops(InvocationRef call, List<InvocationRef> ancestorCallChain) {
        MethodCallInfo.DynamicArgSource dyn = firstDynamicArg(call.dynamicArgSources());
        if (dyn == null) return null;

        if (isHttpConcatShape(dyn)) {
            return foldHttpConcatIfPossible(dyn, ancestorCallChain);
        }
        if (!isUnresolvedHttpParamPassThrough(dyn)) return null;

        int hop = 1, idx = 0;
        while (hop < CollectionResolver.MAX_HOP_DEPTH && ancestorCallChain != null && idx < ancestorCallChain.size()) {
            InvocationRef ancestor = ancestorCallChain.get(idx);
            idx++;
            hop++;
            String lit = firstUrlLikeArg(ancestor.stringArgs());
            if (lit != null) return new HttpPathResolution(lit, "CALL_SITE_PARAM_PROPAGATION");
            MethodCallInfo.DynamicArgSource ancestorDyn = firstDynamicArg(ancestor.dynamicArgSources());
            if (isHttpConcatShape(ancestorDyn)) {
                // The pass-through itself terminates in a concat one hop up — fold it the same way.
                HttpPathResolution folded = foldHttpConcatIfPossible(ancestorDyn,
                        ancestorCallChain.subList(idx, ancestorCallChain.size()));
                if (folded != null) return folded;
                return null;
            }
            if (!isUnresolvedHttpParamPassThrough(ancestorDyn)) return null; // non-PARAM/null — stop
            // still an unresolved pass-through — keep walking
        }
        return null; // bound reached / ancestor chain exhausted — never fabricated
    }

    /**
     * CALL-010: JDBC/Mongo-parallel single-operand concat fold — only ever trusts an ancestor call
     * site when it is unambiguous (exactly one literal argument and no dynamic ones), never
     * guesses between siblings. Mirrors {@code CollectionResolver.foldConcatIfPossible}'s exact
     * safety discipline.
     */
    private HttpPathResolution foldHttpConcatIfPossible(MethodCallInfo.DynamicArgSource dyn,
                                                          List<InvocationRef> ancestorCallChain) {
        if ("CONCAT_PARAM_PARAM".equals(dyn.kind())) {
            return foldTwoParamHttpConcatIfPossible(dyn, ancestorCallChain);
        }
        boolean literalFirst = "CONCAT_LITERAL_PARAM".equals(dyn.kind());
        String literalPart = dyn.ownerClass(); // repurposed field — see DynamicArgSource javadoc

        int hop = 1, idx = 0;
        while (hop < CollectionResolver.MAX_HOP_DEPTH && ancestorCallChain != null && idx < ancestorCallChain.size()) {
            InvocationRef ancestor = ancestorCallChain.get(idx);
            idx++;
            hop++;
            List<String> literalArgs = ancestor.stringArgs();
            List<MethodCallInfo.DynamicArgSource> dynArgs = ancestor.dynamicArgSources();
            boolean hasOneLiteral = literalArgs != null && !literalArgs.isEmpty()
                    && literalArgs.stream().distinct().count() == 1;
            boolean hasNoDynamic = dynArgs == null || dynArgs.isEmpty();
            boolean hasOneDynamic = dynArgs != null && dynArgs.size() == 1;
            boolean hasNoLiteral = literalArgs == null || literalArgs.isEmpty();

            if (hasOneLiteral && hasNoDynamic) {
                String paramValue = literalArgs.get(0);
                String finalPath = literalFirst ? literalPart + paramValue : paramValue + literalPart;
                return new HttpPathResolution(finalPath, "CALL_SITE_CONCAT_PROPAGATION");
            }
            if (hasOneDynamic && hasNoLiteral && "PARAM".equals(dynArgs.get(0).kind())) {
                continue; // unambiguous single pass-through — keep walking
            }
            return null; // ambiguous or resolved-but-non-literal — stay unresolved, never guess
        }
        return null;
    }

    /**
     * CALL-013 (§216's own documented residual, closed): HTTP-parallel of
     * {@code CollectionResolver.foldTwoParamConcatIfPossible} — folds the `param + param`
     * URL/path-concatenation shape (neither operand a same-method literal), chasing both operand
     * names independently up {@code ancestorCallChain}. Same by-name-only disambiguation
     * discipline and partial-fold honesty contract as the Mongo/JDBC originals.
     */
    private HttpPathResolution foldTwoParamHttpConcatIfPossible(MethodCallInfo.DynamicArgSource dyn,
                                                                  List<InvocationRef> ancestorCallChain) {
        String name1 = dyn.name();
        String name2 = dyn.ownerClass(); // repurposed field — see DynamicArgSource javadoc
        String resolved1 = null, resolved2 = null;

        int hop = 1, idx = 0;
        while ((resolved1 == null || resolved2 == null) && hop < CollectionResolver.MAX_HOP_DEPTH
                && ancestorCallChain != null && idx < ancestorCallChain.size()) {
            InvocationRef ancestor = ancestorCallChain.get(idx);
            idx++;
            hop++;
            List<String> literalArgs = ancestor.stringArgs();
            List<MethodCallInfo.DynamicArgSource> dynArgs = ancestor.dynamicArgSources();

            Set<String> paramNames = new LinkedHashSet<>();
            if (dynArgs != null) {
                for (MethodCallInfo.DynamicArgSource d : dynArgs) {
                    if (!"PARAM".equals(d.kind())) return null;
                    boolean matchesOperand = (resolved1 == null && name1.equals(d.name()))
                            || (resolved2 == null && name2.equals(d.name()));
                    if (!matchesOperand) return null;
                    paramNames.add(d.name());
                }
            }
            if (paramNames.size() > 2) return null;

            boolean n1StillForwarded = resolved1 == null && paramNames.contains(name1);
            boolean n2StillForwarded = resolved2 == null && paramNames.contains(name2);

            List<String> distinctLiterals = literalArgs == null ? List.of()
                    : literalArgs.stream().distinct().toList();
            long distinctLiteralCount = distinctLiterals.size();

            if (resolved1 == null && resolved2 == null && paramNames.isEmpty() && distinctLiteralCount == 2) {
                resolved1 = distinctLiterals.get(0);
                resolved2 = distinctLiterals.get(1);
                continue;
            }

            if (n1StillForwarded && n2StillForwarded) {
                if (distinctLiteralCount != 0) return null;
                continue;
            }
            if (n1StillForwarded) {
                if (resolved2 != null || distinctLiteralCount != 1) return null;
                resolved2 = literalArgs.get(0);
                continue;
            }
            if (n2StillForwarded) {
                if (resolved1 != null || distinctLiteralCount != 1) return null;
                resolved1 = literalArgs.get(0);
                continue;
            }
            return null;
        }

        if (resolved1 == null && resolved2 == null) return null;
        if (resolved1 != null && resolved2 != null) {
            return new HttpPathResolution(resolved1 + resolved2, "CALL_SITE_CONCAT_PROPAGATION");
        }
        String partial = resolved1 != null
                ? resolved1 + "{" + name2 + "}"
                : "{" + name1 + "}" + resolved2;
        return new HttpPathResolution(partial, "CALL_SITE_CONCAT_PROPAGATION");
    }

    private static String firstUrlLikeArg(List<String> args) {
        if (args == null) return null;
        for (String s : args) {
            if (s != null && (s.startsWith("http://") || s.startsWith("https://") || s.startsWith("/"))) {
                return s;
            }
        }
        return null;
    }

    /** Strip scheme+host from a full URL, leaving just the path — the part that matters for
     *  cross-JAR endpoint matching (host varies by environment/profile). Leaves bare paths as-is. */
    private static String extractPathFromUrl(String urlOrPath) {
        if (urlOrPath.startsWith("/")) return urlOrPath;
        int schemeEnd = urlOrPath.indexOf("://");
        if (schemeEnd < 0) return urlOrPath;
        int pathStart = urlOrPath.indexOf('/', schemeEnd + 3);
        return pathStart >= 0 ? urlOrPath.substring(pathStart) : "/";
    }

    /** Extract the host from a full URL for use as a module-hint tie-breaker. Null for bare paths
     *  (no host present — nothing to extract). */
    private static String extractHost(String urlOrPath) {
        if (urlOrPath.startsWith("/")) return null;
        int schemeEnd = urlOrPath.indexOf("://");
        if (schemeEnd < 0) return null;
        int hostStart = schemeEnd + 3;
        int hostEnd = urlOrPath.indexOf('/', hostStart);
        String host = hostEnd >= 0 ? urlOrPath.substring(hostStart, hostEnd) : urlOrPath.substring(hostStart);
        int colon = host.indexOf(':');
        return colon >= 0 ? host.substring(0, colon) : host;
    }

    private String inferHttpOperation(String methodName) {
        if (methodName == null) return "READ";
        String n = methodName.toLowerCase();
        if (n.contains("post") || n.contains("put") || n.contains("save") || n.contains("create")) return "WRITE";
        if (n.contains("delete") || n.contains("remove")) return "DELETE";
        if (n.contains("patch") || n.contains("update")) return "UPDATE";
        return "READ";
    }

    /** Derive READ/WRITE/UPDATE/DELETE from a resolved HTTP verb — used once the actual method
     *  invoked is known, replacing the name-guessing inferHttpOperation() as the source of truth. */
    private String inferHttpOperationFromMethod(String httpMethod) {
        return switch (httpMethod) {
            case "POST" -> "WRITE";
            case "PUT", "PATCH" -> "UPDATE";
            case "DELETE" -> "DELETE";
            default -> "READ";
        };
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

    private static final Set<String> REFLECTIVE_OWNER_CLASSES = Set.of(
            "java.lang.Class", "java.lang.reflect.Method",
            "java.lang.reflect.Constructor", "java.lang.reflect.Field");
    private static final Set<String> REFLECTIVE_METHOD_NAMES = Set.of(
            "forName", "invoke", "newInstance", "getMethod", "getDeclaredMethod",
            "getConstructor", "getDeclaredConstructor", "get", "set");

    /**
     * Returns a human-readable "Owner.method" tag (e.g. "java.lang.reflect.Method.invoke") when
     * this invocation is a genuine reflective call — the real owner class AND method name both
     * match a known reflection API — or null otherwise. Deliberately based on the actual resolved
     * invocation target (ASM-derived, from real bytecode), not a string-literal substring scan —
     * see CallNode.reflectiveCalls's doc comment for why the previous approach was both a
     * false-positive and false-negative risk.
     */
    private static final Set<String> JDK_CORE_PREFIXES = Set.of(
            "java.", "javax.", "jakarta.", "sun.", "jdk.", "com.sun.");

    /**
     * Returns the class name if it's a genuine third-party library dependency (audit #10) — i.e.
     * excluded from tree recursion (by {@link #isStandardLibraryClass}/{@link
     * #isExcludedByConfig}) but NOT a JDK-core class. JDK classes are deliberately excluded from
     * this facet: "External Dependency" should mean "a real third-party library," not "any class
     * outside this codebase" (which would include e.g. java.util.List, noise with no real value).
     */
    private String classifyThirdPartyDependency(String className) {
        if (className == null) return null;
        for (String prefix : JDK_CORE_PREFIXES) {
            if (className.startsWith(prefix)) return null;
        }
        return className;
    }

    private String classifyReflectiveCall(String ownerClass, String methodName) {
        if (ownerClass == null || methodName == null) return null;
        if (!REFLECTIVE_OWNER_CLASSES.contains(ownerClass)) return null;
        if (!REFLECTIVE_METHOD_NAMES.contains(methodName)) return null;
        return ownerClass + "." + methodName;
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
                                       Map<String, CallNode> memo, Map<String, List<CallNode>> fragments) {
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
            if (nodeCount[0] >= maxNodesPerTree) break;
            // Event dispatch is not argument pass-through (the listener receives the event object,
            // not the publisher's own parameters) — start a fresh (empty) ancestor chain rather
            // than threading the publisher's, which would be a semantically unrelated call shape.
            CallNode child = buildCallTree(ref.cls(), ref.method(), callStack, depth + 1,
                    nodeCount, controllerJar, memo, fragments, List.of());
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
