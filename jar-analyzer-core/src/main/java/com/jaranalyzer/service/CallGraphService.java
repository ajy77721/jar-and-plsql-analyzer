package com.jaranalyzer.service;

import com.jaranalyzer.model.*;
import com.jaranalyzer.service.CallGraphIndex.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.*;

/**
 * Slim orchestrator for building enriched endpoint call trees.
 * Delegates config loading to {@link DomainConfigLoader} and
 * tree building to {@link CallTreeBuilder}.
 */
@Service
public class CallGraphService {

    private static final Logger log = LoggerFactory.getLogger(CallGraphService.class);

    private final ProgressService progressService;
    private final DomainConfigLoader configLoader;
    private final CallTreeBuilder treeBuilder;
    private final ObjectMapper objectMapper;
    private final CollectionLifecycleDetector collectionLifecycleDetector;

    public CallGraphService(ProgressService progressService,
                            DomainConfigLoader configLoader,
                            CallTreeBuilder treeBuilder,
                            ObjectMapper objectMapper,
                            CollectionLifecycleDetector collectionLifecycleDetector) {
        this.progressService = progressService;
        this.configLoader = configLoader;
        this.treeBuilder = treeBuilder;
        this.objectMapper = objectMapper;
        this.collectionLifecycleDetector = collectionLifecycleDetector;
    }

    // ========== Public API ==========

    /**
     * Synchronized: the CallTreeBuilder stores mutable state (ResolutionContext) on an instance field.
     * Without synchronization, concurrent uploads would corrupt each other's index.
     * All three phases (create → populate → build) must run under the same lock.
     */
    public synchronized void buildIndex() {
        treeBuilder.createContext();
    }

    public synchronized void setJarArtifactMap(Map<String, String> jarArtifactMap) {
        ResolutionContext ctx = treeBuilder.getContext();
        if (ctx != null && jarArtifactMap != null) {
            ctx.jarArtifactMap = jarArtifactMap;
        }
    }

    public synchronized void addToIndex(ClassInfo cls) {
        treeBuilder.indexClass(cls);
    }

    /** Set live MongoDB catalog for collection verification (null = no catalog). */
    public synchronized void setMongoCatalog(Set<String> catalogCollections) {
        ResolutionContext ctx = treeBuilder.getContext();
        if (ctx != null) ctx.mongoCatalogCollections = catalogCollections;
    }

    /** Set config properties extracted from JAR for @Value resolution. */
    public synchronized void setConfigProperties(Map<String, String> properties) {
        ResolutionContext ctx = treeBuilder.getContext();
        if (ctx != null && properties != null) ctx.configProperties = properties;
    }

    public synchronized List<EndpointInfo> buildEndpointsFromIndex() {
        ResolutionContext ctx = treeBuilder.getContext();

        log.info("Building call graphs: {} classes, {} interfaces, {} endpoints",
                ctx.classMap.size(), ctx.interfaceImplMap.size(), ctx.endpoints.size());

        prepareIndexForTreeBuilding(ctx);

        List<EndpointInfo> results = new ArrayList<>();
        int done = 0;
        for (IndexedEndpoint iep : ctx.endpoints) {
            IndexedClass controllerClass = ctx.classMap.get(iep.controllerFqn);
            IndexedMethod method = controllerClass != null ? controllerClass.methods.get(iep.methodKey) : null;
            results.add(buildOneEndpoint(iep.httpMethod, iep.path, iep.controllerFqn,
                    iep.controllerSimpleName, iep.methodName, iep.returnType, iep.parameters,
                    controllerClass, method));
            done++;
            if (done % 20 == 0) {
                log.info("  ... {}/{} endpoints", done, ctx.endpoints.size());
                progressService.detail(done + "/" + ctx.endpoints.size() + " endpoints processed...");
            }
        }

        log.info("Call graph complete: {} endpoints", results.size());
        treeBuilder.clearContext();
        return results;
    }

    /**
     * Build call trees for one or more user-specified targets instead of every discovered
     * endpoint. Lets a fast-changing JAR be analyzed for just the flow(s) the user cares about
     * without waiting on a full multi-thousand-endpoint scan. The full bytecode index is still
     * built exactly as {@link #buildEndpointsFromIndex()} requires (all classes indexed,
     * interface implementations expanded, etc.) — only the "which roots get walked" step is
     * narrowed.
     *
     * @param scopeTarget one or more targets, one per line (blank lines ignored). Each line is
     *                     either {@code "METHOD /path"} (matches an existing discovered REST/async
     *                     endpoint by HTTP method + normalized path) or {@code "FQCN#methodName"}
     *                     (an arbitrary method, REST-exposed or not — e.g. a batch job entry
     *                     point), optionally followed by an overload disambiguator:
     *                     {@code FQCN#methodName(Type1,Type2)} matches by parameter types
     *                     (simple or fully-qualified names accepted), or {@code FQCN#methodName#2}
     *                     matches by parameter count alone. Without a disambiguator, every
     *                     overload of the named method is returned.
     * @return one {@link EndpointInfo} per resolved root across all target lines (deduped by
     *         class+method+paramTypes so the same overload is never analyzed twice even if two
     *         lines resolve to it).
     * @throws IllegalArgumentException if no line resolves to any method at all.
     */
    public synchronized List<EndpointInfo> buildScopedEndpointFromIndex(String scopeTarget) {
        ResolutionContext ctx = treeBuilder.getContext();
        prepareIndexForTreeBuilding(ctx);

        List<String> lines = splitScopeTargets(scopeTarget);
        if (lines.isEmpty()) {
            throw new IllegalArgumentException("scopeTarget did not resolve to any method: '"
                    + scopeTarget + "'. Expected 'METHOD /path' (matching a discovered endpoint) "
                    + "or 'FullyQualifiedClassName#methodName' (optionally '(Type1,Type2)' or "
                    + "'#<argCount>' to pick one overload), one per line.");
        }

        // Dedup across lines (and across overloads matched by more than one line) keyed by
        // class+method+paramTypes — a scoped analysis should never build the same tree twice.
        Map<String, ScopedRoot> resolved = new LinkedHashMap<>();
        List<String> unresolved = new ArrayList<>();
        for (String line : lines) {
            List<ScopedRoot> lineRoots = resolveScopeTarget(ctx, line);
            if (lineRoots.isEmpty()) {
                unresolved.add(line);
                continue;
            }
            for (ScopedRoot root : lineRoots) {
                String key = root.cls.fqn + "#" + root.method.name + root.method.paramTypes;
                resolved.putIfAbsent(key, root);
            }
        }
        if (resolved.isEmpty()) {
            throw new IllegalArgumentException("scopeTarget did not resolve to any method: '"
                    + scopeTarget + "'. Expected 'METHOD /path' (matching a discovered endpoint) "
                    + "or 'FullyQualifiedClassName#methodName' (optionally '(Type1,Type2)' or "
                    + "'#<argCount>' to pick one overload), one per line.");
        }
        if (!unresolved.isEmpty()) {
            log.warn("scopeTarget line(s) did not resolve to any method, skipped: {}", unresolved);
        }

        List<EndpointInfo> results = new ArrayList<>();
        for (ScopedRoot root : resolved.values()) {
            results.add(buildOneEndpoint(root.httpMethod, root.path, root.cls.fqn, root.cls.simpleName,
                    root.method.name, root.method.returnType, toParameterInfoList(root.method.paramTypes),
                    root.cls, root.method));
        }

        log.info("Scoped call graph complete: {} target line(s) -> {} root(s)", lines.size(), results.size());
        treeBuilder.clearContext();
        return results;
    }

    /**
     * Split a possibly multi-line scopeTarget into individual target strings, ignoring blanks.
     * Splits on newlines only — commas must stay intact within a "(Type1,Type2)" disambiguator,
     * so they are never treated as a target separator.
     */
    private static List<String> splitScopeTargets(String scopeTarget) {
        if (scopeTarget == null || scopeTarget.isBlank()) return List.of();
        List<String> out = new ArrayList<>();
        for (String line : scopeTarget.split("[\\r\\n]+")) {
            String t = line.trim();
            if (!t.isEmpty()) out.add(t);
        }
        return out;
    }

    /** Shared setup that must run once before any call tree is built from the index. */
    private void prepareIndexForTreeBuilding(ResolutionContext ctx) {
        // Auto-detect base packages from indexed classes if config is empty
        List<String> basePackages = configLoader.getBasePackages();
        if (basePackages == null || basePackages.isEmpty()) {
            configLoader.setBasePackages(treeBuilder.autoDetectBasePackages());
        }
        // Store detected packages in context for cross-JAR resolution
        ctx.detectedBasePackages = configLoader.getBasePackages();
        log.info("Base packages for external call filtering: {}", configLoader.getBasePackages());

        // Nested JAR classes are indexed for call graph resolution (following calls
        // into shared libraries) but NOT as endpoints — endpoints come only from
        // the main JAR's controllers (sourceJar == null).

        treeBuilder.resolveRepositoryCollections();
        treeBuilder.expandInterfaceImplMapTransitively();

        // Subtree cache disabled: serializing full CallNode trees to disk was slower than
        // rebuilding (1.3GB file at 320/1173 endpoints). The per-endpoint in-memory memo
        // already handles intra-endpoint dedup effectively. Cross-endpoint caching needs a
        // lighter approach (e.g., cache only the node structure, not serialized JSON).
    }

    /** Build one EndpointInfo (tree + fragments + aggregates) for a single resolved root. */
    private EndpointInfo buildOneEndpoint(String httpMethod, String path, String controllerFqn,
                                          String controllerSimpleName, String methodName,
                                          String returnType, List<ParameterInfo> parameters,
                                          IndexedClass controllerClass, IndexedMethod method) {
        EndpointInfo ep = new EndpointInfo();
        ep.setHttpMethod(httpMethod);
        ep.setFullPath(path);
        ep.setControllerClass(controllerFqn);
        ep.setControllerSimpleName(controllerSimpleName);
        ep.setMethodName(methodName);
        ep.setReturnType(returnType);
        ep.setParameters(parameters);

        if (controllerClass != null && method != null) {
            Set<String> callStack = new LinkedHashSet<>();
            int[] nodeCount = {0};
            Map<String, List<CallNode>> fragments = new LinkedHashMap<>();
            CallNode root = treeBuilder.buildCallTree(controllerClass, method, callStack, 0,
                    nodeCount, controllerClass.sourceJar, fragments);
            treeBuilder.cleanOperationTypes(root, fragments);
            ep.setCallTree(root);
            ep.setFragments(fragments);
        }

        ep.computeAggregates();
        ep.setCollectionLifecycleFlows(collectionLifecycleDetector.aggregate(ep.getCallTree(), ep.getFragments()));
        return ep;
    }

    private record ScopedRoot(String httpMethod, String path, IndexedClass cls, IndexedMethod method) {}

    /** IndexedMethod only retains parameter TYPES (names are discarded during indexing) — build
     *  minimal ParameterInfo entries so EndpointInfo.parameters is still populated for a scoped
     *  target's endpoint.json output, consistent in shape with the full-scan path. */
    private static List<ParameterInfo> toParameterInfoList(List<String> paramTypes) {
        List<ParameterInfo> result = new ArrayList<>(paramTypes.size());
        for (String type : paramTypes) {
            ParameterInfo pi = new ParameterInfo();
            pi.setType(type);
            result.add(pi);
        }
        return result;
    }

    /**
     * Resolve a scopeTarget string to one or more (class, method) roots.
     * "METHOD /path" → look up an existing discovered endpoint by method+normalized path.
     * "FQCN#methodName" → look up the class directly; if the method is overloaded, every
     * overload is returned as a separate root rather than guessing one — unless an overload
     * disambiguator is appended: "FQCN#methodName(Type1,Type2)" matches by parameter types
     * (simple or fully-qualified names accepted, same tolerance as {@link CallTreeBuilder}'s
     * targeted-analysis matcher), or "FQCN#methodName#<argCount>" matches by parameter count.
     */
    private List<ScopedRoot> resolveScopeTarget(ResolutionContext ctx, String scopeTarget) {
        if (scopeTarget == null || scopeTarget.isBlank()) return List.of();
        String trimmed = scopeTarget.trim();

        int sharp = trimmed.indexOf('#');
        if (sharp > 0 && !trimmed.contains(" ")) {
            // "FQCN#methodName", optionally with a "(Type1,Type2)" or "#<argCount>" disambiguator.
            String fqcn = trimmed.substring(0, sharp);
            String rest = trimmed.substring(sharp + 1);

            String methodName = rest;
            List<String> paramTypes = null;   // exact type match requested
            Integer argCount = null;          // arity-only match requested

            int paren = rest.indexOf('(');
            if (paren > 0 && rest.endsWith(")")) {
                methodName = rest.substring(0, paren);
                String inner = rest.substring(paren + 1, rest.length() - 1).trim();
                paramTypes = inner.isEmpty() ? List.of()
                        : Arrays.stream(inner.split(",")).map(String::trim).toList();
            } else {
                int secondSharp = rest.indexOf('#');
                if (secondSharp > 0) {
                    String countStr = rest.substring(secondSharp + 1).trim();
                    methodName = rest.substring(0, secondSharp);
                    try {
                        argCount = Integer.parseInt(countStr);
                    } catch (NumberFormatException e) {
                        return List.of();   // malformed disambiguator — no silent partial match
                    }
                }
            }

            IndexedClass cls = ctx.classMap.get(fqcn);
            if (cls == null) return List.of();
            List<ScopedRoot> roots = new ArrayList<>();
            for (IndexedMethod m : cls.methods.values()) {
                if (!methodName.equals(m.name)) continue;
                if (paramTypes != null && !treeBuilder.paramTypesMatch(m.paramTypes, paramTypes)) continue;
                if (argCount != null && m.paramTypes.size() != argCount) continue;
                roots.add(new ScopedRoot(m.httpMethod, m.path, cls, m));
            }
            return roots;
        }

        // "METHOD /path" form — match against already-discovered endpoints by method + path,
        // normalizing {pathVariable} segments so a template matches itself regardless of
        // incidental formatting differences (leading/trailing slash, case on the HTTP verb).
        int firstSpace = trimmed.indexOf(' ');
        if (firstSpace <= 0) return List.of();
        String wantMethod = trimmed.substring(0, firstSpace).trim().toUpperCase(Locale.ROOT);
        String wantPath = normalizePath(trimmed.substring(firstSpace + 1).trim());

        for (IndexedEndpoint iep : ctx.endpoints) {
            String haveMethod = iep.httpMethod != null ? iep.httpMethod.toUpperCase(Locale.ROOT) : "";
            if (!haveMethod.equals(wantMethod)) continue;
            if (!normalizePath(iep.path).equals(wantPath)) continue;
            IndexedClass cls = ctx.classMap.get(iep.controllerFqn);
            IndexedMethod m = cls != null ? cls.methods.get(iep.methodKey) : null;
            if (cls != null && m != null) {
                return List.of(new ScopedRoot(iep.httpMethod, iep.path, cls, m));
            }
        }
        return List.of();
    }

    /** Normalize a REST path template for matching: strip leading/trailing slashes, collapse
     *  any {pathVariable} segment to a single wildcard token so templates compare structurally. */
    private static String normalizePath(String path) {
        if (path == null) return "";
        String p = path.trim();
        if (!p.startsWith("/")) p = "/" + p;
        while (p.length() > 1 && p.endsWith("/")) p = p.substring(0, p.length() - 1);
        return p.replaceAll("\\{[^}]*}", "{}");
    }

    /** A developer-supplied targeted-analysis root: class + method (+ optional param types). */
    public record MethodRef(String className, String methodName, List<String> paramTypes) {}

    /**
     * Targeted "developer analysis": build COMPLETE call trees rooted only at the supplied
     * methods (not the whole project). Overload-safe and interface/impl tolerant via
     * {@link CallTreeBuilder#resolveRoots}. Unresolved roots are skipped and logged.
     * Requires the index to already be populated (buildIndex + addToIndex).
     */
    public synchronized List<EndpointInfo> buildTreesForRoots(List<MethodRef> roots) {
        ResolutionContext ctx = treeBuilder.getContext();
        log.info("Building targeted call trees for {} root(s)", roots.size());

        // Same resolution prep as buildEndpointsFromIndex so cross-JAR/interface links resolve.
        List<String> basePackages = configLoader.getBasePackages();
        if (basePackages == null || basePackages.isEmpty()) {
            configLoader.setBasePackages(treeBuilder.autoDetectBasePackages());
        }
        ctx.detectedBasePackages = configLoader.getBasePackages();
        treeBuilder.resolveRepositoryCollections();
        treeBuilder.expandInterfaceImplMapTransitively();

        List<MethodRef> expanded = expandRoots(ctx, roots);

        // Dedup by class+method+paramTypes — a package root and one of its own classes (or
        // that class's own method) can both be supplied and would otherwise build the same
        // call tree twice.
        Map<String, MethodRef> deduped = new LinkedHashMap<>();
        for (MethodRef ref : expanded) {
            String key = ref.className() + "#" + ref.methodName() + "#" + ref.paramTypes();
            deduped.putIfAbsent(key, ref);
        }

        List<EndpointInfo> results = new ArrayList<>();
        List<String> notFound = new ArrayList<>();
        for (MethodRef ref : deduped.values()) {
            List<CallTreeBuilder.RootMatch> matches =
                    treeBuilder.resolveRoots(ref.className(), ref.methodName(), ref.paramTypes());
            if (matches.isEmpty()) {
                notFound.add(ref.className() + "#" + ref.methodName());
                log.warn("Targeted root not resolved: {}#{}", ref.className(), ref.methodName());
                continue;
            }
            for (CallTreeBuilder.RootMatch m : matches) {
                results.add(buildEndpointForRoot(m.cls(), m.method()));
            }
        }
        if (!notFound.isEmpty()) {
            log.warn("Targeted analysis: {} unresolved root(s): {}", notFound.size(), notFound);
        }
        log.info("Targeted call graph complete: {} endpoints from {} requested root(s)", results.size(), roots.size());
        treeBuilder.clearContext();
        return results;
    }

    /**
     * Expand any root with no methodName (a bare class or package name, e.g. pasted from a
     * "FQCN" or "com.foo.bar" string with no "#method" suffix) into one root per resolvable
     * method: a bare class FQN expands to every method declared on that class; a name with no
     * exact class match but that prefixes one or more indexed classes' FQNs is treated as a
     * package and expands to every method on every class under it. Roots that already carry a
     * methodName pass through unchanged.
     */
    private List<MethodRef> expandRoots(ResolutionContext ctx, List<MethodRef> roots) {
        List<MethodRef> expanded = new ArrayList<>();
        for (MethodRef ref : roots) {
            if (ref.methodName() != null && !ref.methodName().isBlank()) {
                expanded.add(ref);
                continue;
            }
            IndexedClass exact = ctx.classMap.get(ref.className());
            if (exact != null) {
                for (IndexedMethod m : exact.methods.values()) {
                    expanded.add(new MethodRef(exact.fqn, m.name, null));
                }
                log.info("Expanded class root '{}' to {} method(s)", ref.className(), exact.methods.size());
                continue;
            }
            String pkgPrefix = ref.className() + ".";
            List<IndexedClass> underPackage = ctx.classMap.values().stream()
                    .filter(c -> c.fqn.startsWith(pkgPrefix))
                    .toList();
            if (!underPackage.isEmpty()) {
                int methodCount = 0;
                for (IndexedClass c : underPackage) {
                    for (IndexedMethod m : c.methods.values()) {
                        expanded.add(new MethodRef(c.fqn, m.name, null));
                        methodCount++;
                    }
                }
                log.info("Expanded package root '{}' to {} class(es), {} method(s)",
                        ref.className(), underPackage.size(), methodCount);
                continue;
            }
            // Neither a class nor a package prefix matched — leave as-is so it's reported
            // unresolved below rather than silently dropped.
            expanded.add(ref);
        }
        return expanded;
    }

    private EndpointInfo buildEndpointForRoot(IndexedClass cls, IndexedMethod method) {
        EndpointInfo ep = new EndpointInfo();
        ep.setHttpMethod(method.httpMethod != null ? method.httpMethod : "TARGETED");
        ep.setFullPath(method.path != null ? method.path : cls.simpleName + "#" + method.name);
        ep.setControllerClass(cls.fqn);
        ep.setControllerSimpleName(cls.simpleName);
        ep.setMethodName(method.name);
        ep.setReturnType(method.returnType);
        ep.setParameters(new ArrayList<>());

        Set<String> callStack = new LinkedHashSet<>();
        int[] nodeCount = {0};
        // Fragment-aware, matching buildOneEndpoint: without collecting fragments here, a
        // targeted analysis of a wide method silently DROPS the overflow children instead of
        // making them reachable via fragmentRef.
        Map<String, List<CallNode>> fragments = new LinkedHashMap<>();
        CallNode root = treeBuilder.buildCallTree(cls, method, callStack, 0, nodeCount,
                cls.sourceJar, fragments);
        treeBuilder.cleanOperationTypes(root, fragments);
        ep.setCallTree(root);
        ep.setFragments(fragments);
        ep.computeAggregates();
        // Restored: the upstream snapshot dropped this line when aggregate() became 2-arg, which
        // silently stripped collectionLifecycleFlows from every TARGETED analysis.
        ep.setCollectionLifecycleFlows(
                collectionLifecycleDetector.aggregate(ep.getCallTree(), ep.getFragments()));
        return ep;
    }
}
