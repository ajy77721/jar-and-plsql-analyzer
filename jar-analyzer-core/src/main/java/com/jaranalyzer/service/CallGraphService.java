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

    public CallGraphService(ProgressService progressService,
                            DomainConfigLoader configLoader,
                            CallTreeBuilder treeBuilder,
                            ObjectMapper objectMapper) {
        this.progressService = progressService;
        this.configLoader = configLoader;
        this.treeBuilder = treeBuilder;
        this.objectMapper = objectMapper;
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

        List<EndpointInfo> results = new ArrayList<>();
        int done = 0;
        for (IndexedEndpoint iep : ctx.endpoints) {
            EndpointInfo ep = new EndpointInfo();
            ep.setHttpMethod(iep.httpMethod);
            ep.setFullPath(iep.path);
            ep.setControllerClass(iep.controllerFqn);
            ep.setControllerSimpleName(iep.controllerSimpleName);
            ep.setMethodName(iep.methodName);
            ep.setReturnType(iep.returnType);
            ep.setParameters(iep.parameters);

            IndexedClass controllerClass = ctx.classMap.get(iep.controllerFqn);
            if (controllerClass != null) {
                IndexedMethod method = controllerClass.methods.get(iep.methodKey);
                if (method != null) {
                    Set<String> callStack = new LinkedHashSet<>();
                    int[] nodeCount = {0};
                    CallNode root = treeBuilder.buildCallTree(controllerClass, method, callStack, 0, nodeCount, controllerClass.sourceJar);
                    treeBuilder.cleanOperationTypes(root);
                    ep.setCallTree(root);
                }
            }

            ep.computeAggregates();
            results.add(ep);
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
}
