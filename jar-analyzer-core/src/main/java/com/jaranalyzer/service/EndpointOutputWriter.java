package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.model.CallNode;
import com.jaranalyzer.model.EndpointInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;

@Component
class EndpointOutputWriter {

    private static final Logger log = LoggerFactory.getLogger(EndpointOutputWriter.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final com.fasterxml.jackson.databind.ObjectWriter prettyWriter = objectMapper.writerWithDefaultPrettyPrinter();
    private final JarDataPaths jarDataPaths;
    private final TreeChunker treeChunker;
    private final FragmentStore fragmentStore;

    private final ArtifactStore store;

    EndpointOutputWriter(JarDataPaths jarDataPaths, TreeChunker treeChunker,
                         FragmentStore fragmentStore, @Qualifier("jarArtifactStore") ArtifactStore store) {
        this.jarDataPaths = jarDataPaths;
        this.treeChunker = treeChunker;
        this.fragmentStore = fragmentStore;
        this.store = store;
    }

    void writeEndpointOutput(String jarName, EndpointInfo ep, int index) {
        String epDirName = String.format("%03d_%s", index,
                fragmentStore.sanitizeFragment(ep.getControllerSimpleName() + "." + ep.getMethodName()));
        ArtifactKey epDir = jarDataPaths.endpointsDirKey(jarName).resolve(epDirName);

        try {
            Map<String, Object> meta = new LinkedHashMap<>();
            meta.put("httpMethod", ep.getHttpMethod());
            meta.put("fullPath", ep.getFullPath());
            meta.put("controllerClass", ep.getControllerClass());
            meta.put("controllerSimpleName", ep.getControllerSimpleName());
            meta.put("methodName", ep.getMethodName());
            meta.put("returnType", ep.getReturnType());
            meta.put("parameters", ep.getParameters());
            // Controller-level source identity (from call tree root node)
            CallNode root = ep.getCallTree();
            meta.put("sourceJar", root != null && root.getSourceJar() != null ? root.getSourceJar() : "");
            meta.put("jarName", root != null && root.getJarName() != null ? root.getJarName() : "");
            meta.put("sourcePath", root != null && root.getSourcePath() != null ? root.getSourcePath() : "");
            meta.put("analyzedAt", LocalDateTime.now().toString());
            // Aggregated endpoint-level summaries
            meta.put("aggregatedCollections", ep.getAggregatedCollections());
            meta.put("aggregatedCollectionDomains", ep.getAggregatedCollectionDomains());
            meta.put("aggregatedOperations", ep.getAggregatedOperations());
            meta.put("derivedOperationType", ep.getDerivedOperationType());
            meta.put("totalDbOperations", ep.getTotalDbOperations());
            meta.put("totalMethods", ep.getTotalMethods());
            meta.put("totalLoc", ep.getTotalLoc());
            meta.put("inScopeCalls", ep.getInScopeCalls());
            meta.put("externalScopeCalls", ep.getExternalScopeCalls());
            meta.put("modules", ep.getModules());
            meta.put("externalCalls", ep.getExternalCalls());
            meta.put("httpCalls", ep.getHttpCalls());
            meta.put("beans", ep.getBeans());
            meta.put("operationTypesList", ep.getOperationTypesList());
            meta.put("procNames", ep.getProcNames());
            meta.put("dynamicFlows", ep.getDynamicFlows());
            meta.put("aggregationFlows", ep.getAggregationFlows());
            meta.put("dynamicCollectionFlows", ep.getDynamicCollectionFlows());
            meta.put("collectionLifecycleFlows", ep.getCollectionLifecycleFlows());
            fragmentStore.writeFragment(epDir, "endpoint.json",
                    prettyWriter.writeValueAsString(meta));

            // Dictionary form, not the expanded tree: a call tree is a DAG in memory, and writing
            // it nested re-emits every shared subtree once per path reaching it. That is the same
            // blow-up that made analysis.json unwritable, multiplied by the endpoint count.
            // CallTreeInflater.expandSidecar rebuilds the exact nested shape for readers.
            // Written COMPACT, unlike the other fragments. This is the largest derived artifact in
            // the tree — 36.07 MB across 317 endpoints on unified-web, pretty-printed — and it is
            // never read by a human: its only route,
            // GET /{id}/endpoints/{endpoint}/call-tree, expands the dictionary and re-serialises
            // the result, so the stored whitespace never reaches a response. Indenting it costs
            // roughly two thirds of its size for nothing. (endpoint.json and the node_*.json files
            // stay pretty-printed: those ARE served verbatim, so their bytes are a contract.)
            fragmentStore.writeFragment(epDir, "call-tree.json",
                    objectMapper.writeValueAsString(
                            CallTreeFlattener.sidecar(objectMapper, ep.getCallTree())));

            // Persist overflow fragments (nodes whose parent exceeded max-children-per-node)
            // so the "expand N more" affordance has something to fetch. Written before
            // flattening so nodes.json below can walk into them too.
            Map<String, List<CallNode>> fragments = ep.getFragments();
            if (fragments != null && !fragments.isEmpty()) {
                ArtifactKey fragmentsDir = epDir.resolve("fragments");
                for (Map.Entry<String, List<CallNode>> e : fragments.entrySet()) {
                    fragmentStore.writeFragment(fragmentsDir, e.getKey() + ".json",
                            prettyWriter.writeValueAsString(Map.of("children", e.getValue())));
                }
            }

            List<Map<String, Object>> flatNodes = new ArrayList<>();
            Set<CallNode> visited = Collections.newSetFromMap(new IdentityHashMap<>());
            flattenCallTree(ep.getCallTree(), flatNodes, 0, new int[]{0}, visited, fragments);

            // Also compact, and this one DOES change response bytes — unlike call-tree.json, the
            // /nodes route returns this file verbatim. The change is whitespace only, and every
            // consumer parses: summary-trace.js does resp.json() on both this and node_*.json,
            // ExternalApiLinkService stream-parses it looking for HTTP_CALL leaves, and this
            // class's own folder listing parses it too. No consumer reads the raw text.
            // 53.20 MB across 317 endpoints, the largest remaining artifact under endpoints/.
            fragmentStore.writeFragment(epDir, "nodes.json",
                    objectMapper.writeValueAsString(Map.of(
                            "endpoint", ep.getControllerSimpleName() + "." + ep.getMethodName(),
                            "totalNodes", flatNodes.size(),
                            "nodes", flatNodes
                    )));

            ArtifactKey nodesDir = epDir.resolve("nodes");
            for (Map<String, Object> node : flatNodes) {
                String nodeId = String.valueOf(node.get("nodeId"));
                fragmentStore.writeFragment(nodesDir, "node_" + nodeId + ".json",
                        objectMapper.writeValueAsString(node));
            }

            log.info("  Endpoint output: {} ({} nodes)", epDir.leaf(), flatNodes.size());
        } catch (IOException e) {
            log.warn("Failed to write endpoint output for {}: {}", epDirName, e.getMessage());
        }
    }

    void flattenCallTree(CallNode node, List<Map<String, Object>> flatNodes,
                         int depth, int[] idCounter, Set<CallNode> visited,
                         Map<String, List<CallNode>> fragments) {
        if (node == null || !visited.add(node)) return;

        int nodeId = idCounter[0]++;
        Map<String, Object> flat = new LinkedHashMap<>();
        flat.put("nodeId", nodeId);
        flat.put("depth", depth);
        flat.put("id", node.getId());
        flat.put("signature", treeChunker.nodeSignature(node));
        // Class / method identity
        flat.put("className", node.getClassName());
        flat.put("simpleClassName", node.getSimpleClassName());
        flat.put("methodName", node.getMethodName());
        flat.put("parameterTypes", node.getParameterTypes());
        flat.put("returnType", node.getReturnType());
        flat.put("lineCount", node.getLineCount());
        flat.put("stereotype", node.getStereotype());
        flat.put("annotations", node.getAnnotations());
        flat.put("annotationDetails", node.getAnnotationDetails());
        // Source location
        flat.put("sourceJar", node.getSourceJar());
        flat.put("jarName", node.getJarName());
        flat.put("sourcePath", node.getSourcePath());
        flat.put("sourceCode", node.getSourceCode());
        // Call graph topology
        flat.put("callType", node.getCallType());
        flat.put("recursive", node.isRecursive());
        flat.put("crossModule", node.isCrossModule());
        flat.put("module", node.getModule());
        flat.put("domain", node.getDomain());
        flat.put("childCount", node.getChildren() != null ? node.getChildren().size() : 0);
        // Dispatch resolution
        flat.put("dispatchType", node.getDispatchType());
        flat.put("resolvedFrom", node.getResolvedFrom());
        flat.put("qualifierHint", node.getQualifierHint());
        // Cross-JAR HTTP call target (see ExternalApiLinkService)
        flat.put("httpTargetMethod", node.getHttpTargetMethod());
        flat.put("httpTargetPath", node.getHttpTargetPath());
        flat.put("httpTargetModuleHint", node.getHttpTargetModuleHint());
        // DB / collection operations
        flat.put("operationType", node.getOperationType());
        flat.put("operationTypes", node.getOperationTypes());
        flat.put("collectionsAccessed", node.getCollectionsAccessed());
        flat.put("collectionDomains", node.getCollectionDomains());
        flat.put("collectionSources", node.getCollectionSources());
        flat.put("collectionVerification", node.getCollectionVerification());
        flat.put("hasDbInteraction", node.isHasDbInteraction());
        flat.put("dynamicCollectionRefs", node.getDynamicCollectionRefs());
        flat.put("lifecycleStageTags", node.getLifecycleStageTags());
        flat.put("entityFieldMappings", node.getEntityFieldMappings());
        // Advanced feature metadata
        flat.put("aggregationPipeline", node.getAggregationPipeline());
        flat.put("cacheOperations", node.getCacheOperations());
        flat.put("sqlStatements", node.getSqlStatements());
        flat.put("oracleProcRef", node.getOracleProcRef());
        flat.put("oracleSchema", node.getOracleSchema());
        flat.put("stringLiterals", node.getStringLiterals());
        flat.put("fragmentRef", node.getFragmentRef());
        flat.put("truncatedChildCount", node.getTruncatedChildCount());
        flatNodes.add(flat);

        if (node.getChildren() != null) {
            for (CallNode child : node.getChildren()) {
                flattenCallTree(child, flatNodes, depth + 1, idCounter, visited, fragments);
            }
        }
        // Fragment overflow — same node's remaining children, kept out of the inline tree once
        // it exceeded max-children-per-node. Still flattened into nodes.json/nodes/{id}.json so
        // nothing is missing from the persisted output, just reached via a different path (the
        // node's own fragmentRef) than the main tree.
        if (node.getFragmentRef() != null && fragments != null) {
            List<CallNode> overflow = fragments.get(node.getFragmentRef());
            if (overflow != null) {
                for (CallNode child : overflow) {
                    flattenCallTree(child, flatNodes, depth + 1, idCounter, visited, fragments);
                }
            }
        }
    }

    private String endpointLabel(int idx, EndpointInfo ep) {
        return String.format("%03d_%s", idx + 1,
                fragmentStore.sanitizeFragment(ep.getControllerSimpleName() + "." + ep.getMethodName()));
    }

    /** Unwraps to the deepest cause so OOM (wrapped in ExecutionException) is reported as itself, not "null". */
    private static String rootCauseMessage(Throwable e) {
        Throwable cause = e;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause.getClass().getSimpleName() + (cause.getMessage() != null ? ": " + cause.getMessage() : "");
    }

    private static final class PendingWrite {
        final int idx;
        final EndpointInfo ep;
        final Future<?> future;

        PendingWrite(int idx, EndpointInfo ep, Future<?> future) {
            this.idx = idx;
            this.ep = ep;
            this.future = future;
        }
    }

    public void writeEndpointOutputs(String jarName, List<EndpointInfo> endpoints) {
        int total = endpoints.size();
        log.info("Writing per-endpoint output for {} endpoints...", total);

        int threads = Math.min(4, Runtime.getRuntime().availableProcessors());
        ExecutorService pool = Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r, "ep-output-writer");
            t.setDaemon(true);
            return t;
        });

        List<PendingWrite> pending = new ArrayList<>(total);
        for (int i = 0; i < total; i++) {
            final int idx = i;
            final EndpointInfo ep = endpoints.get(i);
            pending.add(new PendingWrite(idx, ep, pool.submit(() -> writeEndpointOutput(jarName, ep, idx + 1))));
        }

        List<PendingWrite> failed = new ArrayList<>();
        boolean cancelled = false;
        int done = 0;
        for (PendingWrite pw : pending) {
            try {
                pw.future.get();
            } catch (InterruptedException e) {
                // Job cancellation interrupts this thread. Restore the flag (Future.get clears
                // it) and stop: the remaining endpoints are abandoned deliberately, NOT queued
                // for retry — retrying here would serially redo exactly the work the user just
                // cancelled, on the interrupted thread, while the queue waits for us.
                Thread.currentThread().interrupt();
                cancelled = true;
                log.info("[5.7] Cancelled after {}/{} endpoint folders — abandoning the rest", done, total);
                break;
            } catch (Exception e) {
                log.warn("[5.7] Endpoint output failed for {}: {}", endpointLabel(pw.idx, pw.ep), rootCauseMessage(e));
                failed.add(pw);
            }
            done++;
            if (done % 50 == 0 || done == total) {
                log.info("[5.7] ({}/{}) endpoint folders written", done, total);
            }
        }
        if (cancelled) {
            pool.shutdownNow();   // stop the in-flight writers too, don't let them finish
            log.info("Per-endpoint output cancelled for {}", JarNameUtil.normalizeKey(jarName));
            return;
        }
        pool.shutdown();

        if (!failed.isEmpty()) {
            log.warn("[5.7] Retrying {} failed endpoint(s) single-threaded: {}", failed.size(),
                    failed.stream().map(pw -> endpointLabel(pw.idx, pw.ep)).toList());
            for (PendingWrite pw : failed) {
                String label = endpointLabel(pw.idx, pw.ep);
                try {
                    writeEndpointOutput(jarName, pw.ep, pw.idx + 1);
                    log.info("[5.7] Retry succeeded for {}", label);
                } catch (Throwable e) {
                    log.error("[5.7] Retry failed for {}: {}", label, rootCauseMessage(e));
                }
            }
        }

        log.info("Per-endpoint output complete: {} folders in {}/endpoints/",
                total, JarNameUtil.normalizeKey(jarName));
    }

    /**
     * ONE prefix listing discovers both the endpoint folders and which of them have a
     * {@code nodes.json}; the old code did a directory list plus an {@code exists} per folder,
     * which is one extra S3 HEAD per endpoint (thousands, for a large JAR).
     */
    public List<Map<String, Object>> listEndpointOutputs(String jarName) {
        ArtifactKey root = jarDataPaths.endpointsDirKey(jarName);
        List<Map<String, Object>> result = new ArrayList<>();
        int depth = root.segments().size();
        try {
            var all = store.statPrefix(root);
            // folder -> its nodes.json key, or null when the folder has none
            Map<String, ArtifactKey> folders = new TreeMap<>();
            for (var k : all.keySet()) {
                var segs = k.segments();
                if (segs.size() <= depth) continue;
                String folder = segs.get(depth);
                folders.putIfAbsent(folder, null);
                if (segs.size() == depth + 2 && k.leaf().equals("nodes.json")) folders.put(folder, k);
            }
            for (var e : folders.entrySet()) {
                Map<String, Object> info = new LinkedHashMap<>();
                info.put("name", e.getKey());
                if (e.getValue() != null) {
                    // Streamed, not readTree'd: nodes.json holds every flattened call-tree node
                    // and runs to hundreds of MB on a big endpoint, while all we want is one int
                    // near the top.
                    try (InputStream in = store.read(e.getValue());
                         var parser = objectMapper.getFactory().createParser(in)) {
                        while (parser.nextToken() != null) {
                            if (parser.currentToken() == com.fasterxml.jackson.core.JsonToken.FIELD_NAME
                                    && "totalNodes".equals(parser.currentName())) {
                                parser.nextToken();
                                info.put("totalNodes", parser.getIntValue());
                                break;
                            }
                        }
                    } catch (IOException ex) { /* skip */ }
                }
                result.add(info);
            }
        } catch (IOException e) {
            log.debug("Failed to list endpoint outputs: {}", e.getMessage());
        }
        return result;
    }
}
