package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.model.CallNode;
import com.jaranalyzer.model.EndpointInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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

    EndpointOutputWriter(JarDataPaths jarDataPaths, TreeChunker treeChunker, FragmentStore fragmentStore) {
        this.jarDataPaths = jarDataPaths;
        this.treeChunker = treeChunker;
        this.fragmentStore = fragmentStore;
    }

    void writeEndpointOutput(String jarName, EndpointInfo ep, int index) {
        String epDirName = String.format("%03d_%s", index,
                fragmentStore.sanitizeFragment(ep.getControllerSimpleName() + "." + ep.getMethodName()));
        Path epDir = jarDataPaths.endpointsDir(jarName).resolve(epDirName);

        try {
            Files.createDirectories(epDir);

            Map<String, Object> meta = new LinkedHashMap<>();
            meta.put("httpMethod", ep.getHttpMethod());
            meta.put("fullPath", ep.getFullPath());
            meta.put("controllerClass", ep.getControllerClass());
            meta.put("controllerSimpleName", ep.getControllerSimpleName());
            meta.put("methodName", ep.getMethodName());
            meta.put("returnType", ep.getReturnType());
            meta.put("parameters", ep.getParameters());
            meta.put("analyzedAt", LocalDateTime.now().toString());
            fragmentStore.writeFragment(epDir, "endpoint.json",
                    prettyWriter.writeValueAsString(meta));

            fragmentStore.writeFragment(epDir, "call-tree.json",
                    prettyWriter.writeValueAsString(ep.getCallTree()));

            List<Map<String, Object>> flatNodes = new ArrayList<>();
            Set<CallNode> visited = Collections.newSetFromMap(new IdentityHashMap<>());
            flattenCallTree(ep.getCallTree(), flatNodes, 0, new int[]{0}, visited);

            fragmentStore.writeFragment(epDir, "nodes.json",
                    prettyWriter.writeValueAsString(Map.of(
                            "endpoint", ep.getControllerSimpleName() + "." + ep.getMethodName(),
                            "totalNodes", flatNodes.size(),
                            "nodes", flatNodes
                    )));

            Path nodesDir = epDir.resolve("nodes");
            Files.createDirectories(nodesDir);
            for (Map<String, Object> node : flatNodes) {
                String nodeId = String.valueOf(node.get("nodeId"));
                fragmentStore.writeFragment(nodesDir, "node_" + nodeId + ".json",
                        prettyWriter.writeValueAsString(node));
            }

            log.info("  Endpoint output: {} ({} nodes)", epDir.getFileName(), flatNodes.size());
        } catch (IOException e) {
            log.warn("Failed to write endpoint output for {}: {}", epDirName, e.getMessage());
        }
    }

    void flattenCallTree(CallNode node, List<Map<String, Object>> flatNodes,
                         int depth, int[] idCounter, Set<CallNode> visited) {
        if (node == null || !visited.add(node)) return;

        int nodeId = idCounter[0]++;
        Map<String, Object> flat = new LinkedHashMap<>();
        flat.put("nodeId", nodeId);
        flat.put("depth", depth);
        flat.put("signature", treeChunker.nodeSignature(node));
        flat.put("className", node.getClassName());
        flat.put("simpleClassName", node.getSimpleClassName());
        flat.put("methodName", node.getMethodName());
        flat.put("parameterTypes", node.getParameterTypes());
        flat.put("returnType", node.getReturnType());
        flat.put("stereotype", node.getStereotype());
        flat.put("annotations", node.getAnnotations());
        flat.put("sourceCode", node.getSourceCode());
        flat.put("collectionsAccessed", node.getCollectionsAccessed());
        flat.put("domain", node.getDomain());
        flat.put("operationType", node.getOperationType());
        flat.put("crossModule", node.isCrossModule());
        flat.put("module", node.getModule());
        flat.put("dispatchType", node.getDispatchType());
        flat.put("resolvedFrom", node.getResolvedFrom());
        flat.put("qualifierHint", node.getQualifierHint());
        flat.put("childCount", node.getChildren() != null ? node.getChildren().size() : 0);
        flat.put("annotationDetails", node.getAnnotationDetails());
        flat.put("stringLiterals", node.getStringLiterals());
        flat.put("collectionDomains", node.getCollectionDomains());
        flatNodes.add(flat);

        if (node.getChildren() != null) {
            for (CallNode child : node.getChildren()) {
                flattenCallTree(child, flatNodes, depth + 1, idCounter, visited);
            }
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

        List<Future<?>> futures = new ArrayList<>(total);
        for (int i = 0; i < total; i++) {
            final int idx = i;
            final EndpointInfo ep = endpoints.get(i);
            futures.add(pool.submit(() -> writeEndpointOutput(jarName, ep, idx + 1)));
        }

        int done = 0;
        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (Exception e) {
                log.warn("[5.7] Endpoint output failed: {}", e.getMessage());
            }
            done++;
            if (done % 50 == 0 || done == total) {
                log.info("[5.7] ({}/{}) endpoint folders written", done, total);
            }
        }
        pool.shutdown();

        log.info("Per-endpoint output complete: {} folders in {}/endpoints/",
                total, JarNameUtil.normalizeKey(jarName));
    }

    public List<Map<String, Object>> listEndpointOutputs(String jarName) {
        Path jarDir = jarDataPaths.endpointsDir(jarName);
        List<Map<String, Object>> result = new ArrayList<>();
        if (!Files.isDirectory(jarDir)) return result;

        try (var stream = Files.list(jarDir)) {
            stream.filter(Files::isDirectory).sorted().forEach(dir -> {
                Map<String, Object> info = new LinkedHashMap<>();
                info.put("name", dir.getFileName().toString());
                Path nodesFile = dir.resolve("nodes.json");
                if (Files.exists(nodesFile)) {
                    try {
                        String content = Files.readString(nodesFile);
                        var tree = objectMapper.readTree(content);
                        if (tree.has("totalNodes")) {
                            info.put("totalNodes", tree.get("totalNodes").asInt());
                        }
                    } catch (IOException e) { /* skip */ }
                }
                result.add(info);
            });
        } catch (IOException e) {
            log.debug("Failed to list endpoint outputs: {}", e.getMessage());
        }
        return result;
    }
}
