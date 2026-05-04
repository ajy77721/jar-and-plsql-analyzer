package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.model.CallNode;
import com.jaranalyzer.model.EndpointInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Intra-endpoint tree chunking for large call trees.
 * <p>
 * A single endpoint can traverse 1M+ lines across hundreds of classes.
 * When the serialized call tree exceeds MAX_CHUNK_CHARS, we break it into
 * branch-level chunks, each small enough for a Claude prompt.
 * <p>
 * Approach (same ACO/swarm philosophy applied inside the tree):
 *   1. SKELETON: root + immediate children (gives Claude the structural overview)
 *   2. BRANCHES: each major subtree becomes its own chunk
 *   3. RECURSIVE: branches still too large get split further (up to MAX_CHUNK_DEPTH)
 *   4. PHEROMONE ORDERING: branches with more unique dependencies processed first
 *   5. ASSEMBLY: all branch outputs merged back into the endpoint's call tree
 */
@Component
class TreeChunker {

    private static final Logger log = LoggerFactory.getLogger(TreeChunker.class);

    @Value("${claude.chunking.max-chunk-chars:50000}")
    int maxChunkChars;

    @Value("${claude.chunking.max-tree-nodes:500}")
    int maxTreeNodes;

    @Value("${claude.chunking.max-chunk-depth:3}")
    int maxChunkDepth;

    @Value("${claude.chunking.max-prompt-chars:180000}")
    int maxPromptChars;

    // Keep static references for backward compat with classes that read these directly
    static int MAX_CHUNK_CHARS = 50_000;
    static int MAX_TREE_NODES = 500;
    static int MAX_CHUNK_DEPTH = 3;
    static int MAX_PROMPT_CHARS = 180_000;

    private final ObjectMapper objectMapper = new ObjectMapper();

    TreeChunker() {}

    @jakarta.annotation.PostConstruct
    void syncStaticFields() {
        MAX_CHUNK_CHARS = maxChunkChars;
        MAX_TREE_NODES = maxTreeNodes;
        MAX_CHUNK_DEPTH = maxChunkDepth;
        MAX_PROMPT_CHARS = maxPromptChars;
    }

    /** Represents one chunk of a call tree for processing. */
    record TreeChunk(String type, String label, String context, int nodeCount) {}

    /**
     * Decide whether to chunk the call tree and produce the chunks.
     * Small trees -> single chunk. Large trees -> skeleton + branch chunks.
     */
    List<TreeChunk> chunkCallTree(EndpointInfo ep, String fullContext) {
        int nodeCount = countNodes(ep.getCallTree());

        // Small tree -- fits in one prompt
        if (fullContext.length() <= MAX_CHUNK_CHARS && nodeCount <= MAX_TREE_NODES) {
            return List.of(new TreeChunk("full", "complete", fullContext, nodeCount));
        }

        log.info("    Tree too large: {} chars, {} nodes -> chunking", fullContext.length(), nodeCount);

        CallNode root = ep.getCallTree();
        if (root == null || root.getChildren() == null || root.getChildren().isEmpty()) {
            // No children to split -- force single chunk even if large
            return List.of(new TreeChunk("full", "complete", fullContext, nodeCount));
        }

        List<TreeChunk> chunks = new ArrayList<>();

        // Chunk 0: Skeleton -- root with children as stubs (names only, no deep trees)
        String skeleton = buildSkeleton(ep, root);
        chunks.add(new TreeChunk("skeleton", "overview", skeleton, 1 + root.getChildren().size()));

        // One chunk per major branch, with recursive splitting if needed
        List<CallNode> branches = root.getChildren();
        for (int i = 0; i < branches.size(); i++) {
            CallNode branch = branches.get(i);
            splitBranch(ep, branch, i, 0, chunks);
        }

        // Deduplicate: same label (ClassName.method(params):return) can appear
        // multiple times when the same method is called from different branches.
        // Keep only the first occurrence (largest context) for each unique signature.
        if (chunks.size() > 2) {
            List<TreeChunk> skeleton_ = new ArrayList<>();
            List<TreeChunk> branchChunks = new ArrayList<>();
            for (TreeChunk c : chunks) {
                if ("skeleton".equals(c.type())) skeleton_.add(c);
                else branchChunks.add(c);
            }

            // Sort by node count desc so we keep the richest chunk when deduplicating
            branchChunks.sort((a, b) -> Integer.compare(b.nodeCount(), a.nodeCount()));

            // Deduplicate by label
            Set<String> seenLabels = new HashSet<>();
            List<TreeChunk> deduped = new ArrayList<>();
            int dupeCount = 0;
            for (TreeChunk c : branchChunks) {
                String key = c.label().replaceAll("\\s*\\[overview\\]$", ""); // normalize stub labels
                if (seenLabels.add(key)) {
                    deduped.add(c);
                } else {
                    dupeCount++;
                }
            }
            if (dupeCount > 0) {
                log.info("    Deduplicated {} duplicate branch chunks (kept {} unique)", dupeCount, deduped.size());
            }

            // Batch small branches: combine adjacent small chunks into one prompt
            List<TreeChunk> batched = batchSmallChunks(deduped);
            if (batched.size() < deduped.size()) {
                log.info("    Batched {} small chunks into {} combined chunks", deduped.size(), batched.size());
            }

            chunks.clear();
            chunks.addAll(skeleton_);
            chunks.addAll(batched);
        }

        return chunks;
    }

    /**
     * Combine adjacent small branch chunks into batched chunks.
     * If 5 branches each have 3KB of context, combine them into one 15KB chunk
     * instead of making 5 separate Claude calls.
     */
    List<TreeChunk> batchSmallChunks(List<TreeChunk> chunks) {
        List<TreeChunk> result = new ArrayList<>();
        int batchLimit = MAX_CHUNK_CHARS / 2; // leave room for prompt overhead

        StringBuilder batchContext = new StringBuilder();
        List<String> batchLabels = new ArrayList<>();
        int batchNodes = 0;

        for (TreeChunk c : chunks) {
            if (batchContext.length() + c.context().length() > batchLimit && batchContext.length() > 0) {
                // Flush current batch
                result.add(new TreeChunk("batch",
                        batchLabels.size() + " branches: " + String.join(" + ", batchLabels),
                        batchContext.toString(), batchNodes));
                batchContext.setLength(0);
                batchLabels.clear();
                batchNodes = 0;
            }

            // If this single chunk is already large, don't batch it
            if (c.context().length() > batchLimit) {
                if (batchContext.length() > 0) {
                    result.add(new TreeChunk("batch",
                            batchLabels.size() + " branches: " + String.join(" + ", batchLabels),
                            batchContext.toString(), batchNodes));
                    batchContext.setLength(0);
                    batchLabels.clear();
                    batchNodes = 0;
                }
                result.add(c);
            } else {
                if (batchContext.length() > 0) batchContext.append("\n---\n");
                batchContext.append("### Branch: ").append(c.label()).append("\n");
                batchContext.append(c.context());
                batchLabels.add(c.label());
                batchNodes += c.nodeCount();
            }
        }

        // Flush remaining batch
        if (batchContext.length() > 0) {
            if (batchLabels.size() == 1) {
                // Single item -- don't wrap in a batch
                result.add(new TreeChunk("branch", batchLabels.get(0),
                        batchContext.toString(), batchNodes));
            } else {
                result.add(new TreeChunk("batch",
                        batchLabels.size() + " branches: " + String.join(" + ", batchLabels),
                        batchContext.toString(), batchNodes));
            }
        }

        return result;
    }

    /**
     * Recursively split a branch into chunks if it's too large.
     */
    void splitBranch(EndpointInfo ep, CallNode branch, int branchIndex,
                     int depth, List<TreeChunk> chunks) {
        String branchLabel = branchLabel(branch);
        String branchContext = buildBranchContext(ep, branch, branchIndex);
        int branchNodes = countNodes(branch);

        if (branchContext.length() <= MAX_CHUNK_CHARS || depth >= MAX_CHUNK_DEPTH
                || branch.getChildren() == null || branch.getChildren().isEmpty()) {
            chunks.add(new TreeChunk("branch", branchLabel, branchContext, branchNodes));
            return;
        }

        // Branch too large and has children -- split into sub-branches
        log.info("      Branch {} too large ({} chars, {} nodes) -> splitting",
                branchLabel, branchContext.length(), branchNodes);

        // Add this branch as a stub (header without deep children)
        String branchStub = buildBranchStub(ep, branch, branchIndex);
        List<CallNode> subBranches = branch.getChildren() != null ? branch.getChildren() : List.of();
        chunks.add(new TreeChunk("branch-stub", branchLabel + " [overview]", branchStub,
                1 + subBranches.size()));

        // Recurse into each sub-branch
        for (int i = 0; i < subBranches.size(); i++) {
            splitBranch(ep, subBranches.get(i), i, depth + 1, chunks);
        }
    }

    /**
     * Build skeleton: root node + immediate children as stubs (name, stereotype, collections only).
     * Gives Claude the structural overview before diving into branches.
     */
    String buildSkeleton(EndpointInfo ep, CallNode root) {
        try {
            List<CallNode> rootChildren = root.getChildren() != null ? root.getChildren() : List.of();
            List<Map<String, Object>> childStubs = new ArrayList<>();
            for (int i = 0; i < rootChildren.size(); i++) {
                CallNode child = rootChildren.get(i);
                Map<String, Object> stub = new LinkedHashMap<>();
                stub.put("branchIndex", i);
                stub.put("className", child.getClassName());
                stub.put("simpleClassName", child.getSimpleClassName());
                stub.put("methodName", child.getMethodName());
                stub.put("parameterTypes", child.getParameterTypes());
                stub.put("returnType", child.getReturnType());
                stub.put("signature", nodeSignature(child));
                stub.put("stereotype", child.getStereotype());
                stub.put("collectionsAccessed", child.getCollectionsAccessed());
                stub.put("domain", child.getDomain());
                stub.put("childCount", child.getChildren() != null ? child.getChildren().size() : 0);
                stub.put("totalNodes", countNodes(child));
                childStubs.add(stub);
            }

            Map<String, Object> skel = new LinkedHashMap<>();
            skel.put("type", "skeleton");
            skel.put("controller", ep.getControllerSimpleName());
            skel.put("method", ep.getMethodName());
            skel.put("httpMethod", ep.getHttpMethod() != null ? ep.getHttpMethod() : "");
            skel.put("path", ep.getFullPath() != null ? ep.getFullPath() : "");
            skel.put("totalBranches", rootChildren.size());
            skel.put("rootNode", Map.of(
                    "className", root.getClassName() != null ? root.getClassName() : "",
                    "methodName", root.getMethodName() != null ? root.getMethodName() : "",
                    "stereotype", root.getStereotype() != null ? root.getStereotype() : "",
                    "annotations", root.getAnnotations()
            ));
            skel.put("branches", childStubs);

            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(skel);
        } catch (Exception e) {
            return "{}";
        }
    }

    /**
     * Build context for a single branch of the call tree.
     * Includes parent endpoint info + the full branch subtree.
     */
    String buildBranchContext(EndpointInfo ep, CallNode branch, int branchIndex) {
        try {
            Map<String, Object> ctx = new LinkedHashMap<>();
            ctx.put("type", "branch");
            ctx.put("controller", ep.getControllerSimpleName());
            ctx.put("method", ep.getMethodName());
            ctx.put("branchIndex", branchIndex);
            ctx.put("branchRoot", branchLabel(branch));
            ctx.put("branchSignature", nodeSignature(branch));
            ctx.put("parameterTypes", branch.getParameterTypes());
            ctx.put("returnType", branch.getReturnType());
            ctx.put("callTree", branch);
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(ctx);
        } catch (Exception e) {
            return "{}";
        }
    }

    /**
     * Build stub for a branch -- header info + child stubs, no deep tree.
     * Used when a branch itself needs recursive splitting.
     */
    String buildBranchStub(EndpointInfo ep, CallNode branch, int branchIndex) {
        try {
            List<Map<String, Object>> childStubs = new ArrayList<>();
            for (CallNode child : branch.getChildren() != null ? branch.getChildren() : List.<CallNode>of()) {
                Map<String, Object> cs = new LinkedHashMap<>();
                cs.put("signature", nodeSignature(child));
                cs.put("className", child.getClassName() != null ? child.getClassName() : "");
                cs.put("methodName", child.getMethodName() != null ? child.getMethodName() : "");
                cs.put("parameterTypes", child.getParameterTypes());
                cs.put("returnType", child.getReturnType() != null ? child.getReturnType() : "");
                cs.put("stereotype", child.getStereotype() != null ? child.getStereotype() : "");
                cs.put("nodes", countNodes(child));
                childStubs.add(cs);
            }
            Map<String, Object> stub = new LinkedHashMap<>();
            stub.put("type", "branch-stub");
            stub.put("controller", ep.getControllerSimpleName());
            stub.put("method", ep.getMethodName());
            stub.put("branchIndex", branchIndex);
            stub.put("branchRoot", branchLabel(branch));
            stub.put("branchSignature", nodeSignature(branch));
            stub.put("stereotype", branch.getStereotype());
            stub.put("collections", branch.getCollectionsAccessed());
            stub.put("subBranches", childStubs);
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(stub);
        } catch (Exception e) {
            return "{}";
        }
    }

    /**
     * Build the prompt for a single chunk -- varies by chunk type.
     */
    String buildChunkPrompt(EndpointInfo ep, TreeChunk chunk, int chunkIndex,
                            int totalChunks, String clusterContext) {
        PromptTemplates.DbTechnology tech = PromptTemplates.detectTechnology(ep);
        String result = PromptTemplates.buildChunkAnalysisPrompt(
                ep, chunk.context(), chunk.type(), chunk.label(),
                chunkIndex, totalChunks, clusterContext, tech);

        if (result.length() > MAX_PROMPT_CHARS) {
            log.warn("Prompt exceeds {}KB limit ({} chars) for chunk '{}' — truncating",
                    MAX_PROMPT_CHARS / 1000, result.length(), chunk.label());
            result = result.substring(0, MAX_PROMPT_CHARS - 200)
                    + "\n\n[TRUNCATED — prompt exceeded " + MAX_PROMPT_CHARS + " char limit]";
        } else {
            log.debug("Prompt size: {} chars for chunk '{}'", result.length(), chunk.label());
        }
        return result;
    }

    /**
     * Write chunk plan for a single endpoint's tree decomposition.
     */
    void writeChunkPlan(Path workDir, String fragmentName, List<TreeChunk> chunks,
                        FragmentStore fragmentStore) {
        try {
            List<Map<String, Object>> plan = new ArrayList<>();
            for (int i = 0; i < chunks.size(); i++) {
                TreeChunk c = chunks.get(i);
                plan.add(Map.of(
                        "chunk", i,
                        "type", c.type(),
                        "label", c.label(),
                        "chars", c.context().length(),
                        "nodes", c.nodeCount()
                ));
            }
            fragmentStore.writeFragment(workDir, fragmentName + "_chunk_plan.json",
                    objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(Map.of(
                            "endpoint", fragmentName,
                            "totalChunks", chunks.size(),
                            "chunks", plan
                    )));
        } catch (IOException e) {
            log.debug("Failed to write chunk plan: {}", e.getMessage());
        }
    }

    /** Count total nodes in a call tree. */
    int countNodes(CallNode node) {
        if (node == null) return 0;
        int count = 1;
        if (node.getChildren() != null) {
            for (CallNode child : node.getChildren()) {
                count += countNodes(child);
            }
        }
        return count;
    }

    /**
     * Full unique signature for a CallNode: ClassName.methodName(ParamTypes):ReturnType
     * Handles all edge cases: overloading (same name, different params),
     * covariant return (same name+params, different return type),
     * bridge methods, method hiding, overriding across classes.
     */
    String nodeSignature(CallNode node) {
        String cls = node.getClassName() != null ? node.getClassName() : "?";
        String method = node.getMethodName() != null ? node.getMethodName() : "?";
        String ret = node.getReturnType() != null ? node.getReturnType() : "void";
        // Shorten param types for readability
        List<String> params = node.getParameterTypes();
        String paramSig = "";
        if (params != null && !params.isEmpty()) {
            paramSig = params.stream()
                    .map(p -> p.contains(".") ? p.substring(p.lastIndexOf('.') + 1) : p)
                    .collect(Collectors.joining(","));
        }
        String shortRet = ret.contains(".") ? ret.substring(ret.lastIndexOf('.') + 1) : ret;
        return cls + "." + method + "(" + paramSig + "):" + shortRet;
    }

    /**
     * Display label for a branch node -- uses full className for uniqueness.
     */
    String branchLabel(CallNode node) {
        return nodeSignature(node);
    }
}
