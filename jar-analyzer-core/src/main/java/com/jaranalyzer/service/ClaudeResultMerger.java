package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.model.CallNode;
import com.jaranalyzer.model.EndpointInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Merges Claude CLI analysis output back into endpoint models.
 * Handles JSON extraction from raw Claude responses and call tree enrichment.
 */
@Component
class ClaudeResultMerger {

    private static final Logger log = LoggerFactory.getLogger(ClaudeResultMerger.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    ClaudeResultMerger() {}

    /**
     * Merge Claude's analysis result into the endpoint.
     * Looks for JSON in the output and enriches the call tree with Claude's findings.
     * @return true if the merge was complete (all key fields present), false otherwise
     */
    boolean mergeClaudeResult(EndpointInfo ep, String rawOutput) {
        String epLabel = ep.getControllerSimpleName() + "." + ep.getMethodName();
        try {
            String json = extractJson(rawOutput);
            if (json == null) {
                log.warn("Incomplete merge for {}: no JSON found in Claude output", epLabel);
                return false;
            }

            JsonNode root = objectMapper.readTree(json);

            // Validate key fields
            List<String> missing = new ArrayList<>();
            if (!root.has("procName") || root.get("procName").isNull() || root.get("procName").asText().isBlank()) {
                missing.add("procName");
            }
            if (!root.has("collections") || root.get("collections").isNull() || root.get("collections").isEmpty()) {
                missing.add("collections");
            }
            if (!root.has("riskFlags") || root.get("riskFlags").isNull() || root.get("riskFlags").isEmpty()) {
                missing.add("riskFlags");
            }
            if (!missing.isEmpty()) {
                log.warn("Partial merge for {}: missing fields {}", epLabel, missing);
            }

            if (root.has("callTree")) {
                enrichCallTreeFromClaude(ep.getCallTree(), root.get("callTree"));
            }

            if (root.has("collections")) {
                JsonNode collsNode = root.get("collections");
                if (ep.getCallTree() != null) {
                    List<String> claudeCollections = new ArrayList<>();
                    collsNode.fieldNames().forEachRemaining(claudeCollections::add);
                    addMissingCollections(ep.getCallTree(), claudeCollections);
                }
            }

            // After enrichment, re-derive parent operation types from children
            if (ep.getCallTree() != null) {
                propagateOperationTypes(ep.getCallTree());
            }

            // Recompute endpoint-level aggregates
            ep.computeAggregates();

            boolean complete = missing.isEmpty();
            log.debug("Merged Claude result for {} (complete={})", epLabel, complete);
            return complete;
        } catch (Exception e) {
            log.warn("Failed to parse Claude output for {}: {}", epLabel, e.getMessage());
            return false;
        }
    }

    String extractJson(String text) {
        if (text == null) return null;
        text = text.trim();
        if (text.startsWith("{")) {
            return text;
        }
        // Try to find JSON block in markdown
        int start = text.indexOf("```json");
        if (start >= 0) {
            start = text.indexOf('\n', start) + 1;
            int end = text.indexOf("```", start);
            if (end > start) return text.substring(start, end).trim();
        }
        // Try to find any { ... } block
        start = text.indexOf('{');
        if (start >= 0) {
            int depth = 0;
            for (int i = start; i < text.length(); i++) {
                if (text.charAt(i) == '{') depth++;
                else if (text.charAt(i) == '}') {
                    depth--;
                    if (depth == 0) return text.substring(start, i + 1);
                }
            }
        }
        return null;
    }

    private void enrichCallTreeFromClaude(CallNode staticNode, JsonNode claudeTree) {
        if (staticNode == null || claudeTree == null) return;
        if (claudeTree.has("collections") && claudeTree.get("collections").isArray()
                && staticNode.getCollectionsAccessed() != null) {
            for (JsonNode coll : claudeTree.get("collections")) {
                String collName = coll.asText();
                if (!staticNode.getCollectionsAccessed().contains(collName)) {
                    staticNode.getCollectionsAccessed().add(collName);
                }
            }
        }
        if (claudeTree.has("children") && staticNode.getChildren() != null) {
            JsonNode claudeChildren = claudeTree.get("children");
            for (int i = 0; i < Math.min(staticNode.getChildren().size(), claudeChildren.size()); i++) {
                enrichCallTreeFromClaude(staticNode.getChildren().get(i), claudeChildren.get(i));
            }
        }
    }

    /**
     * Bottom-up propagation: nodes with no direct collections derive operationType from children.
     * Same logic as CorrectionMerger.propagateOperationTypes — keeps both paths consistent.
     */
    private void propagateOperationTypes(CallNode node) {
        if (node == null) return;
        if (node.getChildren() != null) {
            for (CallNode child : node.getChildren()) {
                propagateOperationTypes(child);
            }
        }

        boolean hasCollections = node.getCollectionsAccessed() != null
                && !node.getCollectionsAccessed().isEmpty();
        boolean isDbStereotype = "REPOSITORY".equals(node.getStereotype())
                || "SPRING_DATA".equals(node.getStereotype());
        boolean isLeaf = node.getChildren() == null || node.getChildren().isEmpty();

        // hasDbInteraction: method calls MongoTemplate but collection couldn't be resolved — op still valid.
        if (!hasCollections && !isDbStereotype && !node.isHasDbInteraction()) {
            if (isLeaf) {
                node.setOperationType(null);
            } else {
                String derived = deriveOpFromChildren(node);
                if (!Objects.equals(derived, node.getOperationType())) {
                    node.setOperationType(derived);
                }
            }
        }
    }

    private String deriveOpFromChildren(CallNode node) {
        if (node.getChildren() == null) return null;
        String best = null;
        for (CallNode child : node.getChildren()) {
            String op = child.getOperationType();
            if (op != null && opPriority(op) > opPriority(best)) best = op;
        }
        return best;
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

    private void addMissingCollections(CallNode node, List<String> claudeCollections) {
        if (node == null) return;
        String stereo = node.getStereotype();
        if (("REPOSITORY".equals(stereo) || "SPRING_DATA".equals(stereo))
                && node.getCollectionsAccessed() != null) {
            for (String coll : claudeCollections) {
                if (!node.getCollectionsAccessed().contains(coll)) {
                    String simpleName = node.getSimpleClassName();
                    if (simpleName != null) {
                        String stem = simpleName.replaceAll("(Repository|Repo|Impl)$", "").toLowerCase();
                        if (stem.length() >= 4 && coll.toLowerCase().contains(stem)) {
                            node.getCollectionsAccessed().add(coll);
                        }
                    }
                }
            }
        }
        if (node.getChildren() != null) {
            for (CallNode child : node.getChildren()) {
                addMissingCollections(child, claudeCollections);
            }
        }
    }
}
