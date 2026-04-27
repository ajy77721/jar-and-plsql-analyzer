package com.jaranalyzer.service;

import com.jaranalyzer.model.CallNode;
import com.jaranalyzer.model.CorrectionResult;
import com.jaranalyzer.model.CorrectionResult.*;
import com.jaranalyzer.model.EndpointInfo;
import com.jaranalyzer.model.JarAnalysis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Applies Claude correction results back into the actual JarAnalysis data.
 * Walks each endpoint's call tree, matches nodes by ID, and mutates:
 * - collectionsAccessed (add missing, remove false positives)
 * - operationType (correct based on Claude's analysis)
 * - collectionDomains / collectionSources / collectionVerification
 */
@Component
public class CorrectionMerger {

    private static final Logger log = LoggerFactory.getLogger(CorrectionMerger.class);

    private final DomainConfigLoader domainConfig;

    public CorrectionMerger(DomainConfigLoader domainConfig) {
        this.domainConfig = domainConfig;
    }

    /**
     * Apply all corrections to the analysis. Returns total number of node-level changes applied.
     */
    public int applyCorrections(JarAnalysis analysis, Map<String, CorrectionResult> corrections) {
        int totalChanges = 0;
        for (EndpointInfo ep : analysis.getEndpoints()) {
            String key = ep.getControllerSimpleName() + "." + ep.getMethodName();
            CorrectionResult cr = corrections.get(key);
            if (cr != null) {
                totalChanges += applyToEndpoint(ep, cr);
            }
            // Recompute aggregated endpoint summaries after corrections
            ep.computeAggregates();
        }
        log.info("CorrectionMerger applied {} node-level changes across {} endpoints",
                totalChanges, corrections.size());
        return totalChanges;
    }

    /**
     * Apply a single correction result to one endpoint's call tree.
     * Finds ALL nodes matching each correction's nodeId (same method can appear
     * at multiple call sites) and applies the correction to every occurrence.
     */
    int applyToEndpoint(EndpointInfo ep, CorrectionResult correction) {
        if (correction.getCorrections() == null) return 0;
        CallNode root = ep.getCallTree();
        if (root == null) return 0;

        int changes = 0;
        for (NodeCorrection nc : correction.getCorrections()) {
            List<CallNode> matches = findNodesWithFallback(root, nc.getNodeId());
            if (matches.isEmpty()) {
                log.debug("Node not found in tree (all strategies exhausted): {}", nc.getNodeId());
                continue;
            }
            if (matches.size() > 1) {
                log.debug("Node {} matched {} nodes — applying correction to all", nc.getNodeId(), matches.size());
            }
            for (CallNode node : matches) {
                changes += applyNodeCorrection(node, nc);
            }
        }

        // After all node-level corrections, propagate operation types upward.
        // Parents with no direct collections should derive their op from children.
        changes += propagateOperationTypes(root);

        return changes;
    }

    /**
     * Bottom-up propagation: for nodes with no direct collections,
     * re-derive operationType from children's corrected operations.
     * This ensures that when Claude clears a child's WRITE, the parent
     * (which only had WRITE from method name matching) also gets cleared.
     */
    private int propagateOperationTypes(CallNode node) {
        if (node == null) return 0;
        int changes = 0;

        // Process children first (bottom-up)
        if (node.getChildren() != null) {
            for (CallNode child : node.getChildren()) {
                changes += propagateOperationTypes(child);
            }
        }

        boolean hasCollections = node.getCollectionsAccessed() != null
                && !node.getCollectionsAccessed().isEmpty();
        boolean isDbStereotype = "REPOSITORY".equals(node.getStereotype())
                || "SPRING_DATA".equals(node.getStereotype());
        boolean isLeaf = node.getChildren() == null || node.getChildren().isEmpty();

        // Re-derive for nodes without direct collections and not DB nodes.
        // hasDbInteraction: method calls MongoTemplate but collection couldn't be resolved — op is still valid.
        if (!hasCollections && !isDbStereotype && !node.isHasDbInteraction()) {
            if (isLeaf) {
                // Leaf with no collections → clear false positive operation type
                if (node.getOperationType() != null) {
                    log.debug("Propagation: {} leaf op {} -> null", node.getSimpleClassName() + "." + node.getMethodName(),
                            node.getOperationType());
                    node.setOperationType(null);
                    changes++;
                }
            } else {
                // Non-leaf → derive from children (covers orchestrating methods too)
                String derived = deriveOpFromChildren(node);
                if (!Objects.equals(derived, node.getOperationType())) {
                    log.debug("Propagation: {} op {} -> {}", node.getSimpleClassName() + "." + node.getMethodName(),
                            node.getOperationType(), derived);
                    node.setOperationType(derived);
                    changes++;
                }
            }
        }

        return changes;
    }

    private String deriveOpFromChildren(CallNode node) {
        if (node.getChildren() == null) return null;
        String best = null;
        for (CallNode child : node.getChildren()) {
            String op = child.getOperationType();
            if (op != null && opPriority(op) > opPriority(best)) {
                best = op;
            }
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

    /**
     * Find ALL nodes in the call tree matching the given ID.
     * Same method called from multiple parents creates duplicate nodes with identical IDs —
     * all occurrences need the same correction applied.
     */
    List<CallNode> findAllNodes(CallNode node, String nodeId) {
        List<CallNode> results = new ArrayList<>();
        findAllNodesRecursive(node, nodeId, results);
        return results;
    }

    private void findAllNodesRecursive(CallNode node, String nodeId, List<CallNode> results) {
        if (node == null || nodeId == null) return;
        if (nodeId.equals(node.getId())) {
            results.add(node);
        }
        if (node.getChildren() != null) {
            for (CallNode child : node.getChildren()) {
                findAllNodesRecursive(child, nodeId, results);
            }
        }
    }

    /**
     * Find nodes with fallback strategies to handle interface/implementation ID mismatches.
     * Strategy 1: Exact ID match (covers most cases)
     * Strategy 2: resolvedFrom match — correction targets interface, tree has implementation nodes
     * Strategy 3: Interface fallback — correction targets impl, tree has INTERFACE_FALLBACK node
     */
    List<CallNode> findNodesWithFallback(CallNode root, String nodeId) {
        // Strategy 1: exact match
        List<CallNode> exact = findAllNodes(root, nodeId);
        if (!exact.isEmpty()) return exact;

        // Parse nodeId into className and methodKey
        int hash = nodeId != null ? nodeId.indexOf('#') : -1;
        if (hash < 0) return List.of();
        String corrClassName = nodeId.substring(0, hash);
        String corrMethodKey = nodeId.substring(hash + 1);

        // Strategy 2: correction targets interface name, tree has impl with resolvedFrom == interface
        List<CallNode> byResolvedFrom = new ArrayList<>();
        findByResolvedFrom(root, corrClassName, corrMethodKey, byResolvedFrom);
        if (!byResolvedFrom.isEmpty()) {
            log.info("Fallback match: correction for {} matched {} impl node(s) via resolvedFrom",
                    nodeId, byResolvedFrom.size());
            return byResolvedFrom;
        }

        // Strategy 3: correction targets impl name, tree has INTERFACE_FALLBACK node with matching method
        List<CallNode> byFallback = new ArrayList<>();
        findInterfaceFallbackNodes(root, corrMethodKey, byFallback);
        if (!byFallback.isEmpty()) {
            log.info("Fallback match: correction for {} matched {} INTERFACE_FALLBACK node(s) via method signature",
                    nodeId, byFallback.size());
            return byFallback;
        }

        return List.of();
    }

    /** Find nodes whose resolvedFrom matches the correction's class name and whose method key matches. */
    private void findByResolvedFrom(CallNode node, String interfaceName, String methodKey, List<CallNode> results) {
        if (node == null) return;
        if (interfaceName.equals(node.getResolvedFrom())) {
            String nodeMethodKey = extractMethodKey(node.getId());
            if (methodKey.equals(nodeMethodKey)) {
                results.add(node);
            }
        }
        if (node.getChildren() != null) {
            for (CallNode child : node.getChildren()) {
                findByResolvedFrom(child, interfaceName, methodKey, results);
            }
        }
    }

    /** Find INTERFACE_FALLBACK nodes whose method key matches the correction's method key. */
    private void findInterfaceFallbackNodes(CallNode node, String methodKey, List<CallNode> results) {
        if (node == null) return;
        if ("INTERFACE_FALLBACK".equals(node.getDispatchType())) {
            String nodeMethodKey = extractMethodKey(node.getId());
            if (methodKey.equals(nodeMethodKey)) {
                results.add(node);
            }
        }
        if (node.getChildren() != null) {
            for (CallNode child : node.getChildren()) {
                findInterfaceFallbackNodes(child, methodKey, results);
            }
        }
    }

    /** Extract method key (everything after #) from a node ID like "com.Foo#bar(Ljava/lang/String;)V". */
    private String extractMethodKey(String nodeId) {
        if (nodeId == null) return "";
        int hash = nodeId.indexOf('#');
        return hash >= 0 ? nodeId.substring(hash + 1) : "";
    }

    /**
     * Apply corrections to a single node: collections + operation type.
     */
    private int applyNodeCorrection(CallNode node, NodeCorrection nc) {
        int changes = 0;

        // Fix operation type ("NONE" means no DB operation — clear it to null)
        if (nc.getOperationType() != null && !nc.getOperationType().isBlank()) {
            String correctedOp = "NONE".equalsIgnoreCase(nc.getOperationType()) ? null : nc.getOperationType();
            String old = node.getOperationType();
            if (!Objects.equals(correctedOp, old)) {
                node.setOperationType(correctedOp);
                changes++;
            }
        }

        CollectionCorrections cc = nc.getCollections();
        if (cc == null) return changes;

        List<String> collections = node.getCollectionsAccessed();
        if (collections == null) {
            collections = new ArrayList<>();
            node.setCollectionsAccessed(collections);
        }

        Map<String, String> domains = node.getCollectionDomains();
        if (domains == null) {
            domains = new LinkedHashMap<>();
            node.setCollectionDomains(domains);
        }

        Map<String, String> sources = node.getCollectionSources();
        if (sources == null) {
            sources = new LinkedHashMap<>();
            node.setCollectionSources(sources);
        }

        Map<String, String> verification = node.getCollectionVerification();
        if (verification == null) {
            verification = new LinkedHashMap<>();
            node.setCollectionVerification(verification);
        }

        // Remove false positives
        if (cc.getRemoved() != null) {
            for (String removed : cc.getRemoved()) {
                if (collections.remove(removed)) {
                    domains.remove(removed);
                    sources.remove(removed);
                    verification.put(removed, "CLAUDE_REMOVED");
                    changes++;
                }
            }
        }

        // Add missing collections
        if (cc.getAdded() != null) {
            for (CollectionEntry added : cc.getAdded()) {
                String name = added.getName();
                if (name == null) continue;
                if (!collections.contains(name)) {
                    collections.add(name);
                    sources.put(name, "CLAUDE");
                    domains.put(name, domainConfig.detectCollectionDomain(name));
                    verification.put(name, "CLAUDE_ADDED");
                    changes++;
                }
            }
        }

        // Mark verified collections
        if (cc.getVerified() != null) {
            for (String verified : cc.getVerified()) {
                if (collections.contains(verified)) {
                    verification.put(verified, "CLAUDE_VERIFIED");
                }
            }
        }

        return changes;
    }
}
