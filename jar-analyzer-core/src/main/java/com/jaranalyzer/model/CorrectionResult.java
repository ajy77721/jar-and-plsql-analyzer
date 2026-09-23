package com.jaranalyzer.model;

import java.util.List;
import java.util.Map;

/**
 * Structured result of Claude's collection/operation correction for one endpoint.
 * Each correction references a specific nodeId from the call tree.
 */
public class CorrectionResult {

    private String endpointName;
    private List<NodeCorrection> corrections;
    private EndpointSummary endpointSummary;

    public CorrectionResult() {}

    public String getEndpointName() { return endpointName; }
    public void setEndpointName(String endpointName) { this.endpointName = endpointName; }
    public List<NodeCorrection> getCorrections() { return corrections; }
    public void setCorrections(List<NodeCorrection> corrections) { this.corrections = corrections; }
    public EndpointSummary getEndpointSummary() { return endpointSummary; }
    public void setEndpointSummary(EndpointSummary endpointSummary) { this.endpointSummary = endpointSummary; }

    /** Correction for a single call tree node. */
    public static class NodeCorrection {
        private String nodeId;
        private CollectionCorrections collections;
        private String operationType;
        private String operationTypeReason;

        public NodeCorrection() {}

        public String getNodeId() { return nodeId; }
        public void setNodeId(String nodeId) { this.nodeId = nodeId; }
        public CollectionCorrections getCollections() { return collections; }
        public void setCollections(CollectionCorrections collections) { this.collections = collections; }
        public String getOperationType() { return operationType; }
        public void setOperationType(String operationType) { this.operationType = operationType; }
        public String getOperationTypeReason() { return operationTypeReason; }
        public void setOperationTypeReason(String operationTypeReason) { this.operationTypeReason = operationTypeReason; }
    }

    /** Collection-level corrections: what was added, removed, or verified. */
    public static class CollectionCorrections {
        private List<CollectionEntry> added;
        private List<String> removed;
        private List<String> verified;

        public CollectionCorrections() {}

        public List<CollectionEntry> getAdded() { return added; }
        public void setAdded(List<CollectionEntry> added) { this.added = added; }
        public List<String> getRemoved() { return removed; }
        public void setRemoved(List<String> removed) { this.removed = removed; }
        public List<String> getVerified() { return verified; }
        public void setVerified(List<String> verified) { this.verified = verified; }
    }

    /** A collection entry detected by Claude that static analysis missed. */
    public static class CollectionEntry {
        private String name;
        private String source;
        private String operation;

        public CollectionEntry() {}

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getSource() { return source; }
        public void setSource(String source) { this.source = source; }
        public String getOperation() { return operation; }
        public void setOperation(String operation) { this.operation = operation; }
    }

    /** Endpoint-level summary from Claude's correction analysis. */
    public static class EndpointSummary {
        private List<String> allCollections;
        private String primaryOperation;
        private List<String> crossModuleCalls;
        private double confidence;

        public EndpointSummary() {}

        public List<String> getAllCollections() { return allCollections; }
        public void setAllCollections(List<String> allCollections) { this.allCollections = allCollections; }
        public String getPrimaryOperation() { return primaryOperation; }
        public void setPrimaryOperation(String primaryOperation) { this.primaryOperation = primaryOperation; }
        public List<String> getCrossModuleCalls() { return crossModuleCalls; }
        public void setCrossModuleCalls(List<String> crossModuleCalls) { this.crossModuleCalls = crossModuleCalls; }
        public double getConfidence() { return confidence; }
        public void setConfidence(double confidence) { this.confidence = confidence; }
    }
}
