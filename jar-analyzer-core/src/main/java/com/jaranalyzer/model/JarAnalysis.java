package com.jaranalyzer.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;

// callTreeNodes must precede classes/endpoints: every reader of this document is a forward-only
// streaming pass, so the shared node dictionary has to arrive before the endpoints referencing it.
@com.fasterxml.jackson.annotation.JsonPropertyOrder({
    "jarName", "projectName", "jarSize", "analyzedAt", "totalClasses", "totalEndpoints",
    "analysisMode", "basePackage", "correctionAppliedAt", "correctionCount", "claudeIteration",
    "callTreeFormat", "callTreeNodes", "invocationPool", "endpointPool",
    "advice", "classes", "endpoints"})
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JarAnalysis {
    private String jarName;
    private String projectName;
    private long jarSize;
    private String analyzedAt;
    private int totalClasses;
    private int totalEndpoints;
    private String analysisMode;         // "STATIC" or "CORRECTED"
    private String basePackage;          // filter applied at upload time (null = no filter)
    private String correctionAppliedAt;  // ISO timestamp of last correction merge
    private int correctionCount;         // number of corrections applied
    private int claudeIteration;         // how many times full Claude scan ran
    /**
     * "dag-v1" when call trees are stored as the shared node dictionary below rather than nested
     * under each endpoint; null on documents written before that format existed.
     */
    private String callTreeFormat;

    /**
     * Shared, content-addressed call-tree nodes: {@code nodeId -> node fields + childIds}. Held as
     * a raw tree because nothing in this model needs to interpret it — {@code PersistenceService}
     * expands it into {@link EndpointInfo#getCallTree()} on load and rebuilds it on save, so every
     * consumer of this POJO still sees ordinary nested {@code CallNode} trees. Transient in
     * practice: null on a loaded, rehydrated instance.
     */
    private com.fasterxml.jackson.databind.JsonNode callTreeNodes;

    /**
     * Shared pool of distinct method invocations, referenced by ordinal from
     * {@code methods[].invocationRefs}. Same rationale and lifecycle as {@link #callTreeNodes}:
     * held raw because nothing in this model interprets it, resolved by
     * {@code PersistenceService.rehydrate} on load, and null on a rehydrated instance. Absent on
     * documents written before pooling existed, whose invocations are inline.
     */
    private com.fasterxml.jackson.databind.JsonNode invocationPool;

    /**
     * Shared pools for the duplicated per-endpoint arrays, referenced by ordinal from
     * {@code endpoints[].*Refs}. Same lifecycle as {@link #invocationPool}: raw, resolved by
     * {@code PersistenceService.rehydrate}, null on a rehydrated instance, absent on documents
     * written before pooling existed.
     */
    private com.fasterxml.jackson.databind.JsonNode endpointPool;

    /**
     * Cross-cutting behaviour — aspects, interceptors, filters — that no call tree can contain,
     * because interception happens through a proxy and leaves no static call edge. Ordered before
     * {@code classes} so a streaming reader has it before walking them.
     *
     * <p>Empty rather than null on a JAR with no advice, and omitted from the document entirely in
     * that case ({@code NON_EMPTY}), so existing consumers see no new field. See {@link AdviceInfo}.
     */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private List<AdviceInfo> advice = new ArrayList<>();

    private List<ClassInfo> classes = new ArrayList<>();
    private List<EndpointInfo> endpoints = new ArrayList<>();

    public JarAnalysis() {}

    public List<AdviceInfo> getAdvice() { return advice; }
    public void setAdvice(List<AdviceInfo> advice) { this.advice = advice; }

    public String getCallTreeFormat() { return callTreeFormat; }
    public void setCallTreeFormat(String callTreeFormat) { this.callTreeFormat = callTreeFormat; }
    public com.fasterxml.jackson.databind.JsonNode getEndpointPool() { return endpointPool; }
    public void setEndpointPool(com.fasterxml.jackson.databind.JsonNode endpointPool) {
        this.endpointPool = endpointPool;
    }
    public com.fasterxml.jackson.databind.JsonNode getInvocationPool() { return invocationPool; }
    public void setInvocationPool(com.fasterxml.jackson.databind.JsonNode invocationPool) {
        this.invocationPool = invocationPool;
    }
    // MID-103 — @JsonInclude(ALWAYS) overrides the class default (NON_NULL) for this one property:
    // writeFlattened's streaming path leaves the field null on the POJO (there is no longer a
    // materialised dictionary to hold) and relies on StreamableNodeSerializer firing anyway. Every
    // other caller of this getter/setter (load/rehydrate, any test constructing one directly) is
    // unaffected — the field was already always non-null in every other real write path before
    // this row, so this override changes no other caller's output bytes.
    @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.ALWAYS)
    @com.fasterxml.jackson.databind.annotation.JsonSerialize(
            using = com.jaranalyzer.model.StreamableNodeSerializer.class,
            nullsUsing = com.jaranalyzer.model.StreamableNodeSerializer.class)
    public com.fasterxml.jackson.databind.JsonNode getCallTreeNodes() { return callTreeNodes; }
    public void setCallTreeNodes(com.fasterxml.jackson.databind.JsonNode callTreeNodes) {
        this.callTreeNodes = callTreeNodes;
    }

    public String getJarName() { return jarName; }
    public void setJarName(String jarName) { this.jarName = jarName; }
    public String getProjectName() { return projectName; }
    public void setProjectName(String projectName) { this.projectName = projectName; }
    public long getJarSize() { return jarSize; }
    public void setJarSize(long jarSize) { this.jarSize = jarSize; }
    public String getAnalyzedAt() { return analyzedAt; }
    public void setAnalyzedAt(String analyzedAt) { this.analyzedAt = analyzedAt; }
    public int getTotalClasses() { return totalClasses; }
    public void setTotalClasses(int totalClasses) { this.totalClasses = totalClasses; }
    public int getTotalEndpoints() { return totalEndpoints; }
    public void setTotalEndpoints(int totalEndpoints) { this.totalEndpoints = totalEndpoints; }
    public String getAnalysisMode() { return analysisMode; }
    public void setAnalysisMode(String analysisMode) { this.analysisMode = analysisMode; }
    public String getBasePackage() { return basePackage; }
    public void setBasePackage(String basePackage) { this.basePackage = basePackage; }
    public String getCorrectionAppliedAt() { return correctionAppliedAt; }
    public void setCorrectionAppliedAt(String correctionAppliedAt) { this.correctionAppliedAt = correctionAppliedAt; }
    public int getCorrectionCount() { return correctionCount; }
    public void setCorrectionCount(int correctionCount) { this.correctionCount = correctionCount; }
    public int getClaudeIteration() { return claudeIteration; }
    public void setClaudeIteration(int claudeIteration) { this.claudeIteration = claudeIteration; }
    public List<ClassInfo> getClasses() { return classes; }
    public void setClasses(List<ClassInfo> classes) { this.classes = classes; }
    public List<EndpointInfo> getEndpoints() { return endpoints; }
    public void setEndpoints(List<EndpointInfo> endpoints) { this.endpoints = endpoints; }
}
