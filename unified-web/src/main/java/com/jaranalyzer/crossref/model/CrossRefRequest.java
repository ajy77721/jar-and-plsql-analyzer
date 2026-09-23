package com.jaranalyzer.crossref.model;

/**
 * Input to the cross-reference link operation.
 * The user selects a JAR endpoint and manually enters the Oracle proc/package name.
 */
public class CrossRefRequest {

    /** JAR identifier (e.g. "extension-policy-0.0.1-SNAPSHOT"). */
    private String jarId;

    /** HTTP path of the endpoint (e.g. "/api/v1/process-motor-auto-policy"). */
    private String endpointPath;

    /** Oracle package or procedure name to map against (e.g. "PG_UWGE_CI"). */
    private String procName;

    /** Oracle schema (e.g. "CUSTOMER"). Optional — used to narrow PL/SQL session lookup. */
    private String schema;

    /**
     * Optional: index number of the endpoint directory (N in "N_ControllerClass.method").
     * When provided, resolves the exact call-tree directory without path scanning.
     */
    private Integer endpointIndex;

    public CrossRefRequest() {}

    public String getJarId() { return jarId; }
    public void setJarId(String jarId) { this.jarId = jarId; }
    public String getEndpointPath() { return endpointPath; }
    public void setEndpointPath(String endpointPath) { this.endpointPath = endpointPath; }
    public String getProcName() { return procName; }
    public void setProcName(String procName) { this.procName = procName; }
    public String getSchema() { return schema; }
    public void setSchema(String schema) { this.schema = schema; }
    public Integer getEndpointIndex() { return endpointIndex; }
    public void setEndpointIndex(Integer endpointIndex) { this.endpointIndex = endpointIndex; }
}
