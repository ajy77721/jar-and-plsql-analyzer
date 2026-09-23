package com.jaranalyzer.crossref.model;

/**
 * One parameter match entry: a Java string literal paired with its Oracle parameter equivalent.
 *
 * Example: Java literal "VEH_NO" → Oracle parameter "P_VEH_NO".
 */
public class CrossRefParamMatch {

    /** String literal found in Java bytecode (e.g. "VEH_NO", "CONTRACT_ID"). */
    private String javaLiteral;

    /** Expected Oracle parameter name (P_ prefix convention, e.g. "P_VEH_NO"). */
    private String oracleParam;

    /**
     * Whether this parameter was confirmed in the Oracle FlowNode's parameter list.
     * Null in STATIC_INFERENCE mode (no FlowNode available).
     */
    private Boolean matched;

    /** Oracle parameter mode when confirmed (IN / OUT / IN OUT). Null when not confirmed. */
    private String oracleMode;

    public CrossRefParamMatch() {}

    public CrossRefParamMatch(String javaLiteral, String oracleParam, Boolean matched) {
        this.javaLiteral = javaLiteral;
        this.oracleParam = oracleParam;
        this.matched = matched;
    }

    public String getJavaLiteral() { return javaLiteral; }
    public void setJavaLiteral(String javaLiteral) { this.javaLiteral = javaLiteral; }
    public String getOracleParam() { return oracleParam; }
    public void setOracleParam(String oracleParam) { this.oracleParam = oracleParam; }
    public Boolean getMatched() { return matched; }
    public void setMatched(Boolean matched) { this.matched = matched; }
    public String getOracleMode() { return oracleMode; }
    public void setOracleMode(String oracleMode) { this.oracleMode = oracleMode; }
}
