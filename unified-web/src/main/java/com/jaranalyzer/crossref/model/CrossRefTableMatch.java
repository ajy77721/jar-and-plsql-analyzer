package com.jaranalyzer.crossref.model;

/**
 * One table/collection match entry within a proc link.
 * Represents a single Java MongoDB collection paired with its Oracle table equivalent.
 */
public class CrossRefTableMatch {

    /** MongoDB collection name on the Java side (e.g. "WIP_UWGE_RISK"). */
    private String mongoCollection;

    /** Oracle table name (e.g. "WIP_UWGE_RISK"). Usually identical — direct 1:1 naming. */
    private String oracleTable;

    /** Operation observed in Java (READ / WRITE / UPDATE / DELETE / AGGREGATE). */
    private String javaOperation;

    /** Operation observed in Oracle (SELECT / INSERT / UPDATE / DELETE / MERGE). Null in STATIC_INFERENCE mode. */
    private String oracleOperation;

    /**
     * Whether the operations are semantically equivalent.
     * WRITE/INSERT=true, READ/SELECT=true, UPDATE/UPDATE=true, DELETE/DELETE=true.
     * Null when oracleOperation is unknown (STATIC_INFERENCE).
     */
    private Boolean operationMatch;

    /**
     * Match status:
     *   MATCHED       — same table name, matching operation
     *   OP_MISMATCH   — same table name, different operation
     *   JAVA_ONLY     — collection present in Java, not found in Oracle FlowNode
     *   ORACLE_ONLY   — table present in Oracle FlowNode, not found in Java call tree
     *   INFERRED      — name match inferred (STATIC_INFERENCE mode, no PL/SQL FlowNode to confirm)
     */
    private String matchStatus;

    /** Oracle sequence name that feeds this table's primary key (e.g. "SQ_UWGE_RISK"). Null when unknown. */
    private String oracleSequence;

    public CrossRefTableMatch() {}

    public CrossRefTableMatch(String mongoCollection, String oracleTable,
                               String javaOperation, String matchStatus) {
        this.mongoCollection = mongoCollection;
        this.oracleTable = oracleTable;
        this.javaOperation = javaOperation;
        this.matchStatus = matchStatus;
    }

    public String getMongoCollection() { return mongoCollection; }
    public void setMongoCollection(String mongoCollection) { this.mongoCollection = mongoCollection; }
    public String getOracleTable() { return oracleTable; }
    public void setOracleTable(String oracleTable) { this.oracleTable = oracleTable; }
    public String getJavaOperation() { return javaOperation; }
    public void setJavaOperation(String javaOperation) { this.javaOperation = javaOperation; }
    public String getOracleOperation() { return oracleOperation; }
    public void setOracleOperation(String oracleOperation) { this.oracleOperation = oracleOperation; }
    public Boolean getOperationMatch() { return operationMatch; }
    public void setOperationMatch(Boolean operationMatch) { this.operationMatch = operationMatch; }
    public String getMatchStatus() { return matchStatus; }
    public void setMatchStatus(String matchStatus) { this.matchStatus = matchStatus; }
    public String getOracleSequence() { return oracleSequence; }
    public void setOracleSequence(String oracleSequence) { this.oracleSequence = oracleSequence; }
}
