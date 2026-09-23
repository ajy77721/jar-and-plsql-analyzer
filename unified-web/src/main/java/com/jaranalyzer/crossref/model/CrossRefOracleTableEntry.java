package com.jaranalyzer.crossref.model;

/** A single Oracle table operation from a PL/SQL FlowNode (FULL_COMPARISON mode only). */
public class CrossRefOracleTableEntry {

    private String tableName;
    private String schema;
    private String operation;  // SELECT / INSERT / UPDATE / DELETE / MERGE
    private Integer sourceLine;

    public CrossRefOracleTableEntry() {}

    public CrossRefOracleTableEntry(String tableName, String schema, String operation) {
        this.tableName = tableName;
        this.schema = schema;
        this.operation = operation;
    }

    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }
    public String getSchema() { return schema; }
    public void setSchema(String schema) { this.schema = schema; }
    public String getOperation() { return operation; }
    public void setOperation(String operation) { this.operation = operation; }
    public Integer getSourceLine() { return sourceLine; }
    public void setSourceLine(Integer sourceLine) { this.sourceLine = sourceLine; }
}
