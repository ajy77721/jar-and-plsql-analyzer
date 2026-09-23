package com.plsqlanalyzer.parser.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class TableReference {
    private String tableName;
    private String schemaName;
    private String alias;
    private SqlOperationType operation;
    private int lineNumber;

    public TableReference() {}

    public TableReference(String tableName, String schemaName, String alias,
                          SqlOperationType operation, int lineNumber) {
        this.tableName = tableName;
        this.schemaName = schemaName;
        this.alias = alias;
        this.operation = operation;
        this.lineNumber = lineNumber;
    }

    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }

    public String getSchemaName() { return schemaName; }
    public void setSchemaName(String schemaName) { this.schemaName = schemaName; }

    public String getAlias() { return alias; }
    public void setAlias(String alias) { this.alias = alias; }

    public SqlOperationType getOperation() { return operation; }
    public void setOperation(SqlOperationType operation) { this.operation = operation; }

    public int getLineNumber() { return lineNumber; }
    public void setLineNumber(int lineNumber) { this.lineNumber = lineNumber; }

    @JsonIgnore
    public String getFullTableName() {
        return schemaName != null ? schemaName + "." + tableName : tableName;
    }
}
