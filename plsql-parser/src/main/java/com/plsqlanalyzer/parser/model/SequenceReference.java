package com.plsqlanalyzer.parser.model;

/**
 * Represents a reference to an Oracle SEQUENCE (NEXTVAL or CURRVAL) in PL/SQL code.
 */
public class SequenceReference {
    private String sequenceName;
    private String schemaName;
    private String operation; // "NEXTVAL" or "CURRVAL"
    private int lineNumber;

    public SequenceReference() {}

    public SequenceReference(String sequenceName, String schemaName, String operation, int lineNumber) {
        this.sequenceName = sequenceName;
        this.schemaName = schemaName;
        this.operation = operation;
        this.lineNumber = lineNumber;
    }

    public String getSequenceName() { return sequenceName; }
    public void setSequenceName(String sequenceName) { this.sequenceName = sequenceName; }

    public String getSchemaName() { return schemaName; }
    public void setSchemaName(String schemaName) { this.schemaName = schemaName; }

    public String getOperation() { return operation; }
    public void setOperation(String operation) { this.operation = operation; }

    public int getLineNumber() { return lineNumber; }
    public void setLineNumber(int lineNumber) { this.lineNumber = lineNumber; }

    public String getFullName() {
        return schemaName != null ? schemaName + "." + sequenceName : sequenceName;
    }
}
