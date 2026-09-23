package com.plsql.db2.model;

/**
 * A single {@code NEXT VALUE FOR seq} / {@code NEXTVAL FOR seq} / {@code PREVVAL FOR seq}
 * sequence-reference usage found in an expression.
 */
public class Db2SequenceInfo {

    private String sequenceName;
    private String schema;
    private String operation; // NEXTVAL or PREVVAL
    private int    line;

    public String getSequenceName() { return sequenceName; }
    public void   setSequenceName(String sequenceName) { this.sequenceName = sequenceName; }
    public String getSchema() { return schema; }
    public void   setSchema(String schema) { this.schema = schema; }
    public String getOperation() { return operation; }
    public void   setOperation(String operation) { this.operation = operation; }
    public int    getLine() { return line; }
    public void   setLine(int line) { this.line = line; }
}
