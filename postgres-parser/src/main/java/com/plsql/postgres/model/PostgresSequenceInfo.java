package com.plsql.postgres.model;

/**
 * A sequence usage detected as a Postgres function-call-style invocation:
 * {@code nextval('seq')}, {@code currval('seq')}, {@code setval('seq', n)}, {@code lastval()}.
 *
 * <p>Unlike Oracle's {@code seq.NEXTVAL} dot-syntax or SQL Server's {@code NEXT VALUE FOR seq},
 * Postgres calls these as ordinary functions taking the (schema-qualified) sequence name as a
 * string-literal argument (lastval() takes no argument).
 */
public class PostgresSequenceInfo {

    public enum Operation { NEXTVAL, CURRVAL, SETVAL, LASTVAL }

    private String sequenceName;
    private Operation operation;
    private int line;

    public String getSequenceName() { return sequenceName; }
    public void setSequenceName(String v) { this.sequenceName = v; }
    public Operation getOperation() { return operation; }
    public void setOperation(Operation v) { this.operation = v; }
    public int getLine() { return line; }
    public void setLine(int v) { this.line = v; }
}
