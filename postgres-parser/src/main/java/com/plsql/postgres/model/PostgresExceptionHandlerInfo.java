package com.plsql.postgres.model;

/** A PL/pgSQL {@code EXCEPTION WHEN cond [OR cond]* THEN ...} handler. */
public class PostgresExceptionHandlerInfo {
    private String exceptionName;
    private int line;
    private int lineEnd;
    private int statementsCount;

    public String getExceptionName() { return exceptionName; }
    public void setExceptionName(String v) { this.exceptionName = v; }
    public int getLine() { return line; }
    public void setLine(int v) { this.line = v; }
    public int getLineEnd() { return lineEnd; }
    public void setLineEnd(int v) { this.lineEnd = v; }
    public int getStatementsCount() { return statementsCount; }
    public void setStatementsCount(int v) { this.statementsCount = v; }
}
