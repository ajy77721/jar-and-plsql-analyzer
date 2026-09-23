package com.plsql.sqlserver.model;

/** A TRY/CATCH block (SQL Server's exception handler). */
public class SqlServerExceptionHandlerInfo {
    private String exceptionName = "CATCH";
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
