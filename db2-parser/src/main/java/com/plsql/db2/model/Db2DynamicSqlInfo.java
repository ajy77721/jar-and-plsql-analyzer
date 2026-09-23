package com.plsql.db2.model;

public class Db2DynamicSqlInfo {

    public enum Kind { PREPARE, EXECUTE, EXECUTE_IMMEDIATE }

    private Kind   kind;
    private String stmtName;
    private String sqlSource;
    private int    line;

    /**
     * Bug 8 (DB2 portion): mirrors Oracle's {@code DynamicSqlInfo.unresolvedVarName} (Option C
     * flag). Set when the dynamic SQL source is a variable whose value could be traced back to a
     * {@code SET var = '<literal>' || <runtime-expr> || ...} assignment, but the table name itself
     * fell inside the runtime-only (variable-interrupted) part of the concatenation, so it could
     * not be resolved at parse time. Holds the unresolved variable's name (e.g. {@code v_sql}).
     */
    private String unresolvedVarName;

    /**
     * Mirrors Oracle's {@code DynamicSqlInfo.crossProcSourceHint} (Option B). DB2 SQL PL has no
     * package/subprogram-source-map equivalent to Oracle's cross-procedure RETURN-value tracing,
     * so this is never populated today — kept only for field-shape parity with the shared model
     * family, and so a future cross-routine resolver has somewhere to write its hint.
     */
    private String crossProcSourceHint;

    public Kind   getKind()      { return kind; }
    public void   setKind(Kind kind)         { this.kind = kind; }
    public String getStmtName()  { return stmtName; }
    public void   setStmtName(String stmtName) { this.stmtName = stmtName; }
    public String getSqlSource() { return sqlSource; }
    public void   setSqlSource(String sqlSource) { this.sqlSource = sqlSource; }
    public int    getLine()      { return line; }
    public void   setLine(int line)          { this.line = line; }
    public String getUnresolvedVarName() { return unresolvedVarName; }
    public void   setUnresolvedVarName(String unresolvedVarName) { this.unresolvedVarName = unresolvedVarName; }
    public String getCrossProcSourceHint() { return crossProcSourceHint; }
    public void   setCrossProcSourceHint(String crossProcSourceHint) { this.crossProcSourceHint = crossProcSourceHint; }
}
