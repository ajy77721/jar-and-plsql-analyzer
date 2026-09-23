package com.plsql.mysql.model;

/** A PREPARE or EXECUTE statement detected in a MySQL body. */
public class MySqlDynamicSqlInfo {

    public enum Kind { PREPARE, EXECUTE, DEALLOCATE }

    private Kind   kind;
    private String stmtName;     // the prepared-statement identifier
    private String sqlSource;    // for PREPARE: the @var name or literal SQL
    private int    line;

    /**
     * Variable name whose value could not be resolved at parse time — mirrors Oracle's
     * {@code DynamicSqlInfo.unresolvedVarName} (Bug 8): set when a PREPARE ... FROM @var's source
     * was traced back to a {@code CONCAT(...)} built from a runtime variable (e.g. a table name
     * passed in as a parameter), so no concrete table could be extracted.
     */
    private String unresolvedVarName;

    /**
     * Hint pointing to the proc/function whose value feeds this PREPARE, mirroring Oracle's
     * {@code DynamicSqlInfo.crossProcSourceHint} (Bug 8).
     */
    private String crossProcSourceHint;

    public Kind   getKind()           { return kind; }
    public void   setKind(Kind k)     { this.kind = k; }

    public String getStmtName()           { return stmtName; }
    public void   setStmtName(String n)   { this.stmtName = n; }

    public String getSqlSource()          { return sqlSource; }
    public void   setSqlSource(String s)  { this.sqlSource = s; }

    public int  getLine()      { return line; }
    public void setLine(int l) { this.line = l; }

    public String getUnresolvedVarName()        { return unresolvedVarName; }
    public void   setUnresolvedVarName(String u) { this.unresolvedVarName = u; }

    public String getCrossProcSourceHint()        { return crossProcSourceHint; }
    public void   setCrossProcSourceHint(String h) { this.crossProcSourceHint = h; }
}
