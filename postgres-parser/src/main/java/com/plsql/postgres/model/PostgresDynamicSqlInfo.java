package com.plsql.postgres.model;

import java.util.ArrayList;
import java.util.List;

public class PostgresDynamicSqlInfo {

    public enum Kind { PREPARE, EXECUTE, EXECUTE_IMMEDIATE }

    private Kind   kind;
    private String stmtName;
    private String sqlSource;
    private int    line;
    /** Source text of each USING-bound expression, in order. */
    private final List<String> usingExprs = new ArrayList<>();

    /**
     * Bug 3 (§dynamic-cursor-audit): name of a variable feeding a {@code format(...)}
     * placeholder (e.g. {@code %I}) whose value cannot be resolved at parse time — mirrors
     * plsql-parser's {@code DynamicSqlInfo.unresolvedVarName} (Option C) structurally, so a
     * table name is never fabricated from an unknown runtime value.
     */
    private String unresolvedVarName;

    public Kind   getKind()      { return kind; }
    public void   setKind(Kind kind)         { this.kind = kind; }
    public String getStmtName()  { return stmtName; }
    public void   setStmtName(String stmtName) { this.stmtName = stmtName; }
    public String getSqlSource() { return sqlSource; }
    public void   setSqlSource(String sqlSource) { this.sqlSource = sqlSource; }
    public int    getLine()      { return line; }
    public void   setLine(int line)          { this.line = line; }
    public List<String> getUsingExprs() { return usingExprs; }
    public String getUnresolvedVarName() { return unresolvedVarName; }
    public void   setUnresolvedVarName(String unresolvedVarName) { this.unresolvedVarName = unresolvedVarName; }
}
