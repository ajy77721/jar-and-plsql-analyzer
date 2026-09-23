package com.plsql.sqlserver.model;

public class SqlServerDynamicSqlInfo {

    /**
     * SET_SQL           — @var built as a string literal (source of a dynamic SQL string).
     * EXEC_STRING        — {@code EXEC (@var)} / {@code EXEC ('literal')}: plain string execution,
     *                       no parameterization, no query-plan reuse.
     * EXEC_SP_EXECUTESQL — {@code EXEC sp_executesql @sql, @params, ...}: parameterized, the
     *                       injection-safer of the two dynamic-SQL mechanisms.
     */
    public enum Kind { SET_SQL, EXEC_STRING, EXEC_SP_EXECUTESQL }

    private Kind   kind;
    private String sqlSource;
    private int    line;

    /**
     * Variable name whose value could not be resolved at parse time — mirrors
     * {@code com.plsql.parser.model.DynamicSqlInfo#unresolvedVarName} (Option C flag): set when a
     * dynamic-SQL statement (e.g. {@code EXEC(@sql)}) references a variable whose literal SQL text
     * this visitor has no prior record of (no matching {@code DECLARE @sql = '...'} / {@code SET
     * @sql = '...'} literal assignment was seen earlier in the same routine).
     */
    private String unresolvedVarName;

    /**
     * Hint pointing to the proc/function whose output feeds this dynamic SQL — mirrors
     * {@code com.plsql.parser.model.DynamicSqlInfo#crossProcSourceHint}. Not currently wired by the
     * SQL Server visitor (no package/proc source-map infrastructure exists in this dialect yet);
     * the field exists for model parity with Oracle's {@code DynamicSqlInfo}.
     */
    private String crossProcSourceHint;

    /**
     * The parameter-signature string literal (e.g. {@code N'@p1 INT'}) that follows the {@code
     * @sql} argument in {@code EXEC sp_executesql @sql, N'@p1 INT', @p1=5}. Only ever set on an
     * {@link Kind#EXEC_SP_EXECUTESQL} entry; null when no signature literal was present.
     */
    private String paramSignature;

    /**
     * The bound {@code name=value} pairs (e.g. {@code @p1 -> 5}) that follow the parameter
     * signature in an {@code EXEC sp_executesql} call, in argument order. Only ever populated on
     * an {@link Kind#EXEC_SP_EXECUTESQL} entry; empty when none were present/parseable.
     */
    private java.util.Map<String, String> boundParameters = new java.util.LinkedHashMap<>();

    public Kind   getKind()      { return kind; }
    public void   setKind(Kind kind)         { this.kind = kind; }
    public String getSqlSource() { return sqlSource; }
    public void   setSqlSource(String sqlSource) { this.sqlSource = sqlSource; }
    public int    getLine()      { return line; }
    public void   setLine(int line)          { this.line = line; }
    public String getUnresolvedVarName() { return unresolvedVarName; }
    public void   setUnresolvedVarName(String unresolvedVarName) { this.unresolvedVarName = unresolvedVarName; }
    public String getCrossProcSourceHint() { return crossProcSourceHint; }
    public void   setCrossProcSourceHint(String crossProcSourceHint) { this.crossProcSourceHint = crossProcSourceHint; }
    public String getParamSignature() { return paramSignature; }
    public void   setParamSignature(String paramSignature) { this.paramSignature = paramSignature; }
    public java.util.Map<String, String> getBoundParameters() { return boundParameters; }
    public void   setBoundParameters(java.util.Map<String, String> boundParameters) { this.boundParameters = boundParameters; }
}
