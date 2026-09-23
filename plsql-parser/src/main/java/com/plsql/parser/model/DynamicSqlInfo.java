package com.plsql.parser.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.*;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class DynamicSqlInfo {
    private String type;
    private String sqlExpression;
    private int line;
    private List<String> usingVariables = new ArrayList<>();

    /**
     * The PREPARE statement's name (e.g. MySQL's {@code PREPARE stmtName FROM ...}). Real gap
     * found via audit: dialects that capture this internally (MySQL's {@code
     * MySqlDynamicSqlInfo.stmtName}) used to drop it on conversion into this shared model, making
     * it impossible to pair a PREPARE with its later EXECUTE/DEALLOCATE once a procedure has more
     * than one prepared statement in flight at once.
     */
    private String stmtName;

    /** Variable name whose value could not be resolved at parse time (Option C flag). */
    private String unresolvedVarName;

    /** Hint pointing to the proc/function whose RETURN value feeds this EXECUTE IMMEDIATE (Option B). */
    private String crossProcSourceHint;

    /**
     * Inferred SQL operation type from the literal prefix of a concatenated dynamic SQL string.
     * Set when the literal prefix reveals the SQL verb (SELECT/INSERT/UPDATE/DELETE/MERGE/TRUNCATE)
     * but the table name itself is in a runtime variable — e.g. 'SELECT * FROM ' || v_table.
     * Values: "READ", "WRITE", "DDL".
     */
    private String inferredOperationType;

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getSqlExpression() { return sqlExpression; }
    public void setSqlExpression(String sqlExpression) { this.sqlExpression = sqlExpression; }
    public int getLine() { return line; }
    public void setLine(int line) { this.line = line; }
    public List<String> getUsingVariables() { return usingVariables; }
    public void setUsingVariables(List<String> usingVariables) { this.usingVariables = usingVariables; }
    public String getUnresolvedVarName() { return unresolvedVarName; }
    public void setUnresolvedVarName(String u) { this.unresolvedVarName = u; }
    public String getCrossProcSourceHint() { return crossProcSourceHint; }
    public void setCrossProcSourceHint(String h) { this.crossProcSourceHint = h; }
    public String getInferredOperationType() { return inferredOperationType; }
    public void setInferredOperationType(String t) { this.inferredOperationType = t; }
    public String getStmtName() { return stmtName; }
    public void setStmtName(String stmtName) { this.stmtName = stmtName; }
}
