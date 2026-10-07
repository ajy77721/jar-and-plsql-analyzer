package com.plsql.parser.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.*;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class StatementInfo {
    private String type;
    private int line;
    private int lineEnd;
    private String sqlText;
    private List<String> tables = new ArrayList<>();
    private int nestingDepth;
    private String indexVariable;
    private String collectionName;
    private String boundsExpression;
    private String limitValue;
    private String dmlType;
    private Integer errorCode;
    private String errorMessage;
    private String savepointName;
    /**
     * Which subprogram within a package this statement originated from, set ONLY by
     * {@code DependencyCrawler}'s package-as-entry-point aggregation (see
     * {@link CallInfo#getSubprogramName()} for the full rationale). Null for every other crawl.
     */
    private String subprogramName;

    /**
     * Innermost branch/loop guard this statement sits inside (IF / CASE / LOOP), for the
     * transaction-scope enrichment. Null when the statement is not inside a guard. {@code line} is
     * the guard's own start line and {@code condition} is the (collapsed, truncated) guard text.
     */
    private String guardKind;
    private Integer guardLine;
    private String guardCondition;
    /**
     * Enclosing exception handler body (its exception name, e.g. OTHERS / ZERO_DIVIDE, and the
     * handler's start line). Null when the statement is not inside a handler body.
     */
    private String handlerName;
    private Integer handlerLine;
    /**
     * True only for a SELECT that locks its rows ({@code SELECT ... FOR UPDATE [OF ...] [WAIT n |
     * NOWAIT | SKIP LOCKED]}). Set to {@code true} only — never to {@code false} — so the
     * {@code @JsonInclude(NON_EMPTY)} serialization omits it (null) for every other statement.
     */
    private Boolean forUpdate;
    /**
     * SQL Server only: the routine-level transaction nesting depth AT this statement
     * (BEGIN TRANSACTION emits the depth AFTER increment; COMMIT/ROLLBACK BEFORE decrement —
     * the visitor's own counter convention). Null for every other dialect / no active nesting,
     * so it flows through the multidb statement copy and the writer's txn rows untouched.
     */
    private Integer transactionDepth;

    public String getSubprogramName() { return subprogramName; }
    public void setSubprogramName(String subprogramName) { this.subprogramName = subprogramName; }

    public String getGuardKind() { return guardKind; }
    public void setGuardKind(String guardKind) { this.guardKind = guardKind; }
    public Integer getGuardLine() { return guardLine; }
    public void setGuardLine(Integer guardLine) { this.guardLine = guardLine; }
    public String getGuardCondition() { return guardCondition; }
    public void setGuardCondition(String guardCondition) { this.guardCondition = guardCondition; }
    public String getHandlerName() { return handlerName; }
    public void setHandlerName(String handlerName) { this.handlerName = handlerName; }
    public Integer getHandlerLine() { return handlerLine; }
    public void setHandlerLine(Integer handlerLine) { this.handlerLine = handlerLine; }
    public Boolean getForUpdate() { return forUpdate; }
    public void setForUpdate(Boolean forUpdate) { this.forUpdate = forUpdate; }
    public Integer getTransactionDepth() { return transactionDepth; }
    public void setTransactionDepth(Integer transactionDepth) { this.transactionDepth = transactionDepth; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public int getLine() { return line; }
    public void setLine(int line) { this.line = line; }
    public int getLineEnd() { return lineEnd; }
    public void setLineEnd(int lineEnd) { this.lineEnd = lineEnd; }
    public String getSqlText() { return sqlText; }
    public void setSqlText(String sqlText) { this.sqlText = sqlText; }
    public List<String> getTables() { return tables; }
    public void setTables(List<String> tables) { this.tables = tables; }
    public int getNestingDepth() { return nestingDepth; }
    public void setNestingDepth(int nestingDepth) { this.nestingDepth = nestingDepth; }
    public String getIndexVariable() { return indexVariable; }
    public void setIndexVariable(String indexVariable) { this.indexVariable = indexVariable; }
    public String getCollectionName() { return collectionName; }
    public void setCollectionName(String collectionName) { this.collectionName = collectionName; }
    public String getBoundsExpression() { return boundsExpression; }
    public void setBoundsExpression(String boundsExpression) { this.boundsExpression = boundsExpression; }
    public String getLimitValue() { return limitValue; }
    public void setLimitValue(String limitValue) { this.limitValue = limitValue; }
    public String getDmlType() { return dmlType; }
    public void setDmlType(String dmlType) { this.dmlType = dmlType; }
    public Integer getErrorCode() { return errorCode; }
    public void setErrorCode(Integer errorCode) { this.errorCode = errorCode; }
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    public String getSavepointName() { return savepointName; }
    public void setSavepointName(String savepointName) { this.savepointName = savepointName; }
}
