package com.plsql.sqlserver.model;

/** One executable statement, for the statement inventory / summary. */
public class SqlServerStatementInfo {
    private String type;   // INSERT, UPDATE, DELETE, SELECT, EXEC, IF, WHILE, SET, DECLARE, COMMIT, ROLLBACK, SAVEPOINT, ...
    private int line;
    private String sqlText;
    private String savepointName;   // for SAVEPOINT / ROLLBACK TO <name>

    // Nesting-depth metadata for transaction-control statements (BEGIN_TRAN / COMMIT / ROLLBACK /
    // SAVEPOINT). Convention: BEGIN_TRAN records the depth it is ENTERING (counter incremented
    // first, then recorded), so the first BEGIN TRANSACTION in a routine is depth=1, a nested one
    // is depth=2, etc. COMMIT/ROLLBACK record the depth they are CLOSING (recorded first, then the
    // counter is decremented), so the inner-most COMMIT/ROLLBACK matches the deepest still-open
    // BEGIN_TRAN's depth. SAVEPOINT (SAVE TRANSACTION) records the current depth at the point it
    // was taken (no counter change). Null means "not set for this row" (only transaction-control
    // rows carry a depth; it is omitted from the emitted JSON then — the shared StatementInfo
    // model applies @JsonInclude(NON_EMPTY)). A COMMIT/ROLLBACK issued with no matching
    // BEGIN_TRAN (e.g. relying on autocommit) records depth 0.
    private Integer transactionDepth;

    // ── Branch/loop guard + TRY/CATCH handler context (additive enrichment) ────────────────
    // The innermost enclosing control-flow construct at the moment this statement was recorded.
    // Null (omitted downstream) when the statement sits at the routine's top level.
    private String  guardKind;      // "IF" | "CASE" | "LOOP"
    private Integer guardLine;      // line of the guarding IF/CASE/WHILE keyword
    private String  guardCondition; // the guard's condition text, whitespace-collapsed, <=200 chars
    // Set when the statement is inside a TRY...CATCH's CATCH block.
    private String  handlerName;    // "CATCH"
    private Integer handlerLine;    // line of the BEGIN CATCH keyword
    // ROLLBACK TO <savepoint> / ROLLBACK TRANSACTION <savepoint> — a partial (savepoint) rollback.
    private Boolean isPartial;
    // SELECT ... FOR UPDATE. The T-SQL grammar's selectStatement is `SELECT stmtTail` with an
    // opaque stmtTail and no structured FOR UPDATE clause, so this is currently never set (kept
    // for contract completeness / the shared StatementInfo field).
    private Boolean forUpdate;

    // THROW / RAISERROR argument capture (bugs 6/7).
    private String errorNumber;   // THROW's first arg (error number)
    private String severity;      // RAISERROR's 2nd arg (severity)
    private String state;         // THROW's 3rd arg / RAISERROR's 3rd arg (state)
    private String message;       // THROW's 2nd arg / RAISERROR's 1st arg (message)

    public SqlServerStatementInfo() {}
    public SqlServerStatementInfo(String type, int line) { this.type = type; this.line = line; }

    public String getType() { return type; }
    public void setType(String v) { this.type = v; }
    public int getLine() { return line; }
    public void setLine(int v) { this.line = v; }
    public String getSqlText() { return sqlText; }
    public void setSqlText(String v) { this.sqlText = v; }
    public String getSavepointName() { return savepointName; }
    public void setSavepointName(String v) { this.savepointName = v; }
    public Integer getTransactionDepth() { return transactionDepth; }
    public void setTransactionDepth(Integer v) { this.transactionDepth = v; }
    public String getGuardKind() { return guardKind; }
    public void setGuardKind(String v) { this.guardKind = v; }
    public Integer getGuardLine() { return guardLine; }
    public void setGuardLine(Integer v) { this.guardLine = v; }
    public String getGuardCondition() { return guardCondition; }
    public void setGuardCondition(String v) { this.guardCondition = v; }
    public String getHandlerName() { return handlerName; }
    public void setHandlerName(String v) { this.handlerName = v; }
    public Integer getHandlerLine() { return handlerLine; }
    public void setHandlerLine(Integer v) { this.handlerLine = v; }
    public Boolean getIsPartial() { return isPartial; }
    public void setIsPartial(Boolean v) { this.isPartial = v; }
    public Boolean getForUpdate() { return forUpdate; }
    public void setForUpdate(Boolean v) { this.forUpdate = v; }
    public String getErrorNumber() { return errorNumber; }
    public void setErrorNumber(String v) { this.errorNumber = v; }
    public String getSeverity() { return severity; }
    public void setSeverity(String v) { this.severity = v; }
    public String getState() { return state; }
    public void setState(String v) { this.state = v; }
    public String getMessage() { return message; }
    public void setMessage(String v) { this.message = v; }
}
