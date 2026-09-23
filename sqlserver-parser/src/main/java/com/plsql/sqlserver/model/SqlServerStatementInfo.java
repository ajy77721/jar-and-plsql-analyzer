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
    // was taken (no counter change). 0 means "not inside any BEGIN TRANSACTION" (only meaningful
    // for COMMIT/ROLLBACK issued with no matching BEGIN_TRAN, e.g. relying on autocommit).
    private int transactionDepth;

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
    public int getTransactionDepth() { return transactionDepth; }
    public void setTransactionDepth(int v) { this.transactionDepth = v; }
    public String getErrorNumber() { return errorNumber; }
    public void setErrorNumber(String v) { this.errorNumber = v; }
    public String getSeverity() { return severity; }
    public void setSeverity(String v) { this.severity = v; }
    public String getState() { return state; }
    public void setState(String v) { this.state = v; }
    public String getMessage() { return message; }
    public void setMessage(String v) { this.message = v; }
}
