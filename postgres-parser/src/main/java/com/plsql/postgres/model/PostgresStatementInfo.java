package com.plsql.postgres.model;

/** One transaction-control statement (COMMIT / ROLLBACK / SAVEPOINT). */
public class PostgresStatementInfo {
    private String type;            // COMMIT, ROLLBACK, SAVEPOINT
    private int line;
    private String savepointName;   // for SAVEPOINT / ROLLBACK TO SAVEPOINT <name>
    private String detail;          // free-form extra info, e.g. RAISE message/ERRCODE text

    public PostgresStatementInfo() {}
    public PostgresStatementInfo(String type, int line) { this.type = type; this.line = line; }

    public String getType() { return type; }
    public void setType(String v) { this.type = v; }
    public int getLine() { return line; }
    public void setLine(int v) { this.line = v; }
    public String getSavepointName() { return savepointName; }
    public void setSavepointName(String v) { this.savepointName = v; }
    public String getDetail() { return detail; }
    public void setDetail(String v) { this.detail = v; }
}
