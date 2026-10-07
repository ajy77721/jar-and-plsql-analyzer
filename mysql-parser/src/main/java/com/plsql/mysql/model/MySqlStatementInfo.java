package com.plsql.mysql.model;

/** One transaction-control statement (COMMIT / ROLLBACK / SAVEPOINT). */
public class MySqlStatementInfo {
    private String type;            // COMMIT, ROLLBACK, SAVEPOINT
    private int line;
    private String savepointName;   // for SAVEPOINT / ROLLBACK TO <name>

    /**
     * Index into the owning routine's {@code exceptionHandlers} list when this statement is
     * inside a {@code DECLARE ... HANDLER} body, else {@code null} — see {@link MySqlCallInfo}.
     */
    private Integer handlerIndex;

    // SIGNAL/RESIGNAL/DECLARE CONDITION capture (bug 4: SIGNAL/RESIGNAL parsed but never
    // recorded structurally — mirrors Db2StatementInfo's sqlstate/conditionName/messageText).
    private String sqlstate;
    private String conditionName;
    private String messageText;

    // ── Enrichment (additive; nulls omitted by the writer) ────────────────────
    // Branch/loop context the statement sits inside, if any. kind ∈ {IF, CASE, LOOP}.
    private String guardKind;
    private Integer guardLine;
    private String guardCondition;  // collapsed, ≤ 200 chars
    // Exception-handler body the statement sits inside, if any.
    private String handlerName;     // handler condition/name, or "OTHERS"
    private Integer handlerLine;
    // SELECT ... FOR UPDATE / FOR SHARE (only where the grammar exposes it).
    private Boolean forUpdate;
    // Collapsed source text of the statement (≤ 300 chars).
    private String sqlText;

    public MySqlStatementInfo() {}
    public MySqlStatementInfo(String type, int line) { this.type = type; this.line = line; }

    public String getType() { return type; }
    public void setType(String v) { this.type = v; }
    public int getLine() { return line; }
    public void setLine(int v) { this.line = v; }
    public String getSavepointName() { return savepointName; }
    public void setSavepointName(String v) { this.savepointName = v; }
    public Integer getHandlerIndex() { return handlerIndex; }
    public void setHandlerIndex(Integer v) { this.handlerIndex = v; }
    public String getSqlstate() { return sqlstate; }
    public void setSqlstate(String v) { this.sqlstate = v; }
    public String getConditionName() { return conditionName; }
    public void setConditionName(String v) { this.conditionName = v; }
    public String getMessageText() { return messageText; }
    public void setMessageText(String v) { this.messageText = v; }

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
    public Boolean getForUpdate() { return forUpdate; }
    public void setForUpdate(Boolean v) { this.forUpdate = v; }
    public String getSqlText() { return sqlText; }
    public void setSqlText(String v) { this.sqlText = v; }
}
