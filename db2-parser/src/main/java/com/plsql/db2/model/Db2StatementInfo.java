package com.plsql.db2.model;

/** One transaction-control statement (COMMIT / ROLLBACK / SAVEPOINT). */
public class Db2StatementInfo {
    private String type;            // COMMIT, ROLLBACK, SAVEPOINT
    private int line;
    private String savepointName;   // for SAVEPOINT / ROLLBACK TO SAVEPOINT <name>
    private String conditionName;   // for DECLARE_CONDITION / SIGNAL / RESIGNAL by condition name
    private String sqlstate;        // for DECLARE_CONDITION / SIGNAL / RESIGNAL SQLSTATE '...'
    private String messageText;     // for SIGNAL / RESIGNAL SET MESSAGE_TEXT = '...'
    private String label;           // for LEAVE / ITERATE <loop-label> — the label named in the source.
                                    // MUST stay out of `type`: a dynamic label in the type discriminator
                                    // produces one distinct statement-type key per labeled loop.

    // ── Enrichment (additive; nulls omitted by the writer) ────────────────────
    // Branch/loop context the statement sits inside, if any. kind ∈ {IF, CASE, LOOP}.
    private String guardKind;
    private Integer guardLine;
    private String guardCondition;  // collapsed, ≤ 200 chars
    // Exception-handler body the statement sits inside, if any.
    private String handlerName;     // handler condition/name, or "OTHERS"
    private Integer handlerLine;
    // SELECT ... FOR UPDATE / USE AND KEEP UPDATE LOCKS (only where the grammar exposes it).
    private Boolean forUpdate;
    // Collapsed source text of the statement (≤ 300 chars).
    private String sqlText;

    public Db2StatementInfo() {}
    public Db2StatementInfo(String type, int line) { this.type = type; this.line = line; }

    public String getType() { return type; }
    public void setType(String v) { this.type = v; }
    public int getLine() { return line; }
    public void setLine(int v) { this.line = v; }
    public String getSavepointName() { return savepointName; }
    public void setSavepointName(String v) { this.savepointName = v; }
    public String getConditionName() { return conditionName; }
    public void setConditionName(String v) { this.conditionName = v; }
    public String getSqlstate() { return sqlstate; }
    public void setSqlstate(String v) { this.sqlstate = v; }
    public String getMessageText() { return messageText; }
    public void setMessageText(String v) { this.messageText = v; }
    public String getLabel() { return label; }
    public void setLabel(String v) { this.label = v; }

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
