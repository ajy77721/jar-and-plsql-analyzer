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
}
