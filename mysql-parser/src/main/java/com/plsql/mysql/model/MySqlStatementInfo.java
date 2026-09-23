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
}
