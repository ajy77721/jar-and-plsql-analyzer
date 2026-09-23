package com.plsqlanalyzer.parser.model;

public class StatementInfo {
    private String type;  // IF, WHILE, FOR, LOOP, CASE, SELECT, INSERT, UPDATE, DELETE, MERGE,
                          // CALL, EXECUTE_IMMEDIATE, RETURN, RAISE, COMMIT, ROLLBACK, BLOCK, ASSIGN, NULL
    private int lineNumber;
    private int endLine;
    private String target; // table name for DML, proc name for CALL, variable for ASSIGN

    public StatementInfo() {}

    public StatementInfo(String type, int lineNumber, int endLine, String target) {
        this.type = type;
        this.lineNumber = lineNumber;
        this.endLine = endLine;
        this.target = target;
    }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public int getLineNumber() { return lineNumber; }
    public void setLineNumber(int lineNumber) { this.lineNumber = lineNumber; }

    public int getEndLine() { return endLine; }
    public void setEndLine(int endLine) { this.endLine = endLine; }

    public String getTarget() { return target; }
    public void setTarget(String target) { this.target = target; }
}
