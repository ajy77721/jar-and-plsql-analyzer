package com.plsqlanalyzer.parser.model;

/**
 * Represents a JOIN between tables found in SQL statements.
 * Captures the joined tables, join type, and ON predicate.
 */
public class JoinInfo {
    private String leftTable;
    private String rightTable;
    private String joinType;     // INNER, LEFT, RIGHT, FULL, CROSS
    private String onPredicate;  // The ON clause text (e.g., "a.id = b.parent_id")
    private int lineNumber;

    public JoinInfo() {}

    public JoinInfo(String leftTable, String rightTable, String joinType, String onPredicate, int lineNumber) {
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.joinType = joinType;
        this.onPredicate = onPredicate;
        this.lineNumber = lineNumber;
    }

    public String getLeftTable() { return leftTable; }
    public void setLeftTable(String leftTable) { this.leftTable = leftTable; }

    public String getRightTable() { return rightTable; }
    public void setRightTable(String rightTable) { this.rightTable = rightTable; }

    public String getJoinType() { return joinType; }
    public void setJoinType(String joinType) { this.joinType = joinType; }

    public String getOnPredicate() { return onPredicate; }
    public void setOnPredicate(String onPredicate) { this.onPredicate = onPredicate; }

    public int getLineNumber() { return lineNumber; }
    public void setLineNumber(int lineNumber) { this.lineNumber = lineNumber; }
}
