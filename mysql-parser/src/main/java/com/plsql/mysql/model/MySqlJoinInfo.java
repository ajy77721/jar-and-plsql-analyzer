package com.plsql.mysql.model;

/** A JOIN clause captured from a SELECT: type, joined table, alias, ON condition. */
public class MySqlJoinInfo {
    private String joinType;          // INNER, LEFT, RIGHT, CROSS, STRAIGHT_JOIN, NATURAL, JOIN
    private String joinedTable;
    private String joinedTableSchema;
    private String joinedTableAlias;
    private String leftTable;
    private String leftTableAlias;
    private String condition;
    private int line;
    private int queryId;

    public String getJoinType() { return joinType; }
    public void setJoinType(String v) { this.joinType = v; }

    public String getJoinedTable() { return joinedTable; }
    public void setJoinedTable(String v) { this.joinedTable = v; }

    public String getJoinedTableSchema() { return joinedTableSchema; }
    public void setJoinedTableSchema(String v) { this.joinedTableSchema = v; }

    public String getJoinedTableAlias() { return joinedTableAlias; }
    public void setJoinedTableAlias(String v) { this.joinedTableAlias = v; }

    public String getCondition() { return condition; }
    public void setCondition(String v) { this.condition = v; }

    public String getLeftTable() { return leftTable; }
    public void setLeftTable(String v) { this.leftTable = v; }

    public String getLeftTableAlias() { return leftTableAlias; }
    public void setLeftTableAlias(String v) { this.leftTableAlias = v; }

    public int getLine() { return line; }
    public void setLine(int v) { this.line = v; }

    public int getQueryId() { return queryId; }
    public void setQueryId(int v) { this.queryId = v; }
}
