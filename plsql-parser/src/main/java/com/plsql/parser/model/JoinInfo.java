package com.plsql.parser.model;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class JoinInfo {
    private String joinType;
    private String joinedTable;
    private String joinedTableAlias;
    private String condition;
    private int line;

    public String getJoinType() { return joinType; }
    public void setJoinType(String joinType) { this.joinType = joinType; }
    public String getJoinedTable() { return joinedTable; }
    public void setJoinedTable(String joinedTable) { this.joinedTable = joinedTable; }
    public String getJoinedTableAlias() { return joinedTableAlias; }
    public void setJoinedTableAlias(String joinedTableAlias) { this.joinedTableAlias = joinedTableAlias; }
    public String getCondition() { return condition; }
    public void setCondition(String condition) { this.condition = condition; }
    public int getLine() { return line; }
    public void setLine(int line) { this.line = line; }
}
