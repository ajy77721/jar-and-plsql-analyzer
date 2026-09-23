package com.plsql.parser.model;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class JoinInfo {
    private String joinType;
    private String joinedTable;
    private String joinedTableAlias;
    // Left side of the join, resolved from the ON-condition aliases (NOT defaulted
    // to the FROM base table). May be null when it cannot be resolved.
    private String leftTable;
    private String leftTableAlias;
    private String condition;
    private int line;
    // Identifies the originating query/statement so joins from different SELECTs
    // that share a base table are not lumped together downstream. 0 = unknown.
    private int queryId;

    public String getJoinType() { return joinType; }
    public void setJoinType(String joinType) { this.joinType = joinType; }
    public String getJoinedTable() { return joinedTable; }
    public void setJoinedTable(String joinedTable) { this.joinedTable = joinedTable; }
    public String getJoinedTableAlias() { return joinedTableAlias; }
    public void setJoinedTableAlias(String joinedTableAlias) { this.joinedTableAlias = joinedTableAlias; }
    public String getLeftTable() { return leftTable; }
    public void setLeftTable(String leftTable) { this.leftTable = leftTable; }
    public String getLeftTableAlias() { return leftTableAlias; }
    public void setLeftTableAlias(String leftTableAlias) { this.leftTableAlias = leftTableAlias; }
    public String getCondition() { return condition; }
    public void setCondition(String condition) { this.condition = condition; }
    public int getLine() { return line; }
    public void setLine(int line) { this.line = line; }
    public int getQueryId() { return queryId; }
    public void setQueryId(int queryId) { this.queryId = queryId; }
}
