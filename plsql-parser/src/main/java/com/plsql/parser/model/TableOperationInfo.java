package com.plsql.parser.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.*;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class TableOperationInfo {
    private String tableName;
    private String schema;
    private String operation;
    private int line;
    private String objectType;
    private String alias;
    private String subqueryContext;
    private List<JoinInfo> joins = new ArrayList<>();
    private String dbLink;
    private String mergeClause;

    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }
    public String getSchema() { return schema; }
    public void setSchema(String schema) { this.schema = schema; }
    public String getOperation() { return operation; }
    public void setOperation(String operation) { this.operation = operation; }
    public int getLine() { return line; }
    public void setLine(int line) { this.line = line; }
    public String getObjectType() { return objectType; }
    public void setObjectType(String objectType) { this.objectType = objectType; }
    public String getAlias() { return alias; }
    public void setAlias(String alias) { this.alias = alias; }
    public String getSubqueryContext() { return subqueryContext; }
    public void setSubqueryContext(String subqueryContext) { this.subqueryContext = subqueryContext; }
    public List<JoinInfo> getJoins() { return joins; }
    public void setJoins(List<JoinInfo> joins) { this.joins = joins; }
    public String getDbLink() { return dbLink; }
    public void setDbLink(String dbLink) { this.dbLink = dbLink; }
    public String getMergeClause() { return mergeClause; }
    public void setMergeClause(String mergeClause) { this.mergeClause = mergeClause; }
}
