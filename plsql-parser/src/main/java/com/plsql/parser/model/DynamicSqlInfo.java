package com.plsql.parser.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.*;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class DynamicSqlInfo {
    private String type;
    private String sqlExpression;
    private int line;
    private List<String> usingVariables = new ArrayList<>();

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getSqlExpression() { return sqlExpression; }
    public void setSqlExpression(String sqlExpression) { this.sqlExpression = sqlExpression; }
    public int getLine() { return line; }
    public void setLine(int line) { this.line = line; }
    public List<String> getUsingVariables() { return usingVariables; }
    public void setUsingVariables(List<String> usingVariables) { this.usingVariables = usingVariables; }
}
