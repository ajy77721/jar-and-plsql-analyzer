package com.plsql.parser.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.*;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class StatementInfo {
    private String type;
    private int line;
    private int lineEnd;
    private String sqlText;
    private List<String> tables = new ArrayList<>();
    private int nestingDepth;
    private String indexVariable;
    private String collectionName;
    private String boundsExpression;
    private String limitValue;
    private String dmlType;
    private Integer errorCode;
    private String errorMessage;
    private String savepointName;

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public int getLine() { return line; }
    public void setLine(int line) { this.line = line; }
    public int getLineEnd() { return lineEnd; }
    public void setLineEnd(int lineEnd) { this.lineEnd = lineEnd; }
    public String getSqlText() { return sqlText; }
    public void setSqlText(String sqlText) { this.sqlText = sqlText; }
    public List<String> getTables() { return tables; }
    public void setTables(List<String> tables) { this.tables = tables; }
    public int getNestingDepth() { return nestingDepth; }
    public void setNestingDepth(int nestingDepth) { this.nestingDepth = nestingDepth; }
    public String getIndexVariable() { return indexVariable; }
    public void setIndexVariable(String indexVariable) { this.indexVariable = indexVariable; }
    public String getCollectionName() { return collectionName; }
    public void setCollectionName(String collectionName) { this.collectionName = collectionName; }
    public String getBoundsExpression() { return boundsExpression; }
    public void setBoundsExpression(String boundsExpression) { this.boundsExpression = boundsExpression; }
    public String getLimitValue() { return limitValue; }
    public void setLimitValue(String limitValue) { this.limitValue = limitValue; }
    public String getDmlType() { return dmlType; }
    public void setDmlType(String dmlType) { this.dmlType = dmlType; }
    public Integer getErrorCode() { return errorCode; }
    public void setErrorCode(Integer errorCode) { this.errorCode = errorCode; }
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    public String getSavepointName() { return savepointName; }
    public void setSavepointName(String savepointName) { this.savepointName = savepointName; }
}
