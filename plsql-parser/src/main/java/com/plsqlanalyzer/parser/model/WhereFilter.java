package com.plsqlanalyzer.parser.model;

public class WhereFilter {
    private String columnName;
    private String operator;
    private String value;
    private int lineNumber;

    public WhereFilter() {}

    public WhereFilter(String columnName, String operator, String value, int lineNumber) {
        this.columnName = columnName;
        this.operator = operator;
        this.value = value;
        this.lineNumber = lineNumber;
    }

    public String getColumnName() { return columnName; }
    public void setColumnName(String columnName) { this.columnName = columnName; }

    public String getOperator() { return operator; }
    public void setOperator(String operator) { this.operator = operator; }

    public String getValue() { return value; }
    public void setValue(String value) { this.value = value; }

    public int getLineNumber() { return lineNumber; }
    public void setLineNumber(int lineNumber) { this.lineNumber = lineNumber; }
}
