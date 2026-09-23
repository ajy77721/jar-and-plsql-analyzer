package com.plsqlanalyzer.parser.model;

public class VariableInfo {
    private String name;
    private String dataType;
    private boolean isConstant;
    private int lineNumber;

    public VariableInfo() {}

    public VariableInfo(String name, String dataType, boolean isConstant, int lineNumber) {
        this.name = name;
        this.dataType = dataType;
        this.isConstant = isConstant;
        this.lineNumber = lineNumber;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getDataType() { return dataType; }
    public void setDataType(String dataType) { this.dataType = dataType; }

    public boolean isConstant() { return isConstant; }
    public void setConstant(boolean constant) { isConstant = constant; }

    public int getLineNumber() { return lineNumber; }
    public void setLineNumber(int lineNumber) { this.lineNumber = lineNumber; }
}
