package com.plsql.parser.model;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class VariableInfo {
    private String name;
    private String dataType;
    private String scope;
    private String defaultValue;
    private boolean constant;
    private int line;
    private String sourcePackage;

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getDataType() { return dataType; }
    public void setDataType(String dataType) { this.dataType = dataType; }
    public String getScope() { return scope; }
    public void setScope(String scope) { this.scope = scope; }
    public String getDefaultValue() { return defaultValue; }
    public void setDefaultValue(String defaultValue) { this.defaultValue = defaultValue; }
    public boolean isConstant() { return constant; }
    public void setConstant(boolean constant) { this.constant = constant; }
    public int getLine() { return line; }
    public void setLine(int line) { this.line = line; }
    public String getSourcePackage() { return sourcePackage; }
    public void setSourcePackage(String sourcePackage) { this.sourcePackage = sourcePackage; }
}
