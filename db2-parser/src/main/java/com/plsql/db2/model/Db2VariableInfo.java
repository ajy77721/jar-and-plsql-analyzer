package com.plsql.db2.model;

public class Db2VariableInfo {

    private String name;
    private String dataType;
    private String defaultValue;
    private int    line;

    public String getName()         { return name; }
    public void   setName(String name)             { this.name = name; }
    public String getDataType()     { return dataType; }
    public void   setDataType(String dataType)     { this.dataType = dataType; }
    public String getDefaultValue() { return defaultValue; }
    public void   setDefaultValue(String defaultValue) { this.defaultValue = defaultValue; }
    public int    getLine()         { return line; }
    public void   setLine(int line)                { this.line = line; }
}
