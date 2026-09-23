package com.plsql.sqlserver.model;

public class SqlServerVariableInfo {

    private String name;
    private String dataType;
    private int    line;

    public String getName()     { return name; }
    public void   setName(String name)         { this.name = name; }
    public String getDataType() { return dataType; }
    public void   setDataType(String dataType) { this.dataType = dataType; }
    public int    getLine()     { return line; }
    public void   setLine(int line)            { this.line = line; }
}
