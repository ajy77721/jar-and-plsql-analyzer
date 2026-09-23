package com.plsql.mysql.model;

/** A DECLARE variable inside a MySQL stored procedure body. */
public class MySqlVariableInfo {
    private String name;
    private String dataType;
    private String defaultValue;
    private int    line;

    public String getName()         { return name; }
    public void   setName(String n) { this.name = n; }

    public String getDataType()           { return dataType; }
    public void   setDataType(String t)   { this.dataType = t; }

    public String getDefaultValue()           { return defaultValue; }
    public void   setDefaultValue(String v)   { this.defaultValue = v; }

    public int  getLine()      { return line; }
    public void setLine(int l) { this.line = l; }
}
