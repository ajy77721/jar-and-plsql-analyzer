package com.plsql.sqlserver.model;

public class SqlServerParamInfo {

    public enum Direction { IN, OUT }

    private String    name;
    private String    dataType;
    private Direction direction = Direction.IN;
    private int       line;
    // Bug 9: the header's default value (e.g. "@p2 VARCHAR(50) = 'default'"), parsed by the
    // grammar's `defaultVal` rule but previously never read out of ctx by visitParam().
    private String    defaultValue;

    public String    getName()      { return name; }
    public void      setName(String name)           { this.name = name; }
    public String    getDataType()  { return dataType; }
    public void      setDataType(String dataType)   { this.dataType = dataType; }
    public Direction getDirection() { return direction; }
    public void      setDirection(Direction direction) { this.direction = direction; }
    public int       getLine()      { return line; }
    public void      setLine(int line)              { this.line = line; }
    public String    getDefaultValue() { return defaultValue; }
    public void      setDefaultValue(String defaultValue) { this.defaultValue = defaultValue; }
}
