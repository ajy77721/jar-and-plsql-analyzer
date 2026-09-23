package com.plsql.db2.model;

public class Db2ParamInfo {

    public enum Direction { IN, OUT, INOUT }

    private String    name;
    private String    dataType;
    private Direction direction = Direction.IN;
    private int       line;

    public String    getName()      { return name; }
    public void      setName(String name)           { this.name = name; }
    public String    getDataType()  { return dataType; }
    public void      setDataType(String dataType)   { this.dataType = dataType; }
    public Direction getDirection() { return direction; }
    public void      setDirection(Direction direction) { this.direction = direction; }
    public int       getLine()      { return line; }
    public void      setLine(int line)              { this.line = line; }
}
