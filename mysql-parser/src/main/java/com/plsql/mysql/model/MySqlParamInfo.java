package com.plsql.mysql.model;

/** A single parameter of a MySQL PROCEDURE or FUNCTION. */
public class MySqlParamInfo {

    public enum Direction { IN, OUT, INOUT }

    private String    name;
    private String    dataType;
    private Direction direction = Direction.IN;
    private int       line;

    public String    getName()      { return name; }
    public void      setName(String n) { this.name = n; }

    public String    getDataType()  { return dataType; }
    public void      setDataType(String t) { this.dataType = t; }

    public Direction getDirection() { return direction; }
    public void      setDirection(Direction d) { this.direction = d; }

    public int       getLine()      { return line; }
    public void      setLine(int l) { this.line = l; }
}
