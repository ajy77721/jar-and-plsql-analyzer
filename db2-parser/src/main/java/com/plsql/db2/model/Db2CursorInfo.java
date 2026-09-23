package com.plsql.db2.model;

public class Db2CursorInfo {

    private String name;
    private String selectText;
    private boolean withHold;
    private int    line;

    public String  getName()       { return name; }
    public void    setName(String name)           { this.name = name; }
    public String  getSelectText() { return selectText; }
    public void    setSelectText(String selectText) { this.selectText = selectText; }
    public boolean isWithHold()    { return withHold; }
    public void    setWithHold(boolean withHold)  { this.withHold = withHold; }
    public int     getLine()       { return line; }
    public void    setLine(int line)              { this.line = line; }
}
