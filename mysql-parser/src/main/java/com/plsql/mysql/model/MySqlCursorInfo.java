package com.plsql.mysql.model;

/** A DECLARE … CURSOR FOR … inside a MySQL stored procedure body. */
public class MySqlCursorInfo {
    private String name;
    private String selectText;   // full text of the SELECT
    private int    line;

    public String getName()             { return name; }
    public void   setName(String n)     { this.name = n; }

    public String getSelectText()           { return selectText; }
    public void   setSelectText(String s)   { this.selectText = s; }

    public int  getLine()      { return line; }
    public void setLine(int l) { this.line = l; }
}
