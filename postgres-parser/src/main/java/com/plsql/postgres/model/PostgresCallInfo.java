package com.plsql.postgres.model;

public class PostgresCallInfo {

    private String schema;
    private String name;
    private int    line;

    public String getName()   { return name; }
    public void   setName(String name)    { this.name = name; }
    public String getSchema() { return schema; }
    public void   setSchema(String schema) { this.schema = schema; }
    public int    getLine()   { return line; }
    public void   setLine(int line)       { this.line = line; }

    public String qualifiedName() {
        return schema != null ? schema + "." + name : name;
    }
}
