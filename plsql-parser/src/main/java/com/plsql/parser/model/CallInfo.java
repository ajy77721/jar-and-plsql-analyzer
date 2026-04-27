package com.plsql.parser.model;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class CallInfo {
    private String type;
    private String packageName;
    private String name;
    private String schema;
    private int line;
    private int arguments;

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getPackageName() { return packageName; }
    public void setPackageName(String packageName) { this.packageName = packageName; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getSchema() { return schema; }
    public void setSchema(String schema) { this.schema = schema; }
    public int getLine() { return line; }
    public void setLine(int line) { this.line = line; }
    public int getArguments() { return arguments; }
    public void setArguments(int arguments) { this.arguments = arguments; }
}
