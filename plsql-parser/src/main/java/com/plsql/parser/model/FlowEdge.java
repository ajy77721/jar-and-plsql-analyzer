package com.plsql.parser.model;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class FlowEdge {
    private String fromNodeId;
    private String fromSchema;
    private String fromPackage;
    private String fromObject;
    private String toNodeId;
    private String toSchema;
    private String toPackage;
    private String toObject;
    private int line;
    private int depth;

    public String getFromNodeId() { return fromNodeId; }
    public void setFromNodeId(String fromNodeId) { this.fromNodeId = fromNodeId; }
    public String getFromSchema() { return fromSchema; }
    public void setFromSchema(String fromSchema) { this.fromSchema = fromSchema; }
    public String getFromPackage() { return fromPackage; }
    public void setFromPackage(String fromPackage) { this.fromPackage = fromPackage; }
    public String getFromObject() { return fromObject; }
    public void setFromObject(String fromObject) { this.fromObject = fromObject; }
    public String getToNodeId() { return toNodeId; }
    public void setToNodeId(String toNodeId) { this.toNodeId = toNodeId; }
    public String getToSchema() { return toSchema; }
    public void setToSchema(String toSchema) { this.toSchema = toSchema; }
    public String getToPackage() { return toPackage; }
    public void setToPackage(String toPackage) { this.toPackage = toPackage; }
    public String getToObject() { return toObject; }
    public void setToObject(String toObject) { this.toObject = toObject; }
    public int getLine() { return line; }
    public void setLine(int line) { this.line = line; }
    public int getDepth() { return depth; }
    public void setDepth(int depth) { this.depth = depth; }
}
