package com.plsqlanalyzer.analyzer.model;

public class CallEdge {
    private String fromNodeId;
    private String toNodeId;
    private int callLineNumber;
    private boolean dynamicSql;
    private String callType; // INTERNAL, EXTERNAL, TRIGGER, DYNAMIC

    public CallEdge() {}

    public CallEdge(String fromNodeId, String toNodeId, int callLineNumber, boolean dynamicSql) {
        this.fromNodeId = fromNodeId;
        this.toNodeId = toNodeId;
        this.callLineNumber = callLineNumber;
        this.dynamicSql = dynamicSql;
    }

    public CallEdge(String fromNodeId, String toNodeId, int callLineNumber, boolean dynamicSql, String callType) {
        this(fromNodeId, toNodeId, callLineNumber, dynamicSql);
        this.callType = callType;
    }

    public String getFromNodeId() { return fromNodeId; }
    public void setFromNodeId(String fromNodeId) { this.fromNodeId = fromNodeId; }

    public String getToNodeId() { return toNodeId; }
    public void setToNodeId(String toNodeId) { this.toNodeId = toNodeId; }

    public int getCallLineNumber() { return callLineNumber; }
    public void setCallLineNumber(int callLineNumber) { this.callLineNumber = callLineNumber; }

    public boolean isDynamicSql() { return dynamicSql; }
    public void setDynamicSql(boolean dynamicSql) { this.dynamicSql = dynamicSql; }

    public String getCallType() { return callType; }
    public void setCallType(String callType) { this.callType = callType; }
}
