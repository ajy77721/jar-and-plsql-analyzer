package com.plsql.parser.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.*;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ExceptionHandlerInfo {
    private String exceptionName;
    private int line;
    private int lineEnd;
    private int statementsCount;
    private List<TableOperationInfo> tableOperations = new ArrayList<>();
    private List<CallInfo> calls = new ArrayList<>();

    public String getExceptionName() { return exceptionName; }
    public void setExceptionName(String exceptionName) { this.exceptionName = exceptionName; }
    public int getLine() { return line; }
    public void setLine(int line) { this.line = line; }
    public int getLineEnd() { return lineEnd; }
    public void setLineEnd(int lineEnd) { this.lineEnd = lineEnd; }
    public int getStatementsCount() { return statementsCount; }
    public void setStatementsCount(int statementsCount) { this.statementsCount = statementsCount; }
    public List<TableOperationInfo> getTableOperations() { return tableOperations; }
    public void setTableOperations(List<TableOperationInfo> tableOperations) { this.tableOperations = tableOperations; }
    public List<CallInfo> getCalls() { return calls; }
    public void setCalls(List<CallInfo> calls) { this.calls = calls; }
}
