package com.plsql.parser.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.*;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ParsedObject {
    private String type;
    private String name;
    private String schema;
    private int lineStart;
    private int lineEnd;
    private List<ParameterInfo> parameters = new ArrayList<>();
    private List<VariableInfo> globalVariables = new ArrayList<>();
    private List<VariableInfo> packageVariables = new ArrayList<>();
    private List<SubprogramInfo> subprograms = new ArrayList<>();
    private List<StatementInfo> statements = new ArrayList<>();
    private List<CallInfo> calls = new ArrayList<>();
    private List<CursorInfo> cursors = new ArrayList<>();
    private List<DynamicSqlInfo> dynamicSql = new ArrayList<>();
    private List<TableOperationInfo> tableOperations = new ArrayList<>();
    private List<String> externalPackageVarRefs = new ArrayList<>();
    private List<ExceptionHandlerInfo> exceptionHandlers = new ArrayList<>();
    private DependencySummary dependencies = new DependencySummary();
    private String triggerEvent;
    private String triggerTable;
    private String triggerTiming;
    private String viewQuery;

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getSchema() { return schema; }
    public void setSchema(String schema) { this.schema = schema; }
    public int getLineStart() { return lineStart; }
    public void setLineStart(int lineStart) { this.lineStart = lineStart; }
    public int getLineEnd() { return lineEnd; }
    public void setLineEnd(int lineEnd) { this.lineEnd = lineEnd; }
    public List<ParameterInfo> getParameters() { return parameters; }
    public void setParameters(List<ParameterInfo> parameters) { this.parameters = parameters; }
    public List<VariableInfo> getGlobalVariables() { return globalVariables; }
    public void setGlobalVariables(List<VariableInfo> v) { this.globalVariables = v; }
    public List<VariableInfo> getPackageVariables() { return packageVariables; }
    public void setPackageVariables(List<VariableInfo> v) { this.packageVariables = v; }
    public List<SubprogramInfo> getSubprograms() { return subprograms; }
    public void setSubprograms(List<SubprogramInfo> s) { this.subprograms = s; }
    public List<StatementInfo> getStatements() { return statements; }
    public void setStatements(List<StatementInfo> s) { this.statements = s; }
    public List<CallInfo> getCalls() { return calls; }
    public void setCalls(List<CallInfo> c) { this.calls = c; }
    public List<CursorInfo> getCursors() { return cursors; }
    public void setCursors(List<CursorInfo> c) { this.cursors = c; }
    public List<DynamicSqlInfo> getDynamicSql() { return dynamicSql; }
    public void setDynamicSql(List<DynamicSqlInfo> d) { this.dynamicSql = d; }
    public List<TableOperationInfo> getTableOperations() { return tableOperations; }
    public void setTableOperations(List<TableOperationInfo> t) { this.tableOperations = t; }
    public List<String> getExternalPackageVarRefs() { return externalPackageVarRefs; }
    public void setExternalPackageVarRefs(List<String> e) { this.externalPackageVarRefs = e; }
    public List<ExceptionHandlerInfo> getExceptionHandlers() { return exceptionHandlers; }
    public void setExceptionHandlers(List<ExceptionHandlerInfo> e) { this.exceptionHandlers = e; }
    public DependencySummary getDependencies() { return dependencies; }
    public void setDependencies(DependencySummary d) { this.dependencies = d; }
    public String getTriggerEvent() { return triggerEvent; }
    public void setTriggerEvent(String t) { this.triggerEvent = t; }
    public String getTriggerTable() { return triggerTable; }
    public void setTriggerTable(String t) { this.triggerTable = t; }
    public String getTriggerTiming() { return triggerTiming; }
    public void setTriggerTiming(String t) { this.triggerTiming = t; }
    public String getViewQuery() { return viewQuery; }
    public void setViewQuery(String v) { this.viewQuery = v; }
}
