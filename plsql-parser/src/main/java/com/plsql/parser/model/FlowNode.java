package com.plsql.parser.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.*;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class FlowNode {
    private String nodeId;
    private int order;
    private int depth;
    private String schema;
    private String packageName;
    private String objectName;
    private String objectType;
    private int lineStart;
    private int lineEnd;
    private List<ParameterInfo> parameters = new ArrayList<>();
    private List<VariableInfo> localVariables = new ArrayList<>();
    private List<StatementInfo> statements = new ArrayList<>();
    private List<CallInfo> calls = new ArrayList<>();
    private List<CursorInfo> cursors = new ArrayList<>();
    private List<DynamicSqlInfo> dynamicSql = new ArrayList<>();
    private List<TableOperationInfo> tableOperations = new ArrayList<>();
    private List<ExceptionHandlerInfo> exceptionHandlers = new ArrayList<>();
    private List<String> externalPackageVarRefs = new ArrayList<>();
    private int linesOfCode;
    private int callCount;
    private List<String> calledBy = new ArrayList<>();
    private Map<String, Integer> statementSummary = new LinkedHashMap<>();
    private boolean readable = true;
    private String message;
    private String triggerEvent;
    private String triggerTable;
    private String triggerTiming;

    public String getNodeId() { return nodeId; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }
    public int getOrder() { return order; }
    public void setOrder(int order) { this.order = order; }
    public int getDepth() { return depth; }
    public void setDepth(int depth) { this.depth = depth; }
    public String getSchema() { return schema; }
    public void setSchema(String schema) { this.schema = schema; }
    public String getPackageName() { return packageName; }
    public void setPackageName(String packageName) { this.packageName = packageName; }
    public String getObjectName() { return objectName; }
    public void setObjectName(String objectName) { this.objectName = objectName; }
    public String getObjectType() { return objectType; }
    public void setObjectType(String objectType) { this.objectType = objectType; }
    public int getLineStart() { return lineStart; }
    public void setLineStart(int lineStart) { this.lineStart = lineStart; }
    public int getLineEnd() { return lineEnd; }
    public void setLineEnd(int lineEnd) { this.lineEnd = lineEnd; }
    public List<ParameterInfo> getParameters() { return parameters; }
    public void setParameters(List<ParameterInfo> parameters) { this.parameters = parameters; }
    public List<VariableInfo> getLocalVariables() { return localVariables; }
    public void setLocalVariables(List<VariableInfo> localVariables) { this.localVariables = localVariables; }
    public List<StatementInfo> getStatements() { return statements; }
    public void setStatements(List<StatementInfo> statements) { this.statements = statements; }
    public List<CallInfo> getCalls() { return calls; }
    public void setCalls(List<CallInfo> calls) { this.calls = calls; }
    public List<CursorInfo> getCursors() { return cursors; }
    public void setCursors(List<CursorInfo> cursors) { this.cursors = cursors; }
    public List<DynamicSqlInfo> getDynamicSql() { return dynamicSql; }
    public void setDynamicSql(List<DynamicSqlInfo> dynamicSql) { this.dynamicSql = dynamicSql; }
    public List<TableOperationInfo> getTableOperations() { return tableOperations; }
    public void setTableOperations(List<TableOperationInfo> tableOperations) { this.tableOperations = tableOperations; }
    public List<ExceptionHandlerInfo> getExceptionHandlers() { return exceptionHandlers; }
    public void setExceptionHandlers(List<ExceptionHandlerInfo> exceptionHandlers) { this.exceptionHandlers = exceptionHandlers; }
    public List<String> getExternalPackageVarRefs() { return externalPackageVarRefs; }
    public void setExternalPackageVarRefs(List<String> externalPackageVarRefs) { this.externalPackageVarRefs = externalPackageVarRefs; }
    public int getLinesOfCode() { return linesOfCode; }
    public void setLinesOfCode(int linesOfCode) { this.linesOfCode = linesOfCode; }
    public int getCallCount() { return callCount; }
    public void setCallCount(int callCount) { this.callCount = callCount; }
    public List<String> getCalledBy() { return calledBy; }
    public void setCalledBy(List<String> calledBy) { this.calledBy = calledBy; }
    public Map<String, Integer> getStatementSummary() { return statementSummary; }
    public void setStatementSummary(Map<String, Integer> statementSummary) { this.statementSummary = statementSummary; }
    public boolean isReadable() { return readable; }
    public void setReadable(boolean readable) { this.readable = readable; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public String getTriggerEvent() { return triggerEvent; }
    public void setTriggerEvent(String triggerEvent) { this.triggerEvent = triggerEvent; }
    public String getTriggerTable() { return triggerTable; }
    public void setTriggerTable(String triggerTable) { this.triggerTable = triggerTable; }
    public String getTriggerTiming() { return triggerTiming; }
    public void setTriggerTiming(String triggerTiming) { this.triggerTiming = triggerTiming; }

    public void generateNodeId() {
        this.nodeId = buildShortId();
    }

    public String buildShortId() {
        StringBuilder sb = new StringBuilder();
        if (schema != null && !schema.isEmpty()) sb.append(schema);
        if (packageName != null && !packageName.isEmpty()) sb.append("$").append(packageName);
        if (objectName != null && !objectName.isEmpty()) sb.append("$").append(objectName);
        return sb.toString().toUpperCase().replaceAll("[^A-Z0-9_$]", "_");
    }

    public String buildFullId() {
        StringBuilder sb = new StringBuilder();
        if (schema != null && !schema.isEmpty()) sb.append(schema);
        if (packageName != null && !packageName.isEmpty()) sb.append("$").append(packageName);
        if (objectName != null && !objectName.isEmpty()) sb.append("$").append(objectName);
        if (parameters != null && !parameters.isEmpty()) {
            for (ParameterInfo p : parameters) {
                sb.append("_");
                if (p.getDirection() != null) sb.append(p.getDirection().replace(" ", ""));
                sb.append("_");
                if (p.getName() != null) sb.append(p.getName());
            }
        }
        return sb.toString().toUpperCase().replaceAll("[^A-Z0-9_$]", "_");
    }
}
