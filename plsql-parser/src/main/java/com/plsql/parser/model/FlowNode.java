package com.plsql.parser.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.*;

/**
 * Defense in depth: a chunk file is written and read back by {@code ChunkedFlowWriter} (chunk_*.
 * json in {@code mergeChunks}/{@code buildChunkIndex}/{@code readNodeChunk}) using a plain {@code
 * ObjectMapper} with default {@code FAIL_ON_UNKNOWN_PROPERTIES}. {@code ignoreUnknown = true} means
 * a future field present in a written chunk but not (yet) recognized by this class on read
 * degrades to "field dropped" instead of throwing and aborting the whole merge — see {@code
 * ExceptionHandlerInfo#getParentExceptionName()}'s doc comment for the incident this generalizes
 * from.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
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
    private List<IdentityColumnInfo> identityColumns = new ArrayList<>();
    // Declared FOREIGN KEY relationships. Must live on FlowNode, not only on ParsedObject,
    // because a node is written to chunks/chunk_*.json and read back before the api/nodes
    // document is built — a field absent here does not survive that round trip.
    private List<ForeignKeyInfo> foreignKeys = new ArrayList<>();
    private List<String> externalPackageVarRefs = new ArrayList<>();
    private List<PackageVarRefInfo> externalPackageVarRefDetails = new ArrayList<>();
    private int linesOfCode;
    private int callCount;
    private List<String> calledBy = new ArrayList<>();
    private Map<String, Integer> statementSummary = new LinkedHashMap<>();
    private boolean readable = true;
    private String message;
    private String triggerEvent;
    private String triggerTable;
    private String triggerTiming;
    // Source-parsed trigger facets — persisted to the tables-index triggers[] and node detail so the
    // estate trigger facet can report the WHEN condition, the ROW/STATEMENT level and the body.
    private String triggerWhenClause;
    private String triggerLevel;
    private String triggerBody;
    /**
     * Package-level variable DECLARATIONS ({@code name/dataType/defaultValue/constant}) for a node
     * that is a package (PACKAGE_BODY/PACKAGE_SPEC). Must live on FlowNode, not only on
     * ParsedObject, because the node is written to chunks/chunk_*.json and read back before the
     * api/nodes document is built — a field absent here does not survive that round trip. Empty for
     * every non-package node.
     */
    private List<VariableInfo> packageVariables = new ArrayList<>();
    private int overloadParamCount = -1;   // >=0 only for overloaded package members
    private boolean autonomousTransaction; // PRAGMA AUTONOMOUS_TRANSACTION (Oracle)
    /**
     * Populated ONLY when this node is a package-as-entry-point aggregate (a bare package name
     * crawled as a single node — see DependencyCrawler's aggregation branch): one entry per
     * subprogram in the package, so the UI can still list/drill into individual procedures
     * without a separate crawl/output folder per procedure. Empty for every other node.
     */
    private List<SubprogramSummaryInfo> members = new ArrayList<>();

    public List<SubprogramSummaryInfo> getMembers() { return members; }
    public void setMembers(List<SubprogramSummaryInfo> members) { this.members = members; }

    public int getOverloadParamCount() { return overloadParamCount; }
    public void setOverloadParamCount(int overloadParamCount) { this.overloadParamCount = overloadParamCount; }

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
    public List<IdentityColumnInfo> getIdentityColumns() { return identityColumns; }
    public void setIdentityColumns(List<IdentityColumnInfo> identityColumns) { this.identityColumns = identityColumns; }
    public List<ForeignKeyInfo> getForeignKeys() { return foreignKeys; }
    public void setForeignKeys(List<ForeignKeyInfo> foreignKeys) { this.foreignKeys = foreignKeys; }
    public List<String> getExternalPackageVarRefs() { return externalPackageVarRefs; }
    public void setExternalPackageVarRefs(List<String> externalPackageVarRefs) { this.externalPackageVarRefs = externalPackageVarRefs; }
    public List<PackageVarRefInfo> getExternalPackageVarRefDetails() { return externalPackageVarRefDetails; }
    public void setExternalPackageVarRefDetails(List<PackageVarRefInfo> d) { this.externalPackageVarRefDetails = d; }
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
    public boolean isAutonomousTransaction() { return autonomousTransaction; }
    public void setAutonomousTransaction(boolean v) { this.autonomousTransaction = v; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public String getTriggerEvent() { return triggerEvent; }
    public void setTriggerEvent(String triggerEvent) { this.triggerEvent = triggerEvent; }
    public String getTriggerTable() { return triggerTable; }
    public void setTriggerTable(String triggerTable) { this.triggerTable = triggerTable; }
    public String getTriggerTiming() { return triggerTiming; }
    public void setTriggerTiming(String triggerTiming) { this.triggerTiming = triggerTiming; }
    public String getTriggerWhenClause() { return triggerWhenClause; }
    public void setTriggerWhenClause(String triggerWhenClause) { this.triggerWhenClause = triggerWhenClause; }
    public String getTriggerLevel() { return triggerLevel; }
    public void setTriggerLevel(String triggerLevel) { this.triggerLevel = triggerLevel; }
    public String getTriggerBody() { return triggerBody; }
    public void setTriggerBody(String triggerBody) { this.triggerBody = triggerBody; }
    public List<VariableInfo> getPackageVariables() { return packageVariables; }
    public void setPackageVariables(List<VariableInfo> packageVariables) { this.packageVariables = packageVariables; }

    public void generateNodeId() {
        this.nodeId = buildShortId();
    }

    public String buildShortId() {
        StringBuilder sb = new StringBuilder();
        if (schema != null && !schema.isEmpty()) sb.append(schema);
        if (packageName != null && !packageName.isEmpty()) sb.append("$").append(packageName);
        if (objectName != null && !objectName.isEmpty()) sb.append("$").append(objectName);
        // Overloaded package members (same name, different arity) get a param-count
        // discriminator so they resolve to DISTINCT nodes instead of overwriting each
        // other. Only set for genuine overloads → non-overloaded ids stay byte-identical.
        if (overloadParamCount >= 0) sb.append("__").append(overloadParamCount);
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
