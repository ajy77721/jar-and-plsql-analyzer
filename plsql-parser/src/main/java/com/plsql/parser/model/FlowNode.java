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
    private int overloadParamCount = -1;   // legacy arity fallback; >=0 only for overloaded package members
    /**
     * Signature-based overload discriminator (W1): the sanitized parameter-TYPE list, optionally
     * kind-prefixed ({@code FUNCTION_}/{@code PROCEDURE_}) when the same name is overloaded across
     * both kinds. Appended to the short id after {@code __} so same-arity-different-type overloads
     * resolve to DISTINCT nodes (the legacy {@link #overloadParamCount} arity suffix is kept as the
     * fallback when types are unknown). Null for non-overloaded nodes → their ids stay byte-identical.
     */
    private String overloadSignature;
    private boolean autonomousTransaction; // PRAGMA AUTONOMOUS_TRANSACTION (Oracle)
    // Tasks #26/#27 — the result-set / return-shape fields must live on FlowNode too, because a
    // node is written to chunks/chunk_*.json and read back before api/nodes is built; a field
    // present only on ParsedObject would not survive that round trip.
    private boolean returnsResultSet;
    private int resultSets;
    private String returnType;
    private List<String> returnsColumns = new ArrayList<>();
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
    public String getOverloadSignature() { return overloadSignature; }
    public void setOverloadSignature(String overloadSignature) { this.overloadSignature = overloadSignature; }

    /**
     * The overload discriminator appended to the node id WITHOUT the leading {@code __}, or null
     * when this node is not a genuine overload. Signature-based when known, else the legacy arity
     * count. Kept as the single source of truth for both {@link #buildShortId()} and
     * {@code ChunkedFlowWriter}'s edge-reconciliation so the two can never drift.
     */
    public String getOverloadDiscriminator() {
        if (overloadSignature != null && !overloadSignature.isEmpty()) return overloadSignature;
        if (overloadParamCount >= 0) return String.valueOf(overloadParamCount);
        return null;
    }

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
    public boolean isReturnsResultSet() { return returnsResultSet; }
    public void setReturnsResultSet(boolean v) { this.returnsResultSet = v; }
    public int getResultSets() { return resultSets; }
    public void setResultSets(int v) { this.resultSets = v; }
    public String getReturnType() { return returnType; }
    public void setReturnType(String v) { this.returnType = v; }
    public List<String> getReturnsColumns() { return returnsColumns; }
    public void setReturnsColumns(List<String> v) { this.returnsColumns = v != null ? v : new ArrayList<>(); }
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
        // Overloaded package members get a discriminator so they resolve to DISTINCT nodes
        // instead of overwriting each other: the parameter-TYPE signature when known
        // (e.g. __NUM_VARCHAR2, or __FUNCTION_NUM for a FUNCTION/PROCEDURE same-name pair),
        // else the legacy param-count (__2). Only set for genuine overloads → non-overloaded
        // ids stay byte-identical.
        String overloadDisc = getOverloadDiscriminator();
        if (overloadDisc != null) sb.append("__").append(overloadDisc);
        return sb.toString().toUpperCase().replaceAll("[^A-Z0-9_$]", "_");
    }

    // ── Overload signature helpers (W1) ──────────────────────────────────────────────────────
    // Pure, DB-free statics so the resolution rules are unit-testable without a crawler/parse.

    /** Sanitize a declared parameter type to an id-safe token (NUMBER(10,2) → NUMBER_10_2). */
    public static String sanitizeType(String type) {
        if (type == null || type.isBlank()) return null;
        String s = type.toUpperCase().replaceAll("[^A-Z0-9]", "_").replaceAll("_+", "_");
        s = s.replaceAll("^_+", "").replaceAll("_+$", "");
        return s.isEmpty() ? null : s;
    }

    /** True when the list is non-empty and every entry is a known (non-null, non-blank) type. */
    public static boolean allTypesKnown(List<String> types) {
        if (types == null || types.isEmpty()) return false;
        for (String t : types) {
            if (t == null || t.isBlank()) return false;
        }
        return true;
    }

    /** Join a list of raw type expressions into a signature token, or null when none is known. */
    public static String signatureOfTypes(List<String> types) {
        if (types == null || types.isEmpty()) return "NONE";
        StringBuilder sb = new StringBuilder();
        boolean any = false;
        for (String t : types) {
            String s = sanitizeType(t);
            if (s == null) s = "UNKNOWN"; else any = true;
            if (sb.length() > 0) sb.append("_");
            sb.append(s);
        }
        return any ? sb.toString() : null;
    }

    /** The parameter-type signature of a subprogram, or null when NO parameter type is known. */
    public static String typeSignature(List<ParameterInfo> params) {
        if (params == null || params.isEmpty()) return "NONE";
        List<String> types = new ArrayList<>(params.size());
        for (ParameterInfo p : params) types.add(p == null ? null : p.getDataType());
        return signatureOfTypes(types);
    }

    /** FUNCTION / PROCEDURE / null, normalized from a subprogram's declared type. */
    public static String kindOf(SubprogramInfo sub) {
        if (sub == null || sub.getType() == null) return null;
        String u = sub.getType().toUpperCase();
        if (u.contains("FUNCTION")) return "FUNCTION";
        if (u.contains("PROCEDURE")) return "PROCEDURE";
        return null;
    }

    /** True when a same-named overload set contains BOTH a function and a procedure. */
    public static boolean mixedKinds(List<SubprogramInfo> subs) {
        boolean fn = false, pr = false;
        for (SubprogramInfo s : subs) {
            String k = kindOf(s);
            if ("FUNCTION".equals(k)) fn = true;
            else if ("PROCEDURE".equals(k)) pr = true;
        }
        return fn && pr;
    }

    /** The discriminator for one overload: kind-prefixed type signature, else kind-prefixed arity. */
    public static String discriminatorFor(SubprogramInfo sub, boolean kindMixed) {
        String sig = typeSignature(sub.getParameters());
        String kind = kindMixed ? kindOf(sub) : null;
        String base = sig != null ? sig : String.valueOf(sub.getParameters() == null ? 0 : sub.getParameters().size());
        return kind != null ? kind + "_" + base : base;
    }

    /**
     * Resolve the overload discriminator for a call/node, mirroring the crawler's own selection
     * order. Chain: exact signature (when the grammar supplied argument types) → arity → first
     * match. The latter two are "fallbacks" and are counted into the supplied counters (null-safe),
     * mirroring the visitor's {@code AtomicLong} fallback-counter pattern.
     *
     * @return the discriminator, or null when the name is not genuinely overloaded (or the arity is
     *         unknown AND no argument types were given — the legacy no-split behaviour).
     */
    public static String resolveOverloadDiscriminator(List<SubprogramInfo> overloads, int argCount,
            List<String> argTypes, java.util.concurrent.atomic.AtomicLong arityFallbacks,
            java.util.concurrent.atomic.AtomicLong firstMatchFallbacks) {
        if (overloads == null || overloads.size() <= 1) return null;
        boolean mixed = mixedKinds(overloads);

        // 1. exact signature — only when every argument type is known.
        String want = allTypesKnown(argTypes) ? signatureOfTypes(argTypes) : null;
        if (want != null) {
            for (SubprogramInfo s : overloads) {
                if (want.equals(typeSignature(s.getParameters()))) return discriminatorFor(s, mixed);
            }
        }

        // Legacy behaviour: an unknown arity (and no types) never splits — keeps ids byte-identical.
        if (argCount < 0) return null;

        // 2. arity.
        if (arityFallbacks != null) arityFallbacks.incrementAndGet();
        List<SubprogramInfo> byArity = new ArrayList<>();
        for (SubprogramInfo s : overloads) {
            if (s.getParameters() != null && s.getParameters().size() == argCount) byArity.add(s);
        }
        if (byArity.size() == 1) return discriminatorFor(byArity.get(0), mixed);

        // 3. first match (same arity is ambiguous, or no overload matches this arity).
        if (firstMatchFallbacks != null) firstMatchFallbacks.incrementAndGet();
        SubprogramInfo pick = byArity.isEmpty() ? overloads.get(0) : byArity.get(0);
        return discriminatorFor(pick, mixed);
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
