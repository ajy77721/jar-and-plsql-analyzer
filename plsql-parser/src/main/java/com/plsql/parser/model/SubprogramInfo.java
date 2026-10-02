package com.plsql.parser.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.*;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class SubprogramInfo {
    private String type;
    private String name;
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
    private List<PackageVarRefInfo> externalPackageVarRefDetails = new ArrayList<>();
    private String returnType;
    // Task #26: set when this member hands result sets back (Oracle: an OUT REF CURSOR parameter).
    private boolean returnsResultSet;
    private int resultSets;
    private boolean pragmaAutonomousTransaction;
    /** True when the function has the PIPELINED keyword (table function). */
    private boolean pipelined;
    /** True when this entry is a forward declaration (from package spec) with no body. */
    private boolean forwardDeclaration;
    /**
     * Task #29: true when this subprogram is declared inside another subprogram's body (a
     * nested/local procedure or function), false for a package member or standalone routine.
     * Additive metadata only — it never participates in id generation, so ids of non-nested
     * subprograms stay byte-identical.
     */
    private boolean nested;
    /**
     * Task #29: nesting level — 0 for a top-level/package member, 1 for a subprogram declared one
     * body deep, 2 for one nested inside that, etc. Mirrors {@link #nested} (0 ⇔ nested=false).
     */
    private int scopeDepth;

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public int getLineStart() { return lineStart; }
    public void setLineStart(int ls) { this.lineStart = ls; }
    public int getLineEnd() { return lineEnd; }
    public void setLineEnd(int le) { this.lineEnd = le; }
    public List<ParameterInfo> getParameters() { return parameters; }
    public void setParameters(List<ParameterInfo> p) { this.parameters = p; }
    public List<VariableInfo> getLocalVariables() { return localVariables; }
    public void setLocalVariables(List<VariableInfo> v) { this.localVariables = v; }
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
    public List<ExceptionHandlerInfo> getExceptionHandlers() { return exceptionHandlers; }
    public void setExceptionHandlers(List<ExceptionHandlerInfo> e) { this.exceptionHandlers = e; }
    public List<String> getExternalPackageVarRefs() { return externalPackageVarRefs; }
    public void setExternalPackageVarRefs(List<String> e) { this.externalPackageVarRefs = e; }
    public List<PackageVarRefInfo> getExternalPackageVarRefDetails() { return externalPackageVarRefDetails; }
    public void setExternalPackageVarRefDetails(List<PackageVarRefInfo> e) { this.externalPackageVarRefDetails = e; }
    public String getReturnType() { return returnType; }
    public void setReturnType(String r) { this.returnType = r; }
    public boolean isReturnsResultSet() { return returnsResultSet; }
    public void setReturnsResultSet(boolean v) { this.returnsResultSet = v; }
    public int getResultSets() { return resultSets; }
    public void setResultSets(int v) { this.resultSets = v; }
    public boolean isPragmaAutonomousTransaction() { return pragmaAutonomousTransaction; }
    public void setPragmaAutonomousTransaction(boolean p) { this.pragmaAutonomousTransaction = p; }
    public boolean isPipelined() { return pipelined; }
    public void setPipelined(boolean p) { this.pipelined = p; }
    public boolean isForwardDeclaration() { return forwardDeclaration; }
    public void setForwardDeclaration(boolean f) { this.forwardDeclaration = f; }
    public boolean isNested() { return nested; }
    public void setNested(boolean n) { this.nested = n; }
    public int getScopeDepth() { return scopeDepth; }
    public void setScopeDepth(int d) { this.scopeDepth = d; }
}
