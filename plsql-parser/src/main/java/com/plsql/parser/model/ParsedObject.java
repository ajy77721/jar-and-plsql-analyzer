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
    private List<PackageVarRefInfo> externalPackageVarRefDetails = new ArrayList<>();
    // FOREIGN KEY constraints declared by this object's source (ALTER TABLE ... ADD CONSTRAINT
    // ... FOREIGN KEY ... REFERENCES ..., and the inline column-level REFERENCES spelling).
    // Each dialect parser captures these in its own model; this is where they cross the multidb
    // bridge so the artifact writer — and therefore Mongo ingest and the UI — can see them.
    private List<ForeignKeyInfo> foreignKeys = new ArrayList<>();
    private List<ExceptionHandlerInfo> exceptionHandlers = new ArrayList<>();
    private DependencySummary dependencies = new DependencySummary();
    private String triggerEvent;
    private String triggerTable;
    private String triggerTiming;
    // Source-parsed trigger facets: the WHEN (...) condition text, the ROW/STATEMENT level, and the
    // trigger body's own source. Persisted into api/tables/index.json's triggers[] so the estate
    // trigger facet can report them (DB-discovered triggers already carry definition/status).
    private String triggerWhenClause;
    private String triggerLevel;
    private String triggerBody;
    private String viewQuery;
    /** True when this is a standalone PIPELINED FUNCTION (table function). */
    private boolean pipelined;
    /** SQL Server IDENTITY(seed, increment) columns (audit #8). Empty for every other dialect. */
    private List<IdentityColumnInfo> identityColumns = new ArrayList<>();

    // ---- CREATE SEQUENCE fields (type="SEQUENCE" only) ----
    private Long sequenceStartWith;
    private Long sequenceIncrementBy;
    private Long sequenceCache;
    private Boolean sequenceCycle;

    // ---- CREATE TYPE fields (type="TYPE" only) ----
    /** OBJECT | VARRAY | NESTED_TABLE | UNDER — the shape declared after AS/IS. */
    private String typeCategory;
    /** Element type of a collection type (TABLE OF x / VARRAY(n) OF x), or the supertype for UNDER. */
    private String typeElementType;
    /** FALSE when the declaration carries NOT FINAL; TRUE when FINAL; null when unspecified. */
    private Boolean typeFinal;
    /** FALSE when NOT INSTANTIABLE; TRUE when INSTANTIABLE; null when unspecified. */
    private Boolean typeInstantiable;

    // ---- CREATE INDEX fields (type="INDEX" only) ----
    private boolean indexUnique;
    private boolean indexBitmap;
    private List<String> indexColumns = new ArrayList<>();

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

    private boolean pragmaAutonomousTransaction;
    public boolean isPragmaAutonomousTransaction() { return pragmaAutonomousTransaction; }
    public void setPragmaAutonomousTransaction(boolean v) { this.pragmaAutonomousTransaction = v; }
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
    public List<PackageVarRefInfo> getExternalPackageVarRefDetails() { return externalPackageVarRefDetails; }
    public void setExternalPackageVarRefDetails(List<PackageVarRefInfo> e) { this.externalPackageVarRefDetails = e; }
    public List<ForeignKeyInfo> getForeignKeys() { return foreignKeys; }
    public void setForeignKeys(List<ForeignKeyInfo> f) { this.foreignKeys = f != null ? f : new ArrayList<>(); }
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
    public String getTriggerWhenClause() { return triggerWhenClause; }
    public void setTriggerWhenClause(String t) { this.triggerWhenClause = t; }
    public String getTriggerLevel() { return triggerLevel; }
    public void setTriggerLevel(String t) { this.triggerLevel = t; }
    public String getTriggerBody() { return triggerBody; }
    public void setTriggerBody(String t) { this.triggerBody = t; }
    public String getViewQuery() { return viewQuery; }
    public void setViewQuery(String v) { this.viewQuery = v; }
    public boolean isPipelined() { return pipelined; }
    public void setPipelined(boolean p) { this.pipelined = p; }
    public List<IdentityColumnInfo> getIdentityColumns() { return identityColumns; }
    public void setIdentityColumns(List<IdentityColumnInfo> v) { this.identityColumns = v; }

    public Long getSequenceStartWith() { return sequenceStartWith; }
    public void setSequenceStartWith(Long v) { this.sequenceStartWith = v; }
    public Long getSequenceIncrementBy() { return sequenceIncrementBy; }
    public void setSequenceIncrementBy(Long v) { this.sequenceIncrementBy = v; }
    public Long getSequenceCache() { return sequenceCache; }
    public void setSequenceCache(Long v) { this.sequenceCache = v; }
    public Boolean getSequenceCycle() { return sequenceCycle; }
    public void setSequenceCycle(Boolean v) { this.sequenceCycle = v; }

    public String getTypeCategory() { return typeCategory; }
    public void setTypeCategory(String v) { this.typeCategory = v; }
    public String getTypeElementType() { return typeElementType; }
    public void setTypeElementType(String v) { this.typeElementType = v; }
    public Boolean getTypeFinal() { return typeFinal; }
    public void setTypeFinal(Boolean v) { this.typeFinal = v; }
    public Boolean getTypeInstantiable() { return typeInstantiable; }
    public void setTypeInstantiable(Boolean v) { this.typeInstantiable = v; }

    public boolean isIndexUnique() { return indexUnique; }
    public void setIndexUnique(boolean v) { this.indexUnique = v; }
    public boolean isIndexBitmap() { return indexBitmap; }
    public void setIndexBitmap(boolean v) { this.indexBitmap = v; }
    public List<String> getIndexColumns() { return indexColumns; }
    public void setIndexColumns(List<String> v) { this.indexColumns = v; }
}
