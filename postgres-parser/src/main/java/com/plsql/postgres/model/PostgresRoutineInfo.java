package com.plsql.postgres.model;

import java.util.ArrayList;
import java.util.List;

public class PostgresRoutineInfo {

    // CREATE_TABLE is the identity of a top-level `CREATE TABLE t (...)` script, alongside the
    // existing ALTER_TABLE. The bridge (PostgresProcedureParser#mapType) maps every value it does
    // not name explicitly to "PROCEDURE" via its `default:` branch, so adding a value here cannot
    // break it; the table itself reaches the shared model as a CREATE table operation, which is
    // what downstream lineage actually consumes.
    public enum RoutineType { PROCEDURE, FUNCTION, TRIGGER, VIEW, SEQUENCE, INDEX, ALTER_TABLE,
                              CREATE_TABLE, TYPE, DOMAIN }

    private String       schema;
    private String       name;
    private RoutineType  type = RoutineType.PROCEDURE;
    private int          lineStart;
    private int          lineEnd;
    private List<PostgresParamInfo>      params     = new ArrayList<>();
    private List<PostgresVariableInfo>   variables  = new ArrayList<>();
    private List<PostgresCursorInfo>     cursors    = new ArrayList<>();
    private List<PostgresCallInfo>       calls      = new ArrayList<>();
    private List<PostgresTableOpInfo>    tableOps   = new ArrayList<>();
    private List<PostgresDynamicSqlInfo> dynamicSql = new ArrayList<>();
    private List<PostgresStatementInfo>  statements = new ArrayList<>();
    private List<PostgresSequenceInfo>   sequences  = new ArrayList<>();
    private List<PostgresExceptionHandlerInfo> exceptionHandlers = new ArrayList<>();
    private List<String>            errors     = new ArrayList<>();

    // Trigger metadata (populated for RoutineType.TRIGGER) — mirrors SqlServerRoutineInfo's
    // trigger fields. In PostgreSQL the trigger statement itself has no body, so
    // triggerFunction (the EXECUTE FUNCTION target) is the link to where the logic lives.
    private String triggerTable;
    private String triggerEvent;
    private String triggerTiming;
    private String triggerForEach;
    private String triggerWhen;
    private String triggerFunction;
    // View metadata (populated for RoutineType.VIEW)
    private String  viewQuery;
    private boolean materializedView;
    // Sequence metadata (populated for RoutineType.SEQUENCE, i.e. a top-level CREATE SEQUENCE).
    // Every option is optional in PostgreSQL, so each field stays null when the DDL omits it —
    // callers can therefore tell "not specified" apart from an explicitly written value.
    // sequenceCycle is a Boolean (not boolean) for the same reason: null = neither CYCLE nor
    // NO CYCLE was written.
    private String  sequenceDataType;
    private String  sequenceStart;
    private String  sequenceIncrement;
    private String  sequenceMinValue;   // "NO MINVALUE" when explicitly disabled
    private String  sequenceMaxValue;   // "NO MAXVALUE" when explicitly disabled
    private Boolean sequenceCycle;
    private String  sequenceCache;
    // Index metadata (populated for RoutineType.INDEX, i.e. a top-level CREATE INDEX).
    // indexName is separate from `name` because PostgreSQL lets the name be omitted entirely
    // (the server generates one) — in that case indexName stays null.
    private String       indexName;
    private String       indexTable;
    private String       indexTableSchema;
    private List<String> indexColumns = new ArrayList<>();
    private boolean      indexUnique;
    private boolean      indexConcurrently;
    private String       indexMethod;   // the USING <method> access method, e.g. "gin"
    private String       indexWhere;    // partial-index predicate text, null if not partial
    // ALTER TABLE metadata. Populated for RoutineType.ALTER_TABLE (a top-level ALTER TABLE) AND
    // for an ALTER TABLE embedded inside a routine/DO body — in the embedded case the routine's
    // own type/name are left untouched and only these lists grow, so the statement is no longer
    // silently lost. Lists (not single values) because one body may alter several tables.
    private List<String> alteredTables       = new ArrayList<>();
    private List<String> alterActions        = new ArrayList<>();
    // One entry per FOREIGN KEY constraint added, formatted "childTable->parentTable" — the
    // lineage edge that used to vanish entirely.
    private List<String> alterForeignKeyRefs = new ArrayList<>();
    // The same edges, kept structurally: constraint name plus BOTH column lists, which the
    // "child->parent" string above throws away even though the parse tree carries them. This is
    // the list the multidb bridge converts into the shared ForeignKeyInfo, so it is what reaches
    // the artifact writer, Mongo ingest and the UI. alterForeignKeyRefs is retained because
    // existing tests and callers read it.
    private List<PostgresForeignKeyInfo> foreignKeys = new ArrayList<>();
    // Table-inheritance edges from `CREATE TABLE child (...) INHERITS (parent, ...)`, formatted
    // "childTable->parentTable" — the same shape and the same purpose as alterForeignKeyRefs
    // above. Each parent is ALSO recorded as a SELECT-kind entry in tableOps (see
    // PostgresAnalysisVisitor#recordInherits), which is the part that crosses the multidb bridge
    // into the shared model; this list is the dialect-local, human-readable form of the edge.
    private List<String> tableInheritsRefs = new ArrayList<>();
    private String       alterTableName;      // last/only table altered (top-level convenience)
    private String       alterTableSchema;
    // CREATE TYPE / CREATE DOMAIN metadata (RoutineType.TYPE / RoutineType.DOMAIN).
    // typeKind is "ENUM", "COMPOSITE", "SHELL" or "DOMAIN" so callers can tell the three
    // CREATE TYPE shapes apart from each other and from a domain without re-parsing.
    private String       typeKind;
    private List<String> typeEnumLabels = new ArrayList<>();  // enum labels, quotes stripped
    private List<String> typeAttributes = new ArrayList<>();  // composite attrs, "name TYPE"
    private String       domainBaseType;                      // the domain's underlying type
    private String       domainCheck;                         // CHECK predicate text, null if none

    public String      getSchema()     { return schema; }
    public void        setSchema(String schema)         { this.schema = schema; }
    public String      getName()       { return name; }
    public void        setName(String name)             { this.name = name; }
    public RoutineType getType()       { return type; }
    public void        setType(RoutineType type)        { this.type = type; }
    public int         getLineStart()  { return lineStart; }
    public void        setLineStart(int lineStart)      { this.lineStart = lineStart; }
    public int         getLineEnd()    { return lineEnd; }
    public void        setLineEnd(int lineEnd)          { this.lineEnd = lineEnd; }

    public List<PostgresParamInfo>      getParams()     { return params; }
    public List<PostgresVariableInfo>   getVariables()  { return variables; }
    public List<PostgresCursorInfo>     getCursors()    { return cursors; }
    public List<PostgresCallInfo>       getCalls()      { return calls; }
    public List<PostgresTableOpInfo>    getTableOps()   { return tableOps; }
    public List<PostgresDynamicSqlInfo> getDynamicSql() { return dynamicSql; }
    public List<PostgresStatementInfo>  getStatements() { return statements; }
    public List<PostgresSequenceInfo>   getSequences()  { return sequences; }
    public List<PostgresExceptionHandlerInfo> getExceptionHandlers() { return exceptionHandlers; }
    public List<String>            getErrors()     { return errors; }

    public String  getTriggerTable()    { return triggerTable; }
    public void    setTriggerTable(String v)    { this.triggerTable = v; }
    public String  getTriggerEvent()    { return triggerEvent; }
    public void    setTriggerEvent(String v)    { this.triggerEvent = v; }
    public String  getTriggerTiming()   { return triggerTiming; }
    public void    setTriggerTiming(String v)   { this.triggerTiming = v; }
    public String  getTriggerForEach()  { return triggerForEach; }
    public void    setTriggerForEach(String v)  { this.triggerForEach = v; }
    public String  getTriggerWhen()     { return triggerWhen; }
    public void    setTriggerWhen(String v)     { this.triggerWhen = v; }
    public String  getTriggerFunction() { return triggerFunction; }
    public void    setTriggerFunction(String v) { this.triggerFunction = v; }
    public String  getViewQuery()       { return viewQuery; }
    public void    setViewQuery(String v)       { this.viewQuery = v; }
    public boolean isMaterializedView() { return materializedView; }
    public void    setMaterializedView(boolean v) { this.materializedView = v; }

    public String  getSequenceDataType()  { return sequenceDataType; }
    public void    setSequenceDataType(String v)  { this.sequenceDataType = v; }
    public String  getSequenceStart()     { return sequenceStart; }
    public void    setSequenceStart(String v)     { this.sequenceStart = v; }
    public String  getSequenceIncrement() { return sequenceIncrement; }
    public void    setSequenceIncrement(String v) { this.sequenceIncrement = v; }
    public String  getSequenceMinValue()  { return sequenceMinValue; }
    public void    setSequenceMinValue(String v)  { this.sequenceMinValue = v; }
    public String  getSequenceMaxValue()  { return sequenceMaxValue; }
    public void    setSequenceMaxValue(String v)  { this.sequenceMaxValue = v; }
    public Boolean getSequenceCycle()     { return sequenceCycle; }
    public void    setSequenceCycle(Boolean v)    { this.sequenceCycle = v; }
    public String  getSequenceCache()     { return sequenceCache; }
    public void    setSequenceCache(String v)     { this.sequenceCache = v; }

    public String       getIndexName()         { return indexName; }
    public void         setIndexName(String v)         { this.indexName = v; }
    public String       getIndexTable()        { return indexTable; }
    public void         setIndexTable(String v)        { this.indexTable = v; }
    public String       getIndexTableSchema()  { return indexTableSchema; }
    public void         setIndexTableSchema(String v)  { this.indexTableSchema = v; }
    public List<String> getIndexColumns()      { return indexColumns; }
    public boolean      isIndexUnique()        { return indexUnique; }
    public void         setIndexUnique(boolean v)      { this.indexUnique = v; }
    public boolean      isIndexConcurrently()  { return indexConcurrently; }
    public void         setIndexConcurrently(boolean v) { this.indexConcurrently = v; }
    public String       getIndexMethod()       { return indexMethod; }
    public void         setIndexMethod(String v)       { this.indexMethod = v; }
    public String       getIndexWhere()        { return indexWhere; }
    public void         setIndexWhere(String v)        { this.indexWhere = v; }

    public List<String> getAlteredTables()       { return alteredTables; }
    public List<String> getAlterActions()        { return alterActions; }
    public List<String> getAlterForeignKeyRefs() { return alterForeignKeyRefs; }
    public List<String> getTableInheritsRefs()   { return tableInheritsRefs; }
    public List<PostgresForeignKeyInfo> getForeignKeys() { return foreignKeys; }
    public String       getAlterTableName()        { return alterTableName; }
    public void         setAlterTableName(String v)   { this.alterTableName = v; }
    public String       getAlterTableSchema()      { return alterTableSchema; }
    public void         setAlterTableSchema(String v) { this.alterTableSchema = v; }

    public String       getTypeKind()            { return typeKind; }
    public void         setTypeKind(String v)    { this.typeKind = v; }
    public List<String> getTypeEnumLabels()      { return typeEnumLabels; }
    public List<String> getTypeAttributes()      { return typeAttributes; }
    public String       getDomainBaseType()      { return domainBaseType; }
    public void         setDomainBaseType(String v) { this.domainBaseType = v; }
    public String       getDomainCheck()         { return domainCheck; }
    public void         setDomainCheck(String v) { this.domainCheck = v; }
}
