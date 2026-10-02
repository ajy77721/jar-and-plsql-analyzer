package com.plsql.db2.model;

import java.util.ArrayList;
import java.util.List;

public class Db2RoutineInfo {

    public enum RoutineType { PROCEDURE, FUNCTION, TRIGGER, VIEW, SEQUENCE, INDEX, ALTER_TABLE, TYPE }

    private String       schema;
    private String       name;
    private RoutineType  type = RoutineType.PROCEDURE;
    private int          lineStart;
    private int          lineEnd;
    private List<Db2ParamInfo>      params     = new ArrayList<>();
    private List<Db2VariableInfo>   variables  = new ArrayList<>();
    private List<Db2CursorInfo>     cursors    = new ArrayList<>();
    private List<Db2CallInfo>       calls      = new ArrayList<>();
    private List<Db2TableOpInfo>    tableOps   = new ArrayList<>();
    private List<Db2DynamicSqlInfo> dynamicSql = new ArrayList<>();
    private List<Db2StatementInfo>  statements = new ArrayList<>();
    private List<String>            errors     = new ArrayList<>();
    private List<Db2SequenceInfo>   sequences  = new ArrayList<>();
    private List<Db2ExceptionHandlerInfo> exceptionHandlers = new ArrayList<>();

    /**
     * Whether the routine's compound statement is {@code BEGIN ATOMIC ... END} vs. plain
     * {@code BEGIN ... END} (implicitly NOT ATOMIC). DB2's ATOMIC/NOT ATOMIC affects whether a
     * failed statement partially rolls back just that compound block or the whole unit of work —
     * DB2's closest analogue to Oracle's autonomous-transaction pragma. Previously the grammar
     * accepted the ATOMIC token for parse-success only; nothing captured it.
     */
    private boolean atomic;

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

    public List<Db2ParamInfo>      getParams()     { return params; }
    public List<Db2VariableInfo>   getVariables()  { return variables; }
    public List<Db2CursorInfo>     getCursors()    { return cursors; }
    public List<Db2CallInfo>       getCalls()      { return calls; }
    public List<Db2TableOpInfo>    getTableOps()   { return tableOps; }
    public List<Db2DynamicSqlInfo> getDynamicSql() { return dynamicSql; }
    public List<Db2StatementInfo>  getStatements() { return statements; }
    public List<String>            getErrors()     { return errors; }
    public boolean getAtomic() { return atomic; }
    public void    setAtomic(boolean atomic) { this.atomic = atomic; }

    // ── Trigger metadata (populated for RoutineType.TRIGGER) ────────────────────
    // Mirrors SqlServerRoutineInfo's trigger fields so the two dialects expose the
    // same shape. triggerReferencing holds the REFERENCING correlation names as
    // written, e.g. ["OLD AS o", "NEW AS n"].
    private String triggerTable;
    private String triggerEvent;
    private String triggerTiming;
    private String triggerGranularity;
    private final List<String> triggerReferencing = new ArrayList<>();

    /** View defining query text (populated for RoutineType.VIEW). */
    private String viewQuery;

    public String getTriggerTable()  { return triggerTable; }
    public void   setTriggerTable(String v)  { this.triggerTable = v; }
    public String getTriggerEvent()  { return triggerEvent; }
    public void   setTriggerEvent(String v)  { this.triggerEvent = v; }
    public String getTriggerTiming() { return triggerTiming; }
    public void   setTriggerTiming(String v) { this.triggerTiming = v; }
    public String getTriggerGranularity() { return triggerGranularity; }
    public void   setTriggerGranularity(String v) { this.triggerGranularity = v; }
    public List<String> getTriggerReferencing() { return triggerReferencing; }
    public String getViewQuery()     { return viewQuery; }
    public void   setViewQuery(String v)     { this.viewQuery = v; }

    // ── CREATE SEQUENCE metadata (populated for RoutineType.SEQUENCE) ───────────
    // Option values are kept as the literal text written in the DDL (so "-1" and a
    // missing value are distinguishable); the NO <option> forms set the matching
    // boolean flag instead.
    private String  sequenceDataType;
    private String  sequenceStartWith;
    private String  sequenceIncrementBy;
    private String  sequenceMinValue;
    private String  sequenceMaxValue;
    private String  sequenceCache;
    private boolean sequenceCycle;
    private boolean sequenceNoCycle;
    private boolean sequenceNoCache;
    private boolean sequenceNoMinValue;
    private boolean sequenceNoMaxValue;
    private boolean sequenceOrder;

    public String  getSequenceDataType()    { return sequenceDataType; }
    public void    setSequenceDataType(String v)    { this.sequenceDataType = v; }
    public String  getSequenceStartWith()   { return sequenceStartWith; }
    public void    setSequenceStartWith(String v)   { this.sequenceStartWith = v; }
    public String  getSequenceIncrementBy() { return sequenceIncrementBy; }
    public void    setSequenceIncrementBy(String v) { this.sequenceIncrementBy = v; }
    public String  getSequenceMinValue()    { return sequenceMinValue; }
    public void    setSequenceMinValue(String v)    { this.sequenceMinValue = v; }
    public String  getSequenceMaxValue()    { return sequenceMaxValue; }
    public void    setSequenceMaxValue(String v)    { this.sequenceMaxValue = v; }
    public String  getSequenceCache()       { return sequenceCache; }
    public void    setSequenceCache(String v)       { this.sequenceCache = v; }
    public boolean isSequenceCycle()        { return sequenceCycle; }
    public void    setSequenceCycle(boolean v)      { this.sequenceCycle = v; }
    public boolean isSequenceNoCycle()      { return sequenceNoCycle; }
    public void    setSequenceNoCycle(boolean v)    { this.sequenceNoCycle = v; }
    public boolean isSequenceNoCache()      { return sequenceNoCache; }
    public void    setSequenceNoCache(boolean v)    { this.sequenceNoCache = v; }
    public boolean isSequenceNoMinValue()   { return sequenceNoMinValue; }
    public void    setSequenceNoMinValue(boolean v) { this.sequenceNoMinValue = v; }
    public boolean isSequenceNoMaxValue()   { return sequenceNoMaxValue; }
    public void    setSequenceNoMaxValue(boolean v) { this.sequenceNoMaxValue = v; }
    public boolean isSequenceOrder()        { return sequenceOrder; }
    public void    setSequenceOrder(boolean v)      { this.sequenceOrder = v; }

    // ── CREATE INDEX metadata (populated for RoutineType.INDEX) ────────────────
    // indexColumns holds the key columns in declaration order, with an explicit sort
    // direction appended when written, e.g. ["customer_id", "created_at DESC"].
    private String  indexTable;
    private boolean indexUnique;
    private final List<String> indexColumns = new ArrayList<>();

    public String  getIndexTable()  { return indexTable; }
    public void    setIndexTable(String v) { this.indexTable = v; }
    public boolean isIndexUnique()  { return indexUnique; }
    public void    setIndexUnique(boolean v) { this.indexUnique = v; }
    public List<String> getIndexColumns() { return indexColumns; }

    // ── ALTER TABLE metadata (populated for RoutineType.ALTER_TABLE) ───────────
    // alterTableName is the qualified name of the altered table. alterActions holds one
    // short descriptor per action in written order, e.g.
    //   ["ADD COLUMN priority INT", "ADD CONSTRAINT fk_customer FOREIGN KEY -> customers"].
    // alterReferencedTables holds the qualified target of every ADD ... FOREIGN KEY ...
    // REFERENCES clause — the lineage-bearing part of the statement; the same tables are
    // also emitted as SELECT-flavoured table ops so generic lineage consumers see them.
    private String alterTableName;
    private final List<String> alterActions           = new ArrayList<>();
    private final List<String> alterReferencedTables  = new ArrayList<>();
    private final List<String> alterAddedColumns      = new ArrayList<>();
    private final List<String> alterDroppedColumns    = new ArrayList<>();

    // foreignKeys is the structured form of the same ADD ... FOREIGN KEY clauses: the
    // constraint name, both column lists and the qualified endpoints, kept intact so the
    // multidb bridge can hand a real relationship to the shared model. alterActions and
    // alterReferencedTables stay because existing consumers/tests read them, but they are a
    // rendered description — the referenced column list never appeared there at all.
    private final List<Db2ForeignKeyInfo> foreignKeys = new ArrayList<>();

    public String getAlterTableName() { return alterTableName; }
    public void   setAlterTableName(String v) { this.alterTableName = v; }
    public List<String> getAlterActions()          { return alterActions; }
    public List<String> getAlterReferencedTables() { return alterReferencedTables; }
    public List<String> getAlterAddedColumns()     { return alterAddedColumns; }
    public List<String> getAlterDroppedColumns()   { return alterDroppedColumns; }
    public List<Db2ForeignKeyInfo> getForeignKeys() { return foreignKeys; }

    // ── CREATE TYPE metadata (populated for RoutineType.TYPE) ─────────────────
    // typeKind is "STRUCTURED" for CREATE TYPE ... AS (attrs) MODE DB2SQL and "DISTINCT"
    // for CREATE DISTINCT TYPE ... AS <base-type>. typeBaseType is the source type of a
    // distinct type (null for structured); typeAttributes holds "name TYPE" per attribute
    // of a structured type, in declaration order (empty for distinct).
    private String typeKind;
    private String typeBaseType;
    private String typeMode;
    private boolean typeWithComparisons;
    private final List<String> typeAttributes = new ArrayList<>();

    public String  getTypeKind()     { return typeKind; }
    public void    setTypeKind(String v)     { this.typeKind = v; }
    public String  getTypeBaseType() { return typeBaseType; }
    public void    setTypeBaseType(String v) { this.typeBaseType = v; }
    public String  getTypeMode()     { return typeMode; }
    public void    setTypeMode(String v)     { this.typeMode = v; }
    public boolean isTypeWithComparisons()   { return typeWithComparisons; }
    public void    setTypeWithComparisons(boolean v) { this.typeWithComparisons = v; }
    public List<String> getTypeAttributes()  { return typeAttributes; }

    public List<Db2SequenceInfo> getSequences() { return sequences; }
    public List<Db2ExceptionHandlerInfo> getExceptionHandlers() { return exceptionHandlers; }

    // ── Result-set metadata (task #26) ─────────────────────────────────────────
    // A DB2 procedure returns a result set through a cursor declared WITH RETURN; resultSets counts
    // those declarations, returnsResultSet is resultSets > 0.
    private boolean returnsResultSet;
    private int     resultSets;

    public boolean isReturnsResultSet() { return returnsResultSet; }
    public void    setReturnsResultSet(boolean v) { this.returnsResultSet = v; }
    public int     getResultSets() { return resultSets; }
    public void    setResultSets(int v) { this.resultSets = v; }
}
