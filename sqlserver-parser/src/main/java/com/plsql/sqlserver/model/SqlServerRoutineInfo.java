package com.plsql.sqlserver.model;

import java.util.ArrayList;
import java.util.List;

public class SqlServerRoutineInfo {

    public enum RoutineType { PROCEDURE, FUNCTION, TRIGGER, VIEW, SEQUENCE, INDEX, TYPE }

    private String      schema;
    private String      name;
    private RoutineType type = RoutineType.PROCEDURE;
    private int         lineStart;
    private int         lineEnd;
    private List<SqlServerParamInfo>      params     = new ArrayList<>();
    private List<SqlServerVariableInfo>   variables  = new ArrayList<>();
    private List<SqlServerCallInfo>       calls      = new ArrayList<>();
    private List<SqlServerTableOpInfo>    tableOps   = new ArrayList<>();
    private List<SqlServerDynamicSqlInfo> dynamicSql = new ArrayList<>();
    private List<SqlServerCursorInfo>     cursors    = new ArrayList<>();
    private List<SqlServerStatementInfo>  statements = new ArrayList<>();
    private List<SqlServerExceptionHandlerInfo> exceptionHandlers = new ArrayList<>();
    private List<SqlServerSequenceInfo>   sequences  = new ArrayList<>();
    private List<String>                  errors     = new ArrayList<>();

    // Trigger metadata (populated for RoutineType.TRIGGER)
    private String triggerTable;
    private String triggerEvent;
    private String triggerTiming;
    // View defining query (populated for RoutineType.VIEW)
    private String viewQuery;
    // CREATE SEQUENCE metadata (populated for RoutineType.SEQUENCE)
    private String  sequenceDataType;
    private Long    sequenceStartWith;
    private Long    sequenceIncrementBy;
    private Long    sequenceMinValue;
    private Long    sequenceMaxValue;
    private Boolean sequenceCycle;
    private Boolean sequenceCache;
    private Long    sequenceCacheSize;
    // CREATE INDEX metadata (populated for RoutineType.INDEX)
    private String       indexTable;
    private List<String> indexColumns        = new ArrayList<>();
    private List<String> indexIncludeColumns = new ArrayList<>();
    private boolean      indexUnique;
    private String       indexClustering;   // CLUSTERED / NONCLUSTERED, null if unspecified
    private String       indexFilter;       // filtered-index WHERE predicate, null if none
    // CREATE TYPE metadata (populated for RoutineType.TYPE)
    private String       typeForm;          // "ALIAS" (FROM base type) or "TABLE" (AS TABLE (...))
    private String       typeBaseType;      // alias types only: the underlying base data type
    private Boolean      typeNotNull;       // alias types only: TRUE for NOT NULL, FALSE for NULL
    private List<String> typeColumns = new ArrayList<>();  // table types only: column names
    // Top-level ALTER TABLE metadata (populated when the unit is a standalone ALTER TABLE)
    private String       alterTableName;              // the altered table (schema-qualified as written)
    private String       alterAction;                 // ADD / DROP / ALTER / ... , null if none
    private String       alterConstraintName;         // named constraint, when one was given
    // Every FOREIGN KEY the statement adds. A single ALTER TABLE may add several constraints in
    // one comma-separated ADD list; the previous single-valued alterFkReferencedTable /
    // alterFkColumns / alterFkReferencedColumns fields could only hold one of them, so any
    // further constraint was lost. Empty for non-FK alterations.
    private List<SqlServerForeignKeyInfo> foreignKeys = new ArrayList<>();

    public String      getSchema()     { return schema; }
    public void        setSchema(String schema)              { this.schema = schema; }
    public String      getName()       { return name; }
    public void        setName(String name)                  { this.name = name; }
    public RoutineType getType()       { return type; }
    public void        setType(RoutineType type)             { this.type = type; }
    public int         getLineStart()  { return lineStart; }
    public void        setLineStart(int lineStart)           { this.lineStart = lineStart; }
    public int         getLineEnd()    { return lineEnd; }
    public void        setLineEnd(int lineEnd)               { this.lineEnd = lineEnd; }

    public List<SqlServerParamInfo>      getParams()     { return params; }
    public List<SqlServerVariableInfo>   getVariables()  { return variables; }
    public List<SqlServerCallInfo>       getCalls()      { return calls; }
    public List<SqlServerTableOpInfo>    getTableOps()   { return tableOps; }
    public List<SqlServerDynamicSqlInfo> getDynamicSql() { return dynamicSql; }
    public List<SqlServerCursorInfo>     getCursors()    { return cursors; }
    public List<SqlServerStatementInfo>  getStatements() { return statements; }
    public List<SqlServerExceptionHandlerInfo> getExceptionHandlers() { return exceptionHandlers; }
    public List<SqlServerSequenceInfo>   getSequences()  { return sequences; }
    public List<String>                  getErrors()     { return errors; }

    public String getTriggerTable()  { return triggerTable; }
    public void   setTriggerTable(String v)  { this.triggerTable = v; }
    public String getTriggerEvent()  { return triggerEvent; }
    public void   setTriggerEvent(String v)  { this.triggerEvent = v; }
    public String getTriggerTiming() { return triggerTiming; }
    public void   setTriggerTiming(String v) { this.triggerTiming = v; }
    public String getViewQuery()     { return viewQuery; }
    public void   setViewQuery(String v)     { this.viewQuery = v; }

    public String  getSequenceDataType()    { return sequenceDataType; }
    public void    setSequenceDataType(String v)    { this.sequenceDataType = v; }
    public Long    getSequenceStartWith()   { return sequenceStartWith; }
    public void    setSequenceStartWith(Long v)     { this.sequenceStartWith = v; }
    public Long    getSequenceIncrementBy() { return sequenceIncrementBy; }
    public void    setSequenceIncrementBy(Long v)   { this.sequenceIncrementBy = v; }
    public Long    getSequenceMinValue()    { return sequenceMinValue; }
    public void    setSequenceMinValue(Long v)      { this.sequenceMinValue = v; }
    public Long    getSequenceMaxValue()    { return sequenceMaxValue; }
    public void    setSequenceMaxValue(Long v)      { this.sequenceMaxValue = v; }
    public Boolean getSequenceCycle()       { return sequenceCycle; }
    public void    setSequenceCycle(Boolean v)      { this.sequenceCycle = v; }
    public Boolean getSequenceCache()       { return sequenceCache; }
    public void    setSequenceCache(Boolean v)      { this.sequenceCache = v; }
    public Long    getSequenceCacheSize()   { return sequenceCacheSize; }
    public void    setSequenceCacheSize(Long v)     { this.sequenceCacheSize = v; }

    public String       getIndexTable()          { return indexTable; }
    public void         setIndexTable(String v)          { this.indexTable = v; }
    public List<String> getIndexColumns()        { return indexColumns; }
    public List<String> getIndexIncludeColumns() { return indexIncludeColumns; }
    public boolean      isIndexUnique()          { return indexUnique; }
    public void         setIndexUnique(boolean v)        { this.indexUnique = v; }
    public String       getIndexClustering()     { return indexClustering; }
    public void         setIndexClustering(String v)     { this.indexClustering = v; }
    public String       getIndexFilter()         { return indexFilter; }
    public void         setIndexFilter(String v)         { this.indexFilter = v; }

    public String       getTypeForm()               { return typeForm; }
    public void         setTypeForm(String v)            { this.typeForm = v; }
    public String       getTypeBaseType()           { return typeBaseType; }
    public void         setTypeBaseType(String v)        { this.typeBaseType = v; }
    public Boolean      getTypeNotNull()            { return typeNotNull; }
    public void         setTypeNotNull(Boolean v)        { this.typeNotNull = v; }
    public List<String> getTypeColumns()            { return typeColumns; }

    public String       getAlterTableName()              { return alterTableName; }
    public void         setAlterTableName(String v)           { this.alterTableName = v; }
    public String       getAlterAction()                 { return alterAction; }
    public void         setAlterAction(String v)              { this.alterAction = v; }
    public String       getAlterConstraintName()         { return alterConstraintName; }
    public void         setAlterConstraintName(String v)      { this.alterConstraintName = v; }
    public List<SqlServerForeignKeyInfo> getForeignKeys() { return foreignKeys; }
}
