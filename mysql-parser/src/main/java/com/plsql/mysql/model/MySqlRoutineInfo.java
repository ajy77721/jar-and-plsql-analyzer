package com.plsql.mysql.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Structured representation of a single MySQL PROCEDURE or FUNCTION
 * extracted by the grammar visitor.
 *
 * Kept separate from the shared ParseResult model so this module
 * can evolve independently.
 */
public class MySqlRoutineInfo {

    public enum RoutineType { PROCEDURE, FUNCTION, TRIGGER, VIEW, INDEX, ALTER_TABLE, EVENT }

    // ── Identity ──────────────────────────────────────────────────────────────
    private String      schema;
    private String      name;
    private RoutineType type;
    private int         lineStart = 1;
    private int         lineEnd   = 1;

    // ── Trigger metadata (populated only for RoutineType.TRIGGER) ─────────────
    // Mirrors SqlServerRoutineInfo's triggerTable/triggerEvent/triggerTiming fields
    // so both dialects expose trigger facts under the same names.
    private String      triggerTable;
    private String      triggerEvent;
    private String      triggerTiming;

    // ── Event metadata (populated only for RoutineType.EVENT) ────────────────
    // MySQL's scheduler object: the ON SCHEDULE clause text (e.g. "EVERY 1 DAY"), whether
    // the event is ENABLEd (true), DISABLEd (false) or unspecified (null), and the DO body
    // source. The body is also walked by the ordinary visitor, so CALLs/DML inside it land
    // in the shared lists exactly like a procedure body.
    private String      eventSchedule;
    private Boolean     eventEnabled;
    private String      eventBody;

    // ── Index metadata (populated only for RoutineType.INDEX) ────────────────
    // Same shape as the trigger fields above: the indexed table under indexTable,
    // the indexed columns in declaration order, the UNIQUE/FULLTEXT/SPATIAL flavour
    // under indexKind (null for a plain index) and the USING type under indexType.
    private String       indexTable;
    private String       indexKind;
    private String       indexType;
    private final List<String> indexColumns = new ArrayList<>();

    // ── Foreign keys declared by ALTER TABLE / inline column REFERENCES ───────
    // The one fact a FOREIGN KEY statement exists to state — table X depends on
    // table Y, through these columns — recorded structurally rather than left to be
    // re-inferred from DML. Both tables ALSO appear in tableOps (ALTER on the altered
    // table, REFERENCE on the referenced one) so plain table-level lineage sees the
    // edge without having to know this list exists.
    private final List<MySqlForeignKeyInfo> foreignKeys = new ArrayList<>();

    // ── Parameters ────────────────────────────────────────────────────────────
    private final List<MySqlParamInfo>     params      = new ArrayList<>();

    // ── Variables declared inside the body ───────────────────────────────────
    private final List<MySqlVariableInfo>  variables   = new ArrayList<>();

    // ── Cursors declared inside the body ─────────────────────────────────────
    private final List<MySqlCursorInfo>    cursors     = new ArrayList<>();

    // ── CALL statements (outbound procedure calls) ────────────────────────────
    private final List<MySqlCallInfo>      calls       = new ArrayList<>();

    // ── DML table operations ──────────────────────────────────────────────────
    private final List<MySqlTableOpInfo>   tableOps    = new ArrayList<>();

    // ── Dynamic SQL (PREPARE/EXECUTE) ─────────────────────────────────────────
    private final List<MySqlDynamicSqlInfo> dynamicSql = new ArrayList<>();

    // ── Transaction-control statements (COMMIT / ROLLBACK / SAVEPOINT) ─────────
    private final List<MySqlStatementInfo> statements = new ArrayList<>();

    // ── DECLARE ... HANDLER blocks ────────────────────────────────────────────
    private final List<MySqlExceptionHandlerInfo> exceptionHandlers = new ArrayList<>();

    // ── Parse errors ──────────────────────────────────────────────────────────
    private final List<String>             errors      = new ArrayList<>();

    // ── Getters / setters ─────────────────────────────────────────────────────

    public String      getSchema()     { return schema; }
    public void        setSchema(String s) { this.schema = s; }

    public String      getName()       { return name; }
    public void        setName(String n) { this.name = n; }

    public RoutineType getType()       { return type; }
    public void        setType(RoutineType t) { this.type = t; }

    public String      getTriggerTable()  { return triggerTable; }
    public void        setTriggerTable(String v)  { this.triggerTable = v; }

    public String      getTriggerEvent()  { return triggerEvent; }
    public void        setTriggerEvent(String v)  { this.triggerEvent = v; }

    public String      getTriggerTiming() { return triggerTiming; }
    public void        setTriggerTiming(String v) { this.triggerTiming = v; }

    public String      getEventSchedule() { return eventSchedule; }
    public void        setEventSchedule(String v) { this.eventSchedule = v; }

    public Boolean     getEventEnabled() { return eventEnabled; }
    public void        setEventEnabled(Boolean v) { this.eventEnabled = v; }

    public String      getEventBody() { return eventBody; }
    public void        setEventBody(String v) { this.eventBody = v; }

    public String      getIndexTable()  { return indexTable; }
    public void        setIndexTable(String v)  { this.indexTable = v; }

    public String      getIndexKind()   { return indexKind; }
    public void        setIndexKind(String v)   { this.indexKind = v; }

    public String      getIndexType()   { return indexType; }
    public void        setIndexType(String v)   { this.indexType = v; }

    public boolean     isUniqueIndex()  { return "UNIQUE".equals(indexKind); }

    public List<String> getIndexColumns() { return indexColumns; }

    public int         getLineStart()  { return lineStart; }
    public void        setLineStart(int l) { this.lineStart = l; }

    public int         getLineEnd()    { return lineEnd; }
    public void        setLineEnd(int l) { this.lineEnd = l; }

    public List<MySqlForeignKeyInfo> getForeignKeys() { return foreignKeys; }

    public List<MySqlParamInfo>      getParams()     { return params; }
    public List<MySqlVariableInfo>   getVariables()  { return variables; }
    public List<MySqlCursorInfo>     getCursors()    { return cursors; }
    public List<MySqlCallInfo>       getCalls()      { return calls; }
    public List<MySqlTableOpInfo>    getTableOps()   { return tableOps; }
    public List<MySqlDynamicSqlInfo> getDynamicSql() { return dynamicSql; }
    public List<MySqlStatementInfo>  getStatements() { return statements; }
    public List<MySqlExceptionHandlerInfo> getExceptionHandlers() { return exceptionHandlers; }
    public List<String>              getErrors()     { return errors; }
}
