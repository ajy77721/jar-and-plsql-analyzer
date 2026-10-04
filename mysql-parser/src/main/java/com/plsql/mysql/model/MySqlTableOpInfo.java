package com.plsql.mysql.model;

import java.util.ArrayList;
import java.util.List;

/** A DML operation (SELECT / INSERT / UPDATE / DELETE) detected in a MySQL body. */
public class MySqlTableOpInfo {

    /**
     * ALTER  — the table an {@code ALTER TABLE} statement changes (matches
     *          SqlServerTableOpInfo.DmlType.ALTER, so both dialects name it identically).
     * REFERENCE — the table a FOREIGN KEY constraint points AT. Not a write and not a read,
     *          but a real, declared dependency, so it belongs in tableOps rather than only in
     *          {@link MySqlForeignKeyInfo}: consumers that walk table-ops for lineage see the
     *          edge without needing to know the foreign-key list exists.
     */
    public enum DmlType { SELECT, INSERT, UPDATE, DELETE, REPLACE, TRUNCATE, DYNAMIC, CREATE,
                          ALTER, REFERENCE }

    private DmlType dmlType;
    private String  tableName;
    private String  schemaName;
    private String  alias;
    private int     line;
    private final List<MySqlJoinInfo> joins = new ArrayList<>();

    /** {@code TEMP} for a {@code CREATE TEMPORARY TABLE}; null otherwise (parse-time only). */
    private String  tableKind;
    /** {@code SESSION} for a MySQL temp table; null for non-temp kinds. */
    private String  tempScope;

    public String getTableKind() { return tableKind; }
    public void   setTableKind(String tableKind) { this.tableKind = tableKind; }
    public String getTempScope() { return tempScope; }
    public void   setTempScope(String tempScope) { this.tempScope = tempScope; }

    /**
     * The statement's SQL as written, untruncated.
     *
     * <p>Carried so the shared cross-dialect enricher can derive column-level detail (which columns
     * are written, and from where) without this module having to implement that itself. Every
     * dialect contributes the same one field; the analysis is written once.
     */
    private String sqlText;

    /**
     * Index into the owning routine's {@code exceptionHandlers} list when this op is inside a
     * {@code DECLARE ... HANDLER} body, else {@code null} — see {@link MySqlCallInfo}.
     */
    private Integer handlerIndex;

    public Integer getHandlerIndex()          { return handlerIndex; }
    public void    setHandlerIndex(Integer i) { this.handlerIndex = i; }

    public DmlType getDmlType()           { return dmlType; }
    public void    setDmlType(DmlType t)  { this.dmlType = t; }

    public String  getTableName()             { return tableName; }
    public void    setTableName(String t)     { this.tableName = t; }

    public String  getSchemaName()            { return schemaName; }
    public void    setSchemaName(String s)    { this.schemaName = s; }

    public String  getAlias()            { return alias; }
    public void    setAlias(String a)    { this.alias = a; }

    public int  getLine()      { return line; }
    public void setLine(int l) { this.line = l; }

    public List<MySqlJoinInfo> getJoins() { return joins; }

    public String qualifiedTable() {
        return (schemaName != null && !schemaName.isBlank())
                ? schemaName + "." + tableName : tableName;
    }

    public String getSqlText() { return sqlText; }
    public void setSqlText(String sqlText) { this.sqlText = sqlText; }
}
