package com.plsql.postgres.model;

import java.util.ArrayList;
import java.util.List;

public class PostgresTableOpInfo {

    public enum DmlType { SELECT, INSERT, UPDATE, DELETE, TRUNCATE, DYNAMIC, CREATE }

    private DmlType dmlType;
    private String  tableName;
    private String  schemaName;
    private String  alias;
    private int     line;
    private final List<PostgresJoinInfo> joins = new ArrayList<>();

    /** {@code TEMP} for a {@code CREATE TEMP|TEMPORARY TABLE}; null otherwise (parse-time only). */
    private String  tableKind;
    /** {@code SESSION} for a Postgres temp table; null for non-temp kinds. */
    private String  tempScope;

    public String getTableKind() { return tableKind; }
    public void   setTableKind(String tableKind) { this.tableKind = tableKind; }
    public String getTempScope() { return tempScope; }
    public void   setTempScope(String tempScope) { this.tempScope = tempScope; }

    public String getAlias() { return alias; }
    public void   setAlias(String alias) { this.alias = alias; }

    public List<PostgresJoinInfo> getJoins() { return joins; }

    public DmlType getDmlType()   { return dmlType; }
    public void    setDmlType(DmlType dmlType)   { this.dmlType = dmlType; }
    public String  getTableName() { return tableName; }
    public void    setTableName(String tableName) { this.tableName = tableName; }
    public String  getSchemaName() { return schemaName; }
    public void    setSchemaName(String schemaName) { this.schemaName = schemaName; }
    public int     getLine()      { return line; }
    public void    setLine(int line)             { this.line = line; }

    public String qualifiedTable() {
        return schemaName != null ? schemaName + "." + tableName : tableName;
    }

    /** The statement's SQL as written, for the shared cross-dialect column enricher. */
    private String sqlText;

    public String getSqlText() { return sqlText; }
    public void setSqlText(String sqlText) { this.sqlText = sqlText; }

    /** Column names (or "*") from this INSERT/UPDATE/DELETE's RETURNING clause, if any. */
    private final List<String> returningColumns = new ArrayList<>();

    public List<String> getReturningColumns() { return returningColumns; }
}
