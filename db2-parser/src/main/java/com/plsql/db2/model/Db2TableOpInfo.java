package com.plsql.db2.model;

import java.util.ArrayList;
import java.util.List;

public class Db2TableOpInfo {

    public enum DmlType { SELECT, INSERT, UPDATE, DELETE, MERGE, DYNAMIC, CREATE }

    private DmlType dmlType;
    private String  tableName;
    private String  schemaName;
    private String  alias;
    private int     line;
    private final List<Db2JoinInfo> joins = new ArrayList<>();

    public String getAlias() { return alias; }
    public void   setAlias(String alias) { this.alias = alias; }

    public List<Db2JoinInfo> getJoins() { return joins; }

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
}
