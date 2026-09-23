package com.plsql.parser.model;

/**
 * A SQL Server {@code IDENTITY(seed, increment)} column (audit #8) — the dialect's most common
 * auto-increment mechanism, distinct from a real {@code CREATE SEQUENCE} (which already reaches
 * this shared model via a synthetic {@code NEXTVAL} {@link CallInfo}). Additive-only shared
 * model class: introduced so IDENTITY facts (table, column, seed, increment) survive the
 * dialect→shared conversion instead of being dropped.
 */
public class IdentityColumnInfo {
    private String schema;
    private String tableName;
    private String columnName;
    private long seed = 1;
    private long increment = 1;
    private int line;

    public String getSchema() { return schema; }
    public void setSchema(String v) { this.schema = v; }
    public String getTableName() { return tableName; }
    public void setTableName(String v) { this.tableName = v; }
    public String getColumnName() { return columnName; }
    public void setColumnName(String v) { this.columnName = v; }
    public long getSeed() { return seed; }
    public void setSeed(long v) { this.seed = v; }
    public long getIncrement() { return increment; }
    public void setIncrement(long v) { this.increment = v; }
    public int getLine() { return line; }
    public void setLine(int v) { this.line = v; }
}
