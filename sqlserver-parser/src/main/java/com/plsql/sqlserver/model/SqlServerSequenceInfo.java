package com.plsql.sqlserver.model;

/**
 * A sequence-like usage: either a real {@code NEXT VALUE FOR schema.seq} reference, or (audit
 * #8) an {@code IDENTITY(seed, increment)} column definition — SQL Server's other, far more
 * common auto-increment mechanism. Both share the same "auto-generated numeric value" shape, so
 * one model carries both rather than introducing a second concept; {@link #identityColumn}
 * distinguishes which case a given row is.
 */
public class SqlServerSequenceInfo {
    private String name;
    private String schema;
    private int line;

    // IDENTITY-column fields (audit #8). Unset/false for a real NEXT-VALUE-FOR sequence usage.
    private boolean identityColumn;
    private String  tableName;
    private String  columnName;
    private Long     seed;
    private Long     increment;

    public String getName() { return name; }
    public void setName(String v) { this.name = v; }
    public String getSchema() { return schema; }
    public void setSchema(String v) { this.schema = v; }
    public int getLine() { return line; }
    public void setLine(int v) { this.line = v; }

    public boolean isIdentityColumn() { return identityColumn; }
    public void setIdentityColumn(boolean v) { this.identityColumn = v; }
    public String getTableName() { return tableName; }
    public void setTableName(String v) { this.tableName = v; }
    public String getColumnName() { return columnName; }
    public void setColumnName(String v) { this.columnName = v; }
    public Long getSeed() { return seed; }
    public void setSeed(Long v) { this.seed = v; }
    public Long getIncrement() { return increment; }
    public void setIncrement(Long v) { this.increment = v; }
}
