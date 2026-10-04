package com.plsql.parser.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.*;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class SchemaTableInfo {
    private String schema;
    private String tableName;
    private String objectType;

    /**
     * The true table kind — {@code TABLE} / {@code VIEW} / {@code MATERIALIZED_VIEW} / {@code TEMP}
     * — established at parse time by the dialect visitors (see
     * {@link TableOperationInfo#getTableKind()}). {@code @JsonInclude(NON_EMPTY)} keeps it out of
     * the intermediate artifact for the default TABLE kind, so rows written before this field
     * existed stay byte-identical.
     */
    private String tableKind;

    /** The temp table's lifetime scope ({@code ON COMMIT PRESERVE ROWS} etc.); null unless TEMP. */
    private String tempScope;

    private Set<String> operations = new LinkedHashSet<>();
    private List<Integer> lines = new ArrayList<>();
    private Set<String> referencedBy = new LinkedHashSet<>();

    /**
     * Per-column usage for this table, folded across every statement that touches it.
     *
     * <p>One row per column, not per statement — see {@link TableColumnUsage}. Empty when no
     * statement against this table could be parsed for column detail, which is a normal outcome
     * for dialect SQL that ANSI parsing rejects.
     */
    private java.util.List<TableColumnUsage> columns = new ArrayList<>();

    /** DDL performed on this table — {@code TRUNCATE}, {@code ALTER}, and so on. */
    private java.util.List<com.plsqlanalyzer.parser.model.DdlInfo> ddl = new ArrayList<>();

    public String getSchema() { return schema; }
    public void setSchema(String schema) { this.schema = schema; }
    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }
    public String getObjectType() { return objectType; }
    public void setObjectType(String objectType) { this.objectType = objectType; }
    public String getTableKind() { return tableKind; }
    public void setTableKind(String tableKind) { this.tableKind = tableKind; }
    public String getTempScope() { return tempScope; }
    public void setTempScope(String tempScope) { this.tempScope = tempScope; }
    public Set<String> getOperations() { return operations; }
    public void setOperations(Set<String> operations) { this.operations = operations; }
    public List<Integer> getLines() { return lines; }
    public void setLines(List<Integer> lines) { this.lines = lines; }
    public java.util.List<TableColumnUsage> getColumns() { return columns; }
    public void setColumns(java.util.List<TableColumnUsage> columns) { this.columns = columns; }
    public java.util.List<com.plsqlanalyzer.parser.model.DdlInfo> getDdl() { return ddl; }
    public void setDdl(java.util.List<com.plsqlanalyzer.parser.model.DdlInfo> ddl) { this.ddl = ddl; }
    public Set<String> getReferencedBy() { return referencedBy; }
    public void setReferencedBy(Set<String> referencedBy) { this.referencedBy = referencedBy; }
}
