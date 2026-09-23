package com.plsql.db2.model;

import java.util.ArrayList;
import java.util.List;

/**
 * A FOREIGN KEY relationship declared by {@code ALTER TABLE … ADD [CONSTRAINT x] FOREIGN KEY
 * (cols) REFERENCES refTable[(refCols)]}.
 *
 * <p>This is the one fact such a statement exists to declare: {@code table} depends on
 * {@code referencedTable}, through a named column pairing. Every other kind of table
 * relationship this analyzer reports is inferred from how DML happens to be written; this one
 * is stated outright by the schema, so it is recorded structurally instead of surviving only as
 * the rendered description string in {@code Db2RoutineInfo.alterActions} (which keeps the source
 * columns as raw text and drops the referenced columns entirely).
 *
 * <p>Mirrors {@code MySqlForeignKeyInfo} field for field so the multidb bridge into the shared
 * {@code com.plsql.parser.model.ForeignKeyInfo} is the same mapping in both dialects.
 */
public class Db2ForeignKeyInfo {

    /** Constraint name if one was given ({@code ADD CONSTRAINT fk_x …}), else {@code null}. */
    private String constraintName;

    /** The table being altered — the dependent (child) side of the edge. */
    private String table;
    private String schema;

    /** The referenced (parent) table. */
    private String referencedTable;
    private String referencedSchema;

    private final List<String> columns           = new ArrayList<>();
    private final List<String> referencedColumns = new ArrayList<>();

    private int line;

    public String getConstraintName() { return constraintName; }
    public void   setConstraintName(String v) { this.constraintName = v; }

    public String getTable() { return table; }
    public void   setTable(String v) { this.table = v; }

    public String getSchema() { return schema; }
    public void   setSchema(String v) { this.schema = v; }

    public String getReferencedTable() { return referencedTable; }
    public void   setReferencedTable(String v) { this.referencedTable = v; }

    public String getReferencedSchema() { return referencedSchema; }
    public void   setReferencedSchema(String v) { this.referencedSchema = v; }

    public List<String> getColumns() { return columns; }
    public List<String> getReferencedColumns() { return referencedColumns; }

    public int  getLine() { return line; }
    public void setLine(int v) { this.line = v; }

    @Override
    public String toString() {
        return (constraintName != null ? constraintName + ": " : "")
                + table + columns + " -> " + referencedTable + referencedColumns;
    }
}
