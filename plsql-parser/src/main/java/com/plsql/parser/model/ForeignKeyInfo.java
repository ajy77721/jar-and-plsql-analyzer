package com.plsql.parser.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;

/**
 * One FOREIGN KEY relationship declared by the analysed source — the dialect-neutral form of
 * each dialect's own {@code *ForeignKeyInfo}.
 *
 * <p>Every dialect parser already recognises {@code ALTER TABLE … ADD [CONSTRAINT x] FOREIGN KEY
 * (cols) REFERENCES parent (cols)} and the inline {@code col type REFERENCES parent(col)}
 * spelling. Those parsers each build their own model object; this is the shape that survives the
 * multidb bridge into {@link ParsedObject}, which is what the artifact writer and therefore the
 * Mongo ingest actually consume. Without it, a dialect's foreign keys end at the parser.
 *
 * <p>The distinction worth preserving: every other table-to-table relationship this analyzer
 * reports is <em>inferred</em> from how DML happens to be written. A foreign key is the schema
 * stating the relationship outright, so it is kept structurally — both column lists intact —
 * rather than flattened into a pair of anonymous table operations and re-guessed downstream.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ForeignKeyInfo {

    /** Constraint name if one was given ({@code ADD CONSTRAINT fk_x …}), else {@code null}. */
    private String constraintName;

    /** The table being altered — the dependent (child) side of the edge. */
    private String table;
    private String schema;

    /** The referenced (parent) table. */
    private String referencedTable;
    private String referencedSchema;

    private List<String> columns = new ArrayList<>();
    private List<String> referencedColumns = new ArrayList<>();

    private int line;

    public ForeignKeyInfo() {
    }

    public String getConstraintName() { return constraintName; }
    public void setConstraintName(String constraintName) { this.constraintName = constraintName; }

    public String getTable() { return table; }
    public void setTable(String table) { this.table = table; }

    public String getSchema() { return schema; }
    public void setSchema(String schema) { this.schema = schema; }

    public String getReferencedTable() { return referencedTable; }
    public void setReferencedTable(String referencedTable) { this.referencedTable = referencedTable; }

    public String getReferencedSchema() { return referencedSchema; }
    public void setReferencedSchema(String referencedSchema) { this.referencedSchema = referencedSchema; }

    public List<String> getColumns() { return columns; }
    public void setColumns(List<String> columns) {
        this.columns = columns != null ? columns : new ArrayList<>();
    }

    public List<String> getReferencedColumns() { return referencedColumns; }
    public void setReferencedColumns(List<String> referencedColumns) {
        this.referencedColumns = referencedColumns != null ? referencedColumns : new ArrayList<>();
    }

    public int getLine() { return line; }
    public void setLine(int line) { this.line = line; }

    @Override
    public String toString() {
        return (constraintName != null ? constraintName + ": " : "")
                + table + columns + " -> " + referencedTable + referencedColumns;
    }
}
