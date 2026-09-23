package com.plsqlanalyzer.parser.model;

import java.util.ArrayList;
import java.util.List;

/**
 * A DDL statement found inside PL/SQL.
 *
 * <p>Worth capturing separately from DML because it changes the shape of a table rather than its
 * contents: a procedure that quietly does {@code ALTER TABLE ... ADD} or {@code TRUNCATE} is the
 * kind of thing a reader of the table's page needs to see, and it does not show up in any
 * read/write count.
 *
 * <p>All five forms parse ({@code CreateTable}, {@code Alter}, {@code Drop}, {@code Truncate},
 * {@code CreateIndex}) — verified against JSqlParser before this model was written.
 */
public class DdlInfo {

    /** {@code CREATE_TABLE}, {@code ALTER_TABLE}, {@code DROP}, {@code TRUNCATE}, {@code CREATE_INDEX}. */
    private String action;

    /** The object acted on, uppercased. */
    private String objectName;

    /** {@code TABLE}, {@code INDEX}, … as far as the statement says. */
    private String objectType;

    /** Columns named by the statement — the added columns of an ALTER, an index's key list. */
    private List<String> columns = new ArrayList<>();

    /** The statement as written, so nothing is lost to the summary above. */
    private String rawSql;

    private int lineNumber;

    public DdlInfo() {}

    public DdlInfo(String action, String objectName, String objectType, String rawSql, int lineNumber) {
        this.action = action;
        this.objectName = objectName;
        this.objectType = objectType;
        this.rawSql = rawSql;
        this.lineNumber = lineNumber;
    }

    public String getAction() { return action; }
    public void setAction(String action) { this.action = action; }
    public String getObjectName() { return objectName; }
    public void setObjectName(String objectName) { this.objectName = objectName; }
    public String getObjectType() { return objectType; }
    public void setObjectType(String objectType) { this.objectType = objectType; }
    public List<String> getColumns() { return columns; }
    public void setColumns(List<String> columns) { this.columns = columns; }
    public String getRawSql() { return rawSql; }
    public void setRawSql(String rawSql) { this.rawSql = rawSql; }
    public int getLineNumber() { return lineNumber; }
    public void setLineNumber(int lineNumber) { this.lineNumber = lineNumber; }

    @Override
    public String toString() { return action + " " + objectType + " " + objectName; }
}
