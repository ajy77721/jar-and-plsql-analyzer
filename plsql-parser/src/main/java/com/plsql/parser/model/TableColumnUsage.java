package com.plsql.parser.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * How one column of one table is used across everything that touches it.
 *
 * <p>This is the table-page view of {@link com.plsqlanalyzer.parser.model.ColumnUsage}: the
 * per-statement records are folded together per column, so a table with 400 statements against it
 * yields one row per column rather than 400 rows. Without that folding the table page would carry
 * the same duplication the analysis document had before pooling — and would be unreadable besides.
 *
 * <p>{@link #valueSources} is the part that answers "where does this column's value come from" at a
 * glance: a column written only from {@code BIND} is very different from one written from a
 * {@code SEQUENCE} or from another {@code COLUMN}, and seeing all of the sources together is what
 * makes an odd one obvious.
 */
public class TableColumnUsage {

    private String columnName;

    /** {@code SELECT}, {@code SET}, {@code INSERT} — every way this column is used. */
    private Set<String> usages = new LinkedHashSet<>();

    /** Provenance of written values: {@code LITERAL}, {@code BIND}, {@code SEQUENCE}, … */
    private Set<String> valueSources = new LinkedHashSet<>();

    /** The objects whose statements touch this column. */
    private Set<String> referencedBy = new LinkedHashSet<>();

    /** PL/SQL variables this column is read into, from {@code SELECT ... INTO}. */
    private Set<String> intoVariables = new LinkedHashSet<>();

    /** Line numbers, capped — see {@code DependencyCrawler.MAX_COLUMN_LINES}. */
    private List<Integer> lines = new ArrayList<>();

    /**
     * How many distinct lines actually touch this column, before the cap on {@link #lines}.
     *
     * <p>The cap keeps a heavily-used column from carrying thousands of line numbers onto a table's
     * page. On its own, though, it lies: twenty-five entries look exactly like "this column is used
     * in twenty-five places", so a reader auditing where a column is written stops after the last
     * one and concludes they have seen everything. That is the failure mode this project keeps
     * finding — not a crash, but a confident and incomplete answer.
     *
     * <p>Carrying the true count costs one integer per column and lets the page say
     * {@code showing 25 of 112}, which is the difference between a sample and a claim.
     */
    private int lineCount;

    /**
     * Distinct lines seen while folding, so {@link #lineCount} stays accurate past the cap.
     *
     * <p>Transient and excluded from the document: it exists only for the duration of the crawl,
     * and serialising it would defeat the cap it is helping to report.
     */
    @JsonIgnore
    private transient Set<Integer> seenLines;

    /**
     * One representative write expression, e.g. {@code total + :p_amt}.
     *
     * <p>A single example rather than all of them: the full set is available on the statement, and
     * carrying every variant here is what would make this grow with statement count again.
     */
    private String sampleExpression;

    public TableColumnUsage() {}

    public TableColumnUsage(String columnName) { this.columnName = columnName; }

    public String getColumnName() { return columnName; }
    public void setColumnName(String columnName) { this.columnName = columnName; }
    public Set<String> getUsages() { return usages; }
    public void setUsages(Set<String> usages) { this.usages = usages; }
    public Set<String> getValueSources() { return valueSources; }
    public void setValueSources(Set<String> valueSources) { this.valueSources = valueSources; }
    public Set<String> getReferencedBy() { return referencedBy; }
    public void setReferencedBy(Set<String> referencedBy) { this.referencedBy = referencedBy; }
    public Set<String> getIntoVariables() { return intoVariables; }
    public void setIntoVariables(Set<String> intoVariables) { this.intoVariables = intoVariables; }
    public List<Integer> getLines() { return lines; }
    public void setLines(List<Integer> lines) { this.lines = lines; }
    public int getLineCount() { return lineCount; }
    public void setLineCount(int lineCount) { this.lineCount = lineCount; }

    /** True when {@link #lines} is a sample rather than the whole set. */
    @JsonIgnore
    public boolean isLinesTruncated() { return lineCount > lines.size(); }

    /**
     * Records one line against this column, keeping {@link #lineCount} exact whether or not the
     * line is kept in {@link #lines}.
     *
     * <p>The counting lives here rather than at the call site so the cap and the count cannot drift
     * apart — a count maintained separately from the list it describes is how "showing 25 of 25"
     * would silently reappear.
     *
     * @param line    the line number; ignored when not positive
     * @param maxKept how many line numbers to retain
     */
    public void addLine(int line, int maxKept) {
        if (line <= 0) return;
        if (seenLines == null) seenLines = new LinkedHashSet<>();
        if (!seenLines.add(line)) return;      // already counted: distinct lines only
        lineCount = seenLines.size();
        if (lines.size() < maxKept) lines.add(line);
    }
    public String getSampleExpression() { return sampleExpression; }
    public void setSampleExpression(String sampleExpression) { this.sampleExpression = sampleExpression; }

    @Override
    public String toString() { return columnName + " " + usages + " <- " + valueSources; }
}
