package com.plsqlanalyzer.parser.model;

/**
 * One column touched by a statement, and — for writes — where its value came from.
 *
 * <p>Covers the four shapes the analysis needs to answer "what does this statement do to this
 * table":
 *
 * <ul>
 *   <li><b>SELECT</b> — a selected expression, its alias, and the PL/SQL variable it is read
 *       {@code INTO}, when the statement is a {@code SELECT ... INTO}.</li>
 *   <li><b>UPDATE</b> — the column in the {@code SET} clause and the expression assigned to it.</li>
 *   <li><b>INSERT</b> — the column and the value in the corresponding position.</li>
 *   <li><b>MERGE</b> — the same, per branch.</li>
 * </ul>
 *
 * <p>{@link #valueSource} classifies where a written value comes from, which is the part a reader
 * actually asks about: a literal and a sequence and another column are very different provenance
 * even though all three are "the value".
 */
public class ColumnUsage {

    /** How the column is used: {@code SELECT}, {@code SET}, {@code INSERT}, {@code MERGE_SET}. */
    private String usage;

    /** Column name as written, without the table qualifier. Null for a computed select item. */
    private String columnName;

    /** Table or alias qualifier as written ({@code o} in {@code o.total}), when present. */
    private String qualifier;

    /** The full expression as written — {@code total + :p_amt}, {@code seq_o.NEXTVAL}. */
    private String expression;

    /** Select-list alias ({@code AS amt}), when present. */
    private String alias;

    /**
     * The PL/SQL variable this column is read into, for {@code SELECT ... INTO}.
     *
     * <p>Available because JSqlParser does parse the Oracle form and exposes the target through
     * {@code PlainSelect.getIntoTables()} — verified, not assumed.
     */
    private String intoVariable;

    /**
     * Where a written value comes from: {@code LITERAL}, {@code BIND}, {@code SEQUENCE},
     * {@code COLUMN}, {@code FUNCTION}, {@code SUBQUERY}, {@code EXPRESSION}, or null when the
     * column is only read.
     */
    private String valueSource;

    private int lineNumber;

    public ColumnUsage() {}

    public ColumnUsage(String usage, String columnName, String qualifier, String expression,
                       String alias, String intoVariable, String valueSource, int lineNumber) {
        this.usage = usage;
        this.columnName = columnName;
        this.qualifier = qualifier;
        this.expression = expression;
        this.alias = alias;
        this.intoVariable = intoVariable;
        this.valueSource = valueSource;
        this.lineNumber = lineNumber;
    }

    public String getUsage() { return usage; }
    public void setUsage(String usage) { this.usage = usage; }
    public String getColumnName() { return columnName; }
    public void setColumnName(String columnName) { this.columnName = columnName; }
    public String getQualifier() { return qualifier; }
    public void setQualifier(String qualifier) { this.qualifier = qualifier; }
    public String getExpression() { return expression; }
    public void setExpression(String expression) { this.expression = expression; }
    public String getAlias() { return alias; }
    public void setAlias(String alias) { this.alias = alias; }
    public String getIntoVariable() { return intoVariable; }
    public void setIntoVariable(String intoVariable) { this.intoVariable = intoVariable; }
    public String getValueSource() { return valueSource; }
    public void setValueSource(String valueSource) { this.valueSource = valueSource; }
    public int getLineNumber() { return lineNumber; }
    public void setLineNumber(int lineNumber) { this.lineNumber = lineNumber; }

    @Override
    public String toString() {
        return usage + " " + (columnName != null ? columnName : expression)
                + (valueSource != null ? " <- " + valueSource : "")
                + (intoVariable != null ? " INTO " + intoVariable : "");
    }
}
