package com.plsql.parser.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.*;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class TableOperationInfo {
    private String tableName;
    private String schema;
    private String operation;
    private int line;
    private String objectType;
    private String alias;
    private String subqueryContext;
    private List<JoinInfo> joins = new ArrayList<>();
    private String dbLink;
    private String mergeClause;

    /** null = never attempted; true = ANSI parse succeeded; false = it failed. */
    private Boolean columnsParsed;

    /**
     * The statement's SQL as written, untruncated.
     *
     * <p>The cross-dialect route to column-level detail. Every dialect engine is ANTLR-based, so
     * the original text is recoverable from the token interval; once it is here, one shared
     * enricher ({@code TableOperationEnricher}) can produce {@link #columnUsages} and {@link #ddl}
     * for all five datasources instead of four dialect-specific implementations.
     *
     * <p>Not to be confused with {@code StatementInfo.sqlText}, which the Oracle visitor truncates
     * to 200 characters for display — a truncated statement does not parse.
     */
    private String sqlText;

    /**
     * Which columns this operation reads or writes, and for writes where the value came from.
     * Filled by the shared enricher; empty when the statement could not be parsed.
     */
    private List<com.plsqlanalyzer.parser.model.ColumnUsage> columnUsages = new ArrayList<>();

    /** DDL detail when this operation is CREATE/ALTER/DROP/TRUNCATE/CREATE INDEX. */
    private List<com.plsqlanalyzer.parser.model.DdlInfo> ddl = new ArrayList<>();

    /**
     * The statement's predicate — the {@code WHERE}/{@code ON} conditions (Sep05-01).
     *
     * <h2>Why this field had to exist</h2>
     *
     * <p>The SQL analyser has always produced {@code whereFilters}, and the enricher transferred
     * only {@code columnUsages} and {@code ddl} — so every predicate was computed and dropped.
     * There was nowhere on this type to put one.
     *
     * <p>That was not a cosmetic gap. A predicate column never becomes a {@code ColumnUsage} at
     * all: for
     * {@code SELECT qty_on_hand FROM erp_stock_level WHERE product_id = :p AND warehouse_id = 7}
     * the analyser yields one usage ({@code qty_on_hand}) and two filters. So for a table used
     * mainly as a join or filter target, <b>every</b> column fact about it lived in the discarded
     * collection — which is why {@code ERP_PRODUCT}, with 475 accesses, showed an empty column cell
     * on all but the rows where it happened to sit in a select list. It is also why {@code DELETE}
     * and {@code MERGE} measured 0% column coverage: a DELETE has no select list and no SET, so all
     * of its column information is predicate.
     *
     * <h2>Scope, stated honestly</h2>
     *
     * <p>These are the predicates of the <b>statement</b>, not provably of this table.
     * {@link com.plsqlanalyzer.parser.model.WhereFilter} carries no qualifier, so on a multi-table
     * join a filter cannot be attributed to one side. {@link #predicateScope} records which case a
     * row is in, so the screen can say "this statement's predicate" rather than implying the
     * column belongs to this table. Over-claiming here would repeat the mistake this field fixes.
     */
    private List<com.plsqlanalyzer.parser.model.WhereFilter> whereFilters = new ArrayList<>();

    /** {@code "table"} when the statement referenced one table (so the predicate is exact), else {@code "statement"}. */
    private String predicateScope;

    /** Column names (or {@code "*"}) from this INSERT/UPDATE/DELETE's RETURNING clause, if any. */
    private List<String> returningColumns = new ArrayList<>();

    /**
     * Which subprogram within a package this table operation originated from, set ONLY by
     * {@code DependencyCrawler}'s package-as-entry-point aggregation (see {@link CallInfo#getSubprogramName()}
     * for the full rationale). Null for every other crawl.
     */
    private String subprogramName;

    public String getSubprogramName() { return subprogramName; }
    public void setSubprogramName(String subprogramName) { this.subprogramName = subprogramName; }

    public List<String> getReturningColumns() { return returningColumns; }
    public void setReturningColumns(List<String> r) { this.returningColumns = r; }

    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }
    public String getSchema() { return schema; }
    public void setSchema(String schema) { this.schema = schema; }
    public String getOperation() { return operation; }
    public void setOperation(String operation) { this.operation = operation; }
    public int getLine() { return line; }
    public void setLine(int line) { this.line = line; }
    public String getObjectType() { return objectType; }
    public void setObjectType(String objectType) { this.objectType = objectType; }
    public String getAlias() { return alias; }
    public void setAlias(String alias) { this.alias = alias; }
    public String getSubqueryContext() { return subqueryContext; }
    public void setSubqueryContext(String subqueryContext) { this.subqueryContext = subqueryContext; }
    public List<JoinInfo> getJoins() { return joins; }
    public void setJoins(List<JoinInfo> joins) { this.joins = joins; }
    public String getDbLink() { return dbLink; }
    public void setDbLink(String dbLink) { this.dbLink = dbLink; }
    public String getSqlText() { return sqlText; }
    public void setSqlText(String sqlText) { this.sqlText = sqlText; }
    public List<com.plsqlanalyzer.parser.model.ColumnUsage> getColumnUsages() { return columnUsages; }
    public void setColumnUsages(List<com.plsqlanalyzer.parser.model.ColumnUsage> c) { this.columnUsages = c; }
    public List<com.plsqlanalyzer.parser.model.DdlInfo> getDdl() { return ddl; }
    public void setDdl(List<com.plsqlanalyzer.parser.model.DdlInfo> d) { this.ddl = d; }
    /**
     * Whether ANSI parsing of {@link #sqlText} succeeded, so an empty column list can be read
     * correctly.
     *
     * <p>Without it, three different truths collapsed into one blank cell: a MERGE whose Oracle
     * syntax JSqlParser rejects, a {@code DELETE FROM t} that genuinely names no columns, and a
     * {@code SELECT COUNT(...)} carrying only aggregates. The screen showed "—" for all three, so a
     * reader auditing which columns a table's writers touch could not tell "none" from
     * "we could not tell". Measured on the Oracle analysis: MERGE and DELETE at 0% column coverage,
     * SELECT at 21% — a lot of blank cells, of at least two different kinds.
     */
    public Boolean getColumnsParsed() { return columnsParsed; }
    public void setColumnsParsed(Boolean v) { this.columnsParsed = v; }

    public List<com.plsqlanalyzer.parser.model.WhereFilter> getWhereFilters() { return whereFilters; }
    public void setWhereFilters(List<com.plsqlanalyzer.parser.model.WhereFilter> w) { this.whereFilters = w; }

    public String getPredicateScope() { return predicateScope; }
    public void setPredicateScope(String s) { this.predicateScope = s; }

    public String getMergeClause() { return mergeClause; }
    public void setMergeClause(String mergeClause) { this.mergeClause = mergeClause; }
}
