package com.plsqlanalyzer.parser.service;

import com.plsqlanalyzer.parser.model.*;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Uses JSqlParser to analyze individual SQL statements for detailed
 * table references, operations, and WHERE clause filters.
 */
public class SqlAnalyzer {

    private static final Logger log = LoggerFactory.getLogger(SqlAnalyzer.class);

    public SqlAnalysisResult analyze(String sql, int baseLine) {
        SqlAnalysisResult result = new SqlAnalysisResult();
        result.setRawSql(sql);
        result.setLineNumber(baseLine);

        try {
            Statement stmt = CCJSqlParserUtil.parse(normalizeForParsing(sql));

            if (stmt instanceof Select select) {
                result.setOperationType(SqlOperationType.SELECT);
                extractFromSelect(select, result, baseLine);
            } else if (stmt instanceof Insert insert) {
                result.setOperationType(SqlOperationType.INSERT);
                extractFromInsert(insert, result, baseLine);
            } else if (stmt instanceof Update update) {
                result.setOperationType(SqlOperationType.UPDATE);
                extractFromUpdate(update, result, baseLine);
            } else if (stmt instanceof Delete delete) {
                result.setOperationType(SqlOperationType.DELETE);
                extractFromDelete(delete, result, baseLine);
            } else if (stmt instanceof Merge merge) {
                result.setOperationType(SqlOperationType.MERGE);
                extractFromMerge(merge, result, baseLine);
            } else {
                // DDL was not detected at all before. A procedure that quietly ALTERs or TRUNCATEs
                // a table changes its shape, and that shows up in no read/write count — so it has
                // to be reported on the table's page alongside the DML.
                extractDdl(stmt, result, baseLine);
            }
        } catch (Exception e) {
            log.debug("JSqlParser could not parse SQL at line {}: {}", baseLine, e.getMessage());
        }

        return result;
    }

    /**
     * Records CREATE / ALTER / DROP / TRUNCATE / CREATE INDEX.
     *
     * <p>The affected table is also added as a table reference, so a table's page lists the
     * procedure whatever kind of statement touched it. All five forms were confirmed to parse
     * before this was written.
     */
    private void extractDdl(Statement stmt, SqlAnalysisResult result, int baseLine) {
        String cls = stmt.getClass().getSimpleName();
        String action = null, object = null, type = null;
        try {
            if (stmt instanceof net.sf.jsqlparser.statement.create.table.CreateTable ct) {
                action = "CREATE_TABLE"; type = "TABLE";
                object = ct.getTable() != null ? ct.getTable().getName() : null;
            } else if (stmt instanceof net.sf.jsqlparser.statement.alter.Alter al) {
                action = "ALTER_TABLE"; type = "TABLE";
                object = al.getTable() != null ? al.getTable().getName() : null;
            } else if (stmt instanceof net.sf.jsqlparser.statement.drop.Drop dr) {
                action = "DROP"; type = dr.getType();
                object = dr.getName() != null ? dr.getName().getName() : null;
            } else if (stmt instanceof net.sf.jsqlparser.statement.truncate.Truncate tr) {
                action = "TRUNCATE"; type = "TABLE";
                object = tr.getTable() != null ? tr.getTable().getName() : null;
            } else if (stmt instanceof net.sf.jsqlparser.statement.create.index.CreateIndex ci) {
                action = "CREATE_INDEX"; type = "INDEX";
                object = ci.getTable() != null ? ci.getTable().getName() : null;
            }
        } catch (Exception e) {
            log.debug("DDL detail extraction failed for {} at line {}: {}", cls, baseLine, e.getMessage());
        }
        if (action == null) return;

        DdlInfo info = new DdlInfo(action, object != null ? object.toUpperCase() : null,
                type, result.getRawSql(), baseLine);
        result.getDdl().add(info);
        result.setOperationType(SqlOperationType.DDL);
        if (object != null) {
            TableReference ref = new TableReference();
            ref.setTableName(object.toUpperCase());
            ref.setOperation(SqlOperationType.DDL);
            ref.setLineNumber(baseLine);
            result.getTableReferences().add(ref);
        }
    }

    /**
     * Removes PL/SQL-only sugar that blocks parsing without changing what the statement means.
     *
     * <p>Measured over 708 real Oracle sources: {@code BULK COLLECT INTO} appears in statements
     * that are otherwise ordinary SELECTs, and JSqlParser rejects the whole statement because of
     * two keywords. Dropping them turns
     * {@code SELECT id BULK COLLECT INTO l_ids FROM t} into {@code SELECT id INTO l_ids FROM t},
     * which parses — and the INTO target is then captured exactly as for a scalar
     * {@code SELECT ... INTO}.
     *
     * <p>Kept deliberately narrow. Only constructs whose removal cannot change the columns, tables
     * or predicates belong here; anything that could alter meaning must be left to fail rather than
     * be rewritten into something that parses but says something else. {@link #rawSql} always keeps
     * the original text.
     */
    private String normalizeForParsing(String sql) {
        if (sql == null) return null;
        // BULK COLLECT INTO -> INTO. Case- and whitespace-insensitive.
        return sql.replaceAll("(?is)\\bBULK\\s+COLLECT\\s+INTO\\b", "INTO");
    }

    private void extractFromSelect(Select select, SqlAnalysisResult result, int baseLine) {
        PlainSelect plain = select.getPlainSelect();
        if (plain == null) return;

        String leftTableName = null;
        if (plain.getFromItem() instanceof Table table) {
            addTableRef(result, table, SqlOperationType.SELECT, baseLine);
            leftTableName = table.getName().toUpperCase();
        }
        if (plain.getJoins() != null) {
            for (Join join : plain.getJoins()) {
                if (join.getFromItem() instanceof Table table) {
                    addTableRef(result, table, SqlOperationType.SELECT, baseLine);
                    // Capture join info with predicate
                    extractJoinInfo(result, leftTableName, table, join, baseLine);
                }
            }
        }
        if (plain.getWhere() != null) {
            extractWhereFilters(plain.getWhere(), result, baseLine);
        }

        extractSelectedColumns(plain, result, baseLine);

        // Extract sequence references from the raw SQL
        extractSequenceRefs(result, baseLine);
    }

    /**
     * The select list, and the PL/SQL variables a {@code SELECT ... INTO} reads into.
     *
     * <p>INTO targets are positional: the nth variable receives the nth select item. JSqlParser
     * exposes them through {@code getIntoTables()} — Oracle's PL/SQL form does parse, which was
     * confirmed before this was written rather than assumed.
     */
    private void extractSelectedColumns(PlainSelect plain, SqlAnalysisResult result, int baseLine) {
        if (plain.getSelectItems() == null) return;
        List<String> into = new ArrayList<>();
        try {
            if (plain.getIntoTables() != null) {
                for (Table t : plain.getIntoTables()) into.add(t.getName());
            }
        } catch (Exception ignore) {
            // Older/newer parser without INTO support: the select list is still worth having.
        }
        int i = 0;
        for (SelectItem<?> item : plain.getSelectItems()) {
            Expression expr = item.getExpression();
            String qualifier = null, column = null;
            if (expr instanceof Column c) {
                column = c.getColumnName();
                qualifier = c.getTable() != null ? c.getTable().getName() : null;
            }
            ColumnUsage u = new ColumnUsage("SELECT", column, qualifier,
                    expr != null ? expr.toString() : null,
                    item.getAlias() != null ? item.getAlias().getName() : null,
                    i < into.size() ? into.get(i) : null,
                    null, baseLine);
            result.getColumnUsages().add(u);
            i++;
        }
    }

    /**
     * Where a written value comes from.
     *
     * <p>"UPDATE sets STATUS" is half an answer; the useful half is whether it came from a literal,
     * a bind variable, a sequence, another column, or an expression over them.
     */
    private String classifyValueSource(Expression expr) {
        if (expr == null) return null;
        String t = expr.toString();
        String up = t.toUpperCase();
        // SEQUENCE before COLUMN: `seq_r.NEXTVAL` parses as a qualified Column, so testing
        // `instanceof Column` first classified every sequence as a plain column read.
        if (up.endsWith(".NEXTVAL") || up.endsWith(".CURRVAL")
                || up.contains(".NEXTVAL") || up.contains(".CURRVAL")) return "SEQUENCE";
        if (expr instanceof Column) return "COLUMN";
        if (t.startsWith(":") || t.startsWith("?")) return "BIND";
        if (expr instanceof net.sf.jsqlparser.expression.StringValue
                || expr instanceof net.sf.jsqlparser.expression.LongValue
                || expr instanceof net.sf.jsqlparser.expression.DoubleValue
                || expr instanceof net.sf.jsqlparser.expression.NullValue) return "LITERAL";
        if (expr instanceof net.sf.jsqlparser.statement.select.Select) return "SUBQUERY";
        if (expr instanceof net.sf.jsqlparser.expression.Function) return "FUNCTION";
        if (expr instanceof BinaryExpression) return "EXPRESSION";
        return "EXPRESSION";
    }

    private void extractFromInsert(Insert insert, SqlAnalysisResult result, int baseLine) {
        if (insert.getTable() != null) {
            addTableRef(result, insert.getTable(), SqlOperationType.INSERT, baseLine);
        }
        // Column list paired positionally with VALUES. An INSERT ... SELECT has no VALUES; the
        // select side is captured by the recursive call below, so the column still gets an entry
        // with the source recorded as the subquery.
        if (insert.getColumns() != null) {
            // getValues() THROWS on INSERT ... SELECT rather than returning null, and an
            // uncaught throw here aborted the whole extraction — the column list was already in
            // hand and was being discarded.
            java.util.List<?> vals = null;
            try {
                if (insert.getValues() != null) vals = insert.getValues().getExpressions();
            } catch (Exception noValuesClause) {
                vals = null;
            }
            for (int i = 0; i < insert.getColumns().size(); i++) {
                Column c = insert.getColumns().get(i);
                Object raw = (vals != null && i < vals.size()) ? vals.get(i) : null;
                Expression v = (raw instanceof Expression e) ? e : null;
                result.getColumnUsages().add(new ColumnUsage("INSERT",
                        c.getColumnName(),
                        c.getTable() != null ? c.getTable().getName() : null,
                        v != null ? v.toString() : null, null, null,
                        v != null ? classifyValueSource(v)
                                  : (insert.getSelect() != null ? "SUBQUERY" : null),
                        baseLine));
            }
        }
        if (insert.getSelect() != null) {
            extractFromSelect(insert.getSelect(), result, baseLine);
        }
        extractSequenceRefs(result, baseLine);
    }

    private void extractFromUpdate(Update update, SqlAnalysisResult result, int baseLine) {
        if (update.getTable() != null) {
            addTableRef(result, update.getTable(), SqlOperationType.UPDATE, baseLine);
        }
        if (update.getJoins() != null) {
            String leftTable = update.getTable() != null ? update.getTable().getName().toUpperCase() : null;
            for (Join join : update.getJoins()) {
                if (join.getFromItem() instanceof Table table) {
                    addTableRef(result, table, SqlOperationType.SELECT, baseLine);
                    extractJoinInfo(result, leftTable, table, join, baseLine);
                }
            }
        }
        if (update.getWhere() != null) {
            extractWhereFilters(update.getWhere(), result, baseLine);
        }
        // SET col = expr, one entry per assigned column, with the provenance of each value.
        if (update.getUpdateSets() != null) {
            for (var set : update.getUpdateSets()) {
                List<Column> cols = set.getColumns();
                var vals = set.getValues();
                for (int i = 0; i < cols.size(); i++) {
                    Expression v = (vals != null && i < vals.size()) ? vals.get(i) : null;
                    Column c = cols.get(i);
                    result.getColumnUsages().add(new ColumnUsage("SET",
                            c.getColumnName(),
                            c.getTable() != null ? c.getTable().getName() : null,
                            v != null ? v.toString() : null, null, null,
                            classifyValueSource(v), baseLine));
                }
            }
        }
        extractSequenceRefs(result, baseLine);
    }

    private void extractFromDelete(Delete delete, SqlAnalysisResult result, int baseLine) {
        if (delete.getTable() != null) {
            addTableRef(result, delete.getTable(), SqlOperationType.DELETE, baseLine);
        }
        if (delete.getJoins() != null) {
            String leftTable = delete.getTable() != null ? delete.getTable().getName().toUpperCase() : null;
            for (Join join : delete.getJoins()) {
                if (join.getFromItem() instanceof Table table) {
                    addTableRef(result, table, SqlOperationType.SELECT, baseLine);
                    extractJoinInfo(result, leftTable, table, join, baseLine);
                }
            }
        }
        if (delete.getWhere() != null) {
            extractWhereFilters(delete.getWhere(), result, baseLine);
        }
    }

    private void extractFromMerge(Merge merge, SqlAnalysisResult result, int baseLine) {
        if (merge.getTable() != null) {
            addTableRef(result, merge.getTable(), SqlOperationType.MERGE, baseLine);
        }
        // MERGE ... USING source ON predicate — capture as a join
        // JSqlParser 5.0: getFromItem() for the USING source
        try {
            if (merge.getFromItem() != null && merge.getFromItem() instanceof Table srcTable) {
                addTableRef(result, srcTable, SqlOperationType.SELECT, baseLine);
                String leftTable = merge.getTable() != null ? merge.getTable().getName().toUpperCase() : null;
                String rightTable = srcTable.getName().toUpperCase();
                String onPred = merge.getOnCondition() != null ? merge.getOnCondition().toString() : null;
                JoinInfo ji = new JoinInfo(leftTable, rightTable, "MERGE", onPred, baseLine);
                result.getJoinInfos().add(ji);
            }
        } catch (Exception e) {
            log.debug("Could not extract merge USING at line {}: {}", baseLine, e.getMessage());
        }
        extractSequenceRefs(result, baseLine);
    }

    private void addTableRef(SqlAnalysisResult result, Table table, SqlOperationType op, int line) {
        TableReference ref = new TableReference();
        ref.setTableName(table.getName().toUpperCase());
        if (table.getSchemaName() != null) {
            ref.setSchemaName(table.getSchemaName().toUpperCase());
        }
        if (table.getAlias() != null) {
            ref.setAlias(table.getAlias().getName());
        }
        ref.setOperation(op);
        ref.setLineNumber(line);
        result.getTableReferences().add(ref);
    }

    private void extractWhereFilters(Expression where, SqlAnalysisResult result, int baseLine) {
        if (where == null) return;

        List<WhereFilter> filters = new ArrayList<>();
        extractFiltersRecursive(where, filters, baseLine);
        result.getWhereFilters().addAll(filters);
    }

    private void extractFiltersRecursive(Expression expr, List<WhereFilter> filters, int baseLine) {
        if (expr instanceof BinaryExpression binary) {
            Expression left = binary.getLeftExpression();
            Expression right = binary.getRightExpression();

            if (left instanceof Column col) {
                WhereFilter filter = new WhereFilter();
                filter.setColumnName(col.getColumnName().toUpperCase());
                filter.setOperator(binary.getStringExpression());
                filter.setValue(right != null ? right.toString() : null);
                filter.setLineNumber(baseLine);
                filters.add(filter);
            } else {
                extractFiltersRecursive(left, filters, baseLine);
                extractFiltersRecursive(right, filters, baseLine);
            }
        } else if (expr instanceof InExpression in) {
            if (in.getLeftExpression() instanceof Column col) {
                WhereFilter filter = new WhereFilter();
                filter.setColumnName(col.getColumnName().toUpperCase());
                filter.setOperator("IN");
                filter.setValue(in.getRightExpression() != null ? in.getRightExpression().toString() : "");
                filter.setLineNumber(baseLine);
                filters.add(filter);
            }
        } else if (expr instanceof IsNullExpression isNull) {
            if (isNull.getLeftExpression() instanceof Column col) {
                WhereFilter filter = new WhereFilter();
                filter.setColumnName(col.getColumnName().toUpperCase());
                filter.setOperator(isNull.isNot() ? "IS NOT NULL" : "IS NULL");
                filter.setLineNumber(baseLine);
                filters.add(filter);
            }
        } else if (expr instanceof Between between) {
            if (between.getLeftExpression() instanceof Column col) {
                WhereFilter filter = new WhereFilter();
                filter.setColumnName(col.getColumnName().toUpperCase());
                filter.setOperator("BETWEEN");
                filter.setValue(between.getBetweenExpressionStart() + " AND " + between.getBetweenExpressionEnd());
                filter.setLineNumber(baseLine);
                filters.add(filter);
            }
        } else if (expr instanceof LikeExpression like) {
            if (like.getLeftExpression() instanceof Column col) {
                WhereFilter filter = new WhereFilter();
                filter.setColumnName(col.getColumnName().toUpperCase());
                filter.setOperator("LIKE");
                filter.setValue(like.getRightExpression() != null ? like.getRightExpression().toString() : "");
                filter.setLineNumber(baseLine);
                filters.add(filter);
            }
        }
    }

    /** Extract JOIN info: type and ON predicate */
    private void extractJoinInfo(SqlAnalysisResult result, String leftTableName,
                                  Table rightTable, Join join, int baseLine) {
        String rightTableName = rightTable.getName().toUpperCase();
        String joinType = "INNER"; // default
        if (join.isLeft()) joinType = "LEFT";
        else if (join.isRight()) joinType = "RIGHT";
        else if (join.isFull()) joinType = "FULL";
        else if (join.isCross()) joinType = "CROSS";
        else if (join.isNatural()) joinType = "NATURAL";
        else if (join.isOuter()) joinType = "OUTER";

        String onPredicate = null;
        if (join.getOnExpressions() != null && !join.getOnExpressions().isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (Expression expr : join.getOnExpressions()) {
                if (sb.length() > 0) sb.append(" AND ");
                sb.append(expr.toString());
            }
            onPredicate = sb.toString();
        }

        JoinInfo ji = new JoinInfo(leftTableName, rightTableName, joinType, onPredicate, baseLine);
        result.getJoinInfos().add(ji);
    }

    /** Regex pattern for SEQUENCE_NAME.NEXTVAL or SCHEMA.SEQUENCE_NAME.NEXTVAL / CURRVAL */
    private static final Pattern SEQ_PATTERN = Pattern.compile(
            "(?i)(?:([A-Z_][A-Z0-9_$#]*)\\.)?([A-Z_][A-Z0-9_$#]*)\\.(NEXTVAL|CURRVAL)");

    /** Extract sequence references (NEXTVAL/CURRVAL) from raw SQL */
    private void extractSequenceRefs(SqlAnalysisResult result, int baseLine) {
        String sql = result.getRawSql();
        if (sql == null || sql.isEmpty()) return;

        Matcher m = SEQ_PATTERN.matcher(sql);
        while (m.find()) {
            String schema = m.group(1) != null ? m.group(1).toUpperCase() : null;
            String seqName = m.group(2).toUpperCase();
            String op = m.group(3).toUpperCase();

            // Skip table-column patterns that look like sequences but aren't
            // NEXTVAL/CURRVAL are reserved — this pattern is always a sequence
            SequenceReference ref = new SequenceReference(seqName, schema, op, baseLine);
            result.getSequenceReferences().add(ref);
        }
    }
}
