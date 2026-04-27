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
            Statement stmt = CCJSqlParserUtil.parse(sql);

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
            }
        } catch (Exception e) {
            log.debug("JSqlParser could not parse SQL at line {}: {}", baseLine, e.getMessage());
        }

        return result;
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

        // Extract sequence references from the raw SQL
        extractSequenceRefs(result, baseLine);
    }

    private void extractFromInsert(Insert insert, SqlAnalysisResult result, int baseLine) {
        if (insert.getTable() != null) {
            addTableRef(result, insert.getTable(), SqlOperationType.INSERT, baseLine);
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
