package com.flowpoc.engine;

import com.flowpoc.model.ExtractedQuery;
import com.flowpoc.model.Predicate;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.Update;

import java.util.ArrayList;
import java.util.List;

/**
 * Parses raw SQL strings into {@link ExtractedQuery} with typed {@link Predicate} list.
 * Uses JSqlParser. Failures are non-fatal — returns best-effort result.
 */
public class SqlPredicateExtractor {

    public ExtractedQuery parse(String rawSql, String sourceClass, String sourceMethod) {
        ExtractedQuery.QueryType type  = detectType(rawSql);
        String                   table = "unknown";
        List<Predicate>          preds = new ArrayList<>();

        try {
            Statement stmt = CCJSqlParserUtil.parse(rawSql);
            table = extractTable(stmt);
            preds = extractPredicates(stmt);
        } catch (Exception ignored) {
            // JSqlParser cannot parse every SQL variant; use best-effort table guess
            table = guessTable(rawSql);
        }

        ExtractedQuery eq = new ExtractedQuery(rawSql, type, table, sourceClass, sourceMethod);
        preds.forEach(eq::addPredicate);
        return eq;
    }

    private String extractTable(Statement stmt) {
        if (stmt instanceof PlainSelect ps) {
            FromItem from = ps.getFromItem();
            return from != null ? from.toString().split("\\s")[0] : "unknown";
        }
        if (stmt instanceof Select s && s.getSelectBody() instanceof PlainSelect ps) {
            FromItem from = ps.getFromItem();
            return from != null ? from.toString().split("\\s")[0] : "unknown";
        }
        if (stmt instanceof Update u)  return u.getTable().getName();
        if (stmt instanceof Delete d)  return d.getTable().getName();
        if (stmt instanceof Insert i)  return i.getTable().getName();
        return "unknown";
    }

    private List<Predicate> extractPredicates(Statement stmt) {
        Expression where = null;
        if (stmt instanceof PlainSelect ps)  where = ps.getWhere();
        if (stmt instanceof Select s && s.getSelectBody() instanceof PlainSelect ps)
            where = ps.getWhere();
        if (stmt instanceof Update u)  where = u.getWhere();
        if (stmt instanceof Delete d)  where = d.getWhere();

        List<Predicate> out = new ArrayList<>();
        if (where != null) visitExpression(where, out);
        return out;
    }

    private void visitExpression(Expression expr, List<Predicate> out) {
        if (expr instanceof AndExpression a) {
            visitExpression(a.getLeftExpression(), out);
            visitExpression(a.getRightExpression(), out);
        } else if (expr instanceof OrExpression o) {
            visitExpression(o.getLeftExpression(), out);
            visitExpression(o.getRightExpression(), out);
        } else if (expr instanceof EqualsTo eq) {
            addPred(eq.getLeftExpression(), Predicate.Op.EQ, eq.getRightExpression(), out);
        } else if (expr instanceof NotEqualsTo ne) {
            addPred(ne.getLeftExpression(), Predicate.Op.NEQ, ne.getRightExpression(), out);
        } else if (expr instanceof GreaterThan gt) {
            addPred(gt.getLeftExpression(), Predicate.Op.GT, gt.getRightExpression(), out);
        } else if (expr instanceof GreaterThanEquals gte) {
            addPred(gte.getLeftExpression(), Predicate.Op.GTE, gte.getRightExpression(), out);
        } else if (expr instanceof MinorThan lt) {
            addPred(lt.getLeftExpression(), Predicate.Op.LT, lt.getRightExpression(), out);
        } else if (expr instanceof MinorThanEquals lte) {
            addPred(lte.getLeftExpression(), Predicate.Op.LTE, lte.getRightExpression(), out);
        } else if (expr instanceof LikeExpression like) {
            addPred(like.getLeftExpression(), Predicate.Op.LIKE, like.getRightExpression(), out);
        } else if (expr instanceof IsNullExpression isNull) {
            if (isNull.getLeftExpression() instanceof Column col) {
                out.add(new Predicate(col.getColumnName(),
                        isNull.isNot() ? Predicate.Op.IS_NOT_NULL : Predicate.Op.IS_NULL, null));
            }
        } else if (expr instanceof InExpression in) {
            if (in.getLeftExpression() instanceof Column col) {
                out.add(new Predicate(col.getColumnName(), Predicate.Op.IN,
                        in.getRightExpression() != null ? in.getRightExpression().toString() : "?"));
            }
        } else if (expr instanceof Between b) {
            if (b.getLeftExpression() instanceof Column col) {
                out.add(new Predicate(col.getColumnName(), Predicate.Op.BETWEEN,
                        b.getBetweenExpressionStart() + " AND " + b.getBetweenExpressionEnd()));
            }
        }
    }

    private void addPred(Expression left, Predicate.Op op, Expression right, List<Predicate> out) {
        if (left instanceof Column col) {
            out.add(new Predicate(col.getColumnName(), op,
                    right instanceof JdbcParameter ? "?" :
                    right instanceof JdbcNamedParameter jp ? ":" + jp.getName() :
                    right.toString()));
        }
    }

    private ExtractedQuery.QueryType detectType(String sql) {
        String u = sql.trim().toUpperCase();
        if (u.startsWith("SELECT") || u.startsWith("WITH")) return ExtractedQuery.QueryType.SELECT;
        if (u.startsWith("INSERT")) return ExtractedQuery.QueryType.INSERT;
        if (u.startsWith("UPDATE")) return ExtractedQuery.QueryType.UPDATE;
        if (u.startsWith("DELETE")) return ExtractedQuery.QueryType.DELETE;
        if (u.startsWith("CALL") || u.startsWith("{CALL")) return ExtractedQuery.QueryType.CALL;
        return ExtractedQuery.QueryType.UNKNOWN;
    }

    private String guessTable(String sql) {
        String u = sql.toUpperCase();
        int fromIdx = u.indexOf(" FROM ");
        if (fromIdx >= 0) {
            String rest = sql.substring(fromIdx + 6).trim();
            return rest.split("[ ,\\n\\r(]")[0];
        }
        int intoIdx = u.indexOf(" INTO ");
        if (intoIdx >= 0) {
            String rest = sql.substring(intoIdx + 6).trim();
            return rest.split("[ (\\n\\r]")[0];
        }
        return "unknown";
    }
}
