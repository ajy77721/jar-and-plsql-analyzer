/*
 * Decompiled with CFR 0.152.
 * 
 * Could not load the following classes:
 *  net.sf.jsqlparser.expression.Expression
 *  net.sf.jsqlparser.expression.JdbcNamedParameter
 *  net.sf.jsqlparser.expression.JdbcParameter
 *  net.sf.jsqlparser.expression.operators.conditional.AndExpression
 *  net.sf.jsqlparser.expression.operators.conditional.OrExpression
 *  net.sf.jsqlparser.expression.operators.relational.Between
 *  net.sf.jsqlparser.expression.operators.relational.EqualsTo
 *  net.sf.jsqlparser.expression.operators.relational.GreaterThan
 *  net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals
 *  net.sf.jsqlparser.expression.operators.relational.InExpression
 *  net.sf.jsqlparser.expression.operators.relational.IsNullExpression
 *  net.sf.jsqlparser.expression.operators.relational.LikeExpression
 *  net.sf.jsqlparser.expression.operators.relational.MinorThan
 *  net.sf.jsqlparser.expression.operators.relational.MinorThanEquals
 *  net.sf.jsqlparser.expression.operators.relational.NotEqualsTo
 *  net.sf.jsqlparser.parser.CCJSqlParserUtil
 *  net.sf.jsqlparser.schema.Column
 *  net.sf.jsqlparser.statement.Statement
 *  net.sf.jsqlparser.statement.delete.Delete
 *  net.sf.jsqlparser.statement.insert.Insert
 *  net.sf.jsqlparser.statement.select.FromItem
 *  net.sf.jsqlparser.statement.select.PlainSelect
 *  net.sf.jsqlparser.statement.select.Select
 *  net.sf.jsqlparser.statement.update.Update
 */
package com.flowpoc.engine;

import com.flowpoc.model.ExtractedQuery;
import com.flowpoc.model.Predicate;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;

public class SqlPredicateExtractor {
    public ExtractedQuery parse(String rawSql, String sourceClass, String sourceMethod) {
        ExtractedQuery.QueryType type = this.detectType(rawSql);
        String table = "unknown";
        List<Predicate> preds = new ArrayList<Predicate>();
        try {
            Statement stmt = CCJSqlParserUtil.parse((String)rawSql);
            table = this.extractTable(stmt);
            preds = this.extractPredicates(stmt);
        }
        catch (Exception ignored) {
            table = this.guessTable(rawSql);
        }
        ExtractedQuery eq = new ExtractedQuery(rawSql, type, table, sourceClass, sourceMethod);
        preds.forEach(eq::addPredicate);
        return eq;
    }

    private String extractTable(Statement stmt) {
        Select s;
        Select select;
        if (stmt instanceof PlainSelect) {
            PlainSelect ps = (PlainSelect)stmt;
            FromItem from = ps.getFromItem();
            return from != null ? from.toString().split("\\s")[0] : "unknown";
        }
        if (stmt instanceof Select && (select = (s = (Select)stmt).getSelectBody()) instanceof PlainSelect) {
            PlainSelect ps = (PlainSelect)select;
            FromItem from = ps.getFromItem();
            return from != null ? from.toString().split("\\s")[0] : "unknown";
        }
        if (stmt instanceof Update) {
            Update u = (Update)stmt;
            return u.getTable().getName();
        }
        if (stmt instanceof Delete) {
            Delete d = (Delete)stmt;
            return d.getTable().getName();
        }
        if (stmt instanceof Insert) {
            Insert i = (Insert)stmt;
            return i.getTable().getName();
        }
        return "unknown";
    }

    private List<Predicate> extractPredicates(Statement stmt) {
        Select s;
        Select select;
        Expression where = null;
        if (stmt instanceof PlainSelect) {
            PlainSelect ps = (PlainSelect)stmt;
            where = ps.getWhere();
        }
        if (stmt instanceof Select && (select = (s = (Select)stmt).getSelectBody()) instanceof PlainSelect) {
            PlainSelect ps = (PlainSelect)select;
            where = ps.getWhere();
        }
        if (stmt instanceof Update) {
            Update u = (Update)stmt;
            where = u.getWhere();
        }
        if (stmt instanceof Delete) {
            Delete d = (Delete)stmt;
            where = d.getWhere();
        }
        ArrayList<Predicate> out = new ArrayList<Predicate>();
        if (where != null) {
            this.visitExpression(where, out);
        }
        return out;
    }

    private void visitExpression(Expression expr, List<Predicate> out) {
        Between b;
        Expression expression;
        if (expr instanceof AndExpression) {
            AndExpression a = (AndExpression)expr;
            this.visitExpression(a.getLeftExpression(), out);
            this.visitExpression(a.getRightExpression(), out);
        } else if (expr instanceof OrExpression) {
            OrExpression o = (OrExpression)expr;
            this.visitExpression(o.getLeftExpression(), out);
            this.visitExpression(o.getRightExpression(), out);
        } else if (expr instanceof EqualsTo) {
            EqualsTo eq = (EqualsTo)expr;
            this.addPred(eq.getLeftExpression(), Predicate.Op.EQ, eq.getRightExpression(), out);
        } else if (expr instanceof NotEqualsTo) {
            NotEqualsTo ne = (NotEqualsTo)expr;
            this.addPred(ne.getLeftExpression(), Predicate.Op.NEQ, ne.getRightExpression(), out);
        } else if (expr instanceof GreaterThan) {
            GreaterThan gt = (GreaterThan)expr;
            this.addPred(gt.getLeftExpression(), Predicate.Op.GT, gt.getRightExpression(), out);
        } else if (expr instanceof GreaterThanEquals) {
            GreaterThanEquals gte = (GreaterThanEquals)expr;
            this.addPred(gte.getLeftExpression(), Predicate.Op.GTE, gte.getRightExpression(), out);
        } else if (expr instanceof MinorThan) {
            MinorThan lt = (MinorThan)expr;
            this.addPred(lt.getLeftExpression(), Predicate.Op.LT, lt.getRightExpression(), out);
        } else if (expr instanceof MinorThanEquals) {
            MinorThanEquals lte = (MinorThanEquals)expr;
            this.addPred(lte.getLeftExpression(), Predicate.Op.LTE, lte.getRightExpression(), out);
        } else if (expr instanceof LikeExpression) {
            LikeExpression like = (LikeExpression)expr;
            this.addPred(like.getLeftExpression(), Predicate.Op.LIKE, like.getRightExpression(), out);
        } else if (expr instanceof IsNullExpression) {
            IsNullExpression isNull = (IsNullExpression)expr;
            Expression expression2 = isNull.getLeftExpression();
            if (expression2 instanceof Column) {
                Column col = (Column)expression2;
                out.add(new Predicate(col.getColumnName(), isNull.isNot() ? Predicate.Op.IS_NOT_NULL : Predicate.Op.IS_NULL, null));
            }
        } else if (expr instanceof InExpression) {
            InExpression in = (InExpression)expr;
            Expression expression3 = in.getLeftExpression();
            if (expression3 instanceof Column) {
                Column col = (Column)expression3;
                out.add(new Predicate(col.getColumnName(), Predicate.Op.IN, in.getRightExpression() != null ? in.getRightExpression().toString() : "?"));
            }
        } else if (expr instanceof Between && (expression = (b = (Between)expr).getLeftExpression()) instanceof Column) {
            Column col = (Column)expression;
            out.add(new Predicate(col.getColumnName(), Predicate.Op.BETWEEN, String.valueOf(b.getBetweenExpressionStart()) + " AND " + String.valueOf(b.getBetweenExpressionEnd())));
        }
    }

    private void addPred(Expression left, Predicate.Op op, Expression right, List<Predicate> out) {
        if (left instanceof Column) {
            Object object;
            Column col = (Column)left;
            String string = col.getColumnName();
            if (right instanceof JdbcParameter) {
                object = "?";
            } else if (right instanceof JdbcNamedParameter) {
                JdbcNamedParameter jp = (JdbcNamedParameter)right;
                object = ":" + jp.getName();
            } else {
                object = right.toString();
            }
            out.add(new Predicate(string, op, (String)object));
        }
    }

    private ExtractedQuery.QueryType detectType(String sql) {
        String u = sql.trim().toUpperCase();
        if (u.startsWith("SELECT") || u.startsWith("WITH")) {
            return ExtractedQuery.QueryType.SELECT;
        }
        if (u.startsWith("INSERT")) {
            return ExtractedQuery.QueryType.INSERT;
        }
        if (u.startsWith("UPDATE")) {
            return ExtractedQuery.QueryType.UPDATE;
        }
        if (u.startsWith("DELETE")) {
            return ExtractedQuery.QueryType.DELETE;
        }
        if (u.startsWith("CALL") || u.startsWith("{CALL")) {
            return ExtractedQuery.QueryType.CALL;
        }
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

