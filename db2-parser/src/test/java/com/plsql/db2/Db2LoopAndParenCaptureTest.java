package com.plsql.db2;

import com.plsql.db2.engine.Db2AnalysisEngine;
import com.plsql.db2.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Sep05-12, DB2 — the captured SQL must stop at the end of its own statement.
 *
 * <p>{@code Db2AnalysisVisitor.originalText} is a fork of the PostgreSQL visitor's and carried the
 * same two defects. They are pinned here as well as there rather than only where they were first
 * found: a forked method is exactly the place where a fix applied once quietly fails to hold, and
 * the DB2 numbers (54 statements running into a loop body, 19 with an unbalanced paren) were
 * measured on the real analysis, not assumed from the PostgreSQL result.
 */
class Db2LoopAndParenCaptureTest {

    private final Db2AnalysisEngine engine = new Db2AnalysisEngine();

    private Db2RoutineInfo parse(String body) {
        return engine.parse(
                "CREATE PROCEDURE p()\nLANGUAGE SQL\nBEGIN\n" + body + "\nEND", "p", null);
    }

    private String firstSql(Db2RoutineInfo r) {
        return r.getTableOps().stream()
                .map(Db2TableOpInfo::getSqlText)
                .filter(java.util.Objects::nonNull)
                .findFirst().orElse(null);
    }

    @Test
    @DisplayName("a FOR ... DO read captures the SELECT, not the loop body")
    void forDoCaptureStopsAtDo() {
        // DB2 spells the loop opener `DO`, so the PostgreSQL keyword list alone would not have
        // caught this — which is the reason the two lists differ deliberately.
        Db2RoutineInfo r = parse(
                "  FOR rec AS SELECT id, subtotal FROM ERP_SALES_ORDER\n"
              + "             WHERE subtotal >= 0 ORDER BY id DO\n"
              + "    SET v_sum = v_sum + rec.subtotal;\n"
              + "  END FOR;");

        String sql = firstSql(r);
        assertNotNull(sql, "the read carried no SQL at all");
        String up = sql.toUpperCase(Locale.ROOT);
        assertFalse(up.matches("(?s).*\\bDO\\b.*"),
                "the capture ran into the loop body: " + sql);
        assertTrue(up.contains("WHERE"),
                "the WHERE clause carries the predicate detail and must be kept: " + sql);
    }

    @Test
    @DisplayName("a capture beginning inside a subquery does not close a paren it never opened")
    void captureIsParenBalanced() {
        Db2RoutineInfo r = parse(
                "  SET v_rate = (SELECT COALESCE(MAX(rate),0) FROM ERP_TAX_RATE\n"
              + "                WHERE country_id = p_country_id);");

        for (Db2TableOpInfo t : r.getTableOps()) {
            String sql = t.getSqlText();
            if (sql == null) continue;
            long open = sql.chars().filter(c -> c == '(').count();
            long close = sql.chars().filter(c -> c == ')').count();
            assertTrue(close <= open,
                    "captured text closes a paren it never opened, so it cannot parse: " + sql);
        }
    }

    @Test
    @DisplayName("a CASE ... END expression is not truncated")
    void caseExpressionSurvives() {
        Db2RoutineInfo r = parse(
                "  SELECT CASE WHEN subtotal > 0 THEN 1 ELSE 0 END, id\n"
              + "    INTO v_flag, v_id FROM ERP_SALES_ORDER WHERE id = 7;");

        String sql = firstSql(r);
        assertNotNull(sql, "the read carried no SQL at all");
        assertTrue(sql.toUpperCase(Locale.ROOT).contains("WHERE"),
                "the statement was truncated at CASE's THEN/END: " + sql);
    }
}
