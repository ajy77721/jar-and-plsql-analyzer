package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Sep05-11 and Sep05-12 — two ways a read was reported wrongly rather than incompletely.
 *
 * <p>Both defects were found by measuring a real analysis, not by reading the grammar, and both
 * share a property that makes them worse than a gap: the screen showed something confident and
 * false. A phantom table appears in the table list exactly like a real one, and a statement whose
 * captured text runs into the loop body around it silently loses <em>all</em> of its column detail
 * rather than part of it.
 */
class PostgresLateralAndLoopCaptureTest {

    private final PostgresAnalysisEngine engine = new PostgresAnalysisEngine();

    private PostgresRoutineInfo parse(String body) {
        return engine.parse("CREATE PROCEDURE p()\nLANGUAGE plpgsql\nAS $$\nBEGIN\n"
                + body + "\nEND;\n$$", "p", null);
    }

    private List<String> tableNames(PostgresRoutineInfo r) {
        return r.getTableOps().stream()
                .map(PostgresTableOpInfo::getTableName)
                .filter(java.util.Objects::nonNull)
                .map(s -> s.toUpperCase(java.util.Locale.ROOT))
                .collect(Collectors.toList());
    }

    // ── Sep05-11: LATERAL is a modifier, not a table ──────────────────────────

    @Test
    @DisplayName("CROSS JOIN LATERAL does not invent a table called LATERAL")
    void crossJoinLateralIsNotATable() {
        PostgresRoutineInfo r = parse(
                "  SELECT COUNT(*) INTO v_cnt\n"
              + "  FROM erp.erp_sales_order so\n"
              + "  CROSS JOIN LATERAL (SELECT l.line_amount FROM erp.erp_sales_order_line l\n"
              + "                      WHERE l.order_id = so.order_id) x;");

        List<String> tables = tableNames(r);
        assertFalse(tables.contains("LATERAL"),
                "LATERAL matched the `id` rule and became a table name; tables=" + tables);
        assertTrue(tables.contains("ERP_SALES_ORDER"),
                "the real table must still be found; tables=" + tables);
    }

    @Test
    @DisplayName("a comma-form LATERAL is also a modifier")
    void commaLateralIsNotATable() {
        // `FROM a, LATERAL (…) x` is the other spelling. Fixing only the JOIN form would leave
        // this one to be rediscovered later, which is how the sibling `SET` phantom recurred.
        PostgresRoutineInfo r = parse(
                "  SELECT x.n INTO v_n FROM erp.erp_product p,\n"
              + "         LATERAL (SELECT COUNT(*) AS n FROM erp.erp_bom_component c\n"
              + "                  WHERE c.product_id = p.product_id) x;");

        assertFalse(tableNames(r).contains("LATERAL"), "tables=" + tableNames(r));
    }

    @Test
    @DisplayName("a quoted \"lateral\" identifier is still usable as a name")
    void quotedLateralStillWorks() {
        // LATERAL became a reserved token, so this pins the one case that could regress: a
        // double-quoted identifier must still reach the `id` rule via DQ_ID.
        PostgresRoutineInfo r = parse("  SELECT a INTO v_a FROM erp.\"lateral\";");
        assertTrue(tableNames(r).contains("LATERAL") || tableNames(r).contains("\"LATERAL\""),
                "a quoted identifier must still be readable as a table; tables=" + tableNames(r));
    }

    // ── Sep05-12: the capture must stop before the loop body ──────────────────

    @Test
    @DisplayName("a FOR ... LOOP read captures the SELECT, not the loop body")
    void forLoopCaptureStopsAtLoop() {
        PostgresRoutineInfo r = parse(
                "  FOR rec IN SELECT p.product_id, p.sku FROM erp.erp_product p\n"
              + "             WHERE p.make_buy = 'M' LOOP\n"
              + "    v_total := v_total + 1;\n"
              + "  END LOOP;");

        String sql = r.getTableOps().stream()
                .map(PostgresTableOpInfo::getSqlText)
                .filter(java.util.Objects::nonNull)
                .findFirst().orElse(null);

        assertNotNull(sql, "the read carried no SQL at all");
        assertFalse(sql.toUpperCase(java.util.Locale.ROOT).contains("LOOP"),
                "the capture ran into the loop body, so the text is not a statement: " + sql);
        assertFalse(sql.contains(":="),
                "the loop body leaked into the captured SQL: " + sql);
        assertTrue(sql.toUpperCase(java.util.Locale.ROOT).contains("WHERE"),
                "the WHERE clause is where the predicate detail lives and must be kept: " + sql);
    }

    @Test
    @DisplayName("a CASE ... END in the select list is not mistaken for a block end")
    void caseExpressionSurvives() {
        // The cut deliberately does not fire on THEN/END. If it did, this statement — which is
        // ordinary SQL — would be truncated mid-expression, turning a working parse into a broken
        // one. That would trade 249 known failures for an unknown number of new ones.
        PostgresRoutineInfo r = parse(
                "  SELECT CASE WHEN p.make_buy = 'M' THEN 1 ELSE 0 END AS flag, p.sku\n"
              + "    INTO v_flag, v_sku FROM erp.erp_product p WHERE p.product_id = 7;");

        String sql = r.getTableOps().stream()
                .map(PostgresTableOpInfo::getSqlText)
                .filter(java.util.Objects::nonNull)
                .findFirst().orElse(null);

        assertNotNull(sql, "the read carried no SQL at all");
        assertTrue(sql.toUpperCase(java.util.Locale.ROOT).contains("WHERE"),
                "the statement was truncated at CASE's THEN/END: " + sql);
    }

    @Test
    @DisplayName("a MERGE source subquery capture stops at its own closing paren")
    void mergeUsingSubqueryCaptureIsBalanced() {
        // The remaining 81 PostgreSQL failures were all this: the capture began at the inner
        // SELECT and ran to the statement's semicolon, so the text carried a `)` it never opened
        // and parsed as nothing.
        PostgresRoutineInfo r = parse(
                "  MERGE INTO erp.erp_demand d\n"
              + "  USING (SELECT f.product_id, SUM(f.qty) AS qty FROM erp.erp_forecast f\n"
              + "         GROUP BY f.product_id) s ON (d.product_id = s.product_id)\n"
              + "  WHEN MATCHED THEN UPDATE SET d.qty = s.qty;");

        for (PostgresTableOpInfo t : r.getTableOps()) {
            String sql = t.getSqlText();
            if (sql == null) continue;
            long open = sql.chars().filter(c -> c == '(').count();
            long close = sql.chars().filter(c -> c == ')').count();
            assertTrue(close <= open,
                    "captured text closes a paren it never opened, so it cannot parse: " + sql);
        }
    }
}
