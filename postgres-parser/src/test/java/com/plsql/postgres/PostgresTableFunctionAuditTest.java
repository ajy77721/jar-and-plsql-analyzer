package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AUDIT (read-only, evidence-gathering) test file. Not a bug-fix.
 *
 * PostgreSQL's real equivalent of Oracle's TABLE(fn()) construct is a set-returning function
 * used directly in FROM, e.g. {@code SELECT * FROM my_func(...) AS t(...)} or
 * {@code SELECT * FROM unnest(...)}. Per source inspection, PostgresParser.g4's `tableAtom` has
 * an explicit `atomFunc` alternative, and PostgresAnalysisVisitor.tableOpForAtom (line ~499)
 * deliberately returns null for AtomFuncContext with a comment stating the call edge is captured
 * separately by visitFunctionCall — this looks like a DESIGNED behavior, not an omission; this
 * test records the actual observed output to confirm/refute that on realistic snippets.
 *
 * PostgreSQL has no equivalent of Oracle's old-style {@code (+)} outer-join syntax (confirmed:
 * grep found zero matches for "(+)"/"outer_join_sign" in postgres-parser) — intentionally not
 * tested here.
 */
@DisplayName("AUDIT: PostgreSQL table-valued/set-returning function usage in FROM/JOIN")
public class PostgresTableFunctionAuditTest {

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

    private List<String> callNames(PostgresRoutineInfo r) {
        return r.getCalls().stream()
                .map(PostgresCallInfo::getName)
                .filter(java.util.Objects::nonNull)
                .collect(Collectors.toList());
    }

    @Test
    @DisplayName("AUDIT: JOIN my_func(...) AS t(...) against a real table")
    void auditTableFunctionJoinedToRealTable() {
        PostgresRoutineInfo r = parse(
                "  SELECT o.order_id, t.col1\n" +
                "  FROM orders o\n" +
                "  JOIN get_line_items(o.order_id) AS t(col1, col2) ON t.col1 = o.some_col;");

        List<String> tables = tableNames(r);
        List<String> calls = callNames(r);
        System.out.println("[AUDIT] tableOps=" + tables + " calls=" + calls + " errors=" + r.getErrors());

        assertTrue(tables.contains("ORDERS"), "AUDIT: ORDERS real table must not be dropped");
        boolean foundCall = calls.stream().anyMatch(n -> n.equalsIgnoreCase("get_line_items"));
        assertTrue(foundCall, "AUDIT: get_line_items(...) should be recorded as a call when used as a set-returning function in JOIN");
    }

    @Test
    @DisplayName("AUDIT: bare SELECT * FROM some_pipelined_func(p1, p2)")
    void auditBareSelectFromSetReturningFunction() {
        PostgresRoutineInfo r = parse(
                "  SELECT * FROM some_pipelined_func(1, 2);");

        List<String> tables = tableNames(r);
        List<String> calls = callNames(r);
        System.out.println("[AUDIT] bare select tableOps=" + tables + " calls=" + calls);

        boolean foundCall = calls.stream().anyMatch(n -> n.equalsIgnoreCase("some_pipelined_func"));
        assertTrue(foundCall, "AUDIT: some_pipelined_func should be recorded as a call for bare SELECT * FROM fn(...)");
        // NOTE: per tableOpForAtom's designed null-return for AtomFuncContext, no synthetic
        // table-op/pseudo-table representing the function's row-set is expected to appear here.
    }

    @Test
    @DisplayName("AUDIT: SELECT * FROM unnest(arr) AS t joined with a real table")
    void auditUnnestJoinedToRealTable() {
        PostgresRoutineInfo r = parse(
                "  SELECT c.id, t.val\n" +
                "  FROM customers c\n" +
                "  JOIN unnest(c.tag_ids) AS t(val) ON true;");

        List<String> tables = tableNames(r);
        List<String> calls = callNames(r);
        System.out.println("[AUDIT] unnest tableOps=" + tables + " calls=" + calls + " errors=" + r.getErrors());

        assertTrue(tables.contains("CUSTOMERS"), "CUSTOMERS real table must not be dropped when joined against unnest(...)");
        boolean foundUnnestAsTable = tables.contains("UNNEST");
        boolean foundUnnestAsCall = calls.stream().anyMatch(n -> n.equalsIgnoreCase("unnest"));
        System.out.println("[AUDIT] unnest treated as table=" + foundUnnestAsTable + " treated as call=" + foundUnnestAsCall);
        assertFalse(foundUnnestAsTable, "unnest(...) should not be invented as a fake table named UNNEST");
        assertTrue(foundUnnestAsCall, "unnest(...) used as a set-returning function in JOIN should be recorded as a call, like get_line_items(...)/get_order_lines(...)");
    }

    @Test
    @DisplayName("AUDIT: LATERAL set-returning function joined to a real table")
    void auditLateralFunctionJoin() {
        PostgresRoutineInfo r = parse(
                "  SELECT o.id, x.line_amount\n" +
                "  FROM erp.erp_sales_order o\n" +
                "  CROSS JOIN LATERAL get_order_lines(o.id) AS x(line_amount);");

        List<String> tables = tableNames(r);
        List<String> calls = callNames(r);
        System.out.println("[AUDIT] lateral fn tableOps=" + tables + " calls=" + calls + " errors=" + r.getErrors());

        assertTrue(tables.contains("ERP_SALES_ORDER") || tables.contains("ERP.ERP_SALES_ORDER"),
                "AUDIT: erp_sales_order real table must not be dropped");
        boolean foundCall = calls.stream().anyMatch(n -> n.equalsIgnoreCase("get_order_lines"));
        assertTrue(foundCall, "AUDIT: get_order_lines(...) should be recorded as a call under LATERAL");
    }
}
