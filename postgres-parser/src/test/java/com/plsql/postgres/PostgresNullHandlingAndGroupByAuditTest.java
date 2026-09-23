package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * READ-ONLY AUDIT (2026-09-17) of postgres-parser's handling of:
 *   1. NULL-handling functions (COALESCE / NVL-as-extension) interacting with dynamic SQL
 *      (EXECUTE / format()) and with CASE WHEN in real (non-dynamic) queries.
 *   2. GROUP BY extensions: ROLLUP(...), CUBE(...), GROUPING SETS ((...),(...)).
 *
 * Grep evidence used to scope this audit (see report):
 *  - COALESCE and NVL are both registered as known builtin function names in
 *    PostgresAnalysisVisitor.KNOWN_FUNCTIONS (NVL is a plsql-parser-style extension the
 *    Postgres grammar/visitor also recognizes even though native Postgres has no NVL).
 *  - Neither ROLLUP, CUBE, nor GROUPING appear anywhere in PostgresLexer.g4, PostgresParser.g4,
 *    or PostgresAnalysisVisitor.java — there is no dedicated GROUP BY rule at all. GROUP BY /
 *    WHERE / HAVING / ORDER BY are all swallowed by the fully generic, permissive
 *    `selectRest : selectRestToken*` catch-all (see PostgresParser.g4 ~line 439-460), which
 *    balances parens/CASE/subqueries generically and stops only at a fixed set of statement
 *    boundary tokens. Because `fromClause` is parsed structurally *before* `selectRest` even
 *    begins, nothing that happens inside GROUP BY/HAVING/ORDER BY (however exotic) can affect
 *    which table was already captured from FROM — there is no shared mutable scan position or
 *    lookahead between the two that a ROLLUP/CUBE/GROUPING SETS construct could desync.
 *
 * Prints real captured output for each pattern; asserts only where there is a clear
 * expectation from the above grep evidence. Does not modify any file under src/main.
 */
class PostgresNullHandlingAndGroupByAuditTest {

    private final PostgresAnalysisEngine engine = new PostgresAnalysisEngine();

    private void dump(String label, PostgresRoutineInfo r) {
        System.out.println("========== " + label + " ==========");
        for (PostgresTableOpInfo t : r.getTableOps()) {
            System.out.println("  tableOp: " + t.getDmlType() + " table=" + t.qualifiedTable()
                    + " alias=" + t.getAlias() + " line=" + t.getLine());
        }
        if (r.getDynamicSql() != null) {
            for (PostgresDynamicSqlInfo d : r.getDynamicSql()) {
                System.out.println("  dynSql: kind=" + d.getKind() + " line=" + d.getLine()
                        + " source=" + d.getSqlSource());
            }
        }
        System.out.println("  errors=" + r.getErrors());
    }

    // ── 1a. COALESCE inside a fully-literal EXECUTE string (no concat/format indirection) ──
    @Test
    @DisplayName("1a: COALESCE inside a fully-literal EXECUTE '<sql>' string still extracts the table")
    void coalesceInsideLiteralDynamicSql() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_coalesce_literal_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  EXECUTE 'SELECT COALESCE(status, ''X'') FROM orders WHERE id = 1';\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_coalesce_literal_probe", null);
        dump("coalesceInsideLiteralDynamicSql", r);
        boolean ordersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == PostgresTableOpInfo.DmlType.DYNAMIC);
        System.out.println("  >>> orders captured as DYNAMIC table op despite embedded COALESCE(): " + ordersCaptured);
        org.junit.jupiter.api.Assertions.assertTrue(ordersCaptured,
                "the simple regex-based literal extractor matches on 'FROM orders' regardless of "
                        + "the COALESCE() call preceding it in the SELECT list");
    }

    // ── 1b. COALESCE inside a CONCATENATED (non-literal) EXECUTE string — known pre-existing
    // gap (see PostgresDmlCaptureGapAuditTest #6): concatenated dynamic SQL is never table-parsed
    // at all, with or without COALESCE. This isolates that COALESCE is not the cause. ─────────
    @Test
    @DisplayName("1b: COALESCE inside a concatenated EXECUTE string — pre-existing concat gap, not a new COALESCE-caused bug")
    void coalesceInsideConcatenatedDynamicSql() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_coalesce_concat_probe(p_id INT)\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  v_sql TEXT;\n" +
                "BEGIN\n" +
                "  v_sql := 'SELECT COALESCE(status, ''X'') FROM orders WHERE id = ' || p_id;\n" +
                "  EXECUTE v_sql;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_coalesce_concat_probe", null);
        dump("coalesceInsideConcatenatedDynamicSql", r);
        boolean ordersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> orders captured from concat-built dynamic SQL (assigned to variable, EXECUTE'd by name): "
                + ordersCaptured + " (expect false — the assignment 'v_sql := ...' is never traced by EXECUTE v_sql,"
                + " a distinct pre-existing gap from the direct-concat case in PostgresDmlCaptureGapAuditTest#6)");
        org.junit.jupiter.api.Assertions.assertFalse(ordersCaptured,
                "documenting current (gap) behavior: variable-indirected EXECUTE is not traced back to its assignment");
    }

    // ── 1c. COALESCE nested inside CASE WHEN in a real (non-dynamic) query ────────────────
    @Test
    @DisplayName("1c: COALESCE nested inside CASE WHEN in a real query — table capture must survive")
    void coalesceNestedInCaseWhenRealQuery() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_case_coalesce_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  v_cnt INT;\n" +
                "BEGIN\n" +
                "  SELECT count(*) INTO v_cnt\n" +
                "    FROM orders\n" +
                "   WHERE CASE WHEN COALESCE(a, b) = 1 THEN 1 ELSE 0 END = 1;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_case_coalesce_probe", null);
        dump("coalesceNestedInCaseWhenRealQuery", r);
        boolean ordersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> orders captured despite COALESCE nested inside CASE WHEN in WHERE clause: " + ordersCaptured
                + " (expect true — FROM is parsed structurally before the fully-permissive WHERE/selectRest catch-all"
                + " even sees the CASE token, so there is no shared scan state for COALESCE-in-CASE to desync)");
        org.junit.jupiter.api.Assertions.assertTrue(ordersCaptured,
                "orders must still be captured; COALESCE nested in CASE WHEN lives entirely inside the generic,"
                        + " already-parsed-past selectRest catch-all and cannot affect FROM-clause table capture");
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
    }

    // ── 1d. COALESCE nested in CASE inside the SELECT LIST itself (not WHERE) ─────────────
    @Test
    @DisplayName("1d: COALESCE nested inside CASE in the SELECT list — table capture must survive")
    void coalesceNestedInCaseInSelectList() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_case_coalesce_selectlist_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  v_cnt INT;\n" +
                "BEGIN\n" +
                "  SELECT CASE WHEN COALESCE(a, b) = 1 THEN 'Y' ELSE 'N' END\n" +
                "    INTO v_cnt\n" +
                "    FROM orders o\n" +
                "    JOIN customers c ON o.customer_id = c.id;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_case_coalesce_selectlist_probe", null);
        dump("coalesceNestedInCaseInSelectList", r);
        boolean ordersCaptured = r.getTableOps().stream().anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        boolean customersCaptured = r.getTableOps().stream().anyMatch(t -> "customers".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> orders captured: " + ordersCaptured + "  customers captured: " + customersCaptured);
        org.junit.jupiter.api.Assertions.assertTrue(ordersCaptured);
        org.junit.jupiter.api.Assertions.assertTrue(customersCaptured);
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
    }

    // ── 2a. GROUP BY ROLLUP(a,b) ──────────────────────────────────────────────────────────
    @Test
    @DisplayName("2a: GROUP BY ROLLUP(a,b) does not desync FROM-clause table capture")
    void groupByRollup() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_rollup_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE v_cnt INT;\n" +
                "BEGIN\n" +
                "  SELECT customer_id, product_id, count(*) INTO v_cnt\n" +
                "    FROM orders\n" +
                "   GROUP BY ROLLUP(customer_id, product_id);\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_rollup_probe", null);
        dump("groupByRollup", r);
        boolean ordersCaptured = r.getTableOps().stream().anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> orders captured with GROUP BY ROLLUP(...): " + ordersCaptured + "  errors=" + r.getErrors());
        org.junit.jupiter.api.Assertions.assertTrue(ordersCaptured);
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
    }

    // ── 2b. GROUP BY CUBE(a,b) ─────────────────────────────────────────────────────────────
    @Test
    @DisplayName("2b: GROUP BY CUBE(a,b) does not desync FROM-clause table capture")
    void groupByCube() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_cube_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE v_cnt INT;\n" +
                "BEGIN\n" +
                "  SELECT customer_id, product_id, count(*) INTO v_cnt\n" +
                "    FROM orders o\n" +
                "    JOIN customers c ON o.customer_id = c.id\n" +
                "   GROUP BY CUBE(o.customer_id, o.product_id);\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_cube_probe", null);
        dump("groupByCube", r);
        boolean ordersCaptured = r.getTableOps().stream().anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        boolean customersCaptured = r.getTableOps().stream().anyMatch(t -> "customers".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> orders captured: " + ordersCaptured + "  customers captured: " + customersCaptured
                + "  errors=" + r.getErrors());
        org.junit.jupiter.api.Assertions.assertTrue(ordersCaptured);
        org.junit.jupiter.api.Assertions.assertTrue(customersCaptured);
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
    }

    // ── 2c. GROUP BY GROUPING SETS ((a),(b),(a,b)) ────────────────────────────────────────
    @Test
    @DisplayName("2c: GROUP BY GROUPING SETS ((a),(b),(a,b)) does not desync FROM-clause table capture")
    void groupByGroupingSets() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_grouping_sets_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE v_cnt INT;\n" +
                "BEGIN\n" +
                "  SELECT customer_id, product_id, count(*) INTO v_cnt\n" +
                "    FROM orders\n" +
                "   GROUP BY GROUPING SETS ((customer_id), (product_id), (customer_id, product_id));\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_grouping_sets_probe", null);
        dump("groupByGroupingSets", r);
        boolean ordersCaptured = r.getTableOps().stream().anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> orders captured with GROUP BY GROUPING SETS(...): " + ordersCaptured + "  errors=" + r.getErrors());
        org.junit.jupiter.api.Assertions.assertTrue(ordersCaptured);
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
    }

    // ── 2d. ROLLUP combined with a subquery in HAVING, to stress the permissive catch-all ──
    @Test
    @DisplayName("2d: GROUP BY ROLLUP(...) combined with HAVING containing a subquery")
    void groupByRollupWithHavingSubquery() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_rollup_having_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE v_cnt INT;\n" +
                "BEGIN\n" +
                "  SELECT customer_id, count(*) INTO v_cnt\n" +
                "    FROM orders\n" +
                "   GROUP BY ROLLUP(customer_id)\n" +
                "  HAVING count(*) > (SELECT avg(x) FROM order_thresholds);\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_rollup_having_probe", null);
        dump("groupByRollupWithHavingSubquery", r);
        boolean ordersCaptured = r.getTableOps().stream().anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        boolean thresholdsCaptured = r.getTableOps().stream().anyMatch(t -> "order_thresholds".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> orders captured: " + ordersCaptured + "  order_thresholds (HAVING subquery) captured: "
                + thresholdsCaptured + "  errors=" + r.getErrors());
        org.junit.jupiter.api.Assertions.assertTrue(ordersCaptured);
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
    }
}
