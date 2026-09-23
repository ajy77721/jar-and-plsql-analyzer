package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * READ-ONLY AUDIT (2026-09-17) — Postgres-specific audit of the REAL ANTLR grammar/visitor
 * (PostgresParser.g4 / PostgresLexer.g4 / PostgresAnalysisVisitor.java) for:
 *
 *  1. Oracle-only hierarchical query syntax (CONNECT BY / START WITH / LEVEL /
 *     SYS_CONNECT_BY_PATH / ORDER SIBLINGS BY) — grep of PostgresLexer.g4 and
 *     PostgresParser.g4 confirms NONE of these tokens/rules exist anywhere in the Postgres
 *     grammar (CONNECT_BY, START, SYS_CONNECT_BY_PATH, SIBLINGS, LEVEL are all absent). This is
 *     CORRECT: PostgreSQL itself has no CONNECT BY / START WITH / LEVEL / SYS_CONNECT_BY_PATH /
 *     ORDER SIBLINGS BY syntax (that is Oracle-only; Postgres uses WITH RECURSIVE instead), so
 *     there is nothing to fabricate a test for here. This class intentionally contains no
 *     CONNECT BY test.
 *
 *  2. WITH RECURSIVE CTEs — anchor-branch base table capture, recursive-branch base table
 *     capture, whether the CTE's own name gets wrongly recorded as an external table reference
 *     in the recursive branch, and multi-level chained recursive CTEs.
 *
 *     Grammar fact established by reading PostgresParser.g4:
 *         withClause : WITH cteDef (COMMA cteDef)* ;
 *         cteDef     : id (LPAREN idList RPAREN)? AS LPAREN selectStatement RPAREN ;
 *     There is no RECURSIVE token in PostgresLexer.g4 at all, and withClause has no RECURSIVE
 *     alternative. So for `WITH RECURSIVE cte_name AS (...)`, the parser's `cteDef` tries to
 *     match `id` against the token "RECURSIVE" itself (which lexes as a plain identifier, since
 *     no RECURSIVE keyword token exists), then expects AS next but finds "cte_name" instead —
 *     a structural mismatch that is expected to defeat the structured withClause/cteDef path.
 *
 *  3. Sequences/identity: nextval()/currval(), SERIAL columns, GENERATED ... AS IDENTITY.
 *     Grep of PostgresLexer.g4/PostgresParser.g4 confirms there is no SEQUENCE, NEXTVAL,
 *     CURRVAL, SERIAL, or IDENTITY keyword anywhere in the grammar — nextval('seq')/currval('seq')
 *     are lexed/parsed as ordinary functionCall syntax. However, reading
 *     PostgresAnalysisVisitor.visitFunctionCall (and its SEQUENCE_FUNCTIONS set /
 *     recordSequenceUsage helper) shows NEXTVAL/CURRVAL/SETVAL/LASTVAL are deliberately
 *     intercepted there and recorded as a distinct PostgresSequenceInfo (result.getSequences()),
 *     not as a generic PostgresCallInfo — i.e. there IS a dedicated sequence-reference concept.
 *     This audit verifies that mechanism actually fires and that it does not interfere with real
 *     table capture in the same statement.
 *
 * Prints real captured output; does not modify any file under src/main.
 */
class PostgresRecursiveCteAndSequenceAuditTest {

    private final PostgresAnalysisEngine engine = new PostgresAnalysisEngine();

    private void dump(String label, PostgresRoutineInfo r) {
        System.out.println("========== " + label + " ==========");
        for (PostgresTableOpInfo t : r.getTableOps()) {
            System.out.println("  tableOp: " + t.getDmlType() + " table=" + t.qualifiedTable()
                    + " alias=" + t.getAlias() + " line=" + t.getLine());
        }
        for (PostgresCallInfo c : r.getCalls()) {
            System.out.println("  call: " + c.qualifiedName() + " line=" + c.getLine());
        }
        for (PostgresVariableInfo v : r.getVariables()) {
            System.out.println("  var: " + v.getName() + " type=" + v.getDataType());
        }
        if (r.getStatements() != null) {
            for (PostgresStatementInfo s : r.getStatements()) {
                System.out.println("  stmt: type=" + s.getType() + " line=" + s.getLine());
            }
        }
        for (PostgresSequenceInfo s : r.getSequences()) {
            System.out.println("  sequence: op=" + s.getOperation() + " line=" + s.getLine());
        }
        System.out.println("  errors=" + r.getErrors());
    }

    // ── 1. CONNECT BY / hierarchical Oracle-only syntax ────────────────────────────────────
    // NOT APPLICABLE to Postgres. Confirmed by grep: PostgresLexer.g4 and PostgresParser.g4
    // contain no CONNECT, START (as a hierarchical clause), LEVEL, SYS_CONNECT_BY_PATH, or
    // SIBLINGS tokens/rules. Postgres itself has no CONNECT BY / START WITH / LEVEL /
    // SYS_CONNECT_BY_PATH / ORDER SIBLINGS BY syntax — this is Oracle-only. No test is
    // fabricated for it; this comment documents the negative finding explicitly.

    // ── 2a. WITH RECURSIVE — simple numeric recursive CTE (anchor + recursive branch) ─────
    @Test
    @DisplayName("2a: WITH RECURSIVE simple counter CTE — does the structured withClause/cteDef path even accept RECURSIVE?")
    void simpleRecursiveCte() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_recursive_counter()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  v_cnt INT;\n" +
                "BEGIN\n" +
                "  WITH RECURSIVE counter(n) AS (\n" +
                "    SELECT 1\n" +
                "    UNION ALL\n" +
                "    SELECT n + 1 FROM counter WHERE n < 10\n" +
                "  )\n" +
                "  SELECT count(*) INTO v_cnt FROM counter;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_recursive_counter", null);
        dump("2a: WITH RECURSIVE simple counter", r);

        boolean counterRecordedAsTable = r.getTableOps().stream()
                .anyMatch(t -> "counter".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> CTE's own name 'counter' wrongly recorded as an external table reference: "
                + counterRecordedAsTable);
        System.out.println("  >>> errors: " + r.getErrors());
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(), "WITH RECURSIVE must parse without errors");
        org.junit.jupiter.api.Assertions.assertFalse(counterRecordedAsTable,
                "CTE's own name must not be recorded as an external table");
    }

    // ── 2b. WITH RECURSIVE — realistic org-chart style CTE with real base tables in BOTH
    // the anchor branch and the recursive branch (the classic pattern) ─────────────────────
    @Test
    @DisplayName("2b: WITH RECURSIVE org-chart CTE — base table capture in anchor + recursive branch, and CTE-self-reference check")
    void orgChartRecursiveCte() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_org_chart(p_root INT)\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  v_cnt INT;\n" +
                "BEGIN\n" +
                "  WITH RECURSIVE subordinates AS (\n" +
                "    SELECT employee_id, manager_id, 1 AS depth\n" +
                "    FROM employees\n" +
                "    WHERE employee_id = p_root\n" +
                "    UNION ALL\n" +
                "    SELECT e.employee_id, e.manager_id, s.depth + 1\n" +
                "    FROM employees e\n" +
                "    JOIN subordinates s ON e.manager_id = s.employee_id\n" +
                "  )\n" +
                "  SELECT count(*) INTO v_cnt FROM subordinates;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_org_chart", null);
        dump("2b: WITH RECURSIVE org-chart", r);

        boolean employeesCaptured = r.getTableOps().stream()
                .anyMatch(t -> "employees".equalsIgnoreCase(t.getTableName()));
        boolean subordinatesWronglyRecordedAsExternalTable = r.getTableOps().stream()
                .anyMatch(t -> "subordinates".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> real base table 'employees' captured (anchor AND recursive branch both reference it): "
                + employeesCaptured + " (expect true)");
        System.out.println("  >>> CTE's own name 'subordinates' wrongly recorded as an external table reference in the "
                + "recursive branch (classic self-reference bug): " + subordinatesWronglyRecordedAsExternalTable
                + " (expect false)");
        System.out.println("  >>> errors: " + r.getErrors());
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty());
        org.junit.jupiter.api.Assertions.assertTrue(employeesCaptured);
        org.junit.jupiter.api.Assertions.assertFalse(subordinatesWronglyRecordedAsExternalTable);
    }

    // ── 2c. Multi-level chained/nested recursive CTEs — one recursive CTE feeding another ──
    @Test
    @DisplayName("2c: chained recursive CTEs — a second CTE referencing an earlier WITH RECURSIVE CTE")
    void chainedRecursiveCtes() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_chained_recursive(p_root INT)\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  v_cnt INT;\n" +
                "BEGIN\n" +
                "  WITH RECURSIVE subordinates AS (\n" +
                "    SELECT employee_id, manager_id FROM employees WHERE employee_id = p_root\n" +
                "    UNION ALL\n" +
                "    SELECT e.employee_id, e.manager_id\n" +
                "    FROM employees e\n" +
                "    JOIN subordinates s ON e.manager_id = s.employee_id\n" +
                "  ),\n" +
                "  enriched AS (\n" +
                "    SELECT sub.employee_id, d.dept_name\n" +
                "    FROM subordinates sub\n" +
                "    JOIN departments d ON d.dept_id = sub.employee_id\n" +
                "  )\n" +
                "  SELECT count(*) INTO v_cnt FROM enriched;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_chained_recursive", null);
        dump("2c: chained recursive CTEs (subordinates -> enriched)", r);

        boolean employeesCaptured = r.getTableOps().stream()
                .anyMatch(t -> "employees".equalsIgnoreCase(t.getTableName()));
        boolean departmentsCaptured = r.getTableOps().stream()
                .anyMatch(t -> "departments".equalsIgnoreCase(t.getTableName()));
        boolean subordinatesWronglyAsTable = r.getTableOps().stream()
                .anyMatch(t -> "subordinates".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> base table 'employees' captured: " + employeesCaptured + " (expect true)");
        System.out.println("  >>> base table 'departments' captured (from the second, non-recursive CTE joining the "
                + "first recursive CTE): " + departmentsCaptured + " (expect true)");
        System.out.println("  >>> 'subordinates' (first CTE's own name) wrongly recorded as external table when "
                + "referenced by the second CTE: " + subordinatesWronglyAsTable + " (expect false)");
        System.out.println("  >>> errors: " + r.getErrors());
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty());
        org.junit.jupiter.api.Assertions.assertTrue(employeesCaptured);
        org.junit.jupiter.api.Assertions.assertTrue(departmentsCaptured);
        org.junit.jupiter.api.Assertions.assertFalse(subordinatesWronglyAsTable);
    }

    // ── 3a. nextval()/currval() sequence functions in an INSERT ... SELECT ─────────────────
    @Test
    @DisplayName("3a: nextval()/currval() sequence usage inside INSERT — real table not dropped, sequence call captured generically")
    void sequenceNextvalCurrvalInInsert() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_seq_insert()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  INSERT INTO orders (order_id, customer_id, status)\n" +
                "  VALUES (nextval('orders_seq'), currval('customer_seq'), 'NEW');\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_seq_insert", null);
        dump("3a: nextval/currval in INSERT", r);

        boolean ordersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        boolean nextvalCaptured = r.getSequences().stream()
                .anyMatch(s -> s.getOperation() == PostgresSequenceInfo.Operation.NEXTVAL);
        boolean currvalCaptured = r.getSequences().stream()
                .anyMatch(s -> s.getOperation() == PostgresSequenceInfo.Operation.CURRVAL);
        System.out.println("  >>> real table 'orders' captured despite nextval()/currval() in VALUES: " + ordersCaptured
                + " (expect true)");
        System.out.println("  >>> nextval(...) captured as a distinct PostgresSequenceInfo (not dropped/confused with "
                + "the real table): " + nextvalCaptured + " (expect true)");
        System.out.println("  >>> currval(...) captured as a distinct PostgresSequenceInfo: " + currvalCaptured
                + " (expect true)");
        System.out.println("  >>> errors: " + r.getErrors());
        org.junit.jupiter.api.Assertions.assertTrue(ordersCaptured);
        org.junit.jupiter.api.Assertions.assertTrue(nextvalCaptured);
        org.junit.jupiter.api.Assertions.assertTrue(currvalCaptured);
        boolean bogusValuesCall = r.getCalls().stream()
                .anyMatch(c -> "VALUES".equalsIgnoreCase(c.qualifiedName()));
        org.junit.jupiter.api.Assertions.assertFalse(bogusValuesCall, "VALUES keyword must not be recorded as a call");
    }

    // ── 3b. nextval()/currval() in a plain SELECT (no target table involved) ───────────────
    @Test
    @DisplayName("3b: SELECT nextval('seq') standalone — captured as call, no bogus table op")
    void sequenceNextvalStandaloneSelect() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_seq_select()\n" +
                "RETURNS INT\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  v_id INT;\n" +
                "BEGIN\n" +
                "  SELECT nextval('orders_seq') INTO v_id;\n" +
                "  RETURN v_id;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_seq_select", null);
        dump("3b: standalone SELECT nextval", r);

        boolean nextvalCaptured = r.getSequences().stream()
                .anyMatch(s -> s.getOperation() == PostgresSequenceInfo.Operation.NEXTVAL);
        boolean bogusSeqNameAsTable = r.getTableOps().stream()
                .anyMatch(t -> "orders_seq".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> nextval(...) captured as a PostgresSequenceInfo: " + nextvalCaptured
                + " (expect true)");
        System.out.println("  >>> sequence literal name 'orders_seq' wrongly recorded as a table op: "
                + bogusSeqNameAsTable + " (expect false)");
        System.out.println("  >>> errors: " + r.getErrors());
        org.junit.jupiter.api.Assertions.assertTrue(nextvalCaptured);
        org.junit.jupiter.api.Assertions.assertFalse(bogusSeqNameAsTable);
    }

    // ── 3c. SERIAL column + GENERATED ... AS IDENTITY in CREATE TABLE (DDL swallowed
    // permissively per createStatement's design — confirming it stays a clean, non-crashing
    // no-op rather than corrupting surrounding statements). ────────────────────────────────
    @Test
    @DisplayName("3c: SERIAL / GENERATED ... AS IDENTITY column DDL — permissive createStatement handling, no crash/desync")
    void serialAndIdentityColumnDdl() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_after_identity_ddl()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  CREATE TABLE orders (\n" +
                "    order_id SERIAL PRIMARY KEY,\n" +
                "    order_seq_id BIGINT GENERATED ALWAYS AS IDENTITY,\n" +
                "    customer_id INT\n" +
                "  );\n" +
                "  INSERT INTO orders (customer_id) VALUES (1);\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_after_identity_ddl", null);
        dump("3c: SERIAL/IDENTITY DDL followed by INSERT", r);

        boolean ordersCapturedInInsert = r.getTableOps().stream()
                .anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> 'orders' captured from the INSERT after the SERIAL/IDENTITY CREATE TABLE "
                + "(i.e. the DDL didn't desync the parser): " + ordersCapturedInInsert + " (expect true)");
        System.out.println("  >>> errors: " + r.getErrors());
        org.junit.jupiter.api.Assertions.assertTrue(ordersCapturedInInsert);
    }
}
