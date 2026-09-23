package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * READ-ONLY AUDIT (2026-09-17) — PostgreSQL-UNIQUE concept coverage for postgres-parser,
 * tested against the real ANTLR-based parser via PostgresAnalysisEngine. Covers:
 *   16. Custom composite types / array-typed variables + indexing/slicing/array functions
 *       (checking for the same "array/attribute-access misclassified as CallInfo" bug class
 *       previously found for collection methods like v_coll.COUNT / v_coll.EXISTS(i)).
 *   17. RAISE NOTICE / RAISE EXCEPTION with format placeholders (checking RAISE isn't
 *       misclassified as a bogus function call).
 *   18. LISTEN/NOTIFY + window functions (ROW_NUMBER() OVER (...)) — table capture and
 *       whether OVER/PARTITION BY corrupts CallInfo capture.
 * Prints real captured output; does not modify any file under src/main.
 */
class PostgresUniqueConceptsAuditTest {

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
        System.out.println("  errors=" + r.getErrors());
    }

    // ── 16a. Standalone CREATE TYPE ... AS (...) as its own statement — does the entry point
    // handle it at all, gracefully or badly? ───────────────────────────────────────────────
    @Test
    @DisplayName("16a: standalone CREATE TYPE AS (composite) — entry point support check")
    void standaloneCreateType() {
        String src = "CREATE TYPE address AS (street text, city text);\n";
        PostgresRoutineInfo r = engine.parse(src, "address_type_probe", null);
        dump("16a: standalone CREATE TYPE", r);
        System.out.println("  >>> errors on standalone CREATE TYPE: " + r.getErrors());
    }

    // ── 16. Custom composite type + array-typed variable: indexing, slicing, array_append ──
    @Test
    @DisplayName("16: array-typed variable indexing/slicing/array functions vs CallInfo misclassification")
    void arrayTypedVariableUsage() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_array_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  v_ids integer[];\n" +
                "  v_first integer;\n" +
                "  v_slice integer[];\n" +
                "  v_addr address;\n" +
                "BEGIN\n" +
                "  v_ids := ARRAY[1,2,3,4,5];\n" +
                "  v_first := v_ids[1];\n" +
                "  v_slice := v_ids[1:3];\n" +
                "  v_ids := array_append(v_ids, 6);\n" +
                "  v_addr.street := 'Main St';\n" +
                "  RAISE NOTICE 'first=%, addr=%', v_first, v_addr.street;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_array_probe", null);
        dump("16: array-typed variable usage", r);

        boolean bogusIdsCall = r.getCalls().stream()
                .anyMatch(c -> "v_ids".equalsIgnoreCase(c.getName()) || "v_addr".equalsIgnoreCase(c.getName()));
        boolean arrayAppendCaptured = r.getCalls().stream()
                .anyMatch(c -> "array_append".equalsIgnoreCase(c.getName()));
        System.out.println("  >>> v_ids/v_addr indexing/attribute-access misclassified as CallInfo: " + bogusIdsCall
                + " (expect false)");
        System.out.println("  >>> array_append(...) captured as a real CallInfo: " + arrayAppendCaptured);
    }

    // ── 17. RAISE NOTICE / RAISE EXCEPTION with format placeholders ────────────────────────
    @Test
    @DisplayName("17: RAISE NOTICE/EXCEPTION with % placeholders not misclassified as a call to RAISE")
    void raiseNoticeAndException() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_raise_probe(v_id INT)\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  v_count INT := 3;\n" +
                "  v_total INT := 10;\n" +
                "BEGIN\n" +
                "  RAISE NOTICE 'Processing % of %', v_count, v_total;\n" +
                "  IF v_id IS NULL THEN\n" +
                "    RAISE EXCEPTION 'Invalid id: %', v_id;\n" +
                "  END IF;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_raise_probe", null);
        dump("17: RAISE NOTICE/EXCEPTION", r);

        boolean bogusRaiseCall = r.getCalls().stream()
                .anyMatch(c -> "RAISE".equalsIgnoreCase(c.getName()));
        System.out.println("  >>> bogus CallInfo entry named RAISE: " + bogusRaiseCall + " (expect false)");
        System.out.println("  >>> errors: " + r.getErrors() + " (expect empty/clean parse)");

        // Regression guard: RAISE EXCEPTION nested inside an IF must not desync the parser
        // (previously produced 3 syntax errors because the otherStatement catch-all stopped
        // consuming at the EXCEPTION token).
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "RAISE NOTICE/EXCEPTION should parse cleanly, got: " + r.getErrors());
        org.junit.jupiter.api.Assertions.assertFalse(bogusRaiseCall);
    }

    // ── 17b. Isolation: RAISE EXCEPTION with a % placeholder at top level (no IF wrapper) —
    // narrows down whether the syntax errors in test 17 come from RAISE EXCEPTION itself or
    // from RAISE EXCEPTION specifically nested inside an IF block.
    @Test
    @DisplayName("17b: isolation — RAISE EXCEPTION with % placeholder, top-level (no IF)")
    void raiseExceptionTopLevelOnly() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_raise_exc_toplevel(v_id INT)\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  RAISE EXCEPTION 'Invalid id: %', v_id;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_raise_exc_toplevel", null);
        dump("17b: RAISE EXCEPTION top-level only", r);
        System.out.println("  >>> errors: " + r.getErrors());
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "RAISE EXCEPTION at top level should parse cleanly, got: " + r.getErrors());
    }

    // ── 17c. Isolation: RAISE EXCEPTION nested inside IF, with NOTHING else in the function
    // (isolates whether it's the IF-nesting itself, independent of the earlier RAISE NOTICE).
    @Test
    @DisplayName("17c: isolation — RAISE EXCEPTION nested inside IF only")
    void raiseExceptionInsideIfOnly() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_raise_exc_if(v_id INT)\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  IF v_id IS NULL THEN\n" +
                "    RAISE EXCEPTION 'Invalid id: %', v_id;\n" +
                "  END IF;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_raise_exc_if", null);
        dump("17c: RAISE EXCEPTION inside IF only", r);
        System.out.println("  >>> errors: " + r.getErrors());
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "RAISE EXCEPTION inside IF should parse cleanly, got: " + r.getErrors());
    }

    // ── 18. LISTEN/NOTIFY + window function ROW_NUMBER() OVER (PARTITION BY ...) ───────────
    @Test
    @DisplayName("18: LISTEN/NOTIFY + ROW_NUMBER() OVER (...) window function table/call capture")
    void listenNotifyAndWindowFunction() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_window_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  v_cnt INT;\n" +
                "BEGIN\n" +
                "  LISTEN payroll_channel;\n" +
                "  SELECT count(*) INTO v_cnt FROM (\n" +
                "    SELECT emp_id, dept, salary,\n" +
                "           ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn\n" +
                "    FROM employees\n" +
                "  ) ranked WHERE ranked.rn = 1;\n" +
                "  NOTIFY payroll_channel, 'processed';\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_window_probe", null);
        dump("18: LISTEN/NOTIFY + window function", r);

        boolean employeesCaptured = r.getTableOps().stream()
                .anyMatch(t -> "employees".equalsIgnoreCase(t.getTableName()));
        boolean rowNumberAsCall = r.getCalls().stream()
                .anyMatch(c -> "ROW_NUMBER".equalsIgnoreCase(c.getName()) || "row_number".equalsIgnoreCase(c.getName()));
        boolean overOrPartitionAsCallOrTable = r.getCalls().stream()
                .anyMatch(c -> "OVER".equalsIgnoreCase(c.getName()) || "PARTITION".equalsIgnoreCase(c.getName()))
                || r.getTableOps().stream()
                .anyMatch(t -> "OVER".equalsIgnoreCase(t.getTableName()) || "PARTITION".equalsIgnoreCase(t.getTableName())
                        || "dept".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> employees table captured despite window function in SELECT list: " + employeesCaptured
                + " (expect true)");
        System.out.println("  >>> ROW_NUMBER() captured as a CallInfo: " + rowNumberAsCall);
        System.out.println("  >>> OVER/PARTITION BY misparsed into a bogus CallInfo/TableOp (e.g. 'dept' as table): "
                + overOrPartitionAsCallOrTable + " (expect false)");
        System.out.println("  >>> errors: " + r.getErrors() + " (expect empty/clean parse)");

        // Regression guard: previously the window function's OVER/PARTITION BY clause was not
        // consumed by any exprAtom, so expr stopped right after `ROW_NUMBER()`, fromClause was
        // never entered, and the whole query fell into the permissive selectRest catch-all -
        // silently dropping `employees` from tableOps (a real, non-crashing data-loss bug).
        org.junit.jupiter.api.Assertions.assertTrue(employeesCaptured,
                "employees table must be captured even with a window function in the SELECT list");
        org.junit.jupiter.api.Assertions.assertFalse(overOrPartitionAsCallOrTable,
                "OVER/PARTITION BY must not be misparsed into a bogus CallInfo/TableOp");
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "LISTEN/NOTIFY + window function should parse cleanly, got: " + r.getErrors());
    }

    // ── 18b. Control: derived-table FROM (subquery) WITHOUT any window function — isolates
    // whether the item-18 table-capture gap is caused by ROW_NUMBER()/OVER specifically, or is
    // a pre-existing "derived table as the outer FROM source" gap unrelated to window functions.
    @Test
    @DisplayName("18b: control — derived-table FROM (subquery) with no window function")
    void derivedTableNoWindowFunctionControl() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_derived_control()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  v_cnt INT;\n" +
                "BEGIN\n" +
                "  SELECT count(*) INTO v_cnt FROM (\n" +
                "    SELECT emp_id, dept, salary FROM employees WHERE salary > 1000\n" +
                "  ) ranked WHERE ranked.salary > 2000;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_derived_control", null);
        dump("18b: control - derived table, no window function", r);
        boolean employeesCaptured = r.getTableOps().stream()
                .anyMatch(t -> "employees".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> employees captured (no window function present): " + employeesCaptured);
    }
}
