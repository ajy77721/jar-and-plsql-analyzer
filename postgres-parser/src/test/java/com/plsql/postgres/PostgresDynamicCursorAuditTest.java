package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * READ-ONLY AUDIT (2026-09-17) — dynamic/string-based cursor + related PL/pgSQL idiom
 * capture gaps for postgres-parser. Companion to PostgresDmlCaptureGapAuditTest, focused on
 * OPEN ... FOR EXECUTE forms, format()-built dynamic SQL, cursor FOR loops, cursor
 * parameters, and %ROWTYPE anchored to a cursor. Prints real captured output; does not
 * modify any file under src/main.
 */
class PostgresDynamicCursorAuditTest {

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
                        + " source=" + d.getSqlSource() + " usingExprs=" + d.getUsingExprs());
            }
        }
        if (r.getCursors() != null) {
            for (PostgresCursorInfo c : r.getCursors()) {
                System.out.println("  cursor=" + c.getName() + " selectText=" + c.getSelectText()
                        + " ops=" + c.getOperations().stream().map(PostgresCursorInfo.Op::getKind).toList());
            }
        }
        System.out.println("  errors=" + r.getErrors());
    }

    // 1. OPEN cur FOR EXECUTE v_sql; where v_sql was built via concatenation (variable-interrupted)
    @Test
    @DisplayName("1: OPEN cursor FOR EXECUTE variable-interrupted dynamic SQL")
    void openForExecuteVariable() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_open_for_exec_var()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  v_sql TEXT;\n" +
                "  v_table_name TEXT := 'orders';\n" +
                "  cur1 REFCURSOR;\n" +
                "BEGIN\n" +
                "  v_sql := 'SELECT * FROM ' || v_table_name || ' WHERE status = $1';\n" +
                "  OPEN cur1 FOR EXECUTE v_sql;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_open_for_exec_var", null);
        dump("1: OPEN FOR EXECUTE (variable-interrupted)", r);

        // Bug 2 fix: OPEN cur FOR EXECUTE <expr> now parses cleanly (no error recovery) and is
        // captured as an EXECUTE dynamic-SQL entry whose source is the variable expression.
        assertTrue(r.getErrors().isEmpty(), "expected no parse errors: " + r.getErrors());
        assertEquals(1, r.getDynamicSql().size());
        PostgresDynamicSqlInfo d = r.getDynamicSql().get(0);
        assertEquals(PostgresDynamicSqlInfo.Kind.EXECUTE, d.getKind());
        assertEquals("v_sql", d.getSqlSource());
        // No literal/format() SQL is present at parse time — a table must never be fabricated.
        assertTrue(r.getTableOps().isEmpty());
    }

    // 2. OPEN cur FOR EXECUTE '<literal>' USING v_status; — fully literal with USING bind
    @Test
    @DisplayName("2: OPEN cursor FOR EXECUTE fully-literal SQL with USING bind")
    void openForExecuteLiteralUsing() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_open_for_exec_lit()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  cur1 REFCURSOR;\n" +
                "  v_status TEXT := 'ACTIVE';\n" +
                "BEGIN\n" +
                "  OPEN cur1 FOR EXECUTE 'SELECT * FROM orders WHERE status = $1' USING v_status;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_open_for_exec_lit", null);
        dump("2: OPEN FOR EXECUTE (literal + USING)", r);

        // Bug 2 fix: fully-literal OPEN ... FOR EXECUTE '<sql>' USING v now parses, is captured
        // as an EXECUTE dynamic-SQL entry with its USING bind recorded, and the table name is
        // extractable at parse time (mirrors the plain EXECUTE literal-extraction path).
        assertTrue(r.getErrors().isEmpty(), "expected no parse errors: " + r.getErrors());
        assertEquals(1, r.getDynamicSql().size());
        PostgresDynamicSqlInfo d = r.getDynamicSql().get(0);
        assertEquals(PostgresDynamicSqlInfo.Kind.EXECUTE, d.getKind());
        assertEquals("'SELECT * FROM orders WHERE status = $1'", d.getSqlSource());
        assertEquals(1, d.getUsingExprs().size());
        assertEquals("v_status", d.getUsingExprs().get(0));
        assertEquals(1, r.getTableOps().size());
        assertEquals(PostgresTableOpInfo.DmlType.DYNAMIC, r.getTableOps().get(0).getDmlType());
        assertEquals("orders", r.getTableOps().get(0).qualifiedTable());
    }

    // 3. OPEN cur FOR EXECUTE format('SELECT * FROM %I WHERE id = $1', v_table_name) USING v_id;
    @Test
    @DisplayName("3: OPEN cursor FOR EXECUTE format(%I,...) idiomatic dynamic SQL")
    void openForExecuteFormat() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_open_for_exec_format()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  cur1 REFCURSOR;\n" +
                "  v_table_name TEXT := 'orders';\n" +
                "  v_id INT := 42;\n" +
                "BEGIN\n" +
                "  OPEN cur1 FOR EXECUTE format('SELECT * FROM %I WHERE id = $1', v_table_name) USING v_id;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_open_for_exec_format", null);
        dump("3: OPEN FOR EXECUTE format(%I, ...)", r);

        // Bug 2+3 fix: OPEN ... FOR EXECUTE format(...) parses cleanly and is captured as an
        // EXECUTE entry. v_table_name is a variable (not a literal) at the format() call site,
        // so a table must never be fabricated from it — instead unresolvedVarName records the gap.
        assertTrue(r.getErrors().isEmpty(), "expected no parse errors: " + r.getErrors());
        assertEquals(1, r.getDynamicSql().size());
        PostgresDynamicSqlInfo d = r.getDynamicSql().get(0);
        assertEquals(PostgresDynamicSqlInfo.Kind.EXECUTE, d.getKind());
        assertEquals("format('SELECT * FROM %I WHERE id = $1', v_table_name)", d.getSqlSource());
        assertEquals(1, d.getUsingExprs().size());
        assertEquals("v_id", d.getUsingExprs().get(0));
        assertEquals("v_table_name", d.getUnresolvedVarName());
        assertTrue(r.getTableOps().isEmpty(), "must not fabricate a table name from a variable");
    }

    // 3b. Plain EXECUTE format(...) (no OPEN cursor) — sanity check whether format() is ever
    // recognized anywhere in the visitor, independent of the cursor-open path.
    @Test
    @DisplayName("3b: plain EXECUTE format(%I,...) without a cursor")
    void plainExecuteFormat() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_exec_format()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  v_table_name TEXT := 'orders';\n" +
                "BEGIN\n" +
                "  EXECUTE format('SELECT count(*) FROM %I', v_table_name);\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_exec_format", null);
        dump("3b: plain EXECUTE format(%I, ...)", r);

        // Bug 3 fix: format(%I, ...) is recognized independent of the cursor-open path too.
        assertTrue(r.getErrors().isEmpty(), "expected no parse errors: " + r.getErrors());
        assertEquals(1, r.getDynamicSql().size());
        PostgresDynamicSqlInfo d = r.getDynamicSql().get(0);
        assertEquals(PostgresDynamicSqlInfo.Kind.EXECUTE, d.getKind());
        assertEquals("format('SELECT count(*) FROM %I', v_table_name)", d.getSqlSource());
        assertEquals("v_table_name", d.getUnresolvedVarName());
        assertTrue(r.getTableOps().isEmpty(), "must not fabricate a table name from a variable");
    }

    // 4. Cursor FOR loop: FOR rec IN <query> LOOP ... END LOOP — table capture in the header SELECT.
    @Test
    @DisplayName("4: cursor FOR loop with SELECT embedded directly in the header")
    void cursorForLoopInlineSelect() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_for_loop_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  rec RECORD;\n" +
                "BEGIN\n" +
                "  FOR rec IN SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.cust_id LOOP\n" +
                "    RAISE NOTICE '%', rec.id;\n" +
                "  END LOOP;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_for_loop_probe", null);
        dump("4: cursor FOR loop inline SELECT", r);
    }

    // 5. Cursor with parameters: CURSOR cur(p_id INT) FOR SELECT ... WHERE id = p_id;
    @Test
    @DisplayName("5: cursor declared with parameters")
    void cursorWithParameters() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_cursor_params()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  cur1 CURSOR (p_id INT) FOR SELECT * FROM orders WHERE id = p_id;\n" +
                "BEGIN\n" +
                "  OPEN cur1(42);\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_cursor_params", null);
        dump("5: cursor with parameters", r);

        // Bug 4 fix: `CURSOR cur(p_id INT) FOR SELECT ...` and `OPEN cur(42)` both parse cleanly
        // (previously a hard parse error); the cursor's parameter list is captured, and its
        // SELECT's table is still extracted.
        assertTrue(r.getErrors().isEmpty(), "expected no parse errors: " + r.getErrors());
        assertEquals(1, r.getCursors().size());
        PostgresCursorInfo c = r.getCursors().get(0);
        assertEquals("cur1", c.getName());
        assertEquals(1, c.getParameters().size());
        PostgresParamInfo p = c.getParameters().get(0);
        assertEquals("p_id", p.getName());
        assertEquals("INT", p.getDataType());
        assertEquals(PostgresParamInfo.Direction.IN, p.getDirection());
        assertEquals(1, r.getTableOps().size());
        assertEquals("orders", r.getTableOps().get(0).qualifiedTable());
        assertEquals(1, c.getOperations().size());
        assertEquals("OPEN", c.getOperations().get(0).getKind());
    }

    // 6. %ROWTYPE anchored to a cursor: v_rec cur1%ROWTYPE;
    @Test
    @DisplayName("6: %ROWTYPE anchored to a cursor")
    void cursorRowtype() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_cursor_rowtype()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  cur1 CURSOR FOR SELECT * FROM orders;\n" +
                "  v_rec cur1%ROWTYPE;\n" +
                "BEGIN\n" +
                "  OPEN cur1;\n" +
                "  FETCH cur1 INTO v_rec;\n" +
                "  CLOSE cur1;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = engine.parse(src, "fn_cursor_rowtype", null);
        dump("6: %ROWTYPE anchored to cursor", r);
    }
}
