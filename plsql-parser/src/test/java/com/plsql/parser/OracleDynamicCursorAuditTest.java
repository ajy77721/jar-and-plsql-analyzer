package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Read-only audit (2026-09-17): dynamic/string-based REF CURSOR and related cursor idioms for
 * Oracle. PRINTS what was captured (tables, dynamic sql info, cursors). Diagnostic harness only —
 * see the invoking task's report for pass/gap verdicts per pattern. Does NOT modify src/main.
 */
public class OracleDynamicCursorAuditTest extends ParserTestBase {

    private static void dump(String label, SubprogramInfo sub) {
        System.out.println("=== " + label + " ===");
        if (sub == null) {
            System.out.println("  (subprogram not found)");
            return;
        }
        List<TableOperationInfo> ops = sub.getTableOperations();
        if (ops == null || ops.isEmpty()) {
            System.out.println("  (no table operations captured)");
        } else {
            for (TableOperationInfo t : ops) {
                System.out.println("  table=" + t.getTableName()
                        + " op=" + t.getOperation()
                        + " objectType=" + t.getObjectType());
            }
        }
        if (sub.getDynamicSql() != null) {
            for (DynamicSqlInfo d : sub.getDynamicSql()) {
                System.out.println("  dynamicSql type=" + d.getType()
                        + " expr=" + d.getSqlExpression()
                        + " unresolvedVar=" + d.getUnresolvedVarName()
                        + " crossProcHint=" + d.getCrossProcSourceHint()
                        + " using=" + d.getUsingVariables());
            }
        } else {
            System.out.println("  (no dynamicSql list)");
        }
        if (sub.getCursors() != null) {
            for (CursorInfo c : sub.getCursors()) {
                System.out.println("  cursor=" + c.getName() + " forLoop=" + c.isForLoop()
                        + " refCursor=" + c.isRefCursor()
                        + " params=" + c.getParameters()
                        + " tables=" + c.getTables()
                        + " stmtOps=" + c.getStmtOperations());
            }
        }
    }

    // 1a. OPEN FOR fully-literal concatenated dynamic SQL (no variables in the table part)
    @Test
    @DisplayName("1a: OPEN cur FOR dynamic SQL string, fully literal concatenation")
    void openForLiteralConcat() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_lit_cur IS
                    TYPE ref_cursor_type IS REF CURSOR;
                    v_cursor ref_cursor_type;
                    v_sql VARCHAR2(500);
                  BEGIN
                    v_sql := 'SELECT * FROM ' || 'orders' || ' WHERE status = ''OPEN''';
                    OPEN v_cursor FOR v_sql;
                  END proc_lit_cur;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "openForLiteralConcat");
        dump("1a: literal concat OPEN FOR", findSub(result, "PROC_LIT_CUR"));
    }

    // 1b. OPEN FOR variable-interrupted dynamic SQL (table name from a variable)
    @Test
    @DisplayName("1b: OPEN cur FOR dynamic SQL string, variable-interrupted table name")
    void openForVariableInterrupted() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_var_cur(p_table_name IN VARCHAR2) IS
                    TYPE ref_cursor_type IS REF CURSOR;
                    v_cursor ref_cursor_type;
                    v_sql VARCHAR2(500);
                  BEGIN
                    v_sql := 'SELECT * FROM ' || p_table_name || ' WHERE status = :1';
                    OPEN v_cursor FOR v_sql USING 'OPEN';
                  END proc_var_cur;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "openForVariableInterrupted");
        dump("1b: variable-interrupted OPEN FOR", findSub(result, "PROC_VAR_CUR"));
    }

    // 1c. OPEN FOR directly against an inline literal string expression (no intermediate var)
    @Test
    @DisplayName("1c: OPEN cur FOR inline literal string with USING bind")
    void openForInlineLiteralWithUsing() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_inline_cur(p_status IN VARCHAR2) IS
                    TYPE ref_cursor_type IS REF CURSOR;
                    v_cursor ref_cursor_type;
                  BEGIN
                    OPEN v_cursor FOR 'SELECT * FROM orders WHERE status = :1' USING p_status;
                  END proc_inline_cur;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "openForInlineLiteralWithUsing");
        dump("1c: inline literal OPEN FOR ... USING", findSub(result, "PROC_INLINE_CUR"));
    }

    // 1d. OPEN FOR a fully dynamic table name built from concatenation across two variables
    // (worst case: nothing resolvable at parse time)
    @Test
    @DisplayName("1d: OPEN cur FOR fully unresolvable dynamic SQL (two runtime variables)")
    void openForFullyUnresolvable() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_unresolvable_cur(p_schema IN VARCHAR2, p_table IN VARCHAR2) IS
                    TYPE ref_cursor_type IS REF CURSOR;
                    v_cursor ref_cursor_type;
                    v_sql VARCHAR2(500);
                  BEGIN
                    v_sql := 'SELECT * FROM ' || p_schema || '.' || p_table;
                    OPEN v_cursor FOR v_sql;
                  END proc_unresolvable_cur;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "openForFullyUnresolvable");
        dump("1d: fully unresolvable OPEN FOR", findSub(result, "PROC_UNRESOLVABLE_CUR"));
    }

    // 7. Cursor FOR loop (implicit cursor, SELECT embedded directly in FOR header)
    @Test
    @DisplayName("7: cursor FOR loop with inline SELECT, multi-table")
    void cursorForLoopInlineSelect() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_for_loop IS
                  BEGIN
                    FOR rec IN (SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id) LOOP
                      NULL;
                    END LOOP;
                  END proc_for_loop;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "cursorForLoopInlineSelect");
        dump("7: cursor FOR loop inline SELECT", findSub(result, "PROC_FOR_LOOP"));
    }

    // 8. Cursor with parameters — must not be misclassified as a call
    @Test
    @DisplayName("8: cursor with parameters, distinguished from a call")
    void cursorWithParameters() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_cur_params IS
                    CURSOR cur_by_status(p_status VARCHAR2, p_min_amt NUMBER) IS
                      SELECT id, amount FROM orders WHERE status = p_status AND amount >= p_min_amt;
                    v_rec cur_by_status%ROWTYPE;
                  BEGIN
                    OPEN cur_by_status('OPEN', 100);
                    FETCH cur_by_status INTO v_rec;
                    CLOSE cur_by_status;
                  END proc_cur_params;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "cursorWithParameters");
        dump("8/9: cursor with params + %ROWTYPE anchored to cursor", findSub(result, "PROC_CUR_PARAMS"));
        // Bug 7: v_rec cur_by_status%ROWTYPE anchors to a declared CURSOR, not a table — must not be
        // recorded as a fake table dependency named CUR_BY_STATUS.
        for (ParsedObject obj : result.getObjects()) {
            System.out.println("  [9] dependency tables=" + obj.getDependencies().getTables());
            org.junit.jupiter.api.Assertions.assertFalse(
                    obj.getDependencies().getTables().contains("CUR_BY_STATUS"),
                    "cursor%ROWTYPE must not be recorded as a table dependency: "
                            + obj.getDependencies().getTables());
        }
        org.junit.jupiter.api.Assertions.assertTrue(
                result.getObjects().stream().anyMatch(o ->
                        o.getDependencies().getTables().contains("ORDERS")),
                "the real table dependency (ORDERS, from the cursor's own SELECT) must still be captured");
    }

    // 6. SYS_REFCURSOR passed as OUT param between procedures (cross-procedure tracking)
    @Test
    @DisplayName("6: SYS_REFCURSOR OUT param opened in one proc, fetched via a caller")
    void refCursorAcrossProcedures() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE open_orders_cursor(p_cur OUT SYS_REFCURSOR) IS
                  BEGIN
                    OPEN p_cur FOR SELECT id, status FROM orders WHERE status = 'OPEN';
                  END open_orders_cursor;

                  PROCEDURE consume_orders_cursor IS
                    v_cur SYS_REFCURSOR;
                    v_id NUMBER;
                    v_status VARCHAR2(20);
                  BEGIN
                    open_orders_cursor(v_cur);
                    LOOP
                      FETCH v_cur INTO v_id, v_status;
                      EXIT WHEN v_cur%NOTFOUND;
                    END LOOP;
                    CLOSE v_cur;
                  END consume_orders_cursor;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "refCursorAcrossProcedures");
        dump("6a: open_orders_cursor (owns the OPEN FOR SELECT)", findSub(result, "OPEN_ORDERS_CURSOR"));
        dump("6b: consume_orders_cursor (receives cursor via call, only FETCH/CLOSE)", findSub(result, "CONSUME_ORDERS_CURSOR"));
    }
}
