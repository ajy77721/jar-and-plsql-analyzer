package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.SqlServerRoutineInfo;
import com.plsql.sqlserver.model.SqlServerTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * AUDIT ONLY — read-only investigation of three control-flow-adjacent T-SQL concepts against the
 * real ANTLR grammar/visitor:
 *   1) IF [NOT] EXISTS(SELECT ...) wrapping DML (the common "upsert" pattern)
 *   2) WHILE @@FETCH_STATUS = 0 cursor loops with nested IF/BREAK/CONTINUE
 *   3) GOTO/label edge cases (forward jump, label-as-loop-target)
 *
 * Does NOT modify any main/ code. Each test prints dump() evidence and then asserts what the
 * ACTUAL observed behaviour is (not what would be ideal), so the assertions themselves are part
 * of the audit record. See the class-level report emitted to stdout for the ranked bug list.
 */
class SqlServerControlFlowAuditTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private SqlServerRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    private void dump(String label, SqlServerRoutineInfo r) {
        System.out.println("=== " + label + " ===");
        System.out.println("tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.qualifiedTable() + "@" + t.getLine())
                .toList());
        System.out.println("calls: " + r.getCalls().stream()
                .map(c -> (c.getSchema() != null ? c.getSchema() + "." : "") + c.getName() + "@" + c.getLine())
                .toList());
        System.out.println("statements: " + r.getStatements().stream()
                .map(s -> s.getType() + "@" + s.getLine())
                .toList());
        System.out.println("cursors: " + r.getCursors().stream()
                .map(c -> c.getName() + " query=" + c.getQuery() + " ops=" + c.getStmtOperations())
                .toList());
        System.out.println("errors: " + r.getErrors());
    }

    // ─────────────────────────────────────────────────────────────────────────────────────────
    // Concept 1: IF [NOT] EXISTS(SELECT ...) wrapping DML — the "upsert" pattern
    // ─────────────────────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Concept1-A: IF NOT EXISTS(SELECT...) BEGIN INSERT END ELSE BEGIN UPDATE END")
    void concept1_ifNotExistsUpsert() {
        String src =
                "CREATE PROCEDURE usp_upsert_customer\n" +
                "    @cust_id INT, @name NVARCHAR(100)\n" +
                "AS\nBEGIN\n" +
                "IF NOT EXISTS (SELECT 1 FROM customers WHERE id = @cust_id)\n" +
                "BEGIN\n" +
                "  INSERT INTO customers (id, name) VALUES (@cust_id, @name);\n" +
                "END\n" +
                "ELSE\n" +
                "BEGIN\n" +
                "  UPDATE customers SET name = @name WHERE id = @cust_id;\n" +
                "END\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("concept1_ifNotExistsUpsert", r);

        boolean existsSelectCaptured = r.getTableOps().stream()
                .anyMatch(t -> "customers".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.SELECT);
        boolean insertCaptured = r.getTableOps().stream()
                .anyMatch(t -> "customers".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.INSERT);
        boolean updateCaptured = r.getTableOps().stream()
                .anyMatch(t -> "customers".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.UPDATE);

        System.out.println("concept1-A: EXISTS-subquery SELECT captured=" + existsSelectCaptured
                + " INSERT captured=" + insertCaptured + " UPDATE captured=" + updateCaptured
                + " totalTableOps=" + r.getTableOps().size());

        // ACTUAL OBSERVED: the IF's condition (`expr`) is never scanned by any table-extraction
        // routine — ifStatement has no visitIfStatement override, and the generic per-body scans
        // (extractCaseAndSubqueries / extractQualifiedCalls / extractSequences) only ever cover
        // CASE/SUBQUERY *statement inventory* markers and qualified calls, never FROM/JOIN table
        // extraction (that only happens inside visitSelectStatement / addTableOpsFromText, which
        // is never invoked for the parenthesised EXISTS(SELECT ...) sitting inside an `expr`).
        // So the "customers" read inside EXISTS is silently dropped; only INSERT + UPDATE survive.
        org.junit.jupiter.api.Assertions.assertTrue(existsSelectCaptured,
                "FIXED: the SELECT inside IF NOT EXISTS(...) must be captured as a table op");
        org.junit.jupiter.api.Assertions.assertTrue(insertCaptured, "INSERT branch is captured");
        org.junit.jupiter.api.Assertions.assertTrue(updateCaptured, "ELSE UPDATE branch is captured");
        org.junit.jupiter.api.Assertions.assertEquals(3, r.getTableOps().size(),
                "3 ops: the EXISTS-subquery SELECT plus INSERT + UPDATE");
    }

    @Test
    @DisplayName("Concept1-B: IF EXISTS(SELECT...) single-branch DELETE (no ELSE)")
    void concept1_ifExistsSingleBranch() {
        String src =
                "CREATE PROCEDURE usp_purge_order\n" +
                "    @order_id INT\n" +
                "AS\nBEGIN\n" +
                "IF EXISTS (SELECT 1 FROM order_lines WHERE order_id = @order_id AND qty = 0)\n" +
                "BEGIN\n" +
                "  DELETE FROM order_lines WHERE order_id = @order_id AND qty = 0;\n" +
                "END\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("concept1_ifExistsSingleBranch", r);

        boolean existsSelectCaptured = r.getTableOps().stream()
                .anyMatch(t -> "order_lines".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.SELECT);
        boolean deleteCaptured = r.getTableOps().stream()
                .anyMatch(t -> "order_lines".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.DELETE);

        System.out.println("concept1-B: EXISTS-subquery SELECT captured=" + existsSelectCaptured
                + " DELETE captured=" + deleteCaptured + " totalTableOps=" + r.getTableOps().size());

        // Same defect as 1-A: the EXISTS(SELECT ...) read is dropped, DELETE alone survives.
        org.junit.jupiter.api.Assertions.assertTrue(existsSelectCaptured,
                "FIXED: EXISTS-subquery SELECT is captured even with a single IF branch (no ELSE)");
        org.junit.jupiter.api.Assertions.assertTrue(deleteCaptured);
        org.junit.jupiter.api.Assertions.assertEquals(2, r.getTableOps().size());
    }

    @Test
    @DisplayName("Concept1-C: nested IF EXISTS inside a WHILE loop (compounding the two audit concepts)")
    void concept1_ifExistsInsideWhile() {
        String src =
                "CREATE PROCEDURE usp_drain_queue\n" +
                "AS\nBEGIN\n" +
                "DECLARE @n INT = 0;\n" +
                "WHILE @n < 10\n" +
                "BEGIN\n" +
                "  IF EXISTS (SELECT 1 FROM queue_items WHERE processed = 0)\n" +
                "  BEGIN\n" +
                "    UPDATE queue_items SET processed = 1 WHERE id = @n;\n" +
                "  END\n" +
                "  SET @n = @n + 1;\n" +
                "END\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("concept1_ifExistsInsideWhile", r);

        boolean existsSelectCaptured = r.getTableOps().stream()
                .anyMatch(t -> "queue_items".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.SELECT);
        boolean updateCaptured = r.getTableOps().stream()
                .anyMatch(t -> "queue_items".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.UPDATE);

        System.out.println("concept1-C: EXISTS-subquery SELECT captured (inside WHILE)=" + existsSelectCaptured
                + " UPDATE captured=" + updateCaptured);

        org.junit.jupiter.api.Assertions.assertTrue(existsSelectCaptured,
                "FIXED: also captured inside a WHILE loop body");
        org.junit.jupiter.api.Assertions.assertTrue(updateCaptured);
    }

    // ─────────────────────────────────────────────────────────────────────────────────────────
    // Concept 2: WHILE @@FETCH_STATUS = 0 cursor loop, nested IF/BREAK/CONTINUE
    // ─────────────────────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Concept2-A: full cursor loop with nested IF/CONTINUE and IF/BREAK")
    void concept2_cursorLoopWithBreakContinue() {
        String src =
                "CREATE PROCEDURE usp_process_orders\n" +
                "AS\nBEGIN\n" +
                "DECLARE @id INT;\n" +
                "DECLARE cur CURSOR FOR SELECT id FROM orders WHERE status = 'PENDING';\n" +
                "OPEN cur;\n" +
                "FETCH NEXT FROM cur INTO @id;\n" +
                "WHILE @@FETCH_STATUS = 0\n" +
                "BEGIN\n" +
                "  IF @id % 2 = 0\n" +
                "  BEGIN\n" +
                "    UPDATE orders SET status = 'EVEN' WHERE id = @id;\n" +
                "    CONTINUE;\n" +
                "  END\n" +
                "  IF @id > 1000\n" +
                "    BREAK;\n" +
                "  UPDATE orders SET status = 'ODD' WHERE id = @id;\n" +
                "  FETCH NEXT FROM cur INTO @id;\n" +
                "END\n" +
                "CLOSE cur;\n" +
                "DEALLOCATE cur;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("concept2_cursorLoopWithBreakContinue", r);

        long ordersSelectCount = r.getTableOps().stream()
                .filter(t -> "orders".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.SELECT)
                .count();
        long ordersUpdateCount = r.getTableOps().stream()
                .filter(t -> "orders".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.UPDATE)
                .count();
        boolean cursorRecorded = r.getCursors().stream().anyMatch(c -> "cur".equalsIgnoreCase(c.getName()));
        boolean whileStatementRecorded = r.getStatements().stream().anyMatch(s -> "WHILE_LOOP".equals(s.getType()));
        boolean bothIfsRecorded = r.getStatements().stream().filter(s -> "IF".equals(s.getType())).count() == 2;

        System.out.println("concept2-A: orders SELECT(cursor source) count=" + ordersSelectCount
                + " orders UPDATE count=" + ordersUpdateCount
                + " cursorRecorded=" + cursorRecorded
                + " whileStatementRecorded=" + whileStatementRecorded
                + " bothIfsRecorded=" + bothIfsRecorded
                + " errors=" + r.getErrors());

        // ACTUAL OBSERVED: this works correctly. The cursor's own SELECT (its DECLARE ... FOR
        // SELECT sibling statement) is walked as an ordinary sibling statement and captured once
        // as a SELECT op on "orders"; the two UPDATE statements inside the WHILE body (one before
        // CONTINUE, one before the next FETCH) are captured independently via visitUpdateStatement,
        // which does not de-dupe by table+type (unlike addTableOp), so both survive as separate
        // ops. Nested IF/BREAK/CONTINUE do not block ordinary ANTLR tree recursion since there is
        // no override that short-circuits visitChildren for ifStatement/whileStatement.
        org.junit.jupiter.api.Assertions.assertEquals(1, ordersSelectCount,
                "cursor source SELECT on orders is captured exactly once");
        org.junit.jupiter.api.Assertions.assertEquals(2, ordersUpdateCount,
                "both UPDATE orders statements inside the loop body are captured, none clobbers the other");
        org.junit.jupiter.api.Assertions.assertTrue(cursorRecorded);
        org.junit.jupiter.api.Assertions.assertTrue(whileStatementRecorded);
        org.junit.jupiter.api.Assertions.assertTrue(bothIfsRecorded);
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors() == null || r.getErrors().isEmpty(),
                "no parse errors for a standard cursor+WHILE+BREAK/CONTINUE loop");
    }

    @Test
    @DisplayName("Concept2-B: cursor loop with a call inside the loop body, nested one level deeper")
    void concept2_cursorLoopWithNestedCall() {
        String src =
                "CREATE PROCEDURE usp_notify_pending\n" +
                "AS\nBEGIN\n" +
                "DECLARE @id INT;\n" +
                "DECLARE cur CURSOR FOR SELECT id FROM notifications WHERE sent = 0;\n" +
                "OPEN cur;\n" +
                "FETCH NEXT FROM cur INTO @id;\n" +
                "WHILE @@FETCH_STATUS = 0\n" +
                "BEGIN\n" +
                "  IF @id IS NOT NULL\n" +
                "  BEGIN\n" +
                "    EXEC dbo.usp_send_notification @id;\n" +
                "    UPDATE notifications SET sent = 1 WHERE id = @id;\n" +
                "  END\n" +
                "  FETCH NEXT FROM cur INTO @id;\n" +
                "END\n" +
                "CLOSE cur;\n" +
                "DEALLOCATE cur;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("concept2_cursorLoopWithNestedCall", r);

        boolean callCaptured = r.getCalls().stream()
                .anyMatch(c -> "usp_send_notification".equalsIgnoreCase(c.getName()));
        boolean updateCaptured = r.getTableOps().stream()
                .anyMatch(t -> "notifications".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.UPDATE);
        boolean selectCaptured = r.getTableOps().stream()
                .anyMatch(t -> "notifications".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.SELECT);

        System.out.println("concept2-B: call captured=" + callCaptured
                + " UPDATE captured=" + updateCaptured + " cursor-source SELECT captured=" + selectCaptured
                + " errors=" + r.getErrors());

        org.junit.jupiter.api.Assertions.assertTrue(callCaptured,
                "EXEC call two levels deep inside WHILE->IF is captured");
        org.junit.jupiter.api.Assertions.assertTrue(updateCaptured);
        org.junit.jupiter.api.Assertions.assertTrue(selectCaptured);
    }

    // ─────────────────────────────────────────────────────────────────────────────────────────
    // Concept 3: GOTO/label edge cases
    // ─────────────────────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Concept3-A: forward-jumping GOTO (label defined AFTER the GOTO)")
    void concept3_forwardGoto() {
        String src =
                "CREATE PROCEDURE usp_forward_jump\n" +
                "    @flag INT\n" +
                "AS\nBEGIN\n" +
                "IF @flag = 1\n" +
                "  GOTO SkipWork;\n" +
                "UPDATE audit_log SET note = 'did work' WHERE id = 1;\n" +
                "INSERT INTO audit_log (id, note) VALUES (2, 'more work');\n" +
                "SkipWork:\n" +
                "SELECT 1;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("concept3_forwardGoto", r);

        boolean updateCaptured = r.getTableOps().stream()
                .anyMatch(t -> "audit_log".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.UPDATE);
        boolean insertCaptured = r.getTableOps().stream()
                .anyMatch(t -> "audit_log".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.INSERT);
        boolean gotoRecorded = r.getStatements().stream().anyMatch(s -> "GOTO".equals(s.getType()));

        System.out.println("concept3-A: parse errors=" + r.getErrors()
                + " UPDATE captured=" + updateCaptured + " INSERT captured=" + insertCaptured
                + " gotoRecorded=" + gotoRecorded);

        // ACTUAL OBSERVED: parses cleanly (labelStatement: ID COLON is one of the `statement`
        // alternatives, so a forward reference needs no special grammar support — the label is
        // just another statement in the same list, matched independently of where any GOTO that
        // targets it happens to sit). The statements between GOTO and the label are ordinary
        // sibling statements and are captured exactly as if the GOTO were not there at all (this
        // parser does no control-flow-reachability analysis, so it also does NOT flag that the
        // UPDATE/INSERT are unreachable when @flag = 1 — that's out of scope, not a bug here).
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors() == null || r.getErrors().isEmpty(),
                "forward-jumping GOTO parses without error");
        org.junit.jupiter.api.Assertions.assertTrue(updateCaptured);
        org.junit.jupiter.api.Assertions.assertTrue(insertCaptured);
        org.junit.jupiter.api.Assertions.assertTrue(gotoRecorded);
    }

    @Test
    @DisplayName("Concept3-B: label used as a retry loop target (backward GOTO)")
    void concept3_labelAsLoopTarget() {
        String src =
                "CREATE PROCEDURE usp_retry_insert\n" +
                "AS\nBEGIN\n" +
                "DECLARE @attempt INT = 0;\n" +
                "retry:\n" +
                "INSERT INTO retry_log (attempt) VALUES (@attempt);\n" +
                "IF @attempt < 3\n" +
                "BEGIN\n" +
                "  SET @attempt = @attempt + 1;\n" +
                "  GOTO retry;\n" +
                "END\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("concept3_labelAsLoopTarget", r);

        boolean insertCaptured = r.getTableOps().stream()
                .anyMatch(t -> "retry_log".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.INSERT);
        long gotoCount = r.getStatements().stream().filter(s -> "GOTO".equals(s.getType())).count();

        System.out.println("concept3-B: parse errors=" + r.getErrors()
                + " INSERT captured=" + insertCaptured + " gotoCount=" + gotoCount);

        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors() == null || r.getErrors().isEmpty(),
                "label-as-loop-target (backward GOTO) parses without error");
        org.junit.jupiter.api.Assertions.assertTrue(insertCaptured);
        org.junit.jupiter.api.Assertions.assertEquals(1, gotoCount);
    }

    @Test
    @DisplayName("Concept3-C: label immediately followed by BEGIN...END block (no statement in between)")
    void concept3_labelFollowedByBlock() {
        String src =
                "CREATE PROCEDURE usp_label_then_block\n" +
                "AS\nBEGIN\n" +
                "GOTO DoWork;\n" +
                "PRINT 'unreachable';\n" +
                "DoWork:\n" +
                "BEGIN\n" +
                "  UPDATE settings SET value = 'x' WHERE id = 1;\n" +
                "END\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("concept3_labelFollowedByBlock", r);

        boolean updateCaptured = r.getTableOps().stream()
                .anyMatch(t -> "settings".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == SqlServerTableOpInfo.DmlType.UPDATE);

        System.out.println("concept3-C: parse errors=" + r.getErrors() + " UPDATE captured=" + updateCaptured);

        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors() == null || r.getErrors().isEmpty(),
                "label immediately followed by a BEGIN...END block parses without error");
        org.junit.jupiter.api.Assertions.assertTrue(updateCaptured);
    }
}
