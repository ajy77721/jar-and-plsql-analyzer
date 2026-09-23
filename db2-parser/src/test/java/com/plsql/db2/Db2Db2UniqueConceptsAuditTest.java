package com.plsql.db2;

import com.plsql.db2.engine.Db2AnalysisEngine;
import com.plsql.db2.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * AUDIT ONLY (read-only investigation, 2026-09-17) — DB2-unique concepts tested against the real
 * ANTLR parser with realistic fixtures. Mirrors the style of Db2DmlCaptureGapAuditTest /
 * Db2DynamicCursorAuditTest. Prints evidence; no hard assertions (exploratory).
 *
 * Covers:
 *  13. XML data type column / XMLTABLE / XMLQUERY usage in a procedure body.
 *  14. DB2 isolation clause (WITH UR / WITH CS) on a SELECT.
 *  15. GET DIAGNOSTICS inside a procedure with nested condition handlers (outer SQLEXCEPTION,
 *      inner SQLSTATE-specific), embedded among real DML.
 */
class Db2Db2UniqueConceptsAuditTest {

    private final Db2AnalysisEngine engine = new Db2AnalysisEngine();

    private static void dump(String label, Db2RoutineInfo r) {
        System.out.println("=== " + label + " errors === " + r.getErrors());
        System.out.println("--- tableOps ---");
        for (Db2TableOpInfo t : r.getTableOps()) {
            System.out.println("  " + t.getDmlType() + " " + t.qualifiedTable()
                    + " alias=" + t.getAlias() + " line=" + t.getLine());
        }
        System.out.println("--- statements ---");
        for (Db2StatementInfo s : r.getStatements()) {
            System.out.println("  " + s.getType() + " line=" + s.getLine());
        }
        System.out.println("--- calls ---");
        for (Db2CallInfo c : r.getCalls()) {
            System.out.println("  " + c.getSchema() + "." + c.getName() + " line=" + c.getLine());
        }
        System.out.println("--- cursors ---");
        for (Db2CursorInfo c : r.getCursors()) {
            System.out.println("  cursor=" + c.getName() + " selectText=" + c.getSelectText()
                    + " line=" + c.getLine());
        }
        System.out.println("--- dynamicSql ---");
        for (Db2DynamicSqlInfo d : r.getDynamicSql()) {
            System.out.println("  kind=" + d.getKind() + " stmtName=" + d.getStmtName()
                    + " sqlSource=" + d.getSqlSource() + " line=" + d.getLine());
        }
    }

    // ── Item 13: XML data type column + XMLTABLE shredding in a procedure body ──

    private static final String XML_SRC =
        "CREATE PROCEDURE p_shred_customer_xml(IN p_payload XML)\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE v_cnt INTEGER DEFAULT 0;\n"
      + "  DECLARE v_doc XML;\n"
      + "  SET v_doc = p_payload;\n"
      + "  INSERT INTO customer_xml_staging (cust_id, doc)\n"
      + "    VALUES (1, v_doc);\n"
      + "  SELECT COUNT(*) INTO v_cnt\n"
      + "  FROM XMLTABLE('/Customers/Customer'\n"
      + "    PASSING v_doc\n"
      + "    COLUMNS\n"
      + "      cust_id INTEGER PATH '@id',\n"
      + "      cust_name VARCHAR(100) PATH 'Name'\n"
      + "  ) AS x;\n"
      + "END";

    @Test
    @DisplayName("13: XML column + XMLTABLE shredding in a procedure body — check no crash / no corruption")
    void xmlTableShredding() {
        Db2RoutineInfo r = engine.parse(XML_SRC, "p_shred_customer_xml", null);
        dump("13: XMLTABLE shredding", r);
        boolean stagingInsertCaptured = r.getTableOps().stream()
                .anyMatch(t -> "customer_xml_staging".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == Db2TableOpInfo.DmlType.INSERT);
        System.out.println(">>> stagingInsertCaptured=" + stagingInsertCaptured
                + " totalTableOps=" + r.getTableOps().size()
                + " errorCount=" + r.getErrors().size());

        // Regression guard: XMLTABLE(...) in a FROM clause previously matched tableAtom's plain
        // `tableRef` alternative (matching only the bare function name), so it was captured as a
        // bogus base table literally named "XMLTABLE" while its real args/alias fell into the
        // permissive selectRest catch-all.
        boolean bogusXmlTableTableOp = r.getTableOps().stream()
                .anyMatch(t -> "XMLTABLE".equalsIgnoreCase(t.getTableName()));
        org.junit.jupiter.api.Assertions.assertFalse(bogusXmlTableTableOp,
                "XMLTABLE(...) must not be captured as a bogus base table named XMLTABLE");
        org.junit.jupiter.api.Assertions.assertTrue(stagingInsertCaptured,
                "the real customer_xml_staging INSERT must still be captured");
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty());
    }

    // ── Item 14: DB2 isolation clause WITH UR (uncommitted read) on a SELECT ──

    private static final String ISOLATION_SRC =
        "CREATE PROCEDURE p_report_read_uncommitted(IN p_region CHAR(2))\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE v_cnt INTEGER DEFAULT 0;\n"
      + "  SELECT COUNT(*) INTO v_cnt\n"
      + "  FROM orders o\n"
      + "  INNER JOIN customers c ON o.cust_id = c.cust_id\n"
      + "  WHERE c.region = p_region\n"
      + "  WITH UR;\n"
      + "  DECLARE cur1 CURSOR FOR\n"
      + "    SELECT order_id, status FROM orders WHERE region = p_region WITH CS;\n"
      + "  OPEN cur1;\n"
      + "  CLOSE cur1;\n"
      + "END";

    @Test
    @DisplayName("14: DB2 isolation clause WITH UR / WITH CS on SELECT — check grammar support / graceful handling")
    void isolationClause() {
        Db2RoutineInfo r = engine.parse(ISOLATION_SRC, "p_report_read_uncommitted", null);
        dump("14: WITH UR / WITH CS isolation clause", r);
        boolean ordersJoinCaptured = r.getTableOps().stream()
                .anyMatch(t -> "ORDERS".equalsIgnoreCase(t.getTableName()) && "o".equalsIgnoreCase(t.getAlias()));
        boolean cursorCaptured = !r.getCursors().isEmpty();
        System.out.println(">>> ordersJoinCaptured(despite trailing WITH UR)=" + ordersJoinCaptured
                + " cursorCaptured(despite trailing WITH CS)=" + cursorCaptured
                + " errorCount=" + r.getErrors().size());
    }

    // ── Item 15: GET DIAGNOSTICS inside nested condition handlers, embedded in real DML flow ──

    private static final String NESTED_HANDLER_SRC =
        "CREATE PROCEDURE p_process_batch_payment(IN p_batch_id INTEGER)\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE v_row_count INTEGER DEFAULT 0;\n"
      + "  DECLARE v_dup_count INTEGER DEFAULT 0;\n"
      + "  DECLARE v_sqlstate CHAR(5) DEFAULT '00000';\n"
      + "  DECLARE v_msg VARCHAR(200);\n"
      + "\n"
      + "  DECLARE EXIT HANDLER FOR SQLEXCEPTION\n"
      + "  BEGIN\n"
      + "    GET DIAGNOSTICS EXCEPTION 1 v_sqlstate = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT;\n"
      + "    INSERT INTO error_log (batch_id, sqlstate, message) VALUES (p_batch_id, v_sqlstate, v_msg);\n"
      + "    ROLLBACK;\n"
      + "  END;\n"
      + "\n"
      + "  BEGIN\n"
      + "    DECLARE CONTINUE HANDLER FOR SQLSTATE '23505'\n"
      + "    BEGIN\n"
      + "      GET DIAGNOSTICS v_dup_count = ROW_COUNT;\n"
      + "      INSERT INTO dup_key_log (batch_id, dup_count) VALUES (p_batch_id, v_dup_count);\n"
      + "    END;\n"
      + "\n"
      + "    UPDATE payment_batch SET status = 'PROCESSING' WHERE batch_id = p_batch_id;\n"
      + "\n"
      + "    INSERT INTO payment_ledger (batch_id, cust_id, amount)\n"
      + "      SELECT p.batch_id, p.cust_id, p.amount\n"
      + "      FROM payment_staging p\n"
      + "      WHERE p.batch_id = p_batch_id;\n"
      + "\n"
      + "    GET DIAGNOSTICS v_row_count = ROW_COUNT;\n"
      + "\n"
      + "    UPDATE payment_batch SET status = 'DONE', rows_processed = v_row_count\n"
      + "      WHERE batch_id = p_batch_id;\n"
      + "  END;\n"
      + "\n"
      + "  COMMIT;\n"
      + "END";

    @Test
    @DisplayName("15: GET DIAGNOSTICS inside nested handlers (outer SQLEXCEPTION, inner SQLSTATE) + real DML")
    void nestedHandlersWithGetDiagnostics() {
        Db2RoutineInfo r = engine.parse(NESTED_HANDLER_SRC, "p_process_batch_payment", null);
        dump("15: nested handlers + GET DIAGNOSTICS", r);
        boolean updateBatchProcessingCaptured = r.getTableOps().stream()
                .anyMatch(t -> "payment_batch".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == Db2TableOpInfo.DmlType.UPDATE);
        boolean ledgerInsertSelectCaptured = r.getTableOps().stream()
                .anyMatch(t -> "payment_ledger".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == Db2TableOpInfo.DmlType.INSERT);
        boolean stagingSourceCaptured = r.getTableOps().stream()
                .anyMatch(t -> "payment_staging".equalsIgnoreCase(t.getTableName()));
        boolean errorLogInsertInOuterHandlerCaptured = r.getTableOps().stream()
                .anyMatch(t -> "error_log".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == Db2TableOpInfo.DmlType.INSERT);
        boolean dupKeyLogInsertInInnerHandlerCaptured = r.getTableOps().stream()
                .anyMatch(t -> "dup_key_log".equalsIgnoreCase(t.getTableName())
                        && t.getDmlType() == Db2TableOpInfo.DmlType.INSERT);
        System.out.println(">>> updateBatchProcessingCaptured=" + updateBatchProcessingCaptured
                + " ledgerInsertSelectCaptured=" + ledgerInsertSelectCaptured
                + " stagingSourceCaptured=" + stagingSourceCaptured
                + " errorLogInsertInOuterHandlerCaptured=" + errorLogInsertInOuterHandlerCaptured
                + " dupKeyLogInsertInInnerHandlerCaptured=" + dupKeyLogInsertInInnerHandlerCaptured
                + " errorCount=" + r.getErrors().size());
    }
}
