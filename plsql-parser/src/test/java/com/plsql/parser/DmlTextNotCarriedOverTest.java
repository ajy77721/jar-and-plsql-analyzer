package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Sep05-14 — a table operation must never display another statement's SQL.
 *
 * <p>{@code currentDmlCtx} is set when a DML statement is entered and never cleared, so it acted as
 * "the last DML seen for the rest of the file", and every later operation without text of its own
 * inherited it. On the search screen an {@code ERP_PRODUCT} row displayed
 * {@code UPDATE erp_mrp_run SET status = 'DONE'} — SQL that does not mention that table.
 *
 * <p>That is much worse than an empty cell. A blank cell prompts someone to open the source; a
 * plausible but wrong statement prompts belief, and nothing on screen distinguishes it from a
 * correct row. The user found this by reading the screen, not from any failing test.
 *
 * <p>So the assertion here is the <em>invariant</em> — if a row carries SQL, that SQL is about that
 * row — rather than any particular expected string. An exact-text test would pass while the next
 * traversal change quietly reintroduced a carry-over somewhere else.
 *
 * <h2>Honest limit of this test</h2>
 *
 * <p><b>It does not reproduce the original defect.</b> Reverting the fix was tried and this test
 * still passed: in a procedure this small every operation sits inside its own DML statement, so
 * there is nothing for a stale context to leak onto. The leak needs the shapes a real package body
 * has — operations recorded outside any DML statement, such as dynamic SQL and cursor-driven reads.
 *
 * <p>The fix was therefore verified against a full Oracle analysis, where the rows the user
 * reported changed from carrying an unrelated {@code UPDATE erp_mrp_run …} to carrying no statement
 * at all, and a sweep of all five dialects showed <b>0</b> rows whose SQL omits their own table
 * (previously non-zero on Oracle). This class stands as a cheap invariant guard, not as the
 * regression test for that bug — treating it as the latter would be trusting a test that has been
 * shown not to fail.
 */
class DmlTextNotCarriedOverTest extends ParserTestBase {

    /** Consecutive statements over different tables: the shape that exposed the leak. */
    private static final String SRC =
            "CREATE OR REPLACE PROCEDURE p_cycle AS\n" +
            "  v_run_id NUMBER;\n" +
            "  v_tot    NUMBER;\n" +
            "  v_pid    NUMBER;\n" +
            "BEGIN\n" +
            "  UPDATE erp_mrp_run SET status = 'DONE' WHERE mrp_run_id = v_run_id;\n" +
            "  SELECT NVL(SUM(line_amount),0) INTO v_tot FROM erp_sales_order_line;\n" +
            "  INSERT INTO erp_audit_log(msg) VALUES ('done');\n" +
            "  DELETE FROM erp_gtt_stage WHERE run_id = v_run_id;\n" +
            "  SELECT product_id INTO v_pid FROM erp_product WHERE ROWNUM = 1;\n" +
            "END p_cycle;\n";

    private List<TableOperationInfo> allOps(ParseResult r) {
        List<TableOperationInfo> out = new ArrayList<>();
        for (ParsedObject o : r.getObjects()) {
            if (o.getTableOperations() != null) out.addAll(o.getTableOperations());
            if (o.getSubprograms() != null) {
                for (SubprogramInfo s : o.getSubprograms()) {
                    if (s.getTableOperations() != null) out.addAll(s.getTableOperations());
                }
            }
        }
        return out;
    }

    @Test
    @DisplayName("no operation carries SQL belonging to a different statement")
    void textIsNeverCarriedOver() {
        List<TableOperationInfo> ops = allOps(parse(SRC));
        assertFalse(ops.isEmpty(), "nothing parsed");

        int checked = 0;
        for (TableOperationInfo t : ops) {
            String sql = t.getSqlText();
            if (sql == null || t.getTableName() == null) continue;
            checked++;
            assertTrue(sql.toUpperCase(Locale.ROOT).contains(t.getTableName().toUpperCase(Locale.ROOT)),
                    "operation on " + t.getTableName() + " (line " + t.getLine()
                            + ") carries SQL that does not mention it: " + sql);
        }
        // Without this the test would pass vacuously the day text capture broke entirely, which is
        // the failure mode that let the original defect through.
        assertTrue(checked >= 4, "expected several operations to carry SQL, saw " + checked);
    }

    @Test
    @DisplayName("the statement each operation does carry is still the right one")
    void theRightTextIsStillAttached() {
        // The fix removes text; this is the half that proves it did not remove too much.
        List<TableOperationInfo> ops = allOps(parse(SRC));
        TableOperationInfo product = ops.stream()
                .filter(t -> "ERP_PRODUCT".equalsIgnoreCase(String.valueOf(t.getTableName())))
                .findFirst().orElse(null);
        assertNotNull(product, "ERP_PRODUCT operation not found at all");
        assertNotNull(product.getSqlText(), "ERP_PRODUCT lost its own statement text");
        assertTrue(product.getSqlText().toUpperCase(Locale.ROOT).contains("ROWNUM"),
                "expected ERP_PRODUCT's own SELECT, got: " + product.getSqlText());
    }
}
