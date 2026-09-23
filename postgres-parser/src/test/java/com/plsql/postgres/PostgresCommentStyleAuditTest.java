package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * READ-ONLY AUDIT (2026-09-17) of postgres-parser's tolerance to (A) WHERE-clause style
 * diversity and (B) comments interspersed around/inside PL/pgSQL CURSOR declarations, and
 * (C) fixtures combining both. Uses the REAL ANTLR parser via PostgresAnalysisEngine, exactly
 * like PostgresDmlCaptureGapAuditTest / PostgresCodingConventionAuditTest.
 *
 * Grammar note (verified against PostgresParser.g4 before writing these fixtures):
 *   - preBeginDeclareItem supports: `id CURSOR (WITH HOLD)? FOR selectStatement SEMI`
 *     (i.e. "cursor_name CURSOR FOR SELECT ...;", used inside the single DECLARE ... BEGIN
 *     block idiom).
 *   - declareCursorStmt supports: `DECLARE id CURSOR (WITH HOLD)? FOR selectStatement SEMI?`
 *     (same "name CURSOR FOR" word order, just with a per-statement DECLARE keyword).
 *   - There is NO grammar alternative for "CURSOR cursor_name FOR ..." (name AFTER the CURSOR
 *     keyword) anywhere in the file. That ordering is therefore expected to either fail to
 *     parse as a cursor at all, or produce parse errors — verified diagnostically below
 *     (dimension B0) rather than assumed.
 *
 * PostgresCursorInfo exposes: getName(), getSelectText(), isWithHold(), getLine(),
 * getOperations() — there is no getLineEnd()/getTables() on the cursor model itself, so
 * "captured tables" for a cursor's SELECT are cross-checked via PostgresRoutineInfo#getTableOps()
 * (table ops whose line falls within/near the cursor's declared line span).
 *
 * READ-ONLY: does not modify any file under src/main. Diagnostic in style — prints evidence,
 * only asserts where confident, does not hard-fail the whole suite on exploratory findings.
 */
class PostgresCommentStyleAuditTest {

    private final PostgresAnalysisEngine engine = new PostgresAnalysisEngine();

    private PostgresRoutineInfo dump(String label, String src) {
        PostgresRoutineInfo r = engine.parse(src, "probe", null);
        System.out.println("========== " + label + " ==========");
        System.out.println("  errors=" + r.getErrors());
        for (PostgresCursorInfo c : r.getCursors()) {
            System.out.println("  cursor: name=" + c.getName() + " line=" + c.getLine()
                    + " withHold=" + c.isWithHold());
            System.out.println("    selectText=[" + c.getSelectText() + "]");
            System.out.println("    selectText.length=" + (c.getSelectText() == null ? -1 : c.getSelectText().length()));
        }
        for (PostgresTableOpInfo t : r.getTableOps()) {
            System.out.println("  tableOp: " + t.getDmlType() + " table=" + t.qualifiedTable()
                    + " alias=" + t.getAlias() + " line=" + t.getLine());
        }
        return r;
    }

    // ─────────────────────────────────────────────────────────────────────
    // A. WHERE-clause style diversity
    // ─────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("A1: AND at line-start vs line-end, mixed within one WHERE")
    void whereAndLineStartVsLineEnd() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_a1_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  c1 CURSOR FOR\n" +
                "    SELECT o.id, o.status, o.customer_id\n" +
                "    FROM orders o\n" +
                "    WHERE o.status = 'OPEN' AND\n" +
                "          o.customer_id IS NOT NULL\n" +
                "      AND o.amount > 100\n" +
                "      OR o.priority = 'HIGH';\n" +
                "BEGIN\n" +
                "  NULL;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = dump("whereAndLineStartVsLineEnd", src);
        boolean ordersCaptured = r.getTableOps().stream().anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> orders captured despite mixed trailing/leading AND/OR: " + ordersCaptured + " (expect true)");
        boolean cursorPresent = r.getCursors().stream().anyMatch(c -> "c1".equalsIgnoreCase(c.getName()));
        System.out.println("  >>> cursor c1 present: " + cursorPresent);
    }

    @Test
    @DisplayName("A2: heavy nested parens in WHERE")
    void whereHeavyNestedParens() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_a2_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  c1 CURSOR FOR\n" +
                "    SELECT o.id\n" +
                "    FROM orders o\n" +
                "    WHERE ((o.status = 'OPEN' AND (o.amount > 50 OR o.amount < 5))\n" +
                "           OR ((o.priority = 'HIGH') AND (o.customer_id IS NOT NULL)));\n" +
                "BEGIN\n" +
                "  NULL;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = dump("whereHeavyNestedParens", src);
        boolean ordersCaptured = r.getTableOps().stream().anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> orders captured despite heavy nested parens: " + ordersCaptured + " (expect true)");
    }

    @Test
    @DisplayName("A3: mixed comparison styles — col=val, val=col, IS NULL / IS NOT NULL")
    void whereMixedComparisonStyles() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_a3_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  c1 CURSOR FOR\n" +
                "    SELECT c.id, c.name\n" +
                "    FROM customers c\n" +
                "    WHERE 'ACTIVE' = c.status\n" +
                "      AND c.deleted_at IS NULL\n" +
                "      AND c.region IS NOT NULL\n" +
                "      AND c.email = c.backup_email;\n" +
                "BEGIN\n" +
                "  NULL;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = dump("whereMixedComparisonStyles", src);
        boolean customersCaptured = r.getTableOps().stream().anyMatch(t -> "customers".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> customers captured with val=col + IS NULL/IS NOT NULL mix: " + customersCaptured + " (expect true)");
    }

    @Test
    @DisplayName("A4: multi-line IN list")
    void whereMultiLineInList() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_a4_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  c1 CURSOR FOR\n" +
                "    SELECT p.id, p.sku\n" +
                "    FROM products p\n" +
                "    WHERE p.category IN (\n" +
                "      'ELECTRONICS',\n" +
                "      'APPLIANCES',\n" +
                "      'FURNITURE'\n" +
                "    );\n" +
                "BEGIN\n" +
                "  NULL;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = dump("whereMultiLineInList", src);
        boolean productsCaptured = r.getTableOps().stream().anyMatch(t -> "products".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> products captured with multi-line IN list: " + productsCaptured + " (expect true)");
    }

    @Test
    @DisplayName("A5: BETWEEN x AND y must not be confused with WHERE ... AND ...")
    void whereBetweenNotConfusedWithAnd() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_a5_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  c1 CURSOR FOR\n" +
                "    SELECT s.id, s.status\n" +
                "    FROM shipments s\n" +
                "    WHERE s.ship_date BETWEEN '2026-01-01' AND '2026-06-30'\n" +
                "      AND s.carrier = 'FEDEX';\n" +
                "BEGIN\n" +
                "  NULL;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = dump("whereBetweenNotConfusedWithAnd", src);
        boolean shipmentsCaptured = r.getTableOps().stream().anyMatch(t -> "shipments".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> shipments captured with BETWEEN x AND y followed by real AND: " + shipmentsCaptured + " (expect true)");
        r.getCursors().forEach(c -> System.out.println("  >>> cursor selectText captures full WHERE incl. BETWEEN+AND: "
                + (c.getSelectText() != null && c.getSelectText().toUpperCase().contains("CARRIER"))));
    }

    @Test
    @DisplayName("A6: LIKE '%pattern%'")
    void whereLikePattern() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_a6_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  c1 CURSOR FOR\n" +
                "    SELECT c.id, c.name\n" +
                "    FROM customers c\n" +
                "    WHERE c.name LIKE '%smith%'\n" +
                "      AND c.email LIKE 'a%';\n" +
                "BEGIN\n" +
                "  NULL;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = dump("whereLikePattern", src);
        boolean customersCaptured = r.getTableOps().stream().anyMatch(t -> "customers".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> customers captured with LIKE '%pattern%': " + customersCaptured + " (expect true)");
    }

    @Test
    @DisplayName("A7: outer WHERE with a subquery that has its own differently-styled WHERE")
    void whereWithDifferentlyStyledSubqueryWhere() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_a7_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  c1 CURSOR FOR\n" +
                "    SELECT o.id\n" +
                "    FROM orders o\n" +
                "    WHERE o.status = 'OPEN'\n" +
                "      AND o.customer_id IN (\n" +
                "        SELECT c.id FROM customers c\n" +
                "        WHERE c.region = 'APAC' AND\n" +
                "              c.tier IN ('GOLD', 'PLATINUM')\n" +
                "      );\n" +
                "BEGIN\n" +
                "  NULL;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = dump("whereWithDifferentlyStyledSubqueryWhere", src);
        boolean ordersCaptured = r.getTableOps().stream().anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        boolean customersCaptured = r.getTableOps().stream().anyMatch(t -> "customers".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> outer orders captured: " + ordersCaptured + " (expect true)");
        System.out.println("  >>> subquery customers captured (nested paren + differently-styled AND): " + customersCaptured
                + " (expect true — GAP if false, since nested paren subquery table drop is a known risk area)");
    }

    // ─────────────────────────────────────────────────────────────────────
    // B0. Sanity: unsupported "CURSOR name FOR" (name AFTER keyword) ordering
    // ─────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("B0: word order CURSOR cursor_name FOR ... (Bug 3 fix — now supported)")
    void unsupportedCursorNameOrderIsDiagnostic() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_b0_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  CURSOR c1 FOR\n" +
                "    SELECT o.id FROM orders o;\n" +
                "BEGIN\n" +
                "  NULL;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = dump("unsupportedCursorNameOrderIsDiagnostic", src);
        org.junit.jupiter.api.Assertions.assertTrue(r.getErrors().isEmpty(),
                "Bug 3 fix: 'CURSOR name FOR ...' word order must parse without error, errors=" + r.getErrors());
        boolean cursorPresent = r.getCursors().stream().anyMatch(c -> "c1".equalsIgnoreCase(c.getName()));
        org.junit.jupiter.api.Assertions.assertTrue(cursorPresent,
                "Bug 3 fix: cursor c1 declared as 'CURSOR c1 FOR ...' must be captured");
        boolean ordersCaptured = r.getTableOps().stream().anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        org.junit.jupiter.api.Assertions.assertTrue(ordersCaptured,
                "Bug 3 fix: orders table from the cursor's SELECT must be captured");
    }

    // ─────────────────────────────────────────────────────────────────────
    // B. All 8 comment positions around a (name-first) CURSOR declaration
    // ─────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("B1: comment before the cursor declaration")
    void commentBeforeDeclaration() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_b1_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  -- fetch all open orders for the daily batch job\n" +
                "  c1 CURSOR FOR\n" +
                "    SELECT o.id, o.status FROM orders o WHERE o.status = 'OPEN';\n" +
                "BEGIN\n" +
                "  NULL;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = dump("commentBeforeDeclaration", src);
        boolean cursorOk = r.getCursors().stream().anyMatch(c -> "c1".equalsIgnoreCase(c.getName()));
        boolean ordersOk = r.getTableOps().stream().anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> cursor captured: " + cursorOk + " orders captured: " + ordersOk + " (expect both true)");
    }

    @Test
    @DisplayName("B2: comment between cursor_name and CURSOR keyword")
    void commentBetweenNameAndCursorKeyword() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_b2_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  c1 -- open orders cursor\n" +
                "     CURSOR FOR\n" +
                "    SELECT o.id, o.status FROM orders o WHERE o.status = 'OPEN';\n" +
                "BEGIN\n" +
                "  NULL;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = dump("commentBetweenNameAndCursorKeyword", src);
        boolean cursorOk = r.getCursors().stream().anyMatch(c -> "c1".equalsIgnoreCase(c.getName()));
        boolean ordersOk = r.getTableOps().stream().anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> cursor captured: " + cursorOk + " orders captured: " + ordersOk
                + " errors=" + r.getErrors() + " (comment sits between the cursor name and CURSOR keyword)");
    }

    @Test
    @DisplayName("B3: comment between CURSOR and FOR keywords")
    void commentBetweenCursorAndForKeywords() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_b3_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  c1 CURSOR -- no scroll needed, forward-only\n" +
                "       FOR\n" +
                "    SELECT o.id, o.status FROM orders o WHERE o.status = 'OPEN';\n" +
                "BEGIN\n" +
                "  NULL;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = dump("commentBetweenCursorAndForKeywords", src);
        boolean cursorOk = r.getCursors().stream().anyMatch(c -> "c1".equalsIgnoreCase(c.getName()));
        boolean ordersOk = r.getTableOps().stream().anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> cursor captured: " + cursorOk + " orders captured: " + ordersOk
                + " errors=" + r.getErrors() + " (comment sits between CURSOR and FOR)");
    }

    @Test
    @DisplayName("B4: comment between FOR and SELECT")
    void commentBetweenForAndSelect() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_b4_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  c1 CURSOR FOR\n" +
                "    -- the actual open-orders query\n" +
                "    SELECT o.id, o.status FROM orders o WHERE o.status = 'OPEN';\n" +
                "BEGIN\n" +
                "  NULL;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = dump("commentBetweenForAndSelect", src);
        boolean cursorOk = r.getCursors().stream().anyMatch(c -> "c1".equalsIgnoreCase(c.getName()));
        boolean ordersOk = r.getTableOps().stream().anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> cursor captured: " + cursorOk + " orders captured: " + ordersOk
                + " errors=" + r.getErrors() + " (comment sits between FOR and SELECT)");
    }

    @Test
    @DisplayName("B5: comment inside the select list")
    void commentInsideSelectList() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_b5_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  c1 CURSOR FOR\n" +
                "    SELECT o.id,\n" +
                "           -- customer's current status flag\n" +
                "           o.status,\n" +
                "           o.customer_id\n" +
                "    FROM orders o\n" +
                "    WHERE o.status = 'OPEN';\n" +
                "BEGIN\n" +
                "  NULL;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = dump("commentInsideSelectList", src);
        boolean cursorOk = r.getCursors().stream().anyMatch(c -> "c1".equalsIgnoreCase(c.getName()));
        boolean ordersOk = r.getTableOps().stream().anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> cursor captured: " + cursorOk + " orders captured: " + ordersOk
                + " errors=" + r.getErrors() + " (comment inside the select list, between columns)");
        r.getCursors().forEach(c -> System.out.println("  >>> selectText truncated at comment? full length="
                + (c.getSelectText() == null ? -1 : c.getSelectText().length())
                + " containsCustomerId=" + (c.getSelectText() != null && c.getSelectText().toLowerCase().contains("customer_id"))));
    }

    @Test
    @DisplayName("B6: comment between clauses (between FROM and WHERE)")
    void commentBetweenClauses() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_b6_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  c1 CURSOR FOR\n" +
                "    SELECT o.id, o.status\n" +
                "    FROM orders o\n" +
                "    -- only look at orders still open\n" +
                "    WHERE o.status = 'OPEN';\n" +
                "BEGIN\n" +
                "  NULL;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = dump("commentBetweenClauses", src);
        boolean cursorOk = r.getCursors().stream().anyMatch(c -> "c1".equalsIgnoreCase(c.getName()));
        boolean ordersOk = r.getTableOps().stream().anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> cursor captured: " + cursorOk + " orders captured: " + ordersOk
                + " errors=" + r.getErrors() + " (comment between FROM and WHERE clauses)");
        r.getCursors().forEach(c -> System.out.println("  >>> selectText contains WHERE after comment: "
                + (c.getSelectText() != null && c.getSelectText().toUpperCase().contains("STATUS = 'OPEN'"))));
    }

    @Test
    @DisplayName("B7: trailing comment after the terminating semicolon")
    void trailingCommentAfterSemicolon() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_b7_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  c1 CURSOR FOR\n" +
                "    SELECT o.id, o.status FROM orders o WHERE o.status = 'OPEN'; -- end of cursor decl\n" +
                "  v_cnt INT;\n" +
                "BEGIN\n" +
                "  v_cnt := 0;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = dump("trailingCommentAfterSemicolon", src);
        boolean cursorOk = r.getCursors().stream().anyMatch(c -> "c1".equalsIgnoreCase(c.getName()));
        boolean ordersOk = r.getTableOps().stream().anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        boolean vCntCaptured = r.getVariables().stream().anyMatch(v -> "v_cnt".equalsIgnoreCase(v.getName()));
        System.out.println("  >>> cursor captured: " + cursorOk + " orders captured: " + ordersOk
                + " next-var v_cnt captured (no drift past trailing comment): " + vCntCaptured
                + " errors=" + r.getErrors());
    }

    @Test
    @DisplayName("B8: multi-line block comment before the cursor causing possible line drift")
    void multiLineBlockCommentLineDrift() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_b8_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  /*\n" +
                "   * Cursor: c1\n" +
                "   * Purpose: iterate all open orders for daily reconciliation.\n" +
                "   * Owner: data-platform-team\n" +
                "   */\n" +
                "  c1 CURSOR FOR\n" +
                "    SELECT o.id, o.status FROM orders o WHERE o.status = 'OPEN';\n" +
                "BEGIN\n" +
                "  NULL;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = dump("multiLineBlockCommentLineDrift", src);
        int expectedLine = lineOf(src, "c1 CURSOR FOR");
        PostgresCursorInfo c1 = r.getCursors().stream().filter(c -> "c1".equalsIgnoreCase(c.getName())).findFirst().orElse(null);
        System.out.println("  >>> expected cursor decl line=" + expectedLine
                + " actual reported line=" + (c1 == null ? "NOT CAPTURED" : c1.getLine())
                + " (GAP if actual != expected — multi-line block comment before decl causing line drift)");
    }

    @Test
    @DisplayName("B9: mixed -- and /* */ comments within one cursor statement")
    void mixedLineAndBlockCommentsInOneStatement() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_b9_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  c1 CURSOR FOR -- forward-only cursor\n" +
                "    SELECT o.id, /* order id */ o.status, o.customer_id -- key cols\n" +
                "    FROM orders o /* base table */\n" +
                "    WHERE o.status = 'OPEN' -- only open orders\n" +
                "      AND o.customer_id IS NOT NULL; /* guard against orphaned rows */\n" +
                "BEGIN\n" +
                "  NULL;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = dump("mixedLineAndBlockCommentsInOneStatement", src);
        boolean cursorOk = r.getCursors().stream().anyMatch(c -> "c1".equalsIgnoreCase(c.getName()));
        boolean ordersOk = r.getTableOps().stream().anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> cursor captured: " + cursorOk + " orders captured: " + ordersOk
                + " errors=" + r.getErrors() + " (mixed -- and /* */ comments scattered throughout one statement)");
        r.getCursors().forEach(c -> System.out.println("  >>> selectText contains full WHERE (customer_id IS NOT NULL survives trailing comments): "
                + (c.getSelectText() != null && c.getSelectText().toLowerCase().contains("is not null"))));
    }

    // ─────────────────────────────────────────────────────────────────────
    // C. Combined: unusual WHERE style + interspersed comments in the same cursor
    // ─────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("C1: leading-AND/OR style + heavy nesting + comments interspersed")
    void combined_leadingAndOr_nestedParens_withComments() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_c1_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  -- daily reconciliation cursor\n" +
                "  c1 CURSOR FOR\n" +
                "    SELECT o.id, /* pk */ o.status\n" +
                "    FROM orders o\n" +
                "    WHERE ((o.status = 'OPEN' -- still open\n" +
                "            AND o.amount > 0)\n" +
                "           OR (o.priority = 'HIGH' /* escalated */\n" +
                "               AND o.customer_id IS NOT NULL)); -- final guard\n" +
                "BEGIN\n" +
                "  NULL;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = dump("combined_leadingAndOr_nestedParens_withComments", src);
        boolean cursorOk = r.getCursors().stream().anyMatch(c -> "c1".equalsIgnoreCase(c.getName()));
        boolean ordersOk = r.getTableOps().stream().anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> cursor captured: " + cursorOk + " orders captured: " + ordersOk + " errors=" + r.getErrors());
    }

    @Test
    @DisplayName("C2: BETWEEN + IN multi-line list + comments between clauses, subquery inside")
    void combined_betweenAndInList_withComments_andSubquery() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_c2_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  c1 CURSOR FOR\n" +
                "    SELECT s.id, s.status\n" +
                "    FROM shipments s\n" +
                "    -- restrict to the reconciliation window\n" +
                "    WHERE s.ship_date BETWEEN '2026-01-01' AND '2026-06-30' -- window\n" +
                "      AND s.carrier IN (\n" +
                "        'FEDEX', /* preferred */\n" +
                "        'UPS'\n" +
                "      )\n" +
                "      AND s.order_id IN (SELECT o.id FROM orders o WHERE o.status = 'OPEN'); -- linked orders\n" +
                "BEGIN\n" +
                "  NULL;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = dump("combined_betweenAndInList_withComments_andSubquery", src);
        boolean shipmentsOk = r.getTableOps().stream().anyMatch(t -> "shipments".equalsIgnoreCase(t.getTableName()));
        boolean ordersOk = r.getTableOps().stream().anyMatch(t -> "orders".equalsIgnoreCase(t.getTableName()));
        System.out.println("  >>> shipments captured: " + shipmentsOk + " (expect true)");
        System.out.println("  >>> subquery orders captured (BETWEEN+AND, multi-line IN, comments, nested subquery all combined): "
                + ordersOk + " (expect true — GAP if false)");
    }

    @Test
    @DisplayName("C3: mixed val=col comparisons, LIKE, trailing-OR style, and a doc block-comment before decl")
    void combined_valEqCol_like_trailingOr_blockComment() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_c3_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  /* customer segmentation cursor\n" +
                "     used by the nightly marketing job */\n" +
                "  c1 CURSOR FOR\n" +
                "    SELECT c.id, c.name\n" +
                "    FROM customers c\n" +
                "    WHERE 'GOLD' = c.tier OR\n" +
                "          c.name LIKE '%corp%' OR -- corporate accounts\n" +
                "          c.deleted_at IS NULL;\n" +
                "BEGIN\n" +
                "  NULL;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = dump("combined_valEqCol_like_trailingOr_blockComment", src);
        boolean customersOk = r.getTableOps().stream().anyMatch(t -> "customers".equalsIgnoreCase(t.getTableName()));
        int expectedLine = lineOf(src, "c1 CURSOR FOR");
        PostgresCursorInfo c1 = r.getCursors().stream().filter(c -> "c1".equalsIgnoreCase(c.getName())).findFirst().orElse(null);
        System.out.println("  >>> customers captured: " + customersOk + " (expect true)");
        System.out.println("  >>> expected decl line=" + expectedLine + " actual=" + (c1 == null ? "NOT CAPTURED" : c1.getLine()));
    }

    // ─────────────────────────────────────────────────────────────────────
    // Bug 6: cursor selectText must preserve original whitespace, not be a run-on string.
    // ─────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Bug 6: cursor selectText preserves whitespace (not ctx.getText()'s run-on string)")
    void cursorSelectTextPreservesWhitespace() {
        String src =
                "CREATE OR REPLACE FUNCTION fn_bug6_probe()\n" +
                "RETURNS VOID\n" +
                "LANGUAGE plpgsql AS $$\n" +
                "DECLARE\n" +
                "  c1 CURSOR FOR\n" +
                "    SELECT o.id, o.status\n" +
                "    FROM orders o\n" +
                "    WHERE o.status = 'OPEN';\n" +
                "BEGIN\n" +
                "  NULL;\n" +
                "END;\n" +
                "$$;\n";
        PostgresRoutineInfo r = dump("cursorSelectTextPreservesWhitespace", src);
        PostgresCursorInfo c1 = r.getCursors().stream().filter(c -> "c1".equalsIgnoreCase(c.getName())).findFirst().orElse(null);
        org.junit.jupiter.api.Assertions.assertNotNull(c1, "cursor c1 must be captured");
        String txt = c1.getSelectText();
        org.junit.jupiter.api.Assertions.assertNotNull(txt);
        org.junit.jupiter.api.Assertions.assertTrue(txt.contains(" "),
                "Bug 6 fix: selectText must preserve original whitespace, got: [" + txt + "]");
        org.junit.jupiter.api.Assertions.assertTrue(txt.toUpperCase().contains("ORDERS"));
    }

    private int lineOf(String text, String needle) {
        int idx = text.indexOf(needle);
        if (idx < 0) return -1;
        int line = 1;
        for (int i = 0; i < idx; i++) if (text.charAt(i) == '\n') line++;
        return line;
    }
}
