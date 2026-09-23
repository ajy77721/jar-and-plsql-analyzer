package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.SqlServerCursorInfo;
import com.plsql.sqlserver.model.SqlServerRoutineInfo;
import com.plsql.sqlserver.model.SqlServerTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AUDIT ONLY — investigates whether SqlServerAnalysisEngine/SqlServerAnalysisVisitor correctly
 * capture WHERE-clause style diversity and comments placed at various positions around T-SQL
 * cursor declarations (DECLARE cursor_name CURSOR FOR SELECT ...).
 *
 * Does NOT touch main/ code. Prints dump() evidence for each pattern and documents actual
 * behavior (including gaps) via assertions/println, diagnostic style — only asserts where
 * confident, otherwise records evidence for a human to review.
 *
 * Key structural fact discovered while writing this audit (see
 * SqlServerAnalysisVisitor#addTableOpsFromText / #attachJoins / #TABLE_REF_PATTERN /
 * #JOIN_PATTERN): table/join extraction runs directly against the raw {@code originalText()}
 * of a statement, WITHOUT first stripping comments (unlike #extractQualifiedCalls, which calls
 * #stripLineAndBlockComments before scanning for outbound calls). A cursor's raw query text
 * (SqlServerCursorInfo#getQuery()) is likewise the verbatim originalText() of ctx.stmtTail(),
 * comments included. This is exactly the mechanism this audit probes.
 */
class SqlServerCommentStyleAuditTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private SqlServerRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    private void dump(String label, SqlServerRoutineInfo r) {
        System.out.println("=== " + label + " ===");
        System.out.println("errors: " + r.getErrors());
        System.out.println("tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.qualifiedTable() + "@" + t.getLine()
                        + " joins=" + t.getJoins().stream().map(j -> j.getJoinType() + " " + j.getJoinedTable()).toList())
                .toList());
        for (SqlServerCursorInfo c : r.getCursors()) {
            System.out.println("cursor name=" + c.getName() + " line=" + c.getLine());
            System.out.println("  rawQuery=[" + c.getQuery() + "]");
            System.out.println("  stmtOps=" + c.getStmtOperations().stream()
                    .map(o -> o.getOperation() + "@" + o.getLine()).toList());
        }
        System.out.println("lineStart=" + r.getLineStart() + " lineEnd=" + r.getLineEnd());
    }

    private boolean tableAnywhere(SqlServerRoutineInfo r, String name) {
        return r.getTableOps().stream().anyMatch(t -> name.equalsIgnoreCase(t.getTableName()))
                || r.getTableOps().stream().flatMap(t -> t.getJoins().stream())
                        .anyMatch(j -> name.equalsIgnoreCase(j.getJoinedTable()));
    }

    // ═════════════════════════════════════════════════════════════════════════════════════════
    // A. WHERE-clause style diversity
    // ═════════════════════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("A1: AND/OR at line-start vs line-end style")
    void a1_andOr_lineStartVsLineEnd() {
        String src =
                "CREATE PROCEDURE usp_where_andor_style\n" +
                "AS\nBEGIN\n" +
                "    SELECT o.OrderId, o.Status\n" +
                "    FROM Orders o\n" +
                "    WHERE o.Status = 'OPEN' AND\n" +      // AND at line-end
                "          o.Amount > 100\n" +
                "          OR o.Priority = 'HIGH'\n" +      // OR at line-start
                "    AND o.Region = 'EAST';\n" +            // AND at line-start
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("a1_andOr_lineStartVsLineEnd", r);

        boolean orders = tableAnywhere(r, "Orders");
        System.out.println("a1: Orders captured=" + orders + " (WHERE AND/OR line-position style must not affect FROM-clause table capture)");
        assertTrue(orders, "expected Orders captured regardless of AND/OR line-position style in WHERE");
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
    }

    @Test
    @DisplayName("A2: heavy nested parens in WHERE")
    void a2_heavyNestedParens() {
        String src =
                "CREATE PROCEDURE usp_where_nested_parens\n" +
                "AS\nBEGIN\n" +
                "    SELECT o.OrderId\n" +
                "    FROM Orders o\n" +
                "    WHERE ((o.Status = 'OPEN' AND (o.Amount > 100 OR o.Amount < 0))\n" +
                "           OR ((o.Priority = 'HIGH') AND (o.Region = 'EAST' OR o.Region = 'WEST')))\n" +
                "    AND NOT (o.Cancelled = 1);\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("a2_heavyNestedParens", r);

        boolean orders = tableAnywhere(r, "Orders");
        System.out.println("a2: Orders captured=" + orders);
        assertTrue(orders, "expected Orders captured despite heavily nested WHERE parens");
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
    }

    @Test
    @DisplayName("A3: mixed comparison styles + BETWEEN-AND not confused with WHERE-AND + LIKE")
    void a3_mixedComparisonStyles_betweenAnd_like() {
        String src =
                "CREATE PROCEDURE usp_where_mixed_comparisons\n" +
                "AS\nBEGIN\n" +
                "    SELECT o.OrderId\n" +
                "    FROM Orders o\n" +
                "    WHERE 'OPEN' = o.Status\n" +                 // val = col (reversed)
                "      AND o.ShipDate IS NOT NULL\n" +
                "      AND o.CancelDate IS NULL\n" +
                "      AND o.Amount BETWEEN 100 AND 500\n" +      // BETWEEN...AND, not a WHERE-AND
                "      AND o.CustomerId IN (\n" +
                "          1, 2, 3,\n" +
                "          4, 5\n" +                              // multi-line IN list
                "      )\n" +
                "      AND o.Notes LIKE '%urgent%';\n" +          // LIKE wildcard
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("a3_mixedComparisonStyles_betweenAnd_like", r);

        boolean orders = tableAnywhere(r, "Orders");
        System.out.println("a3: Orders captured=" + orders
                + " (checks BETWEEN x AND y is not mistaken for a second WHERE-level AND that would "
                + "split/duplicate the FROM-clause scan, and that multi-line IN() + LIKE '%...%' don't break table capture)");
        assertTrue(orders, "expected Orders captured with BETWEEN/IN/LIKE mixed WHERE styles");
        long ordersCount = r.getTableOps().stream().filter(t -> "Orders".equalsIgnoreCase(t.getTableName())).count();
        System.out.println("a3: Orders table-op entry count=" + ordersCount + " (expect exactly 1, not duplicated by BETWEEN...AND)");
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
    }

    @Test
    @DisplayName("A4: WHERE with subquery having its own differently-styled WHERE")
    void a4_whereWithDifferentlyStyledSubqueryWhere() {
        String src =
                "CREATE PROCEDURE usp_where_subquery_diff_style\n" +
                "AS\nBEGIN\n" +
                "    SELECT o.OrderId\n" +
                "    FROM Orders o\n" +
                "    WHERE o.CustomerId IN (\n" +
                "        SELECT c.CustomerId\n" +
                "        FROM Customers c\n" +
                "        WHERE (c.Active = 1)\n" +
                "        AND (c.Region = 'EAST' OR c.Region = 'WEST')\n" +
                "        OR c.VipFlag = 1\n" +
                "    )\n" +
                "    AND o.Amount > 0;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("a4_whereWithDifferentlyStyledSubqueryWhere", r);

        boolean orders = tableAnywhere(r, "Orders");
        boolean customers = tableAnywhere(r, "Customers");
        System.out.println("a4: Orders(outer)=" + orders + " Customers(subquery, own AND/OR style)=" + customers);
        assertTrue(orders, "expected outer Orders captured");
        assertTrue(customers, "expected Customers captured from WHERE-IN subquery with its own differently-styled WHERE");
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
    }

    // ═════════════════════════════════════════════════════════════════════════════════════════
    // B. All 8 comment positions around CURSOR declarations
    // ═════════════════════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("B1: comment before DECLARE")
    void b1_commentBeforeDeclare() {
        String src =
                "CREATE PROCEDURE usp_cur_b1\n" +
                "AS\nBEGIN\n" +
                "    -- open the order cursor\n" +
                "    DECLARE cur_orders CURSOR FOR\n" +
                "        SELECT o.OrderId FROM Orders o WHERE o.Status = 'OPEN';\n" +
                "    OPEN cur_orders;\n" +
                "    FETCH NEXT FROM cur_orders INTO @dummy;\n" +
                "    CLOSE cur_orders;\n" +
                "    DEALLOCATE cur_orders;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("b1_commentBeforeDeclare", r);

        SqlServerCursorInfo cur = r.getCursors().stream().findFirst().orElse(null);
        assertNotNull(cur, "expected cursor captured with a leading line-comment before DECLARE");
        assertEquals("cur_orders", cur.getName());
        boolean orders = tableAnywhere(r, "Orders");
        System.out.println("b1: cursor query=[" + cur.getQuery() + "] Orders captured=" + orders);
        // Bug 7 fix: SqlServerCursorInfo#getQuery() used to be the literal string "CURSOR FOR"
        // because the grammar's stmtTail() stops right before the SELECT keyword (the SELECT is
        // parsed as a separate sibling statement). visitDeclareStatement now looks up that
        // sibling SelectStatement and appends its own original text, so getQuery() returns the
        // full "CURSOR FOR SELECT ..." text.
        assertTrue(cur.getQuery() != null && cur.getQuery().toUpperCase().contains("SELECT"),
                "Bug 7 fix: getQuery() must include the SELECT body, actual=[" + cur.getQuery() + "]");
        assertTrue(cur.getQuery().toUpperCase().contains("ORDERS"),
                "getQuery() must include the cursor's FROM table, actual=[" + cur.getQuery() + "]");
        assertTrue(orders, "expected Orders captured from cursor query when comment precedes DECLARE");
    }

    @Test
    @DisplayName("B2: comment between cursor_name and CURSOR keyword")
    void b2_commentBetweenNameAndCursorKeyword() {
        String src =
                "CREATE PROCEDURE usp_cur_b2\n" +
                "AS\nBEGIN\n" +
                "    DECLARE cur_orders /* the order cursor */ CURSOR FOR\n" +
                "        SELECT o.OrderId FROM Orders o WHERE o.Status = 'OPEN';\n" +
                "    OPEN cur_orders;\n" +
                "    CLOSE cur_orders;\n" +
                "    DEALLOCATE cur_orders;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("b2_commentBetweenNameAndCursorKeyword", r);

        SqlServerCursorInfo cur = r.getCursors().stream().findFirst().orElse(null);
        System.out.println("b2: cursor captured=" + (cur != null) + " errors=" + r.getErrors());
        assertNotNull(cur, "expected cursor captured despite block comment between name and CURSOR keyword");
        boolean orders = tableAnywhere(r, "Orders");
        System.out.println("b2: cursor query=[" + (cur != null ? cur.getQuery() : null) + "] Orders captured=" + orders);
        assertTrue(orders, "expected Orders still captured");
    }

    @Test
    @DisplayName("B3: comment between CURSOR and FOR")
    void b3_commentBetweenCursorAndFor() {
        String src =
                "CREATE PROCEDURE usp_cur_b3\n" +
                "AS\nBEGIN\n" +
                "    DECLARE cur_orders CURSOR -- LOCAL FAST_FORWARD would go here\n" +
                "    FOR\n" +
                "        SELECT o.OrderId FROM Orders o WHERE o.Status = 'OPEN';\n" +
                "    OPEN cur_orders;\n" +
                "    CLOSE cur_orders;\n" +
                "    DEALLOCATE cur_orders;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("b3_commentBetweenCursorAndFor", r);

        SqlServerCursorInfo cur = r.getCursors().stream().findFirst().orElse(null);
        System.out.println("b3: cursor captured=" + (cur != null) + " errors=" + r.getErrors());
        assertNotNull(cur, "expected cursor captured despite line comment between CURSOR and FOR");
        boolean orders = tableAnywhere(r, "Orders");
        System.out.println("b3: cursor query=[" + (cur != null ? cur.getQuery() : null) + "] Orders captured=" + orders);
        assertTrue(orders, "expected Orders still captured");
    }

    @Test
    @DisplayName("B4: comment between FOR and SELECT")
    void b4_commentBetweenForAndSelect() {
        String src =
                "CREATE PROCEDURE usp_cur_b4\n" +
                "AS\nBEGIN\n" +
                "    DECLARE cur_orders CURSOR FOR\n" +
                "        -- main query for the cursor\n" +
                "        SELECT o.OrderId FROM Orders o WHERE o.Status = 'OPEN';\n" +
                "    OPEN cur_orders;\n" +
                "    CLOSE cur_orders;\n" +
                "    DEALLOCATE cur_orders;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("b4_commentBetweenForAndSelect", r);

        SqlServerCursorInfo cur = r.getCursors().stream().findFirst().orElse(null);
        assertNotNull(cur, "expected cursor captured despite comment between FOR and SELECT");
        boolean orders = tableAnywhere(r, "Orders");
        System.out.println("b4: cursor query=[" + cur.getQuery() + "] Orders captured=" + orders
                + " — check for comment-caused truncation of captured SQL");
        assertTrue(orders, "expected Orders still captured with comment between FOR and SELECT");
    }

    @Test
    @DisplayName("B5: comment inside the select list")
    void b5_commentInsideSelectList() {
        String src =
                "CREATE PROCEDURE usp_cur_b5\n" +
                "AS\nBEGIN\n" +
                "    DECLARE cur_orders CURSOR FOR\n" +
                "        SELECT o.OrderId, -- the primary key\n" +
                "               o.Status\n" +
                "        FROM Orders o\n" +
                "        WHERE o.Status = 'OPEN';\n" +
                "    OPEN cur_orders;\n" +
                "    CLOSE cur_orders;\n" +
                "    DEALLOCATE cur_orders;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("b5_commentInsideSelectList", r);

        SqlServerCursorInfo cur = r.getCursors().stream().findFirst().orElse(null);
        assertNotNull(cur, "expected cursor captured despite comment inside select list");
        boolean orders = tableAnywhere(r, "Orders");
        System.out.println("b5: cursor query=[" + cur.getQuery() + "] Orders captured=" + orders);
        assertTrue(orders, "expected Orders still captured with comment inside the select list");
    }

    @Test
    @DisplayName("B6: comment between clauses (FROM/WHERE boundary)")
    void b6_commentBetweenClauses() {
        String src =
                "CREATE PROCEDURE usp_cur_b6\n" +
                "AS\nBEGIN\n" +
                "    DECLARE cur_orders CURSOR FOR\n" +
                "        SELECT o.OrderId\n" +
                "        FROM Orders o\n" +
                "        -- only open orders are relevant here\n" +
                "        WHERE o.Status = 'OPEN';\n" +
                "    OPEN cur_orders;\n" +
                "    CLOSE cur_orders;\n" +
                "    DEALLOCATE cur_orders;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("b6_commentBetweenClauses", r);

        SqlServerCursorInfo cur = r.getCursors().stream().findFirst().orElse(null);
        assertNotNull(cur, "expected cursor captured despite comment between FROM and WHERE clauses");
        boolean orders = tableAnywhere(r, "Orders");
        System.out.println("b6: cursor query=[" + cur.getQuery() + "] Orders captured=" + orders);
        assertTrue(orders, "expected Orders still captured with comment between FROM and WHERE");
    }

    @Test
    @DisplayName("B7: trailing comment after terminating semicolon")
    void b7_trailingCommentAfterTerminator() {
        String src =
                "CREATE PROCEDURE usp_cur_b7\n" +
                "AS\nBEGIN\n" +
                "    DECLARE cur_orders CURSOR FOR\n" +
                "        SELECT o.OrderId FROM Orders o WHERE o.Status = 'OPEN'; -- end of cursor decl\n" +
                "    OPEN cur_orders;\n" +
                "    CLOSE cur_orders;\n" +
                "    DEALLOCATE cur_orders;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("b7_trailingCommentAfterTerminator", r);

        SqlServerCursorInfo cur = r.getCursors().stream().findFirst().orElse(null);
        assertNotNull(cur, "expected cursor captured with a trailing comment after the terminating semicolon");
        boolean orders = tableAnywhere(r, "Orders");
        System.out.println("b7: cursor query=[" + cur.getQuery() + "] Orders captured=" + orders
                + " — check whether trailing comment leaked into raw captured query text");
        assertTrue(orders, "expected Orders still captured with trailing comment after ;");
    }

    @Test
    @DisplayName("B8: multi-line block comment variant + line-number drift check")
    void b8_multiLineBlockComment_lineDrift() {
        String src =
                "CREATE PROCEDURE usp_cur_b8\n" +
                "AS\nBEGIN\n" +
                "    DECLARE cur_orders CURSOR FOR\n" +
                "        /*\n" +
                "         * Multi-line block comment\n" +
                "         * explaining the cursor query\n" +
                "         * across several lines\n" +
                "         */\n" +
                "        SELECT o.OrderId\n" +                 // line 9
                "        FROM Orders o\n" +
                "        WHERE o.Status = 'OPEN';\n" +
                "    OPEN cur_orders;\n" +
                "    FETCH NEXT FROM cur_orders INTO @dummy;\n" + // line 13
                "    CLOSE cur_orders;\n" +
                "    DEALLOCATE cur_orders;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("b8_multiLineBlockComment_lineDrift", r);

        SqlServerCursorInfo cur = r.getCursors().stream().findFirst().orElse(null);
        assertNotNull(cur, "expected cursor captured despite multi-line block comment before SELECT");
        boolean orders = tableAnywhere(r, "Orders");
        System.out.println("b8: cursor declared line=" + cur.getLine() + " (expect 4) query=[" + cur.getQuery() + "]");
        System.out.println("b8: Orders captured=" + orders);
        SqlServerTableOpInfo ordersOp = r.getTableOps().stream()
                .filter(t -> "Orders".equalsIgnoreCase(t.getTableName())).findFirst().orElse(null);
        System.out.println("b8: Orders table-op line=" + (ordersOp != null ? ordersOp.getLine() : "N/A")
                + " (SELECT o.OrderId is on source line 9, but the intervening multi-line block comment spans "
                + "lines 4-8; if the table-op line comes back as 9 that confirms lineAt() correctly counts real "
                + "newlines inside the comment rather than drifting)");
        cur.getStmtOperations().forEach(op ->
                System.out.println("b8: lifecycle op=" + op.getOperation() + "@" + op.getLine()));
        assertEquals(4, cur.getLine(), "cursor DECLARE line must not drift");
        assertTrue(orders, "expected Orders still captured across a multi-line block comment");
    }

    @Test
    @DisplayName("B9: mixed -- and /* */ comments within one cursor statement")
    void b9_mixedCommentStyles_oneStatement() {
        String src =
                "CREATE PROCEDURE usp_cur_b9\n" +
                "AS\nBEGIN\n" +
                "    -- declare the working cursor\n" +
                "    DECLARE cur_orders /* local */ CURSOR FOR -- forward only\n" +
                "        SELECT /* pk */ o.OrderId, o.Status /* status col */\n" +
                "        FROM Orders o -- base table\n" +
                "        /* filter to open orders only */\n" +
                "        WHERE o.Status = 'OPEN'; -- done\n" +
                "    OPEN cur_orders;\n" +
                "    CLOSE cur_orders;\n" +
                "    DEALLOCATE cur_orders;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("b9_mixedCommentStyles_oneStatement", r);

        SqlServerCursorInfo cur = r.getCursors().stream().findFirst().orElse(null);
        System.out.println("b9: cursor captured=" + (cur != null) + " errors=" + r.getErrors());
        assertNotNull(cur, "expected cursor captured despite mixed -- and /* */ comments in one statement");
        boolean orders = tableAnywhere(r, "Orders");
        System.out.println("b9: cursor query=[" + (cur != null ? cur.getQuery() : null) + "] Orders captured=" + orders);
        assertTrue(orders, "expected Orders still captured despite heavily mixed comment styles");
    }

    // ═════════════════════════════════════════════════════════════════════════════════════════
    // C. Unusual WHERE style + interspersed comments combined, same cursor
    // ═════════════════════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("C1: line-end AND + trailing comments per line + nested parens, inside cursor")
    void c1_combined_lineEndAnd_trailingComments_nestedParens() {
        String src =
                "CREATE PROCEDURE usp_cur_c1\n" +
                "AS\nBEGIN\n" +
                "    DECLARE cur_orders CURSOR FOR\n" +
                "        SELECT o.OrderId\n" +
                "        FROM Orders o -- base orders table\n" +
                "        WHERE (o.Status = 'OPEN' AND -- must be open\n" +
                "               (o.Amount > 100 OR o.Priority = 'HIGH')) -- amount or priority gate\n" +
                "        AND o.CustomerId IN ( -- eligible customers\n" +
                "            SELECT c.CustomerId FROM Customers c WHERE c.Active = 1 -- nested WHERE, own style\n" +
                "        );\n" +
                "    OPEN cur_orders;\n" +
                "    CLOSE cur_orders;\n" +
                "    DEALLOCATE cur_orders;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("c1_combined_lineEndAnd_trailingComments_nestedParens", r);

        SqlServerCursorInfo cur = r.getCursors().stream().findFirst().orElse(null);
        assertNotNull(cur, "expected cursor captured");
        boolean orders = tableAnywhere(r, "Orders");
        boolean customers = tableAnywhere(r, "Customers");
        System.out.println("c1: Orders(outer)=" + orders + " Customers(nested WHERE-IN subquery, trailing comments throughout)=" + customers);
        System.out.println("c1: query=[" + cur.getQuery() + "]");
        assertTrue(orders, "expected Orders captured through trailing comments + nested parens + line-end AND");
        // Diagnostic only — does the WHERE-IN subquery table survive trailing comments on every line?
        System.out.println("c1: GAP CANDIDATE if Customers=false — subquery table dropped when every clause line "
                + "carries a trailing -- comment inside a cursor's raw (uncommented-stripped) query text");
    }

    @Test
    @DisplayName("C2: block comments between every clause + BETWEEN/IN mixed styles, inside cursor")
    void c2_combined_blockCommentsEveryClause_betweenIn() {
        String src =
                "CREATE PROCEDURE usp_cur_c2\n" +
                "AS\nBEGIN\n" +
                "    DECLARE cur_orders CURSOR FOR\n" +
                "        SELECT o.OrderId\n" +
                "        /* from clause */\n" +
                "        FROM Orders o\n" +
                "        /* where clause starts */\n" +
                "        WHERE o.Amount BETWEEN 100 AND 500\n" +
                "        /* and customer filter */\n" +
                "        AND o.CustomerId IN (1, 2, 3)\n" +
                "        /* and region filter, OR style */\n" +
                "        AND (o.Region = 'EAST' OR o.Region = 'WEST');\n" +
                "    OPEN cur_orders;\n" +
                "    CLOSE cur_orders;\n" +
                "    DEALLOCATE cur_orders;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("c2_combined_blockCommentsEveryClause_betweenIn", r);

        SqlServerCursorInfo cur = r.getCursors().stream().findFirst().orElse(null);
        assertNotNull(cur, "expected cursor captured");
        boolean orders = tableAnywhere(r, "Orders");
        System.out.println("c2: Orders=" + orders + " query=[" + cur.getQuery() + "]");
        // GAP CANDIDATE: FROM_PATTERN/TABLE_REF_PATTERN require `\bFROM\s+<name>`. A block comment
        // immediately after FROM's clause label (here it precedes FROM, not between FROM and the
        // table name, so this case is expected to pass) — kept as a control case to contrast with
        // B2/B3/B4 where the comment sits INSIDE the FROM/CURSOR/SELECT boundary itself.
        assertTrue(orders, "expected Orders captured with block comments between every clause");
    }

    @Test
    @DisplayName("C3: comment directly between FROM and table name (tight boundary) — direct regex-break probe")
    void c3_commentDirectlyBetweenFromAndTableName() {
        String src =
                "CREATE PROCEDURE usp_cur_c3\n" +
                "AS\nBEGIN\n" +
                "    DECLARE cur_orders CURSOR FOR\n" +
                "        SELECT o.OrderId\n" +
                "        FROM -- the orders table\n" +
                "            Orders o\n" +
                "        WHERE o.Status = 'OPEN';\n" +
                "    OPEN cur_orders;\n" +
                "    CLOSE cur_orders;\n" +
                "    DEALLOCATE cur_orders;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("c3_commentDirectlyBetweenFromAndTableName", r);

        SqlServerCursorInfo cur = r.getCursors().stream().findFirst().orElse(null);
        assertNotNull(cur, "expected cursor captured");
        boolean orders = tableAnywhere(r, "Orders");
        System.out.println("c3: Orders captured=" + orders + " query=[" + cur.getQuery() + "]");
        assertTrue(orders, "Bug 2 fix: Orders must be captured even when a -- comment appears "
                + "directly between FROM and the table name");
    }
}
