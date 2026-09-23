package com.plsql.db2;

import com.plsql.db2.engine.Db2AnalysisEngine;
import com.plsql.db2.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AUDIT ONLY (read-only investigation, 2026-09-17) — printed evidence for comment placement
 * around {@code DECLARE ... CURSOR FOR SELECT ...} and WHERE-clause style diversity, mirroring
 * the existing pattern in {@code Db2DmlCaptureGapAuditTest} / {@code Db2CodingConventionAuditTest}.
 * Each routine below is a single, self-contained CREATE PROCEDURE body (the grammar's
 * {@code compilationUnit} accepts exactly one routine per {@code engine.parse} call — see the
 * class comment on {@code Db2CodingConventionAuditTest}).
 *
 * <p>Findings are printed and reported back to the caller, not asserted as pass/fail gates,
 * except where a real capture break is being pinned down with high confidence.
 */
class Db2CommentStyleAuditTest {

    private final Db2AnalysisEngine engine = new Db2AnalysisEngine();

    private static void dump(String label, Db2RoutineInfo r) {
        System.out.println("=== " + label + " errors === " + r.getErrors());
        System.out.println("--- cursors ---");
        for (Db2CursorInfo c : r.getCursors()) {
            System.out.println("  name=" + c.getName() + " line=" + c.getLine()
                    + " withHold=" + c.isWithHold());
            System.out.println("  selectText=[" + c.getSelectText() + "]");
        }
        System.out.println("--- tableOps ---");
        for (Db2TableOpInfo t : r.getTableOps()) {
            System.out.println("  " + t.getDmlType() + " " + t.qualifiedTable()
                    + " alias=" + t.getAlias() + " line=" + t.getLine()
                    + " sql=[" + t.getSqlText() + "]");
            for (Db2JoinInfo j : t.getJoins()) {
                System.out.println("      JOIN " + j.getJoinType() + " " + j.getJoinedTable()
                        + " alias=" + j.getJoinedTableAlias() + " on=" + j.getCondition());
            }
        }
        System.out.println("--- statements ---");
        for (Db2StatementInfo s : r.getStatements()) {
            System.out.println("  " + s.getType() + " line=" + s.getLine());
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    // A. WHERE-clause style diversity (no comments yet — isolate the AND/OR/
    //    paren/comparison dimension before mixing in comment placement).
    // ═════════════════════════════════════════════════════════════════════════

    // A1: AND at line-start (leading-AND style) vs AND at line-end (trailing-AND style).
    private static final String WHERE_AND_STYLE_SRC =
        "CREATE PROCEDURE p_where_and_style(IN p_region CHAR(2), IN p_min INTEGER)\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE cur_lead CURSOR FOR\n"
      + "    SELECT o.order_id, o.status\n"
      + "    FROM orders o\n"
      + "    WHERE o.region = p_region\n"
      + "      AND o.total >= p_min\n"
      + "      AND o.status <> 'CANCELLED';\n"
      + "  DECLARE cur_trail CURSOR FOR\n"
      + "    SELECT o.order_id, o.status\n"
      + "    FROM orders o WHERE o.region = p_region AND\n"
      + "    o.total >= p_min AND\n"
      + "    o.status <> 'CANCELLED';\n"
      + "END";

    @Test
    @DisplayName("A1: leading-AND vs trailing-AND style across two cursors — full WHERE captured either way?")
    void whereAndStyleLeadingVsTrailing() {
        Db2RoutineInfo r = engine.parse(WHERE_AND_STYLE_SRC, "p_where_and_style", null);
        dump("A1", r);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        Db2CursorInfo lead = findCursor(r, "cur_lead");
        Db2CursorInfo trail = findCursor(r, "cur_trail");
        assertNotNull(lead);
        assertNotNull(trail);
        boolean leadHasAllPredicates = lead.getSelectText() != null
                && lead.getSelectText().toUpperCase().contains("CANCELLED");
        boolean trailHasAllPredicates = trail.getSelectText() != null
                && trail.getSelectText().toUpperCase().contains("CANCELLED");
        System.out.println(">>> leadHasAllPredicates=" + leadHasAllPredicates
                + " trailHasAllPredicates=" + trailHasAllPredicates);
    }

    // A2: heavy nested parens grouping OR/AND.
    private static final String WHERE_NESTED_PARENS_SRC =
        "CREATE PROCEDURE p_where_nested_parens(IN p_region CHAR(2))\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE cur1 CURSOR FOR\n"
      + "    SELECT o.order_id\n"
      + "    FROM orders o\n"
      + "    WHERE ((o.region = p_region AND (o.status = 'OPEN' OR o.status = 'PENDING'))\n"
      + "           OR (o.priority = 1 AND (o.total > 100 OR o.rush_flag = 1)));\n"
      + "END";

    @Test
    @DisplayName("A2: heavy nested parens (OR/AND grouping) inside WHERE — captured intact?")
    void whereHeavyNestedParens() {
        Db2RoutineInfo r = engine.parse(WHERE_NESTED_PARENS_SRC, "p_where_nested_parens", null);
        dump("A2", r);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        Db2CursorInfo cur1 = findCursor(r, "cur1");
        assertNotNull(cur1);
        boolean rushFlagPresent = cur1.getSelectText() != null
                && cur1.getSelectText().toUpperCase().contains("RUSH_FLAG");
        System.out.println(">>> rushFlagPresent(end of deeply nested predicate)=" + rushFlagPresent);
    }

    // A3: mixed comparison styles — col=val, val=col, IS NULL/IS NOT NULL, IN multi-line list,
    //     BETWEEN x AND y (must not be confused with a WHERE-level AND), LIKE '%pattern%'.
    private static final String WHERE_MIXED_COMPARISONS_SRC =
        "CREATE PROCEDURE p_where_mixed_comparisons(IN p_region CHAR(2))\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE cur1 CURSOR FOR\n"
      + "    SELECT o.order_id, o.customer_name\n"
      + "    FROM orders o\n"
      + "    WHERE p_region = o.region\n"
      + "      AND o.cancelled_date IS NULL\n"
      + "      AND o.customer_name IS NOT NULL\n"
      + "      AND o.status IN (\n"
      + "            'OPEN',\n"
      + "            'PENDING',\n"
      + "            'BACKORDERED'\n"
      + "          )\n"
      + "      AND o.order_date BETWEEN DATE('2026-01-01') AND DATE('2026-12-31')\n"
      + "      AND o.customer_name LIKE '%CORP%';\n"
      + "END";

    @Test
    @DisplayName("A3: col=val/val=col, IS NULL/IS NOT NULL, multi-line IN list, BETWEEN x AND y, LIKE '%p%' — no BETWEEN/AND confusion")
    void whereMixedComparisonStyles() {
        Db2RoutineInfo r = engine.parse(WHERE_MIXED_COMPARISONS_SRC, "p_where_mixed_comparisons", null);
        dump("A3", r);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        Db2CursorInfo cur1 = findCursor(r, "cur1");
        assertNotNull(cur1);
        String txt = cur1.getSelectText() == null ? "" : cur1.getSelectText().toUpperCase();
        boolean likeClauseSurvived = txt.contains("CORP");
        boolean betweenUpperBoundSurvived = txt.contains("2026-12-31");
        System.out.println(">>> likeClauseSurvived(trailing predicate)=" + likeClauseSurvived
                + " betweenUpperBoundSurvived(BETWEEN's own AND not mistaken for WHERE terminator)="
                + betweenUpperBoundSurvived);
    }

    // A4: WHERE with a subquery that has its own, differently-styled WHERE (trailing-AND
    //     outer vs leading-AND inner, plus a nested IN list inside the subquery).
    private static final String WHERE_NESTED_SUBQUERY_STYLE_SRC =
        "CREATE PROCEDURE p_where_subquery_style(IN p_region CHAR(2))\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE cur1 CURSOR FOR\n"
      + "    SELECT o.order_id\n"
      + "    FROM orders o\n"
      + "    WHERE o.region = p_region AND\n"
      + "          o.cust_id IN (\n"
      + "            SELECT c.cust_id\n"
      + "            FROM customers c\n"
      + "            WHERE c.active = 1\n"
      + "              AND c.tier IN ('GOLD', 'PLATINUM')\n"
      + "          );\n"
      + "END";

    @Test
    @DisplayName("A4: outer WHERE trailing-AND style, inner subquery WHERE leading-AND style, own IN list — inner tables/predicates captured?")
    void whereWithDifferentlyStyledSubqueryWhere() {
        Db2RoutineInfo r = engine.parse(WHERE_NESTED_SUBQUERY_STYLE_SRC, "p_where_subquery_style", null);
        dump("A4", r);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        Db2CursorInfo cur1 = findCursor(r, "cur1");
        assertNotNull(cur1);
        String txt = cur1.getSelectText() == null ? "" : cur1.getSelectText().toUpperCase();
        boolean subqueryPredicateSurvived = txt.contains("PLATINUM");
        boolean customersCapturedAsTableOp = r.getTableOps().stream()
                .anyMatch(t -> "CUSTOMERS".equalsIgnoreCase(t.getTableName()));
        System.out.println(">>> subqueryPredicateSurvivedInCursorText(GOLD/PLATINUM)=" + subqueryPredicateSurvived
                + " customersCapturedAsTableOp(fromWhereSubquery)=" + customersCapturedAsTableOp);
    }

    // ═════════════════════════════════════════════════════════════════════════
    // B. All comment positions around a CURSOR declaration.
    // ═════════════════════════════════════════════════════════════════════════

    // B1: comment before DECLARE.
    private static final String CMT_BEFORE_DECLARE_SRC =
        "CREATE PROCEDURE p_cmt_before_declare(IN p_region CHAR(2))\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  -- fetches all open orders for a region\n"
      + "  DECLARE cur1 CURSOR FOR\n"
      + "    SELECT o.order_id FROM orders o WHERE o.region = p_region;\n"
      + "END";

    @Test
    @DisplayName("B1: line comment before DECLARE — cursor still captured at correct line?")
    void commentBeforeDeclare() {
        Db2RoutineInfo r = engine.parse(CMT_BEFORE_DECLARE_SRC, "p_cmt_before_declare", null);
        dump("B1", r);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        Db2CursorInfo cur1 = findCursor(r, "cur1");
        assertNotNull(cur1, "cursor cur1 not captured at all — comment before DECLARE broke capture");
        System.out.println(">>> declaredAtLine(expect 5)=" + cur1.getLine());
    }

    // B2: comment between cursor_name and CURSOR keyword.
    private static final String CMT_BETWEEN_NAME_AND_CURSOR_SRC =
        "CREATE PROCEDURE p_cmt_name_cursor(IN p_region CHAR(2))\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE cur1 /* the main cursor */ CURSOR FOR\n"
      + "    SELECT o.order_id FROM orders o WHERE o.region = p_region;\n"
      + "END";

    @Test
    @DisplayName("B2: block comment between cursor_name and CURSOR — name and query still captured?")
    void commentBetweenNameAndCursorKeyword() {
        Db2RoutineInfo r = engine.parse(CMT_BETWEEN_NAME_AND_CURSOR_SRC, "p_cmt_name_cursor", null);
        dump("B2", r);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        Db2CursorInfo cur1 = findCursor(r, "cur1");
        assertNotNull(cur1, "cursor cur1 not captured — comment between name and CURSOR broke capture");
    }

    // B3: comment between CURSOR and FOR.
    private static final String CMT_BETWEEN_CURSOR_AND_FOR_SRC =
        "CREATE PROCEDURE p_cmt_cursor_for(IN p_region CHAR(2))\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE cur1 CURSOR -- read-only forward cursor\n"
      + "    FOR SELECT o.order_id FROM orders o WHERE o.region = p_region;\n"
      + "END";

    @Test
    @DisplayName("B3: line comment between CURSOR and FOR — cursor still captured?")
    void commentBetweenCursorAndFor() {
        Db2RoutineInfo r = engine.parse(CMT_BETWEEN_CURSOR_AND_FOR_SRC, "p_cmt_cursor_for", null);
        dump("B3", r);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        Db2CursorInfo cur1 = findCursor(r, "cur1");
        assertNotNull(cur1, "cursor cur1 not captured — comment between CURSOR and FOR broke capture");
    }

    // B4: comment between FOR and SELECT.
    private static final String CMT_BETWEEN_FOR_AND_SELECT_SRC =
        "CREATE PROCEDURE p_cmt_for_select(IN p_region CHAR(2))\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE cur1 CURSOR FOR\n"
      + "    /* the actual query follows */\n"
      + "    SELECT o.order_id FROM orders o WHERE o.region = p_region;\n"
      + "END";

    @Test
    @DisplayName("B4: block comment between FOR and SELECT — cursor still captured, tables still captured?")
    void commentBetweenForAndSelect() {
        Db2RoutineInfo r = engine.parse(CMT_BETWEEN_FOR_AND_SELECT_SRC, "p_cmt_for_select", null);
        dump("B4", r);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        Db2CursorInfo cur1 = findCursor(r, "cur1");
        assertNotNull(cur1, "cursor cur1 not captured — comment between FOR and SELECT broke capture");
        boolean ordersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "ORDERS".equalsIgnoreCase(t.getTableName()));
        System.out.println(">>> ordersCaptured=" + ordersCaptured);
    }

    // B5: comment inside the select list.
    private static final String CMT_INSIDE_SELECT_LIST_SRC =
        "CREATE PROCEDURE p_cmt_select_list(IN p_region CHAR(2))\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE cur1 CURSOR FOR\n"
      + "    SELECT o.order_id, -- primary key\n"
      + "           o.status,  /* current status */\n"
      + "           o.total\n"
      + "    FROM orders o\n"
      + "    WHERE o.region = p_region;\n"
      + "END";

    @Test
    @DisplayName("B5: line + block comments inside select list — all three columns / table still captured?")
    void commentInsideSelectList() {
        Db2RoutineInfo r = engine.parse(CMT_INSIDE_SELECT_LIST_SRC, "p_cmt_select_list", null);
        dump("B5", r);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        Db2CursorInfo cur1 = findCursor(r, "cur1");
        assertNotNull(cur1, "cursor cur1 not captured — comment inside select list broke capture");
        String txt = cur1.getSelectText() == null ? "" : cur1.getSelectText();
        boolean totalColumnSurvived = txt.toUpperCase().contains("TOTAL");
        System.out.println(">>> selectTextHasWhitespace(readability)=" + (txt.contains(" "))
                + " totalColumnSurvivedPastTwoComments=" + totalColumnSurvived);
        assertTrue(totalColumnSurvived, "select list column must survive interspersed comments");
        assertTrue(txt.contains(" "),
                "Bug 5 fix: Db2CursorInfo.selectText must preserve original whitespace, not be a "
                        + "run-on string — got: " + txt);
    }

    // B6: comment between clauses (FROM ... /* comment */ WHERE ...).
    private static final String CMT_BETWEEN_CLAUSES_SRC =
        "CREATE PROCEDURE p_cmt_between_clauses(IN p_region CHAR(2))\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE cur1 CURSOR FOR\n"
      + "    SELECT o.order_id\n"
      + "    FROM orders o\n"
      + "    -- only open, non-cancelled orders for the requested region\n"
      + "    WHERE o.region = p_region AND o.status = 'OPEN';\n"
      + "END";

    @Test
    @DisplayName("B6: line comment between FROM and WHERE clauses — WHERE predicate still captured?")
    void commentBetweenClauses() {
        Db2RoutineInfo r = engine.parse(CMT_BETWEEN_CLAUSES_SRC, "p_cmt_between_clauses", null);
        dump("B6", r);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        Db2CursorInfo cur1 = findCursor(r, "cur1");
        assertNotNull(cur1, "cursor cur1 not captured — comment between FROM and WHERE broke capture");
        boolean statusPredicateSurvived = cur1.getSelectText() != null
                && cur1.getSelectText().toUpperCase().contains("OPEN");
        System.out.println(">>> statusPredicateSurvivedPastInterClauseComment=" + statusPredicateSurvived);
    }

    // B7: trailing comment after the terminating semicolon.
    private static final String CMT_TRAILING_AFTER_SEMI_SRC =
        "CREATE PROCEDURE p_cmt_trailing(IN p_region CHAR(2))\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE cur1 CURSOR FOR\n"
      + "    SELECT o.order_id FROM orders o WHERE o.region = p_region; -- end of cur1\n"
      + "  DECLARE v_cnt INTEGER DEFAULT 0;\n"
      + "  SET v_cnt = 1;\n"
      + "END";

    @Test
    @DisplayName("B7: trailing line comment right after the terminating ; — next statement (DECLARE v_cnt) still captured?")
    void commentTrailingAfterSemicolon() {
        Db2RoutineInfo r = engine.parse(CMT_TRAILING_AFTER_SEMI_SRC, "p_cmt_trailing", null);
        dump("B7", r);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        Db2CursorInfo cur1 = findCursor(r, "cur1");
        assertNotNull(cur1, "cursor cur1 not captured");
        boolean vCntCaptured = r.getVariables().stream().anyMatch(v -> "v_cnt".equalsIgnoreCase(v.getName()));
        System.out.println(">>> vCntCapturedAfterTrailingComment=" + vCntCaptured);
        assertTrue(vCntCaptured, "trailing comment after cursor's terminating ; dropped the next DECLARE");
    }

    // B8: multi-line block comment spanning several lines before the query — checks for
    //     line-number drift on the statements that follow.
    private static final String CMT_MULTILINE_BLOCK_SRC =
        "CREATE PROCEDURE p_cmt_multiline_block(IN p_region CHAR(2))\n"                       // 1
      + "LANGUAGE SQL\n"                                                                      // 2
      + "BEGIN\n"                                                                             // 3
      + "  /* This cursor selects all open orders.\n"                                         // 4
      + "     It is used by the nightly batch job.\n"                                         // 5
      + "     See ticket ERP-1234 for details.\n"                                             // 6
      + "     Do not change the WHERE clause without approval. */\n"                          // 7
      + "  DECLARE cur1 CURSOR FOR\n"                                                          // 8
      + "    SELECT o.order_id FROM orders o WHERE o.region = p_region;\n"                    // 9
      + "  DECLARE v_cnt INTEGER DEFAULT 0;\n"                                                // 10
      + "END";                                                                                 // 11

    @Test
    @DisplayName("B8: 4-line block comment before DECLARE CURSOR — line numbers of cursor and following DECLARE still correct?")
    void commentMultilineBlockLineDrift() {
        Db2RoutineInfo r = engine.parse(CMT_MULTILINE_BLOCK_SRC, "p_cmt_multiline_block", null);
        dump("B8", r);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        Db2CursorInfo cur1 = findCursor(r, "cur1");
        assertNotNull(cur1, "cursor cur1 not captured — multi-line block comment broke capture");
        System.out.println(">>> cursorLine(expect 8)=" + cur1.getLine());
        Db2VariableInfo vCnt = r.getVariables().stream()
                .filter(v -> "v_cnt".equalsIgnoreCase(v.getName())).findFirst().orElse(null);
        assertNotNull(vCnt, "v_cnt DECLARE after the cursor was dropped");
        System.out.println(">>> vCntLine(expect 10)=" + vCnt.getLine());
        assertEquals(8, cur1.getLine(), "line-number drift: cursor line should be 8 (the physical DECLARE line)");
        assertEquals(10, vCnt.getLine(), "line-number drift: v_cnt DECLARE line should be 10");
    }

    // B9: mixed -- and /* */ comments in a single cursor statement.
    private static final String CMT_MIXED_STYLES_SRC =
        "CREATE PROCEDURE p_cmt_mixed_styles(IN p_region CHAR(2))\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE cur1 CURSOR FOR -- open orders cursor\n"
      + "    SELECT o.order_id, /* pk */ o.status\n"
      + "    FROM orders o -- base table\n"
      + "    WHERE o.region = p_region /* region filter */\n"
      + "      AND o.status = 'OPEN'; -- final predicate\n"
      + "END";

    @Test
    @DisplayName("B9: mixed line + block comments scattered through one cursor statement — full capture survives?")
    void commentMixedStylesInOneStatement() {
        Db2RoutineInfo r = engine.parse(CMT_MIXED_STYLES_SRC, "p_cmt_mixed_styles", null);
        dump("B9", r);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        Db2CursorInfo cur1 = findCursor(r, "cur1");
        assertNotNull(cur1, "cursor cur1 not captured — mixed comment styles broke capture");
        boolean finalPredicateSurvived = cur1.getSelectText() != null
                && cur1.getSelectText().toUpperCase().contains("OPEN");
        boolean ordersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "ORDERS".equalsIgnoreCase(t.getTableName()));
        System.out.println(">>> finalPredicateSurvivedPastFourInterspersedComments=" + finalPredicateSurvived
                + " ordersCaptured=" + ordersCaptured);
    }

    // ═════════════════════════════════════════════════════════════════════════
    // C. Combined: unusual WHERE style + interspersed comments in the same cursor.
    // ═════════════════════════════════════════════════════════════════════════

    // C1: trailing-AND style + comment right at the AND, plus a nested-paren subquery.
    private static final String COMBINED_TRAILING_AND_WITH_COMMENTS_SRC =
        "CREATE PROCEDURE p_combined_trailing_and(IN p_region CHAR(2), IN p_min INTEGER)\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE cur1 CURSOR FOR -- combined style probe\n"
      + "    SELECT o.order_id\n"
      + "    FROM orders o\n"
      + "    WHERE o.region = p_region AND -- region gate\n"
      + "          o.total >= p_min AND    /* minimum order value */\n"
      + "          o.cust_id IN (SELECT c.cust_id FROM customers c WHERE c.vip_flag = 1); -- vip only\n"
      + "END";

    @Test
    @DisplayName("C1: trailing-AND WHERE style + comments at every AND + WHERE subquery — combined survival check")
    void combinedTrailingAndWithComments() {
        Db2RoutineInfo r = engine.parse(COMBINED_TRAILING_AND_WITH_COMMENTS_SRC, "p_combined_trailing_and", null);
        dump("C1", r);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        Db2CursorInfo cur1 = findCursor(r, "cur1");
        assertNotNull(cur1, "cursor cur1 not captured");
        boolean vipPredicateSurvived = cur1.getSelectText() != null
                && cur1.getSelectText().toUpperCase().contains("VIP_FLAG");
        boolean customersCaptured = r.getTableOps().stream()
                .anyMatch(t -> "CUSTOMERS".equalsIgnoreCase(t.getTableName()));
        System.out.println(">>> vipPredicateSurvived=" + vipPredicateSurvived
                + " customersCapturedFromWhereSubquery=" + customersCaptured);
    }

    // C2: leading-AND/OR style with heavy nested parens, block comments between every group.
    private static final String COMBINED_NESTED_PARENS_WITH_COMMENTS_SRC =
        "CREATE PROCEDURE p_combined_nested_parens(IN p_region CHAR(2))\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE cur1 CURSOR FOR\n"
      + "    SELECT o.order_id\n"
      + "    FROM orders o\n"
      + "    WHERE ( /* region or priority group */\n"
      + "            o.region = p_region\n"
      + "            OR /* fallback */ o.priority = 1\n"
      + "          )\n"
      + "      AND ( /* status group */ o.status = 'OPEN' OR o.status = 'PENDING' );\n"
      + "END";

    @Test
    @DisplayName("C2: leading-AND/OR + heavy nested parens + block comments between every group — full predicate survives?")
    void combinedNestedParensWithComments() {
        Db2RoutineInfo r = engine.parse(COMBINED_NESTED_PARENS_WITH_COMMENTS_SRC, "p_combined_nested_parens", null);
        dump("C2", r);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        Db2CursorInfo cur1 = findCursor(r, "cur1");
        assertNotNull(cur1, "cursor cur1 not captured");
        boolean pendingSurvived = cur1.getSelectText() != null
                && cur1.getSelectText().toUpperCase().contains("PENDING");
        System.out.println(">>> pendingSurvived(last predicate, end of doubly-nested-paren WHERE)=" + pendingSurvived);
    }

    // C3: BETWEEN...AND with a comment sitting right on the BETWEEN's own AND — checks the
    //     BETWEEN-AND vs WHERE-AND confusion explicitly under comment interference.
    private static final String COMBINED_BETWEEN_AND_WITH_COMMENT_SRC =
        "CREATE PROCEDURE p_combined_between_and(IN p_region CHAR(2))\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  DECLARE cur1 CURSOR FOR\n"
      + "    SELECT o.order_id\n"
      + "    FROM orders o\n"
      + "    WHERE o.region = p_region\n"
      + "      AND o.order_date BETWEEN DATE('2026-01-01') -- start\n"
      + "                        AND    /* end of range */ DATE('2026-12-31')\n"
      + "      AND o.status = 'OPEN';\n"
      + "END";

    @Test
    @DisplayName("C3: comment sitting directly on BETWEEN's own AND — upper bound and trailing WHERE-AND predicate both survive?")
    void combinedBetweenAndWithComment() {
        Db2RoutineInfo r = engine.parse(COMBINED_BETWEEN_AND_WITH_COMMENT_SRC, "p_combined_between_and", null);
        dump("C3", r);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        Db2CursorInfo cur1 = findCursor(r, "cur1");
        assertNotNull(cur1, "cursor cur1 not captured");
        String txt = cur1.getSelectText() == null ? "" : cur1.getSelectText().toUpperCase();
        boolean upperBoundSurvived = txt.contains("2026-12-31");
        boolean trailingWhereAndSurvived = txt.contains("OPEN");
        System.out.println(">>> upperBoundSurvived(BETWEEN's own AND not truncated by comment)=" + upperBoundSurvived
                + " trailingWhereAndPredicateSurvived=" + trailingWhereAndSurvived);
        assertTrue(upperBoundSurvived, "BETWEEN's upper bound must not be truncated by a comment");
        assertTrue(trailingWhereAndSurvived, "trailing WHERE-AND predicate must survive");
    }

    // ── Bug 8: StatementTextCapture must not treat a `;` inside a comment as the real
    // statement terminator (confirmed via a constructed repro: latent risk in the audit). ──

    private static final String SEMI_INSIDE_COMMENT_SRC =
        "CREATE PROCEDURE p_bug8(IN p_id INT)\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  UPDATE orders SET status = 'X' -- note: ends with ; not real\n"
      + "  WHERE id = p_id;\n"
      + "END";

    @Test
    @DisplayName("Bug 8: a `;` inside a -- comment must not truncate statement text capture")
    void semicolonInsideCommentDoesNotTruncateStatement() {
        Db2RoutineInfo r = engine.parse(SEMI_INSIDE_COMMENT_SRC, "p_bug8", null);
        dump("Bug8", r);
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        Db2TableOpInfo op = r.getTableOps().stream()
                .filter(t -> "orders".equalsIgnoreCase(t.getTableName()))
                .findFirst().orElse(null);
        assertNotNull(op, "orders table operation must be captured");
        String sql = op.getSqlText() == null ? "" : op.getSqlText().toUpperCase();
        assertTrue(sql.contains("WHERE ID = P_ID"),
                "sqlText must include the real WHERE clause past the fake ';' inside the comment, got: " + sql);
    }

    // ── helpers ──

    private static Db2CursorInfo findCursor(Db2RoutineInfo r, String name) {
        return r.getCursors().stream()
                .filter(c -> name.equalsIgnoreCase(c.getName()))
                .findFirst().orElse(null);
    }
}
