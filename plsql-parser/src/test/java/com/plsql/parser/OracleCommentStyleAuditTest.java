package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Read-only audit (2026-09-17): probes WHERE-clause style diversity and comment placement
 * around CURSOR declarations against the real ANTLR parser/visitor, and PRINTS what was
 * captured (cursor query text, line/lineEnd, tables, table operations). This is a diagnostic
 * harness — mirroring OracleDmlCaptureGapAuditTest — not a strict regression suite. Only a few
 * high-confidence points are hard-asserted; everything else is printed as evidence for manual
 * review.
 */
public class OracleCommentStyleAuditTest extends ParserTestBase {

    private static void dumpCursor(String label, SubprogramInfo sub, String cursorName) {
        System.out.println("=== " + label + " ===");
        if (sub == null) {
            System.out.println("  (subprogram not found)");
            return;
        }
        CursorInfo cursor = null;
        if (sub.getCursors() != null) {
            for (CursorInfo c : sub.getCursors()) {
                if (c.getName() != null && c.getName().equalsIgnoreCase(cursorName)) {
                    cursor = c;
                    break;
                }
            }
        }
        if (cursor == null) {
            System.out.println("  (cursor '" + cursorName + "' not found; cursors present: "
                    + (sub.getCursors() == null ? "none" : sub.getCursors().stream()
                            .map(CursorInfo::getName).collect(Collectors.joining(", "))) + ")");
        } else {
            System.out.println("  cursor.name=" + cursor.getName());
            System.out.println("  cursor.line=" + cursor.getLine() + " cursor.lineEnd=" + cursor.getLineEnd());
            System.out.println("  cursor.query=[[[" + cursor.getQuery() + "]]]");
            System.out.println("  cursor.tables=" + cursor.getTables());
        }
        List<TableOperationInfo> ops = sub.getTableOperations();
        if (ops == null || ops.isEmpty()) {
            System.out.println("  (no table operations captured for enclosing procedure)");
        } else {
            for (TableOperationInfo t : ops) {
                System.out.println("  op: table=" + t.getTableName()
                        + " op=" + t.getOperation()
                        + " alias=" + t.getAlias()
                        + " subqueryContext=" + t.getSubqueryContext());
            }
        }
    }

    private Set<String> tableNames(SubprogramInfo sub) {
        return sub.getTableOperations().stream()
                .map(t -> t.getTableName() == null ? "" : t.getTableName().toUpperCase())
                .collect(Collectors.toSet());
    }

    // ------------------------------------------------------------------
    // A1. AND/OR at START of line vs END of line (two separate cursors)
    // ------------------------------------------------------------------
    @Test
    @DisplayName("A1a: AND/OR at START of line")
    void andOrAtStartOfLine() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_and_start IS
                    CURSOR cur_and_start IS
                      SELECT o.id, o.status, o.customer_id
                      FROM orders o
                      WHERE o.status = 'OPEN'
                      AND o.total_amount > 100
                      OR o.priority = 'HIGH';
                  BEGIN
                    FOR r IN cur_and_start LOOP
                      NULL;
                    END LOOP;
                  END proc_and_start;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "AND/OR at start of line");
        SubprogramInfo sub = findSub(result, "PROC_AND_START");
        dumpCursor("A1a: AND/OR at START of line", sub, "CUR_AND_START");
        if (sub != null) {
            org.junit.jupiter.api.Assertions.assertTrue(tableNames(sub).contains("ORDERS"),
                    "orders table must be captured regardless of AND/OR line placement");
        }
    }

    @Test
    @DisplayName("A1b: AND/OR at END of line")
    void andOrAtEndOfLine() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_and_end IS
                    CURSOR cur_and_end IS
                      SELECT o.id, o.status, o.customer_id
                      FROM orders o
                      WHERE o.status = 'OPEN' AND
                            o.total_amount > 100 OR
                            o.priority = 'HIGH';
                  BEGIN
                    FOR r IN cur_and_end LOOP
                      NULL;
                    END LOOP;
                  END proc_and_end;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "AND/OR at end of line");
        SubprogramInfo sub = findSub(result, "PROC_AND_END");
        dumpCursor("A1b: AND/OR at END of line", sub, "CUR_AND_END");
        if (sub != null) {
            org.junit.jupiter.api.Assertions.assertTrue(tableNames(sub).contains("ORDERS"),
                    "orders table must be captured regardless of AND/OR line placement");
        }
    }

    // ------------------------------------------------------------------
    // A2. Heavy nested parenthesization
    // ------------------------------------------------------------------
    @Test
    @DisplayName("A2: heavy nested parens WHERE (a=1 AND (b=2 OR c=3)) AND d=4")
    void heavyNestedParens() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_nested_parens IS
                    CURSOR cur_nested IS
                      SELECT o.id, o.status, o.region, o.priority
                      FROM orders o
                      WHERE (o.status = 'OPEN' AND (o.region = 'EU' OR o.region = 'APAC'))
                        AND o.priority = 'HIGH'
                        AND o.customer_id IN (SELECT id FROM active_customers WHERE active = 'Y');
                  BEGIN
                    FOR r IN cur_nested LOOP
                      NULL;
                    END LOOP;
                  END proc_nested_parens;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "heavy nested parens");
        SubprogramInfo sub = findSub(result, "PROC_NESTED_PARENS");
        dumpCursor("A2: heavy nested parens", sub, "CUR_NESTED");
        org.junit.jupiter.api.Assertions.assertNotNull(sub);
        org.junit.jupiter.api.Assertions.assertTrue(tableNames(sub).contains("ACTIVE_CUSTOMERS"),
                "active_customers (nested-in-IN subquery inside cursor WHERE) must be captured (Bug 1 fix)");
    }

    // ------------------------------------------------------------------
    // A3. Mixed comparison styles
    // ------------------------------------------------------------------
    @Test
    @DisplayName("A3: mixed comparison styles (reversed eq, IS NULL, IN multi-line, BETWEEN, LIKE)")
    void mixedComparisonStyles() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_mixed_cmp IS
                    CURSOR cur_mixed IS
                      SELECT o.id, o.status, o.region, o.created_date, o.notes
                      FROM orders o
                      WHERE 'OPEN' = o.status
                        AND o.cancelled_date IS NULL
                        AND o.approved_flag IS NOT NULL
                        AND o.region IN (
                              'EU',
                              'APAC',
                              'NA'
                            )
                        AND o.created_date BETWEEN DATE '2026-01-01' AND DATE '2026-12-31'
                        AND o.notes LIKE '%urgent%';
                  BEGIN
                    FOR r IN cur_mixed LOOP
                      NULL;
                    END LOOP;
                  END proc_mixed_cmp;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "mixed comparison styles");
        SubprogramInfo sub = findSub(result, "PROC_MIXED_CMP");
        dumpCursor("A3: mixed comparison styles", sub, "CUR_MIXED");
        if (sub != null) {
            org.junit.jupiter.api.Assertions.assertTrue(tableNames(sub).contains("ORDERS"),
                    "orders table must be captured even with BETWEEN's AND present");
        }
    }

    // ------------------------------------------------------------------
    // A4. WHERE with subquery that has its OWN WHERE in a DIFFERENT style
    // ------------------------------------------------------------------
    @Test
    @DisplayName("A4: outer WHERE end-of-line AND, inner subquery WHERE start-of-line AND")
    void mixedStylesSameStatement() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_mixed_styles IS
                    CURSOR cur_mixed_styles IS
                      SELECT o.id, o.customer_id
                      FROM orders o
                      WHERE o.status = 'OPEN' AND
                            o.customer_id IN (
                              SELECT c.id
                              FROM active_customers c
                              WHERE c.region = 'EU'
                              AND c.tier = 'GOLD'
                            );
                  BEGIN
                    FOR r IN cur_mixed_styles LOOP
                      NULL;
                    END LOOP;
                  END proc_mixed_styles;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "mixed styles same statement");
        SubprogramInfo sub = findSub(result, "PROC_MIXED_STYLES");
        dumpCursor("A4: mixed styles same statement (outer end-AND, inner start-AND)", sub, "CUR_MIXED_STYLES");
        org.junit.jupiter.api.Assertions.assertNotNull(sub);
        org.junit.jupiter.api.Assertions.assertTrue(tableNames(sub).contains("ACTIVE_CUSTOMERS"),
                "active_customers (inner IN-subquery, different AND style) must be captured (Bug 1 fix)");
    }

    // ------------------------------------------------------------------
    // B1. Comments BEFORE cursor decl
    // ------------------------------------------------------------------
    @Test
    @DisplayName("B1: line comment, block comment, and multi-line docblock BEFORE cursor decl")
    void commentsBeforeCursorDecl() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_comment_before IS
                    -- simple line comment right before the cursor
                    /* single-line block comment */
                    /**
                     * Multi-line docblock describing the cursor
                     * @purpose fetch open orders for a customer
                     */
                    CURSOR cur_comment_before IS
                      SELECT o.id, o.status
                      FROM orders o
                      WHERE o.status = 'OPEN';
                  BEGIN
                    FOR r IN cur_comment_before LOOP
                      NULL;
                    END LOOP;
                  END proc_comment_before;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "comments before cursor decl");
        SubprogramInfo sub = findSub(result, "PROC_COMMENT_BEFORE");
        dumpCursor("B1: comments BEFORE cursor decl (line + block + docblock)", sub, "CUR_COMMENT_BEFORE");
    }

    // ------------------------------------------------------------------
    // B2. Comment BETWEEN cursor name and IS
    // ------------------------------------------------------------------
    @Test
    @DisplayName("B2: comment between cursor name and IS")
    void commentBetweenNameAndIs() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_comment_name_is IS
                    CURSOR cur_name_is -- comment right after the cursor name
                    IS
                      SELECT o.id, o.status
                      FROM orders o
                      WHERE o.status = 'OPEN';
                  BEGIN
                    FOR r IN cur_name_is LOOP
                      NULL;
                    END LOOP;
                  END proc_comment_name_is;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "comment between name and IS");
        SubprogramInfo sub = findSub(result, "PROC_COMMENT_NAME_IS");
        dumpCursor("B2: comment BETWEEN cursor name and IS", sub, "CUR_NAME_IS");
    }

    // ------------------------------------------------------------------
    // B3. Comment BETWEEN IS and SELECT
    // ------------------------------------------------------------------
    @Test
    @DisplayName("B3: comment (and block comment) between IS and SELECT")
    void commentBetweenIsAndSelect() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_comment_is_select IS
                    CURSOR cur_is_select IS
                      /* block comment between IS and SELECT */
                      -- another line comment here too
                      SELECT o.id, o.status
                      FROM orders o
                      WHERE o.status = 'OPEN';
                  BEGIN
                    FOR r IN cur_is_select LOOP
                      NULL;
                    END LOOP;
                  END proc_comment_is_select;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "comment between IS and SELECT");
        SubprogramInfo sub = findSub(result, "PROC_COMMENT_IS_SELECT");
        dumpCursor("B3: comment BETWEEN IS and SELECT", sub, "CUR_IS_SELECT");
    }

    // ------------------------------------------------------------------
    // B4. Comment INSIDE select list between columns
    // ------------------------------------------------------------------
    @Test
    @DisplayName("B4: comment inside select list between columns")
    void commentInsideSelectList() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_comment_select_list IS
                    CURSOR cur_select_list IS
                      SELECT o.id,
                             -- customer identifier
                             o.customer_id,
                             /* order status code */
                             o.status,
                             o.total_amount
                      FROM orders o
                      WHERE o.status = 'OPEN';
                  BEGIN
                    FOR r IN cur_select_list LOOP
                      NULL;
                    END LOOP;
                  END proc_comment_select_list;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "comment inside select list");
        SubprogramInfo sub = findSub(result, "PROC_COMMENT_SELECT_LIST");
        dumpCursor("B4: comment INSIDE select list between columns", sub, "CUR_SELECT_LIST");
    }

    // ------------------------------------------------------------------
    // B5. Comment BETWEEN clauses (FROM..WHERE, WHERE cond1..cond2)
    // ------------------------------------------------------------------
    @Test
    @DisplayName("B5: comment between FROM and WHERE, and between WHERE conditions")
    void commentBetweenClauses() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_comment_between_clauses IS
                    CURSOR cur_between_clauses IS
                      SELECT o.id, o.status, o.region
                      FROM orders o
                      -- filter to open orders only
                      WHERE o.status = 'OPEN'
                      -- and restrict to EU region
                      AND o.region = 'EU'
                      /* final filter: high priority only */
                      AND o.priority = 'HIGH';
                  BEGIN
                    FOR r IN cur_between_clauses LOOP
                      NULL;
                    END LOOP;
                  END proc_comment_between_clauses;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "comment between clauses");
        SubprogramInfo sub = findSub(result, "PROC_COMMENT_BETWEEN_CLAUSES");
        dumpCursor("B5: comment BETWEEN clauses (FROM/WHERE, WHERE cond1/cond2)", sub, "CUR_BETWEEN_CLAUSES");
    }

    // ------------------------------------------------------------------
    // B6. Comment at END of cursor's terminating ;
    // ------------------------------------------------------------------
    @Test
    @DisplayName("B6: trailing comment after cursor's terminating semicolon")
    void trailingCommentAfterSemicolon() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_comment_trailing IS
                    CURSOR cur_trailing IS
                      SELECT o.id, o.status
                      FROM orders o
                      WHERE o.status = 'OPEN'; -- end of cursor declaration
                  BEGIN
                    FOR r IN cur_trailing LOOP
                      NULL;
                    END LOOP;
                  END proc_comment_trailing;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "trailing comment after semicolon");
        SubprogramInfo sub = findSub(result, "PROC_COMMENT_TRAILING");
        dumpCursor("B6: trailing comment AFTER terminating ;", sub, "CUR_TRAILING");
    }

    // ------------------------------------------------------------------
    // B7/B8. Multi-line block comments in several positions + mixed -- and /* */ in SAME cursor
    // ------------------------------------------------------------------
    @Test
    @DisplayName("B7/B8: multi-line block comments in multiple positions, mixed -- and /* */ styles")
    void multiLineAndMixedCommentStyles() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_comment_mixed IS
                    /*
                     * Multi-line block comment BEFORE the cursor,
                     * describing overall purpose across several lines.
                     */
                    CURSOR cur_mixed_comments
                    /* comment between name and IS, multi-line
                       continued on a second physical line */
                    IS
                      -- comment between IS and SELECT
                      SELECT o.id,
                             o.customer_id, -- trailing on a column
                             /*
                              * multi-line block comment
                              * inside the select list
                              */
                             o.status
                      FROM orders o
                      -- comment before WHERE
                      WHERE o.status = 'OPEN'
                      /* multi-line block comment
                         between WHERE conditions */
                      AND o.region = 'EU'; -- trailing on terminator
                  BEGIN
                    FOR r IN cur_mixed_comments LOOP
                      NULL;
                    END LOOP;
                  END proc_comment_mixed;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "multi-line and mixed comment styles");
        SubprogramInfo sub = findSub(result, "PROC_COMMENT_MIXED");
        dumpCursor("B7/B8: multi-line block comments in multiple positions + mixed -- and /* */", sub, "CUR_MIXED_COMMENTS");
    }

    // ------------------------------------------------------------------
    // C1-C3. Combine unusual WHERE style + interspersed comments in same cursor
    // ------------------------------------------------------------------
    @Test
    @DisplayName("C1: AND-at-start WHERE style with interspersed comments before/between conditions")
    void combinedAndStartWithComments() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_combo_and_start IS
                    -- fetches high-value open orders, see JIRA-1234
                    CURSOR cur_combo_and_start IS
                      SELECT o.id, o.status, o.total_amount, o.customer_id
                      FROM orders o
                      WHERE o.status = 'OPEN'
                      -- only orders above the threshold
                      AND o.total_amount > 500
                      /* exclude orders from blocked customers */
                      AND o.customer_id NOT IN (SELECT id FROM blocked_customers);
                  BEGIN
                    FOR r IN cur_combo_and_start LOOP
                      NULL;
                    END LOOP;
                  END proc_combo_and_start;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "combined AND-start with comments");
        SubprogramInfo sub = findSub(result, "PROC_COMBO_AND_START");
        dumpCursor("C1: AND-at-start + interspersed comments (NOT IN subquery)", sub, "CUR_COMBO_AND_START");
        org.junit.jupiter.api.Assertions.assertNotNull(sub);
        org.junit.jupiter.api.Assertions.assertTrue(tableNames(sub).contains("BLOCKED_CUSTOMERS"),
                "blocked_customers (NOT IN subquery table) must be captured (Bug 1 fix)");
    }

    @Test
    @DisplayName("C2: nested parens + BETWEEN + comments interspersed inside a docblock cursor")
    void combinedNestedParensBetweenComments() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_combo_nested_between IS
                    /**
                     * Cursor: cur_combo_nested_between
                     * Selects shipments needing review within a date window,
                     * for orders that are either rush or flagged for QA.
                     */
                    CURSOR cur_combo_nested_between IS
                      SELECT s.id, s.order_id, s.ship_date, s.qa_flag
                      FROM shipments s
                      -- date window check
                      WHERE s.ship_date BETWEEN DATE '2026-01-01' AND DATE '2026-03-31'
                        AND (s.qa_flag = 'Y' OR (s.rush_flag = 'Y' AND s.carrier = 'FEDEX'))
                        /* only shipments tied to an active order */
                        AND s.order_id IN (SELECT id FROM orders WHERE status <> 'CANCELLED');
                  BEGIN
                    FOR r IN cur_combo_nested_between LOOP
                      NULL;
                    END LOOP;
                  END proc_combo_nested_between;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "combined nested parens + BETWEEN + comments");
        SubprogramInfo sub = findSub(result, "PROC_COMBO_NESTED_BETWEEN");
        dumpCursor("C2: nested parens + BETWEEN + docblock/comments", sub, "CUR_COMBO_NESTED_BETWEEN");
        org.junit.jupiter.api.Assertions.assertNotNull(sub);
        Set<String> comboNames = tableNames(sub);
        org.junit.jupiter.api.Assertions.assertTrue(comboNames.contains("SHIPMENTS"), "shipments must be captured");
        org.junit.jupiter.api.Assertions.assertTrue(comboNames.contains("ORDERS"),
                "orders (IN-subquery table) must be captured (Bug 1 fix)");
    }

    @Test
    @DisplayName("C3: reversed-operand + LIKE + OR-at-end style with mixed -- and /* */ comments")
    void combinedReversedOperandOrEndComments() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE proc_combo_reversed_or_end IS
                    CURSOR cur_combo_reversed_or_end IS -- main filter cursor
                      SELECT c.id, c.name, c.region, c.tier
                      /* customers table, aliased c */
                      FROM customers c
                      WHERE 'GOLD' = c.tier OR
                            'PLATINUM' = c.tier OR
                            -- catch VIP-flagged customers regardless of tier
                            c.notes LIKE '%VIP%'
                      AND c.region IS NOT NULL;
                  BEGIN
                    FOR r IN cur_combo_reversed_or_end LOOP
                      NULL;
                    END LOOP;
                  END proc_combo_reversed_or_end;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "combined reversed operand + OR-at-end + comments");
        SubprogramInfo sub = findSub(result, "PROC_COMBO_REVERSED_OR_END");
        dumpCursor("C3: reversed operand + LIKE + OR-at-end + mixed comments", sub, "CUR_COMBO_REVERSED_OR_END");
        if (sub != null) {
            org.junit.jupiter.api.Assertions.assertTrue(tableNames(sub).contains("CUSTOMERS"),
                    "customers table must be captured despite reversed-operand/OR-at-end style and comments");
        }
    }
}
