package com.plsql.mysql;

import com.plsql.mysql.engine.MySqlGrammarEngine;
import com.plsql.mysql.model.MySqlCursorInfo;
import com.plsql.mysql.model.MySqlJoinInfo;
import com.plsql.mysql.model.MySqlRoutineInfo;
import com.plsql.mysql.model.MySqlTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * READ-ONLY AUDIT (2026-09-17) — comment-placement and WHERE-clause style diversity audit
 * for mysql-parser, mirroring the evidence-first, non-strict style of
 * MySqlDmlCaptureGapAuditTest / VarVsCallVerificationTest. Does NOT touch
 * mysql-parser/src/main/**. Exercises the real MySqlGrammarEngine + MySqlAnalysisVisitor
 * against realistic multi-procedure MySQL fixtures using DELIMITER $$ style bodies (the
 * DELIMITER lines themselves are stripped before feeding the engine, since MySqlGrammarEngine
 * parses one routine body at a time exactly like the other audit tests in this package — the
 * fixtures are still written as a DBA would write them, comments included).
 *
 * Section A: WHERE-clause style diversity (AND/OR position, nesting, comparison-style mix,
 * IN/BETWEEN/LIKE, subqueries with their own differently-styled WHERE).
 * Section B: comment placement around DECLARE ... CURSOR FOR ... (MySQL syntax) at all 8
 * positions called out in the task, plus multi-line block comments and mixed -- / star-comment
 * styles in one statement.
 * Section C: A+B combined — unusual WHERE style with interspersed comments in the same cursor.
 *
 * For every fixture we print: parse errors, cursor name(s), raw captured selectText, line
 * number, and captured table ops for the enclosing routine — then assert only on the things we
 * are confident about. Where the evidence documents a real gap, that is asserted explicitly
 * (assertTrue/assertFalse on the *observed* gap), same convention as the DML capture audit.
 */
public class MySqlCommentStyleAuditTest {

    private static List<String> tableNames(MySqlRoutineInfo r) {
        return r.getTableOps().stream().map(MySqlTableOpInfo::getTableName).toList();
    }

    private static MySqlCursorInfo findCursor(MySqlRoutineInfo r, String name) {
        return r.getCursors().stream()
                .filter(c -> name.equalsIgnoreCase(c.getName()))
                .findFirst()
                .orElse(null);
    }

    private static void dump(String tag, MySqlRoutineInfo r) {
        System.out.println("[" + tag + "] errors: " + r.getErrors());
        System.out.println("[" + tag + "] cursors: " + r.getCursors().stream()
                .map(c -> c.getName() + "@L" + c.getLine() + " => <<" + c.getSelectText() + ">>")
                .toList());
        System.out.println("[" + tag + "] tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.getTableName() + "@L" + t.getLine())
                .toList());
    }

    // ═════════════════════════════════════════════════════════════════════════════════════
    // SECTION A — WHERE-clause style diversity
    // ═════════════════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("A1a: AND/OR at START of line (leading-operator style)")
    void whereLeadingOperatorStyle() {
        String sql = """
                CREATE PROCEDURE proc_where_leading()
                BEGIN
                  DECLARE cur_a CURSOR FOR
                    SELECT o.id, o.status, o.total
                    FROM orders o
                    WHERE o.status = 'OPEN'
                      AND o.total > 100
                      OR o.priority = 'HIGH';
                  OPEN cur_a;
                  CLOSE cur_a;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_where_leading", null);
        dump("A1a", r);

        MySqlCursorInfo cur = findCursor(r, "cur_a");
        assertNotNull(cur, "cursor must be found even with leading AND/OR style");
        System.out.println("[A1a] selectText contains 'OPEN' literal? " + cur.getSelectText().contains("OPEN"));
        System.out.println("[A1a] selectText contains 'priority'? " + cur.getSelectText().contains("priority"));
        assertTrue(tableNames(r).stream().anyMatch(t -> "orders".equalsIgnoreCase(t)),
                "base table must be captured regardless of leading-operator WHERE style");
        assertTrue(r.getErrors().isEmpty(), "leading AND/OR WHERE style should not raise parse errors: " + r.getErrors());
    }

    @Test
    @DisplayName("A1b: AND/OR at END of line (trailing-operator style)")
    void whereTrailingOperatorStyle() {
        String sql = """
                CREATE PROCEDURE proc_where_trailing()
                BEGIN
                  DECLARE cur_b CURSOR FOR
                    SELECT o.id, o.status, o.total
                    FROM orders o
                    WHERE o.status = 'OPEN' AND
                          o.total > 100 OR
                          o.priority = 'HIGH';
                  OPEN cur_b;
                  CLOSE cur_b;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_where_trailing", null);
        dump("A1b", r);

        MySqlCursorInfo cur = findCursor(r, "cur_b");
        assertNotNull(cur, "cursor must be found even with trailing-operator style");
        assertTrue(tableNames(r).stream().anyMatch(t -> "orders".equalsIgnoreCase(t)),
                "base table must be captured regardless of trailing-operator WHERE style");
        assertTrue(r.getErrors().isEmpty(), "trailing AND/OR WHERE style should not raise parse errors: " + r.getErrors());
    }

    @Test
    @DisplayName("A2: heavy nested parenthesization in WHERE")
    void whereHeavyNestedParens() {
        String sql = """
                CREATE PROCEDURE proc_where_nested()
                BEGIN
                  SELECT p.id, p.name
                  FROM products p
                  WHERE (p.category_id = 1 AND (p.price > 100 OR p.on_sale = 1)) AND p.active = 1;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_where_nested", null);
        dump("A2", r);

        assertTrue(tableNames(r).stream().anyMatch(t -> "products".equalsIgnoreCase(t)),
                "base table must be captured with heavily nested parens in WHERE");
        assertTrue(r.getErrors().isEmpty(), "nested-paren WHERE should not raise parse errors: " + r.getErrors());
    }

    @Test
    @DisplayName("A3: mixed comparison styles — reversed operands, IS NULL, multi-line IN, BETWEEN, LIKE")
    void whereMixedComparisonStyles() {
        String sql = """
                CREATE PROCEDURE proc_where_mixed()
                BEGIN
                  SELECT e.id, e.name, e.hire_date, e.salary
                  FROM employees e
                  WHERE 'ACTIVE' = e.status
                    AND e.termination_date IS NULL
                    AND e.manager_id IS NOT NULL
                    AND e.dept_id IN (
                        10,
                        20,
                        30
                    )
                    AND e.hire_date BETWEEN '2020-01-01' AND '2025-12-31'
                    AND e.name LIKE '%smith%';
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_where_mixed", null);
        dump("A3", r);

        assertTrue(tableNames(r).stream().anyMatch(t -> "employees".equalsIgnoreCase(t)),
                "base table must be captured");
        assertTrue(r.getErrors().isEmpty(),
                "reversed operands / IS NULL / multi-line IN / BETWEEN..AND / LIKE must all parse cleanly "
                        + "without BETWEEN's AND being confused with a WHERE-level AND: " + r.getErrors());
    }

    @Test
    @DisplayName("A4: WHERE with subquery that has its OWN WHERE in a different style than outer")
    void whereSubqueryDifferentStyleThanOuter() {
        String sql = """
                CREATE PROCEDURE proc_where_subq_mixed_style()
                BEGIN
                  SELECT d.dept_name
                  FROM departments d
                  WHERE d.active = 1
                    AND d.id IN (
                      SELECT a.dept_id
                      FROM active_assignments a
                      WHERE a.role = 'LEAD' OR
                            a.role = 'MANAGER'
                    );
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_where_subq_mixed_style", null);
        dump("A4", r);

        assertTrue(tableNames(r).stream().anyMatch(t -> "departments".equalsIgnoreCase(t)),
                "outer FROM table must be captured");
        assertTrue(tableNames(r).stream().anyMatch(t -> "active_assignments".equalsIgnoreCase(t)),
                "subquery table (trailing-operator WHERE style, different from outer leading style) must be captured");
        assertTrue(r.getErrors().isEmpty(),
                "outer leading-AND style + inner trailing-OR style in the same statement should not raise parse errors: "
                        + r.getErrors());
    }

    // ═════════════════════════════════════════════════════════════════════════════════════
    // SECTION B — comment placement around DECLARE ... CURSOR FOR ... (8 positions)
    // ═════════════════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("B1: line comment BEFORE the DECLARE keyword")
    void commentBeforeDeclare() {
        String sql = """
                CREATE PROCEDURE proc_cmt_before()
                BEGIN
                  -- fetch all pending orders for nightly batch
                  DECLARE cur_pending CURSOR FOR
                    SELECT id, status FROM orders WHERE status = 'PENDING';
                  OPEN cur_pending;
                  CLOSE cur_pending;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_cmt_before", null);
        dump("B1", r);

        MySqlCursorInfo cur = findCursor(r, "cur_pending");
        assertNotNull(cur, "cursor must be captured with a line comment before DECLARE");
        System.out.println("[B1] cursor line = " + cur.getLine() + " (expect 4, the DECLARE line, 1-indexed incl. comment line 3)");
        assertEquals(4, cur.getLine(), "DECLARE line number must not drift because of a preceding comment line");
        assertTrue(tableNames(r).stream().anyMatch(t -> "orders".equalsIgnoreCase(t)));
    }

    @Test
    @DisplayName("B2: comment BETWEEN cursor_name and CURSOR keyword")
    void commentBetweenNameAndCursorKeyword() {
        String sql = """
                CREATE PROCEDURE proc_cmt_after_name()
                BEGIN
                  DECLARE cur_after_name /* declares the row source */ CURSOR FOR
                    SELECT id, status FROM orders WHERE status = 'PENDING';
                  OPEN cur_after_name;
                  CLOSE cur_after_name;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_cmt_after_name", null);
        dump("B2", r);

        MySqlCursorInfo cur = findCursor(r, "cur_after_name");
        assertNotNull(cur, "cursor name must be captured even with a block comment between name and CURSOR keyword");
        assertTrue(tableNames(r).stream().anyMatch(t -> "orders".equalsIgnoreCase(t)));
        assertTrue(r.getErrors().isEmpty(), "comment between cursor name and CURSOR keyword should not raise parse errors: " + r.getErrors());
    }

    @Test
    @DisplayName("B3: comment BETWEEN CURSOR and FOR")
    void commentBetweenCursorAndFor() {
        String sql = """
                CREATE PROCEDURE proc_cmt_cursor_for()
                BEGIN
                  DECLARE cur_c CURSOR /* row source below */ FOR
                    SELECT id, status FROM orders WHERE status = 'PENDING';
                  OPEN cur_c;
                  CLOSE cur_c;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_cmt_cursor_for", null);
        dump("B3", r);

        MySqlCursorInfo cur = findCursor(r, "cur_c");
        assertNotNull(cur, "cursor must be captured with a comment between CURSOR and FOR");
        assertTrue(tableNames(r).stream().anyMatch(t -> "orders".equalsIgnoreCase(t)));
        assertTrue(r.getErrors().isEmpty(), "comment between CURSOR and FOR should not raise parse errors: " + r.getErrors());
    }

    @Test
    @DisplayName("B4: comment BETWEEN FOR and SELECT")
    void commentBetweenForAndSelect() {
        String sql = """
                CREATE PROCEDURE proc_cmt_for_select()
                BEGIN
                  DECLARE cur_d CURSOR FOR
                    -- the actual query starts here
                    SELECT id, status FROM orders WHERE status = 'PENDING';
                  OPEN cur_d;
                  CLOSE cur_d;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_cmt_for_select", null);
        dump("B4", r);

        MySqlCursorInfo cur = findCursor(r, "cur_d");
        assertNotNull(cur, "cursor must be captured with a comment between FOR and SELECT");
        System.out.println("[B4] cursor.getLine() = " + cur.getLine() + " (start of DECLARE, unaffected — the "
                + "comment sits inside the selectStatement's leading trivia, before the SELECT token)");
        assertTrue(tableNames(r).stream().anyMatch(t -> "orders".equalsIgnoreCase(t)));
        assertTrue(r.getErrors().isEmpty(), "comment between FOR and SELECT should not raise parse errors: " + r.getErrors());
    }

    @Test
    @DisplayName("B5: comment INSIDE the select list")
    void commentInsideSelectList() {
        String sql = """
                CREATE PROCEDURE proc_cmt_select_list()
                BEGIN
                  DECLARE cur_e CURSOR FOR
                    SELECT id, /* order status code */ status, total
                    FROM orders
                    WHERE status = 'PENDING';
                  OPEN cur_e;
                  CLOSE cur_e;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_cmt_select_list", null);
        dump("B5", r);

        MySqlCursorInfo cur = findCursor(r, "cur_e");
        assertNotNull(cur, "cursor must be captured with a comment inside the select list");
        System.out.println("[B5] selectText: " + cur.getSelectText());
        // Bug 4 fix: selectText is now captured via an input-stream slice by start/stop token
        // index (originalText()), preserving the original source whitespace, instead of
        // ctx.getText()'s raw visible-channel-only token concatenation.
        assertTrue(cur.getSelectText().contains(" "),
                "Bug 4 fix: MySqlCursorInfo.selectText must preserve original whitespace, not be a "
                        + "run-on string like 'id,status' — got: " + cur.getSelectText());
        assertTrue(cur.getSelectText().contains("id"), "selectText must still contain the select list");
        assertTrue(tableNames(r).stream().anyMatch(t -> "orders".equalsIgnoreCase(t)));
        assertTrue(r.getErrors().isEmpty(), "comment inside select list should not raise parse errors: " + r.getErrors());
    }

    @Test
    @DisplayName("B6: comment BETWEEN clauses (FROM ... /* comment */ WHERE ...)")
    void commentBetweenClauses() {
        String sql = """
                CREATE PROCEDURE proc_cmt_between_clauses()
                BEGIN
                  DECLARE cur_f CURSOR FOR
                    SELECT id, status
                    FROM orders
                    -- only pending rows matter for this batch
                    WHERE status = 'PENDING';
                  OPEN cur_f;
                  CLOSE cur_f;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_cmt_between_clauses", null);
        dump("B6", r);

        MySqlCursorInfo cur = findCursor(r, "cur_f");
        assertNotNull(cur, "cursor must be captured with a comment between FROM and WHERE clauses");
        assertTrue(cur.getSelectText().toUpperCase().contains("WHERE"),
                "WHERE clause text must still be present after the between-clauses comment (not truncated)");
        assertTrue(tableNames(r).stream().anyMatch(t -> "orders".equalsIgnoreCase(t)));
        assertTrue(r.getErrors().isEmpty(), "comment between FROM and WHERE should not raise parse errors: " + r.getErrors());
    }

    @Test
    @DisplayName("B7: trailing comment AFTER the terminating semicolon")
    void commentAfterTerminatingSemicolon() {
        String sql = """
                CREATE PROCEDURE proc_cmt_trailing()
                BEGIN
                  DECLARE cur_g CURSOR FOR
                    SELECT id, status FROM orders WHERE status = 'PENDING'; -- end of cursor decl
                  OPEN cur_g;
                  CLOSE cur_g;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_cmt_trailing", null);
        dump("B7", r);

        MySqlCursorInfo cur = findCursor(r, "cur_g");
        assertNotNull(cur, "cursor must be captured with a trailing comment after the terminating semicolon");
        assertFalse(cur.getSelectText().contains("end of cursor decl"),
                "trailing comment content must not leak into the captured selectText");
        assertTrue(tableNames(r).stream().anyMatch(t -> "orders".equalsIgnoreCase(t)));
        assertTrue(r.getErrors().isEmpty(), "trailing comment after ';' should not raise parse errors: " + r.getErrors());
    }

    @Test
    @DisplayName("B7a: comments inside an ON condition are stripped from the captured condition text")
    void commentsInsideOnConditionAreStrippedFromCapturedText() {
        // The captured ON-condition is shown as the label of a join edge. Captured verbatim it
        // read "a.id = /* the key */ b.id -- trailing\n AND a.x = b.x" — the commentary, including
        // an end-of-line comment that visually swallowed the AND on the next line, was presented
        // as if it were part of the predicate. SQL Server already strips comments here and
        // Postgres never had them (it captures getText()); MySQL now matches, while keeping the
        // original spacing that getText() would flatten.
        String sql = """
                CREATE PROCEDURE proc_on_cmt()
                BEGIN
                  SELECT o.id
                    FROM orders o
                    JOIN customers c ON o.cust_id = /* the key */ c.id -- trailing note
                         AND o.region = c.region;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_on_cmt", null);
        dump("B7a", r);

        String cond = r.getTableOps().stream()
                .flatMap(o -> o.getJoins().stream())
                .map(MySqlJoinInfo::getCondition)
                .filter(java.util.Objects::nonNull)
                .findFirst().orElse(null);
        assertNotNull(cond, "the JOIN and its ON condition must be captured");
        assertFalse(cond.contains("the key"), "block comment leaked into condition: " + cond);
        assertFalse(cond.contains("trailing note"), "line comment leaked into condition: " + cond);
        assertTrue(cond.contains("o.cust_id"), cond);
        assertTrue(cond.contains("c.id"), cond);
        assertTrue(cond.contains("AND"), "the predicate after the line comment must survive: " + cond);
        assertTrue(cond.contains("o.region"), cond);
    }

    @Test
    @DisplayName("B7a2: a string literal in an ON condition that LOOKS like a comment is untouched")
    void commentLikeStringLiteralInOnConditionIsNotStripped() {
        // The guard on the stripping above: only the whitespace GAPS between default-channel
        // tokens are scanned, so literal content — which is a token — can never be mistaken for a
        // comment, however much it looks like one.
        String sql = """
                CREATE PROCEDURE proc_on_literal()
                BEGIN
                  SELECT o.id
                    FROM orders o
                    JOIN customers c ON c.note = '-- not a comment /* nor this */';
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_on_literal", null);
        dump("B7a2", r);

        String cond = r.getTableOps().stream()
                .flatMap(o -> o.getJoins().stream())
                .map(MySqlJoinInfo::getCondition)
                .filter(java.util.Objects::nonNull)
                .findFirst().orElse(null);
        assertNotNull(cond, "the JOIN and its ON condition must be captured");
        assertTrue(cond.contains("'-- not a comment /* nor this */'"),
                "string literal content must survive comment stripping verbatim: " + cond);
    }

    @Test
    @DisplayName("B7b: `--` with no following whitespace is subtraction, not a comment (real MySQL rule)")
    void doubleDashWithoutWhitespaceIsSubtractionNotAComment() {
        // The MySQL manual (Comment Syntax) is explicit and differs from the SQL standard here:
        // a `--` comment requires "at least one whitespace or control character" after the second
        // dash. `10--3` is therefore 10 minus negative 3, which MySQL evaluates to 13.
        //
        // The lexer used to accept a bare `'--' ~[\r\n]*`, so `SET v = 10--3;` lexed as `SET v =
        // 10` followed by a comment, the SET failed to parse, and — because the enclosing body
        // then fell into the unsupported-statement catch-all — the INSERT after it was lost too.
        // That is the familiar shape: not one wrong value, an entire silently-empty analysis.
        String sql = """
                CREATE PROCEDURE proc_dash_arith()
                BEGIN
                  DECLARE v INT;
                  SET v = 10--3;
                  INSERT INTO orders (qty) VALUES (v);
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_dash_arith", null);
        dump("B7b", r);

        assertTrue(r.getErrors().isEmpty(), "10--3 is arithmetic in MySQL: " + r.getErrors());
        assertTrue(tableNames(r).stream().anyMatch(t -> "orders".equalsIgnoreCase(t)),
                "the statement AFTER the arithmetic must survive; got " + tableNames(r));
    }

    @Test
    @DisplayName("B7c: `-- ` and a bare `--` at end of line are still comments")
    void doubleDashFollowedByWhitespaceOrEolIsStillAComment() {
        // The other half of the same rule — tightening LINE_COMMENT must not cost us the ordinary
        // spellings. A bare `--` with nothing after it is followed by the newline, which is a
        // whitespace character, so it is a comment.
        String sql = """
                CREATE PROCEDURE proc_dash_comment()
                BEGIN --
                  -- a normal comment
                  INSERT INTO orders (qty) VALUES (1); --
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_dash_comment", null);
        dump("B7c", r);

        assertTrue(r.getErrors().isEmpty(), "ordinary -- comments must still work: " + r.getErrors());
        assertTrue(tableNames(r).stream().anyMatch(t -> "orders".equalsIgnoreCase(t)));
    }

    @Test
    @DisplayName("B8: multi-line block comment BEFORE DECLARE, plus mixed -- and block comments in one statement")
    void multiLineBlockCommentAndMixedStyles() {
        String sql = """
                CREATE PROCEDURE proc_cmt_multiline_mixed()
                BEGIN
                  DECLARE done INT DEFAULT 0;
                  /*
                   * Cursor: cur_h
                   * Purpose: iterate high-value shipments for the reconciliation job.
                   * Owner: billing-team
                   */
                  DECLARE cur_h CURSOR FOR -- declares the reconciliation cursor
                    SELECT /* shipment id */ s.id,
                           s.tracking_no, -- carrier tracking number
                           s.status
                    FROM shipments s /* alias: s */
                    WHERE s.status = 'IN_TRANSIT' -- only in-flight shipments
                      AND s.value_cents > 100000;
                  OPEN cur_h;
                  CLOSE cur_h;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_cmt_multiline_mixed", null);
        dump("B8", r);

        MySqlCursorInfo cur = findCursor(r, "cur_h");
        assertNotNull(cur, "cursor must be captured despite a multi-line block comment before DECLARE and "
                + "mixed -- / block comments throughout the statement");
        System.out.println("[B8] cursor.getLine() = " + cur.getLine() + " (expect 9, the actual DECLARE line, "
                + "after the 5-line block comment on lines 4-8 — check whether it drifts)");
        assertEquals(9, cur.getLine(),
                "DECLARE line number must equal the true source line of the DECLARE token, not be thrown off "
                        + "by the preceding 5-line block comment");
        assertTrue(tableNames(r).stream().anyMatch(t -> "shipments".equalsIgnoreCase(t)),
                "base table must be captured despite heavy mixed commenting throughout the cursor SELECT");
        assertTrue(r.getErrors().isEmpty(),
                "multi-line block comment + mixed -- and block comments in one statement should not raise parse errors: "
                        + r.getErrors());
    }

    // ═════════════════════════════════════════════════════════════════════════════════════
    // SECTION C — A + B combined: unusual WHERE style with interspersed comments
    // ═════════════════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("C1: leading-operator WHERE + comments interspersed between every condition")
    void combinedLeadingOperatorWithComments() {
        String sql = """
                CREATE PROCEDURE proc_combo_leading_cmt()
                BEGIN
                  -- nightly reconciliation cursor
                  DECLARE cur_combo1 CURSOR FOR
                    SELECT o.id, o.status, o.total
                    FROM orders o
                    WHERE o.status = 'OPEN' -- must still be open
                      AND o.total > 100 /* high value only */
                      AND o.customer_id IN (
                        SELECT c.id FROM customers c WHERE c.region = 'APAC' -- region filter
                      );
                  OPEN cur_combo1;
                  CLOSE cur_combo1;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_combo_leading_cmt", null);
        dump("C1", r);

        MySqlCursorInfo cur = findCursor(r, "cur_combo1");
        assertNotNull(cur, "cursor must be captured with leading-operator WHERE + interspersed comments");
        assertTrue(tableNames(r).stream().anyMatch(t -> "orders".equalsIgnoreCase(t)),
                "outer table must be captured");
        assertTrue(tableNames(r).stream().anyMatch(t -> "customers".equalsIgnoreCase(t)),
                "subquery table referenced after a commented AND condition must still be captured "
                        + "(comments must not sever the WHERE clause's subquery from extraction)");
        assertTrue(r.getErrors().isEmpty(),
                "leading-operator WHERE with comments interspersed should not raise parse errors: " + r.getErrors());
    }

    @Test
    @DisplayName("C2: nested parens + BETWEEN + trailing-operator style, comments between every clause")
    void combinedNestedParensBetweenTrailingWithComments() {
        String sql = """
                CREATE PROCEDURE proc_combo_nested_between_cmt()
                BEGIN
                  DECLARE cur_combo2 CURSOR FOR /* main reconciliation query */
                    SELECT i.id, i.amount, i.due_date
                    FROM invoices i -- invoice header
                    WHERE (i.status = 'OPEN' OR /* also include overdue */ i.status = 'OVERDUE') AND
                          i.due_date BETWEEN '2026-01-01' AND '2026-12-31' AND
                          /* exclude test accounts */
                          i.account_id NOT IN (
                            SELECT a.id FROM test_accounts a -- exclusion list
                          );
                  OPEN cur_combo2;
                  CLOSE cur_combo2;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_combo_nested_between_cmt", null);
        dump("C2", r);

        MySqlCursorInfo cur = findCursor(r, "cur_combo2");
        assertNotNull(cur, "cursor must be captured with nested parens + BETWEEN + trailing style + comments everywhere");
        assertTrue(tableNames(r).stream().anyMatch(t -> "invoices".equalsIgnoreCase(t)),
                "outer table must be captured despite comment between CURSOR/FOR and after FROM");
        assertTrue(tableNames(r).stream().anyMatch(t -> "test_accounts".equalsIgnoreCase(t)),
                "NOT IN subquery table (nested inside parens, after BETWEEN..AND, with a comment right before "
                        + "the subquery) must still be captured — BETWEEN's AND must not be confused with WHERE's AND, "
                        + "and the nested parens must not drop the subquery table");
        assertTrue(r.getErrors().isEmpty(),
                "combined nested-parens + BETWEEN + trailing-operator + comments-everywhere should not raise parse errors: "
                        + r.getErrors());
    }

    @Test
    @DisplayName("C3: multiple cursors in one procedure, each with a different WHERE style and comment mix")
    void combinedMultipleCursorsDifferentStylesInOneProc() {
        String sql = """
                CREATE PROCEDURE proc_combo_multi_cursor()
                BEGIN
                  DECLARE done INT DEFAULT 0;
                  -- cursor 1: leading operators, block comment mid-clause
                  DECLARE cur_x CURSOR FOR
                    SELECT p.id, p.name
                    FROM products p
                    WHERE p.active = 1
                      AND (p.price > 50 /* threshold */ OR p.featured = 1);
                  /* cursor 2: trailing operators, line comments mid-clause */
                  DECLARE cur_y CURSOR FOR
                    SELECT w.id, w.qty
                    FROM warehouse_stock w -- current stock levels
                    WHERE w.qty BETWEEN 1 AND 1000 AND -- must have some stock
                          w.location LIKE 'US-%';
                  OPEN cur_x;
                  OPEN cur_y;
                  CLOSE cur_x;
                  CLOSE cur_y;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_combo_multi_cursor", null);
        dump("C3", r);

        MySqlCursorInfo curX = findCursor(r, "cur_x");
        MySqlCursorInfo curY = findCursor(r, "cur_y");
        assertNotNull(curX, "cur_x (leading-operator style, block comment mid-clause) must be captured");
        assertNotNull(curY, "cur_y (trailing-operator style, line comments mid-clause, preceded by a block comment) must be captured");
        System.out.println("[C3] cur_x line=" + curX.getLine() + " cur_y line=" + curY.getLine());
        assertTrue(tableNames(r).stream().anyMatch(t -> "products".equalsIgnoreCase(t)));
        assertTrue(tableNames(r).stream().anyMatch(t -> "warehouse_stock".equalsIgnoreCase(t)));
        assertTrue(r.getErrors().isEmpty(),
                "two cursors, different WHERE styles, different comment placements, in one procedure, "
                        + "should not raise parse errors: " + r.getErrors());
    }
}
