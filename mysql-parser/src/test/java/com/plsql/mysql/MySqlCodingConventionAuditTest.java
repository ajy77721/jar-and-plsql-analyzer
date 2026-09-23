package com.plsql.mysql;

import com.plsql.mysql.engine.MySqlGrammarEngine;
import com.plsql.mysql.model.MySqlRoutineInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AUDIT ONLY (2026-09-16) — real-world MySQL coding-CONVENTION robustness audit.
 *
 * Does NOT touch mysql-parser/src/main/**. Exercises the real MySqlGrammarEngine +
 * MySqlAnalysisVisitor against realistic Hungarian-naming, mixed-case, deeply-nested,
 * heavily-commented, DELIMITER-laden MySQL stored-procedure source, one dimension per test.
 *
 * See task report for the numbered gap/robust list; this file is the reproducing evidence.
 */
public class MySqlCodingConventionAuditTest {

    // ── Dimension 1: Hungarian naming prefixes (v_, p_/in_/out_/inout_, g_, c_) ──────────

    @Test
    @DisplayName("D1: Hungarian-prefixed locals/params, incl. c_get_customer must not be treated as a call")
    void hungarianNamingPrefixes() {
        String sql = """
                CREATE PROCEDURE proc_hungarian(
                    IN p_customer_id INT,
                    IN in_flag TINYINT,
                    OUT out_result INT,
                    INOUT inout_counter INT
                )
                BEGIN
                    DECLARE v_total INT DEFAULT 0;
                    DECLARE c_get_customer INT DEFAULT 42;
                    SET @g_last_customer = p_customer_id;
                    SET v_total = c_get_customer + p_customer_id;
                    CALL get_customer(p_customer_id, out_result);
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_hungarian", null);
        System.out.println("[D1] params: " + r.getParams().stream()
                .map(p -> p.getDirection() + " " + p.getName()).toList());
        System.out.println("[D1] variables: " + r.getVariables().stream()
                .map(v -> v.getName() + ":" + v.getDataType()).toList());
        System.out.println("[D1] calls: " + names(r.getCalls()));
        System.out.println("[D1] errors: " + r.getErrors());

        assertEquals(4, r.getParams().size(), "all 4 params should be captured");
        assertTrue(r.getVariables().stream().anyMatch(v -> "v_total".equalsIgnoreCase(v.getName())));
        assertTrue(r.getVariables().stream().anyMatch(v -> "c_get_customer".equalsIgnoreCase(v.getName())),
                "c_get_customer must be captured as a DECLARE'd variable");
        // The key risk this test targets: c_get_customer must NOT show up as a phantom CALL
        // just because its name looks like "get_customer" with a prefix.
        assertFalse(r.getCalls().stream().anyMatch(c -> "c_get_customer".equalsIgnoreCase(c.getName())),
                "c_get_customer must not be misclassified as a call by name alone");
        assertTrue(r.getCalls().stream().anyMatch(c -> "get_customer".equalsIgnoreCase(c.getName())),
                "the real CALL get_customer(...) must still be captured");
        // Note: MySQL has no cursor-prefix idiom analogous to Oracle's c_ cursor convention in
        // the same slot as c_ constants — MySQL cursors are just DECLARE ... CURSOR FOR, so this
        // sub-case is N/A and not separately tested here.
    }

    // ── Dimension 2: mixed/inconsistent keyword & identifier casing ─────────────────────

    @Test
    @DisplayName("D2: mixed-case keywords (BEGIN/begin, SELECT/select) and identifiers throughout")
    void mixedCaseKeywordsAndIdentifiers() {
        String sql = """
                create procedure Proc_MixedCase(In P_Id INT, out P_Name VARCHAR(50))
                Begin
                    declare V_Count INT default 0;
                    SELECT COUNT(*) into V_Count FROM Customers WHERE id = P_Id;
                    select Name into P_Name from customers WHERE Id = p_id;
                    IF V_Count > 0 THEN
                        Call DoSomething(p_id);
                    end if;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "Proc_MixedCase", null);
        System.out.println("[D2] name: " + r.getName());
        System.out.println("[D2] params: " + r.getParams().stream().map(p -> p.getName()).toList());
        System.out.println("[D2] variables: " + r.getVariables().stream().map(v -> v.getName()).toList());
        System.out.println("[D2] tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.getTableName()).toList());
        System.out.println("[D2] calls: " + names(r.getCalls()));
        System.out.println("[D2] errors: " + r.getErrors());

        assertEquals(2, r.getParams().size());
        assertTrue(r.getVariables().stream().anyMatch(v -> "V_Count".equalsIgnoreCase(v.getName())));
        assertTrue(r.getTableOps().stream().anyMatch(t -> "Customers".equalsIgnoreCase(t.getTableName())));
        assertTrue(r.getCalls().stream().anyMatch(c -> "DoSomething".equalsIgnoreCase(c.getName())));
        assertTrue(r.getErrors().isEmpty(), "mixed-case keywords should not raise parse errors: " + r.getErrors());
    }

    // ── Dimension 3: 10+ params one per line, trailing commas, trailing comments ────────

    @Test
    @DisplayName("D3: procedure with 10+ params, one per line, inconsistent indent, trailing comments")
    void manyParamsOnePerLineWithTrailingComments() {
        String sql = """
                CREATE PROCEDURE proc_many_params(
                  IN p_id INT, -- customer id
                      IN p_name VARCHAR(100), -- customer name
                  IN p_email VARCHAR(150),      -- email address
                        OUT p_status INT, -- result status
                  IN p_addr1 VARCHAR(100), -- address line 1
                  IN p_addr2 VARCHAR(100), -- address line 2
                      IN p_city VARCHAR(50), -- city
                  IN p_state VARCHAR(50), -- state
                  IN p_zip VARCHAR(20), -- zip
                        INOUT p_counter INT, -- running counter
                  IN p_notes TEXT -- final notes, no trailing comma
                )
                BEGIN
                  SET p_status = 1;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_many_params", null);
        System.out.println("[D3] param count: " + r.getParams().size());
        System.out.println("[D3] params: " + r.getParams().stream()
                .map(p -> p.getDirection() + " " + p.getName() + " " + p.getDataType() + " @L" + p.getLine())
                .toList());
        System.out.println("[D3] errors: " + r.getErrors());

        assertEquals(11, r.getParams().size(), "all 11 params must be captured despite formatting");
        assertTrue(r.getParams().stream().anyMatch(p -> "p_notes".equalsIgnoreCase(p.getName())));
        assertTrue(r.getErrors().isEmpty(), "trailing per-param comments should not break parsing: " + r.getErrors());
    }

    // ── Dimension 4: labeled blocks, deep nested IF/ELSEIF, nested CASE, buried call ─────

    @Test
    @DisplayName("D4: labeled BEGIN...END label, IF/ELSEIF nested 5+ deep, nested CASE, buried real call")
    void deeplyNestedControlFlowWithLabeledBlock() {
        String sql = """
                CREATE PROCEDURE proc_deep_nesting(IN p_level INT)
                BEGIN
                  outer_block: BEGIN
                    IF p_level = 1 THEN
                      IF p_level = 2 THEN
                        IF p_level = 3 THEN
                          IF p_level = 4 THEN
                            IF p_level = 5 THEN
                              CASE p_level
                                WHEN 5 THEN
                                  CASE
                                    WHEN p_level > 0 THEN
                                      CALL deeply_buried_proc(p_level);
                                    ELSE
                                      SET p_level = 0;
                                  END CASE;
                                ELSE
                                  SET p_level = -1;
                              END CASE;
                            ELSEIF p_level = 6 THEN
                              SET p_level = 6;
                            ELSE
                              SET p_level = -2;
                            END IF;
                          ELSEIF p_level = 7 THEN
                            SET p_level = 7;
                          END IF;
                        END IF;
                      END IF;
                    END IF;
                  END outer_block;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_deep_nesting", null);
        System.out.println("[D4] calls: " + r.getCalls().stream()
                .map(c -> c.getName() + " @L" + c.getLine()).toList());
        System.out.println("[D4] errors: " + r.getErrors());

        assertTrue(r.getCalls().stream().anyMatch(c -> "deeply_buried_proc".equalsIgnoreCase(c.getName())),
                "call buried at deepest nesting level must still be captured");
        assertTrue(r.getErrors().isEmpty(), "deep nesting + label should not break parsing: " + r.getErrors());
    }

    // ── Dimension 5: multiple statements per line, unusual semicolon placement ──────────

    @Test
    @DisplayName("D5: multiple statements per line separated by semicolons, unusual placement")
    void multipleStatementsPerLineAndUnusualSemicolons() {
        String sql = """
                CREATE PROCEDURE proc_semicolons(IN p_id INT)
                BEGIN
                  DECLARE v_a INT DEFAULT 0; DECLARE v_b INT DEFAULT 0; SET v_a = 1; SET v_b = 2;
                  IF p_id > 0 THEN SET v_a = v_a + 1; CALL helper_one(v_a); ELSE SET v_b = v_b - 1; CALL helper_two(v_b); END IF;
                  ;;;
                  CALL helper_three(v_a, v_b);;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_semicolons", null);
        System.out.println("[D5] calls: " + r.getCalls().stream()
                .map(c -> c.getName() + " @L" + c.getLine()).toList());
        System.out.println("[D5] variables: " + r.getVariables().stream().map(v -> v.getName()).toList());
        System.out.println("[D5] errors: " + r.getErrors());

        // Bug 4 (fixed): a bare "extra-semicolon" run ";;;" on its own line used to derail the
        // parser and silently drop everything after it, including the real
        // "CALL helper_three(v_a, v_b);;" right after it. routineStatement now has an explicit
        // "stmtEmpty : SEMI" alternative so a bare ';' is a legal no-op, same as Oracle's fix
        // for the equivalent bug.
        assertTrue(r.getErrors().isEmpty(), "empty statements (';;;') should no longer produce parse errors: " + r.getErrors());
        assertTrue(r.getCalls().stream().anyMatch(c -> "helper_one".equalsIgnoreCase(c.getName())),
                "helper_one (before the ';;;' line) should be captured");
        assertTrue(r.getCalls().stream().anyMatch(c -> "helper_two".equalsIgnoreCase(c.getName())),
                "helper_two (before the ';;;' line) should be captured");
        boolean helperThreeCaptured = r.getCalls().stream().anyMatch(c -> "helper_three".equalsIgnoreCase(c.getName()));
        System.out.println("[D5] helper_three captured = " + helperThreeCaptured);
        assertTrue(helperThreeCaptured,
                "CALL helper_three(...) right after the bare ';;;' line must be captured");
        assertTrue(r.getVariables().stream().anyMatch(v -> "v_a".equalsIgnoreCase(v.getName())));
        assertTrue(r.getVariables().stream().anyMatch(v -> "v_b".equalsIgnoreCase(v.getName())));
    }

    // ── Dimension 6: large header/docblock comments, no line-number drift ───────────────

    @Test
    @DisplayName("D6: large structured header + docblock before proc — first real statement line number is exact")
    void headerCommentBlockNoLineDrift() {
        String sql = """
                -- =====================================================================
                -- Author:      J. Smith
                -- Date:        2024-01-15
                -- JIRA:        PROJ-1234
                -- =====================================================================
                -- Revision History:
                --   Rev | Date       | Author   | Description
                --   1.0 | 2024-01-15 | jsmith   | Initial version
                --   1.1 | 2024-03-02 | adoe     | Added validation
                --   1.2 | 2024-06-10 | bwhite   | Bugfix for null handling
                -- =====================================================================
                /**
                 * proc_documented
                 * Looks up a customer and logs the access.
                 * @param p_id customer id
                 */
                CREATE PROCEDURE proc_documented(IN p_id INT)
                BEGIN
                  DECLARE v_found INT DEFAULT 0;
                  SELECT COUNT(*) INTO v_found FROM customers WHERE id = p_id;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_documented", null);
        System.out.println("[D6] lineStart=" + r.getLineStart() + " lineEnd=" + r.getLineEnd());
        System.out.println("[D6] variables: " + r.getVariables().stream()
                .map(v -> v.getName() + " @L" + v.getLine()).toList());
        System.out.println("[D6] tableOps: " + r.getTableOps().stream()
                .map(t -> t.getTableName() + " @L" + t.getLine()).toList());
        System.out.println("[D6] errors: " + r.getErrors());

        // Line 15 is "CREATE PROCEDURE ...", line 17 is "DECLARE v_found ...".
        long createLine = sql.lines().toList().indexOf("CREATE PROCEDURE proc_documented(IN p_id INT)") + 1;
        long declareLine = sql.lines().toList().indexOf("  DECLARE v_found INT DEFAULT 0;") + 1;
        System.out.println("[D6] expected CREATE line=" + createLine + " expected DECLARE line=" + declareLine);

        assertEquals((int) createLine, r.getLineStart(), "lineStart must match actual CREATE line, no drift from header comments");
        var found = r.getVariables().stream().filter(v -> "v_found".equalsIgnoreCase(v.getName())).findFirst();
        assertTrue(found.isPresent());
        assertEquals((int) declareLine, found.get().getLine(), "DECLARE line must not drift because of header/docblock comments");
    }

    // ── Dimension 7: long CONCAT() string built across lines with comments interspersed ─

    @Test
    @DisplayName("D7: long string via CONCAT() across lines with interspersed comments")
    void longConcatStringAcrossLinesWithComments() {
        String sql = """
                CREATE PROCEDURE proc_concat(IN p_name VARCHAR(50))
                BEGIN
                  DECLARE v_msg VARCHAR(500);
                  SET v_msg = CONCAT(
                    'Hello, ',       -- greeting part
                    p_name,          /* the customer name */
                    ', welcome to ', -- fixed text
                    'our system. ',  # hash-style comment
                    'Please contact ',
                    'support@example.com ',  -- contact info
                    'for help.'
                  );
                  CALL log_message(v_msg);
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_concat", null);
        System.out.println("[D7] variables: " + r.getVariables().stream().map(v -> v.getName()).toList());
        System.out.println("[D7] calls: " + names(r.getCalls()));
        System.out.println("[D7] errors: " + r.getErrors());

        assertTrue(r.getVariables().stream().anyMatch(v -> "v_msg".equalsIgnoreCase(v.getName())));
        assertTrue(r.getCalls().stream().anyMatch(c -> "log_message".equalsIgnoreCase(c.getName())));
        assertTrue(r.getErrors().isEmpty(),
                "multi-line CONCAT with mixed comment styles interspersed should not break parsing: " + r.getErrors());
    }

    // ── Dimension 8: DELIMITER changes mid-file around CREATE PROCEDURE blocks ──────────

    @Test
    @DisplayName("D8: raw mysqldump-style script with DELIMITER $$ ... DELIMITER ; is preprocessed and parses cleanly")
    void delimiterChangesMidFileRawMysqldumpStyle() {
        // Realistic raw export: DELIMITER is a mysql-CLIENT directive, not SQL - a real
        // server-side parser (and INFORMATION_SCHEMA.ROUTINES) never sees it. Bug 3 (fixed):
        // this used to NPE in MySqlAnalysisVisitor because ctx.routineType was null - the
        // DELIMITER directive is now stripped (and the custom terminator normalized back to
        // ';') by MySqlGrammarEngine.preprocess() before the ANTLR lexer ever sees the text.
        String sql = """
                DELIMITER $$

                CREATE PROCEDURE proc_delim_one(IN p_id INT)
                BEGIN
                  DECLARE v_x INT DEFAULT 0;
                  SET v_x = p_id + 1;
                  CALL after_delim_call(v_x);
                END$$

                DELIMITER ;
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_delim_one", null);
        System.out.println("[D8] name=" + r.getName());
        System.out.println("[D8] calls: " + names(r.getCalls()));
        System.out.println("[D8] variables: " + r.getVariables().stream().map(v -> v.getName()).toList());
        System.out.println("[D8] errors: " + r.getErrors());

        assertNotNull(r, "DELIMITER directives must not crash the engine (was: NPE on ctx.routineType)");
        assertTrue(r.getErrors().isEmpty(), "DELIMITER directives should be fully stripped: " + r.getErrors());
        assertEquals("proc_delim_one", r.getName());
        assertTrue(r.getCalls().stream().anyMatch(c -> "after_delim_call".equalsIgnoreCase(c.getName())),
                "CALL after the DELIMITER-wrapped body must still be captured");
        assertTrue(r.getVariables().stream().anyMatch(v -> "v_x".equalsIgnoreCase(v.getName())));
    }

    private static String names(java.util.List<com.plsql.mysql.model.MySqlCallInfo> calls) {
        return calls == null ? "null" : calls.stream()
                .map(c -> (c.getSchema() != null ? c.getSchema() + "." : "") + c.getName())
                .toList().toString();
    }
}
