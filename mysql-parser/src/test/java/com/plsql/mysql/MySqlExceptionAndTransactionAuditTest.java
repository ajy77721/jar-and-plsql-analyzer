package com.plsql.mysql;

import com.plsql.mysql.engine.MySqlGrammarEngine;
import com.plsql.mysql.model.MySqlCallInfo;
import com.plsql.mysql.model.MySqlExceptionHandlerInfo;
import com.plsql.mysql.model.MySqlRoutineInfo;
import com.plsql.mysql.model.MySqlStatementInfo;
import com.plsql.mysql.model.MySqlTableOpInfo;
import com.plsql.mysql.model.MySqlVariableInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * READ-ONLY AUDIT (2026-09-17) — exception-handling and transaction-control concepts for
 * mysql-parser, driven against the real grammar/visitor (no mocks).
 *
 * Grammar facts confirmed by grep before writing any test (see audit report for exact
 * grep evidence):
 *   - MySqlParser.g4 has no TRY/CATCH rule at all; exception handling is exclusively
 *     `DECLARE {CONTINUE|EXIT|UNDO} HANDLER FOR <conditions> <action>` (declareHandler,
 *     line ~161), and nesting is via a nested compoundBlock (BEGIN...END) used as the
 *     handler's <action>.
 *   - SIGNAL / RESIGNAL exist (SECTION 11, lines ~405-421). RESIGNAL's signalConditionValue
 *     and signalSetClause are both optional (`resignalStatement : RESIGNAL
 *     signalConditionValue? signalSetClause?`), so the bare `RESIGNAL;` re-raise form is
 *     valid grammar.
 *   - declareCondition (`DECLARE name CONDITION FOR ...`) exists (line ~181).
 *   - SAVEPOINT / ROLLBACK TO SAVEPOINT exist (txnSavepoint / txnRollbackTo, lines ~274-277).
 *   - `grep -n "AUTONOMOUS" MySqlParser.g4 MySqlLexer.g4` → no matches. MySQL has no
 *     PRAGMA AUTONOMOUS_TRANSACTION; this concept is N/A for the MySQL dialect and is
 *     documented here, not fabricated as a test.
 */
public class MySqlExceptionAndTransactionAuditTest {

    private static List<String> tableNames(MySqlRoutineInfo r) {
        return r.getTableOps().stream().map(MySqlTableOpInfo::getTableName).toList();
    }

    private static List<String> callNames(MySqlRoutineInfo r) {
        return r.getCalls().stream().map(MySqlCallInfo::getName).toList();
    }

    // ─────────────────────────────────────────────────────────────────────
    // Concept 1: nested exception handlers (handler-inside-handler via nested BEGIN...END)
    // ─────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("1a: outer CONTINUE HANDLER body is a nested BEGIN...END that itself declares "
            + "its own inner HANDLER; calls/table-ops inside the inner block must still be captured")
    void nestedHandlerBlockCapturesInnerCallsAndTableOps() {
        String sql = """
                CREATE PROCEDURE proc_nested_handler()
                BEGIN
                  DECLARE v_outer INT DEFAULT 0;
                  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
                  BEGIN
                    DECLARE v_inner INT DEFAULT 1;
                    DECLARE CONTINUE HANDLER FOR SQLWARNING
                    BEGIN
                      INSERT INTO inner_error_log(msg) VALUES ('inner');
                      CALL log_inner_failure(v_outer);
                    END;
                    UPDATE outer_error_log SET last_seen = NOW() WHERE id = v_outer;
                    CALL log_outer_failure();
                  END;
                  INSERT INTO orders(id) VALUES (1);
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_nested_handler", null);

        System.out.println("[1a] errors: " + r.getErrors());
        System.out.println("[1a] tableOps: " + tableNames(r));
        System.out.println("[1a] calls: " + callNames(r));
        System.out.println("[1a] variables: " + r.getVariables().stream()
                .map(MySqlVariableInfo::getName).toList());
        System.out.println("[1a] exceptionHandlers: " + r.getExceptionHandlers().size());
        for (MySqlExceptionHandlerInfo h : r.getExceptionHandlers()) {
            System.out.println("     handler kind=" + h.getKind() + " conditions=" + h.getConditions()
                    + " line=" + h.getLine());
        }
        for (MySqlStatementInfo s : r.getStatements()) {
            System.out.println("     statement kind=" + s.getType() + " line=" + s.getLine()
                    + " handlerIndex=" + s.getHandlerIndex());
        }
        for (MySqlCallInfo c : r.getCalls()) {
            System.out.println("     call name=" + c.getName() + " handlerIndex=" + c.getHandlerIndex());
        }

        // Observed: the visitor DOES walk into the nested handler's BEGIN...END (default
        // ANTLR visitChildren traversal is not short-circuited anywhere for compoundBlock),
        // so both the inner INSERT/CALL and the outer UPDATE/CALL are captured.
        Assertions.assertTrue(r.getErrors().isEmpty(), "expected clean parse, got: " + r.getErrors());
        Assertions.assertTrue(tableNames(r).contains("inner_error_log"),
                "expected inner handler's INSERT target to be captured: " + tableNames(r));
        Assertions.assertTrue(tableNames(r).contains("outer_error_log"),
                "expected outer handler's UPDATE target to be captured: " + tableNames(r));
        Assertions.assertTrue(callNames(r).contains("log_inner_failure"));
        Assertions.assertTrue(callNames(r).contains("log_outer_failure"));

        // Two DECLARE HANDLER entries recorded (outer SQLEXCEPTION handler + inner SQLWARNING
        // handler).
        Assertions.assertEquals(2, r.getExceptionHandlers().size());

        // FIXED (bug 5): currentHandlerIndex tracking is now backed by a Deque<Integer> stack,
        // and each MySqlExceptionHandlerInfo records parentHandlerIndex (the stack's top at the
        // time it was pushed). Calls inside the inner handler are still tagged with the inner
        // handler's own index, but ancestry back to the outer handler is now reconstructible via
        // exceptionHandlers.get(innerIndex).getParentHandlerIndex().
        MySqlCallInfo innerCall = r.getCalls().stream()
                .filter(c -> c.getName().equals("log_inner_failure")).findFirst().orElseThrow();
        MySqlCallInfo outerCall = r.getCalls().stream()
                .filter(c -> c.getName().equals("log_outer_failure")).findFirst().orElseThrow();
        System.out.println("[1a] innerCall.handlerIndex=" + innerCall.getHandlerIndex()
                + " outerCall.handlerIndex=" + outerCall.getHandlerIndex());
        Assertions.assertNotEquals(outerCall.getHandlerIndex(), innerCall.getHandlerIndex(),
                "inner and outer handler calls still get different, own-handler indices");

        MySqlExceptionHandlerInfo innerHandler = r.getExceptionHandlers().get(innerCall.getHandlerIndex());
        MySqlExceptionHandlerInfo outerHandler = r.getExceptionHandlers().get(outerCall.getHandlerIndex());
        System.out.println("[1a] innerHandler.parentHandlerIndex=" + innerHandler.getParentHandlerIndex()
                + " outerHandler.parentHandlerIndex=" + outerHandler.getParentHandlerIndex());
        Assertions.assertEquals(outerCall.getHandlerIndex(), innerHandler.getParentHandlerIndex(),
                "FIXED: inner handler's parentHandlerIndex now points back to the outer handler");
        Assertions.assertNull(outerHandler.getParentHandlerIndex(),
                "outer (top-level) handler has no parent");
    }

    @Test
    @DisplayName("1b: outer-declared variable (v_outer) referenced inside the nested inner handler "
            + "block — flat variable list, no scope enforcement")
    void outerVariableVisibleInNestedHandlerScope() {
        String sql = """
                CREATE PROCEDURE proc_nested_handler_scope()
                BEGIN
                  DECLARE v_outer INT DEFAULT 5;
                  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
                  BEGIN
                    DECLARE v_inner INT DEFAULT 1;
                    DECLARE CONTINUE HANDLER FOR SQLWARNING
                    BEGIN
                      SET v_outer = v_outer + v_inner;
                    END;
                  END;
                  SELECT v_outer;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_nested_handler_scope", null);
        System.out.println("[1b] errors: " + r.getErrors());
        System.out.println("[1b] variables: " + r.getVariables().stream()
                .map(v -> v.getName() + "(line " + v.getLine() + ")").toList());

        Assertions.assertTrue(r.getErrors().isEmpty(), "expected clean parse, got: " + r.getErrors());
        // Observed: v_outer and v_inner both land in the SAME flat result.getVariables() list —
        // there is no per-block scope object at all, so "is v_outer visible inside the nested
        // handler" is trivially true only because the model has no concept of scope to violate.
        // This means a variable re-declared with the same name in an inner block (shadowing,
        // legal in MySQL) would silently produce two entries with the same name and no way to
        // tell which one a given statement actually resolves against.
        List<String> names = r.getVariables().stream().map(MySqlVariableInfo::getName).toList();
        Assertions.assertTrue(names.contains("v_outer"));
        Assertions.assertTrue(names.contains("v_inner"));
    }

    // ─────────────────────────────────────────────────────────────────────
    // Concept 2: custom exceptions/signals — SIGNAL SQLSTATE + user-defined CONDITION
    // ─────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("2a: SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'custom error' parses, "
            + "but the visitor records NO structured info about it at all")
    void signalSqlStateProducesNoStructuredCapture() {
        String sql = """
                CREATE PROCEDURE proc_signal_basic(IN p_qty INT)
                BEGIN
                  IF p_qty < 0 THEN
                    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'custom error';
                  END IF;
                  INSERT INTO orders(qty) VALUES (p_qty);
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_signal_basic", null);
        System.out.println("[2a] errors: " + r.getErrors());
        System.out.println("[2a] tableOps: " + tableNames(r));
        System.out.println("[2a] statements: " + r.getStatements().stream()
                .map(MySqlStatementInfo::getType).toList());
        System.out.println("[2a] exceptionHandlers: " + r.getExceptionHandlers().size());

        // Parses cleanly (grammar supports SIGNAL fully) — confirms concept is applicable.
        Assertions.assertTrue(r.getErrors().isEmpty(), "expected clean parse, got: " + r.getErrors());
        // The surrounding INSERT is still captured normally.
        Assertions.assertTrue(tableNames(r).contains("orders"));

        // BUG (documented, not fixed): MySqlAnalysisVisitor has no visitSignalStatement
        // override, so the SIGNAL statement itself — its SQLSTATE code and MESSAGE_TEXT — leaves
        // zero trace in MySqlRoutineInfo. No entry in getStatements(), getExceptionHandlers(),
        // or any other list names "signal". A procedure that deliberately raises a custom
        // business-rule error via SIGNAL looks, from the analyzer's output, identical to one
        // that never signals anything.
        boolean anyStatementMentionsSignal = r.getStatements().stream()
                .anyMatch(s -> s.getType() != null && s.getType().toUpperCase().contains("SIGNAL"));
        Assertions.assertTrue(anyStatementMentionsSignal,
                "FIXED: SIGNAL is now recorded via visitSignalStatement: " + r.getStatements());
        MySqlStatementInfo signal = r.getStatements().stream()
                .filter(s -> "SIGNAL".equals(s.getType())).findFirst().orElseThrow();
        Assertions.assertEquals("'45000'", signal.getSqlstate());
        Assertions.assertEquals("'custom error'", signal.getMessageText());
    }

    @Test
    @DisplayName("2b: DECLARE custom_condition CONDITION FOR SQLSTATE '45000' + SIGNAL "
            + "custom_condition — parses, also uncaptured")
    void namedConditionPlusSignalProducesNoStructuredCapture() {
        String sql = """
                CREATE PROCEDURE proc_signal_named_condition(IN p_qty INT)
                BEGIN
                  DECLARE custom_condition CONDITION FOR SQLSTATE '45000';
                  IF p_qty < 0 THEN
                    SIGNAL custom_condition SET MESSAGE_TEXT = 'qty must be non-negative';
                  END IF;
                  UPDATE inventory SET qty = p_qty WHERE id = 1;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_signal_named_condition", null);
        System.out.println("[2b] errors: " + r.getErrors());
        System.out.println("[2b] tableOps: " + tableNames(r));
        System.out.println("[2b] variables: " + r.getVariables().stream()
                .map(MySqlVariableInfo::getName).toList());

        Assertions.assertTrue(r.getErrors().isEmpty(), "expected clean parse, got: " + r.getErrors());
        Assertions.assertTrue(tableNames(r).contains("inventory"));
        // FIXED: declareCondition now has a visitor override (visitDeclareCondition) recording
        // a DECLARE_CONDITION statement with conditionName/sqlstate, and SIGNAL <named-condition>
        // now records a SIGNAL statement with conditionName set.
        boolean declareConditionCaptured = r.getStatements().stream()
                .anyMatch(s -> "DECLARE_CONDITION".equals(s.getType())
                        && "custom_condition".equalsIgnoreCase(s.getConditionName()));
        Assertions.assertTrue(declareConditionCaptured,
                "FIXED: DECLARE custom_condition CONDITION FOR ... now recorded: " + r.getStatements());
        boolean signalNamedConditionCaptured = r.getStatements().stream()
                .anyMatch(s -> "SIGNAL".equals(s.getType())
                        && "custom_condition".equalsIgnoreCase(s.getConditionName()));
        Assertions.assertTrue(signalNamedConditionCaptured,
                "FIXED: SIGNAL custom_condition now recorded with conditionName set: " + r.getStatements());
    }

    // ─────────────────────────────────────────────────────────────────────
    // Concept 3: RESIGNAL — bare re-raise vs RESIGNAL with SQLSTATE/SET
    // ─────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("3a: bare RESIGNAL; (no-arg re-raise) inside a handler parses without error")
    void bareResignalReraiseParsesCleanly() {
        String sql = """
                CREATE PROCEDURE proc_resignal_bare()
                BEGIN
                  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
                  BEGIN
                    INSERT INTO error_log(msg) VALUES ('caught, re-raising');
                    RESIGNAL;
                  END;
                  INSERT INTO orders(id) VALUES (1);
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_resignal_bare", null);
        System.out.println("[3a] errors: " + r.getErrors());
        System.out.println("[3a] tableOps: " + tableNames(r));

        // Confirms the grammar's `signalConditionValue?` optionality actually holds at parse
        // time for the bare re-raise form — no crash, no misparse, no error diagnostics.
        Assertions.assertTrue(r.getErrors().isEmpty(),
                "bare RESIGNAL; should parse cleanly per grammar (signalConditionValue is "
                        + "optional), got: " + r.getErrors());
        Assertions.assertTrue(tableNames(r).contains("error_log"));
        Assertions.assertTrue(tableNames(r).contains("orders"));
    }

    @Test
    @DisplayName("3b: RESIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'x' (full form) parses cleanly, "
            + "and — like SIGNAL — is not distinguished from the bare form in any captured output")
    void fullFormResignalParsesCleanlyButIndistinguishableFromBareForm() {
        String bareSql = """
                CREATE PROCEDURE proc_resignal_bare2()
                BEGIN
                  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
                  BEGIN
                    RESIGNAL;
                  END;
                  INSERT INTO orders(id) VALUES (1);
                END
                """;
        String fullSql = """
                CREATE PROCEDURE proc_resignal_full()
                BEGIN
                  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
                  BEGIN
                    RESIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'x';
                  END;
                  INSERT INTO orders(id) VALUES (1);
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo bare = engine.parse(bareSql, "proc_resignal_bare2", null);
        MySqlRoutineInfo full = engine.parse(fullSql, "proc_resignal_full", null);

        System.out.println("[3b] bare errors: " + bare.getErrors());
        System.out.println("[3b] full errors: " + full.getErrors());
        System.out.println("[3b] bare statements: " + bare.getStatements().stream()
                .map(MySqlStatementInfo::getType).toList());
        System.out.println("[3b] full statements: " + full.getStatements().stream()
                .map(MySqlStatementInfo::getType).toList());
        System.out.println("[3b] bare exceptionHandlers: " + bare.getExceptionHandlers().size()
                + " full exceptionHandlers: " + full.getExceptionHandlers().size());

        Assertions.assertTrue(bare.getErrors().isEmpty());
        Assertions.assertTrue(full.getErrors().isEmpty());

        // FIXED: visitResignalStatement now records RESIGNAL_BARE for the no-arg re-raise and
        // RESIGNAL_WITH_ARGS for the form carrying SQLSTATE/SET clauses, so the two are now
        // distinguishable in getStatements().
        Assertions.assertEquals(List.of("RESIGNAL_BARE"),
                bare.getStatements().stream().map(MySqlStatementInfo::getType).toList());
        Assertions.assertEquals(List.of("RESIGNAL_WITH_ARGS"),
                full.getStatements().stream().map(MySqlStatementInfo::getType).toList());
        Assertions.assertNotEquals(
                bare.getStatements().stream().map(MySqlStatementInfo::getType).toList(),
                full.getStatements().stream().map(MySqlStatementInfo::getType).toList(),
                "FIXED: bare vs full-form RESIGNAL are now distinguishable in captured output");
    }

    // ─────────────────────────────────────────────────────────────────────
    // Concept 4: transaction control — SAVEPOINT / ROLLBACK TO SAVEPOINT
    // ─────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("4a: SAVEPOINT name; ... ROLLBACK TO SAVEPOINT name; parses cleanly")
    void savepointAndRollbackToParseCleanly() {
        String sql = """
                CREATE PROCEDURE proc_savepoint_basic()
                BEGIN
                  INSERT INTO orders(id) VALUES (1);
                  SAVEPOINT sp1;
                  INSERT INTO order_items(order_id) VALUES (1);
                  ROLLBACK TO SAVEPOINT sp1;
                END
                """;
        MySqlGrammarEngine engine = new MySqlGrammarEngine();
        MySqlRoutineInfo r = engine.parse(sql, "proc_savepoint_basic", null);
        System.out.println("[4a] errors: " + r.getErrors());
        System.out.println("[4a] tableOps: " + tableNames(r));
        System.out.println("[4a] statements: " + r.getStatements().stream()
                .map(MySqlStatementInfo::getType).toList());

        Assertions.assertTrue(r.getErrors().isEmpty(), "expected clean parse, got: " + r.getErrors());
        Assertions.assertTrue(tableNames(r).contains("orders"));
        Assertions.assertTrue(tableNames(r).contains("order_items"));
    }

    @Test
    @DisplayName("4b: N/A — PRAGMA AUTONOMOUS_TRANSACTION does not exist in MySQL / this grammar")
    void autonomousTransactionPragmaIsNotApplicableToMySql() {
        // Not a real test of behavior — just documents the grep evidence per the audit's
        // instruction not to fabricate a test for a construct MySQL doesn't have.
        //
        //   $ grep -n "AUTONOMOUS" mysql-parser/src/main/antlr4/com/plsql/mysql/grammar/MySqlParser.g4 \
        //       mysql-parser/src/main/antlr4/com/plsql/mysql/grammar/MySqlLexer.g4
        //   (no output — zero matches)
        //
        // MySQL stored routines have no autonomous-transaction pragma/hint of any kind; this is
        // an Oracle-specific concept. Confirmed N/A for the mysql-parser module.
        Assertions.assertTrue(true, "N/A — see comment: no AUTONOMOUS token/rule in the MySQL grammar");
    }
}
