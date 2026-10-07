package com.plsql.mysql;

import com.plsql.mysql.engine.MySqlGrammarEngine;
import com.plsql.mysql.model.MySqlRoutineInfo;
import com.plsql.mysql.model.MySqlStatementInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Enrichment coverage for MySQL transaction-statement capture: guard (IF/CASE/loop context),
 * handler (DECLARE ... HANDLER body context), sqlText, and the newly-wired
 * {@code START_TRANSACTION} (txnStart) / {@code RELEASE_SAVEPOINT} (txnReleaseSavepoint) rows.
 *
 * <p>{@code isPartial} is NOT a model field: the writer derives it from a ROLLBACK row that
 * carries a savepoint name, so the ROLLBACK-TO-SAVEPOINT tests here assert the inputs the writer
 * uses ({@code type=ROLLBACK} + {@code savepointName}).
 */
class MySqlTransactionEnrichmentTest {

    private final MySqlGrammarEngine engine = new MySqlGrammarEngine();

    private MySqlRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    private static MySqlStatementInfo first(MySqlRoutineInfo r, String type) {
        return r.getStatements().stream()
                .filter(s -> type.equals(s.getType())).findFirst().orElseThrow();
    }

    @Test
    @DisplayName("IF-conditional COMMIT carries guard {kind:IF, line, condition} + sqlText")
    void ifConditionalCommitCarriesGuard() {
        MySqlRoutineInfo r = parse(
                "CREATE PROCEDURE p_guard()\n"
              + "BEGIN\n"
              + "  DECLARE v_mode VARCHAR(20);\n"
              + "  IF v_mode = 'FULL' THEN\n"
              + "    COMMIT;\n"
              + "  END IF;\n"
              + "END");
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        MySqlStatementInfo commit = first(r, "COMMIT");
        assertEquals("IF", commit.getGuardKind());
        assertNotNull(commit.getGuardLine());
        assertTrue(commit.getGuardCondition().contains("v_mode = 'FULL'"),
                "guard condition: " + commit.getGuardCondition());
        assertNotNull(commit.getSqlText());
        assertTrue(commit.getSqlText().length() <= 300, "sqlText capped at 300");
    }

    @Test
    @DisplayName("ROLLBACK TO SAVEPOINT inside DECLARE ... HANDLER carries handler + savepoint "
            + "(the inputs the writer uses to derive isPartial)")
    void rollbackToSavepointInsideHandler() {
        MySqlRoutineInfo r = parse(
                "CREATE PROCEDURE p_handler()\n"
              + "BEGIN\n"
              + "  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION\n"
              + "  BEGIN\n"
              + "    ROLLBACK TO SAVEPOINT sp1;\n"
              + "  END;\n"
              + "END");
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        MySqlStatementInfo rb = r.getStatements().stream()
                .filter(s -> "ROLLBACK".equals(s.getType()) && "sp1".equals(s.getSavepointName()))
                .findFirst().orElseThrow();
        assertEquals("SQLEXCEPTION", rb.getHandlerName());
        assertNotNull(rb.getHandlerLine());
        assertNotNull(rb.getSavepointName(), "savepointName is the isPartial input");
    }

    @Test
    @DisplayName("START_TRANSACTION (txnStart) and RELEASE_SAVEPOINT (txnReleaseSavepoint) are emitted")
    void startTransactionAndReleaseSavepoint() {
        MySqlRoutineInfo r = parse(
                "CREATE PROCEDURE p_new_txn()\n"
              + "BEGIN\n"
              + "  START TRANSACTION;\n"
              + "  SAVEPOINT sp2;\n"
              + "  RELEASE SAVEPOINT sp2;\n"
              + "END");
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        MySqlStatementInfo start = first(r, "START_TRANSACTION");
        assertTrue(start.getLine() > 0);
        assertNotNull(start.getSqlText());
        MySqlStatementInfo release = first(r, "RELEASE_SAVEPOINT");
        assertEquals("sp2", release.getSavepointName());
        assertNotNull(release.getSqlText());
    }

    @Test
    @DisplayName("CASE branch guard: per-WHEN condition for the WHEN body, \"<case>\" for ELSE")
    void caseBranchGuard() {
        MySqlRoutineInfo r = parse(
                "CREATE PROCEDURE p_case()\n"
              + "BEGIN\n"
              + "  DECLARE v_x INT;\n"
              + "  CASE v_x\n"
              + "    WHEN 1 THEN\n"
              + "      COMMIT;\n"
              + "    ELSE\n"
              + "      ROLLBACK;\n"
              + "  END CASE;\n"
              + "END");
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        MySqlStatementInfo commit = first(r, "COMMIT");
        assertEquals("CASE", commit.getGuardKind());
        assertNotNull(commit.getGuardCondition());
        MySqlStatementInfo rollback = first(r, "ROLLBACK");
        assertEquals("CASE", rollback.getGuardKind());
        assertEquals("<case>", rollback.getGuardCondition());
    }
}
