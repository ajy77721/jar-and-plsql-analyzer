package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.PostgresRoutineInfo;
import com.plsql.postgres.model.PostgresStatementInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Enrichment coverage for Postgres transaction-statement capture: guard (IF/CASE/loop context),
 * handler (EXCEPTION WHEN ... THEN body context), sqlText, and the newly-wired
 * {@code RELEASE_SAVEPOINT} (txnReleaseSavepoint) row.
 *
 * <p>{@code isPartial} is derived by the writer from a ROLLBACK row carrying a savepoint name, so
 * the ROLLBACK-TO-SAVEPOINT tests assert those inputs.
 */
class PostgresTransactionEnrichmentTest {

    private final PostgresAnalysisEngine engine = new PostgresAnalysisEngine();

    private PostgresRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    @Test
    @DisplayName("IF-conditional COMMIT carries guard {kind:IF, line, condition} + sqlText")
    void ifConditionalCommitCarriesGuard() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE p_guard()\nLANGUAGE SQL\nBEGIN\n"
              + "  IF v_mode = 'FULL' THEN\n"
              + "    COMMIT;\n"
              + "  END IF;\n"
              + "END");
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        PostgresStatementInfo commit = r.getStatements().stream()
                .filter(s -> "COMMIT".equals(s.getType())).findFirst().orElseThrow();
        assertEquals("IF", commit.getGuardKind());
        assertNotNull(commit.getGuardLine());
        assertTrue(commit.getGuardCondition().contains("v_mode = 'FULL'"),
                "guard condition: " + commit.getGuardCondition());
        assertNotNull(commit.getSqlText());
        assertTrue(commit.getSqlText().length() <= 300);
    }

    @Test
    @DisplayName("ROLLBACK TO SAVEPOINT inside EXCEPTION WHEN OTHERS THEN carries handler + savepoint")
    void rollbackToSavepointInsideHandler() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE p_handler()\nLANGUAGE SQL\nBEGIN\n"
              + "  SAVEPOINT sp1;\n"
              + "EXCEPTION WHEN OTHERS THEN\n"
              + "  ROLLBACK TO SAVEPOINT sp1;\n"
              + "END");
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        PostgresStatementInfo rb = r.getStatements().stream()
                .filter(s -> "ROLLBACK".equals(s.getType()) && "sp1".equals(s.getSavepointName()))
                .findFirst().orElseThrow();
        assertEquals("OTHERS", rb.getHandlerName());
        assertNotNull(rb.getHandlerLine());
        assertNotNull(rb.getSavepointName(), "savepointName is the isPartial input");
    }

    @Test
    @DisplayName("RELEASE SAVEPOINT (txnReleaseSavepoint) is emitted with its name")
    void releaseSavepointEmitted() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE p_release()\nLANGUAGE SQL\nBEGIN\n"
              + "  SAVEPOINT sp2;\n"
              + "  RELEASE SAVEPOINT sp2;\n"
              + "END");
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        PostgresStatementInfo rel = r.getStatements().stream()
                .filter(s -> "RELEASE_SAVEPOINT".equals(s.getType())).findFirst().orElseThrow();
        assertEquals("sp2", rel.getSavepointName());
        assertNotNull(rel.getSqlText());
    }

    @Test
    @DisplayName("WHILE loop body guard is {kind:LOOP}")
    void whileLoopGuard() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE p_loop()\nLANGUAGE SQL\nBEGIN\n"
              + "  WHILE v_n < 10 LOOP\n"
              + "    COMMIT;\n"
              + "  END LOOP;\n"
              + "END");
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        PostgresStatementInfo commit = r.getStatements().stream()
                .filter(s -> "COMMIT".equals(s.getType())).findFirst().orElseThrow();
        assertEquals("LOOP", commit.getGuardKind());
        assertTrue(commit.getGuardCondition().contains("v_n < 10"),
                "guard condition: " + commit.getGuardCondition());
    }
}
