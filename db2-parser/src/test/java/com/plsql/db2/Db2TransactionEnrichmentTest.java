package com.plsql.db2;

import com.plsql.db2.engine.Db2AnalysisEngine;
import com.plsql.db2.model.Db2RoutineInfo;
import com.plsql.db2.model.Db2StatementInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Enrichment coverage for DB2 transaction-statement capture: guard (IF/CASE/loop context),
 * handler (DECLARE ... HANDLER FOR ... body context), sqlText, and the newly-wired
 * {@code RELEASE_SAVEPOINT} (txnReleaseSavepoint) row.
 *
 * <p>{@code isPartial} is derived by the writer from a ROLLBACK row carrying a savepoint name, so
 * the ROLLBACK-TO-SAVEPOINT tests assert those inputs.
 */
class Db2TransactionEnrichmentTest {

    private final Db2AnalysisEngine engine = new Db2AnalysisEngine();

    private Db2RoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    private static Db2StatementInfo first(Db2RoutineInfo r, String type) {
        return r.getStatements().stream()
                .filter(s -> type.equals(s.getType())).findFirst().orElseThrow();
    }

    @Test
    @DisplayName("IF-conditional COMMIT carries guard {kind:IF, line, condition} + sqlText")
    void ifConditionalCommitCarriesGuard() {
        Db2RoutineInfo r = parse(
                "CREATE PROCEDURE p_guard()\nLANGUAGE SQL\nBEGIN\n"
              + "  DECLARE v_mode VARCHAR(20);\n"
              + "  IF v_mode = 'FULL' THEN\n"
              + "    COMMIT;\n"
              + "  END IF;\n"
              + "END");
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        Db2StatementInfo commit = first(r, "COMMIT");
        assertEquals("IF", commit.getGuardKind());
        assertNotNull(commit.getGuardLine());
        assertTrue(commit.getGuardCondition().contains("v_mode = 'FULL'"),
                "guard condition: " + commit.getGuardCondition());
        assertNotNull(commit.getSqlText());
        assertTrue(commit.getSqlText().length() <= 300);
    }

    @Test
    @DisplayName("ROLLBACK TO SAVEPOINT inside DECLARE ... HANDLER body carries handler + savepoint")
    void rollbackToSavepointInsideHandler() {
        Db2RoutineInfo r = parse(
                "CREATE PROCEDURE p_handler()\nLANGUAGE SQL\nBEGIN\n"
              + "  DECLARE EXIT HANDLER FOR SQLEXCEPTION\n"
              + "  BEGIN\n"
              + "    ROLLBACK TO SAVEPOINT sp1;\n"
              + "  END;\n"
              + "END");
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        Db2StatementInfo rb = r.getStatements().stream()
                .filter(s -> "ROLLBACK".equals(s.getType()) && "sp1".equals(s.getSavepointName()))
                .findFirst().orElseThrow();
        assertEquals("SQLEXCEPTION", rb.getHandlerName());
        assertNotNull(rb.getHandlerLine());
        assertNotNull(rb.getSavepointName(), "savepointName is the isPartial input");
    }

    @Test
    @DisplayName("RELEASE TO SAVEPOINT (txnReleaseSavepoint) is emitted with its name")
    void releaseSavepointEmitted() {
        Db2RoutineInfo r = parse(
                "CREATE PROCEDURE p_release()\nLANGUAGE SQL\nBEGIN\n"
              + "  SAVEPOINT sp2;\n"
              + "  RELEASE TO SAVEPOINT sp2;\n"
              + "END");
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        Db2StatementInfo rel = first(r, "RELEASE_SAVEPOINT");
        assertEquals("sp2", rel.getSavepointName());
        assertNotNull(rel.getSqlText());
    }

    @Test
    @DisplayName("CASE WHEN branch guard applied to nested COMMIT")
    void caseBranchGuard() {
        Db2RoutineInfo r = parse(
                "CREATE PROCEDURE p_case()\nLANGUAGE SQL\nBEGIN\n"
              + "  DECLARE v_x INTEGER DEFAULT 0;\n"
              + "  CASE v_x\n"
              + "    WHEN 1 THEN\n"
              + "      COMMIT;\n"
              + "    ELSE\n"
              + "      ROLLBACK;\n"
              + "  END CASE;\n"
              + "END");
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        Db2StatementInfo commit = first(r, "COMMIT");
        assertEquals("CASE", commit.getGuardKind());
        assertNotNull(commit.getGuardCondition());
    }
}
