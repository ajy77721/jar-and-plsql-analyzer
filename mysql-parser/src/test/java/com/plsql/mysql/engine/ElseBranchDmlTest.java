package com.plsql.mysql.engine;

import com.plsql.mysql.model.MySqlRoutineInfo;
import com.plsql.mysql.model.MySqlTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DML inside ELSE and ELSEIF branches must survive.
 *
 * <p>The catch-all added for TRUNCATE ({@code unknownStatement}) originally excluded only
 * {@code SEMI}, {@code BEGIN} and {@code END}. Block-continuation keywords never start a statement,
 * so an {@code ELSEIF} that the {@code ifStatement} rule failed to consume was swallowed whole by
 * the catch-all — <b>along with every statement inside that branch</b>.
 *
 * <p>Measured on the real MySQL analysis: <b>160</b> skipped statements, 94 {@code ELSE} and 66
 * {@code ELSEIF}, each carrying an {@code INSERT INTO erp_audit_log} that vanished from the result.
 * That is worse than the whole-body failure the catch-all was added to prevent, because a lost
 * branch still looks like a successful parse.
 *
 * <p>DB2's {@code otherStatement} has carried the same exclusion list for the same reason, citing
 * its own reserve_stock bug; the MySQL rule was written without it.
 */
class ElseBranchDmlTest {

    private static final MySqlGrammarEngine ENGINE = new MySqlGrammarEngine();

    private Set<String> tablesOf(MySqlRoutineInfo r) {
        return r.getTableOps().stream()
                .map(o -> o.getTableName().toLowerCase())
                .collect(Collectors.toSet());
    }

    @Test
    @DisplayName("an INSERT in an ELSE or ELSEIF branch is not lost")
    void elseBranchDmlSurvives() {
        MySqlRoutineInfo r = ENGINE.parse("""
                CREATE PROCEDURE p()
                BEGIN
                  IF v_x > 0 THEN
                    INSERT INTO t_then (id) VALUES (1);
                  ELSEIF v_y > 0 THEN
                    INSERT INTO t_elseif (id) VALUES (2);
                  ELSE
                    INSERT INTO t_else (id) VALUES (3);
                  END IF;
                END""", "P", "db");

        Set<String> tables = tablesOf(r);
        assertTrue(tables.contains("t_then"),   "THEN branch lost: " + tables);
        assertTrue(tables.contains("t_elseif"), "ELSEIF branch lost: " + tables);
        assertTrue(tables.contains("t_else"),   "ELSE branch lost: " + tables);
    }

    @Test
    @DisplayName("no ELSE/ELSEIF is reported as an unsupported statement")
    void branchesAreNotSkipped() {
        MySqlRoutineInfo r = ENGINE.parse("""
                CREATE PROCEDURE p()
                BEGIN
                  IF a = 1 THEN
                    UPDATE audit SET n = n + 1;
                  ELSEIF a = 2 THEN
                    UPDATE audit SET n = n + 2;
                  ELSE
                    UPDATE audit SET n = n + 3;
                  END IF;
                END""", "P", "db");

        // The catch-all must never claim a branch keyword as its own statement.
        String skipped = String.join(" | ", r.getErrors());
        assertFalse(skipped.toUpperCase().contains("ELSEIF"),
                "an ELSEIF branch was swallowed by the catch-all: " + skipped);
        assertFalse(skipped.toUpperCase().contains("ELSE\n") || skipped.toUpperCase().contains("— ELSE"),
                "an ELSE branch was swallowed by the catch-all: " + skipped);
    }

    @Test
    @DisplayName("the TRUNCATE catch-all still works — the reason the rule exists")
    void catchAllStillSkipsUnknownStatements() {
        MySqlRoutineInfo r = ENGINE.parse("""
                CREATE PROCEDURE p()
                BEGIN
                  GRANT SELECT ON staging TO some_role;
                  INSERT INTO staging (id) VALUES (1);
                END""", "P", "db");
        assertTrue(tablesOf(r).contains("staging"),
                "the neighbouring INSERT must survive an unknown statement: " + tablesOf(r));
    }
}
