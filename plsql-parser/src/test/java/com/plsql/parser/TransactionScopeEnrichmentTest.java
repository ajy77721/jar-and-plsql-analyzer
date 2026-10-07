package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Visitor-side proof for the transaction-scope enrichment: transaction statements carry the
 * innermost branch/loop guard, the enclosing exception handler, and their own SQL text; and a
 * {@code SELECT ... FOR UPDATE} is flagged rather than given a new statement type.
 *
 * <p>Additive contract: the existing {@code type}/{@code line}/{@code savepointName} capture is
 * unchanged — these tests only assert the NEW fields.
 */
public class TransactionScopeEnrichmentTest extends ParserTestBase {

    private static final String FIXTURE = """
            CREATE OR REPLACE PACKAGE BODY txn_pkg AS
              PROCEDURE do_txn IS
              BEGIN
                COMMIT;
                IF v_flag = 1 THEN
                  COMMIT;
                END IF;
                FOR i IN 1..10 LOOP
                  SAVEPOINT loop_sp;
                  LOCK TABLE accounts IN EXCLUSIVE MODE;
                END LOOP;
                SET TRANSACTION READ ONLY;
              EXCEPTION
                WHEN OTHERS THEN
                  ROLLBACK TO SAVEPOINT before_update;
                  COMMIT;
              END do_txn;
            END txn_pkg;
            /""";

    private StatementInfo find(List<StatementInfo> stmts, String type, int line) {
        for (StatementInfo s : stmts) {
            if (type.equals(s.getType()) && s.getLine() == line) return s;
        }
        return null;
    }

    @Test
    @DisplayName("unconditional COMMIT: no guard, no handler, carries sqlText")
    void unconditionalCommit() {
        ParseResult r = parse(FIXTURE);
        assertNoParsErrors(r, "txn fixture");
        SubprogramInfo proc = findSub(r, "DO_TXN");
        assertNotNull(proc);

        StatementInfo c = find(proc.getStatements(), "COMMIT", 4);
        assertNotNull(c, "line-4 COMMIT must be recorded");
        assertNull(c.getGuardKind(), "a top-level COMMIT sits inside no guard");
        assertNull(c.getHandlerName(), "a top-level COMMIT sits inside no handler");
        assertNotNull(c.getSqlText(), "COMMIT must now carry its own sqlText");
        assertTrue(c.getSqlText().toUpperCase().startsWith("COMMIT"));
    }

    @Test
    @DisplayName("COMMIT inside IF carries {kind=IF, line, condition}")
    void commitInsideIf() {
        ParseResult r = parse(FIXTURE);
        assertNoParsErrors(r, "txn fixture");
        SubprogramInfo proc = findSub(r, "DO_TXN");
        assertNotNull(proc);

        StatementInfo c = find(proc.getStatements(), "COMMIT", 6);
        assertNotNull(c, "line-6 COMMIT (inside the IF) must be recorded");
        assertEquals("IF", c.getGuardKind());
        assertEquals(5, c.getGuardLine(), "guard line is the IF's own start line");
        assertEquals("v_flag = 1", c.getGuardCondition());
        assertNull(c.getHandlerName());
    }

    @Test
    @DisplayName("SAVEPOINT / LOCK TABLE inside FOR loop carry {kind=LOOP, line, bounds}")
    void loopGuardedStatements() {
        ParseResult r = parse(FIXTURE);
        assertNoParsErrors(r, "txn fixture");
        SubprogramInfo proc = findSub(r, "DO_TXN");
        assertNotNull(proc);

        StatementInfo sp = find(proc.getStatements(), "SAVEPOINT", 9);
        assertNotNull(sp, "SAVEPOINT inside the loop must be recorded");
        assertEquals("LOOP", sp.getGuardKind());
        assertEquals(8, sp.getGuardLine(), "guard line is the FOR loop's own start line");
        assertTrue(sp.getGuardCondition().contains("1..10"), "loop guard condition is the loop bounds");
        assertEquals("LOOP_SP", sp.getSavepointName(), "existing savepoint-name capture is unchanged");

        StatementInfo lock = find(proc.getStatements(), "LOCK_TABLE", 10);
        assertNotNull(lock, "LOCK TABLE must be recorded as a LOCK_TABLE statement");
        assertEquals("LOOP", lock.getGuardKind());
        assertEquals(8, lock.getGuardLine());
        assertNotNull(lock.getSqlText());
        assertTrue(lock.getSqlText().toUpperCase().startsWith("LOCK TABLE"));
    }

    @Test
    @DisplayName("SET TRANSACTION is recorded as its own type with sqlText")
    void setTransactionStatement() {
        ParseResult r = parse(FIXTURE);
        assertNoParsErrors(r, "txn fixture");
        SubprogramInfo proc = findSub(r, "DO_TXN");
        assertNotNull(proc);

        StatementInfo set = find(proc.getStatements(), "SET_TRANSACTION", 12);
        assertNotNull(set, "SET TRANSACTION must be recorded");
        assertNull(set.getGuardKind());
        assertNull(set.getHandlerName());
        assertNotNull(set.getSqlText());
        assertTrue(set.getSqlText().toUpperCase().startsWith("SET TRANSACTION"));
    }

    @Test
    @DisplayName("ROLLBACK TO SAVEPOINT inside WHEN OTHERS carries the handler linkage")
    void rollbackInsideHandler() {
        ParseResult r = parse(FIXTURE);
        assertNoParsErrors(r, "txn fixture");
        SubprogramInfo proc = findSub(r, "DO_TXN");
        assertNotNull(proc);

        StatementInfo rb = find(proc.getStatements(), "ROLLBACK", 15);
        assertNotNull(rb, "ROLLBACK inside the handler must be recorded");
        assertEquals("BEFORE_UPDATE", rb.getSavepointName(), "savepoint name capture unchanged");
        assertNull(rb.getGuardKind(), "no branch/loop guard inside the handler");
        assertEquals("OTHERS", rb.getHandlerName());
        assertEquals(14, rb.getHandlerLine(), "handler line is the WHEN clause's own line");

        StatementInfo hc = find(proc.getStatements(), "COMMIT", 16);
        assertNotNull(hc);
        assertEquals("OTHERS", hc.getHandlerName(), "handler linkage applies to every body statement");
        assertNull(hc.getGuardKind());
    }

    @Test
    @DisplayName("SELECT ... FOR UPDATE is flagged on the SELECT row, not given a new type")
    void selectForUpdateFlag() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY pkg_lock AS
                  PROCEDURE lock_rows IS
                    v NUMBER;
                  BEGIN
                    SELECT id INTO v FROM accounts WHERE id = 1 FOR UPDATE;
                    SELECT id INTO v FROM accounts WHERE id = 2;
                    SELECT id INTO v FROM accounts WHERE id = 3 FOR UPDATE NOWAIT;
                    SELECT id INTO v FROM accounts WHERE id = 4 FOR UPDATE SKIP LOCKED;
                  END lock_rows;
                END pkg_lock;
                /""";
        ParseResult r = parse(sql);
        assertNoParsErrors(r, "FOR UPDATE fixture");
        SubprogramInfo proc = findSub(r, "LOCK_ROWS");
        assertNotNull(proc);

        List<StatementInfo> sels = proc.getStatements().stream()
                .filter(s -> "SELECT".equals(s.getType())).toList();
        assertEquals(4, sels.size(), "each SELECT recorded once, all still type SELECT");
        assertTrue(Boolean.TRUE.equals(sels.get(0).getForUpdate()), "FOR UPDATE sets forUpdate");
        assertNull(sels.get(1).getForUpdate(), "plain SELECT leaves forUpdate unset (omitted)");
        assertTrue(Boolean.TRUE.equals(sels.get(2).getForUpdate()), "FOR UPDATE NOWAIT sets forUpdate");
        assertTrue(Boolean.TRUE.equals(sels.get(3).getForUpdate()), "FOR UPDATE SKIP LOCKED sets forUpdate");
    }

    @Test
    @DisplayName("handler linkage is restored after a nested handler / nested block")
    void handlerRestoredAfterNestedHandler() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY pkg_nest AS
                  PROCEDURE nested_handlers IS
                  BEGIN
                    NULL;
                  EXCEPTION
                    WHEN OTHERS THEN
                      BEGIN
                        COMMIT;
                      EXCEPTION
                        WHEN ZERO_DIVIDE THEN
                          COMMIT;
                      END;
                      COMMIT;
                  END nested_handlers;
                END pkg_nest;
                /""";
        ParseResult r = parse(sql);
        assertNoParsErrors(r, "nested handlers fixture");
        SubprogramInfo proc = findSub(r, "NESTED_HANDLERS");
        assertNotNull(proc);

        List<StatementInfo> commits = proc.getStatements().stream()
                .filter(s -> "COMMIT".equals(s.getType())).toList();
        assertEquals(3, commits.size());
        assertEquals("OTHERS", commits.get(0).getHandlerName(), "outer handler body");
        assertEquals("ZERO_DIVIDE", commits.get(1).getHandlerName(), "inner handler body");
        assertEquals("OTHERS", commits.get(2).getHandlerName(),
                "after the nested handler, the outer handler linkage is restored");
    }

    @Test
    @DisplayName("CASE guard: each WHEN branch refines the condition; ELSE falls back to <case>")
    void caseGuardConditionPerBranch() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY pkg_case AS
                  PROCEDURE do_case IS
                  BEGIN
                    CASE v_kind
                      WHEN 1 THEN
                        COMMIT;
                      WHEN 2 THEN
                        COMMIT;
                      ELSE
                        COMMIT;
                    END CASE;
                  END do_case;
                END pkg_case;
                /""";
        ParseResult r = parse(sql);
        assertNoParsErrors(r, "CASE fixture");
        SubprogramInfo proc = findSub(r, "DO_CASE");
        assertNotNull(proc);

        List<StatementInfo> commits = proc.getStatements().stream()
                .filter(s -> "COMMIT".equals(s.getType())).toList();
        assertEquals(3, commits.size());
        for (StatementInfo c : commits) {
            assertEquals("CASE", c.getGuardKind());
            assertEquals(4, c.getGuardLine(), "guard line is the CASE's own start line");
        }
        assertEquals("1", commits.get(0).getGuardCondition(), "WHEN 1 branch condition");
        assertEquals("2", commits.get(1).getGuardCondition(), "WHEN 2 branch condition");
        assertEquals("<case>", commits.get(2).getGuardCondition(), "ELSE falls back to <case>");
    }

    @Test
    @DisplayName("handler linkage does not leak past the block that owns the handler")
    void handlerDoesNotLeakPastBlock() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY pkg_leak AS
                  PROCEDURE after_block IS
                  BEGIN
                    BEGIN
                      COMMIT;
                    EXCEPTION
                      WHEN OTHERS THEN
                        COMMIT;
                    END;
                    COMMIT;
                  END after_block;
                END pkg_leak;
                /""";
        ParseResult r = parse(sql);
        assertNoParsErrors(r, "handler leak fixture");
        SubprogramInfo proc = findSub(r, "AFTER_BLOCK");
        assertNotNull(proc);

        List<StatementInfo> commits = proc.getStatements().stream()
                .filter(s -> "COMMIT".equals(s.getType())).toList();
        assertEquals(3, commits.size());
        assertNull(commits.get(0).getHandlerName(), "statement in the protected block: no handler");
        assertEquals("OTHERS", commits.get(1).getHandlerName(), "statement in the handler body");
        assertNull(commits.get(2).getHandlerName(),
                "after END; the handler linkage must be gone — it did not leak");
    }
}
