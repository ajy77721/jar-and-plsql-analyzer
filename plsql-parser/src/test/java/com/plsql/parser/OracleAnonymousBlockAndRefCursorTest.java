package com.plsql.parser;

import com.plsql.parser.model.ParseResult;
import com.plsql.parser.model.ParameterInfo;
import com.plsql.parser.model.ParsedObject;
import com.plsql.parser.model.SubprogramInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tasks #25 + #26, Oracle side.
 *
 * <p>#25: a top-level {@code DECLARE … BEGIN … END;} script section parses via the grammar's
 * {@code anonymous_block} rule but used to produce no {@link ParsedObject} — every helper was a
 * no-op with an empty context stack, so the whole section was silently dropped. It is now an ANON
 * unit whose table ops/calls are captured like any other unit.
 *
 * <p>#26: an OUT REF CURSOR parameter is Oracle's result-set hand-back; the parameter is flagged and
 * the owning unit marked.
 */
class OracleAnonymousBlockAndRefCursorTest extends ParserTestBase {

    private static ParsedObject onlyObject(ParseResult r, String type) {
        for (ParsedObject o : r.getObjects()) {
            if (type.equals(o.getType())) return o;
        }
        return null;
    }

    // ── #25 anonymous block ────────────────────────────────────────────────────

    @Test
    @DisplayName("#25 DECLARE…BEGIN…END script section becomes an ANON unit with its table ops")
    void declareBlockBecomesAnonUnit() {
        String sql = """
                DECLARE
                  v NUMBER := 1;
                BEGIN
                  INSERT INTO audit_log(id, msg) VALUES (v, 'hi');
                END;
                /
                """;
        ParseResult r = parse(sql);
        assertNoParsErrors(r, "anonymous DECLARE block");

        ParsedObject anon = onlyObject(r, "ANON");
        assertNotNull(anon, "the anonymous block must become an analyzable ANON unit, not be dropped");
        assertTrue(anon.getName() != null && anon.getName().startsWith("ANON_"),
                "a synthetic line-qualified name keeps several blocks distinct, was: " + anon.getName());
        assertTrue(anon.getTableOperations().stream().anyMatch(t ->
                        "AUDIT_LOG".equalsIgnoreCase(t.getTableName())
                                && "INSERT".equalsIgnoreCase(t.getOperation())),
                "the block's INSERT must be captured like any unit's table op");
    }

    @Test
    @DisplayName("#25 a bare BEGIN…END block (no DECLARE) is also an ANON unit")
    void bareBeginBlockBecomesAnonUnit() {
        String sql = """
                BEGIN
                  UPDATE orders SET status = 'X' WHERE id = 1;
                END;
                /
                """;
        ParseResult r = parse(sql);
        assertNoParsErrors(r, "bare BEGIN block");
        ParsedObject anon = onlyObject(r, "ANON");
        assertNotNull(anon, "a bare BEGIN…END block must not be silently dropped");
        assertTrue(anon.getTableOperations().stream().anyMatch(t ->
                        "ORDERS".equalsIgnoreCase(t.getTableName())),
                "the block's UPDATE target must be captured");
    }

    // ── #26 OUT REF CURSOR ─────────────────────────────────────────────────────

    @Test
    @DisplayName("#26 standalone proc with OUT SYS_REFCURSOR is a result-set-returning unit")
    void standaloneProcOutRefCursorIsResultSet() {
        String sql = """
                CREATE OR REPLACE PROCEDURE get_rows(p_cur OUT SYS_REFCURSOR) AS
                BEGIN
                  OPEN p_cur FOR SELECT id FROM orders;
                END;
                /
                """;
        ParseResult r = parse(sql);
        assertNoParsErrors(r, "OUT SYS_REFCURSOR proc");

        ParsedObject proc = onlyObject(r, "PROCEDURE");
        assertNotNull(proc);
        assertTrue(proc.isReturnsResultSet(), "an OUT REF CURSOR hands a result set back");
        assertEquals(1, proc.getResultSets());

        ParameterInfo p = proc.getParameters().stream()
                .filter(x -> "P_CUR".equalsIgnoreCase(x.getName())).findFirst().orElse(null);
        assertNotNull(p);
        assertTrue(p.isRefCursor(), "SYS_REFCURSOR must be recognised as a ref cursor");
        assertTrue(p.isOutRefCursor(), "an OUT ref cursor is the result-set hand-back");
    }

    @Test
    @DisplayName("#26 package member with OUT SYS_REFCURSOR is marked on the SubprogramInfo")
    void packageMemberOutRefCursorIsResultSet() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY my_pkg AS
                  PROCEDURE fetch_all(p_cur OUT SYS_REFCURSOR) IS
                  BEGIN
                    OPEN p_cur FOR SELECT id FROM orders;
                  END fetch_all;
                END my_pkg;
                /
                """;
        ParseResult r = parse(sql);
        assertNoParsErrors(r, "package member OUT ref cursor");

        SubprogramInfo sub = findSub(r, "fetch_all");
        assertNotNull(sub);
        assertTrue(sub.isReturnsResultSet());
        assertEquals(1, sub.getResultSets());
        assertTrue(sub.getParameters().get(0).isOutRefCursor());
    }

    @Test
    @DisplayName("#26 a plain IN parameter is neither a ref cursor nor a result set")
    void plainParameterIsNotRefCursor() {
        String sql = """
                CREATE OR REPLACE PROCEDURE do_work(p_id NUMBER) AS
                BEGIN
                  NULL;
                END;
                /
                """;
        ParseResult r = parse(sql);
        ParsedObject proc = onlyObject(r, "PROCEDURE");
        assertNotNull(proc);
        assertFalse(proc.isReturnsResultSet());
        assertEquals(0, proc.getResultSets());
        assertFalse(proc.getParameters().get(0).isRefCursor());
    }
}
