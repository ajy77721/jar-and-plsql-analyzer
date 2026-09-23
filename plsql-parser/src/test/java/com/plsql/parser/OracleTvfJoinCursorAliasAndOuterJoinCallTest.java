package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for four defects found while auditing obscure Oracle writing styles:
 *
 * <ol>
 *   <li>{@code JOIN TABLE(pkg.fn(...)) t ON ...} in a plain SELECT produced a JoinInfo with
 *       {@code joinedTable == null} (the cursor path already fell back to the alias).</li>
 *   <li>Table ops derived from CURSOR declarations always had {@code alias == null}.</li>
 *   <li>Old-style outer-join markers ({@code b.acct_id(+)}) were mis-detected by the
 *       SQL-embedded-call regex as a call to package B's function ACCT_ID.</li>
 *   <li>A locally declared collection type used as a constructor ({@code NESTED_T()}) was
 *       recorded as a phantom EXTERNAL call/dependency.</li>
 * </ol>
 */
@DisplayName("TVF joins, cursor aliases, (+) markers and local type constructors")
public class OracleTvfJoinCursorAliasAndOuterJoinCallTest extends ParserTestBase {

    @Test
    @DisplayName("JOIN TABLE(pkg.fn(...)) t names the join target by its alias, exactly once")
    void tvfJoinUsesAliasAsJoinedTable() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC (p_id NUMBER) IS
                    v1 VARCHAR2(100);
                  BEGIN
                    SELECT t.col1 INTO v1
                      FROM ORDERS o
                      JOIN TABLE(PKG_UTILS.GET_LINE_ITEMS(o.order_id)) t
                        ON t.some_col = o.some_col
                     WHERE o.order_id = p_id;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "TVF join");

        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);

        TableOperationInfo orders = proc.getTableOperations().stream()
                .filter(t -> "ORDERS".equalsIgnoreCase(t.getTableName()))
                .findFirst().orElseThrow();
        assertEquals(1, orders.getJoins().size());
        JoinInfo ji = orders.getJoins().get(0);

        // The TVF result has no table name; the alias is the only stable identifier.
        assertEquals("T", ji.getJoinedTable());
        assertEquals("T", ji.getJoinedTableAlias());

        // The alias must NOT leak into the table-operation model as if it were a real table.
        assertTrue(proc.getTableOperations().stream()
                        .noneMatch(t -> "T".equalsIgnoreCase(t.getTableName())),
                "TVF alias must not be recorded as a table operation");

        // The underlying function is still a dependency — recorded exactly once, not twice.
        long calls = proc.getCalls().stream()
                .filter(c -> "GET_LINE_ITEMS".equalsIgnoreCase(c.getName())).count();
        assertEquals(1, calls, "GET_LINE_ITEMS should be recorded exactly once");
    }

    @Test
    @DisplayName("Table ops derived from a CURSOR declaration carry their FROM-clause alias")
    void cursorTableOpsCarryAlias() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    CURSOR c1 IS
                      SELECT o.id, c.name
                        FROM HR.ORDERS o, CUSTOMERS c
                       WHERE o.cid = c.id;
                  BEGIN
                    FOR r IN c1 LOOP NULL; END LOOP;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "cursor alias");

        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);

        TableOperationInfo orders = proc.getTableOperations().stream()
                .filter(t -> "ORDERS".equalsIgnoreCase(t.getTableName()))
                .findFirst().orElseThrow();
        assertEquals("HR", orders.getSchema(), "schema must still be preserved");
        assertEquals("O", orders.getAlias());

        TableOperationInfo cust = proc.getTableOperations().stream()
                .filter(t -> "CUSTOMERS".equalsIgnoreCase(t.getTableName()))
                .findFirst().orElseThrow();
        assertEquals("C", cust.getAlias());
    }

    @Test
    @DisplayName("Old-style (+) outer-join markers are not mistaken for package function calls")
    void outerJoinSignIsNotACall() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v NUMBER;
                  BEGIN
                    SELECT x.total INTO v
                      FROM ( SELECT a.id, b.total
                               FROM ACCOUNTS a, BALANCES b
                              WHERE a.id = b.acct_id(+) ) x
                      JOIN LEDGER l ON l.id = x.id
                     WHERE x.id > 0;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "(+) with ANSI join");

        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertTrue(proc.getCalls().stream()
                        .noneMatch(c -> "ACCT_ID".equalsIgnoreCase(c.getName())),
                "b.acct_id(+) is a column with an outer-join marker, not a call: "
                        + proc.getCalls().stream().map(CallInfo::getName).toList());

        // The (+) join itself is still detected.
        TableOperationInfo acc = proc.getTableOperations().stream()
                .filter(t -> "ACCOUNTS".equalsIgnoreCase(t.getTableName()))
                .findFirst().orElseThrow();
        assertTrue(acc.getJoins().stream()
                        .anyMatch(j -> "BALANCES".equalsIgnoreCase(j.getJoinedTable())
                                && "LEFT OUTER".equals(j.getJoinType())),
                "(+) outer join to BALANCES should still be detected");
    }

    @Test
    @DisplayName("A locally declared collection type constructor is not a phantom EXTERNAL call")
    void localTypeConstructorIsNotACall() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  TYPE   rec_t   IS    RECORD (
                       id    NUMBER ,
                       nm    VARCHAR2 ( 50 ) );
                  TYPE tab_t IS TABLE OF rec_t INDEX BY PLS_INTEGER;
                  type NesTed_T is table of Number;
                  PROCEDURE PROC IS
                    l_tab   tab_t;
                    l_rec   rec_t;
                    l_n     NesTed_T := NesTed_T();
                  BEGIN
                    l_rec.id := 1;
                    l_tab ( 1 ) := l_rec;
                    l_n.EXTEND;
                    INSERT INTO TARGET_TBL (id) VALUES (l_rec.id);
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "collection type constructor");

        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertTrue(proc.getCalls().stream()
                        .noneMatch(c -> "NESTED_T".equalsIgnoreCase(c.getName())),
                "NESTED_T() is a local collection constructor, not an external call: "
                        + proc.getCalls().stream().map(CallInfo::getName).toList());

        // Real work in the same procedure is still captured.
        assertTrue(proc.getTableOperations().stream()
                .anyMatch(t -> "TARGET_TBL".equalsIgnoreCase(t.getTableName())));
    }
}
