package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AUDIT (read-only, evidence-gathering) test file. Not a bug-fix. Records ACTUAL observed
 * behavior of the Oracle (plsql-parser) visitor for two concepts explicitly requested by the
 * user:
 *
 * <ol>
 *   <li>Table functions used directly in a real SELECT's FROM/JOIN clause (not inside a
 *       CURSOR declaration — that case is already covered by
 *       {@link CursorJoinAndTableFunctionTest}).</li>
 *   <li>Old-style Oracle outer join syntax {@code column(+) = column}.</li>
 * </ol>
 *
 * Each test documents, in comments, what was actually observed at time of writing.
 */
@DisplayName("AUDIT: Oracle TABLE(fn()) in plain SELECT FROM/JOIN, and (+) outer join semantics")
public class OracleTableFunctionAndOuterJoinAuditTest extends ParserTestBase {

    @Test
    @DisplayName("AUDIT: JOIN TABLE(pkg.fn(...)) t ON ... inside a plain SELECT INTO (not a CURSOR)")
    void auditTableFunctionJoinInPlainSelectInto() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC (p_order_id NUMBER) IS
                    v_col1 VARCHAR2(100);
                  BEGIN
                    SELECT t.col1
                      INTO v_col1
                      FROM ORDERS o
                      JOIN TABLE(PKG_UTILS.GET_LINE_ITEMS(o.order_id)) t
                        ON t.some_col = o.some_col
                     WHERE o.order_id = p_order_id;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);

        // (a) parses without error
        assertNoParsErrors(result, "TABLE(fn()) join in plain SELECT INTO");

        ParsedObject pkg = result.getObjects().get(0);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc, "PROC should be found");

        // (b) GET_LINE_ITEMS should be recorded as a call
        boolean foundCall = pkg.getCalls().stream()
                .anyMatch(ci -> "GET_LINE_ITEMS".equalsIgnoreCase(ci.getName()))
                || proc.getCalls().stream()
                .anyMatch(ci -> "GET_LINE_ITEMS".equalsIgnoreCase(ci.getName()));

        // (c) ORDERS (the real table) should still be recorded as a table op
        boolean foundOrders = proc.getTableOperations().stream()
                .anyMatch(t -> t.getTableName() != null && t.getTableName().equalsIgnoreCase("ORDERS"));

        System.out.println("[AUDIT] foundCall(GET_LINE_ITEMS)=" + foundCall
                + " foundOrders(table-op)=" + foundOrders
                + " tableOps=" + proc.getTableOperations().stream().map(TableOperationInfo::getTableName).toList()
                + " calls(proc)=" + proc.getCalls().stream().map(CallInfo::getName).toList()
                + " calls(pkg)=" + pkg.getCalls().stream().map(CallInfo::getName).toList());

        assertTrue(foundCall, "AUDIT: GET_LINE_ITEMS should be recorded as a call from TABLE(...) in a plain SELECT INTO join");
        assertTrue(foundOrders, "AUDIT: ORDERS (real table) must not be dropped from table-ops when joined against TABLE(fn())");
    }

    @Test
    @DisplayName("AUDIT: bare SELECT * FROM TABLE(some_pipelined_func(p1, p2))")
    void auditBareSelectFromTableFunction() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC (p1 NUMBER, p2 NUMBER) IS
                  BEGIN
                    FOR r IN (SELECT * FROM TABLE(SOME_PIPELINED_FUNC(p1, p2))) LOOP
                      NULL;
                    END LOOP;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "bare SELECT * FROM TABLE(fn(...))");

        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);

        boolean foundCall = proc.getCalls().stream()
                .anyMatch(ci -> "SOME_PIPELINED_FUNC".equalsIgnoreCase(ci.getName()));

        System.out.println("[AUDIT] bare SELECT*FROM TABLE(...) foundCall=" + foundCall
                + " tableOps=" + proc.getTableOperations().stream().map(TableOperationInfo::getTableName).toList()
                + " calls=" + proc.getCalls().stream().map(CallInfo::getName).toList());

        assertTrue(foundCall, "AUDIT: SOME_PIPELINED_FUNC should be recorded as a call for bare SELECT * FROM TABLE(fn(...))");
        // NOTE: by design (per source inspection of extractCallsFromTableCollectionExpression),
        // no TableOperationInfo/pseudo-table-op is ever emitted for the TABLE(...) result itself
        // (there is no synthetic table op representing alias "t"/the collection result) — this
        // is consistent across both cursor and plain-SELECT usage, not a regression specific to
        // this test.
    }

    @Test
    @DisplayName("AUDIT: old-style outer join a.id = b.a_id(+) — single condition")
    void auditOldStyleOuterJoinSingleCondition() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_x VARCHAR2(100);
                    v_y VARCHAR2(100);
                  BEGIN
                    SELECT a.x, b.y
                      INTO v_x, v_y
                      FROM TAB_A a, TAB_B b
                     WHERE a.id = b.a_id(+);
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "old-style outer join (+) single condition");

        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);

        List<String> tableOpNames = proc.getTableOperations().stream()
                .map(TableOperationInfo::getTableName)
                .filter(java.util.Objects::nonNull)
                .map(String::toUpperCase)
                .toList();

        boolean foundA = tableOpNames.contains("TAB_A");
        boolean foundB = tableOpNames.contains("TAB_B");

        // Does any table-op carry join info reflecting the (+) outer join?
        boolean anyJoinRecorded = proc.getTableOperations().stream()
                .anyMatch(t -> t.getJoins() != null && !t.getJoins().isEmpty());

        System.out.println("[AUDIT] oldStyleOuterJoin single: foundA(TAB_A)=" + foundA
                + " foundB(TAB_B)=" + foundB + " anyJoinRecorded=" + anyJoinRecorded
                + " tableOps=" + tableOpNames);
        for (TableOperationInfo t : proc.getTableOperations()) {
            if (t.getJoins() != null) {
                for (var j : t.getJoins()) {
                    System.out.println("[AUDIT] JOIN detail: onTable=" + t.getTableName()
                            + " joinType=" + j.getJoinType()
                            + " joinedTable=" + j.getJoinedTable()
                            + " leftTable=" + j.getLeftTable()
                            + " condition=" + j.getCondition());
                }
            }
        }

        assertTrue(foundA, "AUDIT: TAB_A must be recorded as a table op");
        assertTrue(foundB, "AUDIT: TAB_B must be recorded as a table op");

        // FIXED: outer_join_sign is now read by the WHERE-clause equijoin heuristic, so
        // the previously-generic IMPLICIT join is reclassified with real outer-join semantics.
        assertTrue(anyJoinRecorded, "TAB_A's implicit join to TAB_B must carry join info");
        JoinInfo joinInfo = proc.getTableOperations().stream()
                .filter(t -> "TAB_A".equalsIgnoreCase(t.getTableName()))
                .flatMap(t -> t.getJoins().stream())
                .filter(j -> "TAB_B".equalsIgnoreCase(j.getJoinedTable()) || "B".equalsIgnoreCase(j.getJoinedTableAlias()))
                .findFirst().orElse(null);
        assertNotNull(joinInfo, "Join detail for TAB_B must be present");
        // b.a_id(+) means TAB_B is the optional side => LEFT OUTER from TAB_A's perspective.
        assertEquals("LEFT OUTER", joinInfo.getJoinType());
        assertEquals("TAB_A", joinInfo.getLeftTable());
        assertNotNull(joinInfo.getCondition());
    }

    @Test
    @DisplayName("AUDIT: old-style outer join with multiple (+) conditions in compound WHERE")
    void auditOldStyleOuterJoinMultipleConditions() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_x VARCHAR2(100);
                  BEGIN
                    SELECT a.x
                      INTO v_x
                      FROM TAB_A a, TAB_B b, TAB_C c
                     WHERE a.id = b.a_id(+)
                       AND b.id = c.b_id(+)
                       AND a.status = 'ACTIVE';
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "old-style outer join (+) multiple conditions");

        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);

        List<String> tableOpNames = proc.getTableOperations().stream()
                .map(TableOperationInfo::getTableName)
                .filter(java.util.Objects::nonNull)
                .map(String::toUpperCase)
                .toList();

        System.out.println("[AUDIT] oldStyleOuterJoin multiple: tableOps=" + tableOpNames);

        assertTrue(tableOpNames.contains("TAB_A"), "AUDIT: TAB_A must be present");
        assertTrue(tableOpNames.contains("TAB_B"), "AUDIT: TAB_B must be present");
        assertTrue(tableOpNames.contains("TAB_C"), "AUDIT: TAB_C must be present");

        // FIXED: both ANDed (+) conditions must be reclassified as LEFT OUTER joins.
        List<JoinInfo> joins = proc.getTableOperations().stream()
                .filter(t -> "TAB_A".equalsIgnoreCase(t.getTableName()))
                .flatMap(t -> t.getJoins().stream())
                .toList();
        JoinInfo bJoin = joins.stream().filter(j -> "TAB_B".equalsIgnoreCase(j.getJoinedTable())
                || "B".equalsIgnoreCase(j.getJoinedTableAlias())).findFirst().orElse(null);
        JoinInfo cJoin = joins.stream().filter(j -> "TAB_C".equalsIgnoreCase(j.getJoinedTable())
                || "C".equalsIgnoreCase(j.getJoinedTableAlias())).findFirst().orElse(null);
        assertNotNull(bJoin, "Join detail for TAB_B must be present");
        assertNotNull(cJoin, "Join detail for TAB_C must be present");
        assertEquals("LEFT OUTER", bJoin.getJoinType());
        assertEquals("LEFT OUTER", cJoin.getJoinType());
        assertEquals("TAB_A", bJoin.getLeftTable());
        assertEquals("TAB_B", cJoin.getLeftTable());
    }

    @Test
    @DisplayName("AUDIT: right-side old-style outer join a.id(+) = b.a_id")
    void auditOldStyleOuterJoinRightSide() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_x VARCHAR2(100);
                  BEGIN
                    SELECT a.x
                      INTO v_x
                      FROM TAB_A a, TAB_B b
                     WHERE a.id(+) = b.a_id;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "old-style outer join (+) right side");

        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);

        List<String> tableOpNames = proc.getTableOperations().stream()
                .map(TableOperationInfo::getTableName)
                .filter(java.util.Objects::nonNull)
                .map(String::toUpperCase)
                .toList();

        System.out.println("[AUDIT] oldStyleOuterJoin right-side: tableOps=" + tableOpNames);

        assertTrue(tableOpNames.contains("TAB_A"), "AUDIT: TAB_A must be present");
        assertTrue(tableOpNames.contains("TAB_B"), "AUDIT: TAB_B must be present");

        // FIXED: a.id(+) means TAB_A (the base table carrying the (+) column) is the
        // optional side and TAB_B (the joined table) is preserved => RIGHT OUTER from
        // this join's viewpoint (joined table is the preserved/left-hand side).
        JoinInfo joinInfo = proc.getTableOperations().stream()
                .filter(t -> "TAB_A".equalsIgnoreCase(t.getTableName()))
                .flatMap(t -> t.getJoins().stream())
                .filter(j -> "TAB_B".equalsIgnoreCase(j.getJoinedTable()) || "B".equalsIgnoreCase(j.getJoinedTableAlias()))
                .findFirst().orElse(null);
        assertNotNull(joinInfo, "Join detail for TAB_B must be present");
        assertEquals("RIGHT OUTER", joinInfo.getJoinType());
        assertEquals("TAB_B", joinInfo.getLeftTable());
        assertNotNull(joinInfo.getCondition());
    }
}
