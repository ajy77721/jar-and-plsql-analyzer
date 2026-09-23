package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Permanent regression cover for three Oracle writing-style bugs that were found and fixed in
 * {@code PlSqlAnalysisVisitor}:
 *
 * <ol>
 *   <li>An UNALIASED join target resolved its own left side (the join pointed at itself),
 *       because buildAliasMap maps every table name to itself so unqualified references
 *       resolve. The joined table's own bare name is now excluded from left-side resolution,
 *       but only when it carries no alias, so an aliased self-join still works.</li>
 *   <li>Comma-joins and old-style {@code (+)} outer joins were lost entirely when the query was
 *       written inside a CURSOR declaration — the cursor path never reaches visitFrom_clause.
 *       {@code extractJoinsFromSelect} now calls attachImplicitCommaJoins explicitly.</li>
 *   <li>A qualified {@code TABLE(PKG . MY_FN(x))} table-collection call with whitespace around
 *       the dot was dropped as a dependency inside a cursor, because the text pattern used
 *       required the dots to be written without surrounding whitespace. Resolution is now
 *       grammar-based and therefore whitespace-insensitive.</li>
 * </ol>
 */
@DisplayName("Oracle writing-style audit: join left-table, cursor comma/(+) joins, spaced TABLE(pkg.fn)")
public class OracleWritingStyleJoinAndCursorAuditTest extends ParserTestBase {

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private List<JoinInfo> allJoins(ParseResult result) {
        List<JoinInfo> joins = new ArrayList<>();
        for (ParsedObject obj : result.getObjects()) {
            for (TableOperationInfo t : obj.getTableOperations()) {
                joins.addAll(t.getJoins());
            }
            for (SubprogramInfo sub : obj.getSubprograms()) {
                for (TableOperationInfo t : sub.getTableOperations()) {
                    joins.addAll(t.getJoins());
                }
            }
        }
        return joins;
    }

    private JoinInfo joinTo(List<JoinInfo> joins, String joinedTable) {
        return joins.stream()
                .filter(j -> j.getJoinedTable() != null
                        && (j.getJoinedTable().equalsIgnoreCase(joinedTable)
                            || j.getJoinedTable().toUpperCase().endsWith("." + joinedTable.toUpperCase())))
                .findFirst().orElse(null);
    }

    private List<CallInfo> allCalls(ParseResult result) {
        List<CallInfo> calls = new ArrayList<>();
        for (ParsedObject obj : result.getObjects()) {
            calls.addAll(obj.getCalls());
            for (SubprogramInfo sub : obj.getSubprograms()) {
                calls.addAll(sub.getCalls());
            }
        }
        return calls;
    }

    private CallInfo callNamed(List<CallInfo> calls, String name) {
        return calls.stream()
                .filter(c -> c.getName() != null && c.getName().equalsIgnoreCase(name))
                .findFirst().orElse(null);
    }

    // ------------------------------------------------------------------
    // FIX 1 — unaliased join target must not resolve its own left side
    // ------------------------------------------------------------------

    @Test
    @DisplayName("Fix 1: chained UNALIASED join targets resolve the true left table, not themselves")
    void unaliasedJoinTargetsDoNotResolveToThemselves() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_id NUMBER;
                  BEGIN
                    SELECT a.id INTO v_id
                      FROM MYSCH.TAB_A a
                      JOIN TAB_B ON TAB_B.a_id = a.id
                      JOIN TAB_C ON TAB_C.b_id = TAB_B.id;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "unaliased chained joins");

        List<JoinInfo> joins = allJoins(result);
        JoinInfo jb = joinTo(joins, "TAB_B");
        JoinInfo jc = joinTo(joins, "TAB_C");
        assertNotNull(jb, "JOIN TAB_B should be captured, joins=" + joins);
        assertNotNull(jc, "JOIN TAB_C should be captured, joins=" + joins);

        assertNull(jb.getJoinedTableAlias(), "TAB_B is unaliased");
        assertNull(jc.getJoinedTableAlias(), "TAB_C is unaliased");

        assertEquals("TAB_A", jb.getLeftTable(),
                "First join's left side must be TAB_A, not TAB_B pointing at itself");
        assertEquals("A", jb.getLeftTableAlias());

        assertEquals("TAB_B", jc.getLeftTable(),
                "Second join's left side must be TAB_B, not TAB_C pointing at itself");
        assertEquals("TAB_B", jc.getLeftTableAlias());
    }

    @Test
    @DisplayName("Fix 1 companion: ALIASED self-join still resolves to the true left side")
    void aliasedSelfJoinStillResolvesLeftSide() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_id NUMBER;
                  BEGIN
                    SELECT x.id INTO v_id
                      FROM TAB_T
                      JOIN TAB_T x ON x.id = TAB_T.pid;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "aliased self-join");

        List<JoinInfo> joins = allJoins(result);
        assertFalse(joins.isEmpty(), "self-join should be captured");
        JoinInfo j = joins.get(0);
        assertEquals("X", j.getJoinedTableAlias());
        // The bare table name on the left is legitimate here: the joined side has its own alias,
        // so TAB_T must NOT be excluded from left-side resolution.
        assertEquals("TAB_T", j.getLeftTable(), "aliased self-join left side must still resolve");
        assertEquals("TAB_T", j.getLeftTableAlias());
    }

    // ------------------------------------------------------------------
    // FIX 2 — comma joins and old-style (+) outer joins inside a CURSOR
    // ------------------------------------------------------------------

    private static final String CURSOR_COMMA_JOIN = """
            CREATE OR REPLACE PACKAGE BODY MY_PKG AS
              PROCEDURE PROC IS
                CURSOR c IS
                  SELECT a.id, b.name
                    FROM TAB_A a, TAB_B b
                   WHERE a.id = b.a_id(+);
              BEGIN
                NULL;
              END PROC;
            END MY_PKG;
            /
            """;

    private static final String INLINE_COMMA_JOIN = """
            CREATE OR REPLACE PACKAGE BODY MY_PKG AS
              PROCEDURE PROC IS
                v_id NUMBER;
                v_nm VARCHAR2(100);
              BEGIN
                SELECT a.id, b.name INTO v_id, v_nm
                  FROM TAB_A a, TAB_B b
                 WHERE a.id = b.a_id(+);
              END PROC;
            END MY_PKG;
            /
            """;

    @Test
    @DisplayName("Fix 2: comma join with old-style (+) inside a CURSOR is captured as LEFT OUTER")
    void cursorCommaJoinWithOuterSignIsCaptured() {
        ParseResult result = parse(CURSOR_COMMA_JOIN);
        assertNoParsErrors(result, "cursor comma/(+) join");

        List<JoinInfo> joins = allJoins(result);
        JoinInfo j = joinTo(joins, "TAB_B");
        assertNotNull(j, "cursor comma join to TAB_B must be captured, joins=" + joins);
        assertEquals("LEFT OUTER", j.getJoinType(),
                "a.id = b.a_id(+) is a LEFT OUTER join, not a bare IMPLICIT");
        assertEquals("B", j.getJoinedTableAlias());
        assertEquals("TAB_A", j.getLeftTable());
        assertNotNull(j.getCondition(), "(+) condition must be populated");
        assertTrue(j.getCondition().toLowerCase().replace(" ", "").contains("b.a_id"),
                "condition should carry the equijoin, was: " + j.getCondition());
    }

    @Test
    @DisplayName("Fix 2 comparison: CURSOR form and inline SELECT INTO form extract the same join")
    void cursorAndInlineCommaJoinsAgree() {
        JoinInfo cursorJoin = joinTo(allJoins(parse(CURSOR_COMMA_JOIN)), "TAB_B");
        JoinInfo inlineJoin = joinTo(allJoins(parse(INLINE_COMMA_JOIN)), "TAB_B");
        assertNotNull(cursorJoin, "cursor form must yield a join");
        assertNotNull(inlineJoin, "inline form must yield a join");

        assertEquals(inlineJoin.getJoinType(), cursorJoin.getJoinType());
        assertEquals(inlineJoin.getJoinedTable(), cursorJoin.getJoinedTable());
        assertEquals(inlineJoin.getJoinedTableAlias(), cursorJoin.getJoinedTableAlias());
        assertEquals(inlineJoin.getLeftTable(), cursorJoin.getLeftTable());
        assertEquals(inlineJoin.getCondition(), cursorJoin.getCondition());
    }

    // ------------------------------------------------------------------
    // FIX 3 — TABLE(PKG . MY_FN(x)) with whitespace around the dot
    // ------------------------------------------------------------------

    private static String tableFnCursor(String tableExpr) {
        return """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    CURSOR c IS
                      SELECT t.col
                        FROM TABLE(%s) t;
                  BEGIN
                    NULL;
                  END PROC;
                END MY_PKG;
                /
                """.formatted(tableExpr);
    }

    @Test
    @DisplayName("Fix 3: TABLE(PKG . MY_FN(x)) with spaces around the dot is still a dependency")
    void spacedQualifiedTableFunctionIsCapturedAsCall() {
        ParseResult result = parse(tableFnCursor("PKG . MY_FN(1)"));
        assertNoParsErrors(result, "spaced TABLE(pkg . fn)");

        List<CallInfo> calls = allCalls(result);
        CallInfo c = callNamed(calls, "MY_FN");
        assertNotNull(c, "MY_FN must be recorded as a call, calls=" + calls);
        assertEquals("PKG", c.getPackageName() == null ? null : c.getPackageName().toUpperCase(),
                "package qualifier must survive the spaced dot");
    }

    @Test
    @DisplayName("Fix 3 companion: spaced and tight TABLE(PKG.MY_FN(x)) extract equivalently")
    void spacedAndTightQualifiedTableFunctionAgree() {
        CallInfo spaced = callNamed(allCalls(parse(tableFnCursor("PKG . MY_FN(1)"))), "MY_FN");
        CallInfo tight = callNamed(allCalls(parse(tableFnCursor("PKG.MY_FN(1)"))), "MY_FN");
        assertNotNull(tight, "tight form must be captured");
        assertNotNull(spaced, "spaced form must be captured");

        assertEquals(tight.getName(), spaced.getName());
        assertEquals(tight.getPackageName(), spaced.getPackageName());
        assertEquals(tight.getSchema(), spaced.getSchema());
        assertEquals(tight.getType(), spaced.getType());
        assertEquals(tight.getArguments(), spaced.getArguments());
    }
}
