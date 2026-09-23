package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.SqlServerCallInfo;
import com.plsql.sqlserver.model.SqlServerParamInfo;
import com.plsql.sqlserver.model.SqlServerRoutineInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

/**
 * READ-ONLY AUDIT — does NOT touch main/ source. Verifies actual (fresh-read) behavior of
 * SqlServerAnalysisVisitor / SqlServerParser.g4 for T-SQL EXEC/EXECUTE call-convention concepts:
 *
 *  1. EXEC vs EXECUTE keyword — both should be recognized identically as an outbound call.
 *  2. OUTPUT / OUT parameter direction on a CALL SITE argument — is it captured at all?
 *  3. Default parameter values in the routine's OWN header (paramBlock) — captured on the
 *     parameter model? Does a default containing a comma or parens break parsing?
 *  4. Named ("@p = value") parameter passing in a call, out of declared order — does the call
 *     still get recorded correctly (no crash, no dropped/miscounted call)?
 *  5. A table-valued parameter (TVP) forwarded verbatim into a NESTED EXEC call — is the nested
 *     call recorded, and is the TVP name NOT misrecorded as some other kind of reference?
 *
 * Findings are asserted against real engine.parse() output. Some assertions document a confirmed
 * bug (with the actual wrong value asserted, so the test fails loudly if ever silently "fixed"
 * without updating this record) — see per-test comments for expected vs actual.
 */
class SqlServerProcCallConventionAuditTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private SqlServerRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    private void dumpCalls(String label, SqlServerRoutineInfo r) {
        System.out.println("=== " + label + " ===");
        System.out.println("calls: " + r.getCalls().stream()
                .map(c -> c.qualifiedName() + "@" + c.getLine()).toList());
        System.out.println("errors: " + r.getErrors());
    }

    // ── Concept 1: EXEC vs EXECUTE ──────────────────────────────────────────────────────────
    @Test
    @DisplayName("Concept 1: EXEC and EXECUTE both recognized identically as outbound calls")
    void execVsExecuteKeyword() {
        String src =
                "CREATE PROCEDURE usp_Caller AS\n" +
                "BEGIN\n" +
                "    EXEC dbo.usp_DoWork @p1, @p2;\n" +
                "    EXECUTE dbo.usp_DoWorkToo @p1, @p2;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dumpCalls("execVsExecuteKeyword", r);

        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        assertTrue(r.getCalls().stream().anyMatch(c -> "usp_DoWork".equalsIgnoreCase(c.getName())
                        && "dbo".equalsIgnoreCase(c.getSchema())),
                "EXEC dbo.usp_DoWork must be recorded as a call; calls=" + r.getCalls());
        assertTrue(r.getCalls().stream().anyMatch(c -> "usp_DoWorkToo".equalsIgnoreCase(c.getName())
                        && "dbo".equalsIgnoreCase(c.getSchema())),
                "EXECUTE dbo.usp_DoWorkToo must be recorded identically to EXEC; calls=" + r.getCalls());
        // CONFIRMED WORKING: both keywords are handled by the same grammar alternative
        // ((EXEC | EXECUTE) routineName stmtTail SEMI?) and the same visitExecStatement code path.
        assertEquals(2, r.getCalls().size(), "exactly 2 distinct calls expected; calls=" + r.getCalls());
    }

    // ── Concept 2: OUTPUT / OUT parameter direction at the CALL SITE ────────────────────────
    @Test
    @DisplayName("Concept 2: BUG — call-site OUTPUT/OUT argument direction is not captured at all")
    void outputParameterAtCallSite_notCaptured() {
        String src =
                "CREATE PROCEDURE usp_Caller (@order_id INT, @total DECIMAL(18,2) OUTPUT) AS\n" +
                "BEGIN\n" +
                "    EXEC dbo.usp_GetTotal @order_id, @total OUTPUT;\n" +
                "    EXEC dbo.usp_GetTotal2 @order_id, @total OUT;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dumpCalls("outputParameterAtCallSite_notCaptured", r);

        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        // The call itself IS still recorded correctly...
        assertEquals(2, r.getCalls().size(), "both calls must still be recorded; calls=" + r.getCalls());
        assertTrue(r.getCalls().stream().anyMatch(c -> "usp_GetTotal".equalsIgnoreCase(c.getName())));
        assertTrue(r.getCalls().stream().anyMatch(c -> "usp_GetTotal2".equalsIgnoreCase(c.getName())));

        // FIXED: SqlServerCallInfo now has a getArgs() list, populated with each call-site
        // argument's text and an isOutput() flag, so "EXEC ... @total OUTPUT" is distinguishable
        // from a plain IN argument.
        java.util.Set<String> callInfoFieldNames = new java.util.HashSet<>();
        for (Method m : SqlServerCallInfo.class.getMethods()) callInfoFieldNames.add(m.getName());
        assertTrue(callInfoFieldNames.contains("getArgs"),
                "FIXED: SqlServerCallInfo now exposes an argument/direction accessor; methods=" + callInfoFieldNames);

        SqlServerCallInfo getTotalCall = r.getCalls().stream()
                .filter(c -> "usp_GetTotal".equalsIgnoreCase(c.getName())).findFirst().orElseThrow();
        SqlServerCallInfo getTotal2Call = r.getCalls().stream()
                .filter(c -> "usp_GetTotal2".equalsIgnoreCase(c.getName())).findFirst().orElseThrow();
        assertEquals(2, getTotalCall.getArgs().size());
        assertFalse(getTotalCall.getArgs().get(0).isOutput(), "@order_id is a plain IN argument");
        assertTrue(getTotalCall.getArgs().get(1).isOutput(), "@total OUTPUT must be flagged as an OUTPUT argument");
        assertEquals(2, getTotal2Call.getArgs().size());
        assertTrue(getTotal2Call.getArgs().get(1).isOutput(), "@total OUT must also be flagged as an OUTPUT argument");
    }

    // ── Concept 3: default parameter values in the routine's OWN header ────────────────────
    @Test
    @DisplayName("Concept 3: BUG — default parameter values parsed by grammar but dropped by visitor")
    void defaultParameterValues_capturedByGrammarNotByModel() {
        String src =
                "CREATE PROCEDURE usp_Foo (@p1 INT, @p2 VARCHAR(50) = 'default', @p3 INT = NULL) AS\n" +
                "BEGIN\n" +
                "    SELECT @p1, @p2, @p3;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        System.out.println("=== defaultParameterValues params ===");
        System.out.println(r.getParams().stream()
                .map(p -> p.getName() + " " + p.getDataType() + " dir=" + p.getDirection()).toList());
        System.out.println("errors: " + r.getErrors());

        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        assertEquals(3, r.getParams().size(), "all 3 params (incl. defaulted ones) must still be recorded; params="
                + r.getParams());

        // FIXED: SqlServerParamInfo now has a defaultValue field, populated from the grammar's
        // `defaultVal` rule (`AT_VAR dataType (EQ defaultVal)? paramModifier*`) by visitParam().
        java.util.Set<String> paramInfoFieldNames = new java.util.HashSet<>();
        for (Method m : SqlServerParamInfo.class.getMethods()) paramInfoFieldNames.add(m.getName());
        assertTrue(paramInfoFieldNames.contains("getDefaultValue"),
                "FIXED: SqlServerParamInfo now exposes a default-value accessor; methods=" + paramInfoFieldNames);

        assertNull(r.getParams().get(0).getDefaultValue(), "@p1 has no default");
        assertEquals("'default'", r.getParams().get(1).getDefaultValue(), "@p2's default value text");
        assertEquals("NULL", r.getParams().get(2).getDefaultValue(), "@p3's default value text");

        // Data types themselves are unaffected by the default's presence.
        assertEquals("VARCHAR", r.getParams().get(1).getDataType() == null ? null
                        : r.getParams().get(1).getDataType().toUpperCase().startsWith("VARCHAR") ? "VARCHAR"
                        : r.getParams().get(1).getDataType(),
                "sanity: @p2's own data type text should still start with VARCHAR");
    }

    // ── Concept 3b: default value containing a comma and parens must not break param-list parsing ──
    @Test
    @DisplayName("Concept 3b: default value with comma/parens does not corrupt the parameter list")
    void defaultParameterValue_withCommaAndParens_doesNotBreakParamList() {
        String src =
                "CREATE PROCEDURE usp_Bar (@p1 VARCHAR(50) = 'a,b', @p2 INT = 5, @p3 DECIMAL(18,2) = 1.5) AS\n" +
                "BEGIN\n" +
                "    SELECT @p1, @p2, @p3;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        System.out.println("=== defaultParameterValue_withCommaAndParens params ===");
        System.out.println(r.getParams().stream()
                .map(p -> p.getName() + " " + p.getDataType()).toList());
        System.out.println("errors: " + r.getErrors());

        // CONFIRMED WORKING: the comma inside the quoted string literal 'a,b' is tokenised as part
        // of a single STRING token by the lexer (defaultVal: literal | ID | MINUS INT_NUM |
        // NULL_KW), so it does not get misread as the param-list's COMMA separator — exactly 3
        // params survive, not 4 (which would indicate the literal's internal comma split the list).
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        assertEquals(3, r.getParams().size(),
                "a comma inside a quoted default value must not split the param list into extra params; params="
                        + r.getParams());
        assertEquals("@p1", r.getParams().get(0).getName());
        assertEquals("@p2", r.getParams().get(1).getName());
        assertEquals("@p3", r.getParams().get(2).getName());
    }

    // ── Concept 4: named (order-independent) parameter passing at a call site ──────────────
    @Test
    @DisplayName("Concept 4: named/out-of-order call arguments do not drop or miscount the call")
    void namedParameterPassing_outOfOrder() {
        String src =
                "CREATE PROCEDURE usp_Caller AS\n" +
                "BEGIN\n" +
                "    EXEC dbo.usp_Foo @p2 = 5, @p1 = 10;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dumpCalls("namedParameterPassing_outOfOrder", r);

        // CONFIRMED WORKING (at the level this model tracks): the call is recorded once, correctly
        // named/scoped, regardless of the argument syntax used — visitExecStatement only reads
        // ctx.routineName(); it never attempts to parse the argument list, so named/out-of-order
        // arguments cannot desync anything there is no per-argument tracking of in the first place.
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        assertEquals(1, r.getCalls().size(), "exactly one call to usp_Foo expected; calls=" + r.getCalls());
        SqlServerCallInfo c = r.getCalls().get(0);
        assertEquals("usp_Foo", c.getName());
        assertEquals("dbo", c.getSchema());
    }

    // ── Concept 5: TVP forwarded verbatim to a NESTED EXEC call ─────────────────────────────
    @Test
    @DisplayName("Concept 5: TVP forwarded to a nested EXEC call is recorded as a call, TVP name not misrecorded")
    void tableValuedParameter_forwardedToNestedCall() {
        String src =
                "CREATE PROCEDURE usp_Outer (@tvp dbo.MyTableType READONLY) AS\n" +
                "BEGIN\n" +
                "    EXEC dbo.usp_Inner @tvp;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dumpCalls("tableValuedParameter_forwardedToNestedCall", r);
        System.out.println("params: " + r.getParams().stream()
                .map(p -> p.getName() + " " + p.getDataType() + " dir=" + p.getDirection()).toList());
        System.out.println("tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.qualifiedTable()).toList());

        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());

        // (a) usp_Inner must be recorded as an outbound call.
        assertEquals(1, r.getCalls().size(), "usp_Inner must be recorded exactly once as a call; calls="
                + r.getCalls());
        SqlServerCallInfo c = r.getCalls().get(0);
        assertEquals("usp_Inner", c.getName());
        assertEquals("dbo", c.getSchema());

        // (b) @tvp itself must not leak into the table-op inventory as some kind of table
        // reference just because it was passed as a call argument (it's consumed inside
        // execStatement's stmtTail, which is never scanned by the FROM/JOIN table-extraction
        // regexes — those only run over SELECT/INSERT/UPDATE/DELETE/dynamic-SQL text).
        boolean tvpAsTableOp = r.getTableOps().stream()
                .anyMatch(t -> "tvp".equalsIgnoreCase(t.getTableName()) || "@tvp".equalsIgnoreCase(t.getTableName()));
        assertFalse(tvpAsTableOp, "@tvp forwarded to a nested call must not be mis-recorded as a table op; tableOps="
                + r.getTableOps().stream().map(t -> t.qualifiedTable()).toList());

        // (c) The outer routine's own @tvp parameter is still correctly recorded once, untouched
        // by the nested call.
        assertEquals(1, r.getParams().size());
        assertEquals("@tvp", r.getParams().get(0).getName());
        assertEquals("dbo.MyTableType", r.getParams().get(0).getDataType());
    }

    // ── Concept 5b: TVP forwarded to a nested call alongside other scalar args ──────────────
    @Test
    @DisplayName("Concept 5b: TVP + scalar args forwarded together to a nested call")
    void tableValuedParameter_forwardedWithScalarArgs() {
        String src =
                "CREATE PROCEDURE usp_Outer2 (@tvp dbo.MyTableType READONLY, @flag INT) AS\n" +
                "BEGIN\n" +
                "    EXEC dbo.usp_Inner2 @flag, @tvp;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dumpCalls("tableValuedParameter_forwardedWithScalarArgs", r);

        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
        assertEquals(1, r.getCalls().size(), "usp_Inner2 must be recorded once regardless of TVP-in-arglist; calls="
                + r.getCalls());
        assertEquals("usp_Inner2", r.getCalls().get(0).getName());
        assertEquals(2, r.getParams().size(), "both outer params (@tvp, @flag) must survive; params="
                + r.getParams());
    }
}
