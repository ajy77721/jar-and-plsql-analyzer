package com.plsql.db2;

import com.plsql.db2.engine.Db2AnalysisEngine;
import com.plsql.db2.model.Db2CallInfo;
import com.plsql.db2.model.Db2RoutineInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Sep12, DB2 — a function call nested inside a CALL statement's argument list must
 * still be recorded as a call edge from the containing procedure.
 *
 * <p>Real audit case: real source line 24915 of the seed DDL,
 * {@code ERP_USP_RUN_CYCLE} contains
 * {@code CALL ERP_USP_CLOSE_PERIOD(ERP_FN_FISCAL_PERIOD(CURRENT DATE));}. Before the
 * fix, {@code visitCallStatement} recorded the outer {@code ERP_USP_CLOSE_PERIOD} call
 * but returned {@code null} instead of recursing into the argument list, so the nested
 * {@code ERP_FN_FISCAL_PERIOD} function call was dropped entirely from
 * {@code ERP_USP_RUN_CYCLE}'s own call edges.
 */
class Db2NestedCallInCallStatementTest {

    private final Db2AnalysisEngine engine = new Db2AnalysisEngine();

    private Db2RoutineInfo parse(String body) {
        return engine.parse(
                "CREATE PROCEDURE p()\nLANGUAGE SQL\nBEGIN\n" + body + "\nEND", "p", null);
    }

    @Test
    @DisplayName("CALL proc(func(arg)) records both proc and func as call edges")
    void nestedFunctionCallInsideCallStatementIsCaptured() {
        Db2RoutineInfo r = parse(
                "  CALL ERP_USP_CLOSE_PERIOD(ERP_FN_FISCAL_PERIOD(CURRENT DATE));");

        boolean hasOuter = r.getCalls().stream()
                .anyMatch(c -> "ERP_USP_CLOSE_PERIOD".equalsIgnoreCase(c.getName()));
        boolean hasInner = r.getCalls().stream()
                .anyMatch(c -> "ERP_FN_FISCAL_PERIOD".equalsIgnoreCase(c.getName()));

        assertTrue(hasOuter, "the outer CALL target must still be recorded: "
                + describe(r));
        assertTrue(hasInner, "the nested function call inside the CALL argument list "
                + "was dropped from the containing procedure's own call edges: "
                + describe(r));
    }

    private String describe(Db2RoutineInfo r) {
        StringBuilder sb = new StringBuilder();
        for (Db2CallInfo c : r.getCalls()) {
            sb.append(c.qualifiedName()).append('@').append(c.getLine()).append(' ');
        }
        return sb.toString();
    }
}
