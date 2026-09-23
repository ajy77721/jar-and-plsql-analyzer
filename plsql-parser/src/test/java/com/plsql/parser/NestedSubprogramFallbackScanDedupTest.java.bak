package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Regression test for the fallback SQL-embedded-call scanner
 * ({@code PlSqlAnalysisVisitor.extractFunctionCallsFromSqlText}) double-counting calls made by
 * a NESTED subprogram declared inside an outer function/procedure's own declare section.
 *
 * <p>Root cause (fixed): {@code visitFunction_body}/{@code visitProcedure_body} grab the full
 * source text of the OUTER construct (via {@code getOriginalText(ctx, ...)}), which
 * syntactically includes the full text of any nested procedure_body/function_body declared in
 * the outer body's declare section. The main tree-walk correctly visits the nested subprogram
 * first and records its calls under the NESTED subprogram's own {@code SubprogramInfo}. But the
 * fallback text-scanner then runs on the OUTER body's (nested-inclusive) text after the walk has
 * already popped back to the OUTER subprogram — so its dedup check (built from the OUTER
 * subprogram's own call list) has no way to know the nested subprogram already reported that
 * target, and re-detects it as a brand-new duplicate {@code CallInfo}, stamped with the OUTER
 * body's own start line.
 *
 * <p>Fix: the text fed to the fallback scanner for a function/procedure body now has any
 * directly-nested procedure_body/function_body spans blanked out first
 * ({@code getOriginalTextExcludingNested}), so the fallback scanner never sees — and can never
 * re-detect — calls that belong to a nested subprogram's own body.
 */
public class NestedSubprogramFallbackScanDedupTest extends ParserTestBase {

    @Test
    @DisplayName("Fallback scanner does not duplicate a nested function's external call onto the outer function")
    void nestedFunctionCallIsNotDuplicatedOntoOuterFunction() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY LOG_TEST_PKG AS
                  FUNCTION OUTER_FN RETURN NUMBER IS
                    FUNCTION INNER_FN RETURN NUMBER IS
                    BEGIN
                      PKG_UTIL.LOG_MSG('inner call');
                      RETURN 1;
                    END INNER_FN;
                  BEGIN
                    RETURN INNER_FN();
                  END OUTER_FN;
                END LOG_TEST_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Nested function fallback-scan dedup");

        SubprogramInfo outer = findSub(result, "OUTER_FN");
        SubprogramInfo inner = findSub(result, "INNER_FN");
        assertNotNull(outer, "OUTER_FN should be found");
        assertNotNull(inner, "INNER_FN should be found (flattened into the same object)");

        System.out.println("=== Nested function fallback-scan dedup ===");
        System.out.println("  INNER_FN calls:");
        for (CallInfo c : inner.getCalls()) {
            System.out.println("    name=" + c.getName() + " pkg=" + c.getPackageName()
                    + " line=" + c.getLine() + " type=" + c.getType());
        }
        System.out.println("  OUTER_FN calls:");
        for (CallInfo c : outer.getCalls()) {
            System.out.println("    name=" + c.getName() + " pkg=" + c.getPackageName()
                    + " line=" + c.getLine() + " type=" + c.getType());
        }

        // The nested function genuinely calls PKG_UTIL.LOG_MSG once -- the main tree-walk
        // records this correctly under INNER_FN's own SubprogramInfo.
        long innerLogMsgCalls = inner.getCalls().stream()
                .filter(c -> "LOG_MSG".equalsIgnoreCase(c.getName())
                        && "PKG_UTIL".equalsIgnoreCase(c.getPackageName()))
                .count();
        assertEquals(1, innerLogMsgCalls,
                "INNER_FN should have exactly one recorded call to PKG_UTIL.LOG_MSG");

        // BUG (fixed): the fallback scanner used to re-detect this same target while scanning
        // OUTER_FN's own (nested-inclusive) source text, adding a bogus duplicate CallInfo onto
        // OUTER_FN -- which never itself calls PKG_UTIL.LOG_MSG -- stamped with OUTER_FN's own
        // start line.
        long outerLogMsgCalls = outer.getCalls().stream()
                .filter(c -> "LOG_MSG".equalsIgnoreCase(c.getName())
                        && "PKG_UTIL".equalsIgnoreCase(c.getPackageName()))
                .count();
        assertEquals(0, outerLogMsgCalls,
                "OUTER_FN must NOT get a bogus duplicate call to PKG_UTIL.LOG_MSG from the "
                        + "fallback scan of its nested function's text");

        // Across the whole package, the real number of PKG_UTIL.LOG_MSG call sites is exactly
        // one (the call inside INNER_FN) -- not two (real + fallback-scanner duplicate).
        ParsedObject obj = result.getObjects().get(0);
        long totalLogMsgCalls = obj.getSubprograms().stream()
                .flatMap(s -> s.getCalls().stream())
                .filter(c -> "LOG_MSG".equalsIgnoreCase(c.getName())
                        && "PKG_UTIL".equalsIgnoreCase(c.getPackageName()))
                .count();
        assertEquals(1, totalLogMsgCalls,
                "Total PKG_UTIL.LOG_MSG calls across the whole package should match the single "
                        + "genuine call site, not real+1 bogus duplicate");
    }
}
