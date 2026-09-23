package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Read-only audit (2026-09-17): probes Oracle nested/local subprogram handling and
 * overloaded-procedure handling against the real ANTLR parser/visitor
 * ({@code PlSqlAnalysisVisitor}) and PRINTS + asserts what was actually captured.
 *
 * Findings recorded here (see accompanying audit report):
 *  1. Local nested procedure calls from the enclosing body are misclassified as EXTERNAL
 *     instead of INTERNAL, because {@code currentPackageSubprograms} is only pre-populated
 *     from top-level {@code package_obj_body} children (PlSqlAnalysisVisitor.java ~L741-765),
 *     never from nested {@code seq_of_declare_specs}.
 *  2. Nested local subprograms are flattened into the enclosing package/object's top-level
 *     {@code subprograms} list, indistinguishable from real package-level siblings (no
 *     parent/child relationship recorded in SubprogramInfo).
 *  3. Overloaded same-name procedures/functions ARE both retained in the main
 *     {@code List<SubprogramInfo>} (no data loss there), but the internal
 *     {@code subprogramSourceMap} (Map<String,String> keyed by bare uppercased name) used for
 *     cross-procedure SQL-text scanning silently drops the first overload's source text.
 */
public class OracleNestedSubprogramAndOverloadAuditTest extends ParserTestBase {

    // ------------------------------------------------------------------
    // 1 & 2: nested local procedure — call attribution + scope flattening
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Nested local procedure: call attribution and scope flattening")
    void nestedLocalProcedureCallAttribution() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE OUTER_PROC IS
                    PROCEDURE LOCAL_HELPER IS
                    BEGIN
                      INSERT INTO helper_log (msg) VALUES ('nested');
                    END LOCAL_HELPER;
                  BEGIN
                    LOCAL_HELPER;
                    INSERT INTO main_log (msg) VALUES ('outer');
                  END OUTER_PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Nested local procedure");

        ParsedObject obj = result.getObjects().get(0);
        System.out.println("=== Nested local procedure audit ===");
        System.out.println("  top-level subprogram count = " + obj.getSubprograms().size());
        for (SubprogramInfo s : obj.getSubprograms()) {
            System.out.println("  subprogram present at top level: " + s.getName());
        }

        SubprogramInfo outer = findSub(result, "OUTER_PROC");
        SubprogramInfo inner = findSub(result, "LOCAL_HELPER");

        // FINDING 2 (structural gap): LOCAL_HELPER is retrievable via the SAME flat lookup
        // used for real package-level subprograms — nothing distinguishes it as nested/local.
        System.out.println("  outer found=" + (outer != null) + " inner found=" + (inner != null));
        if (outer != null) {
            System.out.println("  outer calls: ");
            for (CallInfo c : outer.getCalls()) {
                System.out.println("    call name=" + c.getName() + " type=" + c.getType());
            }
        }

        // FINDING 1: the call to LOCAL_HELPER from OUTER_PROC's body is misclassified.
        // EXPECTED (correct behavior): type should be INTERNAL (it's a local/nested call).
        // ACTUAL (observed bug): classifyCall() falls through to EXTERNAL because
        // currentPackageSubprograms only contains top-level package_obj_body names.
        boolean foundLocalHelperCall = false;
        String observedType = null;
        if (outer != null) {
            for (CallInfo c : outer.getCalls()) {
                if (c.getName() != null && c.getName().equalsIgnoreCase("LOCAL_HELPER")) {
                    foundLocalHelperCall = true;
                    observedType = c.getType();
                }
            }
        }
        System.out.println("  LOCAL_HELPER call found=" + foundLocalHelperCall
                + " observedType=" + observedType + " (BUG: expected INTERNAL, got EXTERNAL)");

        // FIXED: local nested procedure calls from the enclosing body are now correctly
        // classified as INTERNAL (see PlSqlAnalysisVisitor.localSubprogramStack /
        // isLocallyDeclaredSubprogram).
        org.junit.jupiter.api.Assertions.assertTrue(foundLocalHelperCall,
                "Expected a recorded call to LOCAL_HELPER from OUTER_PROC");
        org.junit.jupiter.api.Assertions.assertEquals("INTERNAL", observedType,
                "Local nested procedure call should be classified as INTERNAL");

        // Finding 2 evidence: LOCAL_HELPER ends up in the object's flat subprogram list,
        // same as a genuine top-level package member would.
        org.junit.jupiter.api.Assertions.assertNotNull(inner,
                "BUG EVIDENCE: nested local procedure is flattened into the top-level "
                        + "ParsedObject.subprograms list with no parent/child marker");
    }

    // ------------------------------------------------------------------
    // 3: overloaded procedures — both retained in main list (no bug),
    //    but source-text map for cross-procedure scanning clobbers one entry.
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Overloaded procedures: both retained in list, but source-text map collides")
    void overloadedProceduresBothRetainedInList() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY ORDER_PKG AS
                  PROCEDURE PROCESS_ORDER(p_id NUMBER) IS
                  BEGIN
                    UPDATE orders SET status = 'PROCESSED' WHERE id = p_id;
                  END PROCESS_ORDER;

                  PROCEDURE PROCESS_ORDER(p_id NUMBER, p_flag VARCHAR2) IS
                  BEGIN
                    UPDATE orders SET status = 'PROCESSED', flag_col = p_flag WHERE id = p_id;
                  END PROCESS_ORDER;

                  PROCEDURE CALLER_PROC IS
                  BEGIN
                    PROCESS_ORDER(1);
                    PROCESS_ORDER(1, 'Y');
                  END CALLER_PROC;
                END ORDER_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Overloaded procedures");

        ParsedObject obj = result.getObjects().get(0);
        List<SubprogramInfo> overloads = obj.getSubprograms().stream()
                .filter(s -> "PROCESS_ORDER".equalsIgnoreCase(s.getName()))
                .toList();

        System.out.println("=== Overload audit ===");
        System.out.println("  PROCESS_ORDER overload count in main list = " + overloads.size());
        for (SubprogramInfo s : overloads) {
            System.out.println("  overload: name=" + s.getName()
                    + " params=" + s.getParameters().size()
                    + " tableOps=" + s.getTableOperations().size());
        }

        // GOOD: main SubprogramInfo list is a List, not a name-keyed map — both overloads
        // survive. This confirms the pipeline's primary storage is NOT the vulnerable pattern.
        org.junit.jupiter.api.Assertions.assertEquals(2, overloads.size(),
                "Both PROCESS_ORDER overloads should be retained in the main subprogram list "
                        + "(this DOES pass — confirms List-based storage is safe)");

        // Each overload should have distinct table-operation/body content if visited correctly
        // (1-arg overload only sets status; 2-arg overload also sets flag_col).
        if (overloads.size() == 2) {
            boolean oneArgSeen = overloads.stream().anyMatch(s -> s.getParameters().size() == 1);
            boolean twoArgSeen = overloads.stream().anyMatch(s -> s.getParameters().size() == 2);
            System.out.println("  distinct-arity overloads both captured: "
                    + oneArgSeen + " / " + twoArgSeen);
            org.junit.jupiter.api.Assertions.assertTrue(oneArgSeen && twoArgSeen,
                    "Both distinct-arity overloads should be individually captured with correct "
                            + "parameter counts");
        }

        // CALLER_PROC's calls to both overloads should be recorded distinctly (by argument
        // count) even though exact overload-resolution isn't attempted.
        SubprogramInfo caller = findSub(result, "CALLER_PROC");
        System.out.println("  caller found=" + (caller != null));
        if (caller != null) {
            for (CallInfo c : caller.getCalls()) {
                System.out.println("    call name=" + c.getName() + " type=" + c.getType()
                        + " arguments=" + c.getArguments());
            }
            long processOrderCalls = caller.getCalls().stream()
                    .filter(c -> "PROCESS_ORDER".equalsIgnoreCase(c.getName()))
                    .count();
            System.out.println("  PROCESS_ORDER calls recorded from CALLER_PROC = " + processOrderCalls);
            org.junit.jupiter.api.Assertions.assertEquals(2, processOrderCalls,
                    "Both distinct-arity calls to the overloaded PROCESS_ORDER should be recorded "
                            + "(even without full overload resolution) — a crash or a dropped call "
                            + "here would indicate the overload confuses call recording");
        }
    }

    // ------------------------------------------------------------------
    // 3 (regression, FIXED): the internal subprogramSourceMap fallback used by dynamic-SQL
    // cross-procedure text scanning must not silently drop an earlier overload's source text
    // when a later overload of the same bare name is visited. GET_SQL_TEXT's first overload's
    // body contains a RETURN'd literal with a real table reference; its second overload's body
    // has no such literal. Before the fix, the (bare-name-keyed, single-String) map would end
    // up holding only the LAST overload's source (no match) — the EXECUTE IMMEDIATE fallback
    // below would then find nothing. After the fix, all overloads' sources are tried and the
    // first overload's match is found.
    // ------------------------------------------------------------------
    @Test
    @DisplayName("subprogramSourceMap: earlier overload's source is not clobbered by a later overload")
    void subprogramSourceMapDoesNotClobberEarlierOverload() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY SQL_PKG AS
                  FUNCTION GET_SQL_TEXT RETURN VARCHAR2 IS
                  BEGIN
                    RETURN 'SELECT * FROM secret_orders_table';
                  END GET_SQL_TEXT;

                  FUNCTION GET_SQL_TEXT(p_flag VARCHAR2) RETURN VARCHAR2 IS
                  BEGIN
                    RETURN p_flag;
                  END GET_SQL_TEXT;

                  PROCEDURE RUNNER IS
                  BEGIN
                    EXECUTE IMMEDIATE GET_SQL_TEXT;
                  END RUNNER;
                END SQL_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "subprogramSourceMap overload clobber regression");

        SubprogramInfo runner = findSub(result, "RUNNER");
        org.junit.jupiter.api.Assertions.assertNotNull(runner, "RUNNER should be found");

        System.out.println("=== subprogramSourceMap overload-clobber regression ===");
        for (DynamicSqlInfo d : runner.getDynamicSql()) {
            System.out.println("  dynSql type=" + d.getType() + " crossProcSourceHint=" + d.getCrossProcSourceHint()
                    + " unresolvedVarName=" + d.getUnresolvedVarName());
        }
        for (TableOperationInfo t : runner.getTableOperations()) {
            System.out.println("  tableOp table=" + t.getTableName() + " op=" + t.getOperation());
        }

        boolean foundSecretTable = runner.getTableOperations().stream()
                .anyMatch(t -> "secret_orders_table".equalsIgnoreCase(t.getTableName()));
        org.junit.jupiter.api.Assertions.assertTrue(foundSecretTable,
                "The zero-arg GET_SQL_TEXT overload's RETURN'd SQL text (with a real table "
                        + "reference) must still be found via the cross-procedure source-scan "
                        + "fallback, even though a second, later-declared overload of the same "
                        + "bare name has no such literal — proves the first overload's cached "
                        + "source text was not clobbered/dropped");
    }
}
