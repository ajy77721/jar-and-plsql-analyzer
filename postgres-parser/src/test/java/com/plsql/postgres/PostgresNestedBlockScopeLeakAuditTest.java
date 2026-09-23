package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Read-only audit (2026-09-17): Postgres does NOT support nested function/procedure
 * DEFINITIONS (confirmed: {@code routineDefinition} in PostgresParser.g4 is only reachable
 * from {@code compilationUnit}, never from {@code statement}/{@code body} — any nested
 * {@code CREATE FUNCTION}/{@code CREATE PROCEDURE} text is instead swallowed opaquely by the
 * permissive {@code createStatement : CREATE (~SEMI)* SEMI} rule). This is CORRECT and
 * matches real PL/pgSQL semantics — no test needed for that half.
 *
 * Postgres DOES support nested {@code BEGIN ... END} blocks (PostgresParser.g4 beginBlock,
 * reachable recursively from {@code statement}/{@code body}). This test documents a real
 * scope-leak bug: {@code PostgresRoutineInfo.variables} is a single flat {@code ArrayList}
 * for the whole routine (PostgresAnalysisVisitor.java visitDeclareVariableStmt /
 * visitPreBeginDeclareItem add to {@code result.getVariables()} directly, ~L151/L196), with
 * no push/pop of scope around nested {@code beginBlock} visitation. So a variable declared
 * inside an inner block remains visible to {@code isDeclaredVariableInScope} for the rest of
 * the routine, including code AFTER the inner block has closed — a real (if narrow) scope
 * leak, since real PL/pgSQL would raise "column reference is ambiguous"/scope error or simply
 * not see the inner variable outside its block.
 */
class PostgresNestedBlockScopeLeakAuditTest {

    private final PostgresAnalysisEngine engine = new PostgresAnalysisEngine();

    private PostgresRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    @Test
    @DisplayName("Nested BEGIN block variable leaks into flat routine-wide variable list")
    void nestedBlockVariableLeaksToOuterScope() {
        String sql = """
                CREATE OR REPLACE FUNCTION outer_fn() RETURNS void AS $$
                BEGIN
                    BEGIN
                        DECLARE v_inner INT;
                        v_inner := 1;
                    END;
                    -- v_inner is out of scope here in real PL/pgSQL, but the flat
                    -- visitor model still "remembers" it as a routine variable.
                    RAISE NOTICE 'after inner block';
                END;
                $$ LANGUAGE plpgsql;
                """;
        PostgresRoutineInfo r = parse(sql);

        System.out.println("=== Nested block scope-leak audit ===");
        System.out.println("  errors=" + r.getErrors());
        System.out.println("  variables=" + r.getVariables().stream().map(PostgresVariableInfo::getName).toList());

        boolean innerVarPresentInFlatRoutineScope = r.getVariables().stream()
                .anyMatch(v -> v.getName() != null && v.getName().equalsIgnoreCase("v_inner"));

        // FIXED (reporting list intentionally kept flat/backward-compatible): v_inner still
        // shows up in the full flat PostgresRoutineInfo.variables list for reporting purposes.
        assertTrue(innerVarPresentInFlatRoutineScope,
                "v_inner should still be reported in the routine-wide flat variables list "
                        + "(reporting list is intentionally left flat)");
    }

    // ------------------------------------------------------------------
    // Scope-check regression: after the fix, a nested-block-local variable must NOT be
    // considered "in scope" once its declaring block has closed. We probe this indirectly via
    // isDeclaredVariableInScope's only observable effect: attribute-notation call sugar
    // (x.f() == f(x)) is applied only when the qualifier resolves to an in-scope variable.
    // Once v_inner is out of scope, `v_inner.foo()` after the block must be recorded as a real
    // external call qualified by "v_inner" (not silently rewritten to an unqualified foo() call).
    // ------------------------------------------------------------------
    @Test
    @DisplayName("Nested-block variable is out of scope after its block closes (scope-check, not just reporting list)")
    void nestedBlockVariableOutOfScopeAfterClose() {
        String sql = """
                CREATE OR REPLACE FUNCTION outer_fn3() RETURNS void AS $$
                BEGIN
                    BEGIN
                        DECLARE v_inner INT;
                        v_inner := 1;
                    END;
                    PERFORM v_inner.foo();
                END;
                $$ LANGUAGE plpgsql;
                """;
        PostgresRoutineInfo r = parse(sql);
        System.out.println("=== Scope-check regression ===");
        for (PostgresCallInfo c : r.getCalls()) {
            System.out.println("  call schema=" + c.getSchema() + " name=" + c.getName());
        }
        boolean recordedAsQualifiedExternalCall = r.getCalls().stream()
                .anyMatch(c -> "v_inner".equalsIgnoreCase(c.getSchema()) && "foo".equalsIgnoreCase(c.getName()));
        assertTrue(recordedAsQualifiedExternalCall,
                "v_inner is out of scope after its declaring block closed, so v_inner.foo() must "
                        + "be recorded as a real (schema-qualified) external call, not rewritten via "
                        + "attribute-notation sugar as if v_inner were still an in-scope variable");
    }

    @Test
    @DisplayName("Two sibling nested blocks each declaring same-named variable both leak into one flat list")
    void siblingBlocksSameVariableNameBothLeak() {
        String sql = """
                CREATE OR REPLACE FUNCTION outer_fn2() RETURNS void AS $$
                BEGIN
                    BEGIN
                        DECLARE v_temp INT;
                        v_temp := 1;
                    END;
                    BEGIN
                        DECLARE v_temp TEXT;
                        v_temp := 'x';
                    END;
                END;
                $$ LANGUAGE plpgsql;
                """;
        PostgresRoutineInfo r = parse(sql);

        System.out.println("=== Sibling-block same-name variable audit ===");
        System.out.println("  errors=" + r.getErrors());
        long vTempCount = r.getVariables().stream()
                .filter(v -> v.getName() != null && v.getName().equalsIgnoreCase("v_temp"))
                .count();
        System.out.println("  v_temp entries in flat routine-wide list = " + vTempCount);

        // Both sibling blocks' declarations of v_temp (of different, mutually exclusive types)
        // correctly coexist in the flat reporting list (kept flat/backward-compatible by
        // design); scope-check correctness (not this reporting list) is what was actually
        // fixed — see nestedBlockVariableOutOfScopeAfterClose above.
        assertEquals(2, vTempCount,
                "Both block-scoped declarations of the same variable name from non-overlapping "
                        + "sibling blocks should be reported in the flat routine-wide variable list");
    }
}
