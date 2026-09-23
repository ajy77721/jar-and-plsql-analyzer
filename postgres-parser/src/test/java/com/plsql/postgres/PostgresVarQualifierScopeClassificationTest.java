package com.plsql.postgres;

import com.plsql.postgres.engine.PostgresAnalysisEngine;
import com.plsql.postgres.model.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression suite (fixed 2026-09-16) for BUG-CT-048 analog — postgres-parser.
 *
 * PostgresAnalysisVisitor.visitFunctionCall (line ~536-559 in
 * PostgresAnalysisVisitor.java) records ANY {@code id.id(args)} shape as a call whenever a
 * schema-qualifier is present — it never checks whether that qualifier is actually a declared
 * parameter/variable of the current routine (there is no isDeclaredVariableInScope-equivalent
 * anywhere in this module; grep confirms zero scope-tracking machinery exists in
 * postgres-parser/src/main/java).
 *
 * PostgreSQL/PL/pgSQL genuinely supports "attribute notation" call sugar: for a
 * single-argument function, {@code x.f()} is parsed by the real Postgres engine identically
 * to {@code f(x)}. So {@code v_param.upper()} on a local variable is legal PL/pgSQL and is NOT
 * a call to a schema named v_param — this mirrors Oracle's `v_subject_param.EXTEND()`
 * misclassification (BUG-CT-048) exactly, one syntax layer removed.
 *
 * Also note the condition at that line is `schema != null || !BUILTIN_FUNCTIONS.contains(name)`
 * — the `schema != null` disjunct short-circuits true even when name IS a recognized builtin
 * (e.g. UPPER/TRIM), so builtin-sugar calls are also wrongly recorded once *any* qualifier is
 * present, declared-variable or not.
 */
class PostgresVarQualifierScopeClassificationTest {

    private final PostgresAnalysisEngine engine = new PostgresAnalysisEngine();

    private PostgresRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    private void dump(String label, PostgresRoutineInfo r) {
        System.out.println("== " + label + " ==");
        System.out.println("  vars=" + r.getVariables().stream().map(PostgresVariableInfo::getName).toList());
        System.out.println("  params=" + r.getParams().stream().map(PostgresParamInfo::getName).toList());
        for (PostgresCallInfo c : r.getCalls()) {
            System.out.println("  call: schema=" + c.getSchema() + " name=" + c.getName() + " line=" + c.getLine());
        }
        System.out.println("  errors=" + r.getErrors());
    }

    // ── BUG: attribute-notation call on a declared parameter is misclassified as schema.func ──

    @Test
    void paramAttributeNotationCall_isNotMisclassifiedAsExternalSchemaCall() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE test(IN v_param VARCHAR(10))\nLANGUAGE plpgsql\nBEGIN\n" +
                "  DECLARE v_out VARCHAR(10);\n" +
                "  SET v_out = v_param.upper();\n" +
                "END");
        dump("paramAttributeNotationCall", r);

        // FIXED: v_param is a declared IN parameter; v_param.upper() means upper(v_param) — a
        // call to the builtin UPPER, not a call to a schema "v_param". Since UPPER is itself a
        // recognized builtin, the fixed builtin filter also means it is not recorded at all.
        boolean recordedAsSchemaCall = r.getCalls().stream()
                .anyMatch(c -> "v_param".equalsIgnoreCase(c.getSchema()) && "upper".equalsIgnoreCase(c.getName()));
        assertFalse(recordedAsSchemaCall,
                "v_param (a declared parameter) must not be recorded as a schema " +
                "qualifier for a phantom call to 'upper', calls=" + r.getCalls());
    }

    @Test
    void declaredLocalVariableAttributeNotationCall_isNotMisclassified() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE test()\nLANGUAGE plpgsql\nBEGIN\n" +
                "  DECLARE v_name VARCHAR(50);\n" +
                "  DECLARE v_trimmed VARCHAR(50);\n" +
                "  SET v_name = 'hello';\n" +
                "  SET v_trimmed = v_name.trim();\n" +
                "END");
        dump("declaredLocalVariableAttributeNotationCall", r);

        boolean recordedAsSchemaCall = r.getCalls().stream()
                .anyMatch(c -> "v_name".equalsIgnoreCase(c.getSchema()) && "trim".equalsIgnoreCase(c.getName()));
        assertFalse(recordedAsSchemaCall,
                "v_name (a declared local var) must not be treated as a schema, calls=" + r.getCalls());
    }

    // ── Pattern 2: real schema-qualified external call must still be recorded correctly ──

    @Test
    void realExternalSchemaQualifiedCall_recordedCorrectly_alongsideVarAttributeSugar() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE test(IN v_param VARCHAR(10))\nLANGUAGE plpgsql\nBEGIN\n" +
                "  DECLARE v_out VARCHAR(10);\n" +
                "  CALL other_schema.some_func(v_param);\n" +
                "  SET v_out = v_param.upper();\n" +
                "END");
        dump("realExternalCall_plus_varSugar", r);

        assertTrue(r.getCalls().stream().anyMatch(c ->
                "other_schema".equalsIgnoreCase(c.getSchema()) && "some_func".equalsIgnoreCase(c.getName())),
                "the real external call must still be recorded");
        // FIXED: the attribute-notation sugar on v_param must not appear as a phantom schema call
        // alongside the real one.
        assertFalse(r.getCalls().stream().anyMatch(c ->
                "v_param".equalsIgnoreCase(c.getSchema()) && "upper".equalsIgnoreCase(c.getName())));
    }

    // ── Composite/record field access WITHOUT parens is safe (different grammar path) ──

    @Test
    void recordFieldAccessWithoutParens_isNotMisclassifiedAsCall() {
        // myrecvar.fieldname (no parens) does not match the functionCall rule
        // `id (DOT id)? LPAREN ... RPAREN` at all, so it never reaches visitFunctionCall.
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE test()\nLANGUAGE plpgsql\nBEGIN\n" +
                "  DECLARE v_rec RECORD;\n" +
                "  DECLARE v_id INT;\n" +
                "  SET v_id = v_rec.id;\n" +
                "END");
        dump("recordFieldAccessWithoutParens", r);
        assertTrue(r.getCalls().isEmpty(),
                "field access without parens must not be mistaken for a call, calls=" + r.getCalls());
    }

    // ── Pattern 1: multi-level parameter propagation across independently-parsed routines ──
    // NOTE: PostgresAnalysisEngine.parse()/compilationUnit is single-routine
    // (`(routineDefinition | doStatement)? EOF`), so a "multi-procedure file" is necessarily
    // parsed as N independent engine.parse() calls, each with its own fresh
    // PostgresAnalysisVisitor/PostgresRoutineInfo. This structurally rules out the
    // cross-sibling-procedure phantom-variable leak that affects the PL/SQL package visitor,
    // because there is no shared visitor state across routines to leak from. Verified below.

    @Test
    void multiLevelParamPropagation_recordFieldOnOwnParam_notMisclassified_acrossThreeRoutines() {
        // A(v_rec) calls B(v_rec) calls C(v_rec); inside C, v_rec.field_name accessed (no parens)
        String procA =
                "CREATE PROCEDURE proc_a(IN v_rec RECORD)\nLANGUAGE plpgsql\nBEGIN\n" +
                "  CALL proc_b(v_rec);\nEND";
        String procB =
                "CREATE PROCEDURE proc_b(IN v_rec RECORD)\nLANGUAGE plpgsql\nBEGIN\n" +
                "  CALL proc_c(v_rec);\nEND";
        String procC =
                "CREATE PROCEDURE proc_c(IN v_rec RECORD)\nLANGUAGE plpgsql\nBEGIN\n" +
                "  DECLARE v_out INT;\n" +
                "  SET v_out = v_rec.field_name;\nEND";

        PostgresRoutineInfo a = engine.parse(procA, "proc_a", null);
        PostgresRoutineInfo b = engine.parse(procB, "proc_b", null);
        PostgresRoutineInfo c = engine.parse(procC, "proc_c", null);
        dump("proc_a", a); dump("proc_b", b); dump("proc_c", c);

        assertTrue(a.getCalls().stream().anyMatch(x -> "proc_b".equalsIgnoreCase(x.getName())));
        assertTrue(b.getCalls().stream().anyMatch(x -> "proc_c".equalsIgnoreCase(x.getName())));
        // C's field access on its own parameter must not appear as any kind of call at all
        assertTrue(c.getCalls().isEmpty(), "field access on own param wrongly recorded as call: " + c.getCalls());
    }

    @Test
    void scopeResetAcrossSubprograms_sameVarNameReused_noLeak() {
        String procA =
                "CREATE PROCEDURE proc_x()\nLANGUAGE plpgsql\nBEGIN\n" +
                "  DECLARE v_shared INT;\n  SET v_shared = 1;\nEND";
        String procB =
                "CREATE PROCEDURE proc_y()\nLANGUAGE plpgsql\nBEGIN\n" +
                "  SET v_shared = 2;\nEND"; // v_shared never declared in proc_y

        PostgresRoutineInfo a = engine.parse(procA, "proc_x", null);
        PostgresRoutineInfo b = engine.parse(procB, "proc_y", null);
        dump("proc_x", a); dump("proc_y", b);

        assertEquals(1, a.getVariables().size());
        assertEquals("v_shared", a.getVariables().get(0).getName());
        // proc_y must not inherit proc_x's v_shared declaration — confirms fresh state per parse()
        assertTrue(b.getVariables().isEmpty(),
                "proc_y must have no leaked variables from proc_x, got: " + b.getVariables());
    }

    // ── Pattern 2b: same variable name at global level, same schema vs different schema ──

    @Test
    void sameVarName_sameSchemaRealCall_vsQualifiedDifferentSchemaCall() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE hrschema.test(IN v_emp RECORD)\nLANGUAGE plpgsql\nBEGIN\n" +
                "  CALL hrschema.emp_promote(v_emp);\n" +
                "  CALL other_schema.audit_log(v_emp);\n" +
                "END");
        dump("sameVarName_sameSchema_vs_diffSchema", r);

        assertTrue(r.getCalls().stream().anyMatch(c ->
                "hrschema".equalsIgnoreCase(c.getSchema()) && "emp_promote".equalsIgnoreCase(c.getName())));
        assertTrue(r.getCalls().stream().anyMatch(c ->
                "other_schema".equalsIgnoreCase(c.getSchema()) && "audit_log".equalsIgnoreCase(c.getName())));
    }

    // ── Pattern 3: comments interspersed mid-statement ─────────────────────────────────────

    @Test
    void commentsBetweenCallNameAndParens_stillCaptured() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE test()\nLANGUAGE plpgsql\nBEGIN\n" +
                "  CALL process_orders /* mid-name comment */ ();\n" +
                "END");
        dump("commentsBetweenNameAndParens", r);
        assertTrue(r.getCalls().stream().anyMatch(c -> "process_orders".equalsIgnoreCase(c.getName())),
                "call should be captured despite comment before parens: " + r.getCalls());
    }

    @Test
    void commentsBetweenArgs_stillCapturesCallAndAllArgsImplicitly() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE test()\nLANGUAGE plpgsql\nBEGIN\n" +
                "  CALL log_audit('START', -- trailing line comment\n" +
                "                  'SYSTEM', /* block comment */ NULL);\n" +
                "END");
        dump("commentsBetweenArgs", r);
        assertTrue(r.getCalls().stream().anyMatch(c -> "log_audit".equalsIgnoreCase(c.getName())),
                "call should be captured despite comments between args: " + r.getCalls());
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
    }

    @Test
    void commentsMidDeclaration_variableStillExtracted() {
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE test()\nLANGUAGE plpgsql\nBEGIN\n" +
                "  DECLARE -- comment right after DECLARE\n" +
                "    v_count /* comment before type */ INT DEFAULT 0;\n" +
                "END");
        dump("commentsMidDeclaration", r);
        assertTrue(r.getVariables().stream().anyMatch(v -> "v_count".equalsIgnoreCase(v.getName())),
                "variable should still be extracted despite mid-declaration comments: " + r.getVariables());
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors: " + r.getErrors());
    }

    @Test
    void commentsBetweenSchemaAndCallName_stillClassifiesCorrectly() {
        // Also exercises pattern 1+2 combined: comment sits right where the qualifier/name split
        // happens, which is exactly the boundary the addCall-equivalent logic inspects.
        PostgresRoutineInfo r = parse(
                "CREATE PROCEDURE test()\nLANGUAGE plpgsql\nBEGIN\n" +
                "  CALL hrschema /* schema comment */ . emp_promote(1);\n" +
                "END");
        dump("commentsBetweenSchemaAndCallName", r);
        assertTrue(r.getCalls().stream().anyMatch(c ->
                "hrschema".equalsIgnoreCase(c.getSchema()) && "emp_promote".equalsIgnoreCase(c.getName())),
                "call=" + r.getCalls() + " errors=" + r.getErrors());
    }
}
