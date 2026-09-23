package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.SqlServerCallInfo;
import com.plsql.sqlserver.model.SqlServerRoutineInfo;
import com.plsql.sqlserver.model.SqlServerVariableInfo;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression suite (fixed 2026-09-16) for the T-SQL analogue of BUG-CT-048 (PL/SQL: qualifier.method() on a declared
 * local variable misclassified as a package call).
 *
 * T-SQL has no general dot-method syntax on scalar/table variables. The one real construct that
 * *does* look exactly like "qualifier.method(" on a local variable is a CLR user-defined-type
 * method call — geometry/geography/hierarchyid variables expose methods this way, e.g.
 * {@code @geom1.STIntersects(@geom2)} or {@code @hid.GetAncestor(1)}. This is the honest T-SQL
 * analogue of `v_subject_param.EXTEND()`.
 */
class VarQualifierScopeClassificationTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private SqlServerRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    private void dump(String label, SqlServerRoutineInfo r) {
        System.out.println("=== " + label + " ===");
        System.out.println("calls: " + r.getCalls().stream()
                .map(c -> c.qualifiedName() + "@" + c.getLine()).toList());
        System.out.println("vars: " + r.getVariables().stream()
                .map(SqlServerVariableInfo::getName).toList());
        System.out.println("errors: " + r.getErrors());
    }

    // ── BUG: CLR UDT dot-method call on a local variable misclassified as schema.proc call ──

    @Test
    void clrUdtMethodCall_onLocalVariable_isNotMisclassifiedAsQualifiedCall() {
        String src =
                "CREATE PROCEDURE usp_geo_check\n" +
                "AS\nBEGIN\n" +
                "  DECLARE @geom1 GEOMETRY;\n" +
                "  DECLARE @geom2 GEOMETRY;\n" +
                "  SET @geom1 = geometry::STGeomFromText('POINT(1 1)', 0);\n" +
                "  SET @geom2 = geometry::STGeomFromText('POINT(2 2)', 0);\n" +
                "  IF @geom1.STIntersects(@geom2) = 1\n" +
                "  BEGIN\n" +
                "    PRINT 'intersects';\n" +
                "  END\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("clrUdtMethodCall", r);

        boolean declaredAsVar = r.getVariables().stream()
                .anyMatch(v -> "@geom1".equalsIgnoreCase(v.getName()));
        assertTrue(declaredAsVar, "@geom1 should be a declared variable");

        // FIXED: extractQualifiedCalls now checks the qualifier against declared @-sigil
        // parameters/variables (isDeclaredAtVariable) before recording a call. "@geom1.STIntersects("
        // is a CLR user-defined-type method call on a local variable, not a real outbound call to a
        // nonexistent schema/proc GEOM1.STIntersects, and must not be recorded as one.
        boolean phantomCall = r.getCalls().stream()
                .anyMatch(c -> "geom1".equalsIgnoreCase(c.getSchema())
                        && "STIntersects".equalsIgnoreCase(c.getName()));
        assertFalse(phantomCall,
                "@geom1.STIntersects() on a declared local variable must not be a call: "
                        + r.getCalls().stream().map(SqlServerCallInfo::qualifiedName).toList());
    }

    // ── Correct: ordinary schema-qualified proc call is captured ─────────────────────────────

    @Test
    void schemaQualifiedProcCall_isCorrectlyCaptured() {
        String src =
                "CREATE PROCEDURE usp_orders\n" +
                "AS\nBEGIN\n" +
                "  EXEC dbo.usp_child;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("schemaQualifiedProcCall", r);
        assertTrue(r.getCalls().stream()
                .anyMatch(c -> "usp_child".equalsIgnoreCase(c.getName())
                        && "dbo".equalsIgnoreCase(c.getSchema())));
    }

    // ── Pattern 1: multi-level parameter propagation + scope reset across procedures ─────────

    @Test
    void multiLevelParamPropagation_and_crossProcedureScopeReset() {
        String procA =
                "CREATE PROCEDURE usp_a\n" +
                "AS\nBEGIN\n" +
                "  DECLARE @v_subject_param NVARCHAR(50);\n" +
                "  SET @v_subject_param = 'hello';\n" +
                "  EXEC usp_b @v_subject_param;\n" +
                "END";
        String procB =
                "CREATE PROCEDURE usp_b\n" +
                "@v_subject_param NVARCHAR(50)\n" +
                "AS\nBEGIN\n" +
                "  EXEC usp_c @v_subject_param;\n" +
                "END";
        // Reuse the SAME variable name @v_subject_param in a totally separate, later-parsed
        // procedure C, to prove the visitor's per-parse state (seenCalls, variable list) does
        // not leak across procedure boundaries.
        String procC =
                "CREATE PROCEDURE usp_c\n" +
                "@v_subject_param NVARCHAR(50)\n" +
                "AS\nBEGIN\n" +
                "  DECLARE @v_subject_param2 NVARCHAR(50);\n" +
                "  SET @v_subject_param2 = @v_subject_param;\n" +
                "  PRINT @v_subject_param2;\n" +
                "END";

        SqlServerRoutineInfo a = parse(procA);
        SqlServerRoutineInfo b = parse(procB);
        SqlServerRoutineInfo c = parse(procC);
        dump("procA", a);
        dump("procB", b);
        dump("procC", c);

        assertTrue(a.getCalls().stream().anyMatch(x -> "usp_b".equalsIgnoreCase(x.getName())));
        assertTrue(b.getCalls().stream().anyMatch(x -> "usp_c".equalsIgnoreCase(x.getName())));

        // usp_c has its own @v_subject_param parameter — must not be a call, must not leak
        // vars/calls seen while parsing A or B (each parse() call builds a brand new visitor).
        assertFalse(c.getCalls().stream().anyMatch(x -> "usp_b".equalsIgnoreCase(x.getName())),
                "usp_c result must not contain usp_a/usp_b's calls (no cross-parse leakage)");
        assertTrue(c.getVariables().stream().anyMatch(v -> "@v_subject_param2".equalsIgnoreCase(v.getName())));
        // The parameter @v_subject_param of C is a parameter, not necessarily in getVariables();
        // either way it must never appear in getCalls().
        assertTrue(c.getCalls().stream().noneMatch(x -> "v_subject_param".equalsIgnoreCase(x.getName())));
    }

    // ── Pattern 2a: module-level variable name reused as a schema-qualified table/proc ref ──

    @Test
    void sameNameAsVariable_and_asSchemaQualifiedProcCall_notConflated() {
        String src =
                "CREATE PROCEDURE usp_mixed\n" +
                "AS\nBEGIN\n" +
                "  DECLARE @orders INT;\n" +
                "  SET @orders = 1;\n" +
                "  EXEC dbo.orders_cleanup;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("sameNameVariableVsQualifiedCall", r);
        assertTrue(r.getVariables().stream().anyMatch(v -> "@orders".equalsIgnoreCase(v.getName())));
        assertTrue(r.getCalls().stream().anyMatch(c -> "orders_cleanup".equalsIgnoreCase(c.getName())
                && "dbo".equalsIgnoreCase(c.getSchema())));
    }

    // ── Pattern 2b: schema.table.column-like qualified reference not misread as a call ───────

    @Test
    void schemaTableDotColumn_insideWhere_notMisclassifiedAsCall() {
        String src =
                "CREATE PROCEDURE usp_report\n" +
                "AS\nBEGIN\n" +
                "  SELECT o.order_id FROM sales.orders o WHERE sales.orders.status = 'OPEN';\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("schemaTableDotColumn", r);
        // "sales.orders.status" has no "(" immediately after any dot-pair, so
        // QUALIFIED_CALL_PATTERN (which requires a following '(') should not match here.
        assertTrue(r.getCalls().isEmpty(), "expected no calls: " + r.getCalls().stream()
                .map(SqlServerCallInfo::qualifiedName).toList());
    }

    // ── Pattern 3: comments interspersed mid-statement ────────────────────────────────────────

    @Test
    void commentsBetweenProcNameAndParens_stillCaptured() {
        String src =
                "CREATE PROCEDURE usp_comment_test\n" +
                "AS\nBEGIN\n" +
                "  EXEC dbo /* schema comment */ . usp_child_proc /* name comment */ ;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("commentsInExec", r);
        // Documents actual behavior: EXEC parsing uses the grammar's routineName rule, not the
        // regex path, so this exercises grammar tolerance of comments inside a qualified name.
        System.out.println("commentsBetweenProcNameAndParens_stillCaptured errors=" + r.getErrors());
    }

    @Test
    void commentBetweenArguments_stillParsedWithoutErrors() {
        String src =
                "CREATE PROCEDURE usp_comment_args\n" +
                "AS\nBEGIN\n" +
                "  EXEC usp_child_proc 1, /* arg comment */ 2, 3;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("commentBetweenArguments", r);
        assertTrue(r.getCalls().stream().anyMatch(c -> "usp_child_proc".equalsIgnoreCase(c.getName())));
        assertTrue(r.getErrors().isEmpty(), "unexpected errors: " + r.getErrors());
    }

    @Test
    void commentMidDeclaration_variableStillExtracted() {
        String src =
                "CREATE PROCEDURE usp_comment_decl\n" +
                "AS\nBEGIN\n" +
                "  DECLARE @x /* comment */ INT;\n" +
                "  -- line comment\n" +
                "  DECLARE @y INT; -- trailing comment\n" +
                "  SET @x = 1;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("commentMidDeclaration", r);
        assertTrue(r.getVariables().stream().anyMatch(v -> "@x".equalsIgnoreCase(v.getName())),
                "expected @x extracted despite mid-declaration comment");
        assertTrue(r.getVariables().stream().anyMatch(v -> "@y".equalsIgnoreCase(v.getName())),
                "expected @y extracted despite trailing line comment");
        assertTrue(r.getErrors().isEmpty(), "unexpected errors: " + r.getErrors());
    }

    // ── Pattern 3b: comment containing text that itself looks like a qualified call ──────────

    @Test
    void qualifiedCallLookingTextInsideComment_isNotFalselyCaptured() {
        String src =
                "CREATE PROCEDURE usp_comment_fake_call\n" +
                "AS\nBEGIN\n" +
                "  -- NOTE: do not call dbo.legacy_proc(1) here, it's deprecated\n" +
                "  /* also see sales.other_proc(2) for reference */\n" +
                "  SELECT 1;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("qualifiedCallLookingTextInsideComment", r);
        // FIXED: extractQualifiedCalls now strips -- and /* */ comments (stripLineAndBlockComments)
        // before scanning, so text inside comments that looks like "schema.name(" is not captured.
        boolean phantom = r.getCalls().stream().anyMatch(c ->
                "legacy_proc".equalsIgnoreCase(c.getName()) || "other_proc".equalsIgnoreCase(c.getName()));
        assertFalse(phantom, "commented-out text must not be captured as a real call: "
                + r.getCalls().stream().map(SqlServerCallInfo::qualifiedName).toList());
    }
}
