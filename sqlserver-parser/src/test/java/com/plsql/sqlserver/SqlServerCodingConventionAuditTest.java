package com.plsql.sqlserver;

import com.plsql.sqlserver.engine.SqlServerAnalysisEngine;
import com.plsql.sqlserver.model.SqlServerCallInfo;
import com.plsql.sqlserver.model.SqlServerParamInfo;
import com.plsql.sqlserver.model.SqlServerRoutineInfo;
import com.plsql.sqlserver.model.SqlServerTableOpInfo;
import com.plsql.sqlserver.model.SqlServerVariableInfo;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AUDIT ONLY test — investigates whether common real-world T-SQL coding-standard /
 * formatting conventions (Hungarian naming, mixed casing, long param lists, deep nesting,
 * sparse semicolons, header comments, string concatenation + sp_executesql, and GO batch
 * separators) break correct capture by SqlServerAnalysisEngine / SqlServerAnalysisVisitor.
 *
 * This test does NOT touch main/ code. It documents ACTUAL behavior (which may be buggy)
 * with dump() output and assertions that record what really happens.
 */
class SqlServerCodingConventionAuditTest {

    private final SqlServerAnalysisEngine engine = new SqlServerAnalysisEngine();

    private SqlServerRoutineInfo parse(String src) {
        return engine.parse(src, "test_proc", null);
    }

    private void dump(String label, SqlServerRoutineInfo r) {
        System.out.println("=== " + label + " ===");
        System.out.println("params: " + r.getParams().stream()
                .map(p -> p.getName() + ":" + p.getDataType() + ":" + p.getDirection() + "@" + p.getLine()).toList());
        System.out.println("vars: " + r.getVariables().stream()
                .map(v -> v.getName() + ":" + v.getDataType() + "@" + v.getLine()).toList());
        System.out.println("calls: " + r.getCalls().stream()
                .map(c -> c.qualifiedName() + "@" + c.getLine()).toList());
        System.out.println("tableOps: " + r.getTableOps().stream()
                .map(t -> t.getDmlType() + ":" + t.qualifiedTable() + "@" + t.getLine()).toList());
        System.out.println("dynamicSql: " + r.getDynamicSql().stream()
                .map(d -> d.getKind() + "@" + d.getLine() + " src=" + d.getSqlSource()).toList());
        System.out.println("errors: " + r.getErrors());
        System.out.println("lineStart=" + r.getLineStart() + " lineEnd=" + r.getLineEnd());
    }

    // ── Dimension 1: Hungarian naming (locals @v_/@l_, params @p_/@i_/@io_/@o_, name that
    //    looks like a call @c_get_customer, constant-like naming) ──────────────────────────
    @Test
    void dim1_hungarianNaming_varsAndParamsCorrectlyClassified() {
        String src =
                "CREATE PROCEDURE usp_hungarian\n" +
                "    @p_customer_id INT,\n" +
                "    @i_input_flag BIT,\n" +
                "    @io_running_total DECIMAL(18,2),\n" +
                "    @o_result_code INT\n" +
                "AS\nBEGIN\n" +
                "    DECLARE @v_local_name NVARCHAR(100);\n" +
                "    DECLARE @l_temp_val INT;\n" +
                "    DECLARE @c_get_customer NVARCHAR(50);\n" + // name that LOOKS like a call
                "    DECLARE @K_MAX_RETRIES INT = 3;\n" +        // constant-like naming
                "    SET @v_local_name = 'x';\n" +
                "    SET @c_get_customer = 'not a call';\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("dim1_hungarianNaming", r);

        assertTrue(r.getParams().stream().anyMatch(p -> "@p_customer_id".equalsIgnoreCase(p.getName())));
        assertTrue(r.getParams().stream().anyMatch(p -> "@io_running_total".equalsIgnoreCase(p.getName())));
        assertTrue(r.getVariables().stream().anyMatch(v -> "@v_local_name".equalsIgnoreCase(v.getName())));
        assertTrue(r.getVariables().stream().anyMatch(v -> "@c_get_customer".equalsIgnoreCase(v.getName())),
                "expected @c_get_customer captured as a variable despite call-like name");
        // Must NOT be misclassified as a call just because its name contains "get_customer"
        assertFalse(r.getCalls().stream().anyMatch(c -> "get_customer".equalsIgnoreCase(c.getName())
                || "c_get_customer".equalsIgnoreCase(c.getName())),
                "documents whether @c_get_customer name leaks into calls: " + r.getCalls().stream()
                        .map(SqlServerCallInfo::qualifiedName).toList());
        assertTrue(r.getVariables().stream().anyMatch(v -> "@K_MAX_RETRIES".equalsIgnoreCase(v.getName())));

        // Bug 5 (fixed): SET @v = '<plain string>' must not be misclassified as dynamic SQL just
        // because the RHS is quoted - only a string whose content actually looks like SQL
        // (SELECT/INSERT/UPDATE/DELETE/EXEC/CREATE/DROP/ALTER/... ) should count.
        assertTrue(r.getDynamicSql().isEmpty(),
                "plain string literals assigned via SET must not be classified as dynamic SQL: "
                        + r.getDynamicSql());
    }

    // ── Dimension 2: mixed/inconsistent casing of keywords and identifiers ──────────────────
    @Test
    void dim2_mixedCasing_keywordsAndIdentifiers() {
        String src =
                "create PROCEDURE Usp_MixedCase\n" +
                "    @P_Id INT\n" +
                "AS\nbegin\n" +
                "    DECLARE @V_Name NVARCHAR(50);\n" +
                "    Set @V_Name = 'a';\n" +
                "    select @P_Id, @V_Name FROM Dbo.Customers WHERE Id = @P_Id;\n" +
                "    EXEC Dbo.usp_ChildProc @P_Id;\n" +
                "  END";
        SqlServerRoutineInfo r = parse(src);
        dump("dim2_mixedCasing", r);

        assertTrue(r.getParams().stream().anyMatch(p -> "@P_Id".equalsIgnoreCase(p.getName())));
        assertTrue(r.getVariables().stream().anyMatch(v -> "@V_Name".equalsIgnoreCase(v.getName())));
        assertTrue(r.getCalls().stream().anyMatch(c -> "usp_ChildProc".equalsIgnoreCase(c.getName())
                && "Dbo".equalsIgnoreCase(c.getSchema())));
        assertTrue(r.getTableOps().stream().anyMatch(t -> "Customers".equalsIgnoreCase(t.getTableName())));
        assertTrue(r.getErrors().isEmpty(), "unexpected parse errors on mixed-case input: " + r.getErrors());
    }

    // ── Dimension 3: 10+ params, one per line, trailing commas, inconsistent indentation,
    //    trailing comment per parameter ──────────────────────────────────────────────────────
    @Test
    void dim3_longParamList_withTrailingComments() {
        String src =
                "CREATE PROCEDURE usp_many_params\n" +
                "    @p1 INT, -- first param\n" +
                "        @p2 INT, -- second param, extra indent\n" +
                "@p3 INT, -- third, no indent\n" +
                "    @p4 NVARCHAR(50), -- fourth\n" +
                "    @p5 DECIMAL(18,2), -- fifth\n" +
                "    @p6 BIT, -- sixth\n" +
                "    @p7 DATETIME, -- seventh\n" +
                "    @p8 INT = 10, -- eighth with default\n" +
                "    @p9 INT OUTPUT, -- ninth, output param\n" +
                "    @p10 VARCHAR(MAX), -- tenth\n" +
                "    @p11 INT -- eleventh, no trailing comma\n" +
                "AS\nBEGIN\n" +
                "    SET @p9 = @p1 + @p2;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("dim3_longParamList", r);

        assertEquals(11, r.getParams().size(),
                "expected all 11 params captured, got: " + r.getParams().stream()
                        .map(SqlServerParamInfo::getName).toList());
        assertTrue(r.getParams().stream().anyMatch(p -> "@p9".equalsIgnoreCase(p.getName())
                && p.getDirection() == SqlServerParamInfo.Direction.OUT),
                "expected @p9 OUTPUT direction captured correctly");
        assertTrue(r.getErrors().isEmpty(), "unexpected errors: " + r.getErrors());
    }

    // ── Dimension 4: nested control flow — labeled block/GOTO, deep IF/ELSE, nested CASE,
    //    WHILE, real call buried at deepest nesting ─────────────────────────────────────────
    @Test
    void dim4_deepNesting_gotoLabel_realCallAtDeepestLevel() {
        String src =
                "CREATE PROCEDURE usp_deep_nesting\n" +
                "    @p_level INT\n" +
                "AS\nBEGIN\n" +
                "    DECLARE @v_i INT = 0;\n" +
                "    WHILE @v_i < 3\n" +
                "    BEGIN\n" +
                "        IF @p_level = 1\n" +
                "        BEGIN\n" +
                "            IF @p_level = 2\n" +
                "            BEGIN\n" +
                "                IF @p_level = 3\n" +
                "                BEGIN\n" +
                "                    IF @p_level = 4\n" +
                "                    BEGIN\n" +
                "                        IF @p_level = 5\n" +
                "                        BEGIN\n" +
                "                            DECLARE @v_case_result INT;\n" +
                "                            SET @v_case_result = CASE @p_level\n" +
                "                                WHEN 1 THEN CASE WHEN @v_i = 0 THEN 100 ELSE 200 END\n" +
                "                                ELSE 0\n" +
                "                            END;\n" +
                "                            EXEC dbo.usp_deepest_call @p_level;\n" +
                "                            GOTO DoneLabel;\n" +
                "                        END\n" +
                "                    END\n" +
                "                END\n" +
                "            END\n" +
                "        END\n" +
                "        SET @v_i = @v_i + 1;\n" +
                "    END\n" +
                "    DoneLabel:\n" +
                "    PRINT 'done';\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("dim4_deepNesting", r);

        assertTrue(r.getCalls().stream().anyMatch(c -> "usp_deepest_call".equalsIgnoreCase(c.getName())
                        && "dbo".equalsIgnoreCase(c.getSchema())),
                "expected call buried at deepest nesting captured: " + r.getCalls().stream()
                        .map(c -> c.qualifiedName() + "@" + c.getLine()).toList());
        // Check line number correctness: EXEC dbo.usp_deepest_call is on source line 22
        r.getCalls().stream().filter(c -> "usp_deepest_call".equalsIgnoreCase(c.getName())).findFirst()
                .ifPresent(c -> assertEquals(23, c.getLine(),
                        "line-number drift for deeply-nested call, actual=" + c.getLine()));
        System.out.println("dim4 errors=" + r.getErrors());
        // Bug 6 (fixed): the "DoneLabel:" GOTO-target label used to produce a lexer
        // token-recognition error (no COLON token existed). The call graph already recovered
        // correctly around it, so this only asserts getErrors() is now clean.
        assertTrue(r.getErrors().isEmpty(), "GOTO label must not produce a lexer error: " + r.getErrors());
    }

    // ── Dimension 5: multiple statements per line via semicolons; sparse/optional semicolons ──
    @Test
    void dim5_multiStatementsPerLine_andSparseSemicolons() {
        String src =
                "CREATE PROCEDURE usp_semicolons\n" +
                "AS\nBEGIN\n" +
                "    DECLARE @v_a INT DECLARE @v_b INT\n" +      // no semicolons at all
                "    SET @v_a = 1; SET @v_b = 2; EXEC dbo.usp_after_semi @v_a;\n" + // multi-stmt one line
                "    SELECT @v_a, @v_b\n" +                       // no trailing semicolon
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("dim5_multiStatementsPerLine", r);

        assertTrue(r.getVariables().stream().anyMatch(v -> "@v_a".equalsIgnoreCase(v.getName())));
        assertTrue(r.getVariables().stream().anyMatch(v -> "@v_b".equalsIgnoreCase(v.getName())),
                "expected @v_b captured despite missing semicolon before it: " + r.getVariables().stream()
                        .map(SqlServerVariableInfo::getName).toList());
        assertTrue(r.getCalls().stream().anyMatch(c -> "usp_after_semi".equalsIgnoreCase(c.getName())),
                "expected call captured when multiple statements share one line");
        assertTrue(r.getErrors().isEmpty(), "unexpected errors from sparse semicolons: " + r.getErrors());
    }

    // ── Dimension 6: large header comment block + per-procedure docblocks; line-number drift ──
    @Test
    void dim6_headerCommentBlocks_noLineNumberDrift() {
        String src =
                "/* =========================================================\n" +
                " * Module Header\n" +
                " * Author: someone\n" +
                " * Purpose: audit fixture\n" +
                " * ========================================================= */\n" +
                "\n" +
                "/**\n" +
                " * usp_after_header - docblock comment before the procedure\n" +
                " */\n" +
                "CREATE PROCEDURE usp_after_header\n" +
                "AS\nBEGIN\n" +
                "    DECLARE @v_x INT;\n" +               // line 13
                "    EXEC dbo.usp_called_after_header;\n" + // line 14
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("dim6_headerCommentBlocks", r);

        r.getVariables().stream().filter(v -> "@v_x".equalsIgnoreCase(v.getName())).findFirst()
                .ifPresentOrElse(
                        v -> assertEquals(13, v.getLine(), "line-number drift for @v_x, actual=" + v.getLine()),
                        () -> fail("@v_x not captured at all"));
        r.getCalls().stream().filter(c -> "usp_called_after_header".equalsIgnoreCase(c.getName())).findFirst()
                .ifPresentOrElse(
                        c -> assertEquals(14, c.getLine(), "line-number drift for call, actual=" + c.getLine()),
                        () -> fail("usp_called_after_header call not captured at all"));
    }

    // ── Dimension 7: long string built via + concatenation with comments interspersed,
    //    plus dynamic SQL via sp_executesql ─────────────────────────────────────────────────
    @Test
    void dim7_stringConcatenation_andDynamicSql() {
        String src =
                "CREATE PROCEDURE usp_dynamic_sql\n" +
                "    @p_table NVARCHAR(100)\n" +
                "AS\nBEGIN\n" +
                "    DECLARE @v_sql NVARCHAR(MAX);\n" +
                "    SET @v_sql = 'SELECT * ' +\n" +
                "                 -- from clause built dynamically\n" +
                "                 'FROM ' + @p_table +\n" +
                "                 /* where clause */\n" +
                "                 ' WHERE 1 = 1';\n" +
                "    EXEC sp_executesql @v_sql;\n" +
                "END";
        SqlServerRoutineInfo r = parse(src);
        dump("dim7_stringConcatenation", r);

        assertTrue(r.getVariables().stream().anyMatch(v -> "@v_sql".equalsIgnoreCase(v.getName())));
        System.out.println("dim7 dynamicSql entries=" + r.getDynamicSql().size());
        System.out.println("dim7 calls (sp_executesql visibility)=" + r.getCalls().stream()
                .map(SqlServerCallInfo::qualifiedName).toList());
        assertTrue(r.getErrors().isEmpty(),
                "unexpected errors on multi-line concatenation + sp_executesql: " + r.getErrors());
    }

    // ── Dimension 8: GO batch separators between multiple CREATE PROCEDURE statements ───────
    @Test
    void dim8_goBatchSeparators_multipleProceduresInOneScript() {
        String src =
                "CREATE PROCEDURE usp_batch_one\n" +
                "AS\nBEGIN\n" +
                "    EXEC dbo.usp_called_in_batch_one;\n" +
                "END\n" +
                "GO\n" +
                "CREATE PROCEDURE usp_batch_two\n" +
                "AS\nBEGIN\n" +
                "    EXEC dbo.usp_called_in_batch_two;\n" + // line 9
                "END\n" +
                "GO\n";
        // Bug 7 (fixed): compilationUnit only accepts one routine per parse() call, so two
        // CREATE PROCEDUREs separated by GO used to merge into a single parse and misattribute
        // the second procedure's calls to the first. engine.parseBatch() now splits on
        // standalone GO lines first and parses/returns each batch independently.
        java.util.List<SqlServerRoutineInfo> infos = engine.parseBatch(src, "test_proc", null);
        System.out.println("=== dim8_goBatchSeparators ===");
        for (SqlServerRoutineInfo r : infos) {
            System.out.println("name=" + r.getName() + " calls=" + r.getCalls().stream()
                    .map(c -> c.qualifiedName() + "@" + c.getLine()).toList() + " errors=" + r.getErrors());
        }

        assertEquals(2, infos.size(), "two GO-separated CREATE PROCEDUREs must parse into two routines");
        SqlServerRoutineInfo one = infos.get(0);
        SqlServerRoutineInfo two = infos.get(1);
        assertEquals("usp_batch_one", one.getName());
        assertEquals("usp_batch_two", two.getName());
        assertTrue(one.getCalls().stream().anyMatch(c -> "usp_called_in_batch_one".equalsIgnoreCase(c.getName())),
                "batch one's own call must be attributed to batch one");
        assertFalse(one.getCalls().stream().anyMatch(c -> "usp_called_in_batch_two".equalsIgnoreCase(c.getName())),
                "batch two's call must not leak into batch one");
        assertTrue(two.getCalls().stream().anyMatch(c -> "usp_called_in_batch_two".equalsIgnoreCase(c.getName())),
                "batch two's own call must be attributed to batch two");
        assertFalse(two.getCalls().stream().anyMatch(c -> "usp_called_in_batch_one".equalsIgnoreCase(c.getName())),
                "batch one's call must not leak into batch two");
    }
}
