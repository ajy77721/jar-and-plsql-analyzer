package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

/**
 * AUDIT ONLY (2026-09-16). Not a regression suite; not asserting pass/fail hard in most places —
 * prints real captured output for each of the 8 real-world Oracle coding-convention dimensions so
 * findings can be reported. Some assertions are kept where a clear expectation exists; others are
 * observational (printed only) because the "expected" behavior for a formatting quirk is itself
 * the subject of the audit.
 */
public class OracleCodingConventionAuditTest extends ParserTestBase {

    // NOTE: numbers below are 1-based source line numbers within this string, counting from the
    // first line of the text block (the line containing "CREATE OR REPLACE PACKAGE BODY").
    private static final String SQL = """
            -- =====================================================================
            -- Package    : HR_PAYROLL_PKG
            -- Author     : J. Smith
            -- Created    : 2019-03-14
            -- JIRA       : PAY-1234, PAY-1250, PAY-1301
            -- -----------------------------------------------------------------------
            -- Revision History
            -- Date        Author        JIRA       Description
            -- ----------  ------------  ---------  --------------------------------
            -- 2019-03-14  J. Smith      PAY-1234   Initial version
            -- 2020-07-02  A. Kumar      PAY-1250   Added bonus calc
            -- 2022-11-19  R. Patel      PAY-1301   Refactor for multi-currency
            -- =====================================================================
            CREATE OR REPLACE PACKAGE BODY HR_PAYROLL_PKG AS

              g_default_currency VARCHAR2(3) := 'USD';
              c_max_retries CONSTANT NUMBER := 3;

              /* ---------------------------------------------------------------
               * Procedure : c_get_customer
               * Purpose   : NOTE - this is a VARIABLE, not a cursor/call, despite the c_ prefix
               *             and the name looking exactly like a "get" routine.
               * --------------------------------------------------------------- */
              PROCEDURE demo_naming_proc IS
                v_result        NUMBER;
                l_temp          VARCHAR2(100);
                c_get_customer  NUMBER := 42;
                r_emp           employees%ROWTYPE;
                t_ids           DBMS_SQL.NUMBER_TABLE;
                g_local_shadow  employees.salary%TYPE;
              BEGIN
                v_result := c_get_customer + 1;
                l_temp := 'x';
                r_emp.salary := g_local_shadow;
              END demo_naming_proc;

              /* =========================================================
               * Procedure : mixed_case_proc
               * Mixed-case keyword test.
               * ========================================================= */
              PROCEDURE mixed_case_proc IS
                v_count NUMBER;
              Begin
                select count(*) into v_count from employees;
                BEGIN
                  NULL;
                END;
              End mixed_case_proc;

              -----------------------------------------------------------------
              -- Procedure: many_params_proc
              -- 10+ params, one per line, trailing commas, ragged indent,
              -- trailing per-line comments.
              -----------------------------------------------------------------
              PROCEDURE many_params_proc(
                  p_emp_id        IN NUMBER,          -- employee id
                      p_first_name    IN VARCHAR2,        -- first name
                  p_last_name     IN VARCHAR2,        -- last name
                  p_dept_id       IN NUMBER,          -- department
                      io_salary       IN OUT NUMBER,       -- salary, mutated
                  p_hire_date     IN DATE,            -- hire date
                  p_manager_id    IN NUMBER DEFAULT NULL, -- optional manager
                      o_bonus         OUT NUMBER,          -- computed bonus
                  p_currency      IN VARCHAR2 DEFAULT 'USD', -- currency code
                  p_notes         IN VARCHAR2 DEFAULT NULL, -- free text
                  p_flag          IN CHAR DEFAULT 'N',   -- Y/N flag
              ) IS
              BEGIN
                io_salary := io_salary * 1.1; o_bonus := io_salary * 0.05;
              END many_params_proc;

              -----------------------------------------------------------------
              -- Procedure: nested_flow_proc
              -----------------------------------------------------------------
              PROCEDURE nested_flow_proc(p_level IN NUMBER) IS
                v_x NUMBER := 0;
              BEGIN
                <<outer_loop>>
                FOR i IN 1..3 LOOP
                  IF p_level = 1 THEN
                    v_x := 1;
                  ELSIF p_level = 2 THEN
                    v_x := 2;
                  ELSIF p_level = 3 THEN
                    v_x := 3;
                  ELSIF p_level = 4 THEN
                    v_x := 4;
                  ELSIF p_level = 5 THEN
                    CASE p_level
                      WHEN 5 THEN
                        IF v_x = 0 THEN
                          IF i > 1 THEN
                            INSERT INTO audit_log(msg, created_dt) VALUES ('deep', SYSDATE);
                            hr_payroll_pkg.demo_naming_proc();
                          END IF;
                        END IF;
                      ELSE
                        NULL;
                    END CASE;
                  ELSE
                    NULL;
                  END IF;
                  EXIT outer_loop WHEN i = 3;
                END LOOP outer_loop;
              END nested_flow_proc;

              -----------------------------------------------------------------
              -- Procedure: one_liner_proc
              -----------------------------------------------------------------
              PROCEDURE one_liner_proc IS
                v_a NUMBER; v_b NUMBER;
              BEGIN
                v_a := 1; v_b := 2; INSERT INTO temp_tbl(a, b) VALUES (v_a, v_b);
                ; NULL; ;
              END one_liner_proc;

              /* =========================================================
               * Function  : build_dynamic_sql
               * Long dynamic SQL via concatenation with comments between ||.
               * ========================================================= */
              FUNCTION build_dynamic_sql(p_dept_id IN NUMBER) RETURN VARCHAR2 IS
                v_sql VARCHAR2(4000);
              BEGIN
                v_sql := 'SELECT emp_id, first_name, last_name, salary '
                      || 'FROM employees '                       -- base table
                      || 'WHERE dept_id = ' || p_dept_id          -- filter by dept
                      || ' AND status = ''ACTIVE'' '             -- only active
                      -- next clause adds ordering
                      || ' ORDER BY last_name, first_name';
                RETURN v_sql;
              END build_dynamic_sql;

              /* =========================================================
               * Procedure : rowtype_proc
               * Heavy %TYPE / %ROWTYPE usage.
               * ========================================================= */
              PROCEDURE rowtype_proc(p_emp_id IN employees.employee_id%TYPE) IS
                r_emp        employees%ROWTYPE;
                v_salary     employees.salary%TYPE;
                v_dept_name  departments.department_name%TYPE;
              BEGIN
                SELECT * INTO r_emp FROM employees WHERE employee_id = p_emp_id;
                v_salary := r_emp.salary;
                SELECT department_name INTO v_dept_name
                  FROM departments
                 WHERE department_id = r_emp.department_id;
              END rowtype_proc;

            END HR_PAYROLL_PKG;
            /
            """;

    @Test
    @DisplayName("Full audit across 8 Oracle coding-convention dimensions")
    void auditAllDimensions() {
        ParseResult result = parse(SQL);
        System.out.println("=== PARSE ERRORS ===");
        System.out.println(result.getErrors());
        org.junit.jupiter.api.Assertions.assertTrue(result.getErrors() == null || result.getErrors().isEmpty(),
                "trailing comma / stray ';' must no longer produce parse errors: " + result.getErrors());

        // ---------- 1. Hungarian notation, incl. c_get_customer variable-not-call ----------
        SubprogramInfo demoNaming = findSub(result, "demo_naming_proc");
        System.out.println("\n=== [1] demo_naming_proc ===");
        System.out.println("localVariables: " + describeVars(demoNaming));
        System.out.println("calls: " + describeCalls(demoNaming));
        boolean cGetCustomerAsCall = demoNaming != null && demoNaming.getCalls().stream()
                .anyMatch(c -> "c_get_customer".equalsIgnoreCase(c.getName()));
        System.out.println("c_get_customer misclassified as call? " + cGetCustomerAsCall);
        boolean cGetCustomerAsVar = demoNaming != null && demoNaming.getLocalVariables().stream()
                .anyMatch(v -> "c_get_customer".equalsIgnoreCase(v.getName()));
        System.out.println("c_get_customer captured as local variable? " + cGetCustomerAsVar);

        // ---------- 2. Mixed/inconsistent casing ----------
        SubprogramInfo mixedCase = findSub(result, "mixed_case_proc");
        System.out.println("\n=== [2] mixed_case_proc ===");
        System.out.println("found sub (Begin/End mixed case)? " + (mixedCase != null));
        System.out.println("tableOperations: " + describeOps(mixedCase));

        // ---------- 3. 10+ params, trailing commas, ragged indent, trailing comments ----------
        // Bug 1 (fixed): a trailing comma before the closing ')' in a multi-line parameter list
        // used to hard-fail the parse and cascade, wiping out every subprogram declared after it.
        SubprogramInfo manyParams = findSub(result, "many_params_proc");
        System.out.println("\n=== [3] many_params_proc ===");
        System.out.println("found sub? " + (manyParams != null));
        org.junit.jupiter.api.Assertions.assertNotNull(manyParams,
                "trailing comma in parameter list must not cascade and drop the procedure");
        org.junit.jupiter.api.Assertions.assertEquals(11, manyParams.getParameters().size(),
                "all 11 parameters (despite the trailing comma) must be captured");
        for (ParameterInfo p : manyParams.getParameters()) {
            System.out.println("  " + p.getName() + " dir=" + p.getDirection()
                    + " type=" + p.getDataType() + " default=" + p.getDefaultValue()
                    + " line=" + p.getLine());
        }

        // ---------- 4. Nested control flow, labeled block, deep IF/CASE with buried call/insert ----------
        SubprogramInfo nested = findSub(result, "nested_flow_proc");
        System.out.println("\n=== [4] nested_flow_proc ===");
        System.out.println("found sub? " + (nested != null));
        org.junit.jupiter.api.Assertions.assertNotNull(nested,
                "subprograms declared after the trailing-comma procedure must still parse");
        System.out.println("calls: " + describeCalls(nested));
        System.out.println("tableOperations: " + describeOps(nested));

        // ---------- 5. multiple statements per line / stray semicolons ----------
        // Bug 2 (fixed): a bare stray ';' (empty statement) used to hard-fail and cascade.
        SubprogramInfo oneLiner = findSub(result, "one_liner_proc");
        System.out.println("\n=== [5] one_liner_proc ===");
        System.out.println("found sub? " + (oneLiner != null));
        org.junit.jupiter.api.Assertions.assertNotNull(oneLiner,
                "a bare stray ';' (empty statement) must not cascade and drop the procedure");
        System.out.println("localVariables: " + describeVars(oneLiner));
        System.out.println("tableOperations: " + describeOps(oneLiner));

        // ---------- 6. header/docblock comments -> line number drift check ----------
        System.out.println("\n=== [6] line numbers after doc-comment blocks ===");
        if (demoNaming != null) {
            System.out.println("demo_naming_proc lineStart=" + demoNaming.getLineStart()
                    + " lineEnd=" + demoNaming.getLineEnd());
        }
        if (manyParams != null) {
            System.out.println("many_params_proc lineStart=" + manyParams.getLineStart());
        }
        if (nested != null) {
            System.out.println("nested_flow_proc lineStart=" + nested.getLineStart());
            for (CallInfo c : nested.getCalls()) {
                System.out.println("  call " + c.getName() + " at line " + c.getLine());
            }
            for (TableOperationInfo t : nested.getTableOperations()) {
                System.out.println("  op " + t.getOperation() + " " + t.getTableName() + " at line " + t.getLine());
            }
        }

        // ---------- 7. long concatenated dynamic SQL with interspersed comments ----------
        SubprogramInfo dynSql = findSub(result, "build_dynamic_sql");
        System.out.println("\n=== [7] build_dynamic_sql ===");
        System.out.println("found sub? " + (dynSql != null));
        org.junit.jupiter.api.Assertions.assertNotNull(dynSql,
                "subprograms declared after the fixed cascading bugs must still parse");
        if (dynSql != null) {
            System.out.println("dynamicSql entries: " + dynSql.getDynamicSql().size());
            for (DynamicSqlInfo d : dynSql.getDynamicSql()) {
                System.out.println("  " + d.getSqlExpression() + " line=" + d.getLine());
            }
        }

        // ---------- 8. %TYPE / %ROWTYPE heavy usage ----------
        SubprogramInfo rowtypeProc = findSub(result, "rowtype_proc");
        System.out.println("\n=== [8] rowtype_proc ===");
        System.out.println("found sub? " + (rowtypeProc != null));
        if (rowtypeProc != null) {
            System.out.println("parameters: ");
            for (ParameterInfo p : rowtypeProc.getParameters()) {
                System.out.println("  " + p.getName() + " type=" + p.getDataType());
            }
            System.out.println("localVariables: " + describeVars(rowtypeProc));
            System.out.println("tableOperations: " + describeOps(rowtypeProc));
        }
    }

    private static String describeVars(SubprogramInfo s) {
        if (s == null || s.getLocalVariables() == null) return "[]";
        StringBuilder sb = new StringBuilder("[");
        for (VariableInfo v : s.getLocalVariables()) {
            sb.append(v.getName()).append(":").append(v.getDataType())
              .append("@L").append(v.getLine()).append(", ");
        }
        return sb.append("]").toString();
    }

    private static String describeCalls(SubprogramInfo s) {
        if (s == null || s.getCalls() == null) return "[]";
        StringBuilder sb = new StringBuilder("[");
        for (CallInfo c : s.getCalls()) {
            sb.append(c.getName()).append(":").append(c.getType())
              .append("@L").append(c.getLine()).append(", ");
        }
        return sb.append("]").toString();
    }

    private static String describeOps(SubprogramInfo s) {
        if (s == null || s.getTableOperations() == null) return "[]";
        StringBuilder sb = new StringBuilder("[");
        for (TableOperationInfo t : s.getTableOperations()) {
            sb.append(t.getOperation()).append(" ").append(t.getTableName())
              .append("@L").append(t.getLine()).append(", ");
        }
        return sb.append("]").toString();
    }
}
