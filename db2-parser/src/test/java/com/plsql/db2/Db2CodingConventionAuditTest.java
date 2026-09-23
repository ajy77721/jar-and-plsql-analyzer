package com.plsql.db2;

import com.plsql.db2.engine.Db2AnalysisEngine;
import com.plsql.db2.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AUDIT ONLY — findings are printed and reported back to the caller, not asserted as
 * pass/fail gates (except where a real capture break is being pinned down).
 *
 * <p>{@code Db2AnalysisEngine.parse(source, ...)} is documented and used (see
 * {@code Db2ProcedureParser}) as "parse a SINGLE PROCEDURE or FUNCTION source" — the grammar's
 * {@code compilationUnit} is {@code routineDefinition? EOF}: exactly one routine, no leading
 * top-level statements, no multi-routine `@`-delimited scripts. Whatever splits a raw DB2 script
 * into one source string per routine happens upstream of this module. So each test below feeds
 * ONE realistic, convention-heavy CREATE PROCEDURE...END body — mirroring the existing harness
 * pattern in Db2LoopAndParenCaptureTest — rather than a multi-procedure file, and combines the 8
 * requested dimensions naturally within that single-routine constraint.
 */
class Db2CodingConventionAuditTest {

    private final Db2AnalysisEngine engine = new Db2AnalysisEngine();

    // ── Dimension 3 + 6: wide param list, one per line, trailing commas/comments,
    //    inconsistent indentation, preceded by a large header + docblock comment block ──

    private static final String WIDE_PARAMS_SRC =
        "-- ===========================================================================\n" // 1
      + "-- MODULE:   ERP_CUSTOMER_ORDER_PKG                                            \n" // 2
      + "-- PURPOSE:  Audit fixture — wide parameter list + header comment line drift.  \n" // 3
      + "-- AUTHOR:   audit                                                             \n" // 4
      + "-- HISTORY:  2026-09-16  initial                                               \n" // 5
      + "-- ===========================================================================\n" // 6
      + "--------------------------------------------------------------------------------\n"// 7
      + "-- Docblock: p_wide_params                                                       \n"// 8
      + "-- Wide parameter list, one per line, Hungarian prefixes, trailing commas/comments\n"//9
      + "--------------------------------------------------------------------------------\n"// 10
      + "CREATE PROCEDURE p_wide_params(\n"                                                 // 11
      + "  IN  p_customer_id       INTEGER,        -- customer PK\n"                        // 12
      + "      IN p_order_id          INTEGER,   -- order PK, odd indent\n"                 // 13
      + "  IN p_order_date         DATE,             -- order date\n"                       // 14
      + "OUT o_status_code        INTEGER,   -- 0/1\n"                                      // 15
      + "  INOUT io_running_total    DECIMAL(10,2),  -- accumulator\n"                      // 16
      + "  IN p_region_code        CHAR(2),   -- region\n"                                  // 17
      + "  IN p_currency_code      CHAR(3),  -- currency\n"                                 // 18
      + "  IN p_discount_pct       DECIMAL(5,2), -- discount\n"                              // 19
      + "  IN p_tax_pct            DECIMAL(5,2),  -- tax\n"                                  // 20
      + "  IN p_notes              VARCHAR(500), -- free text\n"                             // 21
      + "  IN p_priority           SMALLINT,  -- 1-5\n"                                      // 22
      + "  IN p_created_by         VARCHAR(50)      -- last param, no comma\n"               // 23
      + ")\n"                                                                                // 24
      + "LANGUAGE SQL\n"                                                                     // 25
      + "BEGIN\n"                                                                            // 26
      + "  DECLARE v_dummy INTEGER DEFAULT 0;\n"                                             // 27
      + "  SET v_dummy = p_customer_id; SET o_status_code = 1;\n"                            // 28
      + "END";                                                                               // 29

    @Test
    @DisplayName("Dim 3+6: 12 params (one per line, trailing commas/comments, mixed indent) all captured; no line drift after header/docblock comments")
    void wideParamListAndHeaderLineDrift() {
        Db2RoutineInfo r = engine.parse(WIDE_PARAMS_SRC, "p_wide_params", null);
        System.out.println("=== errors === " + r.getErrors());
        System.out.println("=== params ===");
        for (Db2ParamInfo p : r.getParams()) {
            System.out.println("  " + p.getName() + " dir=" + p.getDirection() + " type=" + p.getDataType() + " line=" + p.getLine());
        }
        System.out.println("=== variables (v_dummy expected at source line 27) ===");
        for (Db2VariableInfo v : r.getVariables()) {
            System.out.println("  " + v.getName() + " line=" + v.getLine());
        }
        System.out.println("=== statements (SET v_dummy=..; SET o_status_code=..; both on line 28) ===");
        for (Db2StatementInfo s : r.getStatements()) {
            System.out.println("  type=" + s.getType() + " line=" + s.getLine());
        }
        assertEquals(12, r.getParams().size(), "expected all 12 params captured, got " + r.getParams().size());
    }

    // ── Dimension 1 + 2: Hungarian naming incl. c_get_customer, mixed keyword/identifier casing ──

    private static final String MIXED_CASE_SRC =
        "create procedure p_mixed_case_and_naming(\n"                                       // 1
      + "  IN p_cust_id INTEGER\n"                                                          // 2
      + ")\n"                                                                                // 3
      + "LANGUAGE SQL\n"                                                                     // 4
      + "begin\n"                                                                            // 5
      + "  DECLARE v_customer_name VARCHAR(100);\n"                                          // 6
      + "  declare l_local_flag INTEGER DEFAULT 0;\n"                                        // 7
      + "  DECLARE g_global_counter INTEGER DEFAULT 0;\n"                                    // 8
      + "  DECLARE c_get_customer INTEGER DEFAULT 0;\n"                                      // 9
      + "  Select Name Into v_customer_name From ERP_CUSTOMER Where Id = p_cust_id;\n"       // 10
      + "  CALL p_wide_params(p_cust_id, 1, CURRENT DATE, l_local_flag, g_global_counter,\n" // 11
      + "                     'NA', 'USD', 0.0, 0.0, 'note', 1, 'sys');\n"                   // 12
      + "END";                                                                                // 13

    @Test
    @DisplayName("Dim 1+2: c_get_customer var not misclassified as a call; mixed-case Select/Into/From/Where captured")
    void hungarianNamingAndMixedCasing() {
        Db2RoutineInfo r = engine.parse(MIXED_CASE_SRC, "p_mixed_case_and_naming", null);
        System.out.println("=== errors === " + r.getErrors());
        System.out.println("=== variables ===");
        for (Db2VariableInfo v : r.getVariables()) {
            System.out.println("  " + v.getName() + " type=" + v.getDataType() + " line=" + v.getLine());
        }
        System.out.println("=== calls ===");
        for (Db2CallInfo c : r.getCalls()) {
            System.out.println("  " + c.getSchema() + "." + c.getName() + " line=" + c.getLine());
        }
        System.out.println("=== tableOps (mixed-case Select/Into/From/Where) ===");
        for (Db2TableOpInfo t : r.getTableOps()) {
            System.out.println("  " + t.getDmlType() + " " + t.getTableName() + " line=" + t.getLine() + " sql=" + t.getSqlText());
        }
        boolean cGetCustomerAsCall = r.getCalls().stream()
                .anyMatch(c -> c.getName() != null && c.getName().equalsIgnoreCase("c_get_customer"));
        assertFalse(cGetCustomerAsCall, "c_get_customer variable was misclassified as a CALL target");

        boolean sawCallWideParams = r.getCalls().stream()
                .anyMatch(c -> c.getName() != null && c.getName().equalsIgnoreCase("p_wide_params"));
        assertTrue(sawCallWideParams, "CALL p_wide_params(...) across a mixed-case, multi-line arg list was not captured");

        boolean sawErpCustomerRead = r.getTableOps().stream()
                .anyMatch(t -> "ERP_CUSTOMER".equalsIgnoreCase(t.getTableName()));
        assertTrue(sawErpCustomerRead, "the mixed-case 'Select ... Into ... From ERP_CUSTOMER Where ...' statement was not captured");
    }

    // ── Dimension 4 + 5: labeled block, IF nested 5 deep, nested CASE, real INSERT at the leaf,
    //    multiple statements per line, unusual semicolon placement ──

    private static final String DEEP_NESTING_SRC =
        "CREATE PROCEDURE p_deep_nesting(IN p_level INTEGER)\n"                              // 1
      + "LANGUAGE SQL\n"                                                                     // 2
      + "outer_block: BEGIN\n"                                                               // 3
      + "  DECLARE v_x INTEGER DEFAULT 0;\n"                                                 // 4
      + "  IF p_level >= 1 THEN\n"                                                           // 5
      + "    IF p_level >= 2 THEN\n"                                                         // 6
      + "      IF p_level >= 3 THEN\n"                                                       // 7
      + "        IF p_level >= 4 THEN\n"                                                     // 8
      + "          IF p_level >= 5 THEN\n"                                                   // 9
      + "            CASE p_level\n"                                                         // 10
      + "              WHEN 5 THEN\n"                                                        // 11
      + "                CASE\n"                                                             // 12
      + "                  WHEN v_x = 0 THEN\n"                                              // 13
      + "                    INSERT INTO ERP_AUDIT_LOG (level, msg) VALUES (p_level, 'deep');\n"//14
      + "                  ELSE SET v_x = 1; SET v_x = v_x + 1;\n"                            // 15
      + "                END CASE;\n"                                                         // 16
      + "              ELSE\n"                                                                // 17
      + "                SET v_x = 2;\n"                                                      // 18
      + "            END CASE;\n"                                                             // 19
      + "          ELSEIF p_level = 0 THEN\n"                                                 // 20
      + "            SET v_x = -1;\n"                                                          // 21
      + "          END IF;\n"                                                                  // 22
      + "        END IF;\n"                                                                     // 23
      + "      END IF;\n"                                                                       // 24
      + "    END IF;\n"                                                                          // 25
      + "  END IF;\n"                                                                            // 26
      + "END outer_block";                                                                       // 27

    @Test
    @DisplayName("Dim 4+5: leaf INSERT inside 5-deep nested IF + nested CASE captured; multi-statement line both SETs captured")
    void deepNestingAndMultiStatementLines() {
        Db2RoutineInfo r = engine.parse(DEEP_NESTING_SRC, "p_deep_nesting", null);
        System.out.println("=== errors === " + r.getErrors());
        System.out.println("=== tableOps (expect INSERT ERP_AUDIT_LOG at line 14) ===");
        for (Db2TableOpInfo t : r.getTableOps()) {
            System.out.println("  " + t.getDmlType() + " " + t.getTableName() + " line=" + t.getLine());
        }
        boolean sawLeafInsert = r.getTableOps().stream()
                .anyMatch(t -> "ERP_AUDIT_LOG".equalsIgnoreCase(t.getTableName()));
        assertTrue(sawLeafInsert, "the INSERT buried at 5+ levels of IF/CASE nesting was not captured at all");
    }

    // ── Dimension 7 + 8: long CONCAT/|| dynamic SQL across lines with interspersed comments,
    //    EXECUTE IMMEDIATE, GET DIAGNOSTICS, SIGNAL SQLSTATE, plus a real CALL right after ──

    private static final String DYNAMIC_SQL_SRC =
        "CREATE PROCEDURE p_dynamic_sql_and_diagnostics(IN p_country_id INTEGER)\n"          // 1
      + "LANGUAGE SQL\n"                                                                     // 2
      + "BEGIN\n"                                                                            // 3
      + "  DECLARE v_sql VARCHAR(2000); DECLARE v_row_count INTEGER DEFAULT 0;\n"             // 4
      + "  DECLARE v_sqlstate CHAR(5);\n"                                                     // 5
      + "  SET v_sql = 'SELECT COUNT(*)'\n"                                                   // 6
      + "    ||  ' FROM ERP_SALES_ORDER'      -- table\n"                                     // 7
      + "    ||  ' WHERE country_id = '       -- predicate start\n"                           // 8
      + "    ||  CAST(p_country_id AS VARCHAR(10)); -- predicate value\n"                     // 9
      + "  EXECUTE IMMEDIATE v_sql;\n"                                                        // 10
      + "  GET DIAGNOSTICS v_row_count = ROW_COUNT;\n"                                        // 11
      + "  IF v_row_count = 0 THEN SIGNAL SQLSTATE '75001' SET MESSAGE_TEXT = 'no rows'; END IF;\n" // 12
      + "  CALL p_mixed_case_and_naming(p_country_id);\n"                                    // 13
      + "END";                                                                                // 14

    @Test
    @DisplayName("Dim 7+8: multi-line CONCAT dynamic SQL, GET DIAGNOSTICS, SIGNAL SQLSTATE don't break the surrounding EXECUTE IMMEDIATE / CALL capture")
    void dynamicSqlAndDb2Idioms() {
        Db2RoutineInfo r = engine.parse(DYNAMIC_SQL_SRC, "p_dynamic_sql_and_diagnostics", null);
        System.out.println("=== errors === " + r.getErrors());
        System.out.println("=== dynamicSql ===");
        for (Db2DynamicSqlInfo d : r.getDynamicSql()) {
            System.out.println("  kind=" + d.getKind() + " line=" + d.getLine() + " src=" + d.getSqlSource());
        }
        System.out.println("=== calls (expect CALL p_mixed_case_and_naming at line 13) ===");
        for (Db2CallInfo c : r.getCalls()) {
            System.out.println("  " + c.getSchema() + "." + c.getName() + " line=" + c.getLine());
        }
        boolean sawExecuteImmediate = r.getDynamicSql().stream()
                .anyMatch(d -> d.getKind() == Db2DynamicSqlInfo.Kind.EXECUTE_IMMEDIATE);
        assertTrue(sawExecuteImmediate, "EXECUTE IMMEDIATE v_sql (built from a multi-line || concat with interspersed comments) was not captured");

        boolean sawTrailingCall = r.getCalls().stream()
                .anyMatch(c -> c.getName() != null && c.getName().equalsIgnoreCase("p_mixed_case_and_naming"));
        assertTrue(sawTrailingCall, "the real CALL statement after SIGNAL/GET DIAGNOSTICS was not captured — those idioms broke surrounding capture");
    }

    // ── Dimension 8 (N/A check): SET SCHEMA / SET CURRENT SCHEMA as a statement inside a routine body ──

    private static final String SET_SCHEMA_SRC =
        "CREATE PROCEDURE p_set_schema_probe()\n"
      + "LANGUAGE SQL\n"
      + "BEGIN\n"
      + "  SET SCHEMA erp_owner;\n"
      + "  CALL p_wide_params(1, 2, CURRENT DATE, 0, 0, 'NA', 'USD', 0.0, 0.0, 'n', 1, 's');\n"
      + "END";

    @Test
    @DisplayName("Dim 8 (fixed): SET SCHEMA as a statement inside a routine body is no longer dropped")
    void setSchemaInsideBody() {
        // Bug 8 (fixed): setStatement only accepted "SET var = expr"; a bare "SET SCHEMA <name>"
        // failed to parse and silently dropped the rest of the body, including the real CALL
        // right after it. setStatement now also accepts "SET SCHEMA <name>" and
        // "SET CURRENT SCHEMA <name>".
        Db2RoutineInfo r = engine.parse(SET_SCHEMA_SRC, "p_set_schema_probe", null);
        System.out.println("=== SET SCHEMA inside body: errors === " + r.getErrors());
        System.out.println("=== calls after SET SCHEMA ===");
        for (Db2CallInfo c : r.getCalls()) {
            System.out.println("  " + c.getSchema() + "." + c.getName() + " line=" + c.getLine());
        }
        assertTrue(r.getErrors().isEmpty(), "SET SCHEMA must not produce parse errors: " + r.getErrors());
        assertTrue(r.getCalls().stream().anyMatch(c -> "p_wide_params".equalsIgnoreCase(c.getName())),
                "the real CALL after SET SCHEMA must still be captured");
    }
}
