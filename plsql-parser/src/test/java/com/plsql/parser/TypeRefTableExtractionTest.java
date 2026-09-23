package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("%TYPE and %ROWTYPE table reference extraction")
public class TypeRefTableExtractionTest extends ParserTestBase {

    @Test
    @DisplayName("table.column%TYPE adds table to dependency summary")
    void testPercentTypeTableDependency() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_val SAPM_SYS_CONSTANTS.NUM_VALUE%TYPE;
                  BEGIN
                    NULL;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "%TYPE table ref");
        ParsedObject obj = result.getObjects().get(0);
        assertTrue(obj.getDependencies().getTables().contains("SAPM_SYS_CONSTANTS"),
                "SAPM_SYS_CONSTANTS from %TYPE should be in dependency tables");
    }

    @Test
    @DisplayName("table%ROWTYPE adds table to dependency summary")
    void testPercentRowTypeTableDependency() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_rec employees%ROWTYPE;
                  BEGIN
                    NULL;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "%ROWTYPE table ref");
        ParsedObject obj = result.getObjects().get(0);
        assertTrue(obj.getDependencies().getTables().contains("EMPLOYEES"),
                "EMPLOYEES from %ROWTYPE should be in dependency tables");
    }

    @Test
    @DisplayName("schema.table.column%TYPE adds table to dependency summary")
    void testSchemaQualifiedPercentType() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_val CUSTOMER.SAPM_SYS_CONSTANTS.CHAR_VALUE%TYPE;
                  BEGIN
                    NULL;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "schema.table.column%TYPE");
        ParsedObject obj = result.getObjects().get(0);
        assertTrue(obj.getDependencies().getTables().stream()
                .anyMatch(t -> t.contains("SAPM_SYS_CONSTANTS")),
                "SAPM_SYS_CONSTANTS from schema.table.column%TYPE should be in dependency tables");
    }

    @Test
    @DisplayName("parameter with table.column%TYPE adds table to dependency summary")
    void testParameterPercentType() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC(p_id employees.employee_id%TYPE) IS
                  BEGIN
                    NULL;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "param %TYPE");
        ParsedObject obj = result.getObjects().get(0);
        assertTrue(obj.getDependencies().getTables().contains("EMPLOYEES"),
                "EMPLOYEES from parameter %TYPE should be in dependency tables");
    }

    @Test
    @DisplayName("Function call in variable initializer is tracked as a call")
    void testFunctionCallInVariableInitializer() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  v_val SAPM_SYS_CONSTANTS.NUM_VALUE%TYPE
                      := pg_util_general.Fn_Get_Num_Value_Constants('UW', 'AMT', TRUNC(SYSDATE));
                  PROCEDURE PROC IS
                  BEGIN
                    NULL;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "func call in var init");
        ParsedObject obj = result.getObjects().get(0);

        // The function call should be captured at the package level
        assertTrue(obj.getCalls().stream().anyMatch(c ->
                "FN_GET_NUM_VALUE_CONSTANTS".equalsIgnoreCase(c.getName())
                        && "PG_UTIL_GENERAL".equalsIgnoreCase(c.getPackageName())),
                "pg_util_general.Fn_Get_Num_Value_Constants should be captured as a call");

        // The %TYPE table should also be tracked
        assertTrue(obj.getDependencies().getTables().contains("SAPM_SYS_CONSTANTS"),
                "SAPM_SYS_CONSTANTS from %TYPE should be in dependency tables");
    }
}
