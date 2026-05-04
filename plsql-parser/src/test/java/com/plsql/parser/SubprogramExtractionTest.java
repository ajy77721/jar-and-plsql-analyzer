package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Subprogram Extraction: Package Bodies, Standalone, Nested, Parameters")
public class SubprogramExtractionTest extends ParserTestBase {

    @Test
    @DisplayName("Package body with multiple subprograms")
    void testMultipleSubprogramsExtracted() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC_A IS
                  BEGIN
                    INSERT INTO log_table VALUES ('A');
                  END PROC_A;

                  FUNCTION FN_B RETURN NUMBER IS
                  BEGIN
                    RETURN 42;
                  END FN_B;

                  PROCEDURE PROC_C (p_id NUMBER) IS
                    v_name VARCHAR2(100);
                  BEGIN
                    SELECT name INTO v_name FROM employees WHERE id = p_id;
                    UPDATE employees SET status = 'ACTIVE' WHERE id = p_id;
                  END PROC_C;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Multi-subprogram package");
        assertEquals(1, result.getObjects().size());
        assertTrue(result.getObjects().get(0).getSubprograms().size() >= 3);

        SubprogramInfo procA = findSub(result, "PROC_A");
        assertNotNull(procA);
        assertFalse(procA.getTableOperations().isEmpty());

        SubprogramInfo procC = findSub(result, "PROC_C");
        assertNotNull(procC);
        assertTrue(procC.getTableOperations().size() >= 2);
    }

    @Test
    @DisplayName("Subprogram ending with END; (no name) is still parsed")
    void testSubprogramEndWithoutName() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE UPDATE_STATUS (p_id NUMBER) IS
                  BEGIN
                    UPDATE employees SET status = 'DONE' WHERE id = p_id;
                  END;

                  FUNCTION GET_COUNT RETURN NUMBER IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt FROM employees;
                    RETURN v_cnt;
                  END;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "END; without name");

        assertNotNull(findSub(result, "UPDATE_STATUS"));
        assertNotNull(findSub(result, "GET_COUNT"));
    }

    @Test
    @DisplayName("Standalone procedure parses correctly")
    void testStandaloneProcedure() {
        String sql = """
                CREATE OR REPLACE PROCEDURE MY_PROC (p_id IN NUMBER) IS
                  v_name VARCHAR2(100);
                BEGIN
                  SELECT name INTO v_name FROM users WHERE id = p_id;
                  UPDATE users SET last_login = SYSDATE WHERE id = p_id;
                END MY_PROC;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Standalone proc");
        assertEquals(1, result.getObjects().size());
        assertEquals("MY_PROC", result.getObjects().get(0).getName().toUpperCase());
    }

    @Test
    @DisplayName("Standalone function parses correctly")
    void testStandaloneFunction() {
        String sql = """
                CREATE OR REPLACE FUNCTION FN_GET_COUNT (p_table VARCHAR2) RETURN NUMBER IS
                  v_cnt NUMBER;
                BEGIN
                  EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || p_table INTO v_cnt;
                  RETURN v_cnt;
                END FN_GET_COUNT;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Standalone function");
        assertEquals(1, result.getObjects().size());
        assertEquals("FN_GET_COUNT", result.getObjects().get(0).getName().toUpperCase());
    }

    @Test
    @DisplayName("Package spec with procedure and function declarations")
    void testPackageSpec() {
        String sql = """
                CREATE OR REPLACE PACKAGE MY_PKG AS
                  PROCEDURE DO_WORK (p_id NUMBER, p_name VARCHAR2);
                  FUNCTION GET_STATUS (p_id NUMBER) RETURN VARCHAR2;
                  PROCEDURE BATCH_RUN;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Package spec");
        assertEquals(1, result.getObjects().size());
        assertTrue(result.getObjects().get(0).getType().toUpperCase().contains("PACKAGE"));
    }

    @Test
    @DisplayName("Nested local procedure inside another procedure")
    void testNestedLocalProcedure() {
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
    }

    @Test
    @DisplayName("Procedure with IN, OUT, IN OUT parameters and defaults")
    void testInOutParameters() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC (
                    p_id       IN     NUMBER,
                    p_name     IN     VARCHAR2 DEFAULT 'UNKNOWN',
                    p_status   IN OUT VARCHAR2,
                    p_result      OUT NUMBER
                  ) IS
                  BEGIN
                    SELECT status INTO p_status FROM employees WHERE id = p_id;
                    p_result := 1;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "IN/OUT params");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertTrue(proc.getParameters() != null && proc.getParameters().size() >= 4);
    }

    @Test
    @DisplayName("PRAGMA AUTONOMOUS_TRANSACTION in subprogram")
    void testPragmaAutonomousTransaction() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE LOG_IT (p_msg VARCHAR2) IS
                    PRAGMA AUTONOMOUS_TRANSACTION;
                  BEGIN
                    INSERT INTO app_log (msg, ts) VALUES (p_msg, SYSTIMESTAMP);
                    COMMIT;
                  END LOG_IT;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "PRAGMA AUTONOMOUS_TRANSACTION");
        SubprogramInfo proc = findSub(result, "LOG_IT");
        assertNotNull(proc);
        assertTrue(proc.getTableOperations().stream()
                .anyMatch(t -> "APP_LOG".equalsIgnoreCase(t.getTableName())));
    }

    @Test
    @DisplayName("%TYPE and %ROWTYPE variable declarations")
    void testTypeAndRowtype() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_name employees.name%TYPE;
                    v_rec  employees%ROWTYPE;
                  BEGIN
                    SELECT * INTO v_rec FROM employees WHERE id = 1;
                    v_name := v_rec.name;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "%TYPE / %ROWTYPE");
    }

    @Test
    @DisplayName("Exception handlers with nested BEGIN/END")
    void testExceptionHandlers() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_id NUMBER;
                  BEGIN
                    SELECT id INTO v_id FROM t1 WHERE rownum = 1;
                  EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                      v_id := 0;
                    WHEN TOO_MANY_ROWS THEN
                      BEGIN
                        INSERT INTO error_log (msg) VALUES ('too many');
                      EXCEPTION
                        WHEN OTHERS THEN NULL;
                      END;
                    WHEN OTHERS THEN
                      RAISE;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Exception handlers");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertTrue(proc.getExceptionHandlers() != null && proc.getExceptionHandlers().size() >= 2);
    }

    @Test
    @DisplayName("Nested BEGIN/END blocks with labels")
    void testNestedBeginEndWithLabels() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    <<outer_block>>
                    BEGIN
                      INSERT INTO t1 (id) VALUES (1);
                      <<inner_block>>
                      BEGIN
                        UPDATE t2 SET val = 'X' WHERE id = 1;
                      EXCEPTION
                        WHEN OTHERS THEN NULL;
                      END inner_block;
                    EXCEPTION
                      WHEN OTHERS THEN
                        ROLLBACK;
                    END outer_block;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Nested labeled BEGIN/END");
    }
}
