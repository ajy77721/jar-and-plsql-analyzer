package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Gap #4: Subquery Context Classification")
public class SubqueryContextClassificationTest extends ParserTestBase {

    @Test
    @DisplayName("EXISTS subquery marks tables with subqueryContext=EXISTS")
    void testExistsSubquery() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_exists BOOLEAN;
                  BEGIN
                    IF EXISTS (SELECT 1 FROM pending_tasks WHERE owner = 'ME') THEN
                      NULL;
                    END IF;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        TableOperationInfo toi = proc.getTableOperations().stream()
                .filter(t -> "PENDING_TASKS".equalsIgnoreCase(t.getTableName()))
                .findFirst().orElse(null);
        assertNotNull(toi);
        assertEquals("EXISTS", toi.getSubqueryContext(),
                "Table in EXISTS subquery should have subqueryContext=EXISTS");
    }

    @Test
    @DisplayName("Scalar subquery in assignment marks tables with subqueryContext=SCALAR")
    void testScalarSubquery() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_cnt NUMBER;
                  BEGIN
                    v_cnt := (SELECT COUNT(*) FROM audit_log WHERE status = 'A');
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        TableOperationInfo toi = proc.getTableOperations().stream()
                .filter(t -> "AUDIT_LOG".equalsIgnoreCase(t.getTableName()))
                .findFirst().orElse(null);
        assertNotNull(toi);
        assertEquals("SCALAR", toi.getSubqueryContext(),
                "Table in scalar subquery should have subqueryContext=SCALAR");
    }

    @Test
    @DisplayName("Top-level SELECT has no subqueryContext (null)")
    void testTopLevelSelect() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt FROM employees WHERE dept = 'IT';
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        TableOperationInfo toi = proc.getTableOperations().stream()
                .filter(t -> "EMPLOYEES".equalsIgnoreCase(t.getTableName()))
                .findFirst().orElse(null);
        assertNotNull(toi);
        assertNull(toi.getSubqueryContext(),
                "Top-level SELECT tables should have null subqueryContext");
    }

    @Test
    @DisplayName("Subquery in INSERT..SELECT subquery context is SCALAR or null")
    void testInsertSelectSubquery() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    INSERT INTO target_table (id, name)
                    SELECT id, name FROM source_table WHERE active = 'Y';
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                "TARGET_TABLE".equalsIgnoreCase(t.getTableName())
                        && "INSERT".equalsIgnoreCase(t.getOperation())),
                "TARGET_TABLE should be INSERT");
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                "SOURCE_TABLE".equalsIgnoreCase(t.getTableName())
                        && "SELECT".equalsIgnoreCase(t.getOperation())),
                "SOURCE_TABLE should be SELECT");
    }
}
