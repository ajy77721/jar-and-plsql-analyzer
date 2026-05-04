package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Cursors, BULK COLLECT, Dynamic SQL")
public class CursorAndDynamicSqlTest extends ParserTestBase {

    @Test
    @DisplayName("Cursor FOR loop captures table from inline query")
    void testCursorForLoop() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    FOR rec IN (SELECT id, name FROM departments) LOOP
                      DBMS_OUTPUT.PUT_LINE(rec.name);
                    END LOOP;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Cursor FOR loop");
    }

    @Test
    @DisplayName("EXECUTE IMMEDIATE parsed as dynamic SQL")
    void testExecuteImmediate() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC (p_table VARCHAR2) IS
                  BEGIN
                    EXECUTE IMMEDIATE 'DELETE FROM ' || p_table || ' WHERE status = ''OLD''';
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "EXECUTE IMMEDIATE");
    }

    @Test
    @DisplayName("BULK COLLECT INTO with LIMIT")
    void testBulkCollect() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    TYPE t_ids IS TABLE OF NUMBER;
                    l_ids t_ids;
                    CURSOR c1 IS SELECT id FROM employees WHERE status = 'A';
                  BEGIN
                    OPEN c1;
                    LOOP
                      FETCH c1 BULK COLLECT INTO l_ids LIMIT 1000;
                      EXIT WHEN l_ids.COUNT = 0;
                      FORALL i IN 1..l_ids.COUNT
                        UPDATE employees SET processed = 'Y' WHERE id = l_ids(i);
                      COMMIT;
                    END LOOP;
                    CLOSE c1;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "BULK COLLECT + FORALL");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertTrue(proc.getTableOperations().stream()
                .anyMatch(t -> "EMPLOYEES".equalsIgnoreCase(t.getTableName())));
    }

    @Test
    @DisplayName("FORALL with SAVE EXCEPTIONS")
    void testForallSaveExceptions() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    TYPE t_recs IS TABLE OF NUMBER;
                    l_recs t_recs := t_recs(1, 2, 3);
                  BEGIN
                    FORALL i IN 1..l_recs.COUNT SAVE EXCEPTIONS
                      INSERT INTO batch_log (id) VALUES (l_recs(i));
                  EXCEPTION
                    WHEN OTHERS THEN
                      FOR j IN 1..SQL%BULK_EXCEPTIONS.COUNT LOOP
                        DBMS_OUTPUT.PUT_LINE(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX);
                      END LOOP;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "FORALL SAVE EXCEPTIONS");
    }

    @Test
    @DisplayName("OPEN cursor FOR SELECT (dynamic ref cursor)")
    void testOpenCursorForSelect() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC (p_cur OUT SYS_REFCURSOR) IS
                  BEGIN
                    OPEN p_cur FOR
                      SELECT id, name, salary
                        FROM employees
                       WHERE dept_id = 10
                       ORDER BY salary DESC;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "OPEN cursor FOR");
    }

    @Test
    @DisplayName("Pipelined function with PIPE ROW")
    void testPipelinedFunction() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  FUNCTION GET_ROWS RETURN t_table PIPELINED IS
                  BEGIN
                    FOR rec IN (SELECT id, name FROM source_data) LOOP
                      PIPE ROW (t_row(rec.id, rec.name));
                    END LOOP;
                    RETURN;
                  END GET_ROWS;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "PIPELINED + PIPE ROW");
    }

    @Test
    @DisplayName("DBMS_SQL usage detected as dynamic SQL")
    void testDbmsSqlDynamicSql() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    l_cursor INTEGER;
                    l_result INTEGER;
                  BEGIN
                    l_cursor := DBMS_SQL.OPEN_CURSOR;
                    DBMS_SQL.PARSE(l_cursor, 'SELECT 1 FROM dual', DBMS_SQL.NATIVE);
                    l_result := DBMS_SQL.EXECUTE(l_cursor);
                    DBMS_SQL.CLOSE_CURSOR(l_cursor);
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "DBMS_SQL");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertFalse(proc.getCalls().isEmpty());
    }
}
