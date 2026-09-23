package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Bug Fix #1: Cursor Tables Promoted to directTables")
public class CursorTablePromotionTest extends ParserTestBase {

    @Test
    @DisplayName("Inline cursor FOR loop tables appear in directTables as SELECT")
    void testCursorForLoopTablesInDirectTables() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    FOR rec IN (SELECT id, name FROM departments d
                                  JOIN locations l ON d.loc_id = l.id) LOOP
                      DBMS_OUTPUT.PUT_LINE(rec.name);
                    END LOOP;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Cursor FOR loop tables");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        List<TableOperationInfo> ops = proc.getTableOperations();

        assertTrue(ops.stream().anyMatch(t ->
                "DEPARTMENTS".equalsIgnoreCase(t.getTableName())
                        && "SELECT".equalsIgnoreCase(t.getOperation())),
                "DEPARTMENTS from cursor FOR loop should be in directTables as SELECT");
        assertTrue(ops.stream().anyMatch(t ->
                "LOCATIONS".equalsIgnoreCase(t.getTableName())
                        && "SELECT".equalsIgnoreCase(t.getOperation())),
                "LOCATIONS from cursor FOR loop should be in directTables as SELECT");
    }

    @Test
    @DisplayName("Declared cursor tables appear in directTables as SELECT")
    void testDeclaredCursorTablesInDirectTables() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    CURSOR c1 IS SELECT id, name FROM employees WHERE dept_id = 10;
                    v_rec c1%ROWTYPE;
                  BEGIN
                    OPEN c1;
                    FETCH c1 INTO v_rec;
                    CLOSE c1;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Declared cursor tables");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertTrue(proc.getTableOperations().stream().anyMatch(t ->
                "EMPLOYEES".equalsIgnoreCase(t.getTableName())
                        && "SELECT".equalsIgnoreCase(t.getOperation())),
                "Declared cursor should promote EMPLOYEES to directTables as SELECT");
    }

    @Test
    @DisplayName("Cursor FOR loop with multiple comma-separated tables")
    void testCursorForLoopMultiTableJoin() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    FOR rec IN (SELECT a.id
                                  FROM orders a, order_items b, products c
                                 WHERE a.id = b.order_id AND b.product_id = c.id) LOOP
                      NULL;
                    END LOOP;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        List<TableOperationInfo> ops = proc.getTableOperations();
        assertTrue(ops.stream().anyMatch(t ->
                "ORDERS".equalsIgnoreCase(t.getTableName()) && "SELECT".equalsIgnoreCase(t.getOperation())));
        assertTrue(ops.stream().anyMatch(t ->
                "ORDER_ITEMS".equalsIgnoreCase(t.getTableName()) && "SELECT".equalsIgnoreCase(t.getOperation())));
        assertTrue(ops.stream().anyMatch(t ->
                "PRODUCTS".equalsIgnoreCase(t.getTableName()) && "SELECT".equalsIgnoreCase(t.getOperation())));
    }
}
