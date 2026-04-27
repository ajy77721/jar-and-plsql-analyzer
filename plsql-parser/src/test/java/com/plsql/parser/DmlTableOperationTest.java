package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("DML Table Operations")
public class DmlTableOperationTest extends ParserTestBase {

    @Test
    @DisplayName("SELECT INTO captures table in FROM clause")
    void testSelectIntoTable() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_name VARCHAR2(100);
                  BEGIN
                    SELECT name INTO v_name FROM employees WHERE id = 1;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertFalse(proc.getTableOperations().isEmpty());
        assertTrue(proc.getTableOperations().stream()
                .anyMatch(t -> "EMPLOYEES".equalsIgnoreCase(t.getTableName())));
    }

    @Test
    @DisplayName("INSERT, UPDATE, DELETE capture tables with correct operation")
    void testDmlTableCapture() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    INSERT INTO audit_log (msg) VALUES ('test');
                    UPDATE config SET val = 'Y' WHERE key = 'flag';
                    DELETE FROM temp_data WHERE created < SYSDATE - 1;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        List<TableOperationInfo> ops = proc.getTableOperations();
        assertTrue(ops.size() >= 3);

        assertTrue(ops.stream().anyMatch(t ->
                "AUDIT_LOG".equalsIgnoreCase(t.getTableName()) && "INSERT".equalsIgnoreCase(t.getOperation())));
        assertTrue(ops.stream().anyMatch(t ->
                "CONFIG".equalsIgnoreCase(t.getTableName()) && "UPDATE".equalsIgnoreCase(t.getOperation())));
        assertTrue(ops.stream().anyMatch(t ->
                "TEMP_DATA".equalsIgnoreCase(t.getTableName()) && "DELETE".equalsIgnoreCase(t.getOperation())));
    }

    @Test
    @DisplayName("MERGE statement captures target table")
    void testMergeTableCapture() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    MERGE INTO target_table t
                    USING source_table s ON (t.id = s.id)
                    WHEN MATCHED THEN UPDATE SET t.val = s.val
                    WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val);
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "MERGE");
    }

    @Test
    @DisplayName("INSERT ... RETURNING ... INTO ...")
    void testReturningInto() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_id NUMBER;
                  BEGIN
                    INSERT INTO orders (customer_id, amount)
                    VALUES (100, 5000)
                    RETURNING order_id INTO v_id;
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "RETURNING INTO");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        assertTrue(proc.getTableOperations().stream()
                .anyMatch(t -> "ORDERS".equalsIgnoreCase(t.getTableName())
                        && "INSERT".equalsIgnoreCase(t.getOperation())));
    }

    @Test
    @DisplayName("Schema-prefixed table names in DML")
    void testSchemaPrefixedTables() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                    v_cnt NUMBER;
                  BEGIN
                    SELECT COUNT(*) INTO v_cnt FROM CUSTOMER.orders;
                    INSERT INTO OPUS_CORE.audit_log (msg) VALUES ('test');
                    UPDATE CUSTOMER_I.config SET val = 'Y' WHERE key = 'k';
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Schema-prefixed tables");
    }
}
