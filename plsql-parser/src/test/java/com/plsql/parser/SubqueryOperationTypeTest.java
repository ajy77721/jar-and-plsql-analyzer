package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Bug Fix #2: Subquery Tables Get Correct Operation Type")
public class SubqueryOperationTypeTest extends ParserTestBase {

    @Test
    @DisplayName("Table in UPDATE WHERE subquery classified as SELECT not UPDATE")
    void testSubqueryTableInUpdateWhere() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    UPDATE orders SET status = 'CLOSED'
                     WHERE customer_id IN (SELECT id FROM inactive_customers);
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "UPDATE WHERE subquery");
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        List<TableOperationInfo> ops = proc.getTableOperations();

        assertTrue(ops.stream().anyMatch(t ->
                "ORDERS".equalsIgnoreCase(t.getTableName())
                        && "UPDATE".equalsIgnoreCase(t.getOperation())),
                "ORDERS should be UPDATE (the target)");
        assertTrue(ops.stream().anyMatch(t ->
                "INACTIVE_CUSTOMERS".equalsIgnoreCase(t.getTableName())
                        && "SELECT".equalsIgnoreCase(t.getOperation())),
                "INACTIVE_CUSTOMERS in subquery should be SELECT, not UPDATE");
        assertFalse(ops.stream().anyMatch(t ->
                "INACTIVE_CUSTOMERS".equalsIgnoreCase(t.getTableName())
                        && "UPDATE".equalsIgnoreCase(t.getOperation())),
                "INACTIVE_CUSTOMERS must NOT be marked as UPDATE");
    }

    @Test
    @DisplayName("Table in DELETE WHERE subquery classified as SELECT not DELETE")
    void testSubqueryTableInDeleteWhere() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    DELETE FROM temp_data
                     WHERE batch_id IN (SELECT batch_id FROM completed_batches);
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        List<TableOperationInfo> ops = proc.getTableOperations();

        assertTrue(ops.stream().anyMatch(t ->
                "TEMP_DATA".equalsIgnoreCase(t.getTableName())
                        && "DELETE".equalsIgnoreCase(t.getOperation())));
        assertTrue(ops.stream().anyMatch(t ->
                "COMPLETED_BATCHES".equalsIgnoreCase(t.getTableName())
                        && "SELECT".equalsIgnoreCase(t.getOperation())),
                "COMPLETED_BATCHES in subquery should be SELECT not DELETE");
    }

    @Test
    @DisplayName("Correlated subquery in UPDATE SET clause — table is SELECT")
    void testCorrelatedSubqueryInUpdateSet() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    UPDATE policy_header h
                       SET token_id = (SELECT b.token_id FROM policy_bases b
                                        WHERE b.contract_no = h.contract_no);
                  END PROC;
                END MY_PKG;
                /
                """;
        ParseResult result = parse(sql);
        SubprogramInfo proc = findSub(result, "PROC");
        assertNotNull(proc);
        List<TableOperationInfo> ops = proc.getTableOperations();

        assertTrue(ops.stream().anyMatch(t ->
                "POLICY_HEADER".equalsIgnoreCase(t.getTableName())
                        && "UPDATE".equalsIgnoreCase(t.getOperation())));
        assertTrue(ops.stream().anyMatch(t ->
                "POLICY_BASES".equalsIgnoreCase(t.getTableName())
                        && "SELECT".equalsIgnoreCase(t.getOperation())),
                "POLICY_BASES in SET subquery should be SELECT not UPDATE");
    }

    @Test
    @DisplayName("Mixed: cursor FOR loop + UPDATE with subquery in same procedure")
    void testMixedCursorAndSubqueryTables() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY MY_PKG AS
                  PROCEDURE PROC IS
                  BEGIN
                    FOR rec IN (SELECT id FROM active_policies) LOOP
                      UPDATE claims SET status = 'REVIEWED'
                       WHERE policy_id = rec.id
                         AND claim_id IN (SELECT claim_id FROM pending_claims);
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
                "ACTIVE_POLICIES".equalsIgnoreCase(t.getTableName())
                        && "SELECT".equalsIgnoreCase(t.getOperation())),
                "ACTIVE_POLICIES from cursor FOR loop should be SELECT");
        assertTrue(ops.stream().anyMatch(t ->
                "CLAIMS".equalsIgnoreCase(t.getTableName())
                        && "UPDATE".equalsIgnoreCase(t.getOperation())),
                "CLAIMS should be UPDATE (target)");
        assertTrue(ops.stream().anyMatch(t ->
                "PENDING_CLAIMS".equalsIgnoreCase(t.getTableName())
                        && "SELECT".equalsIgnoreCase(t.getOperation())),
                "PENDING_CLAIMS in subquery should be SELECT not UPDATE");
    }
}
