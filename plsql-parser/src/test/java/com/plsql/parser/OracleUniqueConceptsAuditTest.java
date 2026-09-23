package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Read-only audit (2026-09-17): probes 5 Oracle-UNIQUE PL/SQL concepts against the real
 * ANTLR parser/visitor and PRINTS what was captured. Diagnostic harness, not a permanent
 * regression suite — verdicts are recorded in the report accompanying this test.
 */
public class OracleUniqueConceptsAuditTest extends ParserTestBase {

    // ------------------------------------------------------------------
    // 1. PRAGMA AUTONOMOUS_TRANSACTION
    // ------------------------------------------------------------------
    @Test
    @DisplayName("1: PRAGMA AUTONOMOUS_TRANSACTION does not interfere with DML capture")
    void pragmaAutonomousTransaction() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE log_audit_event(p_event_id NUMBER, p_msg VARCHAR2) IS
                    PRAGMA AUTONOMOUS_TRANSACTION;
                  BEGIN
                    INSERT INTO audit_log (event_id, message, created_date)
                    VALUES (p_event_id, p_msg, SYSDATE);

                    UPDATE audit_counters SET cnt = cnt + 1 WHERE counter_name = 'AUDIT_EVENTS';

                    COMMIT;
                  EXCEPTION
                    WHEN OTHERS THEN
                      ROLLBACK;
                      RAISE;
                  END log_audit_event;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "PRAGMA AUTONOMOUS_TRANSACTION");
        SubprogramInfo sub = findSub(result, "LOG_AUDIT_EVENT");
        System.out.println("=== 1: PRAGMA AUTONOMOUS_TRANSACTION ===");
        System.out.println("  sub=" + (sub == null ? "NOT FOUND" : sub.getName()));
        if (sub != null) {
            System.out.println("  pragmaAutonomousTransaction=" + sub.isPragmaAutonomousTransaction());
            for (TableOperationInfo t : sub.getTableOperations()) {
                System.out.println("  op: table=" + t.getTableName() + " op=" + t.getOperation());
            }
            for (ExceptionHandlerInfo eh : sub.getExceptionHandlers()) {
                System.out.println("  handler: exceptionName=" + eh.getExceptionName()
                        + " calls=" + eh.getCalls());
            }
        }
    }

    // ------------------------------------------------------------------
    // 2. PIPELINED table function
    // ------------------------------------------------------------------
    @Test
    @DisplayName("2: PIPELINED table function with PIPE ROW")
    void pipelinedTableFunction() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  FUNCTION get_active_orders_tbl RETURN order_tbl_type PIPELINED IS
                    CURSOR cur_orders IS
                      SELECT o.id, o.status, o.customer_id
                      FROM orders o
                      WHERE o.status = 'ACTIVE';
                    v_row order_rec_type;
                  BEGIN
                    FOR r IN cur_orders LOOP
                      v_row.id := r.id;
                      v_row.status := r.status;
                      v_row.customer_id := r.customer_id;
                      PIPE ROW(v_row);
                    END LOOP;
                    RETURN;
                  END get_active_orders_tbl;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "PIPELINED table function");
        SubprogramInfo sub = findSub(result, "GET_ACTIVE_ORDERS_TBL");
        System.out.println("=== 2: PIPELINED table function ===");
        System.out.println("  sub=" + (sub == null ? "NOT FOUND" : sub.getName()));
        if (sub != null) {
            System.out.println("  type=" + sub.getType());
            System.out.println("  returnType=" + sub.getReturnType());
            System.out.println("  pipelined=" + sub.isPipelined());
            for (TableOperationInfo t : sub.getTableOperations()) {
                System.out.println("  op: table=" + t.getTableName() + " op=" + t.getOperation());
            }
            for (CallInfo c : sub.getCalls()) {
                System.out.println("  call: " + c.getName());
            }
        }
        // Regression guard: a package-body FUNCTION ... RETURN <type> PIPELINED IS ... used to
        // always report pipelined=false because nothing read the PIPELINED token onto
        // SubprogramInfo, even though the grammar matched it fine.
        org.junit.jupiter.api.Assertions.assertNotNull(sub);
        org.junit.jupiter.api.Assertions.assertTrue(sub.isPipelined(),
                "PIPELINED function must be reported as pipelined=true");
    }

    // ------------------------------------------------------------------
    // 3. FORALL ... SAVE EXCEPTIONS + BULK COLLECT INTO ... LIMIT
    // ------------------------------------------------------------------
    @Test
    @DisplayName("3: FORALL SAVE EXCEPTIONS with BULK COLLECT LIMIT batch loop")
    void forallSaveExceptionsBulkCollectLimit() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE batch_update_orders IS
                    CURSOR cur_orders IS
                      SELECT id FROM staging_orders WHERE status = 'PENDING';
                    TYPE t_ids IS TABLE OF NUMBER;
                    v_ids t_ids;
                    v_error_count NUMBER := 0;
                    bulk_errors EXCEPTION;
                    PRAGMA EXCEPTION_INIT(bulk_errors, -24381);
                  BEGIN
                    OPEN cur_orders;
                    LOOP
                      FETCH cur_orders BULK COLLECT INTO v_ids LIMIT 200;
                      EXIT WHEN v_ids.COUNT = 0;

                      FORALL i IN 1 .. v_ids.COUNT SAVE EXCEPTIONS
                        UPDATE processed_orders SET status = 'DONE' WHERE id = v_ids(i);

                      EXIT WHEN cur_orders%NOTFOUND;
                    END LOOP;
                    CLOSE cur_orders;
                  EXCEPTION
                    WHEN bulk_errors THEN
                      v_error_count := SQL%BULK_EXCEPTIONS.COUNT;
                      FOR i IN 1 .. v_error_count LOOP
                        INSERT INTO error_log (err_index, err_code)
                        VALUES (SQL%BULK_EXCEPTIONS(i).ERROR_INDEX, SQL%BULK_EXCEPTIONS(i).ERROR_CODE);
                      END LOOP;
                  END batch_update_orders;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        System.out.println("=== 3: FORALL SAVE EXCEPTIONS + BULK COLLECT LIMIT ===");
        System.out.println("  errors=" + result.getErrors());
        SubprogramInfo sub = findSub(result, "BATCH_UPDATE_ORDERS");
        System.out.println("  sub=" + (sub == null ? "NOT FOUND" : sub.getName()));
        if (sub != null) {
            for (TableOperationInfo t : sub.getTableOperations()) {
                System.out.println("  op: table=" + t.getTableName() + " op=" + t.getOperation());
            }
            for (CursorInfo c : sub.getCursors()) {
                System.out.println("  cursor=" + c.getName());
            }
            for (ExceptionHandlerInfo eh : sub.getExceptionHandlers()) {
                System.out.println("  handler: exceptionName=" + eh.getExceptionName());
                for (TableOperationInfo t : eh.getTableOperations()) {
                    System.out.println("    handler-op: table=" + t.getTableName() + " op=" + t.getOperation());
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // 4. PRAGMA EXCEPTION_INIT + named exception handler
    // ------------------------------------------------------------------
    @Test
    @DisplayName("4: PRAGMA EXCEPTION_INIT custom exception with named handler")
    void pragmaExceptionInit() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE insert_customer(p_id NUMBER, p_name VARCHAR2) IS
                    e_dup_val EXCEPTION;
                    PRAGMA EXCEPTION_INIT(e_dup_val, -1);
                  BEGIN
                    INSERT INTO customers (id, name) VALUES (p_id, p_name);
                    COMMIT;
                  EXCEPTION
                    WHEN e_dup_val THEN
                      UPDATE customers SET name = p_name WHERE id = p_id;
                      COMMIT;
                    WHEN OTHERS THEN
                      ROLLBACK;
                      RAISE;
                  END insert_customer;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "PRAGMA EXCEPTION_INIT");
        SubprogramInfo sub = findSub(result, "INSERT_CUSTOMER");
        System.out.println("=== 4: PRAGMA EXCEPTION_INIT ===");
        System.out.println("  sub=" + (sub == null ? "NOT FOUND" : sub.getName()));
        if (sub != null) {
            for (ExceptionHandlerInfo eh : sub.getExceptionHandlers()) {
                System.out.println("  handler: exceptionName=" + eh.getExceptionName()
                        + " line=" + eh.getLine() + " lineEnd=" + eh.getLineEnd()
                        + " kind=" + eh.getKind() + " conditions=" + eh.getConditions());
                for (TableOperationInfo t : eh.getTableOperations()) {
                    System.out.println("    handler-op: table=" + t.getTableName() + " op=" + t.getOperation());
                }
            }
            for (VariableInfo v : sub.getLocalVariables()) {
                System.out.println("  localVar: name=" + v.getName() + " type=" + v.getDataType());
            }
        }
    }

    // ------------------------------------------------------------------
    // 5. CREATE TYPE ... AS OBJECT with MEMBER FUNCTION
    // ------------------------------------------------------------------
    @Test
    @DisplayName("5: CREATE TYPE ... AS OBJECT with MEMBER FUNCTION")
    void createTypeObjectWithMemberFunction() {
        String sql = """
                CREATE OR REPLACE TYPE customer_obj_type AS OBJECT (
                  id NUMBER,
                  name VARCHAR2(100),
                  tier VARCHAR2(20),
                  MEMBER FUNCTION get_discount_pct RETURN NUMBER,
                  MEMBER PROCEDURE apply_discount(p_amount IN OUT NUMBER)
                );
                /
                """;
        ParseResult result;
        try {
            result = parse(sql);
        } catch (RuntimeException ex) {
            System.out.println("=== 5: CREATE TYPE AS OBJECT with MEMBER FUNCTION ===");
            System.out.println("  CRASHED with exception: " + ex);
            throw ex;
        }
        System.out.println("=== 5: CREATE TYPE AS OBJECT with MEMBER FUNCTION ===");
        System.out.println("  errors=" + result.getErrors());
        System.out.println("  objects=" + result.getObjects().size());
        for (ParsedObject obj : result.getObjects()) {
            System.out.println("  object: name=" + obj.getName() + " type=" + obj.getType()
                    + " subprograms=" + obj.getSubprograms().size());
        }
    }

    // 5b. Type BODY variant, only run if grammar happens to support the spec cleanly.
    @Test
    @DisplayName("5b: CREATE TYPE BODY with MEMBER FUNCTION implementation")
    void createTypeBodyWithMemberFunction() {
        String sql = """
                CREATE OR REPLACE TYPE BODY customer_obj_type AS
                  MEMBER FUNCTION get_discount_pct RETURN NUMBER IS
                  BEGIN
                    IF tier = 'GOLD' THEN
                      RETURN 10;
                    ELSE
                      RETURN 0;
                    END IF;
                  END get_discount_pct;

                  MEMBER PROCEDURE apply_discount(p_amount IN OUT NUMBER) IS
                  BEGIN
                    p_amount := p_amount - (p_amount * get_discount_pct / 100);
                  END apply_discount;
                END;
                /
                """;
        ParseResult result;
        try {
            result = parse(sql);
        } catch (RuntimeException ex) {
            System.out.println("=== 5b: CREATE TYPE BODY with MEMBER FUNCTION ===");
            System.out.println("  CRASHED with exception: " + ex);
            throw ex;
        }
        System.out.println("=== 5b: CREATE TYPE BODY with MEMBER FUNCTION ===");
        System.out.println("  errors=" + result.getErrors());
        System.out.println("  objects=" + result.getObjects().size());
        for (ParsedObject obj : result.getObjects()) {
            System.out.println("  object: name=" + obj.getName() + " type=" + obj.getType()
                    + " subprograms=" + obj.getSubprograms().size());
            for (SubprogramInfo sub : obj.getSubprograms()) {
                System.out.println("    sub: name=" + sub.getName() + " type=" + sub.getType());
            }
        }
    }
}
