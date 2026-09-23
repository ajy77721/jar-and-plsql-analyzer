package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Read-only audit (2026-09-17): probes Oracle hierarchical queries (CONNECT BY / START WITH /
 * LEVEL / SYS_CONNECT_BY_PATH / ORDER SIBLINGS BY), recursive CTEs, and sequence NEXTVAL/CURRVAL
 * usage against the real ANTLR parser/visitor and PRINTS what was captured. Diagnostic harness,
 * not a strict regression suite — assertions are kept soft (printed, not always hard-asserted)
 * so failures document gaps instead of breaking the build. Permanent evidence file.
 */
public class OracleHierarchicalAndSequenceAuditTest extends ParserTestBase {

    // ------------------------------------------------------------------
    // 1. CONNECT BY / START WITH / LEVEL
    // ------------------------------------------------------------------
    @Test
    @DisplayName("1a: Basic CONNECT BY PRIOR ... START WITH ... with LEVEL pseudocolumn")
    void connectByStartWithLevel() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE list_org_tree IS
                  BEGIN
                    FOR r IN (
                      SELECT emp_id, manager_id, LEVEL AS depth
                      FROM employees
                      START WITH manager_id IS NULL
                      CONNECT BY PRIOR emp_id = manager_id
                    ) LOOP
                      NULL;
                    END LOOP;
                  END list_org_tree;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        System.out.println("=== 1a: CONNECT BY PRIOR / START WITH / LEVEL ===");
        System.out.println("  errors=" + result.getErrors());
        SubprogramInfo sub = findSub(result, "LIST_ORG_TREE");
        System.out.println("  sub=" + (sub == null ? "NOT FOUND" : sub.getName()));
        if (sub != null) {
            for (TableOperationInfo t : sub.getTableOperations()) {
                System.out.println("  op: table=" + t.getTableName() + " op=" + t.getOperation());
            }
        }
    }

    @Test
    @DisplayName("1b: NOCYCLE CONNECT BY with SYS_CONNECT_BY_PATH and ORDER SIBLINGS BY")
    void nocycleConnectByWithPathAndSiblings() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE print_paths IS
                    v_path VARCHAR2(4000);
                  BEGIN
                    FOR r IN (
                      SELECT emp_id,
                             SYS_CONNECT_BY_PATH(emp_name, '/') AS full_path
                      FROM employees
                      START WITH manager_id IS NULL
                      CONNECT BY NOCYCLE PRIOR emp_id = manager_id
                      ORDER SIBLINGS BY emp_name
                    ) LOOP
                      v_path := r.full_path;
                    END LOOP;
                  END print_paths;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        System.out.println("=== 1b: NOCYCLE CONNECT BY + SYS_CONNECT_BY_PATH + ORDER SIBLINGS BY ===");
        System.out.println("  errors=" + result.getErrors());
        SubprogramInfo sub = findSub(result, "PRINT_PATHS");
        System.out.println("  sub=" + (sub == null ? "NOT FOUND" : sub.getName()));
        if (sub != null) {
            for (TableOperationInfo t : sub.getTableOperations()) {
                System.out.println("  op: table=" + t.getTableName() + " op=" + t.getOperation());
            }
            for (CallInfo c : sub.getCalls()) {
                System.out.println("  call: " + c.getName());
            }
        }
    }

    // ------------------------------------------------------------------
    // 2. Recursive CTEs
    // ------------------------------------------------------------------
    @Test
    @DisplayName("2a: Plain WITH self-referencing CTE (no RECURSIVE keyword)")
    void recursiveCtePlainWith() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE build_org_chart IS
                  BEGIN
                    FOR r IN (
                      WITH org_chart (emp_id, manager_id, lvl) AS (
                        SELECT emp_id, manager_id, 1
                        FROM employees
                        WHERE manager_id IS NULL
                        UNION ALL
                        SELECT e.emp_id, e.manager_id, oc.lvl + 1
                        FROM employees e
                        JOIN org_chart oc ON e.manager_id = oc.emp_id
                      )
                      SELECT * FROM org_chart
                    ) LOOP
                      NULL;
                    END LOOP;
                  END build_org_chart;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        System.out.println("=== 2a: Plain WITH self-referencing CTE ===");
        System.out.println("  errors=" + result.getErrors());
        SubprogramInfo sub = findSub(result, "BUILD_ORG_CHART");
        System.out.println("  sub=" + (sub == null ? "NOT FOUND" : sub.getName()));
        if (sub != null) {
            for (TableOperationInfo t : sub.getTableOperations()) {
                System.out.println("  op: table=" + t.getTableName() + " op=" + t.getOperation()
                        + " alias=" + t.getAlias());
            }
        }
        // Documents the classic bug: does 'ORG_CHART' (the CTE's own name) show up as a
        // table operation, alongside the real base table EMPLOYEES?
        boolean orgChartWronglyAsTable = false;
        boolean employeesCaptured = false;
        if (sub != null) {
            for (TableOperationInfo t : sub.getTableOperations()) {
                if ("ORG_CHART".equalsIgnoreCase(t.getTableName())) orgChartWronglyAsTable = true;
                if ("EMPLOYEES".equalsIgnoreCase(t.getTableName())) employeesCaptured = true;
            }
        }
        org.junit.jupiter.api.Assertions.assertTrue(result.getErrors().isEmpty());
        org.junit.jupiter.api.Assertions.assertNotNull(sub);
        org.junit.jupiter.api.Assertions.assertTrue(employeesCaptured, "real base table EMPLOYEES must be captured");
        org.junit.jupiter.api.Assertions.assertFalse(orgChartWronglyAsTable,
                "CTE's own name ORG_CHART must not be recorded as an external table inside a cursor FOR loop");
    }

    @Test
    @DisplayName("2b: WITH RECURSIVE self-referencing CTE (Oracle 23c+ keyword form)")
    void recursiveCteWithKeyword() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE build_org_chart2 IS
                  BEGIN
                    FOR r IN (
                      WITH RECURSIVE org_chart (emp_id, manager_id, lvl) AS (
                        SELECT emp_id, manager_id, 1
                        FROM employees
                        WHERE manager_id IS NULL
                        UNION ALL
                        SELECT e.emp_id, e.manager_id, oc.lvl + 1
                        FROM employees e
                        JOIN org_chart oc ON e.manager_id = oc.emp_id
                      )
                      SELECT * FROM org_chart
                    ) LOOP
                      NULL;
                    END LOOP;
                  END build_org_chart2;
                END test_pkg;
                /
                """;
        ParseResult result;
        try {
            result = parse(sql);
        } catch (RuntimeException ex) {
            System.out.println("=== 2b: WITH RECURSIVE keyword form ===");
            System.out.println("  CRASHED with exception: " + ex);
            throw ex;
        }
        System.out.println("=== 2b: WITH RECURSIVE keyword form ===");
        System.out.println("  errors=" + result.getErrors());
        SubprogramInfo sub = findSub(result, "BUILD_ORG_CHART2");
        System.out.println("  sub=" + (sub == null ? "NOT FOUND" : sub.getName()));
        boolean employeesCaptured = false;
        boolean orgChartWronglyAsTable = false;
        if (sub != null) {
            for (TableOperationInfo t : sub.getTableOperations()) {
                System.out.println("  op: table=" + t.getTableName() + " op=" + t.getOperation()
                        + " alias=" + t.getAlias());
                if ("EMPLOYEES".equalsIgnoreCase(t.getTableName())) employeesCaptured = true;
                if ("ORG_CHART".equalsIgnoreCase(t.getTableName())) orgChartWronglyAsTable = true;
            }
        }
        org.junit.jupiter.api.Assertions.assertTrue(result.getErrors().isEmpty(),
                "WITH RECURSIVE (23c keyword form) must parse without errors");
        org.junit.jupiter.api.Assertions.assertNotNull(sub);
        org.junit.jupiter.api.Assertions.assertTrue(employeesCaptured);
        org.junit.jupiter.api.Assertions.assertFalse(orgChartWronglyAsTable);
    }

    @Test
    @DisplayName("2c: Multi-level chained recursive CTEs (second CTE recurses on first)")
    void chainedRecursiveCtes() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE build_chained_chart IS
                  BEGIN
                    FOR r IN (
                      WITH base_emp (emp_id, manager_id) AS (
                        SELECT emp_id, manager_id FROM employees WHERE active = 'Y'
                      ),
                      org_chart (emp_id, manager_id, lvl) AS (
                        SELECT emp_id, manager_id, 1
                        FROM base_emp
                        WHERE manager_id IS NULL
                        UNION ALL
                        SELECT be.emp_id, be.manager_id, oc.lvl + 1
                        FROM base_emp be
                        JOIN org_chart oc ON be.manager_id = oc.emp_id
                      )
                      SELECT * FROM org_chart
                    ) LOOP
                      NULL;
                    END LOOP;
                  END build_chained_chart;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        System.out.println("=== 2c: Multi-level chained recursive CTEs ===");
        System.out.println("  errors=" + result.getErrors());
        SubprogramInfo sub = findSub(result, "BUILD_CHAINED_CHART");
        System.out.println("  sub=" + (sub == null ? "NOT FOUND" : sub.getName()));
        boolean employeesCaptured = false;
        boolean baseEmpWronglyAsTable = false;
        boolean orgChartWronglyAsTable = false;
        if (sub != null) {
            for (TableOperationInfo t : sub.getTableOperations()) {
                System.out.println("  op: table=" + t.getTableName() + " op=" + t.getOperation()
                        + " alias=" + t.getAlias());
                if ("EMPLOYEES".equalsIgnoreCase(t.getTableName())) employeesCaptured = true;
                if ("BASE_EMP".equalsIgnoreCase(t.getTableName())) baseEmpWronglyAsTable = true;
                if ("ORG_CHART".equalsIgnoreCase(t.getTableName())) orgChartWronglyAsTable = true;
            }
        }
        org.junit.jupiter.api.Assertions.assertTrue(result.getErrors().isEmpty());
        org.junit.jupiter.api.Assertions.assertNotNull(sub);
        org.junit.jupiter.api.Assertions.assertTrue(employeesCaptured);
        org.junit.jupiter.api.Assertions.assertFalse(baseEmpWronglyAsTable,
                "first chained CTE's own name BASE_EMP must not be recorded as an external table");
        org.junit.jupiter.api.Assertions.assertFalse(orgChartWronglyAsTable,
                "second chained CTE's own name ORG_CHART must not be recorded as an external table");
    }

    @Test
    @DisplayName("2d: Self-referencing CTE used as a plain SELECT (not inside a cursor FOR loop)")
    void recursiveCtePlainSelectNotInForLoop() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE build_org_chart3(p_cur OUT SYS_REFCURSOR) IS
                  BEGIN
                    OPEN p_cur FOR
                      WITH org_chart (emp_id, manager_id, lvl) AS (
                        SELECT emp_id, manager_id, 1
                        FROM employees
                        WHERE manager_id IS NULL
                        UNION ALL
                        SELECT e.emp_id, e.manager_id, oc.lvl + 1
                        FROM employees e
                        JOIN org_chart oc ON e.manager_id = oc.emp_id
                      )
                      SELECT * FROM org_chart;
                  END build_org_chart3;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        System.out.println("=== 2d: Self-referencing CTE via OPEN ... FOR (not a cursor FOR loop) ===");
        System.out.println("  errors=" + result.getErrors());
        SubprogramInfo sub = findSub(result, "BUILD_ORG_CHART3");
        System.out.println("  sub=" + (sub == null ? "NOT FOUND" : sub.getName()));
        if (sub != null) {
            for (TableOperationInfo t : sub.getTableOperations()) {
                System.out.println("  op: table=" + t.getTableName() + " op=" + t.getOperation()
                        + " alias=" + t.getAlias());
            }
        }
    }

    // ------------------------------------------------------------------
    // 3. Sequences: NEXTVAL / CURRVAL
    // ------------------------------------------------------------------
    @Test
    @DisplayName("3a: INSERT VALUES using seq.NEXTVAL — real table must not be dropped")
    void sequenceNextvalInInsertValues() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE add_order(p_customer_id NUMBER) IS
                  BEGIN
                    INSERT INTO orders (order_id, customer_id, created_date)
                    VALUES (orders_seq.NEXTVAL, p_customer_id, SYSDATE);
                  END add_order;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        System.out.println("=== 3a: seq.NEXTVAL in INSERT VALUES ===");
        System.out.println("  errors=" + result.getErrors());
        SubprogramInfo sub = findSub(result, "ADD_ORDER");
        System.out.println("  sub=" + (sub == null ? "NOT FOUND" : sub.getName()));
        if (sub != null) {
            for (TableOperationInfo t : sub.getTableOperations()) {
                System.out.println("  op: table=" + t.getTableName() + " op=" + t.getOperation());
            }
            for (CallInfo c : sub.getCalls()) {
                System.out.println("  call: name=" + c.getName() + " type=" + c.getType()
                        + " pkg=" + c.getPackageName());
            }
            ParsedObject po = result.getObjects().isEmpty() ? null : result.getObjects().get(0);
            if (po != null && po.getDependencies() != null) {
                System.out.println("  dependencies.sequences=" + po.getDependencies().getSequences());
            }
        }
    }

    @Test
    @DisplayName("3b: SELECT seq.NEXTVAL FROM dual assigned to a variable")
    void sequenceNextvalInSelectAssignment() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE get_next_id(p_new_id OUT NUMBER) IS
                  BEGIN
                    SELECT orders_seq.NEXTVAL INTO p_new_id FROM dual;
                  END get_next_id;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        System.out.println("=== 3b: SELECT seq.NEXTVAL FROM dual ===");
        System.out.println("  errors=" + result.getErrors());
        SubprogramInfo sub = findSub(result, "GET_NEXT_ID");
        System.out.println("  sub=" + (sub == null ? "NOT FOUND" : sub.getName()));
        if (sub != null) {
            for (TableOperationInfo t : sub.getTableOperations()) {
                System.out.println("  op: table=" + t.getTableName() + " op=" + t.getOperation());
            }
            for (CallInfo c : sub.getCalls()) {
                System.out.println("  call: name=" + c.getName() + " type=" + c.getType()
                        + " pkg=" + c.getPackageName());
            }
            ParsedObject po = result.getObjects().isEmpty() ? null : result.getObjects().get(0);
            if (po != null && po.getDependencies() != null) {
                System.out.println("  dependencies.sequences=" + po.getDependencies().getSequences());
            }
        }
    }

    @Test
    @DisplayName("3c: Direct assignment v_id := seq.CURRVAL; and schema.seq.NEXTVAL form")
    void sequenceCurrvalAssignmentAndSchemaQualified() {
        String sql = """
                CREATE OR REPLACE PACKAGE BODY test_pkg AS
                  PROCEDURE mirror_last_id(p_customer_id NUMBER) IS
                    v_id NUMBER;
                  BEGIN
                    INSERT INTO orders (order_id, customer_id)
                    VALUES (hr.orders_seq.NEXTVAL, p_customer_id);

                    v_id := orders_seq.CURRVAL;

                    INSERT INTO order_audit (order_id, note)
                    VALUES (v_id, 'mirrored');
                  END mirror_last_id;
                END test_pkg;
                /
                """;
        ParseResult result = parse(sql);
        System.out.println("=== 3c: v_id := seq.CURRVAL; + schema.seq.NEXTVAL ===");
        System.out.println("  errors=" + result.getErrors());
        SubprogramInfo sub = findSub(result, "MIRROR_LAST_ID");
        System.out.println("  sub=" + (sub == null ? "NOT FOUND" : sub.getName()));
        if (sub != null) {
            for (TableOperationInfo t : sub.getTableOperations()) {
                System.out.println("  op: table=" + t.getTableName() + " op=" + t.getOperation());
            }
            for (CallInfo c : sub.getCalls()) {
                System.out.println("  call: name=" + c.getName() + " type=" + c.getType()
                        + " pkg=" + c.getPackageName());
            }
            ParsedObject po = result.getObjects().isEmpty() ? null : result.getObjects().get(0);
            if (po != null && po.getDependencies() != null) {
                System.out.println("  dependencies.sequences=" + po.getDependencies().getSequences());
            }
        }
        // Expect: two real INSERT table ops (ORDERS, ORDER_AUDIT), and sequence usages recorded
        // for ORDERS_SEQ (both NEXTVAL and CURRVAL) without ORDERS_SEQ ever appearing as a table.
        List<String> tableNames = new java.util.ArrayList<>();
        if (sub != null) {
            for (TableOperationInfo t : sub.getTableOperations()) {
                tableNames.add(t.getTableName() == null ? null : t.getTableName().toUpperCase());
            }
        }
        System.out.println("  tableNames=" + tableNames);
        boolean seqLeakedAsTable = tableNames.stream().anyMatch(n -> n != null && n.contains("ORDERS_SEQ"));
        System.out.println("  seqLeakedAsTable=" + seqLeakedAsTable);
    }
}
