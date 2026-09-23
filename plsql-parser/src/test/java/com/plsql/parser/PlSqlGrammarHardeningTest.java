package com.plsql.parser;

import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("PL/SQL Grammar Hardening — Oracle 8i through 23c")
public class PlSqlGrammarHardeningTest extends ParserTestBase {

    // =========================================================================
    // 1. INSERT Variations
    // =========================================================================
    @Nested
    @DisplayName("INSERT Variations")
    class InsertVariations {

        @Test
        @DisplayName("INSERT with RETURNING INTO")
        void insertReturningInto() {
            String sql = wrap("""
                PROCEDURE P IS
                  v_id NUMBER;
                BEGIN
                  INSERT INTO EMPLOYEES (NAME, SALARY) VALUES ('John', 5000)
                    RETURNING EMPLOYEE_ID INTO v_id;
                END P;
                """);
            assertNoParsErrors(parse(sql), "INSERT RETURNING INTO");
        }

        @Test
        @DisplayName("INSERT ALL multi-table conditional")
        void insertAllConditional() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  INSERT ALL
                    WHEN sal > 5000 THEN INTO high_earners (id, sal) VALUES (emp_id, sal)
                    WHEN sal <= 5000 THEN INTO low_earners (id, sal) VALUES (emp_id, sal)
                    ELSE INTO others (id, sal) VALUES (emp_id, sal)
                  SELECT emp_id, sal FROM employees;
                END P;
                """);
            assertNoParsErrors(parse(sql), "INSERT ALL conditional");
        }

        @Test
        @DisplayName("INSERT ALL unconditional multi-table")
        void insertAllUnconditional() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  INSERT ALL
                    INTO audit_log (action, ts) VALUES ('INSERT', SYSDATE)
                    INTO backup_log (action, ts) VALUES ('INSERT', SYSDATE)
                  SELECT * FROM dual;
                END P;
                """);
            assertNoParsErrors(parse(sql), "INSERT ALL unconditional");
        }

        @Test
        @DisplayName("INSERT FIRST conditional")
        void insertFirstConditional() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  INSERT FIRST
                    WHEN amt > 10000 THEN INTO big_txn (id, amt) VALUES (txn_id, amt)
                    WHEN amt > 1000 THEN INTO med_txn (id, amt) VALUES (txn_id, amt)
                    ELSE INTO small_txn (id, amt) VALUES (txn_id, amt)
                  SELECT txn_id, amt FROM transactions;
                END P;
                """);
            assertNoParsErrors(parse(sql), "INSERT FIRST conditional");
        }

        @Test
        @DisplayName("INSERT with subquery")
        void insertWithSubquery() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  INSERT INTO archive_emp (id, name, dept)
                    SELECT employee_id, first_name, department_id
                      FROM employees WHERE status = 'TERM';
                END P;
                """);
            assertNoParsErrors(parse(sql), "INSERT with subquery");
        }

        @Test
        @DisplayName("INSERT with hints")
        void insertWithHints() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  INSERT /*+ APPEND */ INTO big_table (id, data)
                    SELECT id, data FROM staging_table;
                END P;
                """);
            assertNoParsErrors(parse(sql), "INSERT with APPEND hint");
        }
    }

    // =========================================================================
    // 2. MERGE Variations
    // =========================================================================
    @Nested
    @DisplayName("MERGE Variations")
    class MergeVariations {

        @Test
        @DisplayName("MERGE with UPDATE and INSERT")
        void mergeBasic() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  MERGE INTO target t
                  USING source s ON (t.id = s.id)
                  WHEN MATCHED THEN UPDATE SET t.val = s.val
                  WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val);
                END P;
                """);
            assertNoParsErrors(parse(sql), "MERGE basic");
        }

        @Test
        @DisplayName("MERGE with UPDATE DELETE WHERE")
        void mergeWithDelete() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  MERGE INTO target t
                  USING source s ON (t.id = s.id)
                  WHEN MATCHED THEN UPDATE SET t.val = s.val
                    DELETE WHERE t.status = 'DELETED'
                  WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val);
                END P;
                """);
            assertNoParsErrors(parse(sql), "MERGE with DELETE WHERE");
        }

        @Test
        @DisplayName("MERGE using subquery as source")
        void mergeSubquerySource() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  MERGE INTO target t
                  USING (SELECT id, val FROM staging WHERE batch = 1) s ON (t.id = s.id)
                  WHEN MATCHED THEN UPDATE SET t.val = s.val
                  WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val);
                END P;
                """);
            assertNoParsErrors(parse(sql), "MERGE with subquery source");
        }
    }

    // =========================================================================
    // 3. Dynamic SQL / EXECUTE IMMEDIATE
    // =========================================================================
    @Nested
    @DisplayName("EXECUTE IMMEDIATE Variations")
    class ExecuteImmediate {

        @Test
        @DisplayName("EXECUTE IMMEDIATE with INTO and USING")
        void execImmediateIntoUsing() {
            String sql = wrap("""
                PROCEDURE P IS
                  v_cnt NUMBER;
                BEGIN
                  EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM employees WHERE dept = :d'
                    INTO v_cnt USING 10;
                END P;
                """);
            assertNoParsErrors(parse(sql), "EXEC IMMEDIATE INTO USING");
        }

        @Test
        @DisplayName("EXECUTE IMMEDIATE with RETURNING INTO")
        void execImmediateReturningInto() {
            String sql = wrap("""
                PROCEDURE P IS
                  v_id NUMBER;
                BEGIN
                  EXECUTE IMMEDIATE 'INSERT INTO t(name) VALUES(:n) RETURNING id INTO :id'
                    USING 'test' RETURNING INTO v_id;
                END P;
                """);
            assertNoParsErrors(parse(sql), "EXEC IMMEDIATE RETURNING INTO");
        }

        @Test
        @DisplayName("EXECUTE IMMEDIATE DDL with concatenation")
        void execImmediateDdlConcat() {
            String sql = wrap("""
                PROCEDURE P(p_table VARCHAR2) IS
                BEGIN
                  EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || p_table;
                END P;
                """);
            assertNoParsErrors(parse(sql), "EXEC IMMEDIATE DDL concat");
        }

        @Test
        @DisplayName("EXECUTE IMMEDIATE dynamic PL/SQL block")
        void execImmediatePlsqlBlock() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  EXECUTE IMMEDIATE 'BEGIN pkg.proc(:1, :2); END;' USING 10, 'test';
                END P;
                """);
            assertNoParsErrors(parse(sql), "EXEC IMMEDIATE PL/SQL block");
        }

        @Test
        @DisplayName("EXECUTE IMMEDIATE with BULK COLLECT INTO")
        void execImmediateBulkCollect() {
            String sql = wrap("""
                PROCEDURE P IS
                  TYPE t_ids IS TABLE OF NUMBER;
                  v_ids t_ids;
                BEGIN
                  EXECUTE IMMEDIATE 'SELECT id FROM employees' BULK COLLECT INTO v_ids;
                END P;
                """);
            assertNoParsErrors(parse(sql), "EXEC IMMEDIATE BULK COLLECT INTO");
        }
    }

    // =========================================================================
    // 4. CURSOR Patterns
    // =========================================================================
    @Nested
    @DisplayName("Cursor Patterns")
    class CursorPatterns {

        @Test
        @DisplayName("Cursor FOR loop with inline SELECT")
        void cursorForLoopInline() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  FOR rec IN (SELECT id, name FROM employees WHERE dept = 10) LOOP
                    DBMS_OUTPUT.PUT_LINE(rec.name);
                  END LOOP;
                END P;
                """);
            assertNoParsErrors(parse(sql), "Cursor FOR loop inline SELECT");
        }

        @Test
        @DisplayName("Parameterized cursor")
        void parameterizedCursor() {
            String sql = wrap("""
                PROCEDURE P IS
                  CURSOR c_emp(p_dept NUMBER, p_status VARCHAR2) IS
                    SELECT * FROM employees WHERE department_id = p_dept AND status = p_status;
                  r_emp c_emp%ROWTYPE;
                BEGIN
                  OPEN c_emp(10, 'ACTIVE');
                  FETCH c_emp INTO r_emp;
                  CLOSE c_emp;
                END P;
                """);
            assertNoParsErrors(parse(sql), "Parameterized cursor");
        }

        @Test
        @DisplayName("REF CURSOR with OPEN FOR")
        void refCursorOpenFor() {
            String sql = wrap("""
                PROCEDURE P(p_rc OUT SYS_REFCURSOR) IS
                BEGIN
                  OPEN p_rc FOR SELECT id, name FROM employees;
                END P;
                """);
            assertNoParsErrors(parse(sql), "REF CURSOR OPEN FOR");
        }

        @Test
        @DisplayName("REF CURSOR with dynamic OPEN FOR USING")
        void refCursorDynamicOpenFor() {
            String sql = wrap("""
                PROCEDURE P(p_rc OUT SYS_REFCURSOR, p_dept NUMBER) IS
                  v_sql VARCHAR2(4000);
                BEGIN
                  v_sql := 'SELECT * FROM employees WHERE dept_id = :d';
                  OPEN p_rc FOR v_sql USING p_dept;
                END P;
                """);
            assertNoParsErrors(parse(sql), "REF CURSOR dynamic OPEN FOR USING");
        }

        @Test
        @DisplayName("FETCH BULK COLLECT INTO with LIMIT")
        void fetchBulkCollectLimit() {
            String sql = wrap("""
                PROCEDURE P IS
                  CURSOR c IS SELECT * FROM employees;
                  TYPE t IS TABLE OF employees%ROWTYPE;
                  v_batch t;
                BEGIN
                  OPEN c;
                  LOOP
                    FETCH c BULK COLLECT INTO v_batch LIMIT 1000;
                    EXIT WHEN v_batch.COUNT = 0;
                  END LOOP;
                  CLOSE c;
                END P;
                """);
            assertNoParsErrors(parse(sql), "FETCH BULK COLLECT LIMIT");
        }

        @Test
        @DisplayName("Cursor attributes: %FOUND %NOTFOUND %ROWCOUNT %ISOPEN")
        void cursorAttributes() {
            String sql = wrap("""
                PROCEDURE P IS
                  CURSOR c IS SELECT id FROM employees;
                  v_id NUMBER;
                BEGIN
                  OPEN c;
                  FETCH c INTO v_id;
                  IF c%FOUND THEN
                    DBMS_OUTPUT.PUT_LINE('Found: ' || c%ROWCOUNT);
                  END IF;
                  IF c%NOTFOUND THEN NULL; END IF;
                  IF c%ISOPEN THEN CLOSE c; END IF;
                END P;
                """);
            assertNoParsErrors(parse(sql), "Cursor attributes");
        }
    }

    // =========================================================================
    // 5. FORALL & BULK COLLECT
    // =========================================================================
    @Nested
    @DisplayName("FORALL & BULK COLLECT")
    class ForallBulkCollect {

        @Test
        @DisplayName("FORALL with SAVE EXCEPTIONS")
        void forallSaveExceptions() {
            String sql = wrap("""
                PROCEDURE P IS
                  TYPE t IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
                  v_ids t;
                  v_errors NUMBER;
                BEGIN
                  v_ids(1) := 10; v_ids(2) := 20;
                  FORALL i IN 1..v_ids.COUNT SAVE EXCEPTIONS
                    DELETE FROM employees WHERE id = v_ids(i);
                EXCEPTION
                  WHEN OTHERS THEN
                    v_errors := SQL%BULK_EXCEPTIONS.COUNT;
                    FOR j IN 1..v_errors LOOP
                      DBMS_OUTPUT.PUT_LINE(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX || ': ' || SQL%BULK_EXCEPTIONS(j).ERROR_CODE);
                    END LOOP;
                END P;
                """);
            assertNoParsErrors(parse(sql), "FORALL SAVE EXCEPTIONS");
        }

        @Test
        @DisplayName("FORALL INDICES OF")
        void forallIndicesOf() {
            String sql = wrap("""
                PROCEDURE P IS
                  TYPE t IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
                  v_ids t;
                BEGIN
                  v_ids(1) := 10; v_ids(5) := 50;
                  FORALL i IN INDICES OF v_ids
                    UPDATE employees SET status = 'X' WHERE id = v_ids(i);
                END P;
                """);
            assertNoParsErrors(parse(sql), "FORALL INDICES OF");
        }

        @Test
        @DisplayName("FORALL VALUES OF")
        void forallValuesOf() {
            String sql = wrap("""
                PROCEDURE P IS
                  TYPE t_idx IS TABLE OF PLS_INTEGER INDEX BY PLS_INTEGER;
                  TYPE t_data IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
                  v_idx t_idx;
                  v_data t_data;
                BEGIN
                  FORALL i IN VALUES OF v_idx
                    INSERT INTO target (val) VALUES (v_data(i));
                END P;
                """);
            assertNoParsErrors(parse(sql), "FORALL VALUES OF");
        }

        @Test
        @DisplayName("SELECT BULK COLLECT INTO")
        void selectBulkCollect() {
            String sql = wrap("""
                PROCEDURE P IS
                  TYPE t IS TABLE OF NUMBER;
                  v_ids t;
                  v_names DBMS_SQL.VARCHAR2_TABLE;
                BEGIN
                  SELECT employee_id, first_name
                    BULK COLLECT INTO v_ids, v_names
                    FROM employees WHERE department_id = 10;
                END P;
                """);
            assertNoParsErrors(parse(sql), "SELECT BULK COLLECT INTO");
        }
    }

    // =========================================================================
    // 6. Exception Handling
    // =========================================================================
    @Nested
    @DisplayName("Exception Handling Patterns")
    class ExceptionHandling {

        @Test
        @DisplayName("Multiple WHEN branches with OR")
        void multipleWhenBranches() {
            String sql = wrap("""
                PROCEDURE P IS
                  v_val NUMBER;
                BEGIN
                  SELECT col INTO v_val FROM t WHERE id = 1;
                EXCEPTION
                  WHEN NO_DATA_FOUND THEN v_val := 0;
                  WHEN TOO_MANY_ROWS THEN v_val := -1;
                  WHEN DUP_VAL_ON_INDEX OR VALUE_ERROR THEN v_val := -2;
                  WHEN OTHERS THEN
                    RAISE_APPLICATION_ERROR(-20001, 'Error: ' || SQLERRM);
                END P;
                """);
            assertNoParsErrors(parse(sql), "Multiple WHEN exception branches");
        }

        @Test
        @DisplayName("PRAGMA EXCEPTION_INIT")
        void pragmaExceptionInit() {
            String sql = wrap("""
                PROCEDURE P IS
                  e_deadlock EXCEPTION;
                  PRAGMA EXCEPTION_INIT(e_deadlock, -60);
                BEGIN
                  NULL;
                EXCEPTION
                  WHEN e_deadlock THEN
                    DBMS_OUTPUT.PUT_LINE('Deadlock detected');
                END P;
                """);
            assertNoParsErrors(parse(sql), "PRAGMA EXCEPTION_INIT");
        }

        @Test
        @DisplayName("Nested exception handlers")
        void nestedException() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  BEGIN
                    INSERT INTO t VALUES (1);
                  EXCEPTION
                    WHEN DUP_VAL_ON_INDEX THEN
                      BEGIN
                        UPDATE t SET val = 1 WHERE id = 1;
                      EXCEPTION
                        WHEN OTHERS THEN RAISE;
                      END;
                  END;
                END P;
                """);
            assertNoParsErrors(parse(sql), "Nested exception handlers");
        }
    }

    // =========================================================================
    // 7. CASE Expressions and Statements
    // =========================================================================
    @Nested
    @DisplayName("CASE Patterns")
    class CasePatterns {

        @Test
        @DisplayName("Simple CASE statement")
        void simpleCaseStatement() {
            String sql = wrap("""
                PROCEDURE P(p_grade CHAR) IS
                BEGIN
                  CASE p_grade
                    WHEN 'A' THEN DBMS_OUTPUT.PUT_LINE('Excellent');
                    WHEN 'B' THEN DBMS_OUTPUT.PUT_LINE('Good');
                    WHEN 'C' THEN DBMS_OUTPUT.PUT_LINE('Average');
                    ELSE DBMS_OUTPUT.PUT_LINE('Unknown');
                  END CASE;
                END P;
                """);
            assertNoParsErrors(parse(sql), "Simple CASE statement");
        }

        @Test
        @DisplayName("Searched CASE expression in SELECT")
        void searchedCaseInSelect() {
            String sql = wrap("""
                PROCEDURE P IS
                  v_label VARCHAR2(20);
                BEGIN
                  SELECT CASE
                    WHEN salary > 10000 THEN 'HIGH'
                    WHEN salary > 5000 THEN 'MEDIUM'
                    ELSE 'LOW'
                  END INTO v_label FROM employees WHERE ROWNUM = 1;
                END P;
                """);
            assertNoParsErrors(parse(sql), "Searched CASE expression in SELECT");
        }

        @Test
        @DisplayName("CASE expression in assignment")
        void caseInAssignment() {
            String sql = wrap("""
                PROCEDURE P IS
                  v_status VARCHAR2(10);
                  v_code NUMBER := 3;
                BEGIN
                  v_status := CASE v_code
                    WHEN 1 THEN 'NEW'
                    WHEN 2 THEN 'PENDING'
                    WHEN 3 THEN 'DONE'
                    ELSE 'UNKNOWN'
                  END;
                END P;
                """);
            assertNoParsErrors(parse(sql), "CASE expression in assignment");
        }

        @Test
        @DisplayName("Nested CASE expressions")
        void nestedCase() {
            String sql = wrap("""
                PROCEDURE P IS
                  v_result VARCHAR2(50);
                BEGIN
                  SELECT CASE
                    WHEN type = 'A' THEN
                      CASE WHEN amount > 1000 THEN 'A-HIGH' ELSE 'A-LOW' END
                    WHEN type = 'B' THEN 'B-ALL'
                    ELSE 'OTHER'
                  END INTO v_result FROM orders WHERE ROWNUM = 1;
                END P;
                """);
            assertNoParsErrors(parse(sql), "Nested CASE expressions");
        }
    }

    // =========================================================================
    // 8. Compound Triggers (Oracle 11g+)
    // =========================================================================
    @Nested
    @DisplayName("Compound Triggers")
    class CompoundTriggers {

        @Test
        @DisplayName("Compound DML trigger with all timing points")
        void compoundTriggerAllTimingPoints() {
            String sql = """
                CREATE OR REPLACE TRIGGER trg_compound
                FOR INSERT OR UPDATE ON employees
                COMPOUND TRIGGER
                  TYPE t_ids IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
                  v_ids t_ids;

                  BEFORE STATEMENT IS
                  BEGIN
                    v_ids.DELETE;
                  END BEFORE STATEMENT;

                  BEFORE EACH ROW IS
                  BEGIN
                    :NEW.modified_date := SYSDATE;
                  END BEFORE EACH ROW;

                  AFTER EACH ROW IS
                  BEGIN
                    v_ids(v_ids.COUNT + 1) := :NEW.id;
                  END AFTER EACH ROW;

                  AFTER STATEMENT IS
                  BEGIN
                    FORALL i IN 1..v_ids.COUNT
                      INSERT INTO audit_log (emp_id, action_date) VALUES (v_ids(i), SYSDATE);
                  END AFTER STATEMENT;
                END trg_compound;
                /
                """;
            assertNoParsErrors(parse(sql), "Compound trigger all timing points");
        }
    }

    // =========================================================================
    // 9. Conditional Compilation ($IF/$THEN/$END)
    // =========================================================================
    @Nested
    @DisplayName("Conditional Compilation")
    class ConditionalCompilation {

        @Test
        @DisplayName("$IF $THEN $ELSE $END in procedure body")
        void conditionalCompilationBasic() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  $IF DBMS_DB_VERSION.VER_LE_10 $THEN
                    DBMS_OUTPUT.PUT_LINE('10g or earlier');
                  $ELSE
                    DBMS_OUTPUT.PUT_LINE('11g or later');
                  $END
                END P;
                """);
            assertNoParsErrors(parse(sql), "Conditional compilation $IF");
        }
    }

    // =========================================================================
    // 10. Pipelined Functions & PIPE ROW
    // =========================================================================
    @Nested
    @DisplayName("Pipelined Functions")
    class PipelinedFunctions {

        @Test
        @DisplayName("PIPELINED function with PIPE ROW")
        void pipelinedFunction() {
            String sql = """
                CREATE OR REPLACE FUNCTION get_numbers(p_max NUMBER)
                  RETURN sys.odcinumberlist PIPELINED IS
                BEGIN
                  FOR i IN 1..p_max LOOP
                    PIPE ROW(i);
                  END LOOP;
                  RETURN;
                END get_numbers;
                /
                """;
            assertNoParsErrors(parse(sql), "PIPELINED function with PIPE ROW");
        }
    }

    // =========================================================================
    // 11. Hierarchical Queries (CONNECT BY)
    // =========================================================================
    @Nested
    @DisplayName("Hierarchical Queries")
    class HierarchicalQueries {

        @Test
        @DisplayName("CONNECT BY with START WITH")
        void connectByStartWith() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  FOR rec IN (
                    SELECT LEVEL, employee_id, manager_id,
                           SYS_CONNECT_BY_PATH(last_name, '/') path
                      FROM employees
                     START WITH manager_id IS NULL
                   CONNECT BY PRIOR employee_id = manager_id
                     ORDER SIBLINGS BY last_name
                  ) LOOP
                    DBMS_OUTPUT.PUT_LINE(rec.path);
                  END LOOP;
                END P;
                """);
            assertNoParsErrors(parse(sql), "CONNECT BY START WITH");
        }

        @Test
        @DisplayName("CONNECT BY NOCYCLE")
        void connectByNocycle() {
            String sql = wrap("""
                PROCEDURE P IS
                  v_path VARCHAR2(4000);
                BEGIN
                  SELECT SYS_CONNECT_BY_PATH(name, '->') INTO v_path
                    FROM hierarchy
                   WHERE ROWNUM = 1
                   START WITH parent_id IS NULL
                 CONNECT BY NOCYCLE PRIOR id = parent_id;
                END P;
                """);
            assertNoParsErrors(parse(sql), "CONNECT BY NOCYCLE");
        }
    }

    // =========================================================================
    // 12. WITH clause (CTE)
    // =========================================================================
    @Nested
    @DisplayName("WITH Clause (CTE)")
    class WithClause {

        @Test
        @DisplayName("Simple CTE")
        void simpleCte() {
            String sql = wrap("""
                PROCEDURE P IS
                  v_cnt NUMBER;
                BEGIN
                  WITH active_emp AS (
                    SELECT employee_id FROM employees WHERE status = 'ACTIVE'
                  )
                  SELECT COUNT(*) INTO v_cnt FROM active_emp;
                END P;
                """);
            assertNoParsErrors(parse(sql), "Simple CTE");
        }

        @Test
        @DisplayName("Multiple CTEs")
        void multipleCtes() {
            String sql = wrap("""
                PROCEDURE P IS
                  v_cnt NUMBER;
                BEGIN
                  WITH
                    dept_emps AS (SELECT department_id, COUNT(*) cnt FROM employees GROUP BY department_id),
                    big_depts AS (SELECT department_id FROM dept_emps WHERE cnt > 50)
                  SELECT COUNT(*) INTO v_cnt FROM big_depts;
                END P;
                """);
            assertNoParsErrors(parse(sql), "Multiple CTEs");
        }

        @Test
        @DisplayName("Recursive CTE")
        void recursiveCte() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  FOR rec IN (
                    WITH hierarchy(id, parent_id, lvl) AS (
                      SELECT id, parent_id, 1 FROM org WHERE parent_id IS NULL
                      UNION ALL
                      SELECT o.id, o.parent_id, h.lvl + 1
                        FROM org o JOIN hierarchy h ON o.parent_id = h.id
                    )
                    SELECT * FROM hierarchy
                  ) LOOP
                    NULL;
                  END LOOP;
                END P;
                """);
            assertNoParsErrors(parse(sql), "Recursive CTE");
        }
    }

    // =========================================================================
    // 13. Analytic / Window Functions
    // =========================================================================
    @Nested
    @DisplayName("Analytic Functions")
    class AnalyticFunctions {

        @Test
        @DisplayName("ROW_NUMBER, RANK, DENSE_RANK, NTILE")
        void rankingFunctions() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  FOR rec IN (
                    SELECT employee_id,
                           ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) rn,
                           RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) rnk,
                           DENSE_RANK() OVER (ORDER BY salary DESC) drnk,
                           NTILE(4) OVER (ORDER BY salary) quartile
                      FROM employees
                  ) LOOP
                    NULL;
                  END LOOP;
                END P;
                """);
            assertNoParsErrors(parse(sql), "Ranking functions");
        }

        @Test
        @DisplayName("LAG, LEAD, FIRST_VALUE, LAST_VALUE")
        void lagLeadFunctions() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  FOR rec IN (
                    SELECT employee_id,
                           LAG(salary, 1, 0) OVER (ORDER BY hire_date) prev_sal,
                           LEAD(salary, 1, 0) OVER (ORDER BY hire_date) next_sal,
                           FIRST_VALUE(salary) OVER (PARTITION BY dept_id ORDER BY hire_date) first_sal,
                           LAST_VALUE(salary) OVER (PARTITION BY dept_id ORDER BY hire_date
                             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_sal
                      FROM employees
                  ) LOOP
                    NULL;
                  END LOOP;
                END P;
                """);
            assertNoParsErrors(parse(sql), "LAG LEAD FIRST_VALUE LAST_VALUE");
        }

        @Test
        @DisplayName("Running SUM with ROWS BETWEEN")
        void runningSumRowsBetween() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  FOR rec IN (
                    SELECT order_date,
                           SUM(amount) OVER (ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_total
                      FROM orders
                  ) LOOP
                    NULL;
                  END LOOP;
                END P;
                """);
            assertNoParsErrors(parse(sql), "Running SUM ROWS BETWEEN");
        }

        @Test
        @DisplayName("LISTAGG with WITHIN GROUP")
        void listaggWithinGroup() {
            String sql = wrap("""
                PROCEDURE P IS
                  v_names VARCHAR2(4000);
                BEGIN
                  SELECT LISTAGG(last_name, ', ') WITHIN GROUP (ORDER BY last_name)
                    INTO v_names
                    FROM employees WHERE department_id = 10;
                END P;
                """);
            assertNoParsErrors(parse(sql), "LISTAGG WITHIN GROUP");
        }
    }

    // =========================================================================
    // 14. Legacy Patterns
    // =========================================================================
    @Nested
    @DisplayName("Legacy Patterns")
    class LegacyPatterns {

        @Test
        @DisplayName("Old-style outer join (+)")
        void oldStyleOuterJoin() {
            String sql = wrap("""
                PROCEDURE P IS
                  v_name VARCHAR2(100);
                BEGIN
                  SELECT e.name INTO v_name
                    FROM employees e, departments d
                   WHERE e.dept_id = d.id(+)
                     AND ROWNUM = 1;
                END P;
                """);
            assertNoParsErrors(parse(sql), "Old-style outer join (+)");
        }

        @Test
        @DisplayName("DECODE function")
        void decodeFunction() {
            String sql = wrap("""
                PROCEDURE P IS
                  v_label VARCHAR2(20);
                BEGIN
                  SELECT DECODE(status, 'A', 'Active', 'I', 'Inactive', 'Unknown')
                    INTO v_label FROM employees WHERE ROWNUM = 1;
                END P;
                """);
            assertNoParsErrors(parse(sql), "DECODE function");
        }

        @Test
        @DisplayName("NVL, NVL2, COALESCE")
        void nvlCoalesce() {
            String sql = wrap("""
                PROCEDURE P IS
                  v_val VARCHAR2(100);
                BEGIN
                  SELECT NVL(col1, 'default') INTO v_val FROM t WHERE ROWNUM = 1;
                  SELECT NVL2(col1, 'has val', 'no val') INTO v_val FROM t WHERE ROWNUM = 1;
                  SELECT COALESCE(col1, col2, col3, 'fallback') INTO v_val FROM t WHERE ROWNUM = 1;
                END P;
                """);
            assertNoParsErrors(parse(sql), "NVL NVL2 COALESCE");
        }
    }

    // =========================================================================
    // 15. Subquery Patterns
    // =========================================================================
    @Nested
    @DisplayName("Subquery Patterns")
    class SubqueryPatterns {

        @Test
        @DisplayName("EXISTS and NOT EXISTS subquery")
        void existsNotExists() {
            String sql = wrap("""
                PROCEDURE P IS
                  v_cnt NUMBER;
                BEGIN
                  SELECT COUNT(*) INTO v_cnt FROM departments d
                   WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id)
                     AND NOT EXISTS (SELECT 1 FROM archived_depts a WHERE a.id = d.id);
                END P;
                """);
            assertNoParsErrors(parse(sql), "EXISTS NOT EXISTS subquery");
        }

        @Test
        @DisplayName("Scalar subquery in SELECT list")
        void scalarSubquery() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  FOR rec IN (
                    SELECT e.name,
                           (SELECT d.name FROM departments d WHERE d.id = e.dept_id) dept_name,
                           (SELECT COUNT(*) FROM orders o WHERE o.emp_id = e.id) order_count
                      FROM employees e
                  ) LOOP
                    NULL;
                  END LOOP;
                END P;
                """);
            assertNoParsErrors(parse(sql), "Scalar subquery in SELECT list");
        }

        @Test
        @DisplayName("IN subquery and ANY/ALL quantified")
        void inAndQuantified() {
            String sql = wrap("""
                PROCEDURE P IS
                  v_cnt NUMBER;
                BEGIN
                  SELECT COUNT(*) INTO v_cnt FROM employees
                   WHERE department_id IN (SELECT id FROM departments WHERE location = 'NYC')
                     AND salary > ALL (SELECT AVG(salary) FROM employees GROUP BY department_id);
                END P;
                """);
            assertNoParsErrors(parse(sql), "IN and ALL quantified subquery");
        }

        @Test
        @DisplayName("FROM clause subquery (inline view)")
        void inlineView() {
            String sql = wrap("""
                PROCEDURE P IS
                  v_cnt NUMBER;
                BEGIN
                  SELECT COUNT(*) INTO v_cnt
                    FROM (SELECT * FROM employees WHERE salary > 5000) high_paid
                    JOIN departments d ON high_paid.dept_id = d.id;
                END P;
                """);
            assertNoParsErrors(parse(sql), "Inline view in FROM");
        }
    }

    // =========================================================================
    // 16. Pragmas and Autonomous Transactions
    // =========================================================================
    @Nested
    @DisplayName("Pragmas")
    class Pragmas {

        @Test
        @DisplayName("PRAGMA AUTONOMOUS_TRANSACTION")
        void autonomousTransaction() {
            String sql = wrap("""
                PROCEDURE LOG_ERROR(p_msg VARCHAR2) IS
                  PRAGMA AUTONOMOUS_TRANSACTION;
                BEGIN
                  INSERT INTO error_log (msg, ts) VALUES (p_msg, SYSDATE);
                  COMMIT;
                END LOG_ERROR;
                """);
            assertNoParsErrors(parse(sql), "PRAGMA AUTONOMOUS_TRANSACTION");
        }

        @Test
        @DisplayName("PRAGMA SERIALLY_REUSABLE at package level")
        void seriallyReusable() {
            String sql = """
                CREATE OR REPLACE PACKAGE BODY sr_pkg AS
                  PRAGMA SERIALLY_REUSABLE;
                  g_counter NUMBER := 0;
                  PROCEDURE INC IS
                  BEGIN
                    g_counter := g_counter + 1;
                  END INC;
                END sr_pkg;
                /
                """;
            assertNoParsErrors(parse(sql), "PRAGMA SERIALLY_REUSABLE");
        }
    }

    // =========================================================================
    // 17. Labeled Loops with EXIT/CONTINUE
    // =========================================================================
    @Nested
    @DisplayName("Labeled Loops")
    class LabeledLoops {

        @Test
        @DisplayName("Labeled nested loops with EXIT and CONTINUE")
        void labeledNestedLoops() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  <<outer_loop>>
                  FOR i IN 1..10 LOOP
                    <<inner_loop>>
                    FOR j IN 1..10 LOOP
                      CONTINUE outer_loop WHEN j = 5;
                      EXIT outer_loop WHEN i * j > 50;
                      NULL;
                    END LOOP inner_loop;
                  END LOOP outer_loop;
                END P;
                """);
            assertNoParsErrors(parse(sql), "Labeled loops EXIT CONTINUE");
        }
    }

    // =========================================================================
    // 18. REGEXP Functions
    // =========================================================================
    @Nested
    @DisplayName("REGEXP Functions")
    class RegexpFunctions {

        @Test
        @DisplayName("REGEXP_LIKE, REGEXP_SUBSTR, REGEXP_REPLACE, REGEXP_INSTR, REGEXP_COUNT")
        void regexpFunctions() {
            String sql = wrap("""
                PROCEDURE P IS
                  v_result VARCHAR2(100);
                  v_pos NUMBER;
                  v_cnt NUMBER;
                BEGIN
                  IF REGEXP_LIKE('Hello World', '^Hello', 'i') THEN
                    v_result := REGEXP_SUBSTR('abc123def', '[0-9]+');
                    v_result := REGEXP_REPLACE('foo bar baz', '\\s+', '_');
                    v_pos := REGEXP_INSTR('abc123', '[0-9]');
                    v_cnt := REGEXP_COUNT('aabbaab', 'a{2}');
                  END IF;
                END P;
                """);
            assertNoParsErrors(parse(sql), "REGEXP functions");
        }
    }

    // =========================================================================
    // 19. Transaction Control
    // =========================================================================
    @Nested
    @DisplayName("Transaction Control")
    class TransactionControl {

        @Test
        @DisplayName("SAVEPOINT, ROLLBACK TO SAVEPOINT, COMMIT")
        void savepointRollback() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  SAVEPOINT sp1;
                  INSERT INTO t VALUES (1);
                  SAVEPOINT sp2;
                  INSERT INTO t VALUES (2);
                  ROLLBACK TO SAVEPOINT sp1;
                  COMMIT;
                END P;
                """);
            assertNoParsErrors(parse(sql), "SAVEPOINT ROLLBACK COMMIT");
        }
    }

    // =========================================================================
    // 20. GROUP BY Variations
    // =========================================================================
    @Nested
    @DisplayName("GROUP BY Variations")
    class GroupByVariations {

        @Test
        @DisplayName("GROUP BY ROLLUP")
        void groupByRollup() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  FOR rec IN (
                    SELECT department_id, job_id, SUM(salary) total
                      FROM employees
                     GROUP BY ROLLUP(department_id, job_id)
                  ) LOOP
                    NULL;
                  END LOOP;
                END P;
                """);
            assertNoParsErrors(parse(sql), "GROUP BY ROLLUP");
        }

        @Test
        @DisplayName("GROUP BY CUBE")
        void groupByCube() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  FOR rec IN (
                    SELECT department_id, job_id, COUNT(*) cnt
                      FROM employees
                     GROUP BY CUBE(department_id, job_id)
                  ) LOOP
                    NULL;
                  END LOOP;
                END P;
                """);
            assertNoParsErrors(parse(sql), "GROUP BY CUBE");
        }

        @Test
        @DisplayName("GROUP BY GROUPING SETS")
        void groupByGroupingSets() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  FOR rec IN (
                    SELECT department_id, job_id, SUM(salary) total
                      FROM employees
                     GROUP BY GROUPING SETS ((department_id), (job_id), ())
                  ) LOOP
                    NULL;
                  END LOOP;
                END P;
                """);
            assertNoParsErrors(parse(sql), "GROUP BY GROUPING SETS");
        }
    }

    // =========================================================================
    // 21. PIVOT / UNPIVOT
    // =========================================================================
    @Nested
    @DisplayName("PIVOT / UNPIVOT")
    class PivotUnpivot {

        @Test
        @DisplayName("PIVOT with aggregate")
        void pivotQuery() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  FOR rec IN (
                    SELECT * FROM (
                      SELECT department_id, job_id, salary FROM employees
                    ) PIVOT (
                      SUM(salary) FOR job_id IN ('CLERK' AS clerk_sal, 'MANAGER' AS mgr_sal)
                    )
                  ) LOOP
                    NULL;
                  END LOOP;
                END P;
                """);
            assertNoParsErrors(parse(sql), "PIVOT query");
        }

        @Test
        @DisplayName("UNPIVOT")
        void unpivotQuery() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  FOR rec IN (
                    SELECT * FROM quarterly_sales
                    UNPIVOT (
                      amount FOR quarter IN (q1_sales AS 'Q1', q2_sales AS 'Q2', q3_sales AS 'Q3', q4_sales AS 'Q4')
                    )
                  ) LOOP
                    NULL;
                  END LOOP;
                END P;
                """);
            assertNoParsErrors(parse(sql), "UNPIVOT query");
        }
    }

    // =========================================================================
    // 22. SET Operations (UNION, MINUS, INTERSECT)
    // =========================================================================
    @Nested
    @DisplayName("SET Operations")
    class SetOperations {

        @Test
        @DisplayName("UNION ALL, MINUS, INTERSECT combined")
        void setOperationsCombined() {
            String sql = wrap("""
                PROCEDURE P IS
                  v_cnt NUMBER;
                BEGIN
                  SELECT COUNT(*) INTO v_cnt FROM (
                    SELECT id FROM employees WHERE dept = 10
                    UNION ALL
                    SELECT id FROM employees WHERE dept = 20
                    MINUS
                    SELECT id FROM terminated_employees
                    INTERSECT
                    SELECT id FROM active_employees
                  );
                END P;
                """);
            assertNoParsErrors(parse(sql), "UNION ALL MINUS INTERSECT");
        }
    }

    // =========================================================================
    // 23. FETCH FIRST / OFFSET (Oracle 12c+)
    // =========================================================================
    @Nested
    @DisplayName("FETCH FIRST / OFFSET")
    class FetchFirst {

        @Test
        @DisplayName("OFFSET ROWS FETCH FIRST n ROWS ONLY")
        void offsetFetchFirst() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  FOR rec IN (
                    SELECT * FROM employees ORDER BY salary DESC
                    OFFSET 10 ROWS FETCH FIRST 5 ROWS ONLY
                  ) LOOP
                    NULL;
                  END LOOP;
                END P;
                """);
            assertNoParsErrors(parse(sql), "OFFSET FETCH FIRST ROWS ONLY");
        }

        @Test
        @DisplayName("FETCH FIRST n PERCENT ROWS ONLY")
        void fetchFirstPercent() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  FOR rec IN (
                    SELECT * FROM employees ORDER BY salary DESC
                    FETCH FIRST 10 PERCENT ROWS ONLY
                  ) LOOP
                    NULL;
                  END LOOP;
                END P;
                """);
            assertNoParsErrors(parse(sql), "FETCH FIRST PERCENT");
        }
    }

    // =========================================================================
    // 24. MULTISET Operations
    // =========================================================================
    @Nested
    @DisplayName("MULTISET Operations")
    class MultisetOperations {

        @Test
        @DisplayName("MULTISET UNION, EXCEPT, INTERSECT")
        void multisetOperations() {
            String sql = wrap("""
                PROCEDURE P IS
                  TYPE t_nums IS TABLE OF NUMBER;
                  a t_nums := t_nums(1, 2, 3);
                  b t_nums := t_nums(3, 4, 5);
                  c t_nums;
                BEGIN
                  c := a MULTISET UNION b;
                  c := a MULTISET UNION ALL b;
                  c := a MULTISET INTERSECT b;
                  c := a MULTISET EXCEPT b;
                END P;
                """);
            assertNoParsErrors(parse(sql), "MULTISET operations");
        }

        @Test
        @DisplayName("TABLE function and CAST MULTISET")
        void tableFunctionCastMultiset() {
            String sql = wrap("""
                PROCEDURE P IS
                  TYPE t_nums IS TABLE OF NUMBER;
                  v_nums t_nums;
                  v_cnt NUMBER;
                BEGIN
                  SELECT COUNT(*) INTO v_cnt FROM TABLE(v_nums);
                  SELECT CAST(MULTISET(SELECT id FROM employees WHERE dept = 10) AS t_nums)
                    INTO v_nums FROM dual;
                END P;
                """);
            assertNoParsErrors(parse(sql), "TABLE function CAST MULTISET");
        }
    }

    // =========================================================================
    // 25. Record Types and %ROWTYPE
    // =========================================================================
    @Nested
    @DisplayName("Record Types and %ROWTYPE")
    class RecordTypes {

        @Test
        @DisplayName("Record type declaration and usage")
        void recordTypeDecl() {
            String sql = wrap("""
                PROCEDURE P IS
                  TYPE t_emp_rec IS RECORD (
                    id NUMBER,
                    name VARCHAR2(100),
                    salary NUMBER(10,2)
                  );
                  v_rec t_emp_rec;
                BEGIN
                  v_rec.id := 1;
                  v_rec.name := 'John';
                  v_rec.salary := 5000.00;
                END P;
                """);
            assertNoParsErrors(parse(sql), "Record type declaration");
        }

        @Test
        @DisplayName("Table %ROWTYPE and cursor %ROWTYPE")
        void rowtypeDecl() {
            String sql = wrap("""
                PROCEDURE P IS
                  CURSOR c IS SELECT * FROM employees;
                  v_emp employees%ROWTYPE;
                  v_crec c%ROWTYPE;
                BEGIN
                  SELECT * INTO v_emp FROM employees WHERE ROWNUM = 1;
                  OPEN c;
                  FETCH c INTO v_crec;
                  CLOSE c;
                END P;
                """);
            assertNoParsErrors(parse(sql), "%ROWTYPE declarations");
        }
    }

    // =========================================================================
    // 26. Object Types
    // =========================================================================
    @Nested
    @DisplayName("Object Types")
    class ObjectTypes {

        @Test
        @DisplayName("Object type with MEMBER FUNCTION")
        void objectTypeMemberFunction() {
            String sql = """
                CREATE OR REPLACE TYPE BODY address_type AS
                  MEMBER FUNCTION get_full_address RETURN VARCHAR2 IS
                  BEGIN
                    RETURN SELF.street || ', ' || SELF.city || ' ' || SELF.zip;
                  END get_full_address;

                  MEMBER PROCEDURE set_zip(p_zip VARCHAR2) IS
                  BEGIN
                    SELF.zip := p_zip;
                  END set_zip;

                  STATIC FUNCTION create_default RETURN address_type IS
                  BEGIN
                    RETURN address_type('Unknown', 'Unknown', '00000');
                  END create_default;

                  MAP MEMBER FUNCTION sort_key RETURN VARCHAR2 IS
                  BEGIN
                    RETURN SELF.zip || SELF.city;
                  END sort_key;
                END;
                /
                """;
            assertNoParsErrors(parse(sql), "Object type MEMBER FUNCTION");
        }
    }

    // =========================================================================
    // 27. Flashback Queries (Oracle 10g+)
    // =========================================================================
    @Nested
    @DisplayName("Flashback Queries")
    class FlashbackQueries {

        @Test
        @DisplayName("AS OF TIMESTAMP")
        void asOfTimestamp() {
            String sql = wrap("""
                PROCEDURE P IS
                  v_name VARCHAR2(100);
                BEGIN
                  SELECT name INTO v_name
                    FROM employees AS OF TIMESTAMP (SYSTIMESTAMP - INTERVAL '1' HOUR)
                   WHERE id = 100;
                END P;
                """);
            assertNoParsErrors(parse(sql), "AS OF TIMESTAMP");
        }

        @Test
        @DisplayName("VERSIONS BETWEEN")
        void versionsBetween() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  FOR rec IN (
                    SELECT versions_starttime, versions_endtime, versions_operation, name
                      FROM employees VERSIONS BETWEEN TIMESTAMP MINVALUE AND MAXVALUE
                     WHERE id = 100
                  ) LOOP
                    NULL;
                  END LOOP;
                END P;
                """);
            assertNoParsErrors(parse(sql), "VERSIONS BETWEEN");
        }
    }

    // =========================================================================
    // 28. Complex Real-World Patterns
    // =========================================================================
    @Nested
    @DisplayName("Complex Real-World Patterns")
    class ComplexRealWorld {

        @Test
        @DisplayName("INSERT with RETURNING BULK COLLECT INTO")
        void insertReturningBulkCollect() {
            String sql = wrap("""
                PROCEDURE P IS
                  TYPE t_ids IS TABLE OF NUMBER;
                  v_ids t_ids;
                BEGIN
                  INSERT INTO employees (name) VALUES ('John')
                    RETURNING employee_id BULK COLLECT INTO v_ids;
                END P;
                """);
            assertNoParsErrors(parse(sql), "INSERT RETURNING BULK COLLECT INTO");
        }

        @Test
        @DisplayName("UPDATE with RETURNING INTO")
        void updateReturningInto() {
            String sql = wrap("""
                PROCEDURE P IS
                  v_old_salary NUMBER;
                BEGIN
                  UPDATE employees SET salary = salary * 1.1
                   WHERE employee_id = 100
                   RETURNING salary INTO v_old_salary;
                END P;
                """);
            assertNoParsErrors(parse(sql), "UPDATE RETURNING INTO");
        }

        @Test
        @DisplayName("DELETE with RETURNING INTO")
        void deleteReturningInto() {
            String sql = wrap("""
                PROCEDURE P IS
                  v_name VARCHAR2(100);
                BEGIN
                  DELETE FROM employees WHERE employee_id = 100
                    RETURNING first_name INTO v_name;
                END P;
                """);
            assertNoParsErrors(parse(sql), "DELETE RETURNING INTO");
        }

        @Test
        @DisplayName("Complex procedure with multiple advanced features")
        void complexMultiFeature() {
            String sql = wrap("""
                PROCEDURE PROCESS_BATCH(p_dept_id NUMBER) IS
                  PRAGMA AUTONOMOUS_TRANSACTION;
                  CURSOR c_emp(p_d NUMBER) IS
                    SELECT e.employee_id, e.salary,
                           RANK() OVER (ORDER BY e.salary DESC) sal_rank,
                           LAG(e.salary) OVER (ORDER BY e.hire_date) prev_sal
                      FROM employees e
                     WHERE e.department_id = p_d
                       AND e.status = NVL(NULL, 'ACTIVE');
                  TYPE t_emp IS TABLE OF c_emp%ROWTYPE;
                  v_batch t_emp;
                  v_err_count NUMBER := 0;
                BEGIN
                  OPEN c_emp(p_dept_id);
                  LOOP
                    FETCH c_emp BULK COLLECT INTO v_batch LIMIT 500;
                    EXIT WHEN v_batch.COUNT = 0;

                    FORALL i IN 1..v_batch.COUNT SAVE EXCEPTIONS
                      MERGE INTO emp_summary t
                      USING (SELECT v_batch(i).employee_id AS id, v_batch(i).salary AS sal FROM dual) s
                      ON (t.emp_id = s.id)
                      WHEN MATCHED THEN UPDATE SET t.salary = s.sal
                      WHEN NOT MATCHED THEN INSERT (emp_id, salary) VALUES (s.id, s.sal);
                  END LOOP;
                  CLOSE c_emp;
                  COMMIT;
                EXCEPTION
                  WHEN OTHERS THEN
                    v_err_count := SQL%BULK_EXCEPTIONS.COUNT;
                    ROLLBACK;
                    RAISE_APPLICATION_ERROR(-20001, 'Failed with ' || v_err_count || ' errors: ' || SQLERRM);
                END PROCESS_BATCH;
                """);
            assertNoParsErrors(parse(sql), "Complex multi-feature procedure");
        }

        @Test
        @DisplayName("Package with mixed DML, cursors, collections, exceptions")
        void fullPackageBody() {
            String sql = """
                CREATE OR REPLACE PACKAGE BODY BATCH_PROCESSOR AS

                  PROCEDURE ARCHIVE_OLD_RECORDS(p_cutoff_date DATE) IS
                    TYPE t_ids IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
                    v_ids t_ids;
                    v_count NUMBER := 0;
                    e_no_records EXCEPTION;
                  BEGIN
                    SELECT id BULK COLLECT INTO v_ids
                      FROM transactions
                     WHERE txn_date < p_cutoff_date;

                    IF v_ids.COUNT = 0 THEN
                      RAISE e_no_records;
                    END IF;

                    FORALL i IN 1..v_ids.COUNT
                      INSERT INTO archive_transactions
                        SELECT * FROM transactions WHERE id = v_ids(i);

                    FORALL i IN 1..v_ids.COUNT
                      DELETE FROM transactions WHERE id = v_ids(i);

                    v_count := SQL%ROWCOUNT;
                    COMMIT;

                    INSERT INTO audit_log (action, details, ts)
                      VALUES ('ARCHIVE', 'Archived ' || v_count || ' records', SYSDATE);
                    COMMIT;

                  EXCEPTION
                    WHEN e_no_records THEN
                      NULL;
                    WHEN OTHERS THEN
                      ROLLBACK;
                      RAISE;
                  END ARCHIVE_OLD_RECORDS;

                  FUNCTION GET_SUMMARY(p_dept NUMBER) RETURN SYS_REFCURSOR IS
                    v_rc SYS_REFCURSOR;
                  BEGIN
                    OPEN v_rc FOR
                      WITH dept_stats AS (
                        SELECT department_id,
                               COUNT(*) emp_count,
                               AVG(salary) avg_sal,
                               LISTAGG(last_name, ', ') WITHIN GROUP (ORDER BY last_name) names
                          FROM employees
                         WHERE department_id = p_dept
                         GROUP BY department_id
                      )
                      SELECT ds.*,
                             (SELECT COUNT(*) FROM projects pr WHERE pr.dept_id = ds.department_id) proj_count
                        FROM dept_stats ds;
                    RETURN v_rc;
                  END GET_SUMMARY;

                END BATCH_PROCESSOR;
                /
                """;
            assertNoParsErrors(parse(sql), "Full package body with mixed features");
        }

        @Test
        @DisplayName("Deeply nested control flow with labeled loops")
        void deeplyNestedControlFlow() {
            String sql = wrap("""
                PROCEDURE P IS
                  v_found BOOLEAN := FALSE;
                BEGIN
                  <<dept_loop>>
                  FOR d IN (SELECT id FROM departments) LOOP
                    <<emp_loop>>
                    FOR e IN (SELECT * FROM employees WHERE dept_id = d.id) LOOP
                      CASE e.status
                        WHEN 'ACTIVE' THEN
                          IF e.salary > 10000 THEN
                            BEGIN
                              UPDATE bonuses SET amount = e.salary * 0.1 WHERE emp_id = e.employee_id;
                              IF SQL%ROWCOUNT = 0 THEN
                                INSERT INTO bonuses (emp_id, amount) VALUES (e.employee_id, e.salary * 0.1);
                              END IF;
                            EXCEPTION
                              WHEN DUP_VAL_ON_INDEX THEN
                                CONTINUE emp_loop;
                            END;
                          END IF;
                        WHEN 'TERMINATED' THEN
                          EXIT dept_loop WHEN v_found;
                        ELSE
                          NULL;
                      END CASE;
                    END LOOP emp_loop;
                  END LOOP dept_loop;
                END P;
                """);
            assertNoParsErrors(parse(sql), "Deeply nested control flow");
        }
    }

    // =========================================================================
    // 29. INSTEAD OF Trigger (Oracle 9i+)
    // =========================================================================
    @Nested
    @DisplayName("INSTEAD OF Trigger")
    class InsteadOfTrigger {

        @Test
        @DisplayName("INSTEAD OF INSERT on view")
        void insteadOfInsert() {
            String sql = """
                CREATE OR REPLACE TRIGGER trg_emp_view
                INSTEAD OF INSERT ON emp_dept_view
                FOR EACH ROW
                BEGIN
                  INSERT INTO employees (id, name) VALUES (:NEW.emp_id, :NEW.emp_name);
                  INSERT INTO dept_assign (emp_id, dept_id) VALUES (:NEW.emp_id, :NEW.dept_id);
                END trg_emp_view;
                /
                """;
            assertNoParsErrors(parse(sql), "INSTEAD OF INSERT trigger");
        }
    }

    // =========================================================================
    // 30. Type Declarations (Nested Table, VARRAY, Associative Array)
    // =========================================================================
    @Nested
    @DisplayName("Type Declarations")
    class TypeDeclarations {

        @Test
        @DisplayName("Nested table, VARRAY, associative array declarations")
        void typeDeclarations() {
            String sql = wrap("""
                PROCEDURE P IS
                  TYPE t_nested IS TABLE OF VARCHAR2(100);
                  TYPE t_varray IS VARRAY(10) OF NUMBER;
                  TYPE t_assoc IS TABLE OF DATE INDEX BY VARCHAR2(50);
                  TYPE t_assoc2 IS TABLE OF employees%ROWTYPE INDEX BY PLS_INTEGER;
                  v1 t_nested := t_nested('a', 'b', 'c');
                  v2 t_varray := t_varray(1, 2, 3);
                  v3 t_assoc;
                BEGIN
                  v3('key1') := SYSDATE;
                  v1.EXTEND;
                  v1(v1.LAST) := 'd';
                END P;
                """);
            assertNoParsErrors(parse(sql), "Type declarations");
        }
    }

    // =========================================================================
    // 31. DBMS_SQL Usage
    // =========================================================================
    @Nested
    @DisplayName("DBMS_SQL Usage")
    class DbmsSqlUsage {

        @Test
        @DisplayName("DBMS_SQL OPEN_CURSOR, PARSE, EXECUTE, CLOSE_CURSOR")
        void dbmsSqlBasic() {
            String sql = wrap("""
                PROCEDURE P(p_table VARCHAR2) IS
                  l_cursor INTEGER;
                  l_result INTEGER;
                BEGIN
                  l_cursor := DBMS_SQL.OPEN_CURSOR;
                  DBMS_SQL.PARSE(l_cursor, 'DELETE FROM ' || p_table || ' WHERE status = :s', DBMS_SQL.NATIVE);
                  DBMS_SQL.BIND_VARIABLE(l_cursor, ':s', 'INACTIVE');
                  l_result := DBMS_SQL.EXECUTE(l_cursor);
                  DBMS_SQL.CLOSE_CURSOR(l_cursor);
                EXCEPTION
                  WHEN OTHERS THEN
                    IF DBMS_SQL.IS_OPEN(l_cursor) THEN
                      DBMS_SQL.CLOSE_CURSOR(l_cursor);
                    END IF;
                    RAISE;
                END P;
                """);
            assertNoParsErrors(parse(sql), "DBMS_SQL operations");
        }
    }

    // =========================================================================
    // 32. Multiple UPDATE SET with subquery
    // =========================================================================
    @Nested
    @DisplayName("UPDATE Variations")
    class UpdateVariations {

        @Test
        @DisplayName("UPDATE SET with subquery assignment")
        void updateSetSubquery() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  UPDATE employees e
                     SET (salary, commission_pct) = (
                       SELECT AVG(salary), AVG(commission_pct)
                         FROM employees WHERE department_id = e.department_id
                     )
                   WHERE e.employee_id = 100;
                END P;
                """);
            assertNoParsErrors(parse(sql), "UPDATE SET with subquery assignment");
        }

        @Test
        @DisplayName("UPDATE with correlated subquery in SET")
        void updateCorrelatedSet() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  UPDATE employees e
                     SET salary = (SELECT MAX(salary) FROM employees WHERE department_id = e.department_id)
                   WHERE e.job_id = 'MANAGER';
                END P;
                """);
            assertNoParsErrors(parse(sql), "UPDATE correlated subquery SET");
        }
    }

    // =========================================================================
    // 33. XMLType Operations
    // =========================================================================
    @Nested
    @DisplayName("XMLType Operations")
    class XmlTypeOps {

        @Test
        @DisplayName("XMLELEMENT, XMLFOREST, XMLAGG")
        void xmlFunctions() {
            String sql = wrap("""
                PROCEDURE P IS
                  v_xml XMLTYPE;
                BEGIN
                  SELECT XMLELEMENT("employees",
                           XMLAGG(
                             XMLELEMENT("emp",
                               XMLFOREST(employee_id AS "id", first_name AS "name")
                             )
                           )
                         )
                    INTO v_xml FROM employees WHERE department_id = 10;
                END P;
                """);
            assertNoParsErrors(parse(sql), "XMLELEMENT XMLFOREST XMLAGG");
        }
    }

    // =========================================================================
    // 34. JSON Operations (Oracle 12c+)
    // =========================================================================
    @Nested
    @DisplayName("JSON Operations")
    class JsonOps {

        @Test
        @DisplayName("JSON_OBJECT and JSON_ARRAY")
        void jsonObjectArray() {
            String sql = wrap("""
                PROCEDURE P IS
                  v_json VARCHAR2(4000);
                BEGIN
                  SELECT JSON_OBJECT(KEY 'name' VALUE first_name, KEY 'id' VALUE employee_id)
                    INTO v_json FROM employees WHERE ROWNUM = 1;
                END P;
                """);
            assertNoParsErrors(parse(sql), "JSON_OBJECT JSON_ARRAY");
        }

        @Test
        @DisplayName("JSON_TABLE")
        void jsonTable() {
            String sql = wrap("""
                PROCEDURE P IS
                BEGIN
                  FOR rec IN (
                    SELECT jt.name, jt.age
                      FROM json_docs d,
                           JSON_TABLE(d.doc, '$' COLUMNS (
                             name VARCHAR2(100) PATH '$.name',
                             age NUMBER PATH '$.age'
                           )) jt
                  ) LOOP
                    NULL;
                  END LOOP;
                END P;
                """);
            assertNoParsErrors(parse(sql), "JSON_TABLE");
        }
    }

    // =========================================================================
    // 35. ACCESSIBLE BY Clause (Oracle 12c+)
    // =========================================================================
    @Nested
    @DisplayName("ACCESSIBLE BY Clause")
    class AccessibleBy {

        @Test
        @DisplayName("Function with ACCESSIBLE BY")
        void accessibleByFunction() {
            String sql = """
                CREATE OR REPLACE FUNCTION helper_fn RETURN NUMBER
                  ACCESSIBLE BY (PACKAGE my_pkg, PROCEDURE admin_proc)
                IS
                BEGIN
                  RETURN 42;
                END helper_fn;
                /
                """;
            assertNoParsErrors(parse(sql), "ACCESSIBLE BY clause");
        }
    }

    // =========================================================================
    // 36. RESULT_CACHE, DETERMINISTIC, PARALLEL_ENABLE
    // =========================================================================
    @Nested
    @DisplayName("Function Annotations")
    class FunctionAnnotations {

        @Test
        @DisplayName("RESULT_CACHE function")
        void resultCacheFunction() {
            String sql = """
                CREATE OR REPLACE FUNCTION get_dept_name(p_id NUMBER) RETURN VARCHAR2
                  RESULT_CACHE RELIES_ON (departments)
                IS
                  v_name VARCHAR2(100);
                BEGIN
                  SELECT name INTO v_name FROM departments WHERE id = p_id;
                  RETURN v_name;
                END get_dept_name;
                /
                """;
            assertNoParsErrors(parse(sql), "RESULT_CACHE function");
        }

        @Test
        @DisplayName("DETERMINISTIC function")
        void deterministicFunction() {
            String sql = """
                CREATE OR REPLACE FUNCTION calc_tax(p_amount NUMBER) RETURN NUMBER
                  DETERMINISTIC
                IS
                BEGIN
                  RETURN p_amount * 0.15;
                END calc_tax;
                /
                """;
            assertNoParsErrors(parse(sql), "DETERMINISTIC function");
        }
    }

    // =========================================================================
    // 37. NOCOPY Parameter Hint
    // =========================================================================
    @Nested
    @DisplayName("NOCOPY Parameter")
    class NocopyParameter {

        @Test
        @DisplayName("IN OUT NOCOPY parameter")
        void nocopyParam() {
            String sql = wrap("""
                PROCEDURE P(p_data IN OUT NOCOPY CLOB) IS
                BEGIN
                  p_data := p_data || ' appended';
                END P;
                """);
            assertNoParsErrors(parse(sql), "IN OUT NOCOPY parameter");
        }
    }

    // =========================================================================
    // Helper: wraps PL/SQL in a package body
    // =========================================================================
    private String wrap(String procBody) {
        return """
            CREATE OR REPLACE PACKAGE BODY TEST_PKG AS
              %s
            END TEST_PKG;
            /
            """.formatted(procBody);
    }
}
