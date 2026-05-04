CREATE OR REPLACE PACKAGE BODY PKG_DYNAMIC_SQL_TEST AS

    -- Package-level constants
    gc_max_sql_len CONSTANT NUMBER := 4000;

    PROCEDURE PC_EXEC_IMM_LITERAL IS
        v_cnt       NUMBER;
        v_table_cnt NUMBER;
    BEGIN
        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM my_table' INTO v_cnt;

        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM CUSTOMER.order_items' INTO v_table_cnt;

        IF v_cnt > 0 THEN
            INSERT INTO query_log (query_text, result_count, run_date)
            VALUES ('SELECT COUNT(*) FROM my_table', v_cnt, SYSDATE);
        END IF;
    END PC_EXEC_IMM_LITERAL;

    PROCEDURE PC_EXEC_IMM_VARIABLE IS
        v_sql       VARCHAR2(4000);
        v_cnt       NUMBER;
        v_dept_cnt  NUMBER;
    BEGIN
        v_sql := 'SELECT COUNT(*) FROM employees WHERE department_id = 10';
        EXECUTE IMMEDIATE v_sql INTO v_cnt;

        v_sql := 'SELECT COUNT(DISTINCT department_id) FROM HR.employees';
        EXECUTE IMMEDIATE v_sql INTO v_dept_cnt;

        INSERT INTO dynamic_query_log (sql_text, row_count, executed_date)
        VALUES (v_sql, v_dept_cnt, SYSDATE);
    END PC_EXEC_IMM_VARIABLE;

    PROCEDURE PC_EXEC_IMM_CONCAT(
        p_table  IN VARCHAR2,
        p_id     IN NUMBER,
        p_schema IN VARCHAR2 DEFAULT NULL
    ) IS
        v_full_table VARCHAR2(200);
        v_sql        VARCHAR2(4000);
    BEGIN
        IF p_schema IS NOT NULL THEN
            v_full_table := p_schema || '.' || p_table;
        ELSE
            v_full_table := p_table;
        END IF;

        v_sql := 'DELETE FROM ' || v_full_table || ' WHERE id = :1';

        EXECUTE IMMEDIATE v_sql USING p_id;

        INSERT INTO audit_log (id, action, table_name, action_date)
        VALUES (audit_seq.NEXTVAL, 'DELETE', v_full_table, SYSDATE);
    END PC_EXEC_IMM_CONCAT;

    PROCEDURE PC_EXEC_IMM_USING_IN(p_id IN NUMBER) IS
        v_name       VARCHAR2(100);
        v_salary     NUMBER;
        v_dept_name  VARCHAR2(100);
    BEGIN
        EXECUTE IMMEDIATE 'SELECT last_name FROM employees WHERE employee_id = :1'
            INTO v_name
            USING p_id;

        EXECUTE IMMEDIATE 'SELECT salary FROM employees WHERE employee_id = :1'
            INTO v_salary
            USING p_id;

        EXECUTE IMMEDIATE 'SELECT d.department_name FROM employees e JOIN departments d ON e.department_id = d.department_id WHERE e.employee_id = :1'
            INTO v_dept_name
            USING p_id;
    END PC_EXEC_IMM_USING_IN;

    PROCEDURE PC_EXEC_IMM_USING_OUT(p_value IN NUMBER) IS
        v_result     NUMBER;
        v_doubled    NUMBER;
        v_msg        VARCHAR2(200);
    BEGIN
        EXECUTE IMMEDIATE 'BEGIN :1 := :2 * 2; END;'
            USING OUT v_result, IN p_value;

        EXECUTE IMMEDIATE 'BEGIN :1 := :2 || '' processed''; END;'
            USING OUT v_msg, IN TO_CHAR(p_value);

        v_doubled := v_result;
    END PC_EXEC_IMM_USING_OUT;

    PROCEDURE PC_EXEC_IMM_INTO IS
        v_max_sal    NUMBER;
        v_min_sal    NUMBER;
        v_emp_name   VARCHAR2(100);
        v_emp_count  NUMBER;
    BEGIN
        EXECUTE IMMEDIATE 'SELECT MAX(salary) FROM HR.employees' INTO v_max_sal;

        EXECUTE IMMEDIATE 'SELECT MIN(salary) FROM HR.employees' INTO v_min_sal;

        EXECUTE IMMEDIATE 'SELECT last_name FROM HR.employees WHERE salary = :1 AND ROWNUM = 1'
            INTO v_emp_name
            USING v_max_sal;

        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM HR.employees WHERE salary BETWEEN :1 AND :2'
            INTO v_emp_count
            USING v_min_sal, v_max_sal;
    END PC_EXEC_IMM_INTO;

    PROCEDURE PC_EXEC_IMM_RETURNING(p_val IN VARCHAR2) IS
        v_id          NUMBER;
        v_created     DATE;
    BEGIN
        EXECUTE IMMEDIATE 'INSERT INTO audit_entries (description) VALUES (:1) RETURNING id INTO :2'
            USING p_val
            RETURNING INTO v_id;

        EXECUTE IMMEDIATE 'UPDATE audit_entries SET status = ''PROCESSED'' WHERE id = :1 RETURNING created_date INTO :2'
            USING v_id
            RETURNING INTO v_created;

        INSERT INTO audit_tracking (entry_id, original_date, processed_date)
        VALUES (v_id, v_created, SYSDATE);
    END PC_EXEC_IMM_RETURNING;

    PROCEDURE PC_EXEC_IMM_BULK IS
        TYPE t_id_tab IS TABLE OF NUMBER;
        TYPE t_name_tab IS TABLE OF VARCHAR2(100);
        v_ids   t_id_tab;
        v_names t_name_tab;
    BEGIN
        EXECUTE IMMEDIATE 'SELECT employee_id FROM employees WHERE department_id = 10'
            BULK COLLECT INTO v_ids;

        EXECUTE IMMEDIATE 'SELECT last_name FROM CUSTOMER.employees WHERE department_id = 20'
            BULK COLLECT INTO v_names;

        FOR i IN 1..v_ids.COUNT LOOP
            NULL;
        END LOOP;
    END PC_EXEC_IMM_BULK;

    PROCEDURE PC_EXEC_IMM_PLSQL_BLOCK(p_param IN VARCHAR2) IS
        v_block VARCHAR2(4000);
    BEGIN
        EXECUTE IMMEDIATE 'BEGIN pkg_util.do_work(:1); END;' USING p_param;

        v_block := 'BEGIN ' ||
                   '  pkg_logger.log_event(:1, :2); ' ||
                   '  pkg_notify.send_alert(:3); ' ||
                   'END;';

        EXECUTE IMMEDIATE v_block USING 'DYNAMIC_EXEC', p_param, p_param;

        EXECUTE IMMEDIATE 'BEGIN DBMS_STATS.GATHER_TABLE_STATS(:1, :2); END;'
            USING 'HR', 'EMPLOYEES';
    END PC_EXEC_IMM_PLSQL_BLOCK;

    PROCEDURE PC_DBMS_SQL_FULL IS
        v_cursor    INTEGER;
        v_sql       VARCHAR2(4000);
        v_rows      INTEGER;
        v_emp_id    NUMBER;
        v_emp_name  VARCHAR2(100);
        v_salary    NUMBER;
        v_fetch_cnt NUMBER := 0;
    BEGIN
        v_sql := 'SELECT employee_id, last_name, salary FROM CUSTOMER.employees WHERE department_id = :dept_id AND salary > :min_sal';

        v_cursor := DBMS_SQL.OPEN_CURSOR;
        DBMS_SQL.PARSE(v_cursor, v_sql, DBMS_SQL.NATIVE);
        DBMS_SQL.BIND_VARIABLE(v_cursor, ':dept_id', 10);
        DBMS_SQL.BIND_VARIABLE(v_cursor, ':min_sal', 5000);
        DBMS_SQL.DEFINE_COLUMN(v_cursor, 1, v_emp_id);
        DBMS_SQL.DEFINE_COLUMN(v_cursor, 2, v_emp_name, 100);
        DBMS_SQL.DEFINE_COLUMN(v_cursor, 3, v_salary);

        v_rows := DBMS_SQL.EXECUTE(v_cursor);

        LOOP
            EXIT WHEN DBMS_SQL.FETCH_ROWS(v_cursor) = 0;
            DBMS_SQL.COLUMN_VALUE(v_cursor, 1, v_emp_id);
            DBMS_SQL.COLUMN_VALUE(v_cursor, 2, v_emp_name);
            DBMS_SQL.COLUMN_VALUE(v_cursor, 3, v_salary);
            v_fetch_cnt := v_fetch_cnt + 1;
        END LOOP;

        DBMS_SQL.CLOSE_CURSOR(v_cursor);

        INSERT INTO query_stats (query_id, rows_fetched, run_date)
        VALUES (query_stat_seq.NEXTVAL, v_fetch_cnt, SYSDATE);
    EXCEPTION
        WHEN OTHERS THEN
            IF DBMS_SQL.IS_OPEN(v_cursor) THEN
                DBMS_SQL.CLOSE_CURSOR(v_cursor);
            END IF;
            RAISE;
    END PC_DBMS_SQL_FULL;

    PROCEDURE PC_DBMS_SQL_VARIABLE IS
        v_cursor    INTEGER;
        v_sql       VARCHAR2(4000);
        v_rows      INTEGER;
        v_affected  NUMBER;
    BEGIN
        v_sql := 'UPDATE employees SET salary = salary * 1.05 WHERE department_id = :dept AND hire_date < :cutoff';

        v_cursor := DBMS_SQL.OPEN_CURSOR;
        DBMS_SQL.PARSE(v_cursor, v_sql, DBMS_SQL.NATIVE);
        DBMS_SQL.BIND_VARIABLE(v_cursor, ':dept', 20);
        DBMS_SQL.BIND_VARIABLE(v_cursor, ':cutoff', ADD_MONTHS(SYSDATE, -24));
        v_rows := DBMS_SQL.EXECUTE(v_cursor);
        v_affected := v_rows;
        DBMS_SQL.CLOSE_CURSOR(v_cursor);

        INSERT INTO salary_adjustment_log (dept_id, rows_updated, adjustment_date)
        VALUES (20, v_affected, SYSDATE);
    EXCEPTION
        WHEN OTHERS THEN
            IF DBMS_SQL.IS_OPEN(v_cursor) THEN
                DBMS_SQL.CLOSE_CURSOR(v_cursor);
            END IF;
            RAISE;
    END PC_DBMS_SQL_VARIABLE;

    PROCEDURE PC_EXEC_IMM_TRUNCATE IS
        v_count_before NUMBER;
    BEGIN
        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM customer.staging_data'
            INTO v_count_before;

        EXECUTE IMMEDIATE 'TRUNCATE TABLE customer.staging_data';

        INSERT INTO truncate_log (table_name, rows_before, truncated_date)
        VALUES ('customer.staging_data', v_count_before, SYSDATE);

        EXECUTE IMMEDIATE 'TRUNCATE TABLE customer.staging_data_detail';
    END PC_EXEC_IMM_TRUNCATE;

END PKG_DYNAMIC_SQL_TEST;
/
