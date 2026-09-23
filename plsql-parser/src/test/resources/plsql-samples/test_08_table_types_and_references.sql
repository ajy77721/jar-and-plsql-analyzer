CREATE OR REPLACE PACKAGE BODY PKG_TABLE_TYPES_TEST AS

    -- 1. Schema-qualified table references in SELECT/INSERT/UPDATE/DELETE
    PROCEDURE PC_SCHEMA_QUALIFIED IS
        v_name  VARCHAR2(100);
        v_count NUMBER;
    BEGIN
        SELECT last_name
          INTO v_name
          FROM CUSTOMER.employees
         WHERE employee_id = 100;

        SELECT COUNT(*)
          INTO v_count
          FROM HR.departments
         WHERE location_id IS NOT NULL;

        INSERT INTO CUSTOMER.audit_log (log_id, action, table_name, created_by, created_at)
        VALUES (audit_seq.NEXTVAL, 'LOOKUP', 'EMPLOYEES', USER, SYSDATE);

        UPDATE HR.departments
           SET manager_id = 200
         WHERE department_id = 10;

        DELETE FROM CUSTOMER.temp_processing
         WHERE process_date < TRUNC(SYSDATE) - 30;

        COMMIT;
    END PC_SCHEMA_QUALIFIED;

    -- 2. Database link references
    PROCEDURE PC_DB_LINK IS
        v_remote_count NUMBER;
        v_cust_name    VARCHAR2(200);
    BEGIN
        SELECT COUNT(*)
          INTO v_remote_count
          FROM remote_table@PROD_DB_LINK
         WHERE status = 'ACTIVE';

        SELECT customer_name
          INTO v_cust_name
          FROM CUSTOMER.orders@REPORTING_LINK
         WHERE order_id = 5001;

        INSERT INTO local_staging (cust_name, record_count, synced_at)
        VALUES (v_cust_name, v_remote_count, SYSTIMESTAMP);

        INSERT INTO sync_history (source_link, row_count, sync_date)
        SELECT 'PROD_DB_LINK', COUNT(*), SYSDATE
          FROM inventory@PROD_DB_LINK
         WHERE warehouse_id = 'WH01';

        COMMIT;
    END PC_DB_LINK;

    -- 3. Short single-letter table aliases
    PROCEDURE PC_TABLE_ALIAS_SHORT IS
        v_total NUMBER;
    BEGIN
        SELECT SUM(e.salary)
          INTO v_total
          FROM employees e
          JOIN departments d ON e.dept_id = d.department_id
          JOIN locations l ON d.location_id = l.location_id
         WHERE l.country_id = 'US'
           AND d.department_name LIKE 'Sales%';

        INSERT INTO salary_totals (country, total_salary, calc_date)
        VALUES ('US', v_total, SYSDATE);
        COMMIT;
    END PC_TABLE_ALIAS_SHORT;

    -- 4. Table aliases (Oracle style without AS)
    PROCEDURE PC_TABLE_ALIAS_WITH_AS IS
        v_dept_count NUMBER;
    BEGIN
        SELECT COUNT(*)
          INTO v_dept_count
          FROM employees emp
          JOIN departments dept ON emp.dept_id = dept.department_id
          LEFT JOIN job_history jh ON emp.employee_id = jh.employee_id
         WHERE dept.department_name = 'Engineering';

        UPDATE report_counts
           SET emp_count    = v_dept_count,
               updated_at   = SYSDATE
         WHERE dept_name = 'Engineering';
        COMMIT;
    END PC_TABLE_ALIAS_WITH_AS;

    -- 5. %TYPE declarations
    PROCEDURE PC_PERCENT_TYPE IS
        v_name   employees.last_name%TYPE;
        v_sal    employees.salary%TYPE;
        v_dept   departments.department_name%TYPE;
        v_id     employees.employee_id%TYPE;
    BEGIN
        SELECT last_name, salary, employee_id
          INTO v_name, v_sal, v_id
          FROM employees
         WHERE employee_id = 101;

        SELECT department_name
          INTO v_dept
          FROM departments
         WHERE department_id = 20;

        INSERT INTO employee_snapshot (emp_id, emp_name, emp_salary, dept_name, snapshot_date)
        VALUES (v_id, v_name, v_sal, v_dept, SYSDATE);
        COMMIT;
    END PC_PERCENT_TYPE;

    -- 6. %ROWTYPE declarations
    PROCEDURE PC_PERCENT_ROWTYPE IS
        v_emp  employees%ROWTYPE;
        CURSOR c1 IS
            SELECT department_id, department_name, manager_id
              FROM departments
             WHERE department_id = 30;
        v_dept c1%ROWTYPE;
    BEGIN
        SELECT *
          INTO v_emp
          FROM employees
         WHERE employee_id = 102;

        OPEN c1;
        FETCH c1 INTO v_dept;
        CLOSE c1;

        INSERT INTO emp_dept_report (emp_id, emp_name, dept_id, dept_name, report_date)
        VALUES (v_emp.employee_id, v_emp.first_name, v_dept.department_id, v_dept.department_name, SYSDATE);
        COMMIT;
    END PC_PERCENT_ROWTYPE;

    -- 7. Partition query
    PROCEDURE PC_PARTITION_QUERY IS
        v_total NUMBER;
    BEGIN
        SELECT SUM(amount)
          INTO v_total
          FROM sales PARTITION (sales_q1_2024)
         WHERE product_id IS NOT NULL;

        INSERT INTO partition_summary (partition_name, total_amount, checked_at)
        VALUES ('sales_q1_2024', v_total, SYSTIMESTAMP);

        DELETE FROM sales PARTITION (sales_archive_2020)
         WHERE amount = 0;

        COMMIT;
    END PC_PARTITION_QUERY;

    -- 8. Global temporary table usage
    PROCEDURE PC_GLOBAL_TEMP_TABLE IS
        v_count NUMBER;
    BEGIN
        DELETE FROM gtt_processing_data;

        INSERT INTO gtt_processing_data (record_id, employee_id, process_flag, amount)
        SELECT ROWNUM, employee_id, 'N', salary
          FROM source_data
         WHERE hire_date >= TRUNC(SYSDATE, 'YEAR');

        UPDATE gtt_processing_data
           SET process_flag = 'Y',
               amount       = amount * 1.05
         WHERE amount > 50000;

        SELECT COUNT(*) INTO v_count
          FROM gtt_processing_data
         WHERE process_flag = 'Y';

        INSERT INTO processing_log (batch_type, record_count, processed_at)
        VALUES ('GTT_SALARY', v_count, SYSTIMESTAMP);

        COMMIT;
    END PC_GLOBAL_TEMP_TABLE;

    -- 9. TABLE() function with pipelined results
    PROCEDURE PC_TABLE_FUNCTION IS
        v_total NUMBER := 0;
    BEGIN
        FOR rec IN (
            SELECT col_id, col_value
              FROM TABLE(pkg_pipeline.get_data(10))
             WHERE col_value IS NOT NULL
        ) LOOP
            v_total := v_total + rec.col_value;
        END LOOP;

        INSERT INTO pipeline_results (dept_id, total_value, run_date)
        VALUES (10, v_total, SYSDATE);

        INSERT INTO pipeline_details (detail_id, dept_id, col_id, col_value)
        SELECT detail_seq.NEXTVAL, 10, t.col_id, t.col_value
          FROM TABLE(pkg_pipeline.get_data(10)) t;

        COMMIT;
    END PC_TABLE_FUNCTION;

    -- 10. DUAL selects with sequences and SYSDATE
    PROCEDURE PC_DUAL_SELECT IS
        v_id   NUMBER;
        v_dt   DATE;
        v_ts   TIMESTAMP;
        v_user VARCHAR2(30);
    BEGIN
        SELECT my_seq.NEXTVAL INTO v_id FROM DUAL;

        SELECT SYSDATE INTO v_dt FROM DUAL;

        SELECT SYSTIMESTAMP INTO v_ts FROM DUAL;

        SELECT USER INTO v_user FROM DUAL;

        INSERT INTO operation_log (log_id, operation, performed_by, performed_at, log_timestamp)
        VALUES (v_id, 'DUAL_TEST', v_user, v_dt, v_ts);

        COMMIT;
    END PC_DUAL_SELECT;

END PKG_TABLE_TYPES_TEST;
/
