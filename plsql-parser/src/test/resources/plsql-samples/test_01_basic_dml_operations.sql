CREATE OR REPLACE PACKAGE BODY PKG_DML_TEST AS

    -- Constants for status codes
    gc_active   CONSTANT VARCHAR2(10) := 'ACTIVE';
    gc_archived CONSTANT VARCHAR2(10) := 'ARCHIVED';

    PROCEDURE PC_SELECT_SIMPLE IS
        v_cnt       NUMBER;
        v_status    VARCHAR2(20);
        v_run_date  DATE := SYSDATE;
    BEGIN
        SELECT COUNT(*)
          INTO v_cnt
          FROM employees
         WHERE dept_id = 10;

        IF v_cnt > 0 THEN
            v_status := gc_active;
        ELSE
            v_status := 'EMPTY';
        END IF;

        INSERT INTO process_log (log_id, message, log_date)
        VALUES (log_seq.NEXTVAL, 'Selected ' || v_cnt || ' rows', v_run_date);
    END PC_SELECT_SIMPLE;

    PROCEDURE PC_SELECT_GROUP_BY IS
        v_dept      NUMBER;
        v_cnt       NUMBER;
        v_max_dept  NUMBER;
        v_total     NUMBER := 0;
    BEGIN
        SELECT dept_id, COUNT(*)
          INTO v_dept, v_cnt
          FROM CUSTOMER.employees
         GROUP BY dept_id
        HAVING COUNT(*) > 5
         ORDER BY dept_id;

        SELECT MAX(dept_id)
          INTO v_max_dept
          FROM CUSTOMER.employees;

        v_total := v_cnt + v_max_dept;
    END PC_SELECT_GROUP_BY;

    PROCEDURE PC_SELECT_DISTINCT IS
        v_name      VARCHAR2(100);
        v_dept_cnt  NUMBER;
    BEGIN
        SELECT DISTINCT department_name
          INTO v_name
          FROM departments
         WHERE ROWNUM = 1;

        SELECT COUNT(DISTINCT department_name)
          INTO v_dept_cnt
          FROM departments;
    END PC_SELECT_DISTINCT;

    PROCEDURE PC_SELECT_CONNECT_BY IS
        v_id        NUMBER;
        v_lvl       NUMBER;
        v_path      VARCHAR2(4000);
        v_max_depth NUMBER;
    BEGIN
        SELECT employee_id, LEVEL, SYS_CONNECT_BY_PATH(last_name, '/')
          INTO v_id, v_lvl, v_path
          FROM HR.employees
         START WITH manager_id IS NULL
       CONNECT BY PRIOR employee_id = manager_id;

        SELECT MAX(LEVEL)
          INTO v_max_depth
          FROM HR.employees
         START WITH manager_id IS NULL
       CONNECT BY PRIOR employee_id = manager_id;
    END PC_SELECT_CONNECT_BY;

    PROCEDURE PC_INSERT_SINGLE_ROW IS
        v_new_id    NUMBER;
        v_action    VARCHAR2(50) := 'TEST';
        v_timestamp DATE := SYSDATE;
    BEGIN
        INSERT INTO audit_log (id, action, created_date)
        VALUES (audit_seq.NEXTVAL, v_action, v_timestamp);

        INSERT INTO audit_log (id, action, created_date)
        VALUES (audit_seq.NEXTVAL, 'BATCH_START', SYSDATE);

        INSERT INTO CUSTOMER.notification_queue (id, message, priority, created_date)
        VALUES (notif_seq.NEXTVAL, 'Audit entry created', 1, SYSDATE);
    END PC_INSERT_SINGLE_ROW;

    PROCEDURE PC_INSERT_MULTI_ROW IS
        v_rows_before NUMBER;
        v_rows_after  NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_rows_before FROM archive_employees;

        INSERT INTO archive_employees
        SELECT *
          FROM employees
         WHERE hire_date < ADD_MONTHS(SYSDATE, -120);

        SELECT COUNT(*) INTO v_rows_after FROM archive_employees;

        INSERT INTO migration_log (operation, rows_moved, run_date)
        VALUES ('ARCHIVE', v_rows_after - v_rows_before, SYSDATE);
    END PC_INSERT_MULTI_ROW;

    PROCEDURE PC_INSERT_ALL_CONDITIONAL IS
        v_total_moved NUMBER := 0;
    BEGIN
        INSERT ALL
            WHEN dept_id = 10 THEN
                INTO dept10_employees (employee_id, first_name, last_name, dept_id)
                VALUES (employee_id, first_name, last_name, dept_id)
            WHEN dept_id = 20 THEN
                INTO dept20_employees (employee_id, first_name, last_name, dept_id)
                VALUES (employee_id, first_name, last_name, dept_id)
            WHEN dept_id = 30 THEN
                INTO dept30_employees (employee_id, first_name, last_name, dept_id)
                VALUES (employee_id, first_name, last_name, dept_id)
            ELSE
                INTO other_employees (employee_id, first_name, last_name, dept_id)
                VALUES (employee_id, first_name, last_name, dept_id)
        SELECT employee_id, first_name, last_name, dept_id
          FROM employees
         WHERE status = gc_active;

        v_total_moved := SQL%ROWCOUNT;

        INSERT INTO process_log (log_id, message, log_date)
        VALUES (log_seq.NEXTVAL, 'INSERT ALL moved ' || v_total_moved || ' rows', SYSDATE);
    END PC_INSERT_ALL_CONDITIONAL;

    PROCEDURE PC_INSERT_FIRST IS
        v_cnt NUMBER;
    BEGIN
        INSERT FIRST
            WHEN salary > 10000 THEN
                INTO high_salary_emp (employee_id, salary)
                VALUES (employee_id, salary)
            WHEN salary > 5000 THEN
                INTO mid_salary_emp (employee_id, salary)
                VALUES (employee_id, salary)
            ELSE
                INTO standard_emp (employee_id, salary)
                VALUES (employee_id, salary)
        SELECT employee_id, salary
          FROM CUSTOMER.employees
         WHERE hire_date >= ADD_MONTHS(SYSDATE, -12);

        v_cnt := SQL%ROWCOUNT;
    END PC_INSERT_FIRST;

    PROCEDURE PC_INSERT_RETURNING(
        p_cust_id IN NUMBER,
        p_amount  IN NUMBER
    ) IS
        v_order_id   NUMBER;
        v_order_date DATE;
    BEGIN
        INSERT INTO orders (id, customer_id, amount, order_date, status)
        VALUES (order_seq.NEXTVAL, p_cust_id, p_amount, SYSDATE, 'NEW')
        RETURNING id INTO v_order_id;

        INSERT INTO order_audit (audit_id, order_id, action, action_date)
        VALUES (order_audit_seq.NEXTVAL, v_order_id, 'CREATED', SYSDATE);

        UPDATE customer_stats
           SET last_order_id = v_order_id,
               total_orders = total_orders + 1
         WHERE customer_id = p_cust_id;
    END PC_INSERT_RETURNING;

    PROCEDURE PC_UPDATE_SIMPLE(p_dept IN NUMBER) IS
        v_affected NUMBER;
    BEGIN
        UPDATE employees
           SET salary = salary * 1.1,
               last_modified_date = SYSDATE,
               last_modified_by = USER
         WHERE department_id = p_dept
           AND status = gc_active;

        v_affected := SQL%ROWCOUNT;

        INSERT INTO salary_change_log (dept_id, employees_updated, change_pct, change_date)
        VALUES (p_dept, v_affected, 10, SYSDATE);
    END PC_UPDATE_SIMPLE;

    PROCEDURE PC_UPDATE_CORRELATED IS
        v_updated NUMBER;
    BEGIN
        UPDATE employees e
           SET salary = (SELECT AVG(salary)
                           FROM employees
                          WHERE department_id = e.department_id),
               commission_pct = (SELECT MAX(commission_pct)
                                   FROM employees
                                  WHERE department_id = e.department_id)
         WHERE performance_rating = 'A';

        v_updated := SQL%ROWCOUNT;
    END PC_UPDATE_CORRELATED;

    PROCEDURE PC_UPDATE_MULTI_COL IS
        v_pre_count  NUMBER;
        v_post_count NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_pre_count
          FROM HR.employees
         WHERE hire_date < SYSDATE - 365;

        UPDATE HR.employees
           SET (salary, commission_pct) = (SELECT avg_sal, avg_comm
                                             FROM dept_averages
                                            WHERE dept_id = employees.department_id)
         WHERE hire_date < SYSDATE - 365;

        v_post_count := SQL%ROWCOUNT;
    END PC_UPDATE_MULTI_COL;

    PROCEDURE PC_DELETE_SIMPLE IS
        v_deleted NUMBER;
    BEGIN
        DELETE FROM temp_data
         WHERE created_date < SYSDATE - 30;

        v_deleted := SQL%ROWCOUNT;

        DELETE FROM temp_data_detail
         WHERE parent_id NOT IN (SELECT id FROM temp_data);

        INSERT INTO cleanup_log (table_name, rows_deleted, cleanup_date)
        VALUES ('TEMP_DATA', v_deleted, SYSDATE);
    END PC_DELETE_SIMPLE;

    PROCEDURE PC_DELETE_SUBQUERY IS
        v_cust_count NUMBER;
    BEGIN
        SELECT COUNT(*)
          INTO v_cust_count
          FROM inactive_customers
         WHERE last_activity < ADD_MONTHS(SYSDATE, -24);

        DELETE FROM order_details
         WHERE order_id IN (SELECT order_id
                              FROM orders
                             WHERE customer_id IN (SELECT customer_id
                                                     FROM inactive_customers
                                                    WHERE last_activity < ADD_MONTHS(SYSDATE, -24)));

        DELETE FROM orders
         WHERE customer_id IN (SELECT customer_id
                                 FROM inactive_customers
                                WHERE last_activity < ADD_MONTHS(SYSDATE, -24));
    END PC_DELETE_SUBQUERY;

    PROCEDURE PC_MERGE_COMPLEX IS
        v_merged NUMBER;
    BEGIN
        MERGE INTO target_table t
        USING (SELECT id, name, amount, category
                 FROM source_table
                WHERE status = gc_active
                  AND amount > 0) s
           ON (t.id = s.id)
         WHEN MATCHED THEN
              UPDATE SET t.name = s.name,
                         t.amount = s.amount,
                         t.category = s.category,
                         t.last_updated = SYSDATE
              DELETE WHERE t.amount = 0
         WHEN NOT MATCHED THEN
              INSERT (id, name, amount, category, created_date)
              VALUES (s.id, s.name, s.amount, s.category, SYSDATE);

        v_merged := SQL%ROWCOUNT;

        INSERT INTO merge_log (source_table, target_table, rows_merged, merge_date)
        VALUES ('SOURCE_TABLE', 'TARGET_TABLE', v_merged, SYSDATE);
    END PC_MERGE_COMPLEX;

END PKG_DML_TEST;
/
