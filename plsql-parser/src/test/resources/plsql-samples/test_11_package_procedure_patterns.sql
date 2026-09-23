CREATE OR REPLACE PACKAGE BODY PKG_CALL_GRAPH_TEST AS

    -- Package-level types for pipelined function
    TYPE t_row IS RECORD (
        col_id    NUMBER,
        col_value VARCHAR2(200)
    );
    TYPE t_tab IS TABLE OF t_row;

    -- 1. Simple internal helper function
    FUNCTION FN_HELPER (p_val NUMBER) RETURN NUMBER IS
        v_result NUMBER;
    BEGIN
        SELECT NVL(MAX(multiplier), 1)
          INTO v_result
          FROM config_params
         WHERE param_type = 'MULTIPLIER'
           AND effective_date <= SYSDATE;

        RETURN v_result * p_val;
    END FN_HELPER;

    -- 2. Internal call to FN_HELPER
    PROCEDURE PC_INTERNAL_CALL (p_emp_id NUMBER, p_base_salary NUMBER) IS
        v_adjusted NUMBER;
    BEGIN
        v_adjusted := FN_HELPER(p_base_salary);

        UPDATE employees
           SET salary     = v_adjusted,
               updated_at = SYSDATE
         WHERE employee_id = p_emp_id;

        INSERT INTO salary_adjustments (adj_id, employee_id, old_salary, new_salary, adjusted_at)
        VALUES (adj_seq.NEXTVAL, p_emp_id, p_base_salary, v_adjusted, SYSTIMESTAMP);

        COMMIT;
    END PC_INTERNAL_CALL;

    -- 3. External package calls
    PROCEDURE PC_EXTERNAL_CALL (p_id NUMBER, p_data VARCHAR2) IS
        v_result  VARCHAR2(200);
        v_valid   BOOLEAN;
    BEGIN
        OTHER_PKG.DO_WORK(p_id);

        v_valid := SCHEMA_OWNER.OTHER_PKG.VALIDATE(p_data);

        IF v_valid THEN
            EXT_PROCESSING.SUBMIT_JOB(p_id, p_data);

            INSERT INTO external_call_log (call_id, target_pkg, target_proc, called_at)
            VALUES (call_log_seq.NEXTVAL, 'EXT_PROCESSING', 'SUBMIT_JOB', SYSTIMESTAMP);
        END IF;

        COMMIT;
    END PC_EXTERNAL_CALL;

    -- 4. Built-in package calls
    PROCEDURE PC_BUILTIN_CALL (p_dir VARCHAR2, p_file VARCHAR2) IS
        v_handle  UTL_FILE.FILE_TYPE;
        v_clob    CLOB;
        v_length  NUMBER;
    BEGIN
        DBMS_OUTPUT.PUT_LINE('Starting file operation for: ' || p_file);

        v_handle := UTL_FILE.FOPEN(p_dir, p_file, 'R');
        UTL_FILE.GET_LINE(v_handle, v_clob);
        UTL_FILE.FCLOSE(v_handle);

        v_length := DBMS_LOB.GETLENGTH(v_clob);

        DBMS_OUTPUT.PUT_LINE('File length: ' || v_length);

        INSERT INTO file_operations (op_id, directory_name, file_name, file_size, read_at)
        VALUES (file_op_seq.NEXTVAL, p_dir, p_file, v_length, SYSTIMESTAMP);

        COMMIT;
    END PC_BUILTIN_CALL;

    -- 5. Three-part name call (schema.package.procedure)
    PROCEDURE PC_THREE_PART_CALL IS
        v_formatted VARCHAR2(100);
        v_result    NUMBER;
    BEGIN
        v_formatted := CUSTOMER.PKG_UTIL.FORMAT_DATE(SYSDATE);

        v_result := FINANCE.PKG_CALC.COMPUTE_TAX(50000, 'US');

        CUSTOMER.PKG_AUDIT.LOG_ACCESS('PKG_CALL_GRAPH_TEST', 'PC_THREE_PART_CALL', USER);

        INSERT INTO three_part_call_log (call_desc, result_val, formatted_date, logged_at)
        VALUES ('TAX_CALC', v_result, v_formatted, SYSTIMESTAMP);

        COMMIT;
    END PC_THREE_PART_CALL;

    -- 6. Pipelined function
    FUNCTION FN_PIPELINED (p_dept_id NUMBER) RETURN t_tab PIPELINED IS
        v_row t_row;
    BEGIN
        FOR rec IN (
            SELECT employee_id, first_name || ' ' || last_name AS full_name
              FROM employees
             WHERE dept_id = p_dept_id
             ORDER BY last_name
        ) LOOP
            v_row.col_id    := rec.employee_id;
            v_row.col_value := rec.full_name;
            PIPE ROW(v_row);
        END LOOP;

        RETURN;
    END FN_PIPELINED;

    -- 7. Autonomous transaction
    PROCEDURE PC_AUTONOMOUS (p_action VARCHAR2, p_detail VARCHAR2) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        INSERT INTO log_table (log_id, log_action, log_detail, log_user, log_timestamp)
        VALUES (log_seq.NEXTVAL, p_action, p_detail, USER, SYSTIMESTAMP);

        INSERT INTO audit_trail (audit_id, action_type, action_detail, performed_by, performed_at)
        VALUES (audit_seq.NEXTVAL, p_action, p_detail, USER, SYSTIMESTAMP);

        COMMIT;
    END PC_AUTONOMOUS;

    -- 8. Nested (locally-defined) procedure
    PROCEDURE PC_NESTED_PROC (p_dept_id NUMBER) IS
        v_total NUMBER := 0;

        PROCEDURE local_process_employee (p_emp_id NUMBER) IS
            v_sal NUMBER;
        BEGIN
            SELECT salary INTO v_sal
              FROM employees
             WHERE employee_id = p_emp_id;

            UPDATE employee_bonuses
               SET bonus_amount = v_sal * 0.10,
                   calculated_at = SYSDATE
             WHERE employee_id = p_emp_id;

            v_total := v_total + 1;
        END local_process_employee;

    BEGIN
        FOR rec IN (
            SELECT employee_id
              FROM employees
             WHERE dept_id = p_dept_id
        ) LOOP
            local_process_employee(rec.employee_id);
        END LOOP;

        INSERT INTO bonus_run_log (dept_id, employees_processed, run_date)
        VALUES (p_dept_id, v_total, SYSDATE);

        COMMIT;
    END PC_NESTED_PROC;

    -- 9. Overloaded procedure (NUMBER parameter)
    PROCEDURE PC_OVERLOADED_A (p_id NUMBER) IS
        v_name VARCHAR2(100);
    BEGIN
        SELECT first_name || ' ' || last_name
          INTO v_name
          FROM employees
         WHERE employee_id = p_id;

        INSERT INTO lookup_log (lookup_type, lookup_key, lookup_result, looked_up_at)
        VALUES ('BY_ID', TO_CHAR(p_id), v_name, SYSTIMESTAMP);

        COMMIT;
    END PC_OVERLOADED_A;

    -- 10. Overloaded procedure (VARCHAR2 parameter)
    PROCEDURE PC_OVERLOADED_A (p_name VARCHAR2) IS
        v_id NUMBER;
    BEGIN
        SELECT employee_id
          INTO v_id
          FROM employees
         WHERE last_name = p_name
           AND ROWNUM = 1;

        INSERT INTO lookup_log (lookup_type, lookup_key, lookup_result, looked_up_at)
        VALUES ('BY_NAME', p_name, TO_CHAR(v_id), SYSTIMESTAMP);

        COMMIT;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            INSERT INTO lookup_log (lookup_type, lookup_key, lookup_result, looked_up_at)
            VALUES ('BY_NAME', p_name, 'NOT_FOUND', SYSTIMESTAMP);
            COMMIT;
    END PC_OVERLOADED_A;

    -- 11. Default parameters
    PROCEDURE PC_DEFAULT_PARAMS (
        p_dept_id  NUMBER,
        p_status   VARCHAR2 DEFAULT 'ACTIVE',
        p_limit    NUMBER   DEFAULT 100,
        p_order_by VARCHAR2 DEFAULT 'LAST_NAME'
    ) IS
        v_count NUMBER;
    BEGIN
        SELECT COUNT(*)
          INTO v_count
          FROM employees
         WHERE dept_id = p_dept_id
           AND status  = p_status;

        INSERT INTO param_usage_log (proc_name, dept_id, status_param, limit_param, result_count, logged_at)
        VALUES ('PC_DEFAULT_PARAMS', p_dept_id, p_status, p_limit, v_count, SYSTIMESTAMP);

        IF v_count > p_limit THEN
            UPDATE dept_alerts
               SET alert_message = 'Count ' || v_count || ' exceeds limit ' || p_limit,
                   alerted_at    = SYSTIMESTAMP
             WHERE department_id = p_dept_id;
        END IF;

        COMMIT;
    END PC_DEFAULT_PARAMS;

    -- 12a. Function designed to be called within SQL
    FUNCTION FN_IN_SQL (p_value NUMBER) RETURN VARCHAR2 IS
        v_label VARCHAR2(50);
    BEGIN
        SELECT category_label
          INTO v_label
          FROM value_categories
         WHERE p_value BETWEEN min_value AND max_value
           AND ROWNUM = 1;

        RETURN v_label;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN 'UNKNOWN';
    END FN_IN_SQL;

    -- 12b. Procedure that calls FN_IN_SQL inside a SQL statement
    PROCEDURE PC_USE_FN_IN_SQL IS
    BEGIN
        INSERT INTO categorized_employees (employee_id, employee_name, salary, salary_category, categorized_at)
        SELECT employee_id,
               first_name || ' ' || last_name,
               salary,
               FN_IN_SQL(salary),
               SYSDATE
          FROM employees
         WHERE dept_id = 50;

        COMMIT;
    END PC_USE_FN_IN_SQL;

END PKG_CALL_GRAPH_TEST;
/
