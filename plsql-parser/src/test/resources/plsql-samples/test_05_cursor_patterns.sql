CREATE OR REPLACE PACKAGE BODY PKG_CURSOR_TEST AS

    PROCEDURE PC_EXPLICIT_CURSOR IS
        CURSOR c_employees IS
            SELECT employee_id, last_name, salary, department_id
              FROM employees
             WHERE department_id = 10
             ORDER BY last_name;

        v_emp_id     employees.employee_id%TYPE;
        v_name       employees.last_name%TYPE;
        v_salary     employees.salary%TYPE;
        v_dept_id    employees.department_id%TYPE;
        v_total_sal  NUMBER := 0;
        v_emp_count  NUMBER := 0;
    BEGIN
        OPEN c_employees;
        LOOP
            FETCH c_employees INTO v_emp_id, v_name, v_salary, v_dept_id;
            EXIT WHEN c_employees%NOTFOUND;
            v_total_sal := v_total_sal + v_salary;
            v_emp_count := v_emp_count + 1;
        END LOOP;
        CLOSE c_employees;

        INSERT INTO dept_salary_summary (dept_id, total_salary, emp_count, summary_date)
        VALUES (10, v_total_sal, v_emp_count, SYSDATE);
    END PC_EXPLICIT_CURSOR;

    PROCEDURE PC_IMPLICIT_CURSOR IS
        v_name       VARCHAR2(100);
        v_salary     NUMBER;
        v_dept_name  VARCHAR2(100);
        v_hire_date  DATE;
    BEGIN
        SELECT e.last_name, e.salary, d.department_name, e.hire_date
          INTO v_name, v_salary, v_dept_name, v_hire_date
          FROM HR.employees e
          JOIN HR.departments d ON e.department_id = d.department_id
         WHERE e.employee_id = 100;

        IF v_salary > 10000 THEN
            INSERT INTO high_earner_log (emp_name, salary, dept_name, log_date)
            VALUES (v_name, v_salary, v_dept_name, SYSDATE);
        END IF;
    END PC_IMPLICIT_CURSOR;

    PROCEDURE PC_CURSOR_FOR_LOOP IS
        v_total    NUMBER := 0;
        v_count    NUMBER := 0;
        v_avg_sal  NUMBER;
    BEGIN
        FOR rec IN (SELECT e.employee_id, e.last_name,
                           d.department_name, e.salary,
                           j.job_title
                      FROM employees e
                      JOIN departments d ON e.department_id = d.department_id
                      JOIN jobs j ON e.job_id = j.job_id
                     WHERE e.salary > 5000
                     ORDER BY e.salary DESC)
        LOOP
            v_total := v_total + rec.salary;
            v_count := v_count + 1;

            INSERT INTO salary_report_detail (emp_id, emp_name, dept_name, salary, report_date)
            VALUES (rec.employee_id, rec.last_name, rec.department_name, rec.salary, SYSDATE);
        END LOOP;

        IF v_count > 0 THEN
            v_avg_sal := v_total / v_count;
        END IF;
    END PC_CURSOR_FOR_LOOP;

    PROCEDURE PC_CURSOR_FOR_LOOP_NAMED IS
        CURSOR c_dept_employees IS
            SELECT e.employee_id, e.last_name, e.salary,
                   e.commission_pct, e.hire_date
              FROM CUSTOMER.employees e
             WHERE e.department_id = 20
               AND e.status = 'ACTIVE'
             ORDER BY e.last_name;

        v_count      NUMBER := 0;
        v_total_comp NUMBER := 0;
    BEGIN
        FOR rec IN c_dept_employees
        LOOP
            v_count := v_count + 1;
            v_total_comp := v_total_comp + rec.salary + NVL(rec.commission_pct, 0);

            IF rec.hire_date < ADD_MONTHS(SYSDATE, -120) THEN
                INSERT INTO tenure_awards (emp_id, emp_name, years_service, award_date)
                VALUES (rec.employee_id, rec.last_name,
                        TRUNC(MONTHS_BETWEEN(SYSDATE, rec.hire_date) / 12), SYSDATE);
            END IF;
        END LOOP;
    END PC_CURSOR_FOR_LOOP_NAMED;

    PROCEDURE PC_PARAMETERIZED_CURSOR(
        p_dept_id IN NUMBER,
        p_status  IN VARCHAR2
    ) IS
        CURSOR c_filtered(cp_dept NUMBER, cp_status VARCHAR2) IS
            SELECT e.employee_id, e.last_name, e.salary, jh.start_date, jh.end_date
              FROM employees e
              JOIN job_history jh ON e.employee_id = jh.employee_id
             WHERE e.department_id = cp_dept
               AND jh.status = cp_status
             ORDER BY jh.start_date DESC;

        v_emp_id     NUMBER;
        v_name       VARCHAR2(100);
        v_salary     NUMBER;
        v_start_dt   DATE;
        v_end_dt     DATE;
        v_rec_count  NUMBER := 0;
    BEGIN
        OPEN c_filtered(p_dept_id, p_status);
        LOOP
            FETCH c_filtered INTO v_emp_id, v_name, v_salary, v_start_dt, v_end_dt;
            EXIT WHEN c_filtered%NOTFOUND;

            v_rec_count := v_rec_count + 1;

            IF v_end_dt IS NULL THEN
                UPDATE job_history
                   SET status = 'CURRENT'
                 WHERE employee_id = v_emp_id
                   AND start_date = v_start_dt;
            END IF;
        END LOOP;
        CLOSE c_filtered;

        INSERT INTO cursor_run_log (cursor_name, params, rows_processed, run_date)
        VALUES ('c_filtered', p_dept_id || '/' || p_status, v_rec_count, SYSDATE);
    END PC_PARAMETERIZED_CURSOR;

    PROCEDURE PC_REF_CURSOR_STATIC(p_cur OUT SYS_REFCURSOR) IS
        v_min_salary NUMBER := 8000;
    BEGIN
        OPEN p_cur FOR
            SELECT e.employee_id, e.last_name, e.salary,
                   d.department_name, l.city
              FROM employees e
              LEFT OUTER JOIN departments d ON e.department_id = d.department_id
              LEFT OUTER JOIN locations l ON d.location_id = l.location_id
             WHERE e.salary > v_min_salary
             ORDER BY e.last_name;
    END PC_REF_CURSOR_STATIC;

    PROCEDURE PC_REF_CURSOR_DYNAMIC(
        p_cur    OUT SYS_REFCURSOR,
        p_dept   IN  NUMBER,
        p_filter IN  VARCHAR2,
        p_min_sal IN NUMBER DEFAULT 0
    ) IS
        v_sql     VARCHAR2(4000);
        v_where   VARCHAR2(1000);
    BEGIN
        v_sql := 'SELECT employee_id, last_name, salary, hire_date FROM employees WHERE department_id = :1 AND salary >= :2';

        IF p_filter IS NOT NULL THEN
            v_sql := v_sql || ' AND last_name LIKE :3';
            OPEN p_cur FOR v_sql USING p_dept, p_min_sal, p_filter;
        ELSE
            OPEN p_cur FOR v_sql USING p_dept, p_min_sal;
        END IF;

        INSERT INTO ref_cursor_log (sql_text, dept_param, opened_date)
        VALUES (SUBSTR(v_sql, 1, 500), p_dept, SYSDATE);
    END PC_REF_CURSOR_DYNAMIC;

    PROCEDURE PC_BULK_COLLECT_FETCH IS
        CURSOR c_all_emps IS
            SELECT employee_id, last_name, salary, department_id
              FROM employees
             ORDER BY employee_id;

        TYPE t_emp_id_tab IS TABLE OF employees.employee_id%TYPE;
        TYPE t_name_tab IS TABLE OF employees.last_name%TYPE;
        TYPE t_sal_tab IS TABLE OF employees.salary%TYPE;
        TYPE t_dept_tab IS TABLE OF employees.department_id%TYPE;

        v_ids     t_emp_id_tab;
        v_names   t_name_tab;
        v_sals    t_sal_tab;
        v_depts   t_dept_tab;
        v_batch   NUMBER := 0;
    BEGIN
        OPEN c_all_emps;
        LOOP
            FETCH c_all_emps BULK COLLECT INTO v_ids, v_names, v_sals, v_depts LIMIT 1000;
            EXIT WHEN v_ids.COUNT = 0;

            v_batch := v_batch + 1;

            FORALL i IN 1..v_ids.COUNT
                INSERT INTO emp_snapshot (emp_id, emp_name, salary, dept_id, batch_num, snapshot_date)
                VALUES (v_ids(i), v_names(i), v_sals(i), v_depts(i), v_batch, SYSDATE);
        END LOOP;
        CLOSE c_all_emps;
    END PC_BULK_COLLECT_FETCH;

    PROCEDURE PC_BULK_COLLECT_NO_LIMIT IS
        CURSOR c_small_result IS
            SELECT employee_id, last_name
              FROM employees
             WHERE department_id = 10;

        TYPE t_id_tab IS TABLE OF employees.employee_id%TYPE;
        TYPE t_name_tab IS TABLE OF employees.last_name%TYPE;
        v_ids   t_id_tab;
        v_names t_name_tab;
    BEGIN
        OPEN c_small_result;
        FETCH c_small_result BULK COLLECT INTO v_ids, v_names;
        CLOSE c_small_result;

        FOR i IN 1..v_ids.COUNT LOOP
            INSERT INTO processing_queue (emp_id, emp_name, queued_date)
            VALUES (v_ids(i), v_names(i), SYSDATE);
        END LOOP;
    END PC_BULK_COLLECT_NO_LIMIT;

    PROCEDURE PC_CURSOR_ATTRIBUTES IS
        CURSOR c_check IS
            SELECT employee_id, last_name, salary
              FROM employees
             WHERE department_id = 30;

        v_id       NUMBER;
        v_name     VARCHAR2(100);
        v_salary   NUMBER;
        v_found    BOOLEAN;
        v_cnt      NUMBER;
        v_upd_cnt  NUMBER;
    BEGIN
        OPEN c_check;
        FETCH c_check INTO v_id, v_name, v_salary;

        IF c_check%FOUND THEN
            v_found := TRUE;
        END IF;

        IF c_check%NOTFOUND THEN
            v_found := FALSE;
        END IF;

        v_cnt := c_check%ROWCOUNT;

        CLOSE c_check;

        UPDATE employees
           SET last_review_date = SYSDATE
         WHERE employee_id = -1;

        v_upd_cnt := SQL%ROWCOUNT;

        IF SQL%ROWCOUNT = 0 THEN
            INSERT INTO no_match_log (operation, table_name, log_date)
            VALUES ('UPDATE', 'EMPLOYEES', SYSDATE);
        END IF;

        DELETE FROM temp_flags WHERE flag_id = -999;

        IF SQL%ROWCOUNT > 0 THEN
            NULL;
        END IF;
    END PC_CURSOR_ATTRIBUTES;

    PROCEDURE PC_FETCH_INTO_RECORD IS
        CURSOR c_emp_rec IS
            SELECT employee_id, last_name, salary, department_id, hire_date
              FROM employees
             WHERE ROWNUM <= 10
             ORDER BY employee_id;

        v_rec       c_emp_rec%ROWTYPE;
        v_processed NUMBER := 0;
    BEGIN
        OPEN c_emp_rec;
        LOOP
            FETCH c_emp_rec INTO v_rec;
            EXIT WHEN c_emp_rec%NOTFOUND;

            v_processed := v_processed + 1;

            INSERT INTO emp_audit (emp_id, emp_name, salary, dept_id, audit_date)
            VALUES (v_rec.employee_id, v_rec.last_name, v_rec.salary,
                    v_rec.department_id, SYSDATE);
        END LOOP;
        CLOSE c_emp_rec;

        INSERT INTO process_summary (process_name, records_processed, run_date)
        VALUES ('FETCH_INTO_RECORD', v_processed, SYSDATE);
    END PC_FETCH_INTO_RECORD;

END PKG_CURSOR_TEST;
/
