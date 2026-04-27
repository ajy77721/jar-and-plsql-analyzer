CREATE OR REPLACE PACKAGE BODY PKG_JOIN_TEST AS

    PROCEDURE PC_INNER_JOIN_EXPLICIT IS
        v_emp_name  VARCHAR2(100);
        v_dept_name VARCHAR2(100);
        v_salary    NUMBER;
        v_hire_date DATE;
    BEGIN
        SELECT e.last_name, d.department_name, e.salary, e.hire_date
          INTO v_emp_name, v_dept_name, v_salary, v_hire_date
          FROM employees e
         INNER JOIN departments d ON e.department_id = d.department_id
         WHERE e.employee_id = 100;

        IF v_salary > 10000 THEN
            INSERT INTO high_earners_report (emp_name, dept_name, salary, report_date)
            VALUES (v_emp_name, v_dept_name, v_salary, SYSDATE);
        END IF;
    END PC_INNER_JOIN_EXPLICIT;

    PROCEDURE PC_INNER_JOIN_IMPLICIT IS
        v_emp_name  VARCHAR2(100);
        v_dept_name VARCHAR2(100);
        v_loc_city  VARCHAR2(100);
        v_country   VARCHAR2(100);
    BEGIN
        SELECT e.last_name, d.department_name, l.city, c.country_name
          INTO v_emp_name, v_dept_name, v_loc_city, v_country
          FROM employees e, departments d, locations l, countries c
         WHERE e.department_id = d.department_id
           AND d.location_id = l.location_id
           AND l.country_id = c.country_id
           AND e.salary > 5000
           AND ROWNUM = 1;
    END PC_INNER_JOIN_IMPLICIT;

    PROCEDURE PC_LEFT_OUTER_JOIN IS
        v_emp_name  VARCHAR2(100);
        v_dept_name VARCHAR2(100);
        v_job_title VARCHAR2(100);
    BEGIN
        SELECT e.last_name, d.department_name, j.job_title
          INTO v_emp_name, v_dept_name, v_job_title
          FROM HR.employees e
          LEFT OUTER JOIN HR.departments d ON e.department_id = d.department_id
          LEFT OUTER JOIN HR.jobs j ON e.job_id = j.job_id
         WHERE ROWNUM = 1;

        IF v_dept_name IS NULL THEN
            INSERT INTO orphan_employees (emp_name, detected_date)
            VALUES (v_emp_name, SYSDATE);
        END IF;
    END PC_LEFT_OUTER_JOIN;

    PROCEDURE PC_RIGHT_OUTER_JOIN IS
        v_dept_name VARCHAR2(100);
        v_emp_count NUMBER;
        v_min_sal   NUMBER;
        v_max_sal   NUMBER;
    BEGIN
        SELECT d.department_name, COUNT(e.employee_id),
               MIN(e.salary), MAX(e.salary)
          INTO v_dept_name, v_emp_count, v_min_sal, v_max_sal
          FROM employees e
         RIGHT OUTER JOIN departments d ON e.department_id = d.department_id
         GROUP BY d.department_name
        HAVING COUNT(e.employee_id) = 0
         ORDER BY d.department_name;

        INSERT INTO empty_dept_report (dept_name, checked_date)
        VALUES (v_dept_name, SYSDATE);
    END PC_RIGHT_OUTER_JOIN;

    PROCEDURE PC_FULL_OUTER_JOIN IS
        v_emp_name  VARCHAR2(100);
        v_proj_name VARCHAR2(100);
        v_hours     NUMBER;
    BEGIN
        SELECT e.last_name, p.project_name, pa.hours_allocated
          INTO v_emp_name, v_proj_name, v_hours
          FROM employees e
          FULL OUTER JOIN project_assignments pa ON e.employee_id = pa.employee_id
          FULL OUTER JOIN projects p ON pa.project_id = p.project_id
         WHERE ROWNUM = 1;

        IF v_emp_name IS NULL THEN
            NULL;
        ELSIF v_proj_name IS NULL THEN
            INSERT INTO unassigned_employees (emp_name, check_date)
            VALUES (v_emp_name, SYSDATE);
        END IF;
    END PC_FULL_OUTER_JOIN;

    PROCEDURE PC_CROSS_JOIN IS
        v_combo_cnt  NUMBER;
        v_prod_cnt   NUMBER;
        v_region_cnt NUMBER;
    BEGIN
        SELECT COUNT(*)
          INTO v_combo_cnt
          FROM CUSTOMER.products p
         CROSS JOIN CUSTOMER.regions r;

        SELECT COUNT(*) INTO v_prod_cnt FROM CUSTOMER.products;
        SELECT COUNT(*) INTO v_region_cnt FROM CUSTOMER.regions;
    END PC_CROSS_JOIN;

    PROCEDURE PC_NATURAL_JOIN IS
        v_emp_name  VARCHAR2(100);
        v_dept_name VARCHAR2(100);
        v_salary    NUMBER;
    BEGIN
        SELECT last_name, department_name, salary
          INTO v_emp_name, v_dept_name, v_salary
          FROM employees
       NATURAL JOIN departments
         WHERE ROWNUM = 1;
    END PC_NATURAL_JOIN;

    PROCEDURE PC_SELF_JOIN IS
        v_emp_name  VARCHAR2(100);
        v_mgr_name  VARCHAR2(100);
        v_emp_sal   NUMBER;
        v_mgr_sal   NUMBER;
        v_dept_id   NUMBER;
    BEGIN
        SELECT e1.last_name, e2.last_name, e1.salary, e2.salary, e1.department_id
          INTO v_emp_name, v_mgr_name, v_emp_sal, v_mgr_sal, v_dept_id
          FROM employees e1
          JOIN employees e2 ON e1.manager_id = e2.employee_id
         WHERE e1.employee_id = 101;

        IF v_emp_sal > v_mgr_sal THEN
            INSERT INTO salary_alerts (emp_name, mgr_name, emp_salary, mgr_salary, dept_id, alert_date)
            VALUES (v_emp_name, v_mgr_name, v_emp_sal, v_mgr_sal, v_dept_id, SYSDATE);
        END IF;
    END PC_SELF_JOIN;

    PROCEDURE PC_MULTI_TABLE_5WAY IS
        v_emp_name  VARCHAR2(100);
        v_dept_name VARCHAR2(100);
        v_loc_city  VARCHAR2(100);
        v_country   VARCHAR2(100);
        v_region    VARCHAR2(100);
    BEGIN
        SELECT e.last_name, d.department_name, l.city, c.country_name, r.region_name
          INTO v_emp_name, v_dept_name, v_loc_city, v_country, v_region
          FROM employees e
         INNER JOIN departments d ON e.department_id = d.department_id
          LEFT OUTER JOIN locations l ON d.location_id = l.location_id
         RIGHT OUTER JOIN countries c ON l.country_id = c.country_id
          FULL OUTER JOIN regions r ON c.region_id = r.region_id
         WHERE ROWNUM = 1;

        INSERT INTO geo_report (emp_name, department, city, country, region, report_date)
        VALUES (v_emp_name, v_dept_name, v_loc_city, v_country, v_region, SYSDATE);
    END PC_MULTI_TABLE_5WAY;

    PROCEDURE PC_JOIN_USING_CLAUSE IS
        v_emp_name  VARCHAR2(100);
        v_dept_name VARCHAR2(100);
        v_loc_city  VARCHAR2(100);
    BEGIN
        SELECT e.last_name, d.department_name, l.city
          INTO v_emp_name, v_dept_name, v_loc_city
          FROM employees e
          JOIN departments d USING (department_id)
          JOIN locations l USING (location_id)
         WHERE ROWNUM = 1;
    END PC_JOIN_USING_CLAUSE;

    PROCEDURE PC_OLD_STYLE_OUTER IS
        v_emp_name  VARCHAR2(100);
        v_dept_name VARCHAR2(100);
        v_loc_city  VARCHAR2(100);
        v_mgr_name  VARCHAR2(100);
    BEGIN
        SELECT e.last_name, d.department_name, l.city, m.last_name
          INTO v_emp_name, v_dept_name, v_loc_city, v_mgr_name
          FROM employees e, departments d, locations l, employees m
         WHERE e.department_id = d.department_id(+)
           AND d.location_id = l.location_id(+)
           AND e.manager_id = m.employee_id(+)
           AND ROWNUM = 1;

        IF v_dept_name IS NULL THEN
            INSERT INTO data_quality_issues (issue_type, description, detected_date)
            VALUES ('ORPHAN_EMP', 'Employee ' || v_emp_name || ' has no department', SYSDATE);
        END IF;
    END PC_OLD_STYLE_OUTER;

    PROCEDURE PC_MIXED_JOINS IS
        v_emp_name    VARCHAR2(100);
        v_dept_name   VARCHAR2(100);
        v_proj_name   VARCHAR2(100);
        v_mgr_name    VARCHAR2(100);
        v_job_title   VARCHAR2(100);
    BEGIN
        SELECT e.last_name, d.department_name, p.project_name,
               m.last_name, j.job_title
          INTO v_emp_name, v_dept_name, v_proj_name,
               v_mgr_name, v_job_title
          FROM employees e
          JOIN departments d ON e.department_id = d.department_id
          LEFT OUTER JOIN jobs j ON e.job_id = j.job_id,
               projects p, employees m
         WHERE e.employee_id = p.lead_id
           AND e.manager_id = m.employee_id(+)
           AND ROWNUM = 1;

        INSERT INTO comprehensive_report (
            emp_name, dept_name, project_name,
            manager_name, job_title, report_date
        ) VALUES (
            v_emp_name, v_dept_name, v_proj_name,
            v_mgr_name, v_job_title, SYSDATE
        );
    END PC_MIXED_JOINS;

END PKG_JOIN_TEST;
/
