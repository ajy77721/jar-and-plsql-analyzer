CREATE OR REPLACE PACKAGE BODY PKG_SUBQUERY_TEST AS

    PROCEDURE PC_SCALAR_IN_SELECT IS
        v_emp_name VARCHAR2(100);
        v_dept_avg NUMBER;
    BEGIN
        SELECT e.last_name,
               (SELECT AVG(salary) FROM employees WHERE department_id = e.department_id) AS dept_avg
          INTO v_emp_name, v_dept_avg
          FROM employees e
         WHERE e.employee_id = 100;
    END PC_SCALAR_IN_SELECT;

    PROCEDURE PC_SUBQUERY_IN_WHERE_IN IS
        v_cnt NUMBER;
    BEGIN
        SELECT COUNT(*)
          INTO v_cnt
          FROM employees
         WHERE department_id IN (SELECT department_id
                                   FROM departments
                                  WHERE location_id IN (SELECT location_id
                                                          FROM locations
                                                         WHERE country_id = 'US'));
    END PC_SUBQUERY_IN_WHERE_IN;

    PROCEDURE PC_SUBQUERY_EXISTS IS
        v_cnt NUMBER;
    BEGIN
        SELECT COUNT(*)
          INTO v_cnt
          FROM departments d
         WHERE EXISTS (SELECT 1
                         FROM employees e
                        WHERE e.department_id = d.department_id
                          AND e.salary > 10000);
    END PC_SUBQUERY_EXISTS;

    PROCEDURE PC_SUBQUERY_NOT_EXISTS IS
        v_cnt NUMBER;
    BEGIN
        SELECT COUNT(*)
          INTO v_cnt
          FROM CUSTOMER.departments d
         WHERE NOT EXISTS (SELECT 1
                             FROM CUSTOMER.employees e
                            WHERE e.department_id = d.department_id);
    END PC_SUBQUERY_NOT_EXISTS;

    PROCEDURE PC_SUBQUERY_ANY_ALL IS
        v_name VARCHAR2(100);
    BEGIN
        SELECT last_name
          INTO v_name
          FROM employees
         WHERE salary > ANY (SELECT salary FROM employees WHERE department_id = 10)
           AND salary < ALL (SELECT salary FROM employees WHERE department_id = 90)
           AND ROWNUM = 1;
    END PC_SUBQUERY_ANY_ALL;

    PROCEDURE PC_CORRELATED_SUBQUERY IS
        v_name   VARCHAR2(100);
        v_salary NUMBER;
    BEGIN
        SELECT e.last_name, e.salary
          INTO v_name, v_salary
          FROM employees e
         WHERE e.salary > (SELECT AVG(e2.salary)
                             FROM employees e2
                            WHERE e2.department_id = e.department_id)
           AND ROWNUM = 1;
    END PC_CORRELATED_SUBQUERY;

    PROCEDURE PC_INLINE_VIEW IS
        v_dept_name VARCHAR2(100);
        v_avg_sal   NUMBER;
        v_emp_count NUMBER;
    BEGIN
        SELECT sub.department_name, sub.avg_salary, sub.emp_count
          INTO v_dept_name, v_avg_sal, v_emp_count
          FROM (SELECT d.department_name,
                       AVG(e.salary) AS avg_salary,
                       COUNT(e.employee_id) AS emp_count
                  FROM HR.employees e
                  JOIN HR.departments d ON e.department_id = d.department_id
                 GROUP BY d.department_name
                HAVING COUNT(e.employee_id) > 5) sub
         WHERE ROWNUM = 1
         ORDER BY sub.avg_salary DESC;
    END PC_INLINE_VIEW;

    PROCEDURE PC_SUBQUERY_IN_HAVING IS
        v_dept  NUMBER;
        v_cnt   NUMBER;
    BEGIN
        SELECT department_id, COUNT(*)
          INTO v_dept, v_cnt
          FROM employees
         GROUP BY department_id
        HAVING COUNT(*) > (SELECT AVG(dept_count)
                             FROM (SELECT COUNT(*) AS dept_count
                                     FROM employees
                                    GROUP BY department_id))
         ORDER BY department_id;
    END PC_SUBQUERY_IN_HAVING;

    PROCEDURE PC_CTE_SINGLE IS
        v_name   VARCHAR2(100);
        v_salary NUMBER;
    BEGIN
        WITH high_earners AS (
            SELECT last_name, salary, department_id
              FROM employees
             WHERE salary > 15000
        )
        SELECT last_name, salary
          INTO v_name, v_salary
          FROM high_earners
         WHERE ROWNUM = 1
         ORDER BY salary DESC;
    END PC_CTE_SINGLE;

    PROCEDURE PC_CTE_MULTIPLE IS
        v_dept_name VARCHAR2(100);
        v_total_sal NUMBER;
        v_rank      NUMBER;
    BEGIN
        WITH dept_salaries AS (
            SELECT department_id, SUM(salary) AS total_salary
              FROM employees
             GROUP BY department_id
        ),
        dept_names AS (
            SELECT ds.department_id, d.department_name, ds.total_salary
              FROM dept_salaries ds
              JOIN departments d ON ds.department_id = d.department_id
        ),
        dept_ranked AS (
            SELECT department_name, total_salary,
                   RANK() OVER (ORDER BY total_salary DESC) AS salary_rank
              FROM dept_names
        )
        SELECT department_name, total_salary, salary_rank
          INTO v_dept_name, v_total_sal, v_rank
          FROM dept_ranked
         WHERE salary_rank = 1;
    END PC_CTE_MULTIPLE;

    PROCEDURE PC_CTE_RECURSIVE IS
        v_id    NUMBER;
        v_name  VARCHAR2(100);
        v_lvl   NUMBER;
    BEGIN
        WITH org_hierarchy (employee_id, last_name, manager_id, hierarchy_level) AS (
            SELECT employee_id, last_name, manager_id, 1 AS hierarchy_level
              FROM employees
             WHERE manager_id IS NULL
            UNION ALL
            SELECT e.employee_id, e.last_name, e.manager_id, oh.hierarchy_level + 1
              FROM employees e
              JOIN org_hierarchy oh ON e.manager_id = oh.employee_id
        )
        SELECT employee_id, last_name, hierarchy_level
          INTO v_id, v_name, v_lvl
          FROM org_hierarchy
         WHERE ROWNUM = 1
         ORDER BY hierarchy_level, last_name;
    END PC_CTE_RECURSIVE;

    PROCEDURE PC_NESTED_3_DEEP IS
        v_name VARCHAR2(100);
        v_sal  NUMBER;
    BEGIN
        SELECT outer_q.last_name, outer_q.salary
          INTO v_name, v_sal
          FROM (SELECT mid_q.last_name, mid_q.salary, mid_q.department_id
                  FROM (SELECT e.last_name, e.salary, e.department_id
                          FROM employees e
                         WHERE e.salary > (SELECT MIN(salary) FROM employees)
                       ) mid_q
                 WHERE mid_q.department_id IN (SELECT department_id
                                                 FROM departments
                                                WHERE location_id = 1700)
               ) outer_q
         WHERE ROWNUM = 1;
    END PC_NESTED_3_DEEP;

    PROCEDURE PC_ANTI_JOIN_NOT_IN IS
        v_cnt NUMBER;
    BEGIN
        SELECT COUNT(*)
          INTO v_cnt
          FROM CUSTOMER.employees
         WHERE employee_id NOT IN (SELECT manager_id
                                     FROM CUSTOMER.employees
                                    WHERE manager_id IS NOT NULL);
    END PC_ANTI_JOIN_NOT_IN;

    PROCEDURE PC_SEMI_JOIN_IN IS
        v_cnt NUMBER;
    BEGIN
        SELECT COUNT(*)
          INTO v_cnt
          FROM departments
         WHERE department_id IN (SELECT DISTINCT department_id
                                   FROM employees
                                  WHERE hire_date > ADD_MONTHS(SYSDATE, -12));
    END PC_SEMI_JOIN_IN;

END PKG_SUBQUERY_TEST;
/
