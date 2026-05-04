CREATE OR REPLACE PACKAGE BODY PKG_ADVANCED_QUERY_TEST AS

    -- 1. ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)
    PROCEDURE PC_ANALYTIC_ROW_NUMBER IS
        CURSOR c_ranked IS
            SELECT employee_id,
                   first_name,
                   dept_id,
                   salary,
                   ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS rn
              FROM hr.employees
             WHERE status = 'ACTIVE';
        v_emp c_ranked%ROWTYPE;
    BEGIN
        FOR v_emp IN c_ranked LOOP
            IF v_emp.rn = 1 THEN
                INSERT INTO dept_top_earners (dept_id, employee_id, salary, ranked_at)
                VALUES (v_emp.dept_id, v_emp.employee_id, v_emp.salary, SYSDATE);
            END IF;
        END LOOP;
        COMMIT;
    END PC_ANALYTIC_ROW_NUMBER;

    -- 2. RANK(), DENSE_RANK(), NTILE(4)
    PROCEDURE PC_ANALYTIC_RANK_DENSE IS
        v_count NUMBER;
    BEGIN
        INSERT INTO salary_rankings (employee_id, dept_id, salary, rnk, dense_rnk, quartile)
        SELECT employee_id,
               dept_id,
               salary,
               RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS rnk,
               DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS dense_rnk,
               NTILE(4) OVER (ORDER BY salary DESC) AS quartile
          FROM employees
         WHERE hire_date >= ADD_MONTHS(SYSDATE, -12);

        SELECT COUNT(*) INTO v_count FROM salary_rankings;
        DBMS_OUTPUT.PUT_LINE('Ranked rows: ' || v_count);
        COMMIT;
    END PC_ANALYTIC_RANK_DENSE;

    -- 3. LAG and LEAD
    PROCEDURE PC_ANALYTIC_LAG_LEAD IS
        CURSOR c_salary_diff IS
            SELECT employee_id,
                   salary,
                   LAG(salary, 1, 0) OVER (PARTITION BY dept_id ORDER BY hire_date) AS prev_salary,
                   LEAD(salary, 1) OVER (PARTITION BY dept_id ORDER BY hire_date) AS next_salary
              FROM employees
             WHERE dept_id IS NOT NULL;
    BEGIN
        FOR rec IN c_salary_diff LOOP
            UPDATE salary_analysis
               SET prev_salary  = rec.prev_salary,
                   next_salary  = rec.next_salary,
                   salary_delta = rec.salary - NVL(rec.prev_salary, rec.salary)
             WHERE employee_id = rec.employee_id;
        END LOOP;
        COMMIT;
    END PC_ANALYTIC_LAG_LEAD;

    -- 4. FIRST_VALUE with window frame
    PROCEDURE PC_ANALYTIC_FIRST_LAST IS
        v_name  VARCHAR2(100);
        v_sal   NUMBER;
    BEGIN
        SELECT first_name, salary
          INTO v_name, v_sal
          FROM (
            SELECT first_name,
                   salary,
                   FIRST_VALUE(first_name) OVER (
                       PARTITION BY dept_id
                       ORDER BY salary DESC
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   ) AS top_earner_name,
                   LAST_VALUE(first_name) OVER (
                       PARTITION BY dept_id
                       ORDER BY salary DESC
                       ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                   ) AS low_earner_name
              FROM employees
             WHERE dept_id = 10
          )
         WHERE ROWNUM = 1;

        INSERT INTO analysis_results (result_key, result_value, created_date)
        VALUES ('TOP_EARNER_DEPT10', v_name, SYSDATE);
        COMMIT;
    END PC_ANALYTIC_FIRST_LAST;

    -- 5. Windowed SUM, AVG, COUNT
    PROCEDURE PC_ANALYTIC_WINDOW IS
    BEGIN
        INSERT INTO txn_running_totals (txn_id, txn_date, amount, running_sum, running_avg, running_cnt)
        SELECT txn_id,
               txn_date,
               amount,
               SUM(amount) OVER (ORDER BY txn_date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS running_sum,
               AVG(amount) OVER (ORDER BY txn_date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS running_avg,
               COUNT(*) OVER (ORDER BY txn_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cnt
          FROM daily_transactions
         WHERE account_id = 1001;
        COMMIT;
    END PC_ANALYTIC_WINDOW;

    -- 6. PIVOT
    PROCEDURE PC_PIVOT IS
        v_mgr_total  NUMBER;
        v_dev_total  NUMBER;
        v_qa_total   NUMBER;
    BEGIN
        SELECT mgr_sal, dev_sal, qa_sal
          INTO v_mgr_total, v_dev_total, v_qa_total
          FROM (
            SELECT dept_id, job_id, salary
              FROM employees
             WHERE status = 'ACTIVE'
          )
          PIVOT (
            SUM(salary)
            FOR job_id IN ('MGR' AS mgr_sal, 'DEV' AS dev_sal, 'QA' AS qa_sal)
          )
         WHERE dept_id = 10;

        INSERT INTO pivot_summary (dept_id, mgr_total, dev_total, qa_total, report_date)
        VALUES (10, v_mgr_total, v_dev_total, v_qa_total, SYSDATE);
        COMMIT;
    END PC_PIVOT;

    -- 7. UNPIVOT
    PROCEDURE PC_UNPIVOT IS
    BEGIN
        INSERT INTO flat_sales (sales_id, quarter_name, amount)
        SELECT sales_id, quarter_name, amount
          FROM quarterly_sales
          UNPIVOT (
            amount FOR quarter_name IN (q1 AS 'Q1', q2 AS 'Q2', q3 AS 'Q3', q4 AS 'Q4')
          )
         WHERE fiscal_year = 2024;
        COMMIT;
    END PC_UNPIVOT;

    -- 8. XMLELEMENT, XMLFOREST, XMLAGG
    PROCEDURE PC_XMLELEMENT IS
        v_xml XMLTYPE;
    BEGIN
        SELECT XMLAGG(
                 XMLELEMENT("employee",
                   XMLFOREST(
                     first_name AS "name",
                     salary     AS "salary",
                     dept_id    AS "department"
                   )
                 )
               )
          INTO v_xml
          FROM employees
         WHERE dept_id = 20;

        INSERT INTO xml_reports (report_type, xml_content, generated_at)
        VALUES ('EMP_DEPT_20', v_xml, SYSTIMESTAMP);
        COMMIT;
    END PC_XMLELEMENT;

    -- 9. JSON_VALUE, JSON_OBJECT, JSON_ARRAYAGG
    PROCEDURE PC_JSON_FUNCTIONS IS
        v_json   CLOB;
        v_status VARCHAR2(50);
    BEGIN
        SELECT JSON_VALUE(payload, '$.order.status')
          INTO v_status
          FROM order_events
         WHERE event_id = 12345;

        SELECT JSON_ARRAYAGG(
                 JSON_OBJECT(
                   'id'     VALUE employee_id,
                   'name'   VALUE first_name || ' ' || last_name,
                   'salary' VALUE salary
                 )
               )
          INTO v_json
          FROM employees
         WHERE dept_id = 30;

        INSERT INTO json_cache (cache_key, json_data, updated_at)
        VALUES ('DEPT30_EMPLOYEES', v_json, SYSTIMESTAMP);
        COMMIT;
    END PC_JSON_FUNCTIONS;

    -- 10. LISTAGG
    PROCEDURE PC_LISTAGG IS
        v_names VARCHAR2(4000);
    BEGIN
        SELECT LISTAGG(first_name, ', ') WITHIN GROUP (ORDER BY first_name)
          INTO v_names
          FROM employees
         WHERE dept_id = 40;

        UPDATE department_summary
           SET employee_list = v_names,
               last_updated  = SYSDATE
         WHERE dept_id = 40;
        COMMIT;
    END PC_LISTAGG;

    -- 11. Flashback query
    PROCEDURE PC_FLASHBACK IS
        v_old_sal NUMBER;
    BEGIN
        SELECT salary
          INTO v_old_sal
          FROM employees AS OF TIMESTAMP (SYSTIMESTAMP - INTERVAL '1' HOUR)
         WHERE employee_id = 100;

        INSERT INTO salary_audit (employee_id, old_salary, current_salary, checked_at)
        SELECT 100,
               v_old_sal,
               e.salary,
               SYSTIMESTAMP
          FROM employees e
         WHERE e.employee_id = 100;
        COMMIT;
    END PC_FLASHBACK;

    -- 12. GROUP BY ROLLUP, CUBE, GROUPING SETS
    PROCEDURE PC_GROUP_BY_EXT IS
    BEGIN
        -- ROLLUP
        INSERT INTO rollup_report (dept_id, job_id, total_salary, report_type)
        SELECT dept_id, job_id, SUM(salary), 'ROLLUP'
          FROM employees
         GROUP BY ROLLUP(dept_id, job_id);

        -- CUBE
        INSERT INTO cube_report (region, product, total_revenue, report_type)
        SELECT region, product, SUM(revenue), 'CUBE'
          FROM regional_sales
         GROUP BY CUBE(region, product);

        -- GROUPING SETS
        INSERT INTO grouping_report (dept_id, job_id, total_salary, report_type)
        SELECT dept_id, job_id, SUM(salary), 'GSETS'
          FROM employees
         GROUP BY GROUPING SETS((dept_id), (job_id), ());

        COMMIT;
    END PC_GROUP_BY_EXT;

END PKG_ADVANCED_QUERY_TEST;
/
