-- =============================================================================
-- 1. Simple view on a single table with filter
-- =============================================================================
CREATE OR REPLACE VIEW v_active_employees AS
SELECT employee_id,
       first_name,
       last_name,
       email,
       dept_id,
       salary,
       hire_date
  FROM employees
 WHERE status = 'ACTIVE'
   AND termination_date IS NULL;
/

-- =============================================================================
-- 2. View with multi-table JOIN
-- =============================================================================
CREATE OR REPLACE VIEW v_employee_details AS
SELECT e.employee_id,
       e.first_name || ' ' || e.last_name AS full_name,
       e.email,
       e.salary,
       e.hire_date,
       d.department_name,
       d.department_id,
       l.city,
       l.state_province,
       l.country_id,
       m.first_name || ' ' || m.last_name AS manager_name
  FROM employees e
  JOIN departments d ON e.dept_id = d.department_id
  JOIN locations l ON d.location_id = l.location_id
  LEFT JOIN employees m ON e.manager_id = m.employee_id
 WHERE e.status = 'ACTIVE';
/

-- =============================================================================
-- 3. View with subquery and CTE (WITH clause)
-- =============================================================================
CREATE OR REPLACE VIEW v_dept_salary_summary AS
WITH dept_stats AS (
    SELECT dept_id,
           COUNT(*) AS emp_count,
           SUM(salary) AS total_salary,
           AVG(salary) AS avg_salary,
           MIN(salary) AS min_salary,
           MAX(salary) AS max_salary
      FROM employees
     WHERE status = 'ACTIVE'
     GROUP BY dept_id
),
company_avg AS (
    SELECT AVG(salary) AS overall_avg
      FROM employees
     WHERE status = 'ACTIVE'
)
SELECT d.department_id,
       d.department_name,
       ds.emp_count,
       ds.total_salary,
       ROUND(ds.avg_salary, 2) AS avg_salary,
       ds.min_salary,
       ds.max_salary,
       ROUND(ca.overall_avg, 2) AS company_avg_salary,
       ROUND(ds.avg_salary - ca.overall_avg, 2) AS variance_from_avg
  FROM departments d
  JOIN dept_stats ds ON d.department_id = ds.dept_id
  CROSS JOIN company_avg ca
 WHERE ds.emp_count > 0;
/

-- =============================================================================
-- 4. Materialized view with JOIN and GROUP BY
-- =============================================================================
CREATE MATERIALIZED VIEW mv_monthly_sales_summary
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
SELECT s.product_id,
       p.product_name,
       p.category_id,
       c.category_name,
       TRUNC(s.sale_date, 'MM') AS sale_month,
       COUNT(*) AS transaction_count,
       SUM(s.quantity) AS total_quantity,
       SUM(s.amount) AS total_amount,
       AVG(s.amount) AS avg_amount,
       MIN(s.sale_date) AS first_sale_date,
       MAX(s.sale_date) AS last_sale_date
  FROM sales s
  JOIN products p ON s.product_id = p.product_id
  JOIN categories c ON p.category_id = c.category_id
 WHERE s.sale_status = 'COMPLETED'
 GROUP BY s.product_id,
          p.product_name,
          p.category_id,
          c.category_name,
          TRUNC(s.sale_date, 'MM');
/

-- =============================================================================
-- 5. Object TYPE BODY with MEMBER FUNCTION and MEMBER PROCEDURE doing DML
-- =============================================================================
CREATE OR REPLACE TYPE BODY typ_employee AS

    MEMBER FUNCTION get_annual_salary RETURN NUMBER IS
        v_bonus NUMBER;
    BEGIN
        SELECT NVL(bonus_pct, 0)
          INTO v_bonus
          FROM bonus_rates
         WHERE job_grade = SELF.job_grade
           AND effective_date <= SYSDATE
           AND ROWNUM = 1;

        RETURN SELF.monthly_salary * 12 * (1 + v_bonus / 100);
    END get_annual_salary;

    MEMBER FUNCTION get_department_name RETURN VARCHAR2 IS
        v_dept_name VARCHAR2(100);
    BEGIN
        SELECT department_name
          INTO v_dept_name
          FROM departments
         WHERE department_id = SELF.dept_id;

        RETURN v_dept_name;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN 'UNKNOWN';
    END get_department_name;

    MEMBER PROCEDURE update_salary (p_new_salary NUMBER) IS
    BEGIN
        INSERT INTO salary_change_log (
            log_id, employee_id, old_salary, new_salary, changed_at
        ) VALUES (
            sal_log_seq.NEXTVAL, SELF.employee_id,
            SELF.monthly_salary, p_new_salary, SYSTIMESTAMP
        );

        UPDATE employees
           SET salary     = p_new_salary,
               updated_at = SYSDATE
         WHERE employee_id = SELF.employee_id;

        SELF.monthly_salary := p_new_salary;
    END update_salary;

    MEMBER PROCEDURE transfer_department (p_new_dept_id NUMBER) IS
        v_old_dept NUMBER;
    BEGIN
        v_old_dept := SELF.dept_id;

        UPDATE employees
           SET dept_id    = p_new_dept_id,
               updated_at = SYSDATE
         WHERE employee_id = SELF.employee_id;

        INSERT INTO transfer_history (
            transfer_id, employee_id, from_dept, to_dept, transfer_date
        ) VALUES (
            transfer_seq.NEXTVAL, SELF.employee_id,
            v_old_dept, p_new_dept_id, SYSDATE
        );

        UPDATE departments
           SET employee_count = employee_count - 1
         WHERE department_id = v_old_dept;

        UPDATE departments
           SET employee_count = employee_count + 1
         WHERE department_id = p_new_dept_id;

        SELF.dept_id := p_new_dept_id;
    END transfer_department;

END;
/

-- =============================================================================
-- 6. Object TYPE BODY with STATIC FUNCTION and MAP MEMBER FUNCTION
-- =============================================================================
CREATE OR REPLACE TYPE BODY typ_currency_amount AS

    MAP MEMBER FUNCTION get_sort_value RETURN NUMBER IS
        v_rate NUMBER;
    BEGIN
        IF SELF.currency_code = 'USD' THEN
            RETURN SELF.amount;
        END IF;

        SELECT exchange_rate
          INTO v_rate
          FROM exchange_rates
         WHERE from_currency = SELF.currency_code
           AND to_currency   = 'USD'
           AND rate_date     = TRUNC(SYSDATE);

        RETURN SELF.amount * v_rate;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            SELECT exchange_rate
              INTO v_rate
              FROM exchange_rates
             WHERE from_currency = SELF.currency_code
               AND to_currency   = 'USD'
               AND rate_date = (
                   SELECT MAX(rate_date)
                     FROM exchange_rates
                    WHERE from_currency = SELF.currency_code
                      AND to_currency   = 'USD'
               );
            RETURN SELF.amount * v_rate;
    END get_sort_value;

    STATIC FUNCTION create_from_usd (
        p_usd_amount   NUMBER,
        p_target_code  VARCHAR2
    ) RETURN typ_currency_amount IS
        v_rate   NUMBER;
        v_result typ_currency_amount;
    BEGIN
        IF p_target_code = 'USD' THEN
            v_result := typ_currency_amount(p_usd_amount, 'USD');
            RETURN v_result;
        END IF;

        SELECT exchange_rate
          INTO v_rate
          FROM exchange_rates
         WHERE from_currency = 'USD'
           AND to_currency   = p_target_code
           AND rate_date     = TRUNC(SYSDATE);

        v_result := typ_currency_amount(ROUND(p_usd_amount * v_rate, 2), p_target_code);

        INSERT INTO currency_conversion_log (
            log_id, from_amount, from_currency, to_amount, to_currency, rate_used, converted_at
        ) VALUES (
            conv_log_seq.NEXTVAL, p_usd_amount, 'USD',
            v_result.amount, p_target_code, v_rate, SYSTIMESTAMP
        );

        RETURN v_result;
    END create_from_usd;

    MEMBER FUNCTION convert_to (p_target_code VARCHAR2) RETURN typ_currency_amount IS
        v_usd_amount NUMBER;
    BEGIN
        v_usd_amount := SELF.get_sort_value;
        RETURN typ_currency_amount.create_from_usd(v_usd_amount, p_target_code);
    END convert_to;

END;
/
