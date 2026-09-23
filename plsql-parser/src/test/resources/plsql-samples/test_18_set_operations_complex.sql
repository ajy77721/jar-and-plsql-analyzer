CREATE OR REPLACE PACKAGE BODY DATA_OPS.PKG_SET_OPS_TEST AS
  ---------------------------------------------------------------------------
  -- Set Operations & Complex Query Patterns Package
  -- Covers: UNION ALL, MINUS, INTERSECT, deep CTEs, nested subqueries,
  --         REGEXP, ROWNUM pagination, FETCH FIRST, MULTISET, CROSS APPLY
  ---------------------------------------------------------------------------

  gc_module CONSTANT VARCHAR2(30) := 'PKG_SET_OPS_TEST';

  -- -----------------------------------------------------------------------
  -- PC_UNION_ALL
  -- Three-query UNION ALL from different tables
  -- -----------------------------------------------------------------------
  PROCEDURE PC_UNION_ALL (
    p_start_date IN DATE,
    p_end_date   IN DATE,
    p_result_cur OUT SYS_REFCURSOR
  )
  IS
  BEGIN
    OPEN p_result_cur FOR
      SELECT 'ONLINE' AS source_type,
             t.transaction_id AS record_id,
             t.customer_id,
             t.transaction_amount AS amount,
             t.transaction_date AS event_date,
             t.status
        FROM SALES.online_transactions t
       WHERE t.transaction_date BETWEEN p_start_date AND p_end_date
      UNION ALL
      SELECT 'BRANCH' AS source_type,
             b.branch_txn_id AS record_id,
             b.customer_id,
             b.amount,
             b.txn_date AS event_date,
             b.txn_status AS status
        FROM SALES.branch_transactions b
        JOIN MASTER.branches br
          ON b.branch_id = br.branch_id
       WHERE b.txn_date BETWEEN p_start_date AND p_end_date
         AND br.active_flag = 'Y'
      UNION ALL
      SELECT 'PARTNER' AS source_type,
             p.partner_txn_id AS record_id,
             p.customer_id,
             p.txn_amount AS amount,
             p.txn_date AS event_date,
             p.status
        FROM PARTNER.partner_transactions p
        JOIN PARTNER.partners pt
          ON p.partner_id = pt.partner_id
       WHERE p.txn_date BETWEEN p_start_date AND p_end_date
         AND pt.status = 'ACTIVE'
       ORDER BY event_date DESC, record_id;
  END PC_UNION_ALL;

  -- -----------------------------------------------------------------------
  -- PC_MINUS_INTERSECT
  -- Combined MINUS and INTERSECT set operations
  -- -----------------------------------------------------------------------
  PROCEDURE PC_MINUS_INTERSECT (
    p_period_id  IN NUMBER,
    p_result_cur OUT SYS_REFCURSOR
  )
  IS
  BEGIN
    OPEN p_result_cur FOR
      (
        -- Customers with online activity MINUS those with complaints
        SELECT c.customer_id, c.customer_name, c.email
          FROM MASTER.customers c
          JOIN SALES.online_transactions t
            ON c.customer_id = t.customer_id
         WHERE t.period_id = p_period_id
        MINUS
        SELECT c2.customer_id, c2.customer_name, c2.email
          FROM MASTER.customers c2
          JOIN SERVICE.complaints cmp
            ON c2.customer_id = cmp.customer_id
         WHERE cmp.period_id = p_period_id
      )
      INTERSECT
      (
        -- Customers who are in the loyalty program
        SELECT c3.customer_id, c3.customer_name, c3.email
          FROM MASTER.customers c3
          JOIN LOYALTY.members lm
            ON c3.customer_id = lm.customer_id
         WHERE lm.status = 'ACTIVE'
      )
       ORDER BY customer_name;
  END PC_MINUS_INTERSECT;

  -- -----------------------------------------------------------------------
  -- PC_CTE_5_DEEP
  -- Five chained CTEs, each referencing the previous one
  -- -----------------------------------------------------------------------
  PROCEDURE PC_CTE_5_DEEP (
    p_department_id IN NUMBER,
    p_result_cur    OUT SYS_REFCURSOR
  )
  IS
  BEGIN
    OPEN p_result_cur FOR
      WITH cte1_base AS (
        SELECT e.employee_id,
               e.first_name || ' ' || e.last_name AS full_name,
               e.department_id,
               e.salary,
               e.hire_date,
               d.department_name
          FROM HR.employees e
          JOIN HR.departments d
            ON e.department_id = d.department_id
         WHERE e.department_id = p_department_id
           AND e.status = 'ACTIVE'
      ),
      cte2_stats AS (
        SELECT c1.employee_id,
               c1.full_name,
               c1.department_name,
               c1.salary,
               c1.hire_date,
               AVG(c1.salary) OVER () AS dept_avg_salary,
               COUNT(*) OVER () AS dept_headcount
          FROM cte1_base c1
      ),
      cte3_ranked AS (
        SELECT c2.employee_id,
               c2.full_name,
               c2.department_name,
               c2.salary,
               c2.hire_date,
               c2.dept_avg_salary,
               c2.dept_headcount,
               RANK() OVER (ORDER BY c2.salary DESC) AS salary_rank,
               c2.salary - c2.dept_avg_salary AS variance_from_avg
          FROM cte2_stats c2
      ),
      cte4_bands AS (
        SELECT c3.*,
               CASE
                 WHEN c3.salary_rank <= CEIL(c3.dept_headcount * 0.25) THEN 'TOP_25'
                 WHEN c3.salary_rank <= CEIL(c3.dept_headcount * 0.50) THEN 'MID_HIGH'
                 WHEN c3.salary_rank <= CEIL(c3.dept_headcount * 0.75) THEN 'MID_LOW'
                 ELSE 'BOTTOM_25'
               END AS salary_band
          FROM cte3_ranked c3
      ),
      cte5_final AS (
        SELECT c4.employee_id,
               c4.full_name,
               c4.department_name,
               c4.salary,
               c4.hire_date,
               c4.salary_rank,
               c4.salary_band,
               c4.variance_from_avg,
               ROUND(MONTHS_BETWEEN(SYSDATE, c4.hire_date) / 12, 1) AS years_of_service,
               LEAD(c4.full_name) OVER (ORDER BY c4.salary DESC) AS next_lower_employee,
               LEAD(c4.salary) OVER (ORDER BY c4.salary DESC) AS next_lower_salary
          FROM cte4_bands c4
      )
      SELECT c5.employee_id,
             c5.full_name,
             c5.department_name,
             c5.salary,
             c5.salary_rank,
             c5.salary_band,
             c5.variance_from_avg,
             c5.years_of_service,
             c5.next_lower_employee,
             c5.salary - NVL(c5.next_lower_salary, 0) AS gap_to_next
        FROM cte5_final c5
       ORDER BY c5.salary_rank;
  END PC_CTE_5_DEEP;

  -- -----------------------------------------------------------------------
  -- PC_NESTED_4_LEVELS
  -- Four levels of nested subqueries
  -- -----------------------------------------------------------------------
  PROCEDURE PC_NESTED_4_LEVELS (
    p_region_code IN VARCHAR2,
    p_result_cur  OUT SYS_REFCURSOR
  )
  IS
  BEGIN
    OPEN p_result_cur FOR
      SELECT outer_q.customer_id,
             outer_q.customer_name,
             outer_q.total_orders,
             outer_q.latest_amount,
             outer_q.region_rank
        FROM (
          SELECT level3.customer_id,
                 level3.customer_name,
                 level3.total_orders,
                 level3.latest_amount,
                 DENSE_RANK() OVER (ORDER BY level3.total_orders DESC) AS region_rank
            FROM (
              SELECT level2.customer_id,
                     level2.customer_name,
                     level2.total_orders,
                     (SELECT MAX(o2.order_amount)
                        FROM SALES.orders o2
                       WHERE o2.customer_id = level2.customer_id
                         AND o2.order_date = (
                               SELECT MAX(o3.order_date)
                                 FROM SALES.orders o3
                                WHERE o3.customer_id = level2.customer_id
                             )
                     ) AS latest_amount
                FROM (
                  SELECT c.customer_id,
                         c.customer_name,
                         COUNT(o.order_id) AS total_orders
                    FROM MASTER.customers c
                    LEFT JOIN SALES.orders o
                      ON c.customer_id = o.customer_id
                   WHERE c.region_code = p_region_code
                     AND c.status = 'ACTIVE'
                   GROUP BY c.customer_id, c.customer_name
                   HAVING COUNT(o.order_id) > 0
                ) level2
            ) level3
           WHERE level3.total_orders >= 5
        ) outer_q
       WHERE outer_q.region_rank <= 20
       ORDER BY outer_q.region_rank;
  END PC_NESTED_4_LEVELS;

  -- -----------------------------------------------------------------------
  -- PC_REGEX_FUNCTIONS
  -- REGEXP_LIKE, REGEXP_SUBSTR, REGEXP_REPLACE, REGEXP_INSTR
  -- -----------------------------------------------------------------------
  PROCEDURE PC_REGEX_FUNCTIONS (
    p_pattern    IN VARCHAR2,
    p_result_cur OUT SYS_REFCURSOR
  )
  IS
  BEGIN
    OPEN p_result_cur FOR
      SELECT c.customer_id,
             c.customer_name,
             c.email,
             c.phone_number,
             -- Extract domain from email
             REGEXP_SUBSTR(c.email, '@(.+)$', 1, 1, NULL, 1) AS email_domain,
             -- Clean phone number (remove non-digits)
             REGEXP_REPLACE(c.phone_number, '[^0-9]', '') AS clean_phone,
             -- Find position of first digit in name (data quality check)
             REGEXP_INSTR(c.customer_name, '[0-9]') AS first_digit_pos,
             -- Extract first word of address
             REGEXP_SUBSTR(c.address_line1, '^\S+') AS address_prefix
        FROM MASTER.customers c
       WHERE REGEXP_LIKE(c.customer_name, p_pattern, 'i')
         AND REGEXP_LIKE(c.email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
         AND c.status = 'ACTIVE'
       ORDER BY c.customer_name;
  END PC_REGEX_FUNCTIONS;

  -- -----------------------------------------------------------------------
  -- PC_ROWNUM_PAGINATION
  -- Classic Oracle ROWNUM-based pagination
  -- -----------------------------------------------------------------------
  PROCEDURE PC_ROWNUM_PAGINATION (
    p_category   IN VARCHAR2,
    p_page_start IN NUMBER DEFAULT 1,
    p_page_end   IN NUMBER DEFAULT 20,
    p_result_cur OUT SYS_REFCURSOR
  )
  IS
  BEGIN
    OPEN p_result_cur FOR
      SELECT *
        FROM (
          SELECT a.product_id,
                 a.product_name,
                 a.category_code,
                 a.unit_price,
                 a.stock_quantity,
                 ROWNUM AS rn
            FROM (
              SELECT p.product_id,
                     p.product_name,
                     p.category_code,
                     p.unit_price,
                     p.stock_quantity
                FROM INVENTORY.products p
                JOIN MASTER.categories cat
                  ON p.category_code = cat.category_code
               WHERE p.category_code = p_category
                 AND p.status = 'ACTIVE'
               ORDER BY p.unit_price DESC, p.product_name
            ) a
           WHERE ROWNUM <= p_page_end
        )
       WHERE rn > p_page_start - 1;
  END PC_ROWNUM_PAGINATION;

  -- -----------------------------------------------------------------------
  -- PC_FETCH_FIRST
  -- Modern Oracle 12c+ OFFSET/FETCH pagination
  -- -----------------------------------------------------------------------
  PROCEDURE PC_FETCH_FIRST (
    p_department_id IN NUMBER,
    p_offset        IN NUMBER DEFAULT 0,
    p_fetch_count   IN NUMBER DEFAULT 20,
    p_result_cur    OUT SYS_REFCURSOR
  )
  IS
  BEGIN
    OPEN p_result_cur FOR
      SELECT e.employee_id,
             e.first_name,
             e.last_name,
             e.salary,
             e.hire_date,
             d.department_name,
             m.first_name || ' ' || m.last_name AS manager_name
        FROM HR.employees e
        JOIN HR.departments d
          ON e.department_id = d.department_id
        LEFT JOIN HR.employees m
          ON e.manager_id = m.employee_id
       WHERE e.department_id = p_department_id
         AND e.status = 'ACTIVE'
       ORDER BY e.salary DESC, e.last_name, e.first_name
      OFFSET p_offset ROWS FETCH FIRST p_fetch_count ROWS ONLY;
  END PC_FETCH_FIRST;

  -- -----------------------------------------------------------------------
  -- PC_MULTISET_OPS
  -- MULTISET UNION on nested table collections, TABLE() operator
  -- -----------------------------------------------------------------------
  PROCEDURE PC_MULTISET_OPS (
    p_dept_id_1  IN NUMBER,
    p_dept_id_2  IN NUMBER,
    p_result_cur OUT SYS_REFCURSOR
  )
  IS
    TYPE t_id_tab IS TABLE OF NUMBER;
    v_dept1_employees t_id_tab;
    v_dept2_employees t_id_tab;
    v_combined        t_id_tab;
  BEGIN
    -- Collect employees from department 1
    SELECT e.employee_id
      BULK COLLECT INTO v_dept1_employees
      FROM HR.employees e
     WHERE e.department_id = p_dept_id_1
       AND e.status = 'ACTIVE';

    -- Collect employees from department 2
    SELECT e.employee_id
      BULK COLLECT INTO v_dept2_employees
      FROM HR.employees e
     WHERE e.department_id = p_dept_id_2
       AND e.status = 'ACTIVE';

    -- Combine using MULTISET UNION
    v_combined := v_dept1_employees MULTISET UNION v_dept2_employees;

    -- Query from the combined collection via TABLE()
    OPEN p_result_cur FOR
      SELECT e.employee_id,
             e.first_name || ' ' || e.last_name AS full_name,
             e.department_id,
             d.department_name,
             e.salary
        FROM HR.employees e
        JOIN HR.departments d
          ON e.department_id = d.department_id
       WHERE e.employee_id IN (
               SELECT COLUMN_VALUE FROM TABLE(v_combined)
             )
       ORDER BY e.department_id, e.last_name;
  END PC_MULTISET_OPS;

  -- -----------------------------------------------------------------------
  -- PC_CROSS_APPLY
  -- CROSS APPLY (lateral join) pattern
  -- -----------------------------------------------------------------------
  PROCEDURE PC_CROSS_APPLY (
    p_min_orders IN NUMBER DEFAULT 1,
    p_result_cur OUT SYS_REFCURSOR
  )
  IS
  BEGIN
    OPEN p_result_cur FOR
      SELECT e.employee_id,
             e.first_name || ' ' || e.last_name AS employee_name,
             e.department_id,
             recent_orders.order_id,
             recent_orders.order_date,
             recent_orders.order_amount,
             recent_orders.order_rank
        FROM HR.employees e
       CROSS APPLY (
          SELECT o.order_id,
                 o.order_date,
                 o.order_amount,
                 ROW_NUMBER() OVER (ORDER BY o.order_date DESC) AS order_rank
            FROM SALES.orders o
           WHERE o.sales_rep_id = e.employee_id
             AND o.status = 'COMPLETED'
             AND o.order_date >= ADD_MONTHS(SYSDATE, -12)
        ) recent_orders
       WHERE recent_orders.order_rank <= 3
         AND e.department_id IN (
               SELECT d.department_id
                 FROM HR.departments d
                WHERE d.department_name LIKE 'Sales%'
             )
       ORDER BY e.employee_id, recent_orders.order_rank;
  END PC_CROSS_APPLY;

END PKG_SET_OPS_TEST;
/
