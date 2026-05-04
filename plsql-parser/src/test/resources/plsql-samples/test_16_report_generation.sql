CREATE OR REPLACE PACKAGE BODY REPORTING.PKG_REPORT_GEN AS
  ---------------------------------------------------------------------------
  -- Report Generation Package
  -- Covers: SYS_REFCURSOR, CTEs, analytics, PIVOT, XML, CSV, hierarchical,
  --         dynamic SQL, UTL_FILE, DBMS_LOB, LISTAGG, CONNECT BY
  ---------------------------------------------------------------------------

  gc_module      CONSTANT VARCHAR2(30) := 'PKG_REPORT_GEN';
  gc_csv_dir     CONSTANT VARCHAR2(60) := 'REPORT_OUTPUT_DIR';
  gc_page_size   CONSTANT PLS_INTEGER  := 25;

  -- -----------------------------------------------------------------------
  -- FN_GET_SUMMARY_REPORT
  -- Returns SYS_REFCURSOR with CTE-based summary using analytics
  -- -----------------------------------------------------------------------
  FUNCTION FN_GET_SUMMARY_REPORT (
    p_start_date  IN DATE,
    p_end_date    IN DATE,
    p_region      IN VARCHAR2 DEFAULT NULL
  ) RETURN SYS_REFCURSOR
  IS
    v_cur SYS_REFCURSOR;
  BEGIN
    OPEN v_cur FOR
      WITH summary_cte AS (
        SELECT t.region_code,
               t.category_code,
               t.transaction_date,
               t.transaction_amount,
               c.category_name,
               r.region_name,
               COUNT(*) OVER (PARTITION BY t.region_code) AS region_total_count,
               SUM(t.transaction_amount) OVER (PARTITION BY t.region_code) AS region_total_amt
          FROM FINANCE.transactions t
          JOIN MASTER.categories c
            ON t.category_code = c.category_code
          JOIN MASTER.regions r
            ON t.region_code = r.region_code
         WHERE t.transaction_date BETWEEN p_start_date AND p_end_date
           AND (p_region IS NULL OR t.region_code = p_region)
           AND t.status = 'COMPLETED'
      ),
      detail_cte AS (
        SELECT s.region_code,
               s.region_name,
               s.category_code,
               s.category_name,
               COUNT(*) AS txn_count,
               SUM(s.transaction_amount) AS total_amount,
               AVG(s.transaction_amount) AS avg_amount,
               MIN(s.transaction_amount) AS min_amount,
               MAX(s.transaction_amount) AS max_amount,
               s.region_total_count,
               s.region_total_amt
          FROM summary_cte s
         GROUP BY s.region_code, s.region_name, s.category_code,
                  s.category_name, s.region_total_count, s.region_total_amt
      )
      SELECT d.region_code,
             d.region_name,
             d.category_code,
             d.category_name,
             d.txn_count,
             d.total_amount,
             d.avg_amount,
             d.min_amount,
             d.max_amount,
             ROW_NUMBER() OVER (PARTITION BY d.region_code ORDER BY d.total_amount DESC) AS category_rank,
             SUM(d.total_amount) OVER (PARTITION BY d.region_code ORDER BY d.total_amount DESC
                                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
             LAG(d.total_amount) OVER (PARTITION BY d.region_code ORDER BY d.total_amount DESC) AS prev_category_amt,
             ROUND(d.total_amount / NULLIF(d.region_total_amt, 0) * 100, 2) AS pct_of_region
        FROM detail_cte d
       ORDER BY d.region_code, d.total_amount DESC;

    RETURN v_cur;
  END FN_GET_SUMMARY_REPORT;

  -- -----------------------------------------------------------------------
  -- FN_GET_PIVOT_REPORT
  -- Opens a ref cursor for a PIVOT query
  -- -----------------------------------------------------------------------
  PROCEDURE FN_GET_PIVOT_REPORT (
    p_year    IN NUMBER,
    p_cur     OUT SYS_REFCURSOR
  )
  IS
  BEGIN
    OPEN p_cur FOR
      SELECT *
        FROM (
          SELECT t.region_code,
                 TO_CHAR(t.transaction_date, 'MON') AS txn_month,
                 t.transaction_amount
            FROM FINANCE.transactions t
           WHERE EXTRACT(YEAR FROM t.transaction_date) = p_year
             AND t.status = 'COMPLETED'
        )
        PIVOT (
          SUM(transaction_amount)
          FOR txn_month IN (
            'JAN' AS jan, 'FEB' AS feb, 'MAR' AS mar,
            'APR' AS apr, 'MAY' AS may, 'JUN' AS jun,
            'JUL' AS jul, 'AUG' AS aug, 'SEP' AS sep,
            'OCT' AS oct, 'NOV' AS nov, 'DEC' AS dec
          )
        )
       ORDER BY region_code;
  END FN_GET_PIVOT_REPORT;

  -- -----------------------------------------------------------------------
  -- PC_GENERATE_XML_REPORT
  -- Generates XML output using XMLELEMENT, XMLAGG, XMLFOREST
  -- -----------------------------------------------------------------------
  PROCEDURE PC_GENERATE_XML_REPORT (
    p_report_date IN DATE,
    p_report_id   OUT NUMBER
  )
  IS
    v_xml_clob  CLOB;
  BEGIN
    SELECT XMLELEMENT("Report",
             XMLATTRIBUTES(
               TO_CHAR(p_report_date, 'YYYY-MM-DD') AS "reportDate",
               gc_module AS "generator"
             ),
             XMLAGG(
               XMLELEMENT("Region",
                 XMLATTRIBUTES(region_code AS "code"),
                 XMLFOREST(
                   region_name     AS "RegionName",
                   total_txns      AS "TotalTransactions",
                   total_amount    AS "TotalAmount",
                   avg_amount      AS "AverageAmount"
                 )
               ) ORDER BY region_code
             )
           ).getClobVal()
      INTO v_xml_clob
      FROM (
        SELECT r.region_code,
               r.region_name,
               COUNT(t.transaction_id) AS total_txns,
               SUM(t.transaction_amount) AS total_amount,
               ROUND(AVG(t.transaction_amount), 2) AS avg_amount
          FROM FINANCE.transactions t
          JOIN MASTER.regions r
            ON t.region_code = r.region_code
         WHERE t.transaction_date = p_report_date
         GROUP BY r.region_code, r.region_name
      );

    -- Store XML report
    SELECT REPORTING.REPORT_SEQ.NEXTVAL INTO p_report_id FROM DUAL;

    INSERT INTO REPORTING.report_output (
      report_id, report_type, report_date,
      report_content, created_date, created_by
    ) VALUES (
      p_report_id, 'XML_SUMMARY', p_report_date,
      v_xml_clob, SYSDATE, USER
    );

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-20010,
        'XML report generation failed: ' || SQLERRM);
  END PC_GENERATE_XML_REPORT;

  -- -----------------------------------------------------------------------
  -- PC_GENERATE_CSV
  -- Generates CSV using LISTAGG, cursor FOR loop, DBMS_LOB, UTL_FILE
  -- -----------------------------------------------------------------------
  PROCEDURE PC_GENERATE_CSV (
    p_report_date IN DATE,
    p_filename    IN VARCHAR2
  )
  IS
    v_file     UTL_FILE.FILE_TYPE;
    v_clob     CLOB;
    v_line     VARCHAR2(4000);
    v_header   VARCHAR2(4000);
  BEGIN
    -- Build CSV header
    v_header := 'REGION_CODE,CATEGORY_CODE,CATEGORY_NAME,TXN_COUNT,TOTAL_AMOUNT,ACCOUNT_LIST';

    -- Initialize CLOB
    DBMS_LOB.CREATETEMPORARY(v_clob, TRUE);
    DBMS_LOB.APPEND(v_clob, v_header || CHR(10));

    -- Generate CSV rows using cursor FOR loop with LISTAGG
    FOR rec IN (
      SELECT t.region_code,
             t.category_code,
             c.category_name,
             COUNT(*) AS txn_count,
             SUM(t.transaction_amount) AS total_amount,
             LISTAGG(DISTINCT TO_CHAR(t.account_id), ';')
               WITHIN GROUP (ORDER BY t.account_id) AS account_list
        FROM FINANCE.transactions t
        JOIN MASTER.categories c
          ON t.category_code = c.category_code
       WHERE t.transaction_date = p_report_date
         AND t.status = 'COMPLETED'
       GROUP BY t.region_code, t.category_code, c.category_name
       ORDER BY t.region_code, t.category_code
    ) LOOP
      v_line := rec.region_code || ',' ||
                rec.category_code || ',' ||
                '"' || REPLACE(rec.category_name, '"', '""') || '",' ||
                rec.txn_count || ',' ||
                rec.total_amount || ',' ||
                '"' || rec.account_list || '"';

      DBMS_LOB.APPEND(v_clob, v_line || CHR(10));
    END LOOP;

    -- Write to file via UTL_FILE
    v_file := UTL_FILE.FOPEN(gc_csv_dir, p_filename, 'W', 32767);

    -- Write in chunks
    FOR i IN 0 .. TRUNC(DBMS_LOB.GETLENGTH(v_clob) / 4000) LOOP
      v_line := DBMS_LOB.SUBSTR(v_clob, 4000, i * 4000 + 1);
      IF v_line IS NOT NULL THEN
        UTL_FILE.PUT_LINE(v_file, v_line);
      END IF;
    END LOOP;

    UTL_FILE.FCLOSE(v_file);
    DBMS_LOB.FREETEMPORARY(v_clob);

    -- Log report generation
    INSERT INTO REPORTING.report_log (
      log_id, report_type, filename, report_date, created_date
    ) VALUES (
      REPORTING.REPORT_LOG_SEQ.NEXTVAL, 'CSV', p_filename,
      p_report_date, SYSDATE
    );

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      IF UTL_FILE.IS_OPEN(v_file) THEN
        UTL_FILE.FCLOSE(v_file);
      END IF;
      IF DBMS_LOB.ISTEMPORARY(v_clob) = 1 THEN
        DBMS_LOB.FREETEMPORARY(v_clob);
      END IF;
      ROLLBACK;
      RAISE;
  END PC_GENERATE_CSV;

  -- -----------------------------------------------------------------------
  -- FN_GET_HIERARCHY
  -- Hierarchical query with CONNECT BY and CTE alternative
  -- -----------------------------------------------------------------------
  FUNCTION FN_GET_HIERARCHY (
    p_root_org_id  IN NUMBER,
    p_use_cte      IN BOOLEAN DEFAULT FALSE
  ) RETURN SYS_REFCURSOR
  IS
    v_cur SYS_REFCURSOR;
  BEGIN
    IF p_use_cte THEN
      -- Recursive CTE version
      OPEN v_cur FOR
        WITH org_hierarchy (org_id, org_name, parent_org_id, lvl, path_str) AS (
          SELECT o.org_id,
                 o.org_name,
                 o.parent_org_id,
                 1 AS lvl,
                 o.org_name AS path_str
            FROM HR.organizations o
           WHERE o.org_id = p_root_org_id
          UNION ALL
          SELECT child.org_id,
                 child.org_name,
                 child.parent_org_id,
                 parent.lvl + 1,
                 parent.path_str || ' > ' || child.org_name
            FROM HR.organizations child
            JOIN org_hierarchy parent
              ON child.parent_org_id = parent.org_id
        )
        SELECT oh.org_id,
               oh.org_name,
               oh.parent_org_id,
               oh.lvl,
               oh.path_str,
               e.employee_count
          FROM org_hierarchy oh
          LEFT JOIN (
            SELECT org_id, COUNT(*) AS employee_count
              FROM HR.employees
             GROUP BY org_id
          ) e ON oh.org_id = e.org_id
         ORDER BY oh.path_str;
    ELSE
      -- CONNECT BY version
      OPEN v_cur FOR
        SELECT o.org_id,
               o.org_name,
               o.parent_org_id,
               LEVEL AS lvl,
               SYS_CONNECT_BY_PATH(o.org_name, ' > ') AS path_str,
               CONNECT_BY_ISLEAF AS is_leaf,
               (SELECT COUNT(*)
                  FROM HR.employees emp
                 WHERE emp.org_id = o.org_id) AS employee_count
          FROM HR.organizations o
         START WITH o.org_id = p_root_org_id
       CONNECT BY PRIOR o.org_id = o.parent_org_id
         ORDER SIBLINGS BY o.org_name;
    END IF;

    RETURN v_cur;
  END FN_GET_HIERARCHY;

  -- -----------------------------------------------------------------------
  -- PC_PAGINATED_QUERY
  -- Dynamic SQL with OFFSET/FETCH for pagination
  -- -----------------------------------------------------------------------
  PROCEDURE PC_PAGINATED_QUERY (
    p_table_name   IN VARCHAR2,
    p_where_clause IN VARCHAR2 DEFAULT NULL,
    p_order_by     IN VARCHAR2 DEFAULT 'id',
    p_page_num     IN NUMBER   DEFAULT 1,
    p_page_size    IN NUMBER   DEFAULT gc_page_size,
    p_result_cur   OUT SYS_REFCURSOR,
    p_total_count  OUT NUMBER
  )
  IS
    v_count_sql  VARCHAR2(4000);
    v_query_sql  VARCHAR2(4000);
    v_offset     NUMBER;
    v_where      VARCHAR2(2000);
  BEGIN
    v_where := CASE
                 WHEN p_where_clause IS NOT NULL THEN ' WHERE ' || p_where_clause
                 ELSE ''
               END;

    -- Get total count via dynamic SQL
    v_count_sql := 'SELECT COUNT(*) FROM REPORTING.' || DBMS_ASSERT.SIMPLE_SQL_NAME(p_table_name) || v_where;
    EXECUTE IMMEDIATE v_count_sql INTO p_total_count;

    -- Calculate offset
    v_offset := (p_page_num - 1) * p_page_size;

    -- Build paginated query with OFFSET/FETCH
    v_query_sql := 'SELECT * FROM REPORTING.' ||
                   DBMS_ASSERT.SIMPLE_SQL_NAME(p_table_name) ||
                   v_where ||
                   ' ORDER BY ' || p_order_by ||
                   ' OFFSET ' || v_offset || ' ROWS' ||
                   ' FETCH FIRST ' || p_page_size || ' ROWS ONLY';

    OPEN p_result_cur FOR v_query_sql;

    -- Log the query
    INSERT INTO REPORTING.query_log (
      log_id, query_text, page_num, page_size,
      total_count, executed_date, executed_by
    ) VALUES (
      REPORTING.QUERY_LOG_SEQ.NEXTVAL, v_query_sql, p_page_num,
      p_page_size, p_total_count, SYSDATE, USER
    );

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-20020,
        'Paginated query failed for table ' || p_table_name || ': ' || SQLERRM);
  END PC_PAGINATED_QUERY;

END PKG_REPORT_GEN;
/
