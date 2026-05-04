CREATE OR REPLACE PACKAGE BODY INTEGRATION.PKG_MEGA_INTEGRATION AS
  ---------------------------------------------------------------------------
  -- Mega Integration Package (15 subprograms)
  -- Covers ALL pattern categories: dynamic SQL, db links, FORALL,
  -- BULK COLLECT, cursors, exception handlers, sequences, schema-qualified
  -- tables, CTEs, analytics, MERGE, PIVOT, LISTAGG, hierarchical queries,
  -- flashback queries, pipelined functions, AUTONOMOUS_TRANSACTION,
  -- labeled loops, EXIT/CONTINUE WHEN, user-defined exceptions, CONNECT BY
  ---------------------------------------------------------------------------

  gc_module      CONSTANT VARCHAR2(40) := 'PKG_MEGA_INTEGRATION';
  gc_batch_limit CONSTANT PLS_INTEGER  := 500;

  -- Custom exceptions
  e_invalid_config  EXCEPTION;
  e_stale_data      EXCEPTION;
  e_merge_conflict  EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_invalid_config, -20200);
  PRAGMA EXCEPTION_INIT(e_stale_data,     -20201);
  PRAGMA EXCEPTION_INIT(e_merge_conflict,  -20202);

  -- Package-level types
  TYPE t_record_rec IS RECORD (
    record_id     NUMBER,
    customer_id   NUMBER,
    product_code  VARCHAR2(30),
    amount        NUMBER,
    category_code VARCHAR2(20),
    region_code   VARCHAR2(10),
    txn_date      DATE,
    status        VARCHAR2(20)
  );

  TYPE t_record_tab IS TABLE OF t_record_rec INDEX BY PLS_INTEGER;

  TYPE t_pipe_rec IS RECORD (
    employee_id    NUMBER,
    full_name      VARCHAR2(100),
    department     VARCHAR2(60),
    transformed_val VARCHAR2(200)
  );

  TYPE t_pipe_tab IS TABLE OF t_pipe_rec;

  -- -----------------------------------------------------------------------
  -- 1. PC_INIT
  -- Reads config, inserts into session_log with sequence
  -- -----------------------------------------------------------------------
  PROCEDURE PC_INIT (
    p_session_id OUT NUMBER,
    p_config_val OUT VARCHAR2
  )
  IS
    v_config_key  VARCHAR2(50) := 'DEFAULT_MODE';
    v_config_val  VARCHAR2(200);
  BEGIN
    SELECT cp.value
      INTO v_config_val
      FROM CUSTOMER.config_params cp
     WHERE cp.param_key = v_config_key
       AND cp.active_flag = 'Y'
       AND ROWNUM = 1;

    p_config_val := v_config_val;

    SELECT INTEGRATION.INIT_SEQ.NEXTVAL INTO p_session_id FROM DUAL;

    INSERT INTO INTEGRATION.session_log (
      session_id, config_value, start_time,
      session_user, module_name
    ) VALUES (
      p_session_id, v_config_val, SYSDATE,
      SYS_CONTEXT('USERENV', 'SESSION_USER'), gc_module
    );

    COMMIT;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RAISE_APPLICATION_ERROR(-20200,
        'Configuration key not found: ' || v_config_key);
  END PC_INIT;

  -- -----------------------------------------------------------------------
  -- 2. FN_GET_CONFIG
  -- SELECT INTO with schema-qualified table and %TYPE
  -- -----------------------------------------------------------------------
  FUNCTION FN_GET_CONFIG (
    p_param_key IN VARCHAR2
  ) RETURN VARCHAR2
  IS
    v_param CUSTOMER.config_params.value%TYPE;
  BEGIN
    SELECT cp.value
      INTO v_param
      FROM CUSTOMER.config_params cp
     WHERE cp.param_key = p_param_key
       AND cp.active_flag = 'Y';

    RETURN v_param;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN NULL;
    WHEN TOO_MANY_ROWS THEN
      SELECT MIN(cp.value)
        INTO v_param
        FROM CUSTOMER.config_params cp
       WHERE cp.param_key = p_param_key
         AND cp.active_flag = 'Y';
      RETURN v_param;
  END FN_GET_CONFIG;

  -- -----------------------------------------------------------------------
  -- 3. PC_LOAD_BATCH
  -- Parameterized cursor, BULK COLLECT, FORALL INSERT with SAVE EXCEPTIONS
  -- -----------------------------------------------------------------------
  PROCEDURE PC_LOAD_BATCH (
    p_region_code IN VARCHAR2,
    p_batch_date  IN DATE,
    p_loaded_cnt  OUT NUMBER,
    p_error_cnt   OUT NUMBER
  )
  IS
    CURSOR c_source (cp_region VARCHAR2, cp_date DATE) IS
      SELECT s.record_id,
             s.customer_id,
             s.product_code,
             s.amount,
             s.category_code,
             s.region_code,
             s.txn_date,
             'PENDING' AS status
        FROM DATA_SCHEMA.source_records s
       WHERE s.region_code = cp_region
         AND s.txn_date   >= cp_date
         AND s.status      = 'READY';

    TYPE t_source_tab IS TABLE OF c_source%ROWTYPE;
    v_batch       t_source_tab;
    v_batch_id    NUMBER;
    e_bulk_errors EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_bulk_errors, -24381);
  BEGIN
    p_loaded_cnt := 0;
    p_error_cnt  := 0;

    SELECT INTEGRATION.BATCH_SEQ.NEXTVAL INTO v_batch_id FROM DUAL;

    OPEN c_source(p_region_code, p_batch_date);
    LOOP
      FETCH c_source BULK COLLECT INTO v_batch LIMIT gc_batch_limit;
      EXIT WHEN v_batch.COUNT = 0;

      BEGIN
        FORALL i IN 1 .. v_batch.COUNT SAVE EXCEPTIONS
          INSERT INTO INTEGRATION.batch_staging (
            staging_id, batch_id, record_id, customer_id,
            product_code, amount, category_code, region_code,
            txn_date, status, load_date
          ) VALUES (
            INTEGRATION.STAGING_SEQ.NEXTVAL,
            v_batch_id,
            v_batch(i).record_id,
            v_batch(i).customer_id,
            v_batch(i).product_code,
            v_batch(i).amount,
            v_batch(i).category_code,
            v_batch(i).region_code,
            v_batch(i).txn_date,
            v_batch(i).status,
            SYSDATE
          );

        p_loaded_cnt := p_loaded_cnt + v_batch.COUNT;

      EXCEPTION
        WHEN e_bulk_errors THEN
          p_error_cnt := p_error_cnt + SQL%BULK_EXCEPTIONS.COUNT;
          p_loaded_cnt := p_loaded_cnt + v_batch.COUNT - SQL%BULK_EXCEPTIONS.COUNT;

          FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
            INSERT INTO INTEGRATION.load_errors (
              error_id, batch_id, record_id, error_index,
              error_code, error_date
            ) VALUES (
              INTEGRATION.ERR_SEQ.NEXTVAL, v_batch_id,
              v_batch(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).record_id,
              SQL%BULK_EXCEPTIONS(j).ERROR_INDEX,
              SQL%BULK_EXCEPTIONS(j).ERROR_CODE,
              SYSDATE
            );
          END LOOP;
      END;

      COMMIT;
    END LOOP;
    CLOSE c_source;

  EXCEPTION
    WHEN OTHERS THEN
      IF c_source%ISOPEN THEN
        CLOSE c_source;
      END IF;
      ROLLBACK;
      RAISE;
  END PC_LOAD_BATCH;

  -- -----------------------------------------------------------------------
  -- 4. PC_PROCESS_RECORDS
  -- Cursor FOR loop with 5-table JOIN, analytics, CASE, correlated UPDATE
  -- -----------------------------------------------------------------------
  PROCEDURE PC_PROCESS_RECORDS (
    p_batch_id  IN NUMBER
  )
  IS
    CURSOR c_process IS
      SELECT bs.staging_id,
             bs.record_id,
             bs.customer_id,
             bs.product_code,
             bs.amount,
             c.customer_name,
             p.product_name,
             cat.category_name,
             r.region_name,
             RANK() OVER (PARTITION BY bs.region_code ORDER BY bs.amount DESC) AS amount_rank,
             SUM(bs.amount) OVER (PARTITION BY bs.customer_id) AS customer_total
        FROM INTEGRATION.batch_staging bs
       INNER JOIN CUSTOMER.customers c
          ON bs.customer_id = c.customer_id
        LEFT JOIN MASTER.products p
          ON bs.product_code = p.product_code
       RIGHT JOIN MASTER.categories cat
          ON bs.category_code = cat.category_code
        JOIN MASTER.regions r
          ON bs.region_code = r.region_code
       WHERE bs.batch_id = p_batch_id
         AND bs.status    = 'PENDING';

    v_priority VARCHAR2(10);
  BEGIN
    FOR rec IN c_process LOOP
      -- Determine priority via CASE with function call
      v_priority := CASE
                      WHEN rec.amount_rank <= 5 AND rec.customer_total > 100000 THEN 'CRITICAL'
                      WHEN rec.amount_rank <= 10 THEN 'HIGH'
                      WHEN FN_GET_CONFIG('ENABLE_MED_PRIORITY') = 'Y' THEN 'MEDIUM'
                      ELSE 'LOW'
                    END;

      -- Update with correlated subquery
      UPDATE INTEGRATION.batch_staging bs
         SET bs.status        = 'PROCESSED',
             bs.priority      = v_priority,
             bs.processed_date = SYSDATE,
             bs.process_notes = (
               SELECT LISTAGG(n.note_text, '; ') WITHIN GROUP (ORDER BY n.note_date)
                 FROM CUSTOMER.customer_notes n
                WHERE n.customer_id = rec.customer_id
                  AND n.note_date >= ADD_MONTHS(SYSDATE, -6)
             )
       WHERE bs.staging_id = rec.staging_id;
    END LOOP;

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      PC_AUDIT_LOG('PC_PROCESS_RECORDS', 'Error: ' || SQLERRM);
      RAISE;
  END PC_PROCESS_RECORDS;

  -- -----------------------------------------------------------------------
  -- 5. PC_MERGE_RESULTS
  -- MERGE with CTE source, WHEN MATCHED UPDATE + DELETE WHERE,
  -- WHEN NOT MATCHED INSERT
  -- -----------------------------------------------------------------------
  PROCEDURE PC_MERGE_RESULTS (
    p_batch_id IN NUMBER
  )
  IS
  BEGIN
    MERGE INTO INTEGRATION.master_results mr
    USING (
      WITH staged_data AS (
        SELECT bs.record_id,
               bs.customer_id,
               bs.product_code,
               bs.amount,
               bs.category_code,
               bs.region_code,
               bs.txn_date,
               bs.priority,
               ROW_NUMBER() OVER (
                 PARTITION BY bs.customer_id, bs.product_code
                 ORDER BY bs.txn_date DESC
               ) AS rn
          FROM INTEGRATION.batch_staging bs
         WHERE bs.batch_id = p_batch_id
           AND bs.status   = 'PROCESSED'
      )
      SELECT record_id, customer_id, product_code, amount,
             category_code, region_code, txn_date, priority
        FROM staged_data
       WHERE rn = 1
    ) src
    ON (mr.customer_id = src.customer_id AND mr.product_code = src.product_code)
    WHEN MATCHED THEN
      UPDATE SET mr.amount        = src.amount,
                 mr.category_code = src.category_code,
                 mr.region_code   = src.region_code,
                 mr.txn_date      = src.txn_date,
                 mr.priority      = src.priority,
                 mr.updated_date  = SYSDATE,
                 mr.batch_id      = p_batch_id
      DELETE WHERE mr.priority = 'OBSOLETE'
    WHEN NOT MATCHED THEN
      INSERT (result_id, customer_id, product_code, amount,
              category_code, region_code, txn_date, priority,
              created_date, batch_id)
      VALUES (INTEGRATION.RESULT_SEQ.NEXTVAL, src.customer_id,
              src.product_code, src.amount, src.category_code,
              src.region_code, src.txn_date, src.priority,
              SYSDATE, p_batch_id);

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      PC_AUDIT_LOG('PC_MERGE_RESULTS', 'Merge error: ' || SQLERRM);
      RAISE;
  END PC_MERGE_RESULTS;

  -- -----------------------------------------------------------------------
  -- 6. FN_GET_REPORT
  -- SYS_REFCURSOR with PIVOT, LISTAGG, GROUP BY ROLLUP
  -- -----------------------------------------------------------------------
  FUNCTION FN_GET_REPORT (
    p_year      IN NUMBER,
    p_region    IN VARCHAR2 DEFAULT NULL
  ) RETURN SYS_REFCURSOR
  IS
    v_cur SYS_REFCURSOR;
  BEGIN
    OPEN v_cur FOR
      WITH base_data AS (
        SELECT mr.region_code,
               mr.category_code,
               TO_CHAR(mr.txn_date, 'Q') AS quarter,
               mr.amount,
               mr.customer_id
          FROM INTEGRATION.master_results mr
         WHERE EXTRACT(YEAR FROM mr.txn_date) = p_year
           AND (p_region IS NULL OR mr.region_code = p_region)
      ),
      summary AS (
        SELECT region_code,
               category_code,
               quarter,
               SUM(amount) AS total_amount,
               COUNT(DISTINCT customer_id) AS unique_customers,
               LISTAGG(DISTINCT category_code, ',')
                 WITHIN GROUP (ORDER BY category_code) OVER (PARTITION BY region_code) AS region_categories
          FROM base_data
         GROUP BY ROLLUP(region_code, category_code, quarter)
      )
      SELECT *
        FROM (
          SELECT s.region_code,
                 s.category_code,
                 s.quarter,
                 s.total_amount,
                 s.unique_customers,
                 s.region_categories
            FROM summary s
           WHERE s.region_code IS NOT NULL
             AND s.category_code IS NOT NULL
             AND s.quarter IS NOT NULL
        )
        PIVOT (
          SUM(total_amount) AS amt,
          MAX(unique_customers) AS cust
          FOR quarter IN ('1' AS q1, '2' AS q2, '3' AS q3, '4' AS q4)
        )
       ORDER BY region_code, category_code;

    RETURN v_cur;
  END FN_GET_REPORT;

  -- -----------------------------------------------------------------------
  -- 7. PC_ARCHIVE_DATA
  -- INSERT INTO archive SELECT, DELETE from active, ARCHIVE_SEQ
  -- -----------------------------------------------------------------------
  PROCEDURE PC_ARCHIVE_DATA (
    p_cutoff_date  IN DATE,
    p_archived_cnt OUT NUMBER
  )
  IS
  BEGIN
    INSERT INTO ARCHIVE.archived_results (
      archive_id, result_id, customer_id, product_code,
      amount, category_code, region_code, txn_date,
      priority, original_created, archived_date
    )
    SELECT ARCHIVE.ARCHIVE_SEQ.NEXTVAL,
           mr.result_id,
           mr.customer_id,
           mr.product_code,
           mr.amount,
           mr.category_code,
           mr.region_code,
           mr.txn_date,
           mr.priority,
           mr.created_date,
           SYSDATE
      FROM INTEGRATION.master_results mr
     WHERE mr.txn_date < p_cutoff_date
       AND mr.priority != 'CRITICAL';

    p_archived_cnt := SQL%ROWCOUNT;

    DELETE FROM INTEGRATION.master_results
     WHERE txn_date < p_cutoff_date
       AND priority != 'CRITICAL';

    INSERT INTO INTEGRATION.archive_log (
      log_id, cutoff_date, archived_count, archive_date, archived_by
    ) VALUES (
      INTEGRATION.ARCHIVE_LOG_SEQ.NEXTVAL, p_cutoff_date,
      p_archived_cnt, SYSDATE, USER
    );

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      PC_AUDIT_LOG('PC_ARCHIVE_DATA', 'Archive error: ' || SQLERRM);
      RAISE;
  END PC_ARCHIVE_DATA;

  -- -----------------------------------------------------------------------
  -- 8. PC_DYNAMIC_CLEANUP
  -- EXECUTE IMMEDIATE TRUNCATE, EXECUTE IMMEDIATE DELETE with USING,
  -- DBMS_SQL.PARSE
  -- -----------------------------------------------------------------------
  PROCEDURE PC_DYNAMIC_CLEANUP (
    p_table_name   IN VARCHAR2,
    p_days_old     IN NUMBER DEFAULT 90,
    p_use_truncate IN BOOLEAN DEFAULT FALSE
  )
  IS
    v_sql       VARCHAR2(4000);
    v_cursor_id INTEGER;
    v_rows      INTEGER;
    v_cutoff    DATE := SYSDATE - p_days_old;
    v_safe_name VARCHAR2(128);
  BEGIN
    v_safe_name := DBMS_ASSERT.SIMPLE_SQL_NAME(p_table_name);

    IF p_use_truncate THEN
      -- Truncate via dynamic DDL
      EXECUTE IMMEDIATE 'TRUNCATE TABLE INTEGRATION.' || v_safe_name;
    ELSE
      -- Delete with bind variable via EXECUTE IMMEDIATE
      v_sql := 'DELETE FROM INTEGRATION.' || v_safe_name ||
               ' WHERE created_date < :cutoff_date';
      EXECUTE IMMEDIATE v_sql USING v_cutoff;
    END IF;

    -- Alternative: DBMS_SQL for complex dynamic cleanup
    v_sql := 'DELETE FROM INTEGRATION.cleanup_queue' ||
             ' WHERE table_name = :tbl AND processed_flag = :flag';

    v_cursor_id := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cursor_id, v_sql, DBMS_SQL.NATIVE);
    DBMS_SQL.BIND_VARIABLE(v_cursor_id, ':tbl', p_table_name);
    DBMS_SQL.BIND_VARIABLE(v_cursor_id, ':flag', 'Y');
    v_rows := DBMS_SQL.EXECUTE(v_cursor_id);
    DBMS_SQL.CLOSE_CURSOR(v_cursor_id);

    INSERT INTO INTEGRATION.cleanup_log (
      log_id, table_name, cleanup_type, rows_affected,
      cleanup_date, cleaned_by
    ) VALUES (
      INTEGRATION.CLEANUP_SEQ.NEXTVAL, p_table_name,
      CASE WHEN p_use_truncate THEN 'TRUNCATE' ELSE 'DELETE' END,
      v_rows, SYSDATE, USER
    );

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      IF DBMS_SQL.IS_OPEN(v_cursor_id) THEN
        DBMS_SQL.CLOSE_CURSOR(v_cursor_id);
      END IF;
      ROLLBACK;
      PC_AUDIT_LOG('PC_DYNAMIC_CLEANUP', 'Cleanup error on ' || p_table_name || ': ' || SQLERRM);
      RAISE;
  END PC_DYNAMIC_CLEANUP;

  -- -----------------------------------------------------------------------
  -- 9. FN_PIPELINED_TRANSFORM
  -- PIPELINED function with PIPE ROW; consumed via TABLE()
  -- -----------------------------------------------------------------------
  FUNCTION FN_PIPELINED_TRANSFORM (
    p_dept_id IN NUMBER
  ) RETURN t_pipe_tab PIPELINED
  IS
    v_rec t_pipe_rec;
  BEGIN
    FOR emp IN (
      SELECT e.employee_id,
             e.first_name || ' ' || e.last_name AS full_name,
             d.department_name
        FROM HR.employees e
        JOIN HR.departments d
          ON e.department_id = d.department_id
       WHERE e.department_id = p_dept_id
         AND e.status = 'ACTIVE'
       ORDER BY e.last_name
    ) LOOP
      v_rec.employee_id     := emp.employee_id;
      v_rec.full_name       := emp.full_name;
      v_rec.department      := emp.department_name;
      v_rec.transformed_val := UPPER(SUBSTR(emp.full_name, 1, 1)) || '_' ||
                               TO_CHAR(emp.employee_id) || '_' ||
                               REPLACE(emp.department_name, ' ', '_');
      PIPE ROW(v_rec);
    END LOOP;

    RETURN;

  EXCEPTION
    WHEN NO_DATA_NEEDED THEN
      NULL; -- Consumer stopped reading
  END FN_PIPELINED_TRANSFORM;

  -- -----------------------------------------------------------------------
  -- 10. PC_AUDIT_LOG
  -- PRAGMA AUTONOMOUS_TRANSACTION; INSERT INTO audit_log; COMMIT
  -- -----------------------------------------------------------------------
  PROCEDURE PC_AUDIT_LOG (
    p_action    IN VARCHAR2,
    p_details   IN VARCHAR2
  )
  IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    INSERT INTO INTEGRATION.audit_log (
      audit_id, module_name, action_name, details,
      audit_timestamp, session_user, os_user
    ) VALUES (
      INTEGRATION.AUDIT_SEQ.NEXTVAL,
      gc_module,
      p_action,
      SUBSTR(p_details, 1, 4000),
      SYSTIMESTAMP,
      SYS_CONTEXT('USERENV', 'SESSION_USER'),
      SYS_CONTEXT('USERENV', 'OS_USER')
    );

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      NULL; -- Audit logger must never propagate exceptions
  END PC_AUDIT_LOG;

  -- -----------------------------------------------------------------------
  -- 11. PC_ERROR_HANDLER
  -- 3-level nested EXCEPTION, RAISE_APPLICATION_ERROR, custom exceptions
  -- -----------------------------------------------------------------------
  PROCEDURE PC_ERROR_HANDLER (
    p_record_id IN NUMBER
  )
  IS
    v_status      VARCHAR2(20);
    v_amount      NUMBER;
    v_config_val  VARCHAR2(200);
  BEGIN
    -- Level 1: Retrieve and validate
    BEGIN
      SELECT mr.priority, mr.amount
        INTO v_status, v_amount
        FROM INTEGRATION.master_results mr
       WHERE mr.result_id = p_record_id;

      IF v_status = 'OBSOLETE' THEN
        RAISE e_stale_data;
      END IF;

      -- Level 2: Process with config check
      BEGIN
        v_config_val := FN_GET_CONFIG('PROCESSING_MODE');

        IF v_config_val IS NULL THEN
          RAISE e_invalid_config;
        END IF;

        -- Level 3: Attempt the update
        BEGIN
          UPDATE INTEGRATION.master_results
             SET status       = 'REPROCESSED',
                 updated_date = SYSDATE
           WHERE result_id = p_record_id
             AND priority  = v_status;

          IF SQL%ROWCOUNT = 0 THEN
            RAISE e_merge_conflict;
          END IF;

          COMMIT;

        EXCEPTION
          WHEN e_merge_conflict THEN
            PC_AUDIT_LOG('ERROR_HANDLER', 'Merge conflict on record ' || p_record_id);
            RAISE_APPLICATION_ERROR(-20202,
              'Concurrent modification detected for record ' || p_record_id);
        END; -- Level 3

      EXCEPTION
        WHEN e_invalid_config THEN
          PC_AUDIT_LOG('ERROR_HANDLER', 'Missing config for processing mode');
          INSERT INTO INTEGRATION.error_records (
            error_id, record_id, error_type, error_msg, error_date
          ) VALUES (
            INTEGRATION.ERR_SEQ.NEXTVAL, p_record_id, 'CONFIG',
            'Processing mode not configured', SYSDATE
          );
          COMMIT;
      END; -- Level 2

    EXCEPTION
      WHEN e_stale_data THEN
        PC_AUDIT_LOG('ERROR_HANDLER', 'Stale data for record ' || p_record_id);
        RAISE_APPLICATION_ERROR(-20201,
          'Record ' || p_record_id || ' has obsolete status');
      WHEN NO_DATA_FOUND THEN
        PC_AUDIT_LOG('ERROR_HANDLER', 'Record not found: ' || p_record_id);
    END; -- Level 1

  EXCEPTION
    WHEN OTHERS THEN
      PC_AUDIT_LOG('ERROR_HANDLER', 'Unhandled: ' || SQLCODE || ' - ' || SQLERRM);
      RAISE;
  END PC_ERROR_HANDLER;

  -- -----------------------------------------------------------------------
  -- 12. PC_ORCHESTRATE_ALL
  -- Calls all subprograms; labeled loops; EXIT WHEN; CONTINUE WHEN
  -- -----------------------------------------------------------------------
  PROCEDURE PC_ORCHESTRATE_ALL (
    p_region_code  IN VARCHAR2,
    p_batch_date   IN DATE,
    p_final_status OUT VARCHAR2
  )
  IS
    v_session_id   NUMBER;
    v_config_val   VARCHAR2(200);
    v_loaded_cnt   NUMBER;
    v_error_cnt    NUMBER;
    v_archived_cnt NUMBER;
    v_batch_id     NUMBER;

    TYPE t_region_tab IS TABLE OF VARCHAR2(10);
    v_regions t_region_tab;
  BEGIN
    -- Initialize
    PC_INIT(p_session_id => v_session_id, p_config_val => v_config_val);
    PC_AUDIT_LOG('ORCHESTRATE', 'Starting orchestration for region ' || p_region_code);

    -- Collect regions to process
    SELECT region_code
      BULK COLLECT INTO v_regions
      FROM MASTER.regions
     WHERE (p_region_code = 'ALL' OR region_code = p_region_code)
       AND active_flag = 'Y';

    -- Process each region with labeled loop
    <<region_loop>>
    FOR i IN 1 .. v_regions.COUNT LOOP
      -- Skip suspended regions
      CONTINUE WHEN v_regions(i) IN (
        SELECT region_code FROM INTEGRATION.suspended_regions WHERE status = 'SUSPENDED'
      );

      BEGIN
        -- Load batch
        PC_LOAD_BATCH(
          p_region_code => v_regions(i),
          p_batch_date  => p_batch_date,
          p_loaded_cnt  => v_loaded_cnt,
          p_error_cnt   => v_error_cnt
        );

        -- Exit early if too many errors
        EXIT region_loop WHEN v_error_cnt > v_loaded_cnt * 0.5;

        -- Process records
        SELECT MAX(batch_id)
          INTO v_batch_id
          FROM INTEGRATION.batch_staging
         WHERE region_code = v_regions(i)
           AND status = 'PENDING';

        IF v_batch_id IS NOT NULL THEN
          PC_PROCESS_RECORDS(p_batch_id => v_batch_id);
          PC_MERGE_RESULTS(p_batch_id => v_batch_id);
        END IF;

      EXCEPTION
        WHEN OTHERS THEN
          PC_AUDIT_LOG('ORCHESTRATE',
            'Error processing region ' || v_regions(i) || ': ' || SQLERRM);
          CONTINUE region_loop; -- Skip to next region on error
      END;
    END LOOP region_loop;

    -- Archive old data
    <<archive_block>>
    BEGIN
      PC_ARCHIVE_DATA(
        p_cutoff_date  => ADD_MONTHS(SYSDATE, -12),
        p_archived_cnt => v_archived_cnt
      );
    EXCEPTION
      WHEN OTHERS THEN
        PC_AUDIT_LOG('ORCHESTRATE', 'Archive step failed: ' || SQLERRM);
    END archive_block;

    -- Cleanup staging
    PC_DYNAMIC_CLEANUP(
      p_table_name   => 'batch_staging',
      p_days_old     => 7,
      p_use_truncate => FALSE
    );

    -- Consume pipelined function for verification
    <<verify_loop>>
    FOR rec IN (
      SELECT pt.employee_id, pt.full_name, pt.transformed_val
        FROM TABLE(FN_PIPELINED_TRANSFORM(10)) pt
    ) LOOP
      NULL; -- Verification placeholder
    END LOOP verify_loop;

    p_final_status := 'COMPLETED';
    PC_AUDIT_LOG('ORCHESTRATE', 'Orchestration completed successfully');

    -- Update session log
    UPDATE INTEGRATION.session_log
       SET end_time     = SYSDATE,
           final_status = p_final_status
     WHERE session_id = v_session_id;

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      p_final_status := 'FAILED';
      PC_AUDIT_LOG('ORCHESTRATE', 'Fatal error: ' || SQLERRM);
      RAISE;
  END PC_ORCHESTRATE_ALL;

  -- -----------------------------------------------------------------------
  -- 13. PC_VALIDATE_DATA
  -- WITH clause (3 CTEs), EXISTS + NOT EXISTS + IN subquery + scalar subquery
  -- -----------------------------------------------------------------------
  PROCEDURE PC_VALIDATE_DATA (
    p_batch_id    IN NUMBER,
    p_invalid_cnt OUT NUMBER
  )
  IS
  BEGIN
    WITH valid_customers AS (
      SELECT c.customer_id, c.customer_name, c.credit_limit
        FROM CUSTOMER.customers c
       WHERE c.status = 'ACTIVE'
         AND c.credit_limit > 0
    ),
    valid_products AS (
      SELECT p.product_code, p.product_name, p.unit_price
        FROM MASTER.products p
       WHERE p.status = 'ACTIVE'
         AND p.unit_price IS NOT NULL
    ),
    batch_summary AS (
      SELECT bs.customer_id,
             SUM(bs.amount) AS total_batch_amount,
             COUNT(*) AS record_count
        FROM INTEGRATION.batch_staging bs
       WHERE bs.batch_id = p_batch_id
       GROUP BY bs.customer_id
    )
    SELECT COUNT(*)
      INTO p_invalid_cnt
      FROM INTEGRATION.batch_staging bs
     WHERE bs.batch_id = p_batch_id
       AND bs.status = 'PENDING'
       AND (
         -- Customer must exist and be active
         NOT EXISTS (
           SELECT 1
             FROM valid_customers vc
            WHERE vc.customer_id = bs.customer_id
         )
         OR
         -- Product must be valid
         NOT EXISTS (
           SELECT 1
             FROM valid_products vp
            WHERE vp.product_code = bs.product_code
         )
         OR
         -- Amount must not exceed credit limit (scalar subquery)
         bs.amount > (
           SELECT NVL(vc2.credit_limit, 0)
             FROM valid_customers vc2
            WHERE vc2.customer_id = bs.customer_id
         )
         OR
         -- Customer batch total must be under threshold
         bs.customer_id IN (
           SELECT bsum.customer_id
             FROM batch_summary bsum
            WHERE bsum.total_batch_amount > 1000000
         )
         OR
         -- Region must be in allowed list
         bs.region_code NOT IN (
           SELECT r.region_code
             FROM MASTER.regions r
            WHERE r.active_flag = 'Y'
         )
       );

    -- Mark invalid records
    UPDATE INTEGRATION.batch_staging bs
       SET bs.status = 'INVALID',
           bs.validation_msg = 'Failed validation checks',
           bs.validated_date = SYSDATE
     WHERE bs.batch_id = p_batch_id
       AND bs.status = 'PENDING'
       AND (
         NOT EXISTS (
           SELECT 1 FROM CUSTOMER.customers c
            WHERE c.customer_id = bs.customer_id AND c.status = 'ACTIVE'
         )
         OR NOT EXISTS (
           SELECT 1 FROM MASTER.products p
            WHERE p.product_code = bs.product_code AND p.status = 'ACTIVE'
         )
       );

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      PC_AUDIT_LOG('PC_VALIDATE_DATA', 'Validation error: ' || SQLERRM);
      RAISE;
  END PC_VALIDATE_DATA;

  -- -----------------------------------------------------------------------
  -- 14. FN_HIERARCHICAL
  -- CONNECT BY START WITH, SYS_CONNECT_BY_PATH, LEVEL pseudocolumn
  -- -----------------------------------------------------------------------
  FUNCTION FN_HIERARCHICAL (
    p_root_id IN NUMBER
  ) RETURN SYS_REFCURSOR
  IS
    v_cur SYS_REFCURSOR;
  BEGIN
    OPEN v_cur FOR
      SELECT o.org_id,
             o.org_name,
             o.parent_org_id,
             LEVEL AS hierarchy_level,
             SYS_CONNECT_BY_PATH(o.org_name, '/') AS full_path,
             CONNECT_BY_ROOT o.org_name AS root_name,
             CONNECT_BY_ISLEAF AS is_leaf_node,
             LPAD(' ', (LEVEL - 1) * 4) || o.org_name AS indented_name,
             (SELECT COUNT(*)
                FROM HR.employees emp
               WHERE emp.org_id = o.org_id
                 AND emp.status = 'ACTIVE') AS active_employees,
             (SELECT SUM(emp2.salary)
                FROM HR.employees emp2
               WHERE emp2.org_id = o.org_id) AS total_salary
        FROM HR.organizations o
       START WITH o.org_id = p_root_id
     CONNECT BY PRIOR o.org_id = o.parent_org_id
       ORDER SIBLINGS BY o.org_name;

    RETURN v_cur;
  END FN_HIERARCHICAL;

  -- -----------------------------------------------------------------------
  -- 15. PC_FLASHBACK_COMPARE
  -- Flashback query with AS OF TIMESTAMP, comparing current vs past state
  -- -----------------------------------------------------------------------
  PROCEDURE PC_FLASHBACK_COMPARE (
    p_hours_ago   IN NUMBER DEFAULT 24,
    p_result_cur  OUT SYS_REFCURSOR
  )
  IS
    v_flashback_time TIMESTAMP := SYSTIMESTAMP - NUMTODSINTERVAL(p_hours_ago, 'HOUR');
  BEGIN
    OPEN p_result_cur FOR
      SELECT e_current.employee_id,
             e_current.first_name || ' ' || e_current.last_name AS current_name,
             e_current.salary AS current_salary,
             e_current.department_id AS current_dept,
             e_past.salary AS past_salary,
             e_past.department_id AS past_dept,
             e_current.salary - NVL(e_past.salary, 0) AS salary_change,
             CASE
               WHEN e_past.employee_id IS NULL THEN 'NEW_HIRE'
               WHEN e_current.salary != e_past.salary THEN 'SALARY_CHANGED'
               WHEN e_current.department_id != e_past.department_id THEN 'DEPT_TRANSFER'
               ELSE 'NO_CHANGE'
             END AS change_type
        FROM HR.employees e_current
        LEFT JOIN HR.employees AS OF TIMESTAMP v_flashback_time e_past
          ON e_current.employee_id = e_past.employee_id
       WHERE e_current.status = 'ACTIVE'
         AND (
           e_past.employee_id IS NULL
           OR e_current.salary != e_past.salary
           OR e_current.department_id != e_past.department_id
         )
       ORDER BY CASE
                  WHEN e_past.employee_id IS NULL THEN 1
                  WHEN e_current.salary != e_past.salary THEN 2
                  ELSE 3
                END,
                ABS(e_current.salary - NVL(e_past.salary, 0)) DESC;

    PC_AUDIT_LOG('PC_FLASHBACK_COMPARE',
      'Compared current state with ' || p_hours_ago || ' hours ago');

  EXCEPTION
    WHEN OTHERS THEN
      PC_AUDIT_LOG('PC_FLASHBACK_COMPARE', 'Flashback error: ' || SQLERRM);
      RAISE;
  END PC_FLASHBACK_COMPARE;

END PKG_MEGA_INTEGRATION;
/
