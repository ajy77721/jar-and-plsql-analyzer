CREATE OR REPLACE PACKAGE BODY ETL.PKG_ETL_PROCESSOR AS
  ---------------------------------------------------------------------------
  -- ETL Batch Processing Package
  -- Extracts from remote sources, transforms, loads to target, cleans up
  ---------------------------------------------------------------------------

  gc_module       CONSTANT VARCHAR2(30) := 'PKG_ETL_PROCESSOR';
  gc_batch_limit  CONSTANT PLS_INTEGER  := 500;

  TYPE t_staging_rec IS RECORD (
    staging_id    staging_data.staging_id%TYPE,
    source_key    staging_data.source_key%TYPE,
    raw_value     staging_data.raw_value%TYPE,
    category_code staging_data.category_code%TYPE,
    amount        staging_data.amount%TYPE,
    effective_dt  staging_data.effective_dt%TYPE,
    region_code   staging_data.region_code%TYPE
  );

  TYPE t_staging_tab IS TABLE OF t_staging_rec INDEX BY PLS_INTEGER;

  TYPE t_target_rec IS RECORD (
    target_id     target_table.target_id%TYPE,
    batch_id      target_table.batch_id%TYPE,
    processed_val target_table.processed_val%TYPE,
    status_code   target_table.status_code%TYPE
  );

  TYPE t_target_tab IS TABLE OF t_target_rec INDEX BY PLS_INTEGER;

  -- -----------------------------------------------------------------------
  -- PC_EXTRACT_TO_STAGING
  -- Truncates staging, pulls data from remote sources via db links
  -- -----------------------------------------------------------------------
  PROCEDURE PC_EXTRACT_TO_STAGING (
    p_batch_date  IN DATE,
    p_region      IN VARCHAR2,
    p_batch_id    OUT NUMBER
  )
  IS
    v_sql       VARCHAR2(4000);
    v_row_count NUMBER := 0;
  BEGIN
    -- Get next batch ID from sequence
    SELECT ETL.BATCH_ID_SEQ.NEXTVAL INTO p_batch_id FROM DUAL;

    -- Truncate staging via dynamic SQL
    EXECUTE IMMEDIATE 'TRUNCATE TABLE ETL.staging_data';

    -- Extract from remote source tables via db links with joins
    INSERT INTO ETL.staging_data (
      staging_id,
      batch_id,
      source_key,
      raw_value,
      category_code,
      amount,
      effective_dt,
      region_code,
      created_by,
      created_date
    )
    SELECT ETL.STAGING_SEQ.NEXTVAL,
           p_batch_id,
           s1.source_key,
           s1.raw_value,
           s2.category_code,
           s1.amount * NVL(s2.conversion_rate, 1),
           s1.effective_dt,
           s1.region_code,
           gc_module,
           SYSDATE
      FROM REMOTE_OWNER.source_transactions@PROD_LINK s1
      JOIN REMOTE_OWNER.source_categories@PROD_LINK s2
        ON s1.category_id = s2.category_id
       AND s2.active_flag = 'Y'
      LEFT JOIN REMOTE_OWNER.source_exclusions@PROD_LINK s3
        ON s1.source_key = s3.source_key
     WHERE s1.effective_dt >= p_batch_date
       AND s1.region_code = p_region
       AND s3.source_key IS NULL;

    v_row_count := SQL%ROWCOUNT;

    -- Also pull supplementary data from a second remote source
    INSERT INTO ETL.staging_data (
      staging_id,
      batch_id,
      source_key,
      raw_value,
      category_code,
      amount,
      effective_dt,
      region_code,
      created_by,
      created_date
    )
    SELECT ETL.STAGING_SEQ.NEXTVAL,
           p_batch_id,
           sp.partner_key,
           sp.partner_value,
           sp.partner_category,
           sp.partner_amount,
           sp.partner_dt,
           sp.region_code,
           gc_module,
           SYSDATE
      FROM PARTNER_SCHEMA.partner_feed@PARTNER_LINK sp
     INNER JOIN PARTNER_SCHEMA.partner_region@PARTNER_LINK pr
        ON sp.region_id = pr.region_id
     WHERE sp.partner_dt >= p_batch_date
       AND pr.region_code = p_region
       AND sp.status = 'ACTIVE';

    v_row_count := v_row_count + SQL%ROWCOUNT;

    -- Log extraction stats
    INSERT INTO ETL.batch_log (
      log_id, batch_id, step_name, row_count, log_date, log_message
    ) VALUES (
      ETL.LOG_SEQ.NEXTVAL, p_batch_id, 'EXTRACT', v_row_count, SYSDATE,
      'Extracted ' || v_row_count || ' rows for region ' || p_region
    );

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      INSERT INTO ETL.error_log (error_id, batch_id, step_name, error_code, error_msg, error_date)
      VALUES (ETL.ERR_SEQ.NEXTVAL, p_batch_id, 'EXTRACT', SQLCODE, SQLERRM, SYSDATE);
      COMMIT;
      RAISE;
  END PC_EXTRACT_TO_STAGING;

  -- -----------------------------------------------------------------------
  -- PC_TRANSFORM_DATA
  -- Applies business rules, correlated updates, merge, and cursor processing
  -- -----------------------------------------------------------------------
  PROCEDURE PC_TRANSFORM_DATA (
    p_batch_id  IN NUMBER
  )
  IS
    CURSOR c_transform IS
      SELECT sd.staging_id,
             sd.source_key,
             sd.raw_value,
             sd.category_code,
             sd.amount,
             sd.region_code,
             rl.lookup_value AS region_name
        FROM ETL.staging_data sd
        JOIN MASTER.region_lookup rl
          ON sd.region_code = rl.region_code
       WHERE sd.batch_id = p_batch_id
         AND sd.status_code IS NULL
       ORDER BY sd.staging_id;

    v_transformed_val  VARCHAR2(200);
    v_multiplier       NUMBER;
    v_row_count        NUMBER := 0;
  BEGIN
    -- Step 1: Normalize category codes using correlated subquery
    UPDATE ETL.staging_data sd
       SET sd.category_code = (
             SELECT mc.standard_code
               FROM MASTER.category_mapping mc
              WHERE mc.source_code = sd.category_code
                AND mc.effective_from <= sd.effective_dt
                AND (mc.effective_to IS NULL OR mc.effective_to >= sd.effective_dt)
                AND ROWNUM = 1
           )
     WHERE sd.batch_id = p_batch_id
       AND EXISTS (
             SELECT 1
               FROM MASTER.category_mapping mc2
              WHERE mc2.source_code = sd.category_code
           );

    -- Step 2: Apply regional multipliers via correlated update
    UPDATE ETL.staging_data sd
       SET sd.amount = sd.amount * (
             SELECT NVL(rm.multiplier, 1)
               FROM MASTER.region_multipliers rm
              WHERE rm.region_code = sd.region_code
                AND rm.category_code = sd.category_code
           ),
           sd.status_code = 'TRANSFORMED'
     WHERE sd.batch_id = p_batch_id
       AND sd.status_code IS NULL;

    -- Step 3: Merge into transformed_data
    MERGE INTO ETL.transformed_data td
    USING (
      SELECT staging_id, source_key, raw_value, category_code,
             amount, effective_dt, region_code, batch_id
        FROM ETL.staging_data
       WHERE batch_id = p_batch_id
         AND status_code = 'TRANSFORMED'
    ) src
    ON (td.source_key = src.source_key AND td.effective_dt = src.effective_dt)
    WHEN MATCHED THEN
      UPDATE SET td.raw_value     = src.raw_value,
                 td.category_code = src.category_code,
                 td.amount        = src.amount,
                 td.batch_id      = src.batch_id,
                 td.updated_date  = SYSDATE
    WHEN NOT MATCHED THEN
      INSERT (transform_id, source_key, raw_value, category_code,
              amount, effective_dt, region_code, batch_id, created_date)
      VALUES (ETL.TRANSFORM_SEQ.NEXTVAL, src.source_key, src.raw_value,
              src.category_code, src.amount, src.effective_dt,
              src.region_code, src.batch_id, SYSDATE);

    -- Step 4: Cursor FOR loop with CASE logic for complex transformations
    FOR rec IN c_transform LOOP
      v_transformed_val := CASE rec.category_code
                             WHEN 'CAT_A' THEN UPPER(rec.raw_value)
                             WHEN 'CAT_B' THEN LOWER(rec.raw_value)
                             WHEN 'CAT_C' THEN INITCAP(rec.raw_value)
                             ELSE rec.raw_value
                           END;

      v_multiplier := CASE
                        WHEN rec.amount > 10000 THEN 0.95
                        WHEN rec.amount > 5000  THEN 0.97
                        ELSE 1.0
                      END;

      UPDATE ETL.transformed_data
         SET processed_val = v_transformed_val,
             final_amount  = rec.amount * v_multiplier,
             region_name   = rec.region_name,
             status_code   = 'READY'
       WHERE source_key    = rec.source_key
         AND batch_id      = p_batch_id;

      v_row_count := v_row_count + 1;
    END LOOP;

    INSERT INTO ETL.batch_log (log_id, batch_id, step_name, row_count, log_date, log_message)
    VALUES (ETL.LOG_SEQ.NEXTVAL, p_batch_id, 'TRANSFORM', v_row_count, SYSDATE,
            'Transformed ' || v_row_count || ' rows');

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      INSERT INTO ETL.error_log (error_id, batch_id, step_name, error_code, error_msg, error_date)
      VALUES (ETL.ERR_SEQ.NEXTVAL, p_batch_id, 'TRANSFORM', SQLCODE, SQLERRM, SYSDATE);
      COMMIT;
      RAISE;
  END PC_TRANSFORM_DATA;

  -- -----------------------------------------------------------------------
  -- PC_LOAD_TARGET
  -- Bulk collects from transformed_data and FORALL inserts to target
  -- -----------------------------------------------------------------------
  PROCEDURE PC_LOAD_TARGET (
    p_batch_id  IN NUMBER
  )
  IS
    CURSOR c_load IS
      SELECT td.transform_id,
             td.source_key,
             td.processed_val,
             td.final_amount,
             td.category_code,
             td.region_code,
             td.effective_dt
        FROM ETL.transformed_data td
       WHERE td.batch_id   = p_batch_id
         AND td.status_code = 'READY';

    TYPE t_load_tab IS TABLE OF c_load%ROWTYPE;
    v_load_data   t_load_tab;
    v_total_rows  NUMBER := 0;
    v_error_count NUMBER := 0;
  BEGIN
    OPEN c_load;
    LOOP
      FETCH c_load BULK COLLECT INTO v_load_data LIMIT gc_batch_limit;
      EXIT WHEN v_load_data.COUNT = 0;

      BEGIN
        FORALL i IN 1 .. v_load_data.COUNT SAVE EXCEPTIONS
          INSERT INTO WAREHOUSE.target_table (
            target_id, batch_id, source_key, processed_val,
            final_amount, category_code, region_code,
            effective_dt, load_date
          ) VALUES (
            WAREHOUSE.TARGET_SEQ.NEXTVAL,
            p_batch_id,
            v_load_data(i).source_key,
            v_load_data(i).processed_val,
            v_load_data(i).final_amount,
            v_load_data(i).category_code,
            v_load_data(i).region_code,
            v_load_data(i).effective_dt,
            SYSDATE
          );

        v_total_rows := v_total_rows + v_load_data.COUNT;

      EXCEPTION
        WHEN OTHERS THEN
          -- Handle SAVE EXCEPTIONS bulk errors
          v_error_count := SQL%BULK_EXCEPTIONS.COUNT;
          FOR j IN 1 .. v_error_count LOOP
            INSERT INTO ETL.load_errors (
              error_id, batch_id, source_key, error_index,
              error_code, error_date
            ) VALUES (
              ETL.ERR_SEQ.NEXTVAL,
              p_batch_id,
              v_load_data(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).source_key,
              SQL%BULK_EXCEPTIONS(j).ERROR_INDEX,
              SQL%BULK_EXCEPTIONS(j).ERROR_CODE,
              SYSDATE
            );
          END LOOP;
          v_total_rows := v_total_rows + v_load_data.COUNT - v_error_count;
      END;

      COMMIT;
    END LOOP;
    CLOSE c_load;

    -- Mark transformed rows as loaded
    UPDATE ETL.transformed_data
       SET status_code = 'LOADED', updated_date = SYSDATE
     WHERE batch_id = p_batch_id AND status_code = 'READY';

    INSERT INTO ETL.batch_log (log_id, batch_id, step_name, row_count, log_date, log_message)
    VALUES (ETL.LOG_SEQ.NEXTVAL, p_batch_id, 'LOAD', v_total_rows, SYSDATE,
            'Loaded ' || v_total_rows || ' rows, ' || v_error_count || ' errors');

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      IF c_load%ISOPEN THEN
        CLOSE c_load;
      END IF;
      ROLLBACK;
      INSERT INTO ETL.error_log (error_id, batch_id, step_name, error_code, error_msg, error_date)
      VALUES (ETL.ERR_SEQ.NEXTVAL, p_batch_id, 'LOAD', SQLCODE, SQLERRM, SYSDATE);
      COMMIT;
      RAISE;
  END PC_LOAD_TARGET;

  -- -----------------------------------------------------------------------
  -- PC_CLEANUP
  -- Removes staging data and logs the cleanup step
  -- -----------------------------------------------------------------------
  PROCEDURE PC_CLEANUP (
    p_batch_id  IN NUMBER
  )
  IS
    v_deleted NUMBER;
  BEGIN
    DELETE FROM ETL.staging_data
     WHERE batch_id = p_batch_id;

    v_deleted := SQL%ROWCOUNT;

    DELETE FROM ETL.transformed_data
     WHERE batch_id   = p_batch_id
       AND status_code = 'LOADED';

    v_deleted := v_deleted + SQL%ROWCOUNT;

    INSERT INTO AUDIT.audit_log (
      audit_id, audit_action, table_name, batch_id,
      row_count, audit_date, audit_user
    ) VALUES (
      AUDIT.AUDIT_SEQ.NEXTVAL, 'CLEANUP', 'staging_data/transformed_data',
      p_batch_id, v_deleted, SYSDATE, USER
    );

    INSERT INTO ETL.batch_log (log_id, batch_id, step_name, row_count, log_date, log_message)
    VALUES (ETL.LOG_SEQ.NEXTVAL, p_batch_id, 'CLEANUP', v_deleted, SYSDATE,
            'Cleaned up ' || v_deleted || ' staging/transform rows');

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      INSERT INTO ETL.error_log (error_id, batch_id, step_name, error_code, error_msg, error_date)
      VALUES (ETL.ERR_SEQ.NEXTVAL, p_batch_id, 'CLEANUP', SQLCODE, SQLERRM, SYSDATE);
      COMMIT;
      RAISE;
  END PC_CLEANUP;

  -- -----------------------------------------------------------------------
  -- PC_LOG_ERROR_AUTONOMOUS
  -- Autonomous transaction error logger used by orchestrator
  -- -----------------------------------------------------------------------
  PROCEDURE PC_LOG_ERROR_AUTONOMOUS (
    p_batch_id   IN NUMBER,
    p_step_name  IN VARCHAR2,
    p_error_code IN NUMBER,
    p_error_msg  IN VARCHAR2
  )
  IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    INSERT INTO ETL.error_log (
      error_id, batch_id, step_name, error_code, error_msg, error_date
    ) VALUES (
      ETL.ERR_SEQ.NEXTVAL, p_batch_id, p_step_name,
      p_error_code, p_error_msg, SYSDATE
    );
    COMMIT;
  END PC_LOG_ERROR_AUTONOMOUS;

  -- -----------------------------------------------------------------------
  -- PC_ORCHESTRATE
  -- Main procedure: calls extract, transform, load, cleanup in sequence
  -- -----------------------------------------------------------------------
  PROCEDURE PC_ORCHESTRATE (
    p_batch_date  IN DATE,
    p_region      IN VARCHAR2,
    p_status      OUT VARCHAR2
  )
  IS
    v_batch_id  NUMBER;
    v_step      VARCHAR2(30);
  BEGIN
    p_status := 'SUCCESS';

    -- Step 1: Extract
    BEGIN
      v_step := 'EXTRACT';
      PC_EXTRACT_TO_STAGING(
        p_batch_date => p_batch_date,
        p_region     => p_region,
        p_batch_id   => v_batch_id
      );
    EXCEPTION
      WHEN OTHERS THEN
        PC_LOG_ERROR_AUTONOMOUS(v_batch_id, v_step, SQLCODE, SQLERRM);
        p_status := 'FAILED_AT_' || v_step;
        RETURN;
    END;

    -- Step 2: Transform
    BEGIN
      v_step := 'TRANSFORM';
      PC_TRANSFORM_DATA(p_batch_id => v_batch_id);
    EXCEPTION
      WHEN OTHERS THEN
        PC_LOG_ERROR_AUTONOMOUS(v_batch_id, v_step, SQLCODE, SQLERRM);
        p_status := 'FAILED_AT_' || v_step;
        RETURN;
    END;

    -- Step 3: Load
    BEGIN
      v_step := 'LOAD';
      PC_LOAD_TARGET(p_batch_id => v_batch_id);
    EXCEPTION
      WHEN OTHERS THEN
        PC_LOG_ERROR_AUTONOMOUS(v_batch_id, v_step, SQLCODE, SQLERRM);
        p_status := 'FAILED_AT_' || v_step;
        RETURN;
    END;

    -- Step 4: Cleanup
    BEGIN
      v_step := 'CLEANUP';
      PC_CLEANUP(p_batch_id => v_batch_id);
    EXCEPTION
      WHEN OTHERS THEN
        PC_LOG_ERROR_AUTONOMOUS(v_batch_id, v_step, SQLCODE, SQLERRM);
        p_status := 'FAILED_AT_' || v_step;
        RETURN;
    END;

    -- Final status log
    INSERT INTO ETL.batch_log (log_id, batch_id, step_name, row_count, log_date, log_message)
    VALUES (ETL.LOG_SEQ.NEXTVAL, v_batch_id, 'ORCHESTRATE', 0, SYSDATE,
            'Batch completed with status: ' || p_status);
    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      PC_LOG_ERROR_AUTONOMOUS(v_batch_id, 'ORCHESTRATE', SQLCODE, SQLERRM);
      p_status := 'FATAL_ERROR';
  END PC_ORCHESTRATE;

END PKG_ETL_PROCESSOR;
/
