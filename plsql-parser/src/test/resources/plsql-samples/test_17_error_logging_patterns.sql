CREATE OR REPLACE PACKAGE BODY UTIL.PKG_ERROR_HANDLER AS
  ---------------------------------------------------------------------------
  -- Error Logging Patterns Package
  -- Covers: retry loops, SAVE EXCEPTIONS, user-defined exceptions,
  --         PRAGMA EXCEPTION_INIT, AUTONOMOUS_TRANSACTION, nested exceptions,
  --         RAISE_APPLICATION_ERROR, SQL%BULK_EXCEPTIONS
  ---------------------------------------------------------------------------

  gc_module       CONSTANT VARCHAR2(30) := 'PKG_ERROR_HANDLER';
  gc_max_retries  CONSTANT PLS_INTEGER  := 3;

  -- -----------------------------------------------------------------------
  -- PC_PROCESS_WITH_RETRY
  -- Nested BEGIN/EXCEPTION with retry counter in a WHILE loop
  -- -----------------------------------------------------------------------
  PROCEDURE PC_PROCESS_WITH_RETRY (
    p_batch_id  IN NUMBER,
    p_status    OUT VARCHAR2
  )
  IS
    v_retry     PLS_INTEGER := 0;
    v_success   BOOLEAN     := FALSE;
    v_row_count NUMBER;
    v_wait_secs NUMBER;
  BEGIN
    WHILE v_retry < gc_max_retries AND NOT v_success LOOP
      BEGIN
        -- Attempt the processing operation
        UPDATE PROCESS.batch_items bi
           SET bi.status      = 'PROCESSING',
               bi.retry_count = v_retry,
               bi.updated_date = SYSDATE
         WHERE bi.batch_id   = p_batch_id
           AND bi.status     IN ('PENDING', 'RETRY');

        v_row_count := SQL%ROWCOUNT;

        IF v_row_count = 0 THEN
          p_status := 'NO_ROWS';
          RETURN;
        END IF;

        -- Perform the actual work: insert results
        INSERT INTO PROCESS.batch_results (
          result_id, batch_id, item_count, process_date, status
        )
        SELECT PROCESS.RESULT_SEQ.NEXTVAL,
               p_batch_id,
               COUNT(*),
               SYSDATE,
               'COMPLETED'
          FROM PROCESS.batch_items
         WHERE batch_id = p_batch_id
           AND status   = 'PROCESSING';

        -- Mark items as completed
        UPDATE PROCESS.batch_items
           SET status       = 'COMPLETED',
               updated_date = SYSDATE
         WHERE batch_id = p_batch_id
           AND status   = 'PROCESSING';

        COMMIT;
        v_success := TRUE;
        p_status  := 'SUCCESS';

      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK;
          v_retry := v_retry + 1;
          v_wait_secs := v_retry * 2; -- exponential-ish backoff marker

          -- Log each retry attempt
          INSERT INTO UTIL.retry_log (
            log_id, batch_id, retry_num, error_code,
            error_msg, log_date
          ) VALUES (
            UTIL.RETRY_LOG_SEQ.NEXTVAL, p_batch_id, v_retry,
            SQLCODE, SQLERRM, SYSDATE
          );
          COMMIT;

          IF v_retry >= gc_max_retries THEN
            p_status := 'FAILED_AFTER_' || gc_max_retries || '_RETRIES';
            -- Mark items for manual review
            UPDATE PROCESS.batch_items
               SET status = 'FAILED', updated_date = SYSDATE
             WHERE batch_id = p_batch_id
               AND status IN ('PENDING', 'RETRY', 'PROCESSING');
            COMMIT;
          ELSE
            -- Mark for retry
            UPDATE PROCESS.batch_items
               SET status = 'RETRY', updated_date = SYSDATE
             WHERE batch_id = p_batch_id
               AND status = 'PROCESSING';
            COMMIT;
            DBMS_LOCK.SLEEP(v_wait_secs);
          END IF;
      END;
    END LOOP;
  END PC_PROCESS_WITH_RETRY;

  -- -----------------------------------------------------------------------
  -- PC_SAVE_EXCEPTIONS_HANDLER
  -- FORALL with SAVE EXCEPTIONS and SQL%BULK_EXCEPTIONS iteration
  -- -----------------------------------------------------------------------
  PROCEDURE PC_SAVE_EXCEPTIONS_HANDLER (
    p_batch_id    IN NUMBER,
    p_error_count OUT NUMBER
  )
  IS
    TYPE t_item_tab IS TABLE OF PROCESS.batch_items%ROWTYPE;
    v_items       t_item_tab;
    v_err_idx     PLS_INTEGER;
    v_err_code    NUMBER;
    e_bulk_errors EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_bulk_errors, -24381);
  BEGIN
    p_error_count := 0;

    SELECT *
      BULK COLLECT INTO v_items
      FROM PROCESS.batch_items
     WHERE batch_id = p_batch_id
       AND status = 'PENDING';

    IF v_items.COUNT = 0 THEN
      RETURN;
    END IF;

    BEGIN
      FORALL i IN 1 .. v_items.COUNT SAVE EXCEPTIONS
        INSERT INTO PROCESS.processed_items (
          processed_id, batch_id, item_id, item_value,
          status, processed_date
        ) VALUES (
          PROCESS.PROCESSED_SEQ.NEXTVAL,
          v_items(i).batch_id,
          v_items(i).item_id,
          v_items(i).item_value,
          'PROCESSED',
          SYSDATE
        );

    EXCEPTION
      WHEN e_bulk_errors THEN
        p_error_count := SQL%BULK_EXCEPTIONS.COUNT;

        FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
          v_err_idx  := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;
          v_err_code := SQL%BULK_EXCEPTIONS(j).ERROR_CODE;

          INSERT INTO UTIL.bulk_error_log (
            log_id, batch_id, item_id, error_index,
            error_code, error_date
          ) VALUES (
            UTIL.BULK_ERR_SEQ.NEXTVAL,
            v_items(v_err_idx).batch_id,
            v_items(v_err_idx).item_id,
            v_err_idx,
            v_err_code,
            SYSDATE
          );
        END LOOP;
    END;

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      RAISE;
  END PC_SAVE_EXCEPTIONS_HANDLER;

  -- -----------------------------------------------------------------------
  -- PC_USER_DEFINED_EXCEPTION
  -- Custom exception with PRAGMA EXCEPTION_INIT and RAISE_APPLICATION_ERROR
  -- -----------------------------------------------------------------------
  PROCEDURE PC_USER_DEFINED_EXCEPTION (
    p_record_id   IN NUMBER,
    p_action_code IN VARCHAR2
  )
  IS
    e_invalid_data    EXCEPTION;
    e_duplicate_entry EXCEPTION;
    e_stale_record    EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_invalid_data,    -20100);
    PRAGMA EXCEPTION_INIT(e_duplicate_entry, -20101);
    PRAGMA EXCEPTION_INIT(e_stale_record,    -20102);

    v_count       NUMBER;
    v_status      VARCHAR2(20);
    v_last_update DATE;
  BEGIN
    -- Validate the record exists
    SELECT status, last_update_date
      INTO v_status, v_last_update
      FROM PROCESS.data_records
     WHERE record_id = p_record_id;

    -- Check for invalid data
    IF v_status = 'INVALID' THEN
      RAISE e_invalid_data;
    END IF;

    -- Check for stale record (older than 24 hours)
    IF v_last_update < SYSDATE - 1 THEN
      RAISE_APPLICATION_ERROR(-20102,
        'Record ' || p_record_id || ' is stale (last updated: ' ||
        TO_CHAR(v_last_update, 'YYYY-MM-DD HH24:MI:SS') || ')');
    END IF;

    -- Check for duplicate
    SELECT COUNT(*)
      INTO v_count
      FROM PROCESS.processed_items
     WHERE item_id = p_record_id
       AND status = 'PROCESSED';

    IF v_count > 0 THEN
      RAISE_APPLICATION_ERROR(-20101,
        'Duplicate entry for record ' || p_record_id);
    END IF;

    -- Perform the action
    UPDATE PROCESS.data_records
       SET status           = p_action_code,
           last_update_date = SYSDATE,
           updated_by       = USER
     WHERE record_id = p_record_id;

    COMMIT;

  EXCEPTION
    WHEN e_invalid_data THEN
      INSERT INTO UTIL.validation_errors (
        error_id, record_id, error_type, error_msg, error_date
      ) VALUES (
        UTIL.VAL_ERR_SEQ.NEXTVAL, p_record_id, 'INVALID_DATA',
        'Record has invalid status', SYSDATE
      );
      COMMIT;
    WHEN e_duplicate_entry THEN
      INSERT INTO UTIL.validation_errors (
        error_id, record_id, error_type, error_msg, error_date
      ) VALUES (
        UTIL.VAL_ERR_SEQ.NEXTVAL, p_record_id, 'DUPLICATE',
        'Record already processed', SYSDATE
      );
      COMMIT;
    WHEN e_stale_record THEN
      INSERT INTO UTIL.validation_errors (
        error_id, record_id, error_type, error_msg, error_date
      ) VALUES (
        UTIL.VAL_ERR_SEQ.NEXTVAL, p_record_id, 'STALE_RECORD',
        'Record is stale', SYSDATE
      );
      COMMIT;
    WHEN NO_DATA_FOUND THEN
      RAISE_APPLICATION_ERROR(-20103,
        'Record ' || p_record_id || ' not found');
  END PC_USER_DEFINED_EXCEPTION;

  -- -----------------------------------------------------------------------
  -- PC_AUTONOMOUS_LOGGER
  -- PRAGMA AUTONOMOUS_TRANSACTION for independent error logging
  -- -----------------------------------------------------------------------
  PROCEDURE PC_AUTONOMOUS_LOGGER (
    p_module     IN VARCHAR2,
    p_step       IN VARCHAR2,
    p_error_code IN NUMBER,
    p_error_msg  IN VARCHAR2,
    p_context    IN VARCHAR2 DEFAULT NULL
  )
  IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    INSERT INTO UTIL.error_log (
      log_id, module_name, step_name, error_code,
      error_message, context_info, log_timestamp,
      session_user, os_user
    ) VALUES (
      UTIL.ERROR_LOG_SEQ.NEXTVAL,
      p_module,
      p_step,
      p_error_code,
      SUBSTR(p_error_msg, 1, 4000),
      p_context,
      SYSTIMESTAMP,
      SYS_CONTEXT('USERENV', 'SESSION_USER'),
      SYS_CONTEXT('USERENV', 'OS_USER')
    );

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      -- Swallow exception: logger must never raise
      NULL;
  END PC_AUTONOMOUS_LOGGER;

  -- -----------------------------------------------------------------------
  -- PC_NESTED_EXCEPTION
  -- Three-level nested BEGIN/EXCEPTION/END with different table ops
  -- -----------------------------------------------------------------------
  PROCEDURE PC_NESTED_EXCEPTION (
    p_order_id  IN NUMBER
  )
  IS
    v_customer_id  NUMBER;
    v_total        NUMBER;
    v_status       VARCHAR2(20);
  BEGIN
    -- Level 1: Validate order
    BEGIN
      SELECT customer_id, order_total, status
        INTO v_customer_id, v_total, v_status
        FROM SALES.orders
       WHERE order_id = p_order_id;

      -- Level 2: Update inventory
      BEGIN
        UPDATE INVENTORY.stock_levels sl
           SET sl.quantity = sl.quantity - (
                 SELECT oi.quantity
                   FROM SALES.order_items oi
                  WHERE oi.order_id = p_order_id
                    AND oi.product_id = sl.product_id
               )
         WHERE sl.product_id IN (
                 SELECT product_id
                   FROM SALES.order_items
                  WHERE order_id = p_order_id
               );

        -- Level 3: Process payment
        BEGIN
          INSERT INTO FINANCE.payments (
            payment_id, order_id, customer_id, amount,
            payment_date, status
          ) VALUES (
            FINANCE.PAYMENT_SEQ.NEXTVAL, p_order_id, v_customer_id,
            v_total, SYSDATE, 'PENDING'
          );

          UPDATE SALES.orders
             SET status = 'PAYMENT_INITIATED', updated_date = SYSDATE
           WHERE order_id = p_order_id;

          COMMIT;

        EXCEPTION
          WHEN DUP_VAL_ON_INDEX THEN
            PC_AUTONOMOUS_LOGGER(gc_module, 'PAYMENT', SQLCODE,
              'Duplicate payment for order ' || p_order_id);
            UPDATE SALES.orders
               SET status = 'PAYMENT_DUPLICATE', updated_date = SYSDATE
             WHERE order_id = p_order_id;
            COMMIT;
        END; -- Level 3

      EXCEPTION
        WHEN OTHERS THEN
          PC_AUTONOMOUS_LOGGER(gc_module, 'INVENTORY', SQLCODE, SQLERRM);
          -- Rollback inventory changes but keep going
          ROLLBACK;
          UPDATE SALES.orders
             SET status = 'INVENTORY_ERROR', updated_date = SYSDATE
           WHERE order_id = p_order_id;
          COMMIT;
      END; -- Level 2

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        PC_AUTONOMOUS_LOGGER(gc_module, 'ORDER_LOOKUP', SQLCODE,
          'Order not found: ' || p_order_id);
      WHEN TOO_MANY_ROWS THEN
        PC_AUTONOMOUS_LOGGER(gc_module, 'ORDER_LOOKUP', SQLCODE,
          'Multiple orders found for ID: ' || p_order_id);
    END; -- Level 1

  EXCEPTION
    WHEN OTHERS THEN
      PC_AUTONOMOUS_LOGGER(gc_module, 'NESTED_OUTER', SQLCODE, SQLERRM);
      RAISE;
  END PC_NESTED_EXCEPTION;

  -- -----------------------------------------------------------------------
  -- PC_EXCEPTION_IN_LOOP
  -- FOR rec IN cursor LOOP with BEGIN/EXCEPTION inside the loop body
  -- -----------------------------------------------------------------------
  PROCEDURE PC_EXCEPTION_IN_LOOP (
    p_batch_id     IN NUMBER,
    p_success_cnt  OUT NUMBER,
    p_error_cnt    OUT NUMBER
  )
  IS
    CURSOR c_items IS
      SELECT item_id, item_value, category_code
        FROM PROCESS.batch_items
       WHERE batch_id = p_batch_id
         AND status = 'PENDING'
       ORDER BY item_id;

    v_lookup_val  VARCHAR2(100);
  BEGIN
    p_success_cnt := 0;
    p_error_cnt   := 0;

    FOR rec IN c_items LOOP
      BEGIN
        -- Lookup reference data
        SELECT ref_value
          INTO v_lookup_val
          FROM MASTER.reference_data
         WHERE ref_code = rec.category_code
           AND active_flag = 'Y';

        -- Process the item
        INSERT INTO PROCESS.processed_items (
          processed_id, batch_id, item_id,
          item_value, ref_value, status, processed_date
        ) VALUES (
          PROCESS.PROCESSED_SEQ.NEXTVAL, p_batch_id, rec.item_id,
          rec.item_value, v_lookup_val, 'SUCCESS', SYSDATE
        );

        UPDATE PROCESS.batch_items
           SET status = 'COMPLETED', updated_date = SYSDATE
         WHERE item_id = rec.item_id;

        p_success_cnt := p_success_cnt + 1;

      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          INSERT INTO UTIL.processing_errors (
            error_id, batch_id, item_id, error_type,
            error_msg, error_date
          ) VALUES (
            UTIL.PROC_ERR_SEQ.NEXTVAL, p_batch_id, rec.item_id,
            'NO_REF_DATA', 'No reference data for code: ' || rec.category_code,
            SYSDATE
          );
          p_error_cnt := p_error_cnt + 1;

        WHEN DUP_VAL_ON_INDEX THEN
          INSERT INTO UTIL.processing_errors (
            error_id, batch_id, item_id, error_type,
            error_msg, error_date
          ) VALUES (
            UTIL.PROC_ERR_SEQ.NEXTVAL, p_batch_id, rec.item_id,
            'DUPLICATE', 'Item already processed: ' || rec.item_id,
            SYSDATE
          );
          p_error_cnt := p_error_cnt + 1;

        WHEN OTHERS THEN
          PC_AUTONOMOUS_LOGGER(gc_module, 'LOOP_ITEM_' || rec.item_id,
            SQLCODE, SQLERRM);
          p_error_cnt := p_error_cnt + 1;
      END;

      -- Commit every 100 rows
      IF MOD(p_success_cnt + p_error_cnt, 100) = 0 THEN
        COMMIT;
      END IF;
    END LOOP;

    COMMIT;

  END PC_EXCEPTION_IN_LOOP;

END PKG_ERROR_HANDLER;
/
