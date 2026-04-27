CREATE OR REPLACE PACKAGE BODY PKG_SEQUENCE_TEST AS

    -- 1. NEXTVAL assigned to variable
    PROCEDURE PC_SEQ_NEXTVAL_ASSIGN IS
        v_id      NUMBER;
        v_prev_id NUMBER;
    BEGIN
        SELECT NVL(MAX(val_obtained), 0)
          INTO v_prev_id
          FROM sequence_usage_log
         WHERE seq_name = 'MY_SEQ';

        v_id := MY_SEQ.NEXTVAL;

        INSERT INTO sequence_usage_log (log_id, seq_name, val_obtained, prev_val, obtained_at)
        VALUES (log_seq.NEXTVAL, 'MY_SEQ', v_id, v_prev_id, SYSTIMESTAMP);

        UPDATE sequence_stats
           SET last_value   = v_id,
               access_count = access_count + 1,
               last_access  = SYSTIMESTAMP
         WHERE seq_name = 'MY_SEQ';

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            INSERT INTO error_log (error_code, error_msg, context_info, logged_at)
            VALUES (SQLCODE, SQLERRM, 'PC_SEQ_NEXTVAL_ASSIGN', SYSTIMESTAMP);
            COMMIT;
    END PC_SEQ_NEXTVAL_ASSIGN;

    -- 2. CURRVAL usage after NEXTVAL in master-detail pattern
    PROCEDURE PC_SEQ_CURRVAL IS
        v_id      NUMBER;
        v_curr_id NUMBER;
        v_count   NUMBER;
    BEGIN
        v_id := MY_SEQ.NEXTVAL;

        INSERT INTO master_records (record_id, record_name, created_at)
        VALUES (v_id, 'Master record', SYSDATE);

        v_curr_id := MY_SEQ.CURRVAL;

        INSERT INTO detail_records (detail_id, master_id, detail_info, created_at)
        VALUES (detail_seq.NEXTVAL, v_curr_id, 'Detail A for master ' || v_curr_id, SYSDATE);

        INSERT INTO detail_records (detail_id, master_id, detail_info, created_at)
        VALUES (detail_seq.NEXTVAL, v_curr_id, 'Detail B for master ' || v_curr_id, SYSDATE);

        INSERT INTO detail_records (detail_id, master_id, detail_info, created_at)
        VALUES (detail_seq.NEXTVAL, v_curr_id, 'Detail C for master ' || v_curr_id, SYSDATE);

        SELECT COUNT(*) INTO v_count
          FROM detail_records
         WHERE master_id = v_curr_id;

        INSERT INTO sequence_audit (seq_name, last_nextval, last_currval, detail_count, audited_at)
        VALUES ('MY_SEQ', v_id, v_curr_id, v_count, SYSTIMESTAMP);

        COMMIT;
    END PC_SEQ_CURRVAL;

    -- 3. Schema-qualified sequence references
    PROCEDURE PC_SEQ_SCHEMA_QUALIFIED IS
        v_id        NUMBER;
        v_ref_id    NUMBER;
        v_ship_id   NUMBER;
    BEGIN
        v_id := CUSTOMER.ORDER_SEQ.NEXTVAL;

        INSERT INTO CUSTOMER.orders (order_id, order_date, status, created_by)
        VALUES (v_id, SYSDATE, 'NEW', USER);

        v_ref_id := FINANCE.INVOICE_SEQ.NEXTVAL;

        INSERT INTO FINANCE.invoices (invoice_id, order_id, invoice_date, amount, currency)
        VALUES (v_ref_id, v_id, SYSDATE, 0, 'USD');

        v_ship_id := LOGISTICS.SHIPMENT_SEQ.NEXTVAL;

        INSERT INTO LOGISTICS.shipments (shipment_id, order_id, status, created_at)
        VALUES (v_ship_id, v_id, 'PENDING', SYSTIMESTAMP);

        INSERT INTO cross_schema_audit (audit_id, order_id, invoice_id, shipment_id, created_at)
        VALUES (CUSTOMER.AUDIT_SEQ.NEXTVAL, v_id, v_ref_id, v_ship_id, SYSTIMESTAMP);

        COMMIT;
    END PC_SEQ_SCHEMA_QUALIFIED;

    -- 4. Sequence in INSERT VALUES clause (multiple tables)
    PROCEDURE PC_SEQ_IN_INSERT (p_name VARCHAR2, p_amount NUMBER) IS
        v_order_id NUMBER;
    BEGIN
        INSERT INTO orders (id, customer_name, order_amount, order_date)
        VALUES (ORDER_SEQ.NEXTVAL, p_name, p_amount, SYSDATE);

        v_order_id := ORDER_SEQ.CURRVAL;

        INSERT INTO order_items (item_id, order_id, item_desc, quantity, unit_price)
        VALUES (ITEM_SEQ.NEXTVAL, v_order_id, 'Primary item', 1, p_amount * 0.8);

        INSERT INTO order_items (item_id, order_id, item_desc, quantity, unit_price)
        VALUES (ITEM_SEQ.NEXTVAL, v_order_id, 'Tax and fees', 1, p_amount * 0.2);

        INSERT INTO notifications (notif_id, order_id, message, sent_at)
        VALUES (NOTIF_SEQ.NEXTVAL, v_order_id, 'New order placed by ' || p_name, SYSTIMESTAMP);

        INSERT INTO order_status_history (history_id, order_id, status, changed_at)
        VALUES (STATUS_HIST_SEQ.NEXTVAL, v_order_id, 'CREATED', SYSTIMESTAMP);

        COMMIT;
    END PC_SEQ_IN_INSERT;

    -- 5. Sequence via SELECT from DUAL
    PROCEDURE PC_SEQ_IN_SELECT_DUAL IS
        v_batch_id   NUMBER;
        v_txn_id     NUMBER;
        v_session_id NUMBER;
    BEGIN
        SELECT BATCH_SEQ.NEXTVAL INTO v_batch_id FROM DUAL;

        SELECT TXN_SEQ.NEXTVAL INTO v_txn_id FROM DUAL;

        SELECT SESSION_SEQ.NEXTVAL INTO v_session_id FROM DUAL;

        INSERT INTO batch_jobs (batch_id, txn_id, session_id, status, created_at)
        VALUES (v_batch_id, v_txn_id, v_session_id, 'INITIATED', SYSTIMESTAMP);

        UPDATE batch_control
           SET last_batch_id  = v_batch_id,
               last_txn_id    = v_txn_id,
               last_run_at    = SYSTIMESTAMP
         WHERE control_name = 'MAIN_BATCH';

        INSERT INTO batch_session_map (batch_id, session_id, mapped_at)
        VALUES (v_batch_id, v_session_id, SYSTIMESTAMP);

        COMMIT;
    END PC_SEQ_IN_SELECT_DUAL;

    -- 6. Sequence in INSERT ... SELECT (bulk generation)
    PROCEDURE PC_SEQ_IN_INSERT_SELECT IS
        v_count       NUMBER;
        v_archive_run NUMBER;
    BEGIN
        SELECT ARCHIVE_RUN_SEQ.NEXTVAL INTO v_archive_run FROM DUAL;

        INSERT INTO archive (id, emp_id, emp_name, archive_run_id, archived_at)
        SELECT ARCHIVE_SEQ.NEXTVAL,
               employee_id,
               first_name || ' ' || last_name,
               v_archive_run,
               SYSDATE
          FROM employees
         WHERE termination_date IS NOT NULL
           AND termination_date < ADD_MONTHS(SYSDATE, -24);

        v_count := SQL%ROWCOUNT;

        INSERT INTO archive_details (detail_id, archive_run_id, dept_id, archived_count)
        SELECT ARCHIVE_DETAIL_SEQ.NEXTVAL,
               v_archive_run,
               dept_id,
               COUNT(*)
          FROM employees
         WHERE termination_date IS NOT NULL
           AND termination_date < ADD_MONTHS(SYSDATE, -24)
         GROUP BY dept_id;

        INSERT INTO archive_log (log_id, archive_run_id, records_archived, archive_date)
        VALUES (ARCHIVE_LOG_SEQ.NEXTVAL, v_archive_run, v_count, SYSDATE);

        COMMIT;
    END PC_SEQ_IN_INSERT_SELECT;

    -- 7. Sequence in UPDATE statement
    PROCEDURE PC_SEQ_IN_UPDATE IS
        v_affected   NUMBER;
        v_run_id     NUMBER;
    BEGIN
        v_run_id := BATCH_RUN_SEQ.NEXTVAL;

        UPDATE batches
           SET batch_id    = BATCH_SEQ.NEXTVAL,
               run_id      = v_run_id,
               assigned_at = SYSTIMESTAMP
         WHERE status = 'NEW'
           AND batch_id IS NULL;

        v_affected := SQL%ROWCOUNT;

        UPDATE batch_run_stats
           SET last_assignment_count = v_affected,
               last_run_id          = v_run_id,
               last_run_at          = SYSTIMESTAMP
         WHERE stat_type = 'BATCH_ASSIGN';

        IF v_affected > 0 THEN
            INSERT INTO batch_assignment_log (log_id, run_id, records_assigned, assigned_at)
            VALUES (BATCH_LOG_SEQ.NEXTVAL, v_run_id, v_affected, SYSTIMESTAMP);
        ELSE
            INSERT INTO batch_skip_log (log_id, run_id, reason, logged_at)
            VALUES (BATCH_LOG_SEQ.NEXTVAL, v_run_id, 'NO_NEW_RECORDS', SYSTIMESTAMP);
        END IF;

        COMMIT;
    END PC_SEQ_IN_UPDATE;

    -- 8. Sequence in expression (string concatenation and formatting)
    PROCEDURE PC_SEQ_IN_EXPRESSION IS
        v_ref        VARCHAR2(50);
        v_barcode    VARCHAR2(100);
        v_ticket_num VARCHAR2(30);
        v_id         NUMBER;
    BEGIN
        v_ref := 'REF-' || LPAD(REF_SEQ.NEXTVAL, 9, '0');

        v_id := ORDER_SEQ.NEXTVAL;
        v_barcode := 'BC-' || TO_CHAR(SYSDATE, 'YYYYMMDD') || '-' || LPAD(v_id, 6, '0');

        v_ticket_num := 'TKT' || TO_CHAR(SYSDATE, 'YYYY') || '-' || LPAD(TICKET_SEQ.NEXTVAL, 7, '0');

        INSERT INTO reference_codes (ref_code, barcode, ticket_num, generated_at)
        VALUES (v_ref, v_barcode, v_ticket_num, SYSTIMESTAMP);

        INSERT INTO code_generation_log (log_id, ref_code, barcode, ticket_num, created_at)
        VALUES (CODE_LOG_SEQ.NEXTVAL, v_ref, v_barcode, v_ticket_num, SYSTIMESTAMP);

        UPDATE code_counters
           SET last_ref_code    = v_ref,
               last_barcode     = v_barcode,
               last_ticket      = v_ticket_num,
               generation_count = generation_count + 1,
               last_generated   = SYSTIMESTAMP
         WHERE counter_group = 'PRIMARY';

        COMMIT;
    END PC_SEQ_IN_EXPRESSION;

END PKG_SEQUENCE_TEST;
/
