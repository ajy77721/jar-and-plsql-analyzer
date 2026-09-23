CREATE OR REPLACE PACKAGE BODY PKG_CONTROL_FLOW_TEST AS

    -- 1. IF / ELSIF / ELSE with different table accesses per branch
    PROCEDURE PC_IF_ELSIF_ELSE (p_category VARCHAR2) IS
        v_count NUMBER;
    BEGIN
        IF p_category = 'PREMIUM' THEN
            SELECT COUNT(*) INTO v_count FROM premium_customers WHERE active = 'Y';
            INSERT INTO premium_report (category, total, report_date)
            VALUES (p_category, v_count, SYSDATE);
        ELSIF p_category = 'STANDARD' THEN
            SELECT COUNT(*) INTO v_count FROM standard_customers WHERE active = 'Y';
            UPDATE report_summary SET standard_count = v_count WHERE report_month = TRUNC(SYSDATE, 'MM');
        ELSIF p_category = 'TRIAL' THEN
            SELECT COUNT(*) INTO v_count FROM trial_customers WHERE signup_date >= TRUNC(SYSDATE) - 30;
            DELETE FROM expired_trials WHERE expiry_date < TRUNC(SYSDATE);
        ELSE
            SELECT COUNT(*) INTO v_count FROM all_customers;
            INSERT INTO audit_log (action, detail, created_at)
            VALUES ('UNKNOWN_CATEGORY', p_category, SYSTIMESTAMP);
        END IF;
        COMMIT;
    END PC_IF_ELSIF_ELSE;

    -- 2. Simple CASE statement
    PROCEDURE PC_CASE_STATEMENT (p_type VARCHAR2) IS
        v_id NUMBER;
    BEGIN
        SELECT gen_seq.NEXTVAL INTO v_id FROM DUAL;

        CASE p_type
            WHEN 'A' THEN
                INSERT INTO table_a (id, type_code, created_at)
                VALUES (v_id, 'A', SYSDATE);
            WHEN 'B' THEN
                INSERT INTO table_b (id, type_code, created_at)
                VALUES (v_id, 'B', SYSDATE);
            WHEN 'C' THEN
                INSERT INTO table_c (id, type_code, created_at)
                VALUES (v_id, 'C', SYSDATE);
            ELSE
                INSERT INTO table_default (id, type_code, created_at)
                VALUES (v_id, p_type, SYSDATE);
        END CASE;
        COMMIT;
    END PC_CASE_STATEMENT;

    -- 3. Searched CASE
    PROCEDURE PC_SEARCHED_CASE (p_amount NUMBER) IS
        v_tier VARCHAR2(20);
    BEGIN
        CASE
            WHEN p_amount >= 100000 THEN
                v_tier := 'PLATINUM';
                UPDATE customer_tier SET tier_level = v_tier WHERE customer_id = 1;
            WHEN p_amount >= 50000 THEN
                v_tier := 'GOLD';
                UPDATE customer_tier SET tier_level = v_tier WHERE customer_id = 1;
            WHEN p_amount >= 10000 THEN
                v_tier := 'SILVER';
                UPDATE customer_tier SET tier_level = v_tier WHERE customer_id = 1;
            ELSE
                v_tier := 'BRONZE';
                INSERT INTO low_value_customers (customer_id, tier, amount, flagged_at)
                VALUES (1, v_tier, p_amount, SYSDATE);
        END CASE;

        INSERT INTO tier_history (customer_id, new_tier, amount, change_date)
        VALUES (1, v_tier, p_amount, SYSDATE);
        COMMIT;
    END PC_SEARCHED_CASE;

    -- 4. Basic LOOP with EXIT WHEN
    PROCEDURE PC_LOOP_BASIC IS
        v_counter  NUMBER := 0;
        v_max      NUMBER;
        v_status   VARCHAR2(20);
    BEGIN
        SELECT COUNT(*) INTO v_max FROM pending_jobs WHERE job_status = 'QUEUED';

        LOOP
            EXIT WHEN v_counter >= v_max;

            SELECT job_status INTO v_status
              FROM pending_jobs
             WHERE ROWNUM = 1 AND job_status = 'QUEUED';

            UPDATE pending_jobs
               SET job_status   = 'PROCESSING',
                   started_at   = SYSTIMESTAMP
             WHERE job_id = (
                SELECT MIN(job_id) FROM pending_jobs WHERE job_status = 'QUEUED'
             );

            v_counter := v_counter + 1;
            EXIT WHEN v_status = 'DONE';
        END LOOP;

        INSERT INTO job_run_log (run_type, jobs_processed, completed_at)
        VALUES ('BASIC_LOOP', v_counter, SYSTIMESTAMP);
        COMMIT;
    END PC_LOOP_BASIC;

    -- 5. WHILE LOOP
    PROCEDURE PC_WHILE_LOOP IS
        v_found  BOOLEAN := TRUE;
        v_batch  NUMBER  := 0;
        v_cnt    NUMBER;
    BEGIN
        WHILE v_found LOOP
            SELECT COUNT(*) INTO v_cnt
              FROM unprocessed_records
             WHERE batch_flag = 'N'
               AND ROWNUM <= 100;

            IF v_cnt = 0 THEN
                v_found := FALSE;
            ELSE
                UPDATE unprocessed_records
                   SET batch_flag  = 'Y',
                       batch_num   = v_batch,
                       processed_at = SYSTIMESTAMP
                 WHERE batch_flag = 'N'
                   AND ROWNUM <= 100;

                v_batch := v_batch + 1;
            END IF;
        END LOOP;

        INSERT INTO batch_log (total_batches, completed_at)
        VALUES (v_batch, SYSTIMESTAMP);
        COMMIT;
    END PC_WHILE_LOOP;

    -- 6. FOR LOOP
    PROCEDURE PC_FOR_LOOP (p_dept_id NUMBER) IS
        v_emp_id NUMBER;
    BEGIN
        FOR i IN 1..10 LOOP
            SELECT emp_seq.NEXTVAL INTO v_emp_id FROM DUAL;

            INSERT INTO employees (employee_id, first_name, last_name, dept_id, hire_date)
            VALUES (v_emp_id, 'First_' || i, 'Last_' || i, p_dept_id, SYSDATE);
        END LOOP;

        UPDATE departments
           SET employee_count = employee_count + 10
         WHERE department_id = p_dept_id;

        COMMIT;
    END PC_FOR_LOOP;

    -- 7. Triple nested loops with SQL in each
    PROCEDURE PC_NESTED_LOOPS IS
        v_val    NUMBER;
        v_found  BOOLEAN;
        v_idx    NUMBER;
    BEGIN
        FOR i IN 1..5 LOOP
            INSERT INTO outer_log (iteration, started_at) VALUES (i, SYSTIMESTAMP);

            v_found := TRUE;
            v_idx   := 0;

            WHILE v_found AND v_idx < 3 LOOP
                SELECT COUNT(*) INTO v_val
                  FROM work_items
                 WHERE category = i AND sub_category = v_idx;

                v_idx := v_idx + 1;

                LOOP
                    UPDATE work_items
                       SET status = 'DONE'
                     WHERE category = i
                       AND sub_category = v_idx
                       AND ROWNUM = 1;

                    EXIT WHEN SQL%ROWCOUNT = 0;

                    INSERT INTO work_audit (category, sub_cat, completed_at)
                    VALUES (i, v_idx, SYSTIMESTAMP);
                END LOOP;

                IF v_val = 0 THEN
                    v_found := FALSE;
                END IF;
            END WHILE;
        END LOOP;
        COMMIT;
    END PC_NESTED_LOOPS;

    -- 8. Labeled loops with CONTINUE and EXIT
    PROCEDURE PC_LABELED_LOOPS IS
        v_total NUMBER := 0;
        v_skip  BOOLEAN;
    BEGIN
        <<outer_loop>>
        FOR i IN 1..10 LOOP
            SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
              INTO v_total
              FROM skip_list
             WHERE item_id = i;

            CONTINUE outer_loop WHEN v_total = 1;

            <<inner_loop>>
            FOR j IN 1..5 LOOP
                EXIT inner_loop WHEN j > 3 AND i > 7;

                INSERT INTO matrix_data (row_id, col_id, cell_value, created_at)
                VALUES (i, j, i * j, SYSDATE);
            END LOOP inner_loop;

            UPDATE matrix_summary
               SET row_complete = 'Y'
             WHERE row_id = i;
        END LOOP outer_loop;
        COMMIT;
    END PC_LABELED_LOOPS;

    -- 9. Complex exception handling
    PROCEDURE PC_EXCEPTION_COMPLEX (p_emp_id NUMBER) IS
        v_name   VARCHAR2(100);
        v_salary NUMBER;
    BEGIN
        SELECT first_name, salary
          INTO v_name, v_salary
          FROM employees
         WHERE employee_id = p_emp_id;

        UPDATE salary_cache
           SET cached_name   = v_name,
               cached_salary = v_salary,
               cached_at     = SYSTIMESTAMP
         WHERE employee_id = p_emp_id;

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            INSERT INTO missing_employees (employee_id, checked_at)
            VALUES (p_emp_id, SYSTIMESTAMP);
        WHEN TOO_MANY_ROWS THEN
            INSERT INTO data_anomalies (table_name, key_value, anomaly_type, detected_at)
            VALUES ('EMPLOYEES', TO_CHAR(p_emp_id), 'DUPLICATE', SYSTIMESTAMP);
        WHEN DUP_VAL_ON_INDEX THEN
            UPDATE salary_cache
               SET cached_name   = v_name,
                   cached_salary = v_salary,
                   cached_at     = SYSTIMESTAMP
             WHERE employee_id = p_emp_id;
        WHEN OTHERS THEN
            INSERT INTO error_log (error_code, error_msg, context_info, logged_at)
            VALUES (SQLCODE, SQLERRM, 'PC_EXCEPTION_COMPLEX:' || p_emp_id, SYSTIMESTAMP);
            RAISE_APPLICATION_ERROR(-20001, 'Unexpected error for emp ' || p_emp_id);
    END PC_EXCEPTION_COMPLEX;

    -- 10. Nested BEGIN/EXCEPTION/END blocks, 3 levels deep
    PROCEDURE PC_EXCEPTION_NESTED (p_order_id NUMBER) IS
        v_cust_id   NUMBER;
        v_addr_id   NUMBER;
        v_status    VARCHAR2(30);
    BEGIN
        -- Level 1
        BEGIN
            SELECT customer_id INTO v_cust_id
              FROM orders
             WHERE order_id = p_order_id;

            -- Level 2
            BEGIN
                SELECT address_id INTO v_addr_id
                  FROM customer_addresses
                 WHERE customer_id = v_cust_id
                   AND address_type = 'SHIPPING'
                   AND ROWNUM = 1;

                -- Level 3
                BEGIN
                    UPDATE shipments
                       SET ship_address_id = v_addr_id,
                           status          = 'READY'
                     WHERE order_id = p_order_id;

                    INSERT INTO shipment_log (order_id, address_id, action, logged_at)
                    VALUES (p_order_id, v_addr_id, 'ADDR_SET', SYSTIMESTAMP);
                EXCEPTION
                    WHEN OTHERS THEN
                        INSERT INTO error_log (error_code, error_msg, context_info, logged_at)
                        VALUES (SQLCODE, SQLERRM, 'SHIPMENT_UPDATE:' || p_order_id, SYSTIMESTAMP);
                END;

            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    INSERT INTO missing_address_log (customer_id, order_id, logged_at)
                    VALUES (v_cust_id, p_order_id, SYSTIMESTAMP);
                    v_addr_id := NULL;
            END;

        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                INSERT INTO orphan_orders (order_id, detected_at)
                VALUES (p_order_id, SYSTIMESTAMP);
        END;

        COMMIT;
    END PC_EXCEPTION_NESTED;

END PKG_CONTROL_FLOW_TEST;
/
