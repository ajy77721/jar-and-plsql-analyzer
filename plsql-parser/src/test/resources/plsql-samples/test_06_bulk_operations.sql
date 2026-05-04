CREATE OR REPLACE PACKAGE BODY PKG_BULK_OPS_TEST AS

    -- Package-level type for batch records
    gc_batch_size CONSTANT NUMBER := 1000;

    PROCEDURE PC_FORALL_INSERT IS
        TYPE t_id_tab IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
        TYPE t_name_tab IS TABLE OF VARCHAR2(100) INDEX BY PLS_INTEGER;
        TYPE t_date_tab IS TABLE OF DATE INDEX BY PLS_INTEGER;
        TYPE t_sal_tab IS TABLE OF NUMBER INDEX BY PLS_INTEGER;

        l_ids      t_id_tab;
        l_names    t_name_tab;
        l_dates    t_date_tab;
        l_salaries t_sal_tab;
        v_before   NUMBER;
        v_after    NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_before FROM target_table;

        SELECT employee_id, last_name, hire_date, salary
          BULK COLLECT INTO l_ids, l_names, l_dates, l_salaries
          FROM employees
         WHERE department_id = 10
           AND status = 'ACTIVE';

        FORALL i IN 1..l_ids.COUNT
            INSERT INTO target_table (id, name, created_date, amount)
            VALUES (l_ids(i), l_names(i), l_dates(i), l_salaries(i));

        SELECT COUNT(*) INTO v_after FROM target_table;

        INSERT INTO batch_log (operation, table_name, rows_before, rows_after, batch_date)
        VALUES ('FORALL_INSERT', 'TARGET_TABLE', v_before, v_after, SYSDATE);
    END PC_FORALL_INSERT;

    PROCEDURE PC_FORALL_UPDATE IS
        TYPE t_id_tab IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
        TYPE t_sal_tab IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
        TYPE t_pct_tab IS TABLE OF NUMBER INDEX BY PLS_INTEGER;

        l_ids        t_id_tab;
        l_salaries   t_sal_tab;
        l_comm_pcts  t_pct_tab;
        v_updated    NUMBER;
    BEGIN
        SELECT employee_id, salary * 1.1, NVL(commission_pct, 0) + 0.05
          BULK COLLECT INTO l_ids, l_salaries, l_comm_pcts
          FROM HR.employees
         WHERE performance_rating = 'A'
           AND department_id IN (SELECT department_id
                                   FROM departments
                                  WHERE budget_status = 'APPROVED');

        FORALL i IN 1..l_ids.COUNT
            UPDATE employees
               SET salary = l_salaries(i),
                   commission_pct = l_comm_pcts(i),
                   last_raise_date = SYSDATE
             WHERE employee_id = l_ids(i);

        v_updated := SQL%ROWCOUNT;

        INSERT INTO salary_change_log (change_type, emp_count, change_date)
        VALUES ('PERFORMANCE_RAISE', v_updated, SYSDATE);
    END PC_FORALL_UPDATE;

    PROCEDURE PC_FORALL_DELETE IS
        TYPE t_id_tab IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
        l_ids      t_id_tab;
        v_deleted  NUMBER;
    BEGIN
        SELECT id
          BULK COLLECT INTO l_ids
          FROM temp_data
         WHERE created_date < SYSDATE - 90
           AND processed_flag = 'Y';

        IF l_ids.COUNT > 0 THEN
            -- Delete child records first
            FORALL i IN 1..l_ids.COUNT
                DELETE FROM temp_data_detail
                 WHERE parent_id = l_ids(i);

            -- Then delete parent records
            FORALL i IN 1..l_ids.COUNT
                DELETE FROM temp_data
                 WHERE id = l_ids(i);

            v_deleted := SQL%ROWCOUNT;

            INSERT INTO cleanup_log (table_name, rows_deleted, cleanup_date)
            VALUES ('TEMP_DATA', v_deleted, SYSDATE);
        END IF;
    END PC_FORALL_DELETE;

    PROCEDURE PC_FORALL_SAVE_EXCEPTIONS IS
        TYPE t_rec IS RECORD (
            id     NUMBER,
            name   VARCHAR2(100),
            amt    NUMBER,
            status VARCHAR2(20)
        );
        TYPE t_batch IS TABLE OF t_rec INDEX BY PLS_INTEGER;

        l_batch      t_batch;
        l_err_cnt    NUMBER;
        l_err_idx    NUMBER;
        l_err_code   NUMBER;
        e_dml_errors EXCEPTION;
        PRAGMA EXCEPTION_INIT(e_dml_errors, -24381);
    BEGIN
        SELECT employee_id, last_name, salary, status
          BULK COLLECT INTO l_batch
          FROM employees
         WHERE department_id = 30
           AND hire_date >= ADD_MONTHS(SYSDATE, -60);

        FORALL i IN 1..l_batch.COUNT SAVE EXCEPTIONS
            INSERT INTO CUSTOMER.target_audit (id, description, amount, status, created_date)
            VALUES (l_batch(i).id, l_batch(i).name, l_batch(i).amt,
                    l_batch(i).status, SYSDATE);

        INSERT INTO batch_result_log (operation, total_rows, success_count, run_date)
        VALUES ('SAVE_EXCEPTIONS_INSERT', l_batch.COUNT, l_batch.COUNT, SYSDATE);
    EXCEPTION
        WHEN e_dml_errors THEN
            l_err_cnt := SQL%BULK_EXCEPTIONS.COUNT;
            FOR j IN 1..l_err_cnt LOOP
                l_err_idx := SQL%BULK_EXCEPTIONS(j).ERROR_INDEX;
                l_err_code := SQL%BULK_EXCEPTIONS(j).ERROR_CODE;

                INSERT INTO error_log (batch_index, error_code, error_message,
                                       source_id, created_date)
                VALUES (
                    l_err_idx,
                    l_err_code,
                    SQLERRM(-l_err_code),
                    l_batch(l_err_idx).id,
                    SYSDATE
                );
            END LOOP;

            INSERT INTO batch_result_log (operation, total_rows, success_count, error_count, run_date)
            VALUES ('SAVE_EXCEPTIONS_INSERT', l_batch.COUNT,
                    l_batch.COUNT - l_err_cnt, l_err_cnt, SYSDATE);
        WHEN OTHERS THEN
            INSERT INTO error_log (batch_index, error_code, error_message, created_date)
            VALUES (-1, SQLCODE, SQLERRM, SYSDATE);
            RAISE;
    END PC_FORALL_SAVE_EXCEPTIONS;

    PROCEDURE PC_FORALL_INDICES_OF IS
        TYPE t_id_tab IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
        TYPE t_name_tab IS TABLE OF VARCHAR2(100) INDEX BY PLS_INTEGER;
        TYPE t_amt_tab IS TABLE OF NUMBER INDEX BY PLS_INTEGER;

        v_ids    t_id_tab;
        v_names  t_name_tab;
        v_amts   t_amt_tab;
    BEGIN
        -- Sparse collection (not all indices populated)
        v_ids(2)  := 101;
        v_ids(5)  := 102;
        v_ids(8)  := 103;
        v_ids(13) := 104;
        v_ids(21) := 105;

        v_names(2)  := 'Smith';
        v_names(5)  := 'Jones';
        v_names(8)  := 'Brown';
        v_names(13) := 'Davis';
        v_names(21) := 'Wilson';

        v_amts(2)  := 5000;
        v_amts(5)  := 6000;
        v_amts(8)  := 7000;
        v_amts(13) := 8000;
        v_amts(21) := 9000;

        FORALL i IN INDICES OF v_ids
            INSERT INTO sparse_target (id, name, amount, created_date)
            VALUES (v_ids(i), v_names(i), v_amts(i), SYSDATE);

        INSERT INTO batch_log (operation, table_name, rows_before, rows_after, batch_date)
        VALUES ('INDICES_OF_INSERT', 'SPARSE_TARGET', 0, SQL%ROWCOUNT, SYSDATE);
    END PC_FORALL_INDICES_OF;

    PROCEDURE PC_FORALL_VALUES_OF IS
        TYPE t_idx_tab IS TABLE OF PLS_INTEGER INDEX BY PLS_INTEGER;
        TYPE t_id_tab IS TABLE OF NUMBER INDEX BY PLS_INTEGER;

        v_idx_collection t_idx_tab;
        v_all_ids        t_id_tab;
        v_deleted        NUMBER;
    BEGIN
        SELECT employee_id
          BULK COLLECT INTO v_all_ids
          FROM employees
         WHERE department_id = 20
         ORDER BY employee_id;

        -- Only process specific indices from the full collection
        v_idx_collection(1) := 1;
        v_idx_collection(2) := 3;
        v_idx_collection(3) := 5;

        IF v_all_ids.COUNT >= 5 THEN
            FORALL i IN VALUES OF v_idx_collection
                DELETE FROM temp_processing
                 WHERE employee_id = v_all_ids(i);

            v_deleted := SQL%ROWCOUNT;

            FORALL i IN VALUES OF v_idx_collection
                INSERT INTO deletion_audit (emp_id, deleted_from, deletion_date)
                VALUES (v_all_ids(i), 'TEMP_PROCESSING', SYSDATE);
        END IF;
    END PC_FORALL_VALUES_OF;

    PROCEDURE PC_SELECT_BULK_COLLECT(p_dept IN NUMBER) IS
        TYPE t_id_tab IS TABLE OF employees.employee_id%TYPE;
        TYPE t_name_tab IS TABLE OF employees.last_name%TYPE;
        TYPE t_sal_tab IS TABLE OF employees.salary%TYPE;
        TYPE t_date_tab IS TABLE OF employees.hire_date%TYPE;

        v_ids     t_id_tab;
        v_names   t_name_tab;
        v_sals    t_sal_tab;
        v_dates   t_date_tab;
        v_avg_sal NUMBER;
    BEGIN
        SELECT employee_id, last_name, salary, hire_date
          BULK COLLECT INTO v_ids, v_names, v_sals, v_dates
          FROM employees
         WHERE department_id = p_dept
         ORDER BY last_name;

        IF v_sals.COUNT > 0 THEN
            v_avg_sal := 0;
            FOR i IN 1..v_sals.COUNT LOOP
                v_avg_sal := v_avg_sal + v_sals(i);
            END LOOP;
            v_avg_sal := v_avg_sal / v_sals.COUNT;

            INSERT INTO dept_stats (dept_id, emp_count, avg_salary, computed_date)
            VALUES (p_dept, v_ids.COUNT, v_avg_sal, SYSDATE);
        END IF;
    END PC_SELECT_BULK_COLLECT;

    PROCEDURE PC_FORALL_MERGE IS
        TYPE t_rec IS RECORD (
            id     NUMBER,
            name   VARCHAR2(100),
            amount NUMBER
        );
        TYPE t_batch IS TABLE OF t_rec INDEX BY PLS_INTEGER;

        v_batch   t_batch;
        v_merged  NUMBER;
    BEGIN
        SELECT employee_id, last_name, salary
          BULK COLLECT INTO v_batch
          FROM CUSTOMER.employees
         WHERE department_id = 50
           AND status = 'ACTIVE';

        FORALL i IN 1..v_batch.COUNT
            MERGE INTO target_summary tgt
            USING (SELECT v_batch(i).id AS id,
                          v_batch(i).name AS name,
                          v_batch(i).amount AS amount
                     FROM DUAL) src
               ON (tgt.id = src.id)
             WHEN MATCHED THEN
                  UPDATE SET tgt.name = src.name,
                             tgt.amount = src.amount,
                             tgt.updated_date = SYSDATE,
                             tgt.update_count = tgt.update_count + 1
             WHEN NOT MATCHED THEN
                  INSERT (id, name, amount, created_date, update_count)
                  VALUES (src.id, src.name, src.amount, SYSDATE, 0);

        v_merged := SQL%ROWCOUNT;

        INSERT INTO merge_log (source_table, target_table, rows_merged, merge_date)
        VALUES ('CUSTOMER.EMPLOYEES', 'TARGET_SUMMARY', v_merged, SYSDATE);
    END PC_FORALL_MERGE;

END PKG_BULK_OPS_TEST;
/
