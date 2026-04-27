-- =============================================================================
-- Trigger 1: Simple BEFORE INSERT with sequence assignment
-- =============================================================================
CREATE OR REPLACE TRIGGER TRG_EMPLOYEES_BI
    BEFORE INSERT ON employees
    FOR EACH ROW
DECLARE
    v_user VARCHAR2(30);
BEGIN
    SELECT USER INTO v_user FROM DUAL;

    :NEW.id         := my_seq.NEXTVAL;
    :NEW.created_by := v_user;
    :NEW.created_at := SYSDATE;

    IF :NEW.status IS NULL THEN
        :NEW.status := 'ACTIVE';
    END IF;

    INSERT INTO employee_creation_log (emp_id, created_by, created_at)
    VALUES (:NEW.id, v_user, SYSTIMESTAMP);
END TRG_EMPLOYEES_BI;
/

-- =============================================================================
-- Trigger 2: AFTER UPDATE trigger inserting into audit trail
-- =============================================================================
CREATE OR REPLACE TRIGGER TRG_EMPLOYEES_AU
    AFTER UPDATE ON employees
    FOR EACH ROW
DECLARE
    v_changes VARCHAR2(4000);
BEGIN
    v_changes := '';

    IF :OLD.salary <> :NEW.salary THEN
        v_changes := v_changes || 'SALARY:' || :OLD.salary || '->' || :NEW.salary || '; ';
    END IF;

    IF :OLD.dept_id <> :NEW.dept_id THEN
        v_changes := v_changes || 'DEPT:' || :OLD.dept_id || '->' || :NEW.dept_id || '; ';
    END IF;

    IF :OLD.job_title <> :NEW.job_title THEN
        v_changes := v_changes || 'JOB:' || :OLD.job_title || '->' || :NEW.job_title || '; ';
    END IF;

    INSERT INTO audit_trail (
        audit_id, table_name, record_id, change_type,
        change_details, changed_by, changed_at
    ) VALUES (
        audit_seq.NEXTVAL, 'EMPLOYEES', :NEW.employee_id, 'UPDATE',
        v_changes, USER, SYSTIMESTAMP
    );
END TRG_EMPLOYEES_AU;
/

-- =============================================================================
-- Trigger 3: Multi-event trigger (BEFORE INSERT OR UPDATE OR DELETE)
-- =============================================================================
CREATE OR REPLACE TRIGGER TRG_ORDERS_BIUD
    BEFORE INSERT OR UPDATE OR DELETE ON orders
    FOR EACH ROW
DECLARE
    v_action   VARCHAR2(10);
    v_order_id NUMBER;
BEGIN
    IF INSERTING THEN
        v_action   := 'INSERT';
        v_order_id := :NEW.order_id;
        :NEW.created_at  := SYSDATE;
        :NEW.created_by  := USER;
        :NEW.version_num := 1;

        UPDATE order_counters
           SET daily_count = daily_count + 1
         WHERE counter_date = TRUNC(SYSDATE);

    ELSIF UPDATING THEN
        v_action   := 'UPDATE';
        v_order_id := :NEW.order_id;
        :NEW.updated_at  := SYSDATE;
        :NEW.updated_by  := USER;
        :NEW.version_num := :OLD.version_num + 1;

        IF :OLD.status = 'SHIPPED' AND :NEW.status = 'CANCELLED' THEN
            INSERT INTO cancellation_requests (order_id, old_status, requested_by, requested_at)
            VALUES (:NEW.order_id, :OLD.status, USER, SYSTIMESTAMP);
        END IF;

    ELSIF DELETING THEN
        v_action   := 'DELETE';
        v_order_id := :OLD.order_id;

        INSERT INTO deleted_orders (
            order_id, customer_id, order_date, total_amount, deleted_by, deleted_at
        ) VALUES (
            :OLD.order_id, :OLD.customer_id, :OLD.order_date, :OLD.total_amount, USER, SYSTIMESTAMP
        );
    END IF;

    INSERT INTO order_audit (audit_id, order_id, action, performed_by, performed_at)
    VALUES (order_audit_seq.NEXTVAL, v_order_id, v_action, USER, SYSTIMESTAMP);
END TRG_ORDERS_BIUD;
/

-- =============================================================================
-- Trigger 4: Compound trigger with all four timing points
-- =============================================================================
CREATE OR REPLACE TRIGGER TRG_INVOICES_COMPOUND
    FOR INSERT OR UPDATE ON invoices
    COMPOUND TRIGGER

    TYPE t_invoice_rec IS RECORD (
        invoice_id   NUMBER,
        amount       NUMBER,
        action_type  VARCHAR2(10)
    );
    TYPE t_invoice_tab IS TABLE OF t_invoice_rec INDEX BY PLS_INTEGER;

    g_invoices t_invoice_tab;
    g_index    PLS_INTEGER := 0;

    BEFORE STATEMENT IS
    BEGIN
        g_index := 0;
        g_invoices.DELETE;

        UPDATE batch_control
           SET batch_status = 'PROCESSING',
               started_at   = SYSTIMESTAMP
         WHERE batch_name = 'INVOICE_TRIGGER';
    END BEFORE STATEMENT;

    BEFORE EACH ROW IS
    BEGIN
        IF INSERTING THEN
            :NEW.created_at := SYSDATE;
            :NEW.created_by := USER;
            IF :NEW.invoice_num IS NULL THEN
                :NEW.invoice_num := 'INV-' || LPAD(invoice_num_seq.NEXTVAL, 8, '0');
            END IF;
        ELSIF UPDATING THEN
            :NEW.updated_at := SYSDATE;
            :NEW.updated_by := USER;
        END IF;
    END BEFORE EACH ROW;

    AFTER EACH ROW IS
    BEGIN
        g_index := g_index + 1;
        g_invoices(g_index).invoice_id  := :NEW.invoice_id;
        g_invoices(g_index).amount      := :NEW.amount;
        IF INSERTING THEN
            g_invoices(g_index).action_type := 'INSERT';
        ELSE
            g_invoices(g_index).action_type := 'UPDATE';
        END IF;
    END AFTER EACH ROW;

    AFTER STATEMENT IS
    BEGIN
        FOR i IN 1..g_invoices.COUNT LOOP
            INSERT INTO invoice_audit (
                audit_id, invoice_id, action_type, amount, audited_at
            ) VALUES (
                invoice_audit_seq.NEXTVAL,
                g_invoices(i).invoice_id,
                g_invoices(i).action_type,
                g_invoices(i).amount,
                SYSTIMESTAMP
            );
        END LOOP;

        UPDATE batch_control
           SET batch_status   = 'COMPLETE',
               completed_at   = SYSTIMESTAMP,
               records_count  = g_index
         WHERE batch_name = 'INVOICE_TRIGGER';
    END AFTER STATEMENT;

END TRG_INVOICES_COMPOUND;
/

-- =============================================================================
-- Trigger 5: INSTEAD OF INSERT on a view
-- =============================================================================
CREATE OR REPLACE TRIGGER TRG_VW_EMP_DEPT_IOI
    INSTEAD OF INSERT ON vw_employee_department
    FOR EACH ROW
DECLARE
    v_dept_id NUMBER;
    v_emp_id  NUMBER;
BEGIN
    -- Ensure department exists
    BEGIN
        SELECT department_id INTO v_dept_id
          FROM departments
         WHERE department_name = :NEW.department_name;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            SELECT dept_seq.NEXTVAL INTO v_dept_id FROM DUAL;
            INSERT INTO departments (department_id, department_name, created_at)
            VALUES (v_dept_id, :NEW.department_name, SYSDATE);
    END;

    -- Insert employee
    v_emp_id := emp_seq.NEXTVAL;
    INSERT INTO employees (employee_id, first_name, last_name, dept_id, salary, hire_date)
    VALUES (v_emp_id, :NEW.first_name, :NEW.last_name, v_dept_id, :NEW.salary, SYSDATE);

    INSERT INTO view_insert_log (log_id, view_name, emp_id, dept_id, inserted_at)
    VALUES (view_log_seq.NEXTVAL, 'VW_EMPLOYEE_DEPARTMENT', v_emp_id, v_dept_id, SYSTIMESTAMP);
END TRG_VW_EMP_DEPT_IOI;
/

-- =============================================================================
-- Trigger 6: Trigger with exception handler
-- =============================================================================
CREATE OR REPLACE TRIGGER TRG_PAYMENTS_BI
    BEFORE INSERT ON payments
    FOR EACH ROW
DECLARE
    v_credit_limit NUMBER;
    v_outstanding  NUMBER;
BEGIN
    SELECT credit_limit INTO v_credit_limit
      FROM customer_accounts
     WHERE customer_id = :NEW.customer_id;

    SELECT NVL(SUM(amount), 0) INTO v_outstanding
      FROM payments
     WHERE customer_id = :NEW.customer_id
       AND payment_status = 'PENDING';

    IF (v_outstanding + :NEW.amount) > v_credit_limit THEN
        :NEW.payment_status := 'HELD';
        :NEW.hold_reason    := 'CREDIT_LIMIT_EXCEEDED';

        INSERT INTO payment_holds (payment_ref, customer_id, amount, limit_amount, held_at)
        VALUES (:NEW.payment_ref, :NEW.customer_id, :NEW.amount, v_credit_limit, SYSTIMESTAMP);
    ELSE
        :NEW.payment_status := 'APPROVED';
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO error_log (
            error_id, error_source, error_code, error_message, context_data, logged_at
        ) VALUES (
            error_seq.NEXTVAL, 'TRG_PAYMENTS_BI', SQLCODE, SQLERRM,
            'CUST:' || :NEW.customer_id || ' AMT:' || :NEW.amount, SYSTIMESTAMP
        );
        :NEW.payment_status := 'ERROR';
END TRG_PAYMENTS_BI;
/

-- =============================================================================
-- Trigger 7: Trigger with :NEW / :OLD value checks in IF statements
-- =============================================================================
CREATE OR REPLACE TRIGGER TRG_PRODUCTS_BU
    BEFORE UPDATE ON products
    FOR EACH ROW
DECLARE
    v_pct_change NUMBER;
BEGIN
    -- Track price changes
    IF :NEW.unit_price <> :OLD.unit_price THEN
        v_pct_change := ((:NEW.unit_price - :OLD.unit_price) / :OLD.unit_price) * 100;

        INSERT INTO price_history (
            history_id, product_id, old_price, new_price, pct_change, changed_by, changed_at
        ) VALUES (
            price_hist_seq.NEXTVAL, :OLD.product_id,
            :OLD.unit_price, :NEW.unit_price, v_pct_change, USER, SYSTIMESTAMP
        );

        IF ABS(v_pct_change) > 20 THEN
            INSERT INTO price_alerts (alert_id, product_id, pct_change, alert_type, created_at)
            VALUES (alert_seq.NEXTVAL, :NEW.product_id, v_pct_change, 'LARGE_CHANGE', SYSTIMESTAMP);
        END IF;
    END IF;

    -- Track status changes
    IF :NEW.product_status <> :OLD.product_status THEN
        INSERT INTO status_changes (
            change_id, entity_type, entity_id, old_status, new_status, changed_at
        ) VALUES (
            status_chg_seq.NEXTVAL, 'PRODUCT', :OLD.product_id,
            :OLD.product_status, :NEW.product_status, SYSTIMESTAMP
        );

        IF :NEW.product_status = 'DISCONTINUED' THEN
            UPDATE inventory
               SET reserved_flag = 'Y',
                   reserved_at   = SYSDATE
             WHERE product_id = :OLD.product_id
               AND quantity_on_hand > 0;
        END IF;
    END IF;

    -- Track stock level changes
    IF :NEW.stock_level < :OLD.stock_level AND :NEW.stock_level < :NEW.reorder_point THEN
        INSERT INTO reorder_requests (
            request_id, product_id, current_stock, reorder_point, requested_at
        ) VALUES (
            reorder_seq.NEXTVAL, :NEW.product_id,
            :NEW.stock_level, :NEW.reorder_point, SYSTIMESTAMP
        );
    END IF;

    :NEW.last_modified_by := USER;
    :NEW.last_modified_at := SYSDATE;
END TRG_PRODUCTS_BU;
/
