CREATE OR REPLACE PACKAGE BODY VALIDATION.PKG_DATA_VALIDATOR AS
  ---------------------------------------------------------------------------
  -- Data Validation Package
  -- Covers: SELECT INTO lookups, NVL, DECODE, correlated subqueries,
  --         EXISTS/NOT EXISTS, function calls in SQL, cursor FOR loop,
  --         nested IF/ELSIF, dynamic validation SQL
  ---------------------------------------------------------------------------

  gc_module CONSTANT VARCHAR2(30) := 'PKG_DATA_VALIDATOR';

  -- -----------------------------------------------------------------------
  -- FN_LOOKUP_CODE
  -- Lookup function that retrieves a value from reference table
  -- Called from SQL in other procedures
  -- -----------------------------------------------------------------------
  FUNCTION FN_LOOKUP_CODE (
    p_code_type IN VARCHAR2,
    p_code      IN VARCHAR2
  ) RETURN VARCHAR2
  IS
    v_description VARCHAR2(200);
  BEGIN
    SELECT lc.description
      INTO v_description
      FROM MASTER.lookup_codes lc
     WHERE lc.code_type = p_code_type
       AND lc.code      = p_code
       AND lc.active_flag = 'Y';

    RETURN v_description;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN NULL;
    WHEN TOO_MANY_ROWS THEN
      SELECT MIN(lc.description)
        INTO v_description
        FROM MASTER.lookup_codes lc
       WHERE lc.code_type = p_code_type
         AND lc.code      = p_code
         AND lc.active_flag = 'Y';
      RETURN v_description;
  END FN_LOOKUP_CODE;

  -- -----------------------------------------------------------------------
  -- PC_VALIDATE_RECORD
  -- Multiple SELECT INTO lookups, CASE WHEN with function calls, NVL, DECODE
  -- -----------------------------------------------------------------------
  PROCEDURE PC_VALIDATE_RECORD (
    p_record_id   IN NUMBER,
    p_is_valid    OUT BOOLEAN,
    p_error_msg   OUT VARCHAR2
  )
  IS
    v_record       DATA_SCHEMA.input_records%ROWTYPE;
    v_region_count NUMBER;
    v_category_ok  VARCHAR2(1);
    v_account_type VARCHAR2(50);
    v_credit_limit NUMBER;
    v_lookup_desc  VARCHAR2(200);
    v_errors       VARCHAR2(4000) := '';
  BEGIN
    p_is_valid := TRUE;

    -- Fetch the record
    SELECT *
      INTO v_record
      FROM DATA_SCHEMA.input_records
     WHERE record_id = p_record_id;

    -- Validate region code exists
    SELECT COUNT(*)
      INTO v_region_count
      FROM MASTER.regions r
     WHERE r.region_code = v_record.region_code
       AND r.active_flag = 'Y';

    IF v_region_count = 0 THEN
      p_is_valid := FALSE;
      v_errors := v_errors || 'Invalid region code: ' || v_record.region_code || '; ';
    END IF;

    -- Validate category using DECODE
    SELECT DECODE(COUNT(*), 0, 'N', 'Y')
      INTO v_category_ok
      FROM MASTER.categories cat
     WHERE cat.category_code = v_record.category_code
       AND cat.status = 'ACTIVE'
       AND (cat.effective_to IS NULL OR cat.effective_to >= SYSDATE);

    IF v_category_ok = 'N' THEN
      p_is_valid := FALSE;
      v_errors := v_errors || 'Invalid or expired category: ' || v_record.category_code || '; ';
    END IF;

    -- Lookup account type using NVL for default
    SELECT NVL(a.account_type, 'STANDARD')
      INTO v_account_type
      FROM CUSTOMER.accounts a
     WHERE a.account_id = v_record.account_id;

    -- Check credit limit based on account type using CASE with function call
    SELECT CASE v_account_type
             WHEN 'PREMIUM' THEN NVL(a.credit_limit, 50000)
             WHEN 'GOLD'    THEN NVL(a.credit_limit, 25000)
             WHEN 'STANDARD' THEN NVL(a.credit_limit, 10000)
             ELSE 5000
           END
      INTO v_credit_limit
      FROM CUSTOMER.accounts a
     WHERE a.account_id = v_record.account_id;

    IF v_record.amount > v_credit_limit THEN
      p_is_valid := FALSE;
      v_errors := v_errors || 'Amount ' || v_record.amount ||
                  ' exceeds credit limit ' || v_credit_limit || '; ';
    END IF;

    -- Validate code using FN_LOOKUP_CODE function in SQL context
    v_lookup_desc := FN_LOOKUP_CODE('TXN_TYPE', v_record.transaction_type);

    IF v_lookup_desc IS NULL THEN
      p_is_valid := FALSE;
      v_errors := v_errors || 'Invalid transaction type: ' || v_record.transaction_type || '; ';
    END IF;

    -- Update record with validation results
    UPDATE DATA_SCHEMA.input_records
       SET validation_status = CASE WHEN p_is_valid THEN 'VALID' ELSE 'INVALID' END,
           validation_msg    = NVL(SUBSTR(v_errors, 1, 4000), 'All checks passed'),
           validated_date    = SYSDATE,
           validated_by      = USER
     WHERE record_id = p_record_id;

    p_error_msg := v_errors;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      p_is_valid  := FALSE;
      p_error_msg := 'Record ' || p_record_id || ' not found';
    WHEN OTHERS THEN
      p_is_valid  := FALSE;
      p_error_msg := 'Unexpected error: ' || SQLERRM;
  END PC_VALIDATE_RECORD;

  -- -----------------------------------------------------------------------
  -- PC_CROSS_VALIDATE
  -- Correlated subqueries with EXISTS and NOT EXISTS in same query
  -- -----------------------------------------------------------------------
  PROCEDURE PC_CROSS_VALIDATE (
    p_batch_id     IN NUMBER,
    p_invalid_cnt  OUT NUMBER
  )
  IS
    v_mismatch_count NUMBER;
  BEGIN
    -- Find records that have matching orders (EXISTS) but no payment (NOT EXISTS)
    -- AND are in the allowed product list (IN subquery)
    -- AND have a valid total (scalar subquery comparison)
    UPDATE DATA_SCHEMA.input_records ir
       SET ir.validation_status = 'CROSS_FAIL',
           ir.validation_msg    = 'Cross-validation failed: order without payment',
           ir.validated_date    = SYSDATE
     WHERE ir.batch_id = p_batch_id
       AND ir.validation_status = 'VALID'
       AND EXISTS (
             SELECT 1
               FROM SALES.orders o
              WHERE o.customer_id = ir.customer_id
                AND o.order_date  = ir.transaction_date
                AND o.status      = 'COMPLETED'
           )
       AND NOT EXISTS (
             SELECT 1
               FROM FINANCE.payments p
              WHERE p.customer_id = ir.customer_id
                AND p.payment_date = ir.transaction_date
                AND p.status       = 'CONFIRMED'
           )
       AND ir.product_code IN (
             SELECT pr.product_code
               FROM MASTER.products pr
              WHERE pr.requires_payment = 'Y'
                AND pr.status = 'ACTIVE'
           )
       AND ir.amount > (
             SELECT NVL(AVG(o2.order_amount), 0) * 1.5
               FROM SALES.orders o2
              WHERE o2.customer_id = ir.customer_id
                AND o2.order_date >= ADD_MONTHS(ir.transaction_date, -12)
           );

    p_invalid_cnt := SQL%ROWCOUNT;

    -- Also flag records where amounts don't match between systems
    UPDATE DATA_SCHEMA.input_records ir
       SET ir.validation_status = 'AMOUNT_MISMATCH',
           ir.validation_msg    = 'Amount mismatch between order and input',
           ir.validated_date    = SYSDATE
     WHERE ir.batch_id = p_batch_id
       AND ir.validation_status = 'VALID'
       AND EXISTS (
             SELECT 1
               FROM SALES.orders o
              WHERE o.customer_id = ir.customer_id
                AND o.order_date  = ir.transaction_date
                AND ABS(o.order_amount - ir.amount) > 0.01
           );

    p_invalid_cnt := p_invalid_cnt + SQL%ROWCOUNT;

    INSERT INTO VALIDATION.validation_log (
      log_id, batch_id, step_name, invalid_count, log_date
    ) VALUES (
      VALIDATION.VAL_LOG_SEQ.NEXTVAL, p_batch_id, 'CROSS_VALIDATE',
      p_invalid_cnt, SYSDATE
    );

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      RAISE;
  END PC_CROSS_VALIDATE;

  -- -----------------------------------------------------------------------
  -- PC_BATCH_VALIDATE
  -- Cursor FOR loop with nested IF/ELSIF doing different lookups per branch
  -- -----------------------------------------------------------------------
  PROCEDURE PC_BATCH_VALIDATE (
    p_batch_id    IN NUMBER,
    p_valid_cnt   OUT NUMBER,
    p_invalid_cnt OUT NUMBER
  )
  IS
    CURSOR c_records IS
      SELECT ir.record_id,
             ir.customer_id,
             ir.transaction_type,
             ir.product_code,
             ir.amount,
             ir.region_code,
             ir.account_id
        FROM DATA_SCHEMA.input_records ir
       WHERE ir.batch_id = p_batch_id
         AND ir.validation_status = 'PENDING'
       ORDER BY ir.record_id;

    v_check_val    VARCHAR2(100);
    v_is_valid     BOOLEAN;
    v_err_msg      VARCHAR2(500);
    v_threshold    NUMBER;
  BEGIN
    p_valid_cnt   := 0;
    p_invalid_cnt := 0;

    FOR rec IN c_records LOOP
      v_is_valid := TRUE;
      v_err_msg  := NULL;

      IF rec.transaction_type = 'PURCHASE' THEN
        -- Validate product exists in inventory
        BEGIN
          SELECT stock_quantity
            INTO v_threshold
            FROM INVENTORY.products
           WHERE product_code = rec.product_code
             AND status = 'AVAILABLE';

          IF v_threshold <= 0 THEN
            v_is_valid := FALSE;
            v_err_msg  := 'Product out of stock: ' || rec.product_code;
          END IF;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            v_is_valid := FALSE;
            v_err_msg  := 'Product not found: ' || rec.product_code;
        END;

      ELSIF rec.transaction_type = 'REFUND' THEN
        -- Validate original purchase exists
        BEGIN
          SELECT 'EXISTS'
            INTO v_check_val
            FROM SALES.orders
           WHERE customer_id  = rec.customer_id
             AND product_code = rec.product_code
             AND status       = 'COMPLETED'
             AND ROWNUM       = 1;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            v_is_valid := FALSE;
            v_err_msg  := 'No original purchase found for refund';
        END;

      ELSIF rec.transaction_type = 'TRANSFER' THEN
        -- Validate both accounts are valid
        BEGIN
          SELECT account_type
            INTO v_check_val
            FROM CUSTOMER.accounts
           WHERE account_id = rec.account_id
             AND status     = 'ACTIVE';
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            v_is_valid := FALSE;
            v_err_msg  := 'Source account not active: ' || rec.account_id;
        END;

      ELSIF rec.transaction_type = 'WITHDRAWAL' THEN
        -- Check balance is sufficient
        BEGIN
          SELECT NVL(balance, 0)
            INTO v_threshold
            FROM CUSTOMER.account_balances
           WHERE account_id = rec.account_id;

          IF v_threshold < rec.amount THEN
            v_is_valid := FALSE;
            v_err_msg  := 'Insufficient balance: ' || v_threshold || ' < ' || rec.amount;
          END IF;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            v_is_valid := FALSE;
            v_err_msg  := 'No balance record for account: ' || rec.account_id;
        END;

      ELSE
        -- Unknown transaction type
        v_is_valid := FALSE;
        v_err_msg  := 'Unknown transaction type: ' || rec.transaction_type;
      END IF;

      -- Update the record status
      UPDATE DATA_SCHEMA.input_records
         SET validation_status = CASE WHEN v_is_valid THEN 'VALID' ELSE 'INVALID' END,
             validation_msg    = v_err_msg,
             validated_date    = SYSDATE
       WHERE record_id = rec.record_id;

      IF v_is_valid THEN
        p_valid_cnt := p_valid_cnt + 1;
      ELSE
        p_invalid_cnt := p_invalid_cnt + 1;
      END IF;

      -- Commit every 500 rows
      IF MOD(p_valid_cnt + p_invalid_cnt, 500) = 0 THEN
        COMMIT;
      END IF;
    END LOOP;

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      RAISE;
  END PC_BATCH_VALIDATE;

  -- -----------------------------------------------------------------------
  -- PC_VALIDATE_WITH_DYNAMIC
  -- Dynamic SQL for flexible table/column validation
  -- -----------------------------------------------------------------------
  PROCEDURE PC_VALIDATE_WITH_DYNAMIC (
    p_table_name  IN VARCHAR2,
    p_column_name IN VARCHAR2,
    p_value       IN VARCHAR2,
    p_exists      OUT BOOLEAN,
    p_row_count   OUT NUMBER
  )
  IS
    v_sql         VARCHAR2(4000);
    v_safe_table  VARCHAR2(128);
    v_safe_column VARCHAR2(128);
    v_count       NUMBER;
  BEGIN
    -- Sanitize inputs to prevent SQL injection
    v_safe_table  := DBMS_ASSERT.SIMPLE_SQL_NAME(p_table_name);
    v_safe_column := DBMS_ASSERT.SIMPLE_SQL_NAME(p_column_name);

    -- Build and execute dynamic count query
    v_sql := 'SELECT COUNT(*) FROM VALIDATION.' || v_safe_table ||
             ' WHERE ' || v_safe_column || ' = :1';

    EXECUTE IMMEDIATE v_sql INTO v_count USING p_value;

    p_exists    := (v_count > 0);
    p_row_count := v_count;

    -- Also check if the value exists in a cross-reference table dynamically
    v_sql := 'SELECT COUNT(*) FROM MASTER.cross_reference' ||
             ' WHERE ref_table = :1 AND ref_column = :2 AND ref_value = :3';

    EXECUTE IMMEDIATE v_sql INTO v_count USING p_table_name, p_column_name, p_value;

    IF v_count = 0 AND p_exists THEN
      -- Value exists in table but not in cross-reference: log a warning
      INSERT INTO VALIDATION.validation_warnings (
        warning_id, table_name, column_name, value_checked,
        warning_msg, warning_date
      ) VALUES (
        VALIDATION.WARN_SEQ.NEXTVAL, p_table_name, p_column_name, p_value,
        'Value found in ' || p_table_name || '.' || p_column_name ||
        ' but not in cross_reference',
        SYSDATE
      );
      COMMIT;
    END IF;

    -- Log the validation
    INSERT INTO VALIDATION.dynamic_val_log (
      log_id, table_name, column_name, value_checked,
      exists_flag, row_count, log_date
    ) VALUES (
      VALIDATION.DYN_LOG_SEQ.NEXTVAL, p_table_name, p_column_name,
      p_value, CASE WHEN p_exists THEN 'Y' ELSE 'N' END,
      p_row_count, SYSDATE
    );

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      p_exists    := FALSE;
      p_row_count := 0;
      INSERT INTO VALIDATION.validation_errors (
        error_id, context_info, error_code, error_msg, error_date
      ) VALUES (
        VALIDATION.VAL_ERR_SEQ.NEXTVAL,
        'Dynamic validation on ' || p_table_name || '.' || p_column_name,
        SQLCODE, SQLERRM, SYSDATE
      );
      COMMIT;
  END PC_VALIDATE_WITH_DYNAMIC;

END PKG_DATA_VALIDATOR;
/
