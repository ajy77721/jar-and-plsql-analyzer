-- =========================================================================
-- Test 15: Compound Trigger + Supporting Audit Package
-- Covers: compound trigger, :OLD/:NEW, FORALL, collections, sequences,
--         autonomous transaction, schema-qualified tables
-- =========================================================================

-- -------------------------------------------------------------------------
-- Part 1: Compound Trigger on FINANCE.main_transactions
-- -------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER FINANCE.TRG_MAIN_TRANSACTIONS_AUDIT
  FOR INSERT OR UPDATE OR DELETE ON FINANCE.main_transactions
  COMPOUND TRIGGER

  -- Type declarations for the change collection
  TYPE t_audit_rec IS RECORD (
    change_id        NUMBER,
    transaction_id   FINANCE.main_transactions.transaction_id%TYPE,
    operation        VARCHAR2(10),
    column_name      VARCHAR2(60),
    old_value        VARCHAR2(4000),
    new_value        VARCHAR2(4000),
    changed_by       VARCHAR2(60),
    changed_date     TIMESTAMP
  );

  TYPE t_audit_tab IS TABLE OF t_audit_rec INDEX BY PLS_INTEGER;

  -- Package-level variables for the compound trigger
  v_audit_entries  t_audit_tab;
  v_idx            PLS_INTEGER := 0;
  v_operation      VARCHAR2(10);
  v_user           VARCHAR2(60);
  v_timestamp      TIMESTAMP;

  -- -----------------------------------------------------------------------
  -- BEFORE STATEMENT: Initialize the collection and capture context
  -- -----------------------------------------------------------------------
  BEFORE STATEMENT IS
  BEGIN
    v_audit_entries.DELETE;
    v_idx       := 0;
    v_user      := SYS_CONTEXT('USERENV', 'SESSION_USER');
    v_timestamp := SYSTIMESTAMP;

    IF INSERTING THEN
      v_operation := 'INSERT';
    ELSIF UPDATING THEN
      v_operation := 'UPDATE';
    ELSIF DELETING THEN
      v_operation := 'DELETE';
    END IF;
  END BEFORE STATEMENT;

  -- -----------------------------------------------------------------------
  -- BEFORE EACH ROW: Capture :OLD and :NEW into the collection
  -- -----------------------------------------------------------------------
  BEFORE EACH ROW IS
  BEGIN
    -- Capture transaction_amount changes
    IF INSERTING OR (UPDATING AND :OLD.transaction_amount != :NEW.transaction_amount) THEN
      v_idx := v_idx + 1;
      v_audit_entries(v_idx).transaction_id := NVL(:NEW.transaction_id, :OLD.transaction_id);
      v_audit_entries(v_idx).operation      := v_operation;
      v_audit_entries(v_idx).column_name    := 'TRANSACTION_AMOUNT';
      v_audit_entries(v_idx).old_value      := TO_CHAR(:OLD.transaction_amount);
      v_audit_entries(v_idx).new_value      := TO_CHAR(:NEW.transaction_amount);
      v_audit_entries(v_idx).changed_by     := v_user;
      v_audit_entries(v_idx).changed_date   := v_timestamp;
    END IF;

    -- Capture status changes
    IF INSERTING OR DELETING OR (UPDATING AND NVL(:OLD.status, '~') != NVL(:NEW.status, '~')) THEN
      v_idx := v_idx + 1;
      v_audit_entries(v_idx).transaction_id := NVL(:NEW.transaction_id, :OLD.transaction_id);
      v_audit_entries(v_idx).operation      := v_operation;
      v_audit_entries(v_idx).column_name    := 'STATUS';
      v_audit_entries(v_idx).old_value      := :OLD.status;
      v_audit_entries(v_idx).new_value      := :NEW.status;
      v_audit_entries(v_idx).changed_by     := v_user;
      v_audit_entries(v_idx).changed_date   := v_timestamp;
    END IF;

    -- Capture account_id changes
    IF INSERTING OR DELETING OR (UPDATING AND :OLD.account_id != :NEW.account_id) THEN
      v_idx := v_idx + 1;
      v_audit_entries(v_idx).transaction_id := NVL(:NEW.transaction_id, :OLD.transaction_id);
      v_audit_entries(v_idx).operation      := v_operation;
      v_audit_entries(v_idx).column_name    := 'ACCOUNT_ID';
      v_audit_entries(v_idx).old_value      := TO_CHAR(:OLD.account_id);
      v_audit_entries(v_idx).new_value      := TO_CHAR(:NEW.account_id);
      v_audit_entries(v_idx).changed_by     := v_user;
      v_audit_entries(v_idx).changed_date   := v_timestamp;
    END IF;

    -- Capture description changes
    IF DELETING THEN
      v_idx := v_idx + 1;
      v_audit_entries(v_idx).transaction_id := :OLD.transaction_id;
      v_audit_entries(v_idx).operation      := 'DELETE';
      v_audit_entries(v_idx).column_name    := 'DESCRIPTION';
      v_audit_entries(v_idx).old_value      := :OLD.description;
      v_audit_entries(v_idx).new_value      := NULL;
      v_audit_entries(v_idx).changed_by     := v_user;
      v_audit_entries(v_idx).changed_date   := v_timestamp;
    ELSIF UPDATING AND NVL(:OLD.description, '~') != NVL(:NEW.description, '~') THEN
      v_idx := v_idx + 1;
      v_audit_entries(v_idx).transaction_id := :NEW.transaction_id;
      v_audit_entries(v_idx).operation      := v_operation;
      v_audit_entries(v_idx).column_name    := 'DESCRIPTION';
      v_audit_entries(v_idx).old_value      := :OLD.description;
      v_audit_entries(v_idx).new_value      := :NEW.description;
      v_audit_entries(v_idx).changed_by     := v_user;
      v_audit_entries(v_idx).changed_date   := v_timestamp;
    END IF;
  END BEFORE EACH ROW;

  -- -----------------------------------------------------------------------
  -- AFTER EACH ROW: placeholder for future per-row logic
  -- -----------------------------------------------------------------------
  AFTER EACH ROW IS
  BEGIN
    NULL; -- Reserved for future per-row post-processing
  END AFTER EACH ROW;

  -- -----------------------------------------------------------------------
  -- AFTER STATEMENT: Bulk insert all captured changes into audit_detail
  -- -----------------------------------------------------------------------
  AFTER STATEMENT IS
  BEGIN
    IF v_idx > 0 THEN
      FORALL i IN 1 .. v_idx
        INSERT INTO AUDIT_SCHEMA.audit_detail (
          audit_id,
          transaction_id,
          operation,
          column_name,
          old_value,
          new_value,
          changed_by,
          changed_date
        ) VALUES (
          AUDIT_SCHEMA.AUDIT_SEQ.NEXTVAL,
          v_audit_entries(i).transaction_id,
          v_audit_entries(i).operation,
          v_audit_entries(i).column_name,
          v_audit_entries(i).old_value,
          v_audit_entries(i).new_value,
          v_audit_entries(i).changed_by,
          v_audit_entries(i).changed_date
        );

      -- Log summary row
      INSERT INTO AUDIT_SCHEMA.audit_summary (
        summary_id, table_name, operation, row_count,
        statement_date, executed_by
      ) VALUES (
        AUDIT_SCHEMA.AUDIT_SUMMARY_SEQ.NEXTVAL,
        'FINANCE.MAIN_TRANSACTIONS',
        v_operation,
        v_idx,
        v_timestamp,
        v_user
      );
    END IF;
  END AFTER STATEMENT;

END TRG_MAIN_TRANSACTIONS_AUDIT;
/


-- -------------------------------------------------------------------------
-- Part 2: Supporting Package Body PKG_AUDIT_UTIL
-- -------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE BODY AUDIT_SCHEMA.PKG_AUDIT_UTIL AS

  gc_module CONSTANT VARCHAR2(30) := 'PKG_AUDIT_UTIL';

  -- -----------------------------------------------------------------------
  -- FN_FORMAT_CHANGE
  -- Formats old/new value pair into a human-readable change description
  -- -----------------------------------------------------------------------
  FUNCTION FN_FORMAT_CHANGE (
    p_column_name  IN VARCHAR2,
    p_old_value    IN VARCHAR2,
    p_new_value    IN VARCHAR2,
    p_operation    IN VARCHAR2
  ) RETURN VARCHAR2
  IS
    v_result VARCHAR2(4000);
  BEGIN
    v_result := CASE p_operation
                  WHEN 'INSERT' THEN
                    p_column_name || ' set to [' || NVL(p_new_value, 'NULL') || ']'
                  WHEN 'DELETE' THEN
                    p_column_name || ' was [' || NVL(p_old_value, 'NULL') || ']'
                  WHEN 'UPDATE' THEN
                    p_column_name || ' changed from [' ||
                    NVL(p_old_value, 'NULL') || '] to [' ||
                    NVL(p_new_value, 'NULL') || ']'
                  ELSE
                    p_column_name || ': ' || NVL(p_old_value, 'NULL') ||
                    ' -> ' || NVL(p_new_value, 'NULL')
                END;
    RETURN v_result;
  END FN_FORMAT_CHANGE;

  -- -----------------------------------------------------------------------
  -- PC_PURGE_OLD_AUDIT
  -- Deletes audit records older than the specified cutoff date
  -- -----------------------------------------------------------------------
  PROCEDURE PC_PURGE_OLD_AUDIT (
    p_cutoff_date  IN DATE,
    p_rows_deleted OUT NUMBER
  )
  IS
    v_detail_count  NUMBER := 0;
    v_summary_count NUMBER := 0;
  BEGIN
    -- Delete detail records
    DELETE FROM AUDIT_SCHEMA.audit_detail
     WHERE changed_date < p_cutoff_date;

    v_detail_count := SQL%ROWCOUNT;

    -- Delete summary records
    DELETE FROM AUDIT_SCHEMA.audit_summary
     WHERE statement_date < p_cutoff_date;

    v_summary_count := SQL%ROWCOUNT;

    p_rows_deleted := v_detail_count + v_summary_count;

    -- Log the purge operation
    INSERT INTO AUDIT_SCHEMA.audit_purge_log (
      purge_id, cutoff_date, detail_rows_deleted,
      summary_rows_deleted, purge_date, purged_by
    ) VALUES (
      AUDIT_SCHEMA.PURGE_SEQ.NEXTVAL, p_cutoff_date,
      v_detail_count, v_summary_count, SYSDATE, USER
    );

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-20001,
        'Audit purge failed: ' || SQLERRM);
  END PC_PURGE_OLD_AUDIT;

END PKG_AUDIT_UTIL;
/
