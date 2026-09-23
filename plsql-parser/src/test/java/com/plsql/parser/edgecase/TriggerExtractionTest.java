package com.plsql.parser.edgecase;

import com.plsql.parser.ParserTestBase;
import com.plsql.parser.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Gap #18: Trigger Extraction -- Multi-event, Compound, INSTEAD OF")
public class TriggerExtractionTest extends ParserTestBase {

    // ---------------------------------------------------------------
    // Helper: get the first (and usually only) ParsedObject from result
    // ---------------------------------------------------------------
    private ParsedObject firstObject(ParseResult result) {
        assertFalse(result.getObjects().isEmpty(), "Should have at least one parsed object");
        return result.getObjects().get(0);
    }

    // ---------------------------------------------------------------
    // Helper: collect all sequence names from the dependency summary
    // ---------------------------------------------------------------
    private Set<String> getSequences(ParseResult result) {
        Set<String> seqs = new java.util.LinkedHashSet<>();
        for (ParsedObject obj : result.getObjects()) {
            seqs.addAll(obj.getDependencies().getSequences());
        }
        return seqs;
    }

    @Test
    @DisplayName("Simple BEFORE INSERT trigger -- event and table extracted")
    void testSimpleBeforeInsertTrigger() {
        String sql = """
                CREATE OR REPLACE TRIGGER trg_emp_bi
                BEFORE INSERT ON employees
                FOR EACH ROW
                BEGIN
                  :NEW.id := emp_seq.NEXTVAL;
                END trg_emp_bi;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Simple BEFORE INSERT trigger");
        ParsedObject obj = firstObject(result);

        assertNotNull(obj.getTriggerEvent(), "Trigger event should be set");
        assertTrue(obj.getTriggerEvent().toUpperCase().contains("INSERT"),
                "Trigger event should contain INSERT, got: " + obj.getTriggerEvent());
        assertNotNull(obj.getTriggerTable(), "Trigger table should be set");
        assertTrue(obj.getTriggerTable().toUpperCase().contains("EMPLOYEES"),
                "Trigger table should be EMPLOYEES, got: " + obj.getTriggerTable());
    }

    @Test
    @DisplayName("AFTER UPDATE trigger -- event is UPDATE")
    void testAfterUpdateTrigger() {
        String sql = """
                CREATE OR REPLACE TRIGGER trg_ord_au
                AFTER UPDATE ON orders
                FOR EACH ROW
                BEGIN
                  INSERT INTO order_audit (order_id, action, ts)
                  VALUES (:OLD.order_id, 'UPDATE', SYSDATE);
                END trg_ord_au;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "AFTER UPDATE trigger");
        ParsedObject obj = firstObject(result);

        assertNotNull(obj.getTriggerEvent());
        assertTrue(obj.getTriggerEvent().toUpperCase().contains("UPDATE"),
                "Trigger event should contain UPDATE, got: " + obj.getTriggerEvent());
    }

    @Test
    @DisplayName("Multi-event trigger -- BEFORE INSERT OR UPDATE OR DELETE")
    void testMultiEventTrigger() {
        String sql = """
                CREATE OR REPLACE TRIGGER trg_txn_multi
                BEFORE INSERT OR UPDATE OR DELETE ON transactions
                FOR EACH ROW
                BEGIN
                  IF INSERTING THEN
                    :NEW.created_by := USER;
                  ELSIF UPDATING THEN
                    :NEW.modified_by := USER;
                  ELSIF DELETING THEN
                    INSERT INTO txn_archive (txn_id) VALUES (:OLD.txn_id);
                  END IF;
                END trg_txn_multi;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Multi-event trigger");
        ParsedObject obj = firstObject(result);

        assertNotNull(obj.getTriggerEvent(), "Trigger event should be set");
        String event = obj.getTriggerEvent().toUpperCase();
        // At minimum, the event should capture INSERT (the first event) or all three
        assertTrue(event.contains("INSERT"),
                "Trigger event should contain INSERT, got: " + event);
        assertNotNull(obj.getTriggerTable());
        assertTrue(obj.getTriggerTable().toUpperCase().contains("TRANSACTIONS"),
                "Trigger table should be TRANSACTIONS");
    }

    @Test
    @DisplayName("Compound trigger parses without error")
    void testCompoundTriggerParsesWithoutError() {
        String sql = """
                CREATE OR REPLACE TRIGGER trg_compound
                FOR INSERT ON batch_items
                COMPOUND TRIGGER
                  g_count PLS_INTEGER := 0;

                  BEFORE STATEMENT IS
                  BEGIN
                    g_count := 0;
                  END BEFORE STATEMENT;

                  BEFORE EACH ROW IS
                  BEGIN
                    :NEW.created_date := SYSDATE;
                    g_count := g_count + 1;
                  END BEFORE EACH ROW;

                  AFTER EACH ROW IS
                  BEGIN
                    INSERT INTO item_log (item_id, action)
                    VALUES (:NEW.item_id, 'INSERTED');
                  END AFTER EACH ROW;

                  AFTER STATEMENT IS
                  BEGIN
                    INSERT INTO batch_summary (cnt, ts)
                    VALUES (g_count, SYSDATE);
                  END AFTER STATEMENT;
                END trg_compound;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Compound trigger");
    }

    @Test
    @DisplayName("Compound trigger -- tables inside body are extracted")
    void testCompoundTriggerTablesExtracted() {
        String sql = """
                CREATE OR REPLACE TRIGGER trg_compound_tbl
                FOR UPDATE ON inventory
                COMPOUND TRIGGER

                  AFTER EACH ROW IS
                  BEGIN
                    INSERT INTO inventory_audit (item_id, old_qty, new_qty)
                    VALUES (:OLD.item_id, :OLD.qty, :NEW.qty);
                  END AFTER EACH ROW;

                  AFTER STATEMENT IS
                  BEGIN
                    UPDATE inventory_stats SET last_updated = SYSDATE
                    WHERE warehouse_id = 1;
                  END AFTER STATEMENT;
                END trg_compound_tbl;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Compound trigger tables");
        ParsedObject obj = firstObject(result);

        // Tables from inside compound trigger body should be extracted
        // They may be at object level or in subprograms depending on implementation
        List<TableOperationInfo> objOps = obj.getTableOperations();
        boolean hasInventoryAudit = objOps.stream()
                .anyMatch(t -> "INVENTORY_AUDIT".equalsIgnoreCase(t.getTableName()));
        boolean hasInventoryStats = objOps.stream()
                .anyMatch(t -> "INVENTORY_STATS".equalsIgnoreCase(t.getTableName()));

        // Also check subprograms in case compound trigger sections are modeled as subprograms
        boolean foundInSubs = result.getObjects().stream()
                .flatMap(o -> o.getSubprograms().stream())
                .flatMap(s -> s.getTableOperations().stream())
                .anyMatch(t -> "INVENTORY_AUDIT".equalsIgnoreCase(t.getTableName())
                        || "INVENTORY_STATS".equalsIgnoreCase(t.getTableName()));

        assertTrue(hasInventoryAudit || hasInventoryStats || foundInSubs,
                "Tables from compound trigger body should be extracted somewhere");
    }

    @Test
    @DisplayName("Compound trigger -- sequence usage extracted")
    void testCompoundTriggerSequenceExtracted() {
        String sql = """
                CREATE OR REPLACE TRIGGER trg_compound_seq
                FOR INSERT ON orders
                COMPOUND TRIGGER

                  BEFORE EACH ROW IS
                  BEGIN
                    :NEW.order_id := order_seq.NEXTVAL;
                  END BEFORE EACH ROW;

                  AFTER EACH ROW IS
                  BEGIN
                    NULL;
                  END AFTER EACH ROW;
                END trg_compound_seq;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Compound trigger sequence");

        Set<String> seqs = getSequences(result);
        // Sequence may be detected from trigger body parsing
        // If not in dependencies, check calls
        boolean seqInDeps = seqs.contains("ORDER_SEQ");
        boolean seqInCalls = result.getObjects().stream()
                .flatMap(o -> o.getCalls().stream())
                .anyMatch(c -> "ORDER_SEQ".equalsIgnoreCase(c.getPackageName())
                        && "NEXTVAL".equalsIgnoreCase(c.getName()));
        boolean seqInSubCalls = result.getObjects().stream()
                .flatMap(o -> o.getSubprograms().stream())
                .flatMap(s -> s.getCalls().stream())
                .anyMatch(c -> "ORDER_SEQ".equalsIgnoreCase(c.getPackageName())
                        && "NEXTVAL".equalsIgnoreCase(c.getName()));
        assertTrue(seqInDeps || seqInCalls || seqInSubCalls,
                "ORDER_SEQ should be detected as a sequence in compound trigger");
    }

    @Test
    @DisplayName("INSTEAD OF trigger on view -- event and table extracted")
    void testInsteadOfTriggerOnView() {
        String sql = """
                CREATE OR REPLACE TRIGGER trg_instead_of
                INSTEAD OF INSERT ON my_view
                FOR EACH ROW
                BEGIN
                  INSERT INTO base_table (id, name)
                  VALUES (:NEW.id, :NEW.name);
                END trg_instead_of;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "INSTEAD OF trigger");
        ParsedObject obj = firstObject(result);

        assertNotNull(obj.getTriggerEvent(), "Trigger event should be set");
        assertTrue(obj.getTriggerEvent().toUpperCase().contains("INSERT"),
                "Trigger event should contain INSERT, got: " + obj.getTriggerEvent());
        assertNotNull(obj.getTriggerTable(), "Trigger table should be set");
        assertTrue(obj.getTriggerTable().toUpperCase().contains("MY_VIEW"),
                "Trigger table should be MY_VIEW, got: " + obj.getTriggerTable());
    }

    @Test
    @DisplayName("Trigger with EXCEPTION WHEN OTHERS handler extracted")
    void testTriggerWithExceptionHandler() {
        String sql = """
                CREATE OR REPLACE TRIGGER trg_with_exc
                BEFORE INSERT ON employees
                FOR EACH ROW
                BEGIN
                  :NEW.created_date := SYSDATE;
                EXCEPTION
                  WHEN OTHERS THEN
                    INSERT INTO error_log (msg, ts)
                    VALUES (SQLERRM, SYSDATE);
                END trg_with_exc;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Trigger with exception handler");

        // Exception handlers may be at object level or subprogram level
        boolean hasExcHandler = result.getObjects().stream()
                .anyMatch(o -> !o.getExceptionHandlers().isEmpty());
        boolean hasExcInSubs = result.getObjects().stream()
                .flatMap(o -> o.getSubprograms().stream())
                .anyMatch(s -> !s.getExceptionHandlers().isEmpty());

        assertTrue(hasExcHandler || hasExcInSubs,
                "Trigger should have an exception handler extracted");
    }

    @Test
    @DisplayName("Trigger with :NEW and :OLD references -- no parse error, body tables extracted")
    void testTriggerWithNewOldReferences() {
        String sql = """
                CREATE OR REPLACE TRIGGER trg_new_old
                BEFORE UPDATE ON employees
                FOR EACH ROW
                BEGIN
                  IF :NEW.salary > :OLD.salary * 2 THEN
                    INSERT INTO salary_alerts (emp_id, old_sal, new_sal, ts)
                    VALUES (:OLD.employee_id, :OLD.salary, :NEW.salary, SYSDATE);
                  END IF;
                END trg_new_old;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Trigger with :NEW/:OLD references");

        // Verify table from trigger body is detected
        boolean hasSalaryAlerts = result.getObjects().stream()
                .flatMap(o -> o.getTableOperations().stream())
                .anyMatch(t -> "SALARY_ALERTS".equalsIgnoreCase(t.getTableName()));
        boolean hasSalaryAlertsInSubs = result.getObjects().stream()
                .flatMap(o -> o.getSubprograms().stream())
                .flatMap(s -> s.getTableOperations().stream())
                .anyMatch(t -> "SALARY_ALERTS".equalsIgnoreCase(t.getTableName()));

        assertTrue(hasSalaryAlerts || hasSalaryAlertsInSubs,
                "SALARY_ALERTS from trigger body should be extracted");
    }

    @Test
    @DisplayName("Trigger calls external package procedure -- CallInfo extracted")
    void testTriggerCallsPackageProcedure() {
        String sql = """
                CREATE OR REPLACE TRIGGER trg_audit_call
                AFTER INSERT ON orders
                FOR EACH ROW
                BEGIN
                  pkg_audit.log_change('ORDERS', :NEW.order_id, 'INSERT');
                END trg_audit_call;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Trigger calling package procedure");

        // Check for external call to pkg_audit.log_change
        boolean hasCall = result.getObjects().stream()
                .flatMap(o -> o.getCalls().stream())
                .anyMatch(c -> "PKG_AUDIT".equalsIgnoreCase(c.getPackageName())
                        && "LOG_CHANGE".equalsIgnoreCase(c.getName()));
        boolean hasCallInSubs = result.getObjects().stream()
                .flatMap(o -> o.getSubprograms().stream())
                .flatMap(s -> s.getCalls().stream())
                .anyMatch(c -> "PKG_AUDIT".equalsIgnoreCase(c.getPackageName())
                        && "LOG_CHANGE".equalsIgnoreCase(c.getName()));

        assertTrue(hasCall || hasCallInSubs,
                "Call to PKG_AUDIT.LOG_CHANGE should be extracted from trigger body");
    }

    @Test
    @DisplayName("Trigger with IF :NEW != :OLD conditional INSERT -- table found")
    void testTriggerWithIfNewOld() {
        String sql = """
                CREATE OR REPLACE TRIGGER trg_status_change
                AFTER UPDATE ON accounts
                FOR EACH ROW
                BEGIN
                  IF :NEW.status != :OLD.status THEN
                    INSERT INTO status_history (account_id, old_status, new_status, changed_at)
                    VALUES (:OLD.account_id, :OLD.status, :NEW.status, SYSDATE);
                  END IF;
                END trg_status_change;
                /
                """;
        ParseResult result = parse(sql);
        assertNoParsErrors(result, "Trigger with IF :NEW != :OLD");

        boolean hasStatusHistory = result.getObjects().stream()
                .flatMap(o -> o.getTableOperations().stream())
                .anyMatch(t -> "STATUS_HISTORY".equalsIgnoreCase(t.getTableName()));
        boolean hasStatusHistoryInSubs = result.getObjects().stream()
                .flatMap(o -> o.getSubprograms().stream())
                .flatMap(s -> s.getTableOperations().stream())
                .anyMatch(t -> "STATUS_HISTORY".equalsIgnoreCase(t.getTableName()));

        assertTrue(hasStatusHistory || hasStatusHistoryInSubs,
                "STATUS_HISTORY from conditional trigger body should be extracted");
    }
}
