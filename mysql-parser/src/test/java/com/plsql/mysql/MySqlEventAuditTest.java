package com.plsql.mysql;

import com.plsql.mysql.engine.MySqlGrammarEngine;
import com.plsql.mysql.model.MySqlRoutineInfo;
import com.plsql.mysql.model.MySqlTableOpInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CREATE EVENT support in mysql-parser — MySQL's scheduled job, exercised against the real
 * MySqlGrammarEngine / MySqlAnalysisVisitor / MySqlParser.g4.
 *
 * <p>Before this, the MySQL grammar had no {@code EVENT} token and no {@code createEvent} rule, so
 * {@code compilationUnit : (createRoutine | …) EOF} rejected CREATE EVENT at the first token after
 * CREATE and the event never reached the visitor — an entire class of estate roots (the MySQL
 * scheduler) was invisible. The grammar now has a {@code createEvent} alternative, and the DO body
 * is walked by the ordinary procedure-body visitor, so a CALL inside an event becomes a real
 * outbound edge.
 */
public class MySqlEventAuditTest {

    @Test
    @DisplayName("EVENT 1: CREATE EVENT … ON SCHEDULE EVERY … DO BEGIN CALL … END")
    void eventEveryDayCallingProcedure() {
        String sql = """
                CREATE EVENT ev_refresh_customer_sales
                ON SCHEDULE EVERY 1 DAY
                DO
                BEGIN
                  CALL erp_usp_refresh_mv();
                END
                """;
        MySqlRoutineInfo r = runOk("EVENT-1 (EVERY 1 DAY, CALL)", sql);
        assertEquals(MySqlRoutineInfo.RoutineType.EVENT, r.getType());
        assertEquals("ev_refresh_customer_sales", r.getName());
        assertEquals("EVERY1DAY", r.getEventSchedule());
        assertTrue(r.getCalls().stream().anyMatch(c -> "erp_usp_refresh_mv".equals(c.getName())),
                "event DO body CALL not captured, got: " + r.getCalls());
    }

    @Test
    @DisplayName("EVENT 2: DISABLE + ON COMPLETION PRESERVE + body DML captured")
    void eventDisabledPreserveWithDml() {
        String sql = """
                CREATE EVENT ev_archive_audit
                ON SCHEDULE EVERY 1 WEEK
                ON COMPLETION PRESERVE
                DISABLE
                COMMENT 'weekly archive'
                DO
                BEGIN
                  INSERT INTO erp_audit_archive (audit_id, entity)
                  SELECT audit_id, entity FROM erp_audit_log;
                END
                """;
        MySqlRoutineInfo r = runOk("EVENT-2 (DISABLE, DML body)", sql);
        assertEquals(MySqlRoutineInfo.RoutineType.EVENT, r.getType());
        assertEquals("ev_archive_audit", r.getName());
        assertEquals(Boolean.FALSE, r.getEventEnabled());
        assertTrue(r.getTableOps().stream().anyMatch(
                        t -> "erp_audit_archive".equals(t.getTableName())
                                && t.getDmlType() == MySqlTableOpInfo.DmlType.INSERT),
                "event body INSERT not captured, got: " + r.getTableOps().stream()
                        .map(t -> t.getDmlType() + ":" + t.getTableName()).toList());
    }

    @Test
    @DisplayName("EVENT 3: ON SCHEDULE AT … is parseable and the event is enabled by default")
    void eventAtSchedule() {
        String sql = """
                CREATE EVENT ev_once
                ON SCHEDULE AT '2030-01-01 00:00:00'
                DO CALL erp_usp_refresh_mv();
                """;
        MySqlRoutineInfo r = runOk("EVENT-3 (AT schedule, single-statement body)", sql);
        assertEquals(MySqlRoutineInfo.RoutineType.EVENT, r.getType());
        assertEquals("ev_once", r.getName());
        assertTrue(r.getEventSchedule().toUpperCase().startsWith("AT"),
                "expected an AT schedule, got: " + r.getEventSchedule());
        assertTrue(r.getCalls().stream().anyMatch(c -> "erp_usp_refresh_mv".equals(c.getName())),
                "single-statement DO CALL not captured, got: " + r.getCalls());
    }

    @Test
    @DisplayName("EVENT 4: a routine body using `event`/`schedule`/`day` as identifiers still parses")
    void identifiersNamedLikeEventKeywordsStillWork() {
        String sql = """
                CREATE PROCEDURE p_uses_event_words()
                BEGIN
                  DECLARE event INT DEFAULT 1;
                  DECLARE schedule INT DEFAULT 2;
                  DECLARE day INT DEFAULT 3;
                  SET event = schedule + day;
                END
                """;
        MySqlRoutineInfo r = runOk("EVENT-4 (keywords as identifiers)", sql);
        assertEquals(MySqlRoutineInfo.RoutineType.PROCEDURE, r.getType());
        assertFalse(r.getVariables().isEmpty(), "expected the DECLAREs to be captured");
    }

    private MySqlRoutineInfo runOk(String label, String sql) {
        MySqlRoutineInfo r = new MySqlGrammarEngine().parse(sql, label, null);
        System.out.println("[" + label + "] type=" + r.getType()
                + " name=" + r.getName()
                + " schedule=" + r.getEventSchedule()
                + " enabled=" + r.getEventEnabled()
                + " calls=" + r.getCalls().stream().map(c -> c.getName()).toList()
                + " tableOps=" + r.getTableOps().stream()
                        .map(t -> t.getDmlType() + ":" + t.getTableName()).toList()
                + " errors=" + r.getErrors());
        assertTrue(r.getErrors().isEmpty(),
                "[" + label + "] expected a clean parse, got errors: " + r.getErrors());
        return r;
    }
}
