package com.plsql.parser.multidb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Scheduled-job catalog fetch per dialect, against mocked JDBC objects — the job counterpart of
 * {@link SourceFetchStrategyMockTest}. Each dialect's {@code listJobs} is asserted to return a
 * {@link SourceFetchStrategy.JobRef} carrying the schedule, enabled flag and target routine, so a
 * scheduled job can become a first-class node.
 */
@ExtendWith(MockitoExtension.class)
class JobFetchStrategyMockTest {

    @Mock Connection conn;
    @Mock PreparedStatement ps;

    // ── Oracle: DBMS_SCHEDULER + legacy DBMS_JOB ─────────────────────────────

    @Test
    void oracle_listJobs_returnsSchedulerAndLegacyJobs() throws Exception {
        ResultSet scheduler = mock(ResultSet.class);
        ResultSet legacy = mock(ResultSet.class);
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(scheduler, legacy);

        when(scheduler.next()).thenReturn(true, false);
        when(scheduler.getString("JOB_NAME")).thenReturn("NIGHTLY_LOAD");
        when(scheduler.getString("REPEAT_INTERVAL")).thenReturn("FREQ=DAILY;BYHOUR=2");
        when(scheduler.getString("ENABLED")).thenReturn("TRUE");
        when(scheduler.getString("JOB_ACTION")).thenReturn("BEGIN erp_load(); END;");
        when(scheduler.getString("PROGRAM_NAME")).thenReturn(null);

        when(legacy.next()).thenReturn(true, false);
        when(legacy.getString("JOB")).thenReturn("42");
        when(legacy.getString("WHAT")).thenReturn("erp_usp_purge;");
        when(legacy.getString("INTERVAL")).thenReturn("SYSDATE+1");
        when(legacy.getString("BROKEN")).thenReturn("N");

        List<SourceFetchStrategy.JobRef> jobs =
                new OracleSourceFetchStrategy().listJobs(conn, "ERP");

        assertEquals(2, jobs.size());
        SourceFetchStrategy.JobRef schedulerJob = jobs.get(0);
        assertEquals("NIGHTLY_LOAD", schedulerJob.name);
        assertEquals("FREQ=DAILY;BYHOUR=2", schedulerJob.schedule);
        assertEquals(Boolean.TRUE, schedulerJob.enabled);
        assertEquals("JOB", schedulerJob.type);
        assertEquals("erp_load", schedulerJob.target);

        SourceFetchStrategy.JobRef legacyJob = jobs.get(1);
        assertEquals("DBMS_JOB_42", legacyJob.name);
        assertEquals(Boolean.TRUE, legacyJob.enabled);
        assertEquals("erp_usp_purge", legacyJob.target);
    }

    @Test
    void oracle_fetchJobAction_readsUserSourceTypeJob() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString("TEXT")).thenReturn("BEGIN\n", "  erp_load();\n", "END;\n");

        String action = new OracleSourceFetchStrategy().fetchJobAction(conn, "NIGHTLY_LOAD", "ERP");

        assertNotNull(action);
        assertTrue(action.contains("erp_load"));
    }

    // ── SQL Server Agent ─────────────────────────────────────────────────────

    @Test
    void sqlserver_listJobs_returnsAgentJobWithStepCommand() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("job_name")).thenReturn("Nightly ETL");
        when(rs.getBoolean("job_enabled")).thenReturn(true);
        when(rs.getString("step_command")).thenReturn("EXEC dbo.usp_load_orders;");
        when(rs.getString("schedule_name")).thenReturn("Nightly at 2am");

        List<SourceFetchStrategy.JobRef> jobs =
                new SqlServerSourceFetchStrategy().listJobs(conn, "dbo");

        assertEquals(1, jobs.size());
        assertEquals("Nightly ETL", jobs.get(0).name);
        assertEquals("Nightly at 2am", jobs.get(0).schedule);
        assertEquals(Boolean.TRUE, jobs.get(0).enabled);
        assertEquals("dbo.usp_load_orders", jobs.get(0).target);
        assertEquals("JOB", jobs.get(0).type);
    }

    // ── Postgres: pg_cron ────────────────────────────────────────────────────

    @Test
    void postgres_listJobs_returnsPgCronJob() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getLong("jobid")).thenReturn(7L);
        when(rs.getString("schedule")).thenReturn("0 2 * * *");
        when(rs.getString("command")).thenReturn("CALL refresh_reporting();");
        when(rs.getString("database")).thenReturn("erp");
        when(rs.getBoolean("active")).thenReturn(true);

        List<SourceFetchStrategy.JobRef> jobs =
                new PostgresSourceFetchStrategy().listJobs(conn, "public");

        assertEquals(1, jobs.size());
        assertEquals("pg_cron_job_7", jobs.get(0).name);
        assertEquals("0 2 * * *", jobs.get(0).schedule);
        assertEquals(Boolean.TRUE, jobs.get(0).enabled);
        assertEquals("refresh_reporting", jobs.get(0).target);
    }

    @Test
    void postgres_listJobs_missingPgCron_degradesToEmpty() throws Exception {
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenThrow(new SQLException("relation \"cron.job\" does not exist"));

        List<SourceFetchStrategy.JobRef> jobs =
                new PostgresSourceFetchStrategy().listJobs(conn, "public");

        assertNotNull(jobs);
        assertTrue(jobs.isEmpty(), "an absent pg_cron catalog must degrade to no jobs, not throw");
    }

    // ── MySQL: EVENT ─────────────────────────────────────────────────────────

    @Test
    void mysql_listJobs_returnsEvent() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(conn.prepareStatement(anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("EVENT_NAME")).thenReturn("ev_refresh_customer_sales");
        when(rs.getString("EVENT_SCHEMA")).thenReturn("erp");
        when(rs.getString("EVENT_DEFINITION")).thenReturn("BEGIN CALL erp_usp_refresh_mv(); END");
        when(rs.getString("INTERVAL_VALUE")).thenReturn("1");
        when(rs.getString("INTERVAL_FIELD")).thenReturn("DAY");
        when(rs.getString("EXECUTE_AT")).thenReturn(null);
        when(rs.getString("STATUS")).thenReturn("ENABLED");

        List<SourceFetchStrategy.JobRef> jobs =
                new MySqlSourceFetchStrategy().listJobs(conn, "erp");

        assertEquals(1, jobs.size());
        assertEquals("ev_refresh_customer_sales", jobs.get(0).name);
        assertEquals("EVERY 1 DAY", jobs.get(0).schedule);
        assertEquals(Boolean.TRUE, jobs.get(0).enabled);
        assertEquals("EVENT", jobs.get(0).type);
        assertEquals("erp_usp_refresh_mv", jobs.get(0).target);
    }
}
