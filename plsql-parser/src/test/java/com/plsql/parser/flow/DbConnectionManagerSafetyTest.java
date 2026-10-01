package com.plsql.parser.flow;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * W3 DB-impact safety: the token-bucket rate limiter, the read-only statement guard, the statement
 * queryTimeout, and the global circuit breaker that aborts a crawl against a non-responding DB.
 */
@DisplayName("DbConnectionManager DB-impact safety")
class DbConnectionManagerSafetyTest {

    private DbConnectionManager manager() {
        return new DbConnectionManager("jdbc:oracle:thin:@//localhost:1521/XE",
                List.of(new DbConnectionManager.DbUser("U", "P", "test")));
    }

    // ── W3.1 rate limiter ─────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("rate limiter caps QPS: acquiring past the bucket capacity blocks")
    void rateLimiterThrottles() {
        DbConnectionManager m = manager();
        m.setRateLimitQps(4);   // capacity 4 tokens, refill 4/s

        long t0 = System.nanoTime();
        for (int i = 0; i < 6; i++) m.acquireQueryPermit();   // 2 tokens over capacity
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000;

        assertTrue(elapsedMs >= 250,
                "6 permits at 4 qps (capacity 4) must wait ~500ms for 2 refills, took " + elapsedMs + "ms");
    }

    @Test
    @DisplayName("rate limiter disabled (qps<=0) never blocks")
    void rateLimiterDisabled() {
        DbConnectionManager m = manager();
        m.setRateLimitQps(0);
        long t0 = System.nanoTime();
        for (int i = 0; i < 10_000; i++) m.acquireQueryPermit();
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
        assertTrue(elapsedMs < 2_000, "disabled limiter must not throttle");
    }

    // ── W3.3 global circuit breaker ───────────────────────────────────────────────────────────

    @Test
    @DisplayName("circuit breaker trips after N consecutive failures and aborts with a clear message")
    void circuitBreakerTrips() {
        DbConnectionManager m = manager();
        m.setGlobalCircuitThreshold(3);

        m.noteQueryFailure();
        m.noteQueryFailure();
        assertFalse(m.isGlobalCircuitOpen(), "must not trip below the threshold");
        m.noteQueryFailure();

        assertTrue(m.isGlobalCircuitOpen());
        IllegalStateException ex = assertThrows(IllegalStateException.class, m::throwIfGlobalCircuitOpen);
        assertTrue(ex.getMessage().contains("not responding"), ex.getMessage());
        assertTrue(ex.getMessage().contains("protect"), ex.getMessage());
        // getAnyConnection fails fast too (no connection attempt to a dead DB).
        assertThrows(IllegalStateException.class, m::getAnyConnection);
    }

    @Test
    @DisplayName("a success resets the consecutive-failure counter")
    void successResetsBreaker() {
        DbConnectionManager m = manager();
        m.setGlobalCircuitThreshold(3);
        m.noteQueryFailure();
        m.noteQueryFailure();
        m.noteQuerySuccess();
        m.noteQueryFailure();
        m.noteQueryFailure();
        assertFalse(m.isGlobalCircuitOpen(), "success must reset the counter");
    }

    // ── W3.4 read-only guarantee ──────────────────────────────────────────────────────────────

    @Test
    @DisplayName("read-only guard rejects non-SELECT statements")
    void readOnlyGuardRejectsNonSelect() {
        assertThrows(IllegalArgumentException.class, () -> DbConnectionManager.assertReadOnly("DELETE FROM T"));
        assertThrows(IllegalArgumentException.class, () -> DbConnectionManager.assertReadOnly("UPDATE T SET X=1"));
        assertThrows(IllegalArgumentException.class, () -> DbConnectionManager.assertReadOnly("DROP TABLE T"));
        assertThrows(IllegalArgumentException.class, () -> DbConnectionManager.assertReadOnly("INSERT INTO T VALUES (1)"));
        assertThrows(IllegalArgumentException.class, () -> DbConnectionManager.assertReadOnly("BEGIN NULL; END;"));
        assertThrows(IllegalArgumentException.class, () -> DbConnectionManager.assertReadOnly(null));
        // SELECT / WITH pass.
        assertDoesNotThrow(() -> DbConnectionManager.assertReadOnly("SELECT 1 FROM DUAL"));
        assertDoesNotThrow(() -> DbConnectionManager.assertReadOnly("  with x as (select 1) select * from x"));
    }

    @Test
    @DisplayName("prepareStatement refuses non-SELECT before touching the connection")
    void prepareStatementRefusesNonSelect() throws Exception {
        DbConnectionManager m = manager();
        Connection conn = mock(Connection.class);
        assertThrows(IllegalArgumentException.class, () -> m.prepareStatement(conn, "DELETE FROM T"));
        verify(conn, never()).prepareStatement(anyString());
    }

    @Test
    @DisplayName("statement creator applies the configured queryTimeout")
    void statementCreatorAppliesTimeout() throws Exception {
        DbConnectionManager m = manager();
        m.setStatementTimeoutSeconds(17);
        Connection conn = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        when(conn.prepareStatement(anyString())).thenReturn(ps);

        PreparedStatement got = m.prepareStatement(conn, "SELECT 1 FROM DUAL");

        assertSame(ps, got);
        verify(ps).setQueryTimeout(17);
    }

    @Test
    @DisplayName("createStatement applies the timeout and the read-only guard")
    void createStatementAppliesTimeout() throws Exception {
        DbConnectionManager m = manager();
        m.setStatementTimeoutSeconds(5);
        Connection conn = mock(Connection.class);
        Statement st = mock(Statement.class);
        when(conn.createStatement()).thenReturn(st);

        m.createStatement(conn, "SELECT 1 FROM DUAL");
        verify(st).setQueryTimeout(5);
        assertThrows(IllegalArgumentException.class, () -> m.createStatement(conn, "TRUNCATE TABLE T"));
    }
}
