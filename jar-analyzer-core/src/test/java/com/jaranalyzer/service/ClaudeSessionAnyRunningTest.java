package com.jaranalyzer.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@code anyRunning()} must agree, in every state, with what the browser used to compute for
 * itself: {@code listSessions().some(s => s.status === 'RUNNING')}.
 *
 * <p>That client-side reduction was the reason a full session list was fetched every 30 s by every
 * open page, and twice per tick on the Corrections tab. Replacing it with a boolean endpoint is
 * only safe if the boolean is the same boolean — including after a session completes, which is the
 * transition the sidebar dot exists to show.
 */
class ClaudeSessionAnyRunningTest {

    private ClaudeSessionManager manager() {
        // The runner is only reached when a session actually executes; registerSession does not.
        return new ClaudeSessionManager(null);
    }

    @Test
    @DisplayName("false with no sessions at all — the steady state a poll spends its life in")
    void idleServerReportsNotRunning() {
        assertFalse(manager().anyRunning());
    }

    @Test
    @DisplayName("true while a session runs, false once it finishes")
    void tracksTheRunningTransition() {
        ClaudeSessionManager m = manager();
        String id = m.registerSession("orders.jar", ClaudeSessionManager.SessionType.FULL_SCAN, "scan");

        assertTrue(m.anyRunning(), "a registered session is RUNNING");
        m.completeSession(id, true, null);
        assertFalse(m.anyRunning(), "a completed session must not keep the dot lit");
    }

    @Test
    @DisplayName("agrees with the client-side reduction it replaces, in every state")
    void matchesTheReductionItReplaces() {
        ClaudeSessionManager m = manager();
        String a = m.registerSession("a.jar", ClaudeSessionManager.SessionType.FULL_SCAN, "a");
        String b = m.registerSession("b.jar", ClaudeSessionManager.SessionType.RESCAN, "b");

        assertEquals(someRunning(m), m.anyRunning());
        m.completeSession(a, false, "boom");
        assertEquals(someRunning(m), m.anyRunning(), "one failed, one still running");
        assertTrue(m.anyRunning());
        m.completeSession(b, true, null);
        assertEquals(someRunning(m), m.anyRunning());
        assertFalse(m.anyRunning());
    }

    /** Exactly what jar/js/app.js did before this endpoint existed. */
    private static boolean someRunning(ClaudeSessionManager m) {
        return m.listSessions().stream()
                .anyMatch(s -> "RUNNING".equals(String.valueOf(s.get("status"))));
    }
}
