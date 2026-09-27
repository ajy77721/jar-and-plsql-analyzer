package com.analyzer.storage.sso;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The propagation side of {@link CurrentHttpSession}: a session id bound for a fan-out must be
 * visible to {@link CurrentHttpSession#currentSessionId()} on the thread that binds it, must nest,
 * and must be gone once the handle is closed — otherwise a pooled worker would carry one request's
 * identity into the next.
 *
 * <p>No Spring web context is bound here, so the reflective request lookup answers {@code null};
 * that is the exact "no request context" state a pool worker starts in, which makes the propagated
 * value the only thing under test.
 */
class CurrentHttpSessionTest {

    @Test
    void propagateBindsNestsAndRestores() throws Exception {
        CurrentHttpSession session = new CurrentHttpSession();
        assertNull(session.currentSessionId(), "no request context, no session");

        try (AutoCloseable outer = session.propagate("sid-1")) {
            assertEquals("sid-1", session.currentSessionId());

            try (AutoCloseable inner = session.propagate("sid-2")) {
                assertEquals("sid-2", session.currentSessionId());
            }
            assertEquals("sid-1", session.currentSessionId(), "inner close restores the outer value");
        }

        assertNull(session.currentSessionId(), "close removes the propagated value entirely");
    }

    @Test
    void aNullOrBlankIdBindsNothing() throws Exception {
        CurrentHttpSession session = new CurrentHttpSession();
        assertNull(session.currentSessionId());

        try (AutoCloseable unused = session.propagate(null)) {
            assertNull(session.currentSessionId());
        }
        try (AutoCloseable unused = session.propagate("  ")) {
            assertNull(session.currentSessionId());
        }
    }
}
