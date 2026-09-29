package com.analyzer.auth;

import com.analyzer.storage.sso.CurrentHttpSession;
import com.analyzer.storage.sso.SsoSessionRegistry;
import com.analyzer.storage.sso.SsoStoreSwapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The fail-fast connect guard: a session that logged in via AWS SSO but has not connected a bucket
 * must be rejected with a 409 <em>before</em> a job starts, while every legacy path (no session, no
 * login, local/MongoDB storage) is left exactly as it was.
 */
class ConnectionGuardTest {

    private final SsoSessionRegistry registry = mock(SsoSessionRegistry.class);
    private final SsoStoreSwapper swapper = mock(SsoStoreSwapper.class);
    private final CurrentHttpSession session = mock(CurrentHttpSession.class);

    private ConnectionGuard guard(String mode) {
        return new ConnectionGuard(registry, swapper, session, mode, mode);
    }

    @Test
    @DisplayName("logged in but not connected is a 409 when the feature is S3-backed")
    void loggedInButNotConnectedIsRejected() {
        when(session.currentSessionId()).thenReturn("sid");
        when(swapper.storesFor("sid")).thenReturn(null);
        when(registry.isAuthenticated("sid")).thenReturn(true);

        ResponseStatusException e = assertThrows(ResponseStatusException.class,
                () -> guard("s3").requireJarConnected());
        assertEquals(HttpStatus.CONFLICT, e.getStatusCode());
        assertEquals(ConnectionGuard.CONNECT_MESSAGE, e.getReason());
    }

    @Test
    @DisplayName("a connected session passes")
    void connectedSessionPasses() {
        when(session.currentSessionId()).thenReturn("sid");
        when(swapper.storesFor("sid"))
                .thenReturn(new SsoStoreSwapper.BuiltStores(null, null, "b", "r", null));
        assertDoesNotThrow(() -> guard("s3").requireJarConnected());
    }

    @Test
    @DisplayName("local / MongoDB storage needs no connection")
    void nonS3ModeNeedsNoConnection() {
        when(session.currentSessionId()).thenReturn("sid");
        when(swapper.storesFor("sid")).thenReturn(null);
        when(registry.isAuthenticated("sid")).thenReturn(true);
        assertDoesNotThrow(() -> guard("local").requireJarConnected());
        assertDoesNotThrow(() -> guard("mongodb").requireJarConnected());
    }

    @Test
    @DisplayName("no request-bound session is the legacy path")
    void noSessionIsLegacy() {
        when(session.currentSessionId()).thenReturn(null);
        assertDoesNotThrow(() -> guard("s3").requireJarConnected());
    }

    @Test
    @DisplayName("an unauthenticated session is left to the auth interceptor")
    void notLoggedInIsLeftAlone() {
        when(session.currentSessionId()).thenReturn("sid");
        when(swapper.storesFor("sid")).thenReturn(null);
        when(registry.isAuthenticated("sid")).thenReturn(false);
        assertDoesNotThrow(() -> guard("hybrid").requireJarConnected());
    }

    @Test
    @DisplayName("each feature is checked against its own storage mode")
    void featureModesAreIndependent() {
        when(session.currentSessionId()).thenReturn("sid");
        when(swapper.storesFor("sid")).thenReturn(null);
        when(registry.isAuthenticated("sid")).thenReturn(true);

        ConnectionGuard g = new ConnectionGuard(registry, swapper, session, "local", "s3");
        assertDoesNotThrow(g::requireJarConnected, "jar is local, so no connect is required");
        assertThrows(ResponseStatusException.class, g::requireParserConnected,
                "parser is S3, so the connect is required");
    }

    @Test
    @DisplayName("an unowned resource stays readable; a routing identity falls back to 'shared'")
    void ownershipAndIdentity() {
        assertTrue(guard("s3").owns(null));
        assertTrue(guard("s3").owns(""));
        when(session.currentSessionId()).thenReturn(null);
        assertEquals(ConnectionGuard.SHARED, guard("s3").routingIdentityKey());
    }
}
