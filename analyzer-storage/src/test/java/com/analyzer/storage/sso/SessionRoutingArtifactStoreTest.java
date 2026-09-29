package com.analyzer.storage.sso;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Routing contract of {@link SessionRoutingArtifactStore} after the F1/F3 fixes: the shared default
 * store may only be served to a thread with <em>no</em> resolvable identity. An identity that is
 * resolvable but incomplete — logged in without a connected bucket, or a background job snapshot
 * that carries a session id but no store — must be refused, never silently downgraded to the shared
 * target.
 */
class SessionRoutingArtifactStoreTest {

    private static final ArtifactKey KEY = ArtifactKey.of("jar", "web-1.0.0-SNAPSHOT", "node_1.json");

    private final ArtifactStore defaultStore = mock(ArtifactStore.class);
    private final SsoStoreSwapper swapper = mock(SsoStoreSwapper.class);
    private final SsoSessionRegistry registry = mock(SsoSessionRegistry.class);
    private final CurrentHttpSession currentSession = mock(CurrentHttpSession.class);

    private SessionRoutingArtifactStore store() {
        return new SessionRoutingArtifactStore("jar", defaultStore, swapper, registry, currentSession);
    }

    private static SsoSessionState loggedIn() {
        SsoSessionState state = mock(SsoSessionState.class);
        when(state.loggedIn()).thenReturn(true);
        return state;
    }

    @Test
    void noSessionIdentityAtAllFallsBackToTheDefaultStore() {
        // The legacy background-job-without-a-snapshot / CLI / pre-SSO-local path must keep working.
        when(currentSession.currentSessionId()).thenReturn(null);
        when(defaultStore.exists(KEY)).thenReturn(true);

        assertTrue(store().exists(KEY), "a thread with no session identity keeps the default fallback");
        verify(defaultStore).exists(KEY);
    }

    @Test
    void sessionKnownButNotLoggedInIsRefused() {
        when(currentSession.currentSessionId()).thenReturn("sess-1");
        when(registry.get("sess-1")).thenReturn(null);

        assertThrows(NotAuthenticatedException.class, () -> store().exists(KEY));
        verify(defaultStore, never()).exists(KEY);
    }

    @Test
    void loggedInButNoBucketConnectedIsRefusedNotServedFromTheDefaultStore() {
        // The live F1 failure: a signed-in user with no connected bucket used to be served the
        // shared default store, writing their artifacts under the wrong principal.
        when(currentSession.currentSessionId()).thenReturn("sess-1");
        SsoSessionState state = loggedIn();
        when(registry.get("sess-1")).thenReturn(state);
        when(swapper.storesFor("sess-1")).thenReturn(null);

        NotAuthenticatedException e =
                assertThrows(NotAuthenticatedException.class, () -> store().exists(KEY));
        assertTrue(e.getMessage().contains("bucket connected"),
                "message must tell the user to connect an account: " + e.getMessage());
        verify(defaultStore, never()).exists(KEY);
    }

    @Test
    void loggedInAndConnectedRoutesToTheUsersOwnStore() {
        ArtifactStore perUser = mock(ArtifactStore.class);
        when(currentSession.currentSessionId()).thenReturn("sess-1");
        SsoSessionState state = loggedIn();
        when(registry.get("sess-1")).thenReturn(state);
        when(swapper.storesFor("sess-1"))
                .thenReturn(new SsoStoreSwapper.BuiltStores(null, perUser, "b", "r", null));
        when(perUser.exists(KEY)).thenReturn(true);

        assertTrue(store().exists(KEY));
        verify(perUser).exists(KEY);
        verify(defaultStore, never()).exists(KEY);
    }

    @Test
    void credentialsOnlyJobSnapshotWithNoStoreIsRefused() {
        // F3: SsoJobContext installed (identity resolvable) but storeFor(name) is null — a
        // credentials-only snapshot. It must not be served the shared default store.
        when(currentSession.currentSessionId()).thenReturn(null);
        try (SsoJobContext ignored = SsoJobContext.install(
                null, new CapturedCredentials("sess-job", AwsBasicCredentials.create("AK", "SK")))) {
            assertThrows(NotAuthenticatedException.class, () -> store().exists(KEY));
        }
        verify(defaultStore, never()).exists(KEY);
    }

    @Test
    void jobSnapshotHoldingNoStoreForThisFeatureIsRefused() {
        // A stores snapshot that resolves the identity but never built this feature's store.
        when(currentSession.currentSessionId()).thenReturn(null);
        try (SsoJobContext ignored = SsoJobContext.install(
                new CapturedStores("sess-job", null, null, "b", "r", null), null)) {
            assertThrows(NotAuthenticatedException.class, () -> store().exists(KEY));
        }
        verify(defaultStore, never()).exists(KEY);
    }

    @Test
    void jobSnapshotStoreWinsOverRequestDiscovery() {
        ArtifactStore jobStore = mock(ArtifactStore.class);
        when(jobStore.exists(KEY)).thenReturn(true);
        try (SsoJobContext ignored = SsoJobContext.install(
                new CapturedStores("sess-job", null, jobStore, "b", "r", null), null)) {
            assertTrue(store().exists(KEY));
        }
        verify(jobStore).exists(KEY);
        verify(defaultStore, never()).exists(KEY);
    }

    @Test
    void incompleteIdentityMessageIsStable() {
        assertEquals(
                "Logged in but no AWS bucket connected — connect your account first "
                        + "(was going to fall back to the shared default store)",
                SessionRoutingArtifactStore.NO_BUCKET_CONNECTED);
    }
}
