package com.analyzer.auth;

import com.analyzer.storage.sso.CapturedCredentials;
import com.analyzer.storage.sso.CapturedStores;
import com.analyzer.storage.sso.CurrentHttpSession;
import com.analyzer.storage.sso.SsoCredentialsResolver;
import com.analyzer.storage.sso.SsoStoreSwapper;
import com.analyzer.usercfg.UserConnectionStore;
import com.plsqlanalyzer.web.parser.service.SidecarWarmUpService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The warm-on-login hand-off: a completed login queues the auto-reconnect to its saved AWS target
 * (so a redeploy no longer silently drops the connection) plus the credential capture and sweep,
 * and returns without blocking the poll response. Mock-level — the sweep's own behaviour is covered
 * by {@code SidecarWarmUpServiceTest}; this test is about the reconnect lookup, the capture wiring,
 * and the off-thread dispatch.
 */
class LoginWarmUpTriggerTest {

    private final SsoStoreSwapper storeSwapper = mock(SsoStoreSwapper.class);
    private final SsoCredentialsResolver credentialsResolver = mock(SsoCredentialsResolver.class);
    private final SidecarWarmUpService warmUp = mock(SidecarWarmUpService.class);
    private final UserConnectionStore userConnectionStore = mock(UserConnectionStore.class);
    private final CurrentHttpSession currentSession = mock(CurrentHttpSession.class);

    /** Same-thread executor, so the queued body runs before {@code onLoginComplete} returns. */
    private final LoginWarmUpTrigger trigger =
            new LoginWarmUpTrigger(storeSwapper, credentialsResolver, warmUp, userConnectionStore,
                    currentSession, Runnable::run);

    @Test
    @DisplayName("a completed login starts a sweep with that session's captured credentials")
    void triggersSweepWithCapturedCredentials() {
        CapturedStores stores = mock(CapturedStores.class);
        CapturedCredentials captured = new CapturedCredentials("session-1",
                AwsSessionCredentials.create("AK", "SK", "ST"));
        when(storeSwapper.captureCurrent()).thenReturn(stores);
        when(credentialsResolver.captureCurrent()).thenReturn(captured);

        trigger.onLoginComplete("session-1");

        verify(warmUp).triggerLoginSweep(stores, captured, "session-1");
    }

    @Test
    @DisplayName("a login whose credentials cannot be captured is still a successful login")
    void neverThrowsWhenCaptureFails() {
        when(credentialsResolver.captureCurrent()).thenThrow(new IllegalStateException("no session"));

        // Must not propagate: a warm-up can never be allowed to fail a login.
        trigger.onLoginComplete("session-1");

        verify(warmUp, never()).triggerLoginSweep(any(), any(), any());
    }

    @Test
    @DisplayName("a completed login auto-reconnects the session to the target saved under its identity")
    void reconnectsFromSavedTarget() {
        // The saved config is found by identity for this login; the hook must re-run the connect path
        // for the SAME session with the saved bucket/region — no user action.
        when(userConnectionStore.findAwsTargetForCurrentUser())
                .thenReturn(new UserConnectionStore.AwsTarget("my-bucket", "ap-south-1"));

        trigger.onLoginComplete("session-1");

        verify(storeSwapper).autoReconnect("session-1", "my-bucket", "ap-south-1");
    }

    @Test
    @DisplayName("a login with no saved target does not attempt an auto-reconnect")
    void noSavedTargetSkipsReconnect() {
        when(userConnectionStore.findAwsTargetForCurrentUser()).thenReturn(null);

        trigger.onLoginComplete("session-1");

        verify(storeSwapper, never()).autoReconnect(anyString(), any(), any());
    }

    @Test
    @DisplayName("a saved target is applied before the warm-up capture, and a lookup failure never fails the login")
    void reconnectLookupFailureIsSwallowed() {
        when(userConnectionStore.findAwsTargetForCurrentUser())
                .thenThrow(new IllegalStateException("store unreachable"));

        // Must not propagate, and the sweep hand-off must still run with whatever it can capture.
        trigger.onLoginComplete("session-1");

        verify(storeSwapper, never()).autoReconnect(anyString(), any(), any());
        verify(credentialsResolver).captureCurrent();
    }

    @Test
    @DisplayName("the reconnect + sweep are dispatched off the calling thread, so the poll never waits")
    void workIsDispatchedAsynchronously() {
        // A queuing executor that runs nothing until told: proves onLoginComplete only submits.
        List<Runnable> queued = new ArrayList<>();
        LoginWarmUpTrigger async = new LoginWarmUpTrigger(storeSwapper, credentialsResolver, warmUp,
                userConnectionStore, currentSession, (Executor) queued::add);
        when(userConnectionStore.findAwsTargetForCurrentUser())
                .thenReturn(new UserConnectionStore.AwsTarget("my-bucket", "ap-south-1"));

        async.onLoginComplete("session-1");

        // Nothing ran on the caller's thread yet — the poll response is free to return.
        assertEquals(1, queued.size(), "the warm-up must be queued, not run inline");
        verify(storeSwapper, never()).autoReconnect(anyString(), any(), any());

        queued.get(0).run();

        // The worker re-binds the login's session so the request-bound captures resolve the caller.
        verify(currentSession).propagate("session-1");
        verify(storeSwapper).autoReconnect("session-1", "my-bucket", "ap-south-1");
        verify(credentialsResolver).captureCurrent();
    }
}
