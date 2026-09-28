package com.analyzer.auth;

import com.analyzer.storage.sso.CapturedCredentials;
import com.analyzer.storage.sso.CapturedStores;
import com.analyzer.storage.sso.SsoCredentialsResolver;
import com.analyzer.storage.sso.SsoStoreSwapper;
import com.plsqlanalyzer.web.parser.service.SidecarWarmUpService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The warm-on-login hand-off: a completed login captures the user's own credentials on the login
 * thread and hands them to the sweep. Mock-level — the sweep's own behaviour is covered by
 * {@code SidecarWarmUpServiceTest}; this test is about the capture and the wiring.
 */
class LoginWarmUpTriggerTest {

    private final SsoStoreSwapper storeSwapper = mock(SsoStoreSwapper.class);
    private final SsoCredentialsResolver credentialsResolver = mock(SsoCredentialsResolver.class);
    private final SidecarWarmUpService warmUp = mock(SidecarWarmUpService.class);

    private final LoginWarmUpTrigger trigger =
            new LoginWarmUpTrigger(storeSwapper, credentialsResolver, warmUp);

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
}
