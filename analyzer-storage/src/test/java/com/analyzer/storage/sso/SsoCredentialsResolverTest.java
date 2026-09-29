package com.analyzer.storage.sso;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Fallback contract of {@link SsoCredentialsResolver} after the F4/F5 fixes: the ambient default
 * chain is used only when it can actually resolve credentials. A resolvable SSO identity that has no
 * usable credentials — or a session-less S3 operation with no ambient credentials at all — is
 * refused with {@link NotAuthenticatedException} instead of silently signing as the wrong or no
 * principal. The CLI/local path (ambient credentials that resolve) is unchanged.
 */
class SsoCredentialsResolverTest {

    private static final AwsCredentials AMBIENT = AwsBasicCredentials.create("AKIAAMBIENT", "ambient-secret");
    private static final AwsCredentials ALICE = AwsBasicCredentials.create("AKIAALICE", "alice-secret");

    private final AwsSsoLoginService login = mock(AwsSsoLoginService.class);
    private final SsoSessionRegistry registry = mock(SsoSessionRegistry.class);
    private final CurrentHttpSession session = mock(CurrentHttpSession.class);

    private SsoCredentialsResolver resolverWith(AwsCredentialsProvider fallback) {
        return new SsoCredentialsResolver(login, registry, session, fallback);
    }

    private static AwsCredentialsProvider resolving(AwsCredentials credentials) {
        return () -> credentials;
    }

    private static AwsCredentialsProvider failing() {
        return () -> {
            throw new IllegalStateException("no ambient credentials on this host");
        };
    }

    private static SsoSessionState loggedInWith(AwsCredentials credentials) {
        SsoSessionState state = mock(SsoSessionState.class);
        when(state.loggedIn()).thenReturn(true);
        when(state.iamCredentials()).thenReturn(credentials);
        return state;
    }

    @Test
    void noSessionUsesAmbientCredentialsWhenTheyResolve() {
        when(session.currentSessionId()).thenReturn(null);
        assertEquals(AMBIENT, resolverWith(resolving(AMBIENT)).resolveCredentials(),
                "the CLI/local path must keep using a resolvable default chain");
    }

    @Test
    void noSessionAndNoAmbientCredentialsIsRefused() {
        when(session.currentSessionId()).thenReturn(null);
        assertThrows(NotAuthenticatedException.class,
                () -> resolverWith(failing()).resolveCredentials());
    }

    @Test
    void sessionWithoutUsableCredentialsUsesAmbientWhenTheyResolve() {
        when(session.currentSessionId()).thenReturn("sess-1");
        when(registry.get("sess-1")).thenReturn(null);
        assertEquals(AMBIENT, resolverWith(resolving(AMBIENT)).resolveCredentials());
    }

    @Test
    void sessionWithoutUsableCredentialsAndNoAmbientIsRefused() {
        when(session.currentSessionId()).thenReturn("sess-1");
        when(registry.get("sess-1")).thenReturn(null);
        assertThrows(NotAuthenticatedException.class,
                () -> resolverWith(failing()).resolveCredentials());
    }

    @Test
    void capturedJobCredentialsWinAndNeverTouchTheAmbientChain() {
        try (SsoJobContext ignored = SsoJobContext.install(
                null, new CapturedCredentials("sess-job", ALICE))) {
            // The ambient provider would throw if consulted — it must not be.
            assertEquals(ALICE, resolverWith(failing()).resolveCredentials());
        }
    }

    @Test
    void resolveForSessionReturnsThatUsersCredentials() {
        SsoSessionState state = loggedInWith(ALICE);
        when(registry.get("sess-1")).thenReturn(state);
        AwsCredentialsProvider provider = resolverWith(failing()).resolveFor("sess-1");
        assertEquals(ALICE, provider.resolveCredentials());
    }

    @Test
    void resolveForSessionWithNoUsableCredentialsIsRefusedEvenIfAmbientWouldResolve() {
        // F5: a per-user client must not fall back to the ambient principal when its own session's
        // credentials are gone (logout mid-job), even though the ambient chain could sign.
        when(registry.get("sess-1")).thenReturn(null);
        AwsCredentialsProvider provider = resolverWith(resolving(AMBIENT)).resolveFor("sess-1");
        assertThrows(NotAuthenticatedException.class, provider::resolveCredentials);
    }

    @Test
    void resolveForNullReturnsTheAmbientProviderUnchanged() {
        AwsCredentialsProvider fallback = resolving(AMBIENT);
        assertSame(fallback, resolverWith(fallback).resolveFor((String) null),
                "a store built without a session keeps the legacy ambient provider");
    }
}
