package com.analyzer.integration;

import com.analyzer.auth.ApiKeyAuthInterceptor;
import com.analyzer.auth.AwsSsoController;
import com.analyzer.auth.LoginWarmUpTrigger;
import com.analyzer.queue.ApiExceptionAdvice;
import com.analyzer.storage.sso.AwsIamIdentityResolver;
import com.analyzer.storage.sso.AwsSsoLoginService;
import com.analyzer.storage.sso.CurrentHttpSession;
import com.analyzer.storage.sso.NotAuthenticatedException;
import com.analyzer.storage.sso.SsoCredentialsResolver;
import com.analyzer.storage.sso.SsoIdentity;
import com.analyzer.storage.sso.SsoSessionRegistry;
import com.analyzer.storage.sso.SsoSessionState;
import com.analyzer.storage.sso.SsoStoreSwapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * F1 + F5 + the auth half of F6, as one end-to-end flow against the <b>real</b>
 * {@link AwsSsoController}, the real {@link SsoSessionRegistry} and the real session state — only
 * the two external boundaries (the AWS SSO device flow and the S3 bucket probe) are mocked, which
 * is exactly what "the mock SSO" and "the mock bucket" mean in this mission.
 *
 * <p>It walks the whole sign-in: {@code /start} issues a device code, {@code /poll} completes the
 * login, {@code /connect} verifies the bucket and reports capabilities, and {@code /session}
 * reports the connected target. F5 then proves two browsers are fully isolated, and F6 proves an
 * unauthenticated API request is a 401 and an expired SSO session is a clean re-auth signal rather
 * than a storage error.
 */
@DisplayName("Integration F1/F5/F6: SSO login -> session -> bucket connect -> capabilities")
class EndToEndAuthFlowTest {

    /** The controller plus the collaborators a test wants to inspect. */
    private record Harness(SsoSessionRegistry registry, AwsSsoLoginService login,
                           SsoStoreSwapper swapper, AwsSsoController controller) {
    }

    private Harness harness() {
        SsoSessionRegistry registry = new SsoSessionRegistry(mock(ApplicationEventPublisher.class));

        AwsSsoLoginService login = mock(AwsSsoLoginService.class);
        when(login.startLogin(any(), any())).thenReturn(new AwsSsoLoginService.DeviceFlowStart(
                "https://device.sso.us-east-1.amazonaws.com/",
                "https://device.sso.us-east-1.amazonaws.com/?user_code=CODE-123",
                "CODE-123", 5, System.currentTimeMillis() + 600_000L));
        when(login.poll(any())).thenAnswer(inv -> {
            SsoSessionState state = inv.getArgument(0);
            // The real flow marks the session logged in and resolves the identity at poll-complete;
            // the stub does the same so /connect's "must be logged in" gate is exercised for real.
            state.loggedIn(true);
            state.identity(new SsoIdentity("Ada Lovelace", "ada@example.com",
                    "123456789012", "Acme", "AdminRole", null));
            return new AwsSsoLoginService.PollResult("complete", "123456789012", "Acme", "AdminRole");
        });

        SsoStoreSwapper swapper = mock(SsoStoreSwapper.class);
        when(swapper.connect(anyString(), any(), any())).thenAnswer(inv -> {
            String bucket = inv.getArgument(1) == null ? "default-bucket" : inv.getArgument(1);
            String region = inv.getArgument(2) == null ? "us-east-1" : inv.getArgument(2);
            return new SsoStoreSwapper.ConnectResult(bucket, region, 2,
                    new SsoStoreSwapper.Capabilities(true, true, true, true, null));
        });
        when(swapper.testCapabilities(anyString(), any(), any()))
                .thenReturn(new SsoStoreSwapper.Capabilities(true, true, true, true, null));

        AwsSsoController controller = new AwsSsoController(login, swapper, registry,
                mock(AwsIamIdentityResolver.class), mock(LoginWarmUpTrigger.class));
        return new Harness(registry, login, swapper, controller);
    }

    /** start -> poll -> connect, returning the connect response. */
    private Map<String, Object> signInAndConnect(Harness h, MockHttpServletRequest req,
                                                 String bucket, String region) {
        Map<String, Object> start = h.controller().start(
                Map.of("startUrl", "https://d-123.awsapps.com/start"), req);
        assertEquals(Boolean.TRUE, start.get("started"));
        assertNotNull(start.get("userCode"), "the device flow must hand the user a code");
        assertEquals("complete", h.controller().poll(req).get("status"));
        return h.controller().connect(Map.of("bucket", bucket, "region", region), req);
    }

    @Test
    @DisplayName("F1: login -> session -> connect -> capabilities all report real state")
    void fullLoginConnectFlow() {
        Harness h = harness();
        MockHttpServletRequest req = new MockHttpServletRequest();

        Map<String, Object> connect = signInAndConnect(h, req, "my-bucket", "us-east-1");

        assertEquals(Boolean.TRUE, connect.get("ok"));
        assertEquals("my-bucket", connect.get("bucket"));
        assertEquals("us-east-1", connect.get("region"));
        assertEquals("Ada Lovelace", connect.get("name"));
        assertEquals("123456789012", connect.get("accountId"));
        assertEquals("AdminRole", connect.get("roleName"));
        @SuppressWarnings("unchecked")
        Map<String, Object> caps = (Map<String, Object>) connect.get("capabilities");
        assertEquals(Boolean.TRUE, caps.get("read"));
        assertEquals(Boolean.TRUE, caps.get("upload"));
        assertEquals(Boolean.TRUE, caps.get("write"));

        Map<String, Object> session = h.controller().session(req);
        assertEquals(Boolean.TRUE, session.get("connected"));
        assertEquals("my-bucket", session.get("bucket"));
        assertEquals("us-east-1", session.get("region"));
        assertEquals("sso", session.get("source"));
        @SuppressWarnings("unchecked")
        Map<String, Object> sessionCaps = (Map<String, Object>) session.get("capabilities");
        assertEquals(Boolean.TRUE, sessionCaps.get("read"));

        // No credential material may ever appear in a login/connect response.
        for (String secret : new String[]{"accessKeyId", "secretKey", "sessionToken",
                "accessToken", "refreshToken", "deviceCode"}) {
            assertFalse(connect.containsKey(secret), secret + " must never be returned");
            assertFalse(session.containsKey(secret), secret + " must never be returned");
        }
    }

    @Test
    @DisplayName("F1: an unknown session reports disconnected with null identity")
    void sessionBeforeLoginIsDisconnected() {
        Harness h = harness();
        MockHttpServletRequest req = new MockHttpServletRequest();
        Map<String, Object> session = h.controller().session(req);
        assertEquals(Boolean.FALSE, session.get("connected"));
        assertEquals(null, session.get("name"));
        assertEquals(null, session.get("capabilities"));
    }

    @Test
    @DisplayName("F5: two browsers connecting different buckets are fully isolated")
    void twoUsersAreIsolated() {
        Harness h = harness();
        MockHttpServletRequest alice = new MockHttpServletRequest();
        MockHttpServletRequest bob = new MockHttpServletRequest();

        assertEquals(Boolean.TRUE, signInAndConnect(h, alice, "alice-bucket", "us-east-1").get("ok"));
        assertEquals(Boolean.TRUE, signInAndConnect(h, bob, "bob-bucket", "eu-west-1").get("ok"));

        String aliceId = alice.getSession(false).getId();
        String bobId = bob.getSession(false).getId();
        assertNotEquals(aliceId, bobId, "each browser must get its own session");

        assertEquals("alice-bucket", h.controller().session(alice).get("bucket"));
        assertEquals("bob-bucket", h.controller().session(bob).get("bucket"));

        // Logging Alice out must not disturb Bob's connected target.
        h.controller().logout(alice);
        assertEquals(Boolean.FALSE, h.controller().session(alice).get("connected"));
        assertEquals(Boolean.TRUE, h.controller().session(bob).get("connected"));
        assertEquals("bob-bucket", h.controller().session(bob).get("bucket"));
        verify(h.swapper()).forget(aliceId);
    }

    @Test
    @DisplayName("F5: two browsers logging in and connecting concurrently stay isolated")
    void concurrentUsersAreIsolated() throws Exception {
        Harness h = harness();
        int n = 2;
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(n);
        java.util.concurrent.CountDownLatch ready = new java.util.concurrent.CountDownLatch(n);
        java.util.concurrent.CountDownLatch go = new java.util.concurrent.CountDownLatch(1);
        java.util.List<java.util.concurrent.Future<MockHttpServletRequest>> futures = new java.util.ArrayList<>();
        try {
            for (int i = 0; i < n; i++) {
                final String bucket = "bucket-" + i;
                final String region = i == 0 ? "us-east-1" : "eu-west-1";
                final String sessionId = "concurrent-session-" + i;
                futures.add(pool.submit(() -> {
                    MockHttpServletRequest req = new MockHttpServletRequest();
                    req.setSession(new org.springframework.mock.web.MockHttpSession(
                            new org.springframework.mock.web.MockServletContext(), sessionId));
                    ready.countDown();
                    go.await();
                    Map<String, Object> out = signInAndConnect(h, req, bucket, region);
                    assertEquals(Boolean.TRUE, out.get("ok"));
                    return req;
                }));
            }
            assertTrue(ready.await(10, java.util.concurrent.TimeUnit.SECONDS),
                    "both logins must reach the barrier");
            go.countDown();
            for (int i = 0; i < n; i++) {
                MockHttpServletRequest req = futures.get(i).get(10, java.util.concurrent.TimeUnit.SECONDS);
                assertEquals("bucket-" + i, h.controller().session(req).get("bucket"),
                        "concurrent logins must not cross targets");
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @DisplayName("F6: connect before login is refused, not silently accepted")
    void connectBeforeLoginIsRefused() {
        Harness h = harness();
        MockHttpServletRequest req = new MockHttpServletRequest();
        Map<String, Object> out = h.controller().connect(Map.of("bucket", "b"), req);
        assertEquals(Boolean.FALSE, out.get("ok"));
        assertTrue(String.valueOf(out.get("error")).contains("Log in"));
    }

    // ── the auth gate ─────────────────────────────────────────────────────────

    private static ObjectProvider<SsoSessionRegistry> provider(SsoSessionRegistry registry) {
        return new ObjectProvider<>() {
            @Override public SsoSessionRegistry getObject() { return registry; }
            @Override public SsoSessionRegistry getObject(Object... args) { return registry; }
            @Override public SsoSessionRegistry getIfAvailable() { return registry; }
            @Override public SsoSessionRegistry getIfUnique() { return registry; }
        };
    }

    @Test
    @DisplayName("F6: an unauthenticated /api/** request is a 401 with the SSO re-auth body")
    void unauthenticatedApiRequestIs401() throws Exception {
        SsoSessionRegistry registry = new SsoSessionRegistry(mock(ApplicationEventPublisher.class));
        ApiKeyAuthInterceptor interceptor =
                new ApiKeyAuthInterceptor(provider(registry), "true", "", "s3", "s3");

        MockHttpServletRequest req = new MockHttpServletRequest("GET", "/api/jar/jars");
        MockHttpServletResponse res = new MockHttpServletResponse();

        assertFalse(interceptor.preHandle(req, res, new Object()),
                "an unauthenticated data request must not reach the handler");
        assertEquals(401, res.getStatus());
        assertTrue(res.getContentAsString().contains("Authenticate via AWS SSO first"),
                "the body must carry the constant the UI keys on");
    }

    @Test
    @DisplayName("F6: expired SSO credentials are a clean re-auth signal (401), not a storage fault")
    void expiredCredentialsSignalReAuth() {
        SsoSessionRegistry registry = new SsoSessionRegistry(mock(ApplicationEventPublisher.class));
        String sessionId = "sess-expired";
        SsoSessionState state = registry.getOrCreate(sessionId);
        state.loggedIn(true);
        state.identity(new SsoIdentity("Ada", "ada@example.com",
                "123456789012", "Acme", "AdminRole", null));

        AwsSsoLoginService login = mock(AwsSsoLoginService.class);
        // The refresh-token grant has lapsed: no usable access token.
        when(login.accessToken(state)).thenReturn(null);

        CurrentHttpSession currentSession = mock(CurrentHttpSession.class);
        when(currentSession.currentSessionId()).thenReturn(sessionId);

        SsoCredentialsResolver resolver = new SsoCredentialsResolver(login, registry, currentSession);

        assertThrows(NotAuthenticatedException.class, resolver::resolveCredentials,
                "an expired session must raise the re-auth exception, not fall back silently");
        assertFalse(registry.isAuthenticated(sessionId),
                "the dead session must be ended so the UI can re-authenticate");

        // And the advice maps exactly that type to 401, never to a 5xx/503.
        var response = new ApiExceptionAdvice().onNotAuthenticated(new NotAuthenticatedException());
        assertEquals(401, response.getStatusCode().value());
        assertEquals(Boolean.FALSE, response.getBody().get("ok"));
        assertEquals("Authenticate via AWS SSO first", response.getBody().get("error"));
    }
}
