package com.analyzer.auth;

import com.analyzer.storage.sso.SsoSessionRegistry;
import jakarta.servlet.http.HttpSession;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockHttpSession;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The mode-aware auth decision: full-local / pure-Mongo deployments have nothing user-scoped to
 * protect, so every API is open with no cookie; s3/hybrid keep the SSO page-lock; and an explicit
 * {@code app.auth.required} overrides the mode-derived default in either direction.
 */
class ApiKeyAuthInterceptorModeTest {

    private static ObjectProvider<SsoSessionRegistry> provider(SsoSessionRegistry registry) {
        return new ObjectProvider<>() {
            @Override public SsoSessionRegistry getObject() { return registry; }
            @Override public SsoSessionRegistry getObject(Object... args) { return registry; }
            @Override public SsoSessionRegistry getIfAvailable() { return registry; }
            @Override public SsoSessionRegistry getIfUnique() { return registry; }
        };
    }

    /** Builds the interceptor exactly as Spring would, with the raw property values. */
    private static ApiKeyAuthInterceptor interceptor(String authRequired, String legacySsoRequired,
                                                     String jarMode, String parserMode,
                                                     SsoSessionRegistry registry) {
        return new ApiKeyAuthInterceptor(provider(registry), authRequired, legacySsoRequired,
                jarMode, parserMode);
    }

    private static boolean preHandle(ApiKeyAuthInterceptor i, String method, String path,
                                     HttpSession session) throws Exception {
        MockHttpServletRequest req = new MockHttpServletRequest(method, path);
        if (session != null) req.setSession((MockHttpSession) session);
        return i.preHandle(req, new MockHttpServletResponse(), new Object());
    }

    // ── the property decision table ──────────────────────────────────────────

    @Test
    @DisplayName("auto-derives: s3/hybrid need auth; local/mongodb do not")
    void autoDerivation() {
        assertTrue(ApiKeyAuthInterceptor.resolveAuthRequired("", "", "s3", "s3"));
        assertTrue(ApiKeyAuthInterceptor.resolveAuthRequired("", "", "hybrid", "hybrid"));
        assertFalse(ApiKeyAuthInterceptor.resolveAuthRequired("", "", "local", "local"));
        assertFalse(ApiKeyAuthInterceptor.resolveAuthRequired("", "", "mongodb", "mongodb"));
    }

    @Test
    @DisplayName("auto-derives true when EITHER feature is s3/hybrid (the mixed deployment)")
    void autoDerivationHonoursPerFeatureOverrides() {
        assertTrue(ApiKeyAuthInterceptor.resolveAuthRequired("", "", "s3", "local"),
                "jar is S3-backed, so there is user-scoped data to protect");
        assertTrue(ApiKeyAuthInterceptor.resolveAuthRequired("", "", "local", "hybrid"),
                "parser is hybrid, so there is user-scoped data to protect");
    }

    @Test
    @DisplayName("an explicit app.auth.required wins in both directions")
    void explicitOverrideWins() {
        assertFalse(ApiKeyAuthInterceptor.resolveAuthRequired("false", "", "s3", "hybrid"));
        assertTrue(ApiKeyAuthInterceptor.resolveAuthRequired("true", "", "local", "mongodb"));
        assertFalse(ApiKeyAuthInterceptor.resolveAuthRequired("FALSE", "", "s3", "s3"));
    }

    @Test
    @DisplayName("the legacy app.auth.sso.required is honoured when app.auth.required is unset")
    void legacyEscapeHatchHonoured() {
        assertFalse(ApiKeyAuthInterceptor.resolveAuthRequired("", "false", "s3", "s3"));
        assertTrue(ApiKeyAuthInterceptor.resolveAuthRequired("", "true", "local", "local"));
        // explicit new property beats the legacy one
        assertTrue(ApiKeyAuthInterceptor.resolveAuthRequired("true", "false", "s3", "s3"));
    }

    @Test
    @DisplayName("an unparseable value fails at startup, naming the property")
    void invalidValueFailsFast() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ApiKeyAuthInterceptor.resolveAuthRequired("yes", "", "local", "local"));
        assertTrue(e.getMessage().contains("app.auth.required"), e.getMessage());
    }

    // ── preHandle behaviour per mode ─────────────────────────────────────────

    @Test
    @DisplayName("local mode: every API GET/POST is allowed with no session at all")
    void localModeAllowsEverythingWithoutASession() throws Exception {
        ApiKeyAuthInterceptor i = interceptor("", "", "local", "local", mock(SsoSessionRegistry.class));
        assertTrue(preHandle(i, "GET", "/api/queue", null));
        assertTrue(preHandle(i, "POST", "/api/jar/analyze", null));
        assertTrue(preHandle(i, "GET", "/api/plsql/analyses", null));
        assertTrue(preHandle(i, "GET", "/jar/", null), "page navigations are open too");
    }

    @Test
    @DisplayName("mongodb mode: also open, with no session")
    void mongoModeAllowsEverything() throws Exception {
        ApiKeyAuthInterceptor i = interceptor("", "", "mongodb", "mongodb", mock(SsoSessionRegistry.class));
        assertTrue(preHandle(i, "GET", "/api/queue", null));
        assertTrue(preHandle(i, "POST", "/api/parser/analyze", null));
    }

    @Test
    @DisplayName("s3 mode: an unauthenticated API call is 401, a page navigation is redirected")
    void s3ModeStillLocks() throws Exception {
        SsoSessionRegistry registry = mock(SsoSessionRegistry.class);
        ApiKeyAuthInterceptor i = interceptor("", "", "s3", "s3", registry);

        MockHttpServletResponse res = new MockHttpServletResponse();
        MockHttpServletRequest req = new MockHttpServletRequest("GET", "/api/queue");
        assertFalse(i.preHandle(req, res, new Object()));
        assertEquals(401, res.getStatus());

        MockHttpServletResponse page = new MockHttpServletResponse();
        MockHttpServletRequest nav = new MockHttpServletRequest("GET", "/jar/");
        assertFalse(i.preHandle(nav, page, new Object()));
        assertEquals(302, page.getStatus());
        assertTrue(page.getRedirectedUrl().endsWith("/?login=1"), page.getRedirectedUrl());
    }

    @Test
    @DisplayName("s3 mode: a logged-in session passes; the SSO endpoints stay public")
    void s3ModeLoggedInPasses() throws Exception {
        SsoSessionRegistry registry = mock(SsoSessionRegistry.class);
        when(registry.isAuthenticated("sid")).thenReturn(true);
        ApiKeyAuthInterceptor i = interceptor("", "", "s3", "s3", registry);

        MockHttpSession session = new MockHttpSession(null, "sid");
        assertTrue(preHandle(i, "GET", "/api/queue", session));
        // SSO endpoints are public in every mode, so an optional login is always reachable.
        assertTrue(preHandle(i, "POST", "/api/auth/aws/start", null));
        assertTrue(preHandle(i, "GET", "/api/auth/aws/session", null));
    }

    @Test
    @DisplayName("explicit override is respected by preHandle")
    void explicitOverrideRespectedByPreHandle() throws Exception {
        // force auth OFF in s3 mode
        ApiKeyAuthInterceptor off = interceptor("false", "", "s3", "s3", mock(SsoSessionRegistry.class));
        assertTrue(preHandle(off, "GET", "/api/queue", null));

        // force auth ON in local mode
        ApiKeyAuthInterceptor on = interceptor("true", "", "local", "local", mock(SsoSessionRegistry.class));
        MockHttpServletResponse res = new MockHttpServletResponse();
        assertFalse(on.preHandle(new MockHttpServletRequest("GET", "/api/queue"), res, new Object()));
        assertEquals(401, res.getStatus());
    }

    @Test
    @DisplayName("when the registry bean is absent the gate fails open (SSO not compiled in)")
    void registryAbsentFailsOpen() throws Exception {
        ApiKeyAuthInterceptor i = interceptor("", "", "s3", "s3", null);
        assertTrue(preHandle(i, "GET", "/api/queue", null));
    }
}
