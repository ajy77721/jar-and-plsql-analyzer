package com.analyzer.auth;

import com.analyzer.storage.sso.SsoSessionRegistry;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.io.IOException;

/**
 * The single authentication gate for the application. It is registered by
 * {@link AuthInterceptorConfig}; without that registration this class is inert (the handler chain
 * never invokes it), so the two are only meaningful together.
 *
 * <p>Two checks are applied, in order:
 * <ol>
 *   <li><b>API key</b> — the session attribute {@value #SESSION_KEY}, set by the {@code /api/auth/*}
 *       login flow. Its absence produces either a 401 JSON body (for {@code /api/**}) or a redirect
 *       to {@code /login} (for everything else).</li>
 *   <li><b>AWS SSO</b> — for data endpoints under {@code /api/} (other than the public
 *       {@code /api/auth/*} routes), the request's session must also be known to
 *       {@link SsoSessionRegistry}. A session that has the API key but never completed the AWS SSO
 *       device flow is rejected with {@code 401} and the body {@code {"ok":false,"error":...}} —
 *       the front end reacts by opening the SSO login modal. Non-API paths deliberately skip this
 *       gate: an SSO-less visitor may still load a page, and that page's own API calls are what get
 *       the 401.</li>
 * </ol>
 *
 * <p>No credential, token, device code or other secret is ever read, logged or echoed here — the
 * interceptor only inspects the presence of the session key and asks the registry whether the
 * opaque session id is authenticated.
 */
@Component
public class ApiKeyAuthInterceptor implements HandlerInterceptor {

    public static final String SESSION_KEY = "auth.api.key";

    private static final String SSO_REQUIRED_401_BODY =
            "{\"ok\":false,\"error\":\"Authenticate via AWS SSO first\"}";
    private static final String API_KEY_401_BODY =
            "{\"error\":\"Not authenticated\",\"code\":401}";

    private final ObjectProvider<SsoSessionRegistry> ssoSessionRegistry;

    /**
     * Escape hatch for deployments that have not (yet) adopted AWS SSO — chiefly the pre-SSO local
     * development workflow.
     *
     * <p>When {@code app.auth.sso.required} is {@code true} (the default) every data endpoint under
     * {@code /api/} demands a session that {@link SsoSessionRegistry} recognises, in addition to the
     * API key. Setting the property to {@code false} skips <em>only</em> that second check: the API
     * key gate above still runs exactly as before and is never disabled by this property. It exists
     * so that a developer running the tool locally, with no IAM Identity Center instance to log in
     * to, can keep using the application while the SSO rollout is in progress — it is a
     * compatibility switch, not a security boundary, and production deployments must leave it at
     * {@code true}.
     *
     * <p>The property is read through {@code @Value} with an inline default of {@code true}, so an
     * unset property behaves as if it were set to {@code true}; nothing needs to be added to
     * {@code application.properties} for the secure default to take effect.
     */
    @Value("${app.auth.sso.required:true}")
    private boolean ssoRequired;

    public ApiKeyAuthInterceptor(ObjectProvider<SsoSessionRegistry> ssoSessionRegistry) {
        this.ssoSessionRegistry = ssoSessionRegistry;
    }

    private static boolean isPublic(String path) {
        return path.equals("/login")
                || path.equals("/login.html")
                || path.startsWith("/api/auth/")
                || path.equals("/favicon.ico")
                || path.equals("/error")
                // The admin portal is self-gating: its static page and /api/admin/** are guarded
                // by the admin username/password login (X-Admin-Key issued by /api/admin/login),
                // so the api-key/SSO gates here must not pre-empt them — otherwise the portal
                // page would redirect to /login and its login call would 401 before the admin
                // gate ever ran.
                || path.startsWith("/admin-portal")
                || path.startsWith("/api/admin/")
                // Docs/manual content is jar-packaged help material served to the browsing UI —
                // not user data. It ships inside the jar and must be readable before sign-in,
                // like every other static asset. ("/api/docs" is the list endpoint — no slash.)
                || path.equals("/api/docs")
                || path.startsWith("/api/docs/");
    }

    @Override
    public boolean preHandle(HttpServletRequest req, HttpServletResponse res, Object handler) throws IOException {
        String path = req.getRequestURI();
        if (isPublic(path)) return true;

        // The AWS SSO device-flow login IS the application's sign-in now. The old Anthropic-key
        // gate was removed as an entry requirement: its login page no longer renders a form, so
        // keeping it as gate 1 locked every user out of every data API (JAR uploads included) with
        // a 401 they had no UI to clear. A session that completed the AWS SSO flow satisfies the
        // whole gate; with the escape hatch off (local dev, no Identity Center) the app is open,
        // exactly as before the SSO rollout.
        if (!ssoGateSatisfied(path, req.getSession(false))) {
            writeJson(res, HttpServletResponse.SC_UNAUTHORIZED, SSO_REQUIRED_401_BODY);
            return false;
        }
        return true;
    }

    /**
     * The AWS SSO half of the gate. It applies only to {@code /api/} paths outside the public
     * {@code /api/auth/*} routes; anything else (a static page, an asset) passes untouched so the
     * browser can load the UI that will then drive the SSO login.
     *
     * <p>If the registry bean is absent — SSO support not compiled in, or its component scan
     * disabled — the request is allowed. This is a deliberate fail-open: the API key gate has
     * already run, and the alternative would be to lock out every deployment that does not ship the
     * registry.
     */
    private boolean ssoGateSatisfied(String path, HttpSession session) {
        if (!ssoRequired) return true;
        if (!path.startsWith("/api/") || path.startsWith("/api/auth/")) return true;

        SsoSessionRegistry registry = ssoSessionRegistry.getIfAvailable();
        if (registry == null) return true;
        if (session == null) return false;

        return registry.isAuthenticated(session.getId());
    }

    private static void writeJson(HttpServletResponse res, int status, String body) throws IOException {
        res.setStatus(status);
        res.setContentType("application/json;charset=UTF-8");
        // Use the output stream, not the writer: something earlier in the chain (e.g. static-resource
        // or compression handling) may already have called getOutputStream(), and calling getWriter()
        // afterwards throws IllegalStateException — turning our 401 into a 500.
        byte[] payload = body.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        res.setContentLength(payload.length);
        res.getOutputStream().write(payload);
    }
}
