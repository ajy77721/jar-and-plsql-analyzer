package com.analyzer.auth;

import com.analyzer.storage.StorageMode;
import com.analyzer.storage.sso.SsoSessionRegistry;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 * <h2>Mode-aware: is authentication required at all?</h2>
 *
 * <p>Authentication is only meaningful when there is something user-scoped to protect. In a pure
 * {@code local} or {@code mongodb} deployment every artifact lives in one shared store that has no
 * per-user partition and no bucket to sign for, so there is nothing for a login to unlock — the
 * gate must not page-lock the tool. In {@code s3}/{@code hybrid} the artifacts are per-user
 * (each SSO session writes to its own bucket through its own credentials), so a session is
 * mandatory.
 *
 * <p>That decision is the {@code app.auth.required} property, resolved once at startup by
 * {@link #resolveAuthRequired}:
 * <ol>
 *   <li><b>Explicit {@code app.auth.required=true|false}</b> always wins, whatever the mode.</li>
 *   <li>Otherwise the legacy <b>{@code app.auth.sso.required}</b> escape hatch, when explicitly
 *       set, is honoured (so an existing local-dev deployment that turned the old SSO gate off is
 *       not silently re-locked).</li>
 *   <li>Otherwise it is <b>auto-derived from the effective storage modes</b>: if either the JAR
 *       analyzer or the PL/SQL parser resolves to {@code s3}/{@code hybrid} — honouring the
 *       per-feature {@code app.storage.jar.mode}/{@code app.storage.parser.mode} overrides — auth
 *       is required; if both are {@code local}/{@code mongodb}, it is not.</li>
 * </ol>
 *
 * <p>When auth is <em>not</em> required this interceptor allows every request unconditionally:
 * no {@code 401} on {@code /api/**}, no page-lock redirect. The AWS SSO endpoints
 * ({@code /api/auth/aws/**}) remain reachable, so an optional login is still possible — but in a
 * pure local/Mongo deployment there is no per-user store for it to bind to (see docs/modes.md), so
 * a later switch to S3 is a re-connection, not a migration.
 *
 * <h2>The gate itself</h2>
 *
 * <p>When auth <em>is</em> required, the request's session must be known to
 * {@link SsoSessionRegistry} — i.e. it completed the AWS SSO device flow. A session that has not is
 * rejected with {@code 401} and the body {@code {"ok":false,"error":...}} for {@code /api/**} (the
 * front end reacts by opening the SSO login modal) or bounced to {@code /?login=1} for a page
 * navigation; asset requests pass so whatever page the visitor is on can render the login. Public
 * paths ({@code /}, {@code /login}, {@code /api/auth/*}, the admin portal and the docs/manual
 * endpoints) never reach the gate.
 *
 * <p>No credential, token, device code or other secret is ever read, logged or echoed here — the
 * interceptor only asks the registry whether the opaque session id is authenticated.
 */
@Component
public class ApiKeyAuthInterceptor implements HandlerInterceptor {

    public static final String SESSION_KEY = "auth.api.key";

    private static final Logger log = LoggerFactory.getLogger(ApiKeyAuthInterceptor.class);

    private static final String SSO_REQUIRED_401_BODY =
            "{\"ok\":false,\"error\":\"Authenticate via AWS SSO first\"}";

    private final ObjectProvider<SsoSessionRegistry> ssoSessionRegistry;

    /**
     * Whether the SSO gate runs at all, resolved once at startup by {@link #resolveAuthRequired}.
     * {@code false} makes {@link #preHandle} a passthrough: no {@code 401} page-lock on
     * {@code /api/**}, no redirect for page navigations, and the SSO endpoints stay reachable for
     * an optional login.
     */
    private final boolean authRequired;

    /**
     * Resolves the mode-aware gate from the raw property strings. Kept as a static, side-effect-free
     * function so the whole decision table is unit-testable without a Spring context.
     *
     * <p>Precedence, in order:
     * <ol>
     *   <li>an explicit {@code app.auth.required} (blank/unset = not explicit) — always wins;</li>
     *   <li>an explicit legacy {@code app.auth.sso.required} — honoured for backwards compatibility
     *       with the pre-mode-aware escape hatch;</li>
     *   <li>auto-derivation: required iff the JAR <em>or</em> parser effective mode is
     *       {@code s3}/{@code hybrid}.</li>
     * </ol>
     *
     * <p>An unparseable value (anything other than {@code true}/{@code false}, case-insensitive)
     * fails the context at startup with the offending property named, matching how an unknown
     * {@code app.storage.mode} is surfaced rather than silently defaulted.
     */
    public static boolean resolveAuthRequired(String authRequired, String legacySsoRequired,
                                       String jarMode, String parserMode) {
        Boolean explicit = parseTriState(authRequired, "app.auth.required");
        if (explicit != null) return explicit;
        Boolean legacy = parseTriState(legacySsoRequired, "app.auth.sso.required");
        if (legacy != null) return legacy;
        return isS3Backed(jarMode) || isS3Backed(parserMode);
    }

    /**
     * {@code "true"}/{@code "false"} (case-insensitive, trimmed) to a {@link Boolean}; blank or
     * {@code null} to {@code null} meaning "not explicitly set". Anything else is a boot failure.
     */
    private static Boolean parseTriState(String raw, String propertyName) {
        if (raw == null || raw.isBlank()) return null;
        String v = raw.trim().toLowerCase(java.util.Locale.ROOT);
        return switch (v) {
            case "true" -> Boolean.TRUE;
            case "false" -> Boolean.FALSE;
            default -> throw new IllegalArgumentException(
                    "Invalid " + propertyName + " '" + raw + "'. Expected: true | false (or leave it unset)");
        };
    }

    /** True when a storage-mode string resolves to S3 or HYBRID — the user-scoped, bucket-backed modes. */
    private static boolean isS3Backed(String modeProp) {
        StorageMode mode = StorageMode.parse(modeProp);
        return mode == StorageMode.S3 || mode == StorageMode.HYBRID;
    }

    public ApiKeyAuthInterceptor(
            ObjectProvider<SsoSessionRegistry> ssoSessionRegistry,
            @Value("${app.auth.required:}") String authRequiredProp,
            @Value("${app.auth.sso.required:}") String legacySsoRequiredProp,
            @Value("${app.storage.jar.mode:${app.storage.mode:local}}") String jarMode,
            @Value("${app.storage.parser.mode:${app.storage.mode:local}}") String parserMode) {
        this.ssoSessionRegistry = ssoSessionRegistry;
        this.authRequired = resolveAuthRequired(
                authRequiredProp, legacySsoRequiredProp, jarMode, parserMode);
        log.info("[auth] app.auth.required={} (jar mode={}, parser mode={}); {}",
                this.authRequired, StorageMode.parse(jarMode), StorageMode.parse(parserMode),
                this.authRequired
                        ? "SSO session required for /api/** and page navigations"
                        : "authentication disabled — all paths allowed, SSO login remains optional");
    }

    /** Whether the SSO gate runs; {@code false} in pure local/Mongo deployments. Tests and diagnostics. */
    public boolean isAuthRequired() {
        return authRequired;
    }

    private static boolean isPublic(String path) {
        return path.equals("/")
                || path.equals("/index.html")
                || path.equals("/login")
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
                || path.startsWith("/api/meta/")
                || path.equals("/api/docs")
                || path.startsWith("/api/docs/");
    }

    @Override
    public boolean preHandle(HttpServletRequest req, HttpServletResponse res, Object handler) throws IOException {
        String path = req.getRequestURI();
        if (isPublic(path)) return true;

        // Mode-aware passthrough: in a pure local/Mongo deployment there is no user-scoped store,
        // so nothing is gated — every API and page is allowed with no session. The SSO endpoints
        // are public (isPublic above), so an optional login remains possible.
        if (!authRequired) return true;

        // The AWS SSO device-flow login IS the application's sign-in now. The old Anthropic-key
        // gate was removed as an entry requirement: its login page no longer renders a form, so
        // keeping it as gate 1 locked every user out of every data API (JAR uploads included) with
        // a 401 they had no UI to clear. A session that completed the AWS SSO flow satisfies the
        // whole gate.
        if (!ssoGateSatisfied(path, req.getSession(false))) {
            if (path.startsWith("/api/")) {
                writeJson(res, HttpServletResponse.SC_UNAUTHORIZED, SSO_REQUIRED_401_BODY);
                return false;
            }
            // Page navigations (e.g. typing /jar/ directly, or a stale bookmark) are bounced to
            // the home page, whose login modal auto-opens when unauthenticated — the UI is the
            // sign-in surface. Static assets (css/js/images) pass so whatever page the user IS on
            // can still render; the User Manual content (/api/docs) is public by design.
            if (!isAssetPath(path)) {
                res.sendRedirect(req.getContextPath() + "/?login=1");
                return false;
            }
            return true;
        }
        return true;
    }

    /**
     * True for asset-looking requests (css/js/images/fonts/maps) that must always load — even for
     * an unauthenticated visitor — so whatever page they are on can render and drive the login.
     * Page-like paths (no asset extension) are navigations and get redirected to the home page.
     */
    private static boolean isAssetPath(String path) {
        int dot = path.lastIndexOf('.');
        if (dot < 0 || dot < path.length() - 8) return false;
        String ext = path.substring(dot + 1).toLowerCase();
        return ext.equals("css") || ext.equals("js") || ext.equals("png") || ext.equals("jpg")
                || ext.equals("jpeg") || ext.equals("gif") || ext.equals("svg") || ext.equals("ico")
                || ext.equals("woff") || ext.equals("woff2") || ext.equals("map") || ext.equals("md");
    }

    /**
     * The AWS SSO gate. It applies to EVERY non-public path — data APIs and page navigations
     * alike (the page-lock requirement: an unauthenticated visitor cannot open /jar/ etc.).
     * The caller decides the response shape: 401 JSON for APIs, a redirect to /?login=1 for
     * page navigations, passthrough for assets.
     *
     * <p>Only called when {@link #authRequired} is true; the mode-aware passthrough in
     * {@link #preHandle} handles the not-required case before this is reached.
     *
     * <p>If the registry bean is absent — SSO support not compiled in, or its component scan
     * disabled — the request is allowed. This is a deliberate fail-open: the alternative would be
     * to lock out every deployment that does not ship the registry.
     */
    private boolean ssoGateSatisfied(String path, HttpSession session) {
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
