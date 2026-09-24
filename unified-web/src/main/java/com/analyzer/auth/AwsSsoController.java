package com.analyzer.auth;

import com.analyzer.storage.sso.AwsSsoLoginService;
import com.analyzer.storage.sso.SsoIdentity;
import com.analyzer.storage.sso.SsoSessionRegistry;
import com.analyzer.storage.sso.SsoSessionState;
import com.analyzer.storage.sso.SsoStoreSwapper;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;
import org.springframework.beans.factory.annotation.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The "AWS SSO login → S3 access" endpoints, under {@code /api/auth/aws}.
 *
 * <p>They sit beside {@link AuthController}'s {@code /api/auth/*} routes because they answer the
 * same question — "who is this session, and what is it allowed to touch?" — and, like those routes,
 * are reachable without the Anthropic API key: a user must be able to log in before they can be
 * authenticated for anything else. The path prefix {@code /api/auth/} is on
 * {@link ApiKeyAuthInterceptor}'s public list, so these work whether or not the interceptor is
 * actually registered.
 *
 * <p>All replies are plain maps, matching {@link AuthController}. The flow is client-paced:
 * {@code /start} returns a URL and code, the browser calls {@code /poll} at the advertised interval
 * until it reports {@code complete}, and only then does {@code /connect} exist a bucket to point the
 * stores at. No token, secret or device code is ever returned — the only secret-shaped value the
 * client sees is the user code, which is meant to be shown to the human.
 *
 * <p><b>Every endpoint operates on the caller's own session.</b> State is keyed by
 * {@code req.getSession().getId()} in {@link SsoSessionRegistry}; there is no shared login. Two
 * browsers logging in, connecting different buckets and logging out are fully independent. The
 * tokens themselves live server-side in the registry, never in the {@code HttpSession} and never in
 * a response body.
 *
 * <p><b>Endpoint shapes (for the front end):</b>
 * <pre>
 * POST /api/auth/aws/start   { "startUrl": "https://d-xxxx.awsapps.com/start",
 *                              "accountId"?: "...", "roleName"?: "..." }   (the last two optional)
 *   -> { started:true, verificationUri, verificationUriComplete, userCode,
 *        intervalSeconds, expiresAt }
 * POST /api/auth/aws/poll
 *   -> { status:"pending"|"complete"|"expired"|"none", accountId?, accountName?, roleName? }
 * GET  /api/auth/aws/accounts
 *   -> { ok:true, accounts:[ { accountId, accountName, roles:[...] }, ... ] }
 * POST /api/auth/aws/connect { "bucket": "...", "region": "...", "displayName": "...",
 *                              "accountId"?: "...", "roleName"?: "..." }  (all optional)
 *   -> success: { ok:true, bucket, region, name, email, displayName, accountId, accountName,
 *                 roleName, account, role, capabilities:{ read, upload, write } }
 *   -> failure: { ok:false, error, capabilities:{ read, upload, write } }
 *      (nothing was built or swapped for this user; their previous target, if any, stays active)
 * GET  /api/auth/aws/session
 *   -> { connected, name, email, displayName, account, role, accountId, accountName, roleName,
 *        bucket, region, capabilities:{ read, upload, write } | null }
 * POST /api/auth/aws/logout
 *   -> { ok:true }
 * </pre>
 * The {@code account}/{@code role} fields are legacy aliases of {@code accountId}/{@code roleName},
 * emitted alongside the canonical names so an existing front end that reads the shorter keys keeps
 * working. Both are always present wherever the canonical field is.
 *
 * <p><b>Tokens never appear here.</b> No response map has a token, secret or device-code key, and
 * none is ever put there.
 *
 * <p>Any error is reported as {@code { ok:false, error:"..." }} rather than a 5xx, so the UI can show
 * the message. The one exception is the per-user store router throwing
 * {@code NotAuthenticatedException} when some other endpoint reaches S3 without a login; that is the
 * web layer's 401, not this controller's concern.
 */
@RestController
@RequestMapping("/api/auth/aws")
public class AwsSsoController {

    private static final Logger log = LoggerFactory.getLogger(AwsSsoController.class);

    /** HttpSession marker set at /start and cleared at /logout; the flow state lives in the registry. */
    private static final String SESSION_START_URL = "aws.sso.startUrl";

    private final AwsSsoLoginService loginService;
    private final SsoStoreSwapper storeSwapper;
    private final SsoSessionRegistry registry;

    /**
     * Configured Identity Center portal URL, prefilled into the login modal so a deployed user does
     * not have to type it. A public URL, never a token — safe to return from {@code /session}.
     * Blank when unset, in which case the UI falls back to its own default.
     */
    @Value("${app.storage.sso.start-url:}")
    private String startUrlProperty;

    public AwsSsoController(AwsSsoLoginService loginService, SsoStoreSwapper storeSwapper,
                            SsoSessionRegistry registry) {
        this.loginService = loginService;
        this.storeSwapper = storeSwapper;
        this.registry = registry;
    }

    /** Step 1: register a client and begin device authorization against the user's portal URL. */
    @PostMapping("/start")
    public Map<String, Object> start(@RequestBody Map<String, String> body, HttpServletRequest req) {
        String startUrl = body == null ? null : body.get("startUrl");
        // Optional per-login pre-targeting: when supplied, the auto-pick prefers this account/role
        // over the deployment default (app.storage.sso.account-id). Both are optional and validated
        // only at connect time against the account list the login actually exposes.
        String requestedAccountId = body == null ? null : body.get("accountId");
        String requestedRoleName = body == null ? null : body.get("roleName");
        try {
            // Create the session (and its registry entry) first, so every later call finds state
            // under the same id even if the browser never sent a JSESSIONID cookie.
            HttpSession session = req.getSession(true);
            SsoSessionState state = registry.getOrCreate(session.getId());
            AwsSsoLoginService.DeviceFlowStart start = loginService.startLogin(state, startUrl);
            // startLogin clears the state, so the request must be recorded AFTER it.
            state.requestedTarget(requestedAccountId, requestedRoleName);
            session.setAttribute(SESSION_START_URL, startUrl == null ? "" : startUrl.trim());
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("started", true);
            out.put("verificationUri", start.verificationUri());
            out.put("verificationUriComplete", start.verificationUriComplete());
            out.put("userCode", start.userCode());
            out.put("intervalSeconds", start.intervalSeconds());
            out.put("expiresAt", start.expiresAtEpochMs());
            return out;
        } catch (IllegalArgumentException e) {
            return error(e.getMessage());
        } catch (RuntimeException e) {
            log.warn("[aws-sso] start failed ({})", e.getClass().getSimpleName());
            return error("Could not start AWS SSO login: " + e.getMessage());
        }
    }

    /**
     * Step 2: poll the caller's device authorization until the user approves. Idempotent and cheap
     * while pending; a request with no login in progress reports {@code "none"}.
     */
    @PostMapping("/poll")
    public Map<String, Object> poll(HttpServletRequest req) {
        try {
            AwsSsoLoginService.PollResult result = loginService.poll(registry.get(sessionId(req)));
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("status", result.status());
            out.put("accountId", result.accountId());
            out.put("accountName", result.accountName());
            out.put("roleName", result.roleName());
            return out;
        } catch (RuntimeException e) {
            log.warn("[aws-sso] poll failed ({})", e.getClass().getSimpleName());
            return error("Could not complete AWS SSO login: " + e.getMessage());
        }
    }

    /**
     * The accounts and roles this session's completed login exposed, for the modal's chooser step.
     *
     * <pre>
     * GET /api/auth/aws/accounts
     *   -> { ok:true, accounts:[ { accountId, accountName, roles:[...] }, ... ] }
     * </pre>
     *
     * <p>Reads the list captured at poll-complete (no AWS round trip here, and no token material).
     * The list is empty before the device flow completes; a caller that has not logged in gets
     * {@code {ok:false}}. An account whose role listing failed appears with an empty {@code roles}
     * array — visible, but not selectable.
     */
    @GetMapping("/accounts")
    public Map<String, Object> accounts(HttpServletRequest req) {
        SsoSessionState state = registry.get(sessionId(req));
        if (state == null || !state.loggedIn()) {
            return error("Log in with AWS SSO before choosing an account.");
        }
        java.util.List<Map<String, Object>> accounts = new java.util.ArrayList<>();
        for (SsoSessionState.AccountChoice c : state.accountChoices()) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("accountId", c.accountId());
            row.put("accountName", c.accountName());
            row.put("roles", c.roles());
            accounts.add(row);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", true);
        out.put("accounts", accounts);
        return out;
    }

    /**
     * Step 3: verify and, only if fully verified, build this user's S3 stores against the connected
     * bucket/region.
     *
     * <p>Requires a completed login for <em>this</em> session — connecting before the user has a role
     * to sign with would only point them at a bucket they cannot read. Bucket/region are optional; a
     * blank value keeps the configured default.
     *
     * <p>The capability probe inside {@link SsoStoreSwapper#connect} is the gate: if the bucket
     * cannot be read, written to, or mutated with the caller's credentials, this returns
     * {@code ok:false} with the per-capability breakdown and <b>nothing is built or swapped for that
     * user</b> — their previous target, if any, stays active and other users are untouched. On
     * success the verified {@code {bucket, region, capabilities}} is recorded in the user's state,
     * which is what {@code /session} reports.
     */
    @PostMapping("/connect")
    public Map<String, Object> connect(@RequestBody(required = false) Map<String, String> body,
                                       HttpServletRequest req) {
        String sessionId = sessionId(req);
        SsoSessionState state = registry.get(sessionId);
        if (state == null || !state.loggedIn()) {
            return error("Log in with AWS SSO before connecting S3 storage.");
        }
        String bucket = body == null ? null : body.get("bucket");
        String region = body == null ? null : body.get("region");
        String displayName = body == null ? null : body.get("displayName");
        // Optional explicit choice from the modal's account/role step: when both are present, switch
        // this session to that pair (validated against the account list the login exposed) before
        // anything is built, so the capability probe and every later S3 call sign as the chosen role.
        // Absent -> the login's auto-pick (configured account, else first) stands, as before.
        Map<String, Object> chooseError = applyChosenPair(state, body);
        if (chooseError != null) return chooseError;
        // A user-chosen display name — the always-available fallback when AWS provides no real
        // identity — is applied BEFORE the swap builds, so the CONNECT audit event the swapper
        // publishes carries it and every later read of this identity shows it. Blank leaves any
        // existing override untouched.
        if (displayName != null && !displayName.isBlank() && state.identity() != null) {
            state.identity(state.identity().withDisplayName(displayName));
        }
        try {
            SsoStoreSwapper.ConnectResult result = storeSwapper.connect(sessionId, bucket, region);
            Map<String, Object> capabilities = capabilitiesMap(result.capabilities());
            if (!result.ok()) {
                // Nothing was built. Report the capability breakdown so the UI can name the missing
                // permission rather than just "failed".
                log.warn("[aws-sso] connect rejected: bucket capability probe failed");
                Map<String, Object> out = error(result.capabilities() == null
                        ? "Bucket capability verification failed."
                        : result.capabilities().error());
                out.put("capabilities", capabilities);
                return out;
            }
            state.verifiedTarget(result.bucket(), result.region(), result.capabilities());
            SsoIdentity identity = state.identity();
            String accountId = identity == null ? null : identity.accountId();
            String roleName = identity == null ? null : identity.roleName();
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("ok", true);
            out.put("bucket", result.bucket());
            out.put("region", result.region());
            out.put("name", identity == null ? null : identity.shownName());
            out.put("email", identity == null ? null : identity.email());
            out.put("displayName", identity == null ? null : identity.displayName());
            out.put("accountId", accountId);
            out.put("accountName", identity == null ? null : identity.accountName());
            out.put("roleName", roleName);
            // Legacy aliases: same values under the shorter keys an existing front end reads.
            out.put("account", accountId);
            out.put("role", roleName);
            out.put("capabilities", capabilities);
            return out;
        } catch (IllegalArgumentException e) {
            return error(e.getMessage());
        } catch (RuntimeException e) {
            log.warn("[aws-sso] connect failed ({})", e.getClass().getSimpleName());
            return error("Could not connect S3 storage: " + e.getMessage());
        }
    }

    /**
     * Dry-run the bucket capability probe for the caller's session: the same Read/Upload/Write
     * checks {@code /connect} performs, but nothing is built, swapped or recorded. Lets the modal
     * offer "Test access" before the user commits to a target. Requires a completed login, exactly
     * like {@code /connect}.
     *
     * <pre>
     * POST /api/auth/aws/test-bucket { "bucket": "...", "region": "..." }
     *   -> { ok:true, capabilities:{ read, upload, write, ok, error } }
     * </pre>
     * A failed probe is a normal, non-error reply: {@code ok:false} with the capability breakdown,
     * so the UI can name the missing permission rather than say "failed".
     */
    @PostMapping("/test-bucket")
    public Map<String, Object> testBucket(@RequestBody(required = false) Map<String, String> body,
                                          HttpServletRequest req) {
        SsoSessionState state = registry.get(sessionId(req));
        if (state == null || !state.loggedIn()) {
            return error("Log in with AWS SSO before testing S3 access.");
        }
        String bucket = body == null ? null : body.get("bucket");
        String region = body == null ? null : body.get("region");
        // Honour the chooser's pair here too: the dry-run must sign as the account/role the user is
        // about to connect with, not the login's auto-pick.
        Map<String, Object> chooseError = applyChosenPair(state, body);
        if (chooseError != null) return chooseError;
        try {
            SsoStoreSwapper.Capabilities caps =
                    storeSwapper.testCapabilities(state.sessionId(), bucket, region);
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("ok", caps != null && caps.ok());
            out.put("capabilities", capabilitiesMap(caps));
            if (caps != null && !caps.ok()) out.put("error", caps.error());
            return out;
        } catch (IllegalArgumentException e) {
            return error(e.getMessage());
        } catch (RuntimeException e) {
            log.warn("[aws-sso] test-bucket failed ({})", e.getClass().getSimpleName());
            return error("Could not test S3 access: " + e.getMessage());
        }
    }

    /**
     * Current state for the UI, for <em>this</em> browser only: is this session logged in, as whom,
     * where do its stores point, and what did its last probe confirm.
     *
     * <p>When the session has never connected, bucket/region report the configured defaults and
     * {@code capabilities} is {@code null}; when it has not logged in, the identity fields are all
     * {@code null}.
     */
    @GetMapping("/session")
    public Map<String, Object> session(HttpServletRequest req) {
        SsoSessionState state = registry.get(sessionId(req));
        SsoIdentity identity = (state == null || !state.loggedIn()) ? null : state.identity();
        String accountId = identity == null ? null : identity.accountId();
        String roleName = identity == null ? null : identity.roleName();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("connected", state != null && state.loggedIn());
        out.put("name", identity == null ? null : identity.shownName());
        out.put("email", identity == null ? null : identity.email());
        out.put("displayName", identity == null ? null : identity.displayName());
        // Legacy aliases, mirroring /connect: same values under the shorter keys.
        out.put("account", accountId);
        out.put("role", roleName);
        out.put("accountId", accountId);
        out.put("accountName", identity == null ? null : identity.accountName());
        out.put("roleName", roleName);
        out.put("bucket", state != null && state.bucket() != null ? state.bucket() : storeSwapper.bucket());
        out.put("region", state != null && state.region() != null ? state.region() : storeSwapper.region());
        out.put("capabilities", capabilitiesMap(state == null ? null : state.capabilities()));
        // Public portal URL for the login modal's prefill (see field javadoc). Not a secret.
        out.put("defaultStartUrl", startUrlProperty);
        return out;
    }

    /**
     * Ends this session's SSO login: its tokens, identity and connected stores are dropped, and the
     * session is removed from the registry. Other users are unaffected. Subsequent S3 operations
     * from this browser route to the default chain (or, if it has no session-bound state, the
     * configured bucket).
     */
    @PostMapping("/logout")
    public Map<String, Object> logout(HttpServletRequest req) {
        String sessionId = sessionId(req);
        if (sessionId != null) {
            // Revoke at AWS first (best-effort), then drop local state unconditionally: an AWS
            // failure must never leave a live local session behind.
            loginService.revokeAwsToken(registry.get(sessionId));
            storeSwapper.forget(sessionId);
            registry.end(sessionId);
        }
        HttpSession session = req.getSession(false);
        if (session != null) {
            session.removeAttribute(SESSION_START_URL);
        }
        return success();
    }

    /**
     * Applies an optional {@code (accountId, roleName)} pair from a request body to this session's
     * identity — the modal chooser's explicit choice, shared by {@code /connect} and {@code /test-bucket}.
     *
     * <p>Returns {@code null} when the body carries no usable pair (nothing to do: the login's
     * auto-pick stands) or when the pair was applied; returns an {@code {ok:false, error}} map when
     * the pair is invalid, for the caller to return verbatim.
     */
    private Map<String, Object> applyChosenPair(SsoSessionState state, Map<String, String> body) {
        String accountId = body == null ? null : body.get("accountId");
        String roleName = body == null ? null : body.get("roleName");
        if (accountId == null || accountId.isBlank() || roleName == null || roleName.isBlank()) {
            return null;
        }
        try {
            SsoIdentity chosen = loginService.chooseIdentity(state, accountId.trim(), roleName.trim());
            if (chosen != null) state.identity(chosen);
            return null;
        } catch (IllegalArgumentException e) {
            return error(e.getMessage());
        } catch (RuntimeException e) {
            log.warn("[aws-sso] choosing account/role failed ({})", e.getClass().getSimpleName());
            return error("Could not use the chosen AWS account/role: " + e.getMessage());
        }
    }

    /** The caller's session id, or {@code null} when the request has no session. */
    private static String sessionId(HttpServletRequest req) {
        HttpSession session = req.getSession(false);
        return session == null ? null : session.getId();
    }

    /** Shapes a probe outcome as the {@code {read, upload, write}} object the UI consumes. */
    private static Map<String, Object> capabilitiesMap(SsoStoreSwapper.Capabilities capabilities) {
        if (capabilities == null) return null;
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("read", capabilities.read());
        out.put("upload", capabilities.upload());
        out.put("write", capabilities.write());
        return out;
    }

    private static Map<String, Object> success() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", true);
        return out;
    }

    private static Map<String, Object> error(String message) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", false);
        out.put("error", message == null ? "Unknown error" : message);
        return out;
    }
}
