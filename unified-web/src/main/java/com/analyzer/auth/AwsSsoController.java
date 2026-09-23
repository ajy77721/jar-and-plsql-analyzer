package com.analyzer.auth;

import com.analyzer.storage.sso.AwsSsoLoginService;
import com.analyzer.storage.sso.SsoStoreSwapper;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;
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
 * <p>All replies are plain maps, matching {@link AuthController}. The flow is deliberately
 * client-paced: {@code /start} returns a URL and code, the browser calls {@code /poll} at the
 * advertised interval until it reports {@code complete}, and only then does {@code /connect} exist
 * a bucket to point the stores at. No token, secret or device code is ever returned — the only
 * secret-shaped value the client sees is the user code, which is meant to be shown to the human.
 *
 * <p><b>Endpoint shapes (for the front end):</b>
 * <pre>
 * POST /api/auth/aws/start   { "startUrl": "https://d-xxxx.awsapps.com/start" }
 *   -> { started:true, verificationUri, verificationUriComplete, userCode,
 *        intervalSeconds, expiresAt }
 * POST /api/auth/aws/poll
 *   -> { status:"pending"|"complete"|"expired"|"none", accountId?, accountName?, roleName? }
 * POST /api/auth/aws/connect { "bucket": "...", "region": "..." }   (both optional)
 *   -> { ok:true, bucket, region, accountId, roleName }
 * GET  /api/auth/aws/session
 *   -> { connected, accountId, accountName, roleName, bucket, region }
 * POST /api/auth/aws/logout
 *   -> { ok:true }
 * </pre>
 * Any error is reported as {@code { ok:false, error:"..." }} rather than a 5xx, so the UI can show
 * the message.
 */
@RestController
@RequestMapping("/api/auth/aws")
public class AwsSsoController {

    private static final Logger log = LoggerFactory.getLogger(AwsSsoController.class);

    /** HttpSession marker set at /start and cleared at /logout; tokens themselves live server-side. */
    private static final String SESSION_START_URL = "aws.sso.startUrl";

    private final AwsSsoLoginService loginService;
    private final SsoStoreSwapper storeSwapper;

    public AwsSsoController(AwsSsoLoginService loginService, SsoStoreSwapper storeSwapper) {
        this.loginService = loginService;
        this.storeSwapper = storeSwapper;
    }

    /** Step 1: register a client and begin device authorization against the user's portal URL. */
    @PostMapping("/start")
    public Map<String, Object> start(@RequestBody Map<String, String> body, HttpServletRequest req) {
        String startUrl = body == null ? null : body.get("startUrl");
        try {
            AwsSsoLoginService.DeviceFlowStart start = loginService.startLogin(startUrl);
            HttpSession session = req.getSession(true);
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

    /** Step 2: poll until the user approves. Idempotent and cheap while pending. */
    @PostMapping("/poll")
    public Map<String, Object> poll() {
        try {
            AwsSsoLoginService.PollResult result = loginService.poll();
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
     * Step 3: apply the connected bucket/region to the live S3-backed stores.
     *
     * <p>Requires a completed login — connecting the stores before the user has a role to sign with
     * would only point them at a bucket they cannot read. Bucket/region are optional; a blank value
     * keeps the configured default.
     */
    @PostMapping("/connect")
    public Map<String, Object> connect(@RequestBody(required = false) Map<String, String> body) {
        if (!loginService.isLoggedIn()) {
            return error("Log in with AWS SSO before connecting S3 storage.");
        }
        String bucket = body == null ? null : body.get("bucket");
        String region = body == null ? null : body.get("region");
        try {
            SsoStoreSwapper.ConnectResult result = storeSwapper.connect(bucket, region);
            AwsSsoLoginService.Identity identity = loginService.identity();
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("ok", true);
            out.put("bucket", result.bucket());
            out.put("region", result.region());
            out.put("accountId", identity == null ? null : identity.accountId());
            out.put("roleName", identity == null ? null : identity.roleName());
            return out;
        } catch (IllegalArgumentException e) {
            return error(e.getMessage());
        } catch (RuntimeException e) {
            log.warn("[aws-sso] connect failed ({})", e.getClass().getSimpleName());
            return error("Could not connect S3 storage: " + e.getMessage());
        }
    }

    /** Current state for the UI: is a user logged in, as whom, and where do the stores point. */
    @GetMapping("/session")
    public Map<String, Object> session() {
        AwsSsoLoginService.Identity identity = loginService.identity();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("connected", loginService.isLoggedIn());
        out.put("accountId", identity == null ? null : identity.accountId());
        out.put("accountName", identity == null ? null : identity.accountName());
        out.put("roleName", identity == null ? null : identity.roleName());
        out.put("bucket", storeSwapper.bucket());
        out.put("region", storeSwapper.region());
        return out;
    }

    /** Clears the SSO session and the session marker; stores revert to the default chain. */
    @PostMapping("/logout")
    public Map<String, Object> logout(HttpServletRequest req) {
        loginService.logout();
        HttpSession session = req.getSession(false);
        if (session != null) session.removeAttribute(SESSION_START_URL);
        return success();
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
