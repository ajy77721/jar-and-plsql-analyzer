package com.analyzer.admin;

import com.analyzer.admin.AdminAuditService.AuditEntry;
import com.analyzer.admin.AdminAuditService.UserRollup;
import com.analyzer.storage.sso.ActiveUserInfo;
import com.analyzer.storage.sso.SsoIdentity;
import com.analyzer.storage.sso.SsoSessionRegistry;
import com.analyzer.storage.sso.SsoStoreSwapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The admin portal's API: who is logged into AWS SSO, what they did, and the two destructive levers
 * (revoke a session, forget a user) plus clearing the audit history.
 *
 * <h2>Authentication</h2>
 *
 * <p>There is no Spring Security in this application, so the gate is this class. A caller signs in
 * once at {@code POST /api/admin/login} with the configured operator username and password; on a
 * match the server mints a random opaque token, keeps it in memory with a TTL, and hands it back.
 * Every other endpoint requires that token in the {@code X-Admin-Key} header, and the check is the
 * header comparison below — nothing else in the handler chain gates {@code /api/admin/**}.
 *
 * <p>Tokens are held in memory only: a restart invalidates every admin session, which is the right
 * trade for a small operator surface (no token store to secure, no revocation list to persist). The
 * credentials themselves are never logged, never echoed and never stored beyond the bean's fields;
 * the comparison is {@link MessageDigest#isEqual} so a wrong password does not leak its prefix
 * through timing.
 *
 * <h2>What this API never returns</h2>
 *
 * <p>No credential material exists in the SSO events or the registry snapshots to begin with, and
 * the one sensitive identifier this API does touch — the container session id — is masked to its
 * first eight characters before it appears in any response.
 */
@RestController
@RequestMapping("/api/admin")
public class AdminController {

    private static final Logger log = LoggerFactory.getLogger(AdminController.class);

    /** Header carrying the token minted by {@code /login}. */
    private static final String KEY_HEADER = "X-Admin-Key";

    /** Issued tokens are valid for twelve hours, refreshed by neither use nor age-out sweeps. */
    private static final long TOKEN_TTL_MS = 12L * 60 * 60 * 1000;

    private static final String LOGIN_REQUIRED = "Admin login required";

    private final AdminAuditService audit;
    private final ObjectProvider<SsoSessionRegistry> ssoRegistry;
    private final String username;
    private final String password;

    /** token → expiry epoch millis. In-memory and deliberately not persisted. */
    private final ConcurrentHashMap<String, Long> liveTokens = new ConcurrentHashMap<>();

    public AdminController(AdminAuditService audit,
                           ObjectProvider<SsoSessionRegistry> ssoRegistry,
                           @Value("${app.admin.username:amp-admin}") String username,
                           @Value("${app.admin.password:amp-admin-pwd}") String password) {
        this.audit = audit;
        this.ssoRegistry = ssoRegistry;
        this.username = username;
        this.password = password;
    }

    // ── authentication ──────────────────────────────────────────────────────────

    /**
     * Exchanges the operator credentials for a bearer token. A mismatch of either field yields the
     * same 403 and the same message, so a probe learns only "wrong", never "which half was right".
     */
    @PostMapping("/login")
    public ResponseEntity<Map<String, Object>> login(@RequestBody(required = false) Map<String, String> body) {
        String suppliedUser = body == null ? null : body.get("username");
        String suppliedPass = body == null ? null : body.get("password");

        // constantTimeEquals rejects a blank configured password outright, so an unset property can
        // never turn into "accepts anything".
        boolean ok = constantTimeEquals(suppliedUser, username) & constantTimeEquals(suppliedPass, password);
        if (!ok) {
            return forbidden("Invalid admin credentials");
        }

        purgeExpiredTokens();
        String token = UUID.randomUUID().toString();
        liveTokens.put(token, System.currentTimeMillis() + TOKEN_TTL_MS);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", true);
        out.put("token", token);
        return ResponseEntity.ok(out);
    }

    /**
     * Drops the caller's token. Optional for the client — discarding the token is sufficient
     * client-side — but making it real means a logout actually invalidates the credential rather
     * than leaving twelve hours of validity behind on a shared machine.
     */
    @PostMapping("/logout")
    public ResponseEntity<Map<String, Object>> logout(
            @RequestHeader(value = KEY_HEADER, required = false) String key) {
        if (key != null && !key.isBlank()) liveTokens.remove(key);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", true);
        return ResponseEntity.ok(out);
    }

    private static boolean constantTimeEquals(String supplied, String expected) {
        if (supplied == null || expected == null || expected.isEmpty()) return false;
        return MessageDigest.isEqual(
                supplied.getBytes(StandardCharsets.UTF_8),
                expected.getBytes(StandardCharsets.UTF_8));
    }

    /** Null when the request may proceed; the 403 to send otherwise. */
    private ResponseEntity<Map<String, Object>> requireAuth(String key) {
        if (key == null || key.isBlank()) return forbidden(LOGIN_REQUIRED);
        Long expiry = liveTokens.get(key);
        if (expiry == null) return forbidden(LOGIN_REQUIRED);
        if (expiry <= System.currentTimeMillis()) {
            liveTokens.remove(key);
            return forbidden(LOGIN_REQUIRED);
        }
        return null;
    }

    private void purgeExpiredTokens() {
        long now = System.currentTimeMillis();
        liveTokens.entrySet().removeIf(e -> e.getValue() <= now);
    }

    private static ResponseEntity<Map<String, Object>> forbidden(String message) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("ok", false);
        body.put("error", message);
        return ResponseEntity.status(403).body(body);
    }

    // ── users ───────────────────────────────────────────────────────────────────

    /**
     * The rollup of every user the audit has seen, merged with the live registry so each row also
     * carries {@code currentlyActive}. A user known only to the registry (an identity-less session,
     * or one whose events are older than the tail) is emitted too: the portal is a view of "who has
     * been here", and hiding a live session because the audit forgot it would be the wrong answer.
     */
    @GetMapping("/users")
    public ResponseEntity<Map<String, Object>> users(
            @RequestHeader(value = KEY_HEADER, required = false) String key) {
        ResponseEntity<Map<String, Object>> denied = requireAuth(key);
        if (denied != null) return denied;

        Map<String, ActiveUserInfo> activeByKey = new LinkedHashMap<>();
        for (ActiveUserInfo active : activeUsers()) {
            String k = activeKey(active);
            if (k != null) activeByKey.putIfAbsent(k, active);
        }

        List<Map<String, Object>> users = new ArrayList<>();
        Set<String> emitted = new HashSet<>();
        for (UserRollup rollup : audit.rollups()) {
            emitted.add(rollup.key());
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("key", rollup.key());
            row.put("name", rollup.name());
            row.put("email", rollup.email());
            row.put("accountId", rollup.accountId());
            row.put("accountName", rollup.accountName());
            row.put("roleName", rollup.roleName());
            row.put("bucket", rollup.bucket());
            row.put("region", rollup.region());
            row.put("firstSeenEpochMs", rollup.firstSeenEpochMs());
            row.put("lastSeenEpochMs", rollup.lastSeenEpochMs());
            row.put("events", rollup.eventCounts());

            ActiveUserInfo active = activeByKey.get(rollup.key());
            row.put("currentlyActive", active != null);
            // The live target wins over the last one the audit recorded, so an operator sees what
            // the session is connected to right now rather than what it connected to last time.
            if (active != null) {
                if (active.bucket() != null) row.put("bucket", active.bucket());
                if (active.region() != null) row.put("region", active.region());
                row.put("sessionIdMasked", AdminAuditService.maskSession(active.sessionId()));
            }
            users.add(row);
        }

        for (Map.Entry<String, ActiveUserInfo> entry : activeByKey.entrySet()) {
            if (emitted.contains(entry.getKey())) continue;
            ActiveUserInfo active = entry.getValue();
            SsoIdentity identity = active.identity();
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("key", entry.getKey());
            row.put("name", identity == null ? null : identity.name());
            row.put("email", identity == null ? null : identity.email());
            row.put("accountId", identity == null ? null : identity.accountId());
            row.put("accountName", identity == null ? null : identity.accountName());
            row.put("roleName", identity == null ? null : identity.roleName());
            row.put("bucket", active.bucket());
            row.put("region", active.region());
            row.put("firstSeenEpochMs", 0L);
            row.put("lastSeenEpochMs", active.lastActivityEpochMs());
            row.put("events", Map.of());
            row.put("currentlyActive", true);
            row.put("sessionIdMasked", AdminAuditService.maskSession(active.sessionId()));
            users.add(row);
        }

        users.sort(Comparator
                .comparingLong((Map<String, Object> r) -> asLong(r.get("lastSeenEpochMs"))).reversed()
                .thenComparing(r -> String.valueOf(r.get("key"))));

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", true);
        out.put("users", users);
        return ResponseEntity.ok(out);
    }

    // ── audit history ───────────────────────────────────────────────────────────

    /** Newest-first events, optionally filtered by exact {@code type} and a {@code user} substring. */
    @GetMapping("/audit")
    public ResponseEntity<Map<String, Object>> audit(
            @RequestHeader(value = KEY_HEADER, required = false) String key,
            @RequestParam(value = "limit", defaultValue = "200") int limit,
            @RequestParam(value = "type", required = false) String type,
            @RequestParam(value = "user", required = false) String user) {
        ResponseEntity<Map<String, Object>> denied = requireAuth(key);
        if (denied != null) return denied;

        int bounded = Math.max(1, Math.min(limit, AdminAuditService.MAX_TAIL));
        List<Map<String, Object>> events = new ArrayList<>();
        for (AuditEntry entry : audit.query(bounded, type, user)) {
            events.add(eventJson(entry));
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", true);
        out.put("events", events);
        return ResponseEntity.ok(out);
    }

    private static Map<String, Object> eventJson(AuditEntry e) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("type", e.type());
        m.put("timestampEpochMs", e.timestampEpochMs());
        m.put("name", e.name());
        m.put("email", e.email());
        m.put("accountId", e.accountId());
        m.put("role", e.roleName());
        m.put("bucket", e.bucket());
        m.put("region", e.region());
        m.put("capabilities", capabilitiesJson(e.capabilities()));
        m.put("sessionIdMasked", AdminAuditService.maskSession(e.sessionId()));
        return m;
    }

    private static Map<String, Object> capabilitiesJson(SsoStoreSwapper.Capabilities caps) {
        if (caps == null) return null;
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("read", caps.read());
        m.put("upload", caps.upload());
        m.put("write", caps.write());
        m.put("ok", caps.ok());
        m.put("error", caps.error());
        return m;
    }

    // ── destructive operations ──────────────────────────────────────────────────

    /**
     * Ends a session by id. The caller may pass the full session id or the masked form the portal
     * displays; a masked id is resolved against the live registry, and only when it matches exactly
     * one session. Anything else reports {@code revoked:false} with a note rather than guessing —
     * revoking the wrong user's session is worse than refusing.
     */
    @PostMapping("/sessions/{sessionId}/revoke")
    public ResponseEntity<Map<String, Object>> revoke(
            @RequestHeader(value = KEY_HEADER, required = false) String key,
            @PathVariable("sessionId") String sessionId) {
        ResponseEntity<Map<String, Object>> denied = requireAuth(key);
        if (denied != null) return denied;

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", true);

        SsoSessionRegistry registry = ssoRegistry.getIfAvailable();
        if (registry == null) {
            out.put("revoked", false);
            out.put("note", "AWS SSO registry is not available in this deployment");
            return ResponseEntity.ok(out);
        }

        String fullId = null;
        int matches = 0;
        for (ActiveUserInfo active : registry.listActive()) {
            String id = active.sessionId();
            if (id == null) continue;
            if (id.equals(sessionId) || AdminAuditService.maskSession(id).equals(sessionId)) {
                fullId = id;
                matches++;
            }
        }

        if (matches == 1) {
            registry.end(fullId);
            out.put("revoked", true);
            out.put("sessionIdMasked", AdminAuditService.maskSession(fullId));
            log.info("[admin] revoked an SSO session (masked id {})", AdminAuditService.maskSession(fullId));
        } else if (matches == 0) {
            out.put("revoked", false);
            out.put("note", "No active SSO session matched. Only masked session ids are exposed; "
                    + "pass the full session id to revoke a session that is not currently listed.");
        } else {
            out.put("revoked", false);
            out.put("note", "The masked id is ambiguous (matches " + matches
                    + " sessions); pass the full session id.");
        }
        return ResponseEntity.ok(out);
    }

    /** Truncates the audit history: the file and the in-memory tail/rollups it feeds. */
    @PostMapping("/audit/clear")
    public ResponseEntity<Map<String, Object>> clearAudit(
            @RequestHeader(value = KEY_HEADER, required = false) String key) {
        ResponseEntity<Map<String, Object>> denied = requireAuth(key);
        if (denied != null) return denied;

        audit.clear();
        log.info("[admin] audit history cleared");
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", true);
        return ResponseEntity.ok(out);
    }

    /** Forgets one user: drops their rollup and rewrites the audit file without their lines. */
    @PostMapping("/users/{key}/forget")
    public ResponseEntity<Map<String, Object>> forgetUser(
            @RequestHeader(value = KEY_HEADER, required = false) String key,
            @PathVariable("key") String userKey) {
        ResponseEntity<Map<String, Object>> denied = requireAuth(key);
        if (denied != null) return denied;

        boolean forgotten = audit.forget(userKey);
        log.info("[admin] forget requested for a user rollup (removed={})", forgotten);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", true);
        out.put("forgotten", forgotten);
        return ResponseEntity.ok(out);
    }

    // ── helpers ─────────────────────────────────────────────────────────────────

    private List<ActiveUserInfo> activeUsers() {
        SsoSessionRegistry registry = ssoRegistry.getIfAvailable();
        if (registry == null) return List.of();
        try {
            List<ActiveUserInfo> active = registry.listActive();
            return active == null ? List.of() : active;
        } catch (RuntimeException ex) {
            log.warn("[admin] could not read the SSO session registry: {}", ex.getMessage());
            return List.of();
        }
    }

    /**
     * The registry key for an active session. A session with no identity yet keys on its masked id,
     * matching what {@link AuditEntry#key()} does for identity-less events.
     */
    private static String activeKey(ActiveUserInfo active) {
        String k = AdminAuditService.identityKey(active.identity());
        if (k != null) return k;
        String masked = AdminAuditService.maskSession(active.sessionId());
        return masked == null ? null : "session:" + masked;
    }

    private static long asLong(Object value) {
        return (value instanceof Number n) ? n.longValue() : 0L;
    }
}
