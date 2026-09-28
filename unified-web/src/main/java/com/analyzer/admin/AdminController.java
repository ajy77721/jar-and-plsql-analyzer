package com.analyzer.admin;

import com.analyzer.admin.AdminAuditService.AuditEntry;
import com.analyzer.admin.AdminAuditService.UserRollup;
import com.analyzer.storage.DiskCacheAdmin;
import com.analyzer.storage.sso.ActiveUserInfo;
import com.analyzer.storage.sso.AwsSsoLoginService;
import com.analyzer.storage.sso.SsoIdentity;
import com.analyzer.storage.sso.SsoSessionRegistry;
import com.analyzer.storage.sso.SsoSessionState;
import com.analyzer.storage.sso.SsoStoreSwapper;
import com.plsqlanalyzer.config.ConnectionConfig;
import com.plsqlanalyzer.config.EnvironmentConfig;
import com.plsqlanalyzer.config.PlsqlConfig;
import com.plsqlanalyzer.config.ProjectConfig;
import com.plsqlanalyzer.web.parser.model.AnalysisInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
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
    private final ObjectProvider<AwsSsoLoginService> ssoLogin;
    /**
     * Read-only operational views. All three are the SAME beans the analyzers themselves use, so
     * the admin portal never re-implements enumeration: connections come from the live config bean,
     * PL/SQL analyses from the parser service's own listing, and JAR analyses from the JAR
     * persistence service's own listing.
     */
    private final PlsqlConfig plsqlConfig;
    private final com.plsqlanalyzer.web.parser.service.AnalysisService parserAnalysisService;
    private final com.jaranalyzer.service.PersistenceService jarPersistenceService;
    /** The on-disk artifact cache, so the portal can report and empty it. */
    private final DiskCacheAdmin diskCache;
    /** The background sidecar warmer, so the portal can trigger a sweep on demand. */
    private final com.plsqlanalyzer.web.parser.service.SidecarWarmUpService sidecarWarmUp;
    private final String username;
    private final String password;

    /** token → expiry epoch millis. In-memory and deliberately not persisted. */
    private final ConcurrentHashMap<String, Long> liveTokens = new ConcurrentHashMap<>();

    /**
     * How far before an analysis's timestamp a CONNECT/LOGIN event may sit and still be treated as
     * that analysis's owner. Best-effort attribution only — the audit records no analysis event, so
     * this is a proximity heuristic, never a claim.
     */
    private static final long OWNER_WINDOW_MS = 12L * 60 * 60 * 1000;

    public AdminController(AdminAuditService audit,
                           ObjectProvider<SsoSessionRegistry> ssoRegistry,
                           ObjectProvider<AwsSsoLoginService> ssoLogin,
                           PlsqlConfig plsqlConfig,
                           @Qualifier("parserAnalysisService")
                           com.plsqlanalyzer.web.parser.service.AnalysisService parserAnalysisService,
                           @Qualifier("jarPersistenceService")
                           com.jaranalyzer.service.PersistenceService jarPersistenceService,
                           DiskCacheAdmin diskCache,
                           com.plsqlanalyzer.web.parser.service.SidecarWarmUpService sidecarWarmUp,
                           // Env-var fallback (APP_ADMIN_USERNAME/PASSWORD) lets Kanopy inject
                           // real credentials via envSecrets without code changes; the property
                           // defaults are for local dev only and must not reach production.
                           @Value("${app.admin.username:${APP_ADMIN_USERNAME:amp-admin}}") String username,
                           @Value("${app.admin.password:${APP_ADMIN_PASSWORD:amp-admin-pwd}}") String password) {
        this.audit = audit;
        this.ssoRegistry = ssoRegistry;
        this.ssoLogin = ssoLogin;
        this.plsqlConfig = plsqlConfig;
        this.parserAnalysisService = parserAnalysisService;
        this.jarPersistenceService = jarPersistenceService;
        this.diskCache = diskCache;
        this.sidecarWarmUp = sidecarWarmUp;
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

    /**
     * Triggers one eager sidecar warm-up sweep on demand, so an operator can fill the precomputed
     * view summaries after a deploy or a bulk re-analysis without waiting for the periodic sweep.
     *
     * <p>Runs in the background and returns immediately: a sweep can take minutes over a large
     * corpus, and an admin request must not sit on it. The counts from the most recent completed
     * sweep are returned alongside, so the caller sees the effect of the previous trigger.
     */
    @PostMapping("/warmup")
    public ResponseEntity<Map<String, Object>> warmup(
            @RequestHeader(value = KEY_HEADER, required = false) String key) {
        ResponseEntity<Map<String, Object>> denied = requireAuth(key);
        if (denied != null) return denied;

        boolean started = sidecarWarmUp.triggerSweep();
        var last = sidecarWarmUp.lastSweep();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", true);
        out.put("started", started);
        out.put("note", started
                ? "warm-up sweep started in the background"
                : "a sweep is already running, or warm-up is disabled (app.sidecar.warmup.enabled)");
        Map<String, Object> previous = new LinkedHashMap<>();
        previous.put("analyses", last.analyses());
        previous.put("warmed", last.warmed());
        previous.put("alreadyComplete", last.alreadyComplete());
        previous.put("failed", last.failed());
        previous.put("sidecarsWritten", last.sidecarsWritten());
        previous.put("durationMs", last.durationMs());
        previous.put("finishedAt", last.finishedAt());
        out.put("lastSweep", previous);
        return ResponseEntity.accepted().body(out);
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

        // Group live sessions by PERSON. A session with no identity yet (mid-login or abandoned)
        // is not a person and never becomes a user row — it is reported separately, below.
        Map<String, List<ActiveUserInfo>> activeByKey = new LinkedHashMap<>();
        List<ActiveUserInfo> pending = new ArrayList<>();
        for (ActiveUserInfo active : activeUsers()) {
            String k = activeKey(active);
            if (k == null) {
                pending.add(active);
                continue;
            }
            activeByKey.computeIfAbsent(k, x -> new ArrayList<>()).add(active);
        }

        List<Map<String, Object>> users = new ArrayList<>();
        Set<String> emitted = new HashSet<>();
        for (UserRollup rollup : audit.rollups()) {
            emitted.add(rollup.key());
            List<ActiveUserInfo> activeSessions = activeByKey.getOrDefault(rollup.key(), List.of());
            Map<String, Object> row = personRow(rollup.key(), activeSessions);
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
            // The live target wins over the last one the audit recorded, so an operator sees what
            // the person is connected to right now rather than what they connected to last time.
            if (!activeSessions.isEmpty()) {
                ActiveUserInfo active = activeSessions.get(0);
                if (active.bucket() != null) row.put("bucket", active.bucket());
                if (active.region() != null) row.put("region", active.region());
            }
            users.add(row);
        }

        for (Map.Entry<String, List<ActiveUserInfo>> entry : activeByKey.entrySet()) {
            if (emitted.contains(entry.getKey())) continue;
            List<ActiveUserInfo> activeSessions = entry.getValue();
            ActiveUserInfo first = activeSessions.get(0);
            SsoIdentity identity = first.identity();
            Map<String, Object> row = personRow(entry.getKey(), activeSessions);
            row.put("name", identity == null ? null : identity.shownName());
            row.put("email", identity == null ? null : identity.email());
            row.put("accountId", identity == null ? null : identity.accountId());
            row.put("accountName", identity == null ? null : identity.accountName());
            row.put("roleName", identity == null ? null : identity.roleName());
            row.put("bucket", first.bucket());
            row.put("region", first.region());
            row.put("firstSeenEpochMs", 0L);
            row.put("lastSeenEpochMs", first.lastActivityEpochMs());
            row.put("events", Map.of());
            users.add(row);
        }

        users.sort(Comparator
                .comparingLong((Map<String, Object> r) -> asLong(r.get("lastSeenEpochMs"))).reversed()
                .thenComparing(r -> String.valueOf(r.get("key"))));

        // Sessions that started a flow but never resolved an identity: not people, so they get
        // their own section (masked id + last activity + per-session revoke) rather than a ghost row.
        List<Map<String, Object>> pendingLogins = new ArrayList<>();
        for (ActiveUserInfo active : pending) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("sessionIdMasked", AdminAuditService.maskSession(active.sessionId()));
            row.put("lastActivityEpochMs", active.lastActivityEpochMs());
            row.put("bucket", active.bucket());
            row.put("region", active.region());
            pendingLogins.add(row);
        }
        pendingLogins.sort(Comparator
                .comparingLong((Map<String, Object> r) -> asLong(r.get("lastActivityEpochMs"))).reversed());

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", true);
        out.put("users", users);
        out.put("pendingLogins", pendingLogins);
        return ResponseEntity.ok(out);
    }

    /**
     * The person-shaped columns shared by both user-row sources. {@code displayName} is the admin's
     * correction (when set) and always wins over the identity-store name; {@code sessionCount} tells
     * the portal how many live sessions a person-level revoke will end.
     */
    private Map<String, Object> personRow(String key, List<ActiveUserInfo> activeSessions) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("key", key);
        row.put("displayName", audit.displayNameFor(key));
        row.put("currentlyActive", !activeSessions.isEmpty());
        row.put("sessionCount", activeSessions.size());
        if (!activeSessions.isEmpty()) {
            row.put("sessionIdMasked", AdminAuditService.maskSession(activeSessions.get(0).sessionId()));
        }
        return row;
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

    private Map<String, Object> eventJson(AuditEntry e) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("type", e.type());
        m.put("timestampEpochMs", e.timestampEpochMs());
        m.put("name", e.name());
        m.put("email", e.email());
        // The audit "User" column chain: an admin display-name correction, else the email/name the
        // event was recorded with, else the account id. The stored line itself is never rewritten —
        // only how it is labelled in this read.
        String displayName = audit.displayNameFor(e.key());
        m.put("displayName", displayName);
        m.put("user", firstNonBlank(displayName, e.email(), e.name(), e.accountId()));
        m.put("accountId", e.accountId());
        m.put("role", e.roleName());
        m.put("bucket", e.bucket());
        m.put("region", e.region());
        // "iam" for a session authenticated with classic access keys, "sso" otherwise.
        m.put("source", e.source());
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

    // ── operational visibility: connections / analyses / usage ───────────────────

    /**
     * EVERY connection across every project and environment in the live config, in one list.
     *
     * <p>Read straight from the {@link PlsqlConfig} bean the analyzers themselves use, so the portal
     * can never drift from what a run would actually dial. Sorted project → environment → name.
     *
     * <h2>What is masked or omitted</h2>
     *
     * <p>Passwords and truststore passwords are never read, let alone emitted — this method never
     * touches {@link ConnectionConfig#getPassword()} or {@code getTruststorePassword()}. Usernames
     * are masked to their first two characters because a login name is still a credential-adjacent
     * identifier, and no TLS file contents are returned: {@code certPaths} is a boolean that says
     * only whether any certificate path is configured.
     */
    @GetMapping("/connections")
    public ResponseEntity<Map<String, Object>> connections(
            @RequestHeader(value = KEY_HEADER, required = false) String key) {
        ResponseEntity<Map<String, Object>> denied = requireAuth(key);
        if (denied != null) return denied;

        List<Map<String, Object>> out = new ArrayList<>();
        List<ProjectConfig> projects = plsqlConfig == null ? null : plsqlConfig.getProjects();
        if (projects != null) {
            for (ProjectConfig project : projects) {
                if (project == null) continue;
                List<EnvironmentConfig> envs = project.getEnvironments();
                if (envs == null) continue;
                for (EnvironmentConfig env : envs) {
                    if (env == null) continue;
                    List<ConnectionConfig> conns = env.getConnections();
                    if (conns == null) continue;
                    for (ConnectionConfig c : conns) {
                        if (c == null) continue;
                        out.add(connectionJson(project.getName(), env.getName(), c));
                    }
                }
            }
        }
        out.sort(Comparator
                .comparing((Map<String, Object> r) -> asString(r.get("project")))
                .thenComparing(r -> asString(r.get("environment")))
                .thenComparing(r -> asString(r.get("name"))));

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("ok", true);
        body.put("connections", out);
        return ResponseEntity.ok(body);
    }

    /** One connection row — credential-free by construction (see {@link #connections}). */
    private static Map<String, Object> connectionJson(String project, String environment, ConnectionConfig c) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("project", project);
        m.put("environment", environment);
        m.put("name", c.getName());
        m.put("dbType", c.getDbType());
        m.put("hostname", c.getHostname());
        m.put("port", c.getEffectivePort());
        m.put("serviceName", c.getServiceName());
        m.put("username", maskUsername(c.getUsername()));
        m.put("tlsMode", tlsMode(c));
        m.put("certPaths", hasCertPaths(c));
        m.put("lastTestedAt", c.getLastTestedAt());
        m.put("tested", c.isTested());
        m.put("schemasCount", c.getSchemas() == null ? 0 : c.getSchemas().size());
        return m;
    }

    /** First two characters plus an ellipsis; short values degrade to one character. Never null-safe leak. */
    static String maskUsername(String username) {
        if (username == null || username.isBlank()) return null;
        String t = username.trim();
        return (t.length() <= 2 ? t.substring(0, 1) : t.substring(0, 2)) + "…";
    }

    /** The configured TLS policy, spelling {@code NONE} explicitly when it is unset/blank. */
    private static String tlsMode(ConnectionConfig c) {
        String mode = c.getSslMode();
        return (mode == null || mode.isBlank()) ? "NONE" : mode.trim().toUpperCase(java.util.Locale.ROOT);
    }

    /** Whether ANY TLS artifact path (cert, key, truststore or wallet) is configured on the connection. */
    private static boolean hasCertPaths(ConnectionConfig c) {
        return isSet(c.getTrustCertPath()) || isSet(c.getClientCertPath()) || isSet(c.getClientKeyPath())
                || isSet(c.getTruststorePath()) || isSet(c.getWalletPath());
    }

    private static boolean isSet(String s) {
        return s != null && !s.isBlank();
    }

    /**
     * BOTH analysis stores' listings, aggregated: the parser's PL/SQL analyses and the JAR
     * persistence service's JAR analyses.
     *
     * <p>Enumeration is delegated to the very services the existing list endpoints call
     * ({@link com.plsqlanalyzer.web.parser.service.AnalysisService#listAnalyses()} and
     * {@link com.jaranalyzer.service.PersistenceService#listJars()}) — this never re-scans a
     * directory or re-implements the index/header reads.
     *
     * <p>Each row carries a best-effort {@code owner}: the audit records no analysis event, so the
     * owner is the person whose most recent CONNECT/LOGIN sits within {@link #OWNER_WINDOW_MS} before
     * the analysis timestamp, or the literal {@code "unknown"} when no such event exists.
     */
    @GetMapping("/analyses")
    public ResponseEntity<Map<String, Object>> analyses(
            @RequestHeader(value = KEY_HEADER, required = false) String key) {
        ResponseEntity<Map<String, Object>> denied = requireAuth(key);
        if (denied != null) return denied;

        List<OwnerEvent> owners = ownerEvents();

        List<Map<String, Object>> plsql = new ArrayList<>();
        try {
            for (AnalysisInfo a : parserAnalysisService.listAnalyses()) {
                plsql.add(plsqlAnalysisJson(a, owners));
            }
        } catch (Exception e) {
            log.warn("[admin] could not list PL/SQL analyses: {}", e.getMessage());
        }
        plsql.sort(Comparator.comparingLong((Map<String, Object> r) -> asLong(r.get("timestampEpochMs"))).reversed());

        List<Map<String, Object>> jar = new ArrayList<>();
        try {
            for (Map<String, Object> j : jarPersistenceService.listJars()) {
                jar.add(jarAnalysisJson(j, owners));
            }
        } catch (Exception e) {
            log.warn("[admin] could not list JAR analyses: {}", e.getMessage());
        }
        jar.sort(Comparator.comparingLong((Map<String, Object> r) -> asLong(r.get("timestampEpochMs"))).reversed());

        Map<String, Object> analyses = new LinkedHashMap<>();
        analyses.put("plsql", plsql);
        analyses.put("jar", jar);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("ok", true);
        body.put("analyses", analyses);
        return ResponseEntity.ok(body);
    }

    private Map<String, Object> plsqlAnalysisJson(AnalysisInfo a, List<OwnerEvent> owners) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("name", a.getName());
        m.put("project", a.getProject());
        m.put("environment", a.getEnvironment());
        m.put("connectionName", a.getConnectionName());
        m.put("dialect", dialectOf(a.getJdbcUrl()));
        m.put("entryPoint", a.getEntryPoint());
        m.put("entrySchema", a.getEntrySchema());
        m.put("totalNodes", a.getTotalNodes());
        m.put("totalTables", a.getTotalTables());
        m.put("totalEdges", a.getTotalEdges());
        m.put("totalLinesOfCode", a.getTotalLinesOfCode());
        m.put("timestampEpochMs", a.getTimestamp());
        m.put("owner", ownerLabelAt(owners, a.getTimestamp()));
        return m;
    }

    private Map<String, Object> jarAnalysisJson(Map<String, Object> j, List<OwnerEvent> owners) {
        long ts = parseTimestamp(j.get("analyzedAt"));
        Object mode = j.get("analysisMode") != null ? j.get("analysisMode") : j.get("mode");
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", j.get("id"));
        m.put("jarName", j.get("jarName"));
        m.put("projectName", j.get("projectName"));
        m.put("totalClasses", j.get("totalClasses"));
        m.put("totalEndpoints", j.get("totalEndpoints"));
        m.put("analyzedAt", j.get("analyzedAt"));
        m.put("mode", mode);
        m.put("timestampEpochMs", ts);
        m.put("owner", ownerLabelAt(owners, ts));
        return m;
    }

    /**
     * Per-user operational rollup: analyses attributed to each person (best-effort, same heuristic
     * as {@link #analyses}), distinct CONNECT activity, live-session state and first/last seen.
     */
    @GetMapping("/usage")
    public ResponseEntity<Map<String, Object>> usage(
            @RequestHeader(value = KEY_HEADER, required = false) String key) {
        ResponseEntity<Map<String, Object>> denied = requireAuth(key);
        if (denied != null) return denied;

        List<OwnerEvent> owners = ownerEvents();

        Map<String, Integer> plsqlByOwner = new HashMap<>();
        Map<String, Integer> jarByOwner = new HashMap<>();
        int plsqlTotal = 0;
        int jarTotal = 0;
        try {
            for (AnalysisInfo a : parserAnalysisService.listAnalyses()) {
                plsqlTotal++;
                OwnerEvent ev = ownerEventAt(owners, a.getTimestamp());
                if (ev != null && ev.key() != null) plsqlByOwner.merge(ev.key(), 1, Integer::sum);
            }
        } catch (Exception e) {
            log.warn("[admin] usage could not list PL/SQL analyses: {}", e.getMessage());
        }
        try {
            for (Map<String, Object> j : jarPersistenceService.listJars()) {
                jarTotal++;
                OwnerEvent ev = ownerEventAt(owners, parseTimestamp(j.get("analyzedAt")));
                if (ev != null && ev.key() != null) jarByOwner.merge(ev.key(), 1, Integer::sum);
            }
        } catch (Exception e) {
            log.warn("[admin] usage could not list JAR analyses: {}", e.getMessage());
        }

        Map<String, Integer> connectByKey = new HashMap<>();
        for (AuditEntry e : audit.query(AdminAuditService.MAX_TAIL, "CONNECT", null)) {
            if (e.key() != null) connectByKey.merge(e.key(), 1, Integer::sum);
        }

        Map<String, Integer> liveByKey = new HashMap<>();
        for (ActiveUserInfo active : activeUsers()) {
            String k = activeKey(active);
            if (k != null) liveByKey.merge(k, 1, Integer::sum);
        }

        List<Map<String, Object>> users = new ArrayList<>();
        Set<String> emitted = new HashSet<>();
        for (UserRollup r : audit.rollups()) {
            emitted.add(r.key());
            int p = plsqlByOwner.getOrDefault(r.key(), 0);
            int jc = jarByOwner.getOrDefault(r.key(), 0);
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("key", r.key());
            row.put("displayName", audit.displayNameFor(r.key()));
            row.put("name", r.name());
            row.put("email", r.email());
            row.put("firstSeenEpochMs", r.firstSeenEpochMs());
            row.put("lastSeenEpochMs", r.lastSeenEpochMs());
            row.put("plsqlAnalyses", p);
            row.put("jarAnalyses", jc);
            row.put("totalAnalyses", p + jc);
            row.put("connectionsTested", connectByKey.getOrDefault(r.key(), 0));
            row.put("currentlyActive", liveByKey.containsKey(r.key()));
            row.put("sessionCount", liveByKey.getOrDefault(r.key(), 0));
            users.add(row);
        }
        for (Map.Entry<String, Integer> e : liveByKey.entrySet()) {
            if (emitted.contains(e.getKey())) continue;
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("key", e.getKey());
            row.put("displayName", audit.displayNameFor(e.getKey()));
            row.put("name", null);
            row.put("email", e.getKey());
            row.put("firstSeenEpochMs", 0L);
            row.put("lastSeenEpochMs", 0L);
            row.put("plsqlAnalyses", plsqlByOwner.getOrDefault(e.getKey(), 0));
            row.put("jarAnalyses", jarByOwner.getOrDefault(e.getKey(), 0));
            row.put("totalAnalyses", plsqlByOwner.getOrDefault(e.getKey(), 0)
                    + jarByOwner.getOrDefault(e.getKey(), 0));
            row.put("connectionsTested", connectByKey.getOrDefault(e.getKey(), 0));
            row.put("currentlyActive", true);
            row.put("sessionCount", e.getValue());
            users.add(row);
        }
        users.sort(Comparator
                .comparingLong((Map<String, Object> r) -> asLong(r.get("lastSeenEpochMs"))).reversed()
                .thenComparing(r -> asString(r.get("key"))));

        Map<String, Object> totals = new LinkedHashMap<>();
        totals.put("users", users.size());
        totals.put("analyses", plsqlTotal + jarTotal);
        totals.put("plsqlAnalyses", plsqlTotal);
        totals.put("jarAnalyses", jarTotal);
        totals.put("activeNow", liveByKey.size());

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("ok", true);
        body.put("totals", totals);
        body.put("users", users);
        return ResponseEntity.ok(body);
    }

    /** A CONNECT/LOGIN marker used for best-effort analysis→user attribution. */
    private record OwnerEvent(long timestampEpochMs, String key, String label) {}

    /** Every CONNECT/LOGIN audit event, ascending by time — the windows analyses are matched against. */
    private List<OwnerEvent> ownerEvents() {
        List<OwnerEvent> out = new ArrayList<>();
        for (AuditEntry e : audit.query(AdminAuditService.MAX_TAIL, null, null)) {
            String type = e.type();
            if (!"CONNECT".equalsIgnoreCase(type) && !"LOGIN".equalsIgnoreCase(type)) continue;
            if (e.timestampEpochMs() <= 0) continue;
            out.add(new OwnerEvent(e.timestampEpochMs(), e.key(), eventUserLabel(e)));
        }
        out.sort(Comparator.comparingLong(OwnerEvent::timestampEpochMs));
        return out;
    }

    /** The most recent CONNECT/LOGIN at or before {@code ts}, within the attribution window; else null. */
    private static OwnerEvent ownerEventAt(List<OwnerEvent> events, long ts) {
        if (ts <= 0 || events.isEmpty()) return null;
        OwnerEvent best = null;
        for (OwnerEvent e : events) {
            if (e.timestampEpochMs() <= ts) best = e;
            else break;
        }
        if (best == null || ts - best.timestampEpochMs() > OWNER_WINDOW_MS) return null;
        return best;
    }

    private static String ownerLabelAt(List<OwnerEvent> events, long ts) {
        OwnerEvent ev = ownerEventAt(events, ts);
        return (ev == null || ev.label() == null) ? "unknown" : ev.label();
    }

    /** The same user chain the audit "User" column uses: display-name override, then email/name/account. */
    private String eventUserLabel(AuditEntry e) {
        String label = firstNonBlank(audit.displayNameFor(e.key()), e.email(), e.name(), e.accountId());
        return label != null ? label : e.key();
    }

    /** Dialect from a JDBC URL — the same substring scan the parser itself uses. */
    private static String dialectOf(String jdbcUrl) {
        String u = jdbcUrl == null ? "" : jdbcUrl.toLowerCase(java.util.Locale.ROOT);
        if (u.contains("sqlserver")) return "SQLSERVER";
        if (u.contains("oracle"))    return "ORACLE";
        if (u.contains("mysql"))     return "MYSQL";
        if (u.contains("db2"))       return "DB2";
        if (u.contains("postgres"))  return "POSTGRES";
        return "UNKNOWN";
    }

    /** Epoch millis from a JAR header's {@code analyzedAt}: an ISO local date-time, instant or number. */
    private static long parseTimestamp(Object value) {
        if (value == null) return 0;
        if (value instanceof Number n) return n.longValue();
        String s = String.valueOf(value).trim();
        if (s.isEmpty()) return 0;
        try {
            return java.time.LocalDateTime.parse(s)
                    .atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
        } catch (Exception ignored) {
            try { return java.time.Instant.parse(s).toEpochMilli(); } catch (Exception ignored2) { return 0; }
        }
    }

    private static String asString(Object value) {
        return value == null ? "" : String.valueOf(value);
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
            // AWS first (best-effort), then always drop local state — an AWS failure must never
            // leave a live local session behind.
            revokeAtAws(registry, fullId);
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

    /**
     * Empties the on-disk artifact cache ({@code {app.data-dir}/cache/store/}) and reports the
     * bytes freed. The cache is a pure read-through mirror of the configured store, so clearing it
     * is always safe — the next read of any artifact simply refetches and re-mirrors. This is the
     * lever for "the cache looks wrong / is filling the disk" without a restart.
     */
    @PostMapping("/cache/clear")
    public ResponseEntity<Map<String, Object>> clearCache(
            @RequestHeader(value = KEY_HEADER, required = false) String key) {
        ResponseEntity<Map<String, Object>> denied = requireAuth(key);
        if (denied != null) return denied;

        long freed = diskCache.clear();
        log.info("[admin] artifact cache cleared ({} bytes freed)", freed);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", true);
        out.put("freedBytes", freed);
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

    /**
     * Sets or corrects the display name an operator wants a person shown as. It is stored as a
     * display-name override in the audit service's rollup (never in the audit file, and never
     * rewriting history), and every read — the Users tab and the audit "User" column — prefers it.
     * A blank value clears the override.
     */
    @PutMapping("/users/{key}/display-name")
    public ResponseEntity<Map<String, Object>> setDisplayName(
            @RequestHeader(value = KEY_HEADER, required = false) String key,
            @PathVariable("key") String userKey,
            @RequestBody(required = false) Map<String, String> body) {
        ResponseEntity<Map<String, Object>> denied = requireAuth(key);
        if (denied != null) return denied;

        String displayName = body == null ? null : body.get("displayName");
        boolean updated = audit.setDisplayName(userKey, displayName);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", true);
        out.put("updated", updated);
        if (!updated) {
            out.put("note", "No user rollup matched that key.");
        } else {
            log.info("[admin] display name updated for a user rollup (cleared={})",
                    displayName == null || displayName.isBlank());
        }
        return ResponseEntity.ok(out);
    }

    /**
     * Revokes <b>every live session belonging to one person</b> — all registry entries whose
     * identity key matches. Each is revoked at AWS first (best-effort) and then ended locally, so a
     * person signed in from two browsers is fully signed out with one action. Per-session revoke
     * ({@link #revoke}) remains for the identity-less "pending logins" section.
     */
    @PostMapping("/users/{key}/revoke")
    public ResponseEntity<Map<String, Object>> revokeUser(
            @RequestHeader(value = KEY_HEADER, required = false) String key,
            @PathVariable("key") String userKey) {
        ResponseEntity<Map<String, Object>> denied = requireAuth(key);
        if (denied != null) return denied;

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", true);

        SsoSessionRegistry registry = ssoRegistry.getIfAvailable();
        if (registry == null) {
            out.put("revoked", 0);
            out.put("note", "AWS SSO registry is not available in this deployment");
            return ResponseEntity.ok(out);
        }

        int revoked = 0;
        for (ActiveUserInfo active : registry.listActive()) {
            String k = activeKey(active);
            if (k == null || !k.equals(userKey)) continue;
            String id = active.sessionId();
            if (id == null) continue;
            revokeAtAws(registry, id);
            registry.end(id);
            revoked++;
        }
        out.put("revoked", revoked);
        if (revoked == 0) {
            out.put("note", "No live sessions matched that user.");
        } else {
            log.info("[admin] revoked {} live session(s) for one user", revoked);
        }
        return ResponseEntity.ok(out);
    }

    /** Best-effort AWS-side RevokeToken for a session, before its local state is dropped. */
    private void revokeAtAws(SsoSessionRegistry registry, String sessionId) {
        if (sessionId == null) return;
        AwsSsoLoginService svc = ssoLogin.getIfAvailable();
        if (svc == null) return;
        SsoSessionState state = registry.get(sessionId);
        // Non-fatal by contract: revokeAwsToken logs and returns false rather than throwing.
        svc.revokeAwsToken(state);
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
     * The registry key for an active session, or {@code null} when the session has no identity yet.
     * A null key is what routes such a session into the "pending logins" section instead of a person
     * row: only someone we can name counts as a user.
     */
    private static String activeKey(ActiveUserInfo active) {
        return AdminAuditService.identityKey(active.identity());
    }

    private static long asLong(Object value) {
        return (value instanceof Number n) ? n.longValue() : 0L;
    }

    private static String firstNonBlank(String... values) {
        for (String v : values) {
            if (v != null && !v.isBlank()) return v;
        }
        return null;
    }
}
