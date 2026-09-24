package com.analyzer.admin;

import com.analyzer.storage.sso.AwsSsoLoginService;
import com.analyzer.storage.sso.SsoAuditEvent;
import com.analyzer.storage.sso.SsoIdentity;
import com.analyzer.storage.sso.SsoSessionRegistry;
import com.analyzer.storage.sso.SsoSessionState;
import com.analyzer.storage.sso.SsoStoreSwapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * The audit sink behind the admin portal: it listens to AWS SSO lifecycle events, appends each one
 * as a single JSON line under {@code {app.data-dir}/admin/audit.jsonl}, and keeps a bounded
 * in-memory tail plus a per-user rollup so the admin API answers without re-reading the file.
 *
 * <h2>Why a JSONL file and not a database</h2>
 *
 * <p>The audit trail is the one thing an operator most wants to survive a restart and is the least
 * suited to an index: it is append-only, read newest-first, and never updated in place. Newline
 * delimited JSON gives exactly that with no dependency — a line is written, flushed and forgotten;
 * a tail read is a single pass; and the file stays greppable by hand, which is what someone
 * debugging a login failure actually reaches for. The in-memory tail exists only as a read cache:
 * the file is the record, the deque is the fast path.
 *
 * <h2>What is deliberately absent</h2>
 *
 * <p>{@link SsoAuditEvent} carries identity and target metadata only — no access token, refresh
 * token, client secret, secret access key or session token is a field on it. Consequently nothing
 * this class writes or logs can leak credential material: there is simply none in reach. The only
 * session-bearing value stored is the opaque container session id, which the admin API masks to its
 * first eight characters before anything leaves the server.
 *
 * <h2>Concurrency</h2>
 *
 * <p>Events are published from request threads (login, connect, logout) and from the registry's lazy
 * sweep, so they can arrive concurrently. The tail is guarded by a single monitor — the writes are
 * cheap and rare enough that a lock is clearer than a lock-free structure — while the per-user
 * rollups are a {@link ConcurrentHashMap} of internally synchronised values.
 *
 * <h2>Retention policy — the last 48 hours only</h2>
 *
 * <p>Audit history is trimmed to the last {@link #RETENTION_MS 48 hours}. The trim runs once at
 * startup and then lazily at most once an hour, piggybacked on the append path: the file is
 * rewritten atomically (tmp+move) without the expired lines, and the same lines are dropped from the
 * in-memory tail and the rollups it feeds. {@link #MAX_TAIL} still bounds memory independently.
 *
 * <p><b>Cleaning a record set also revokes any still-live session it belonged to.</b> When a purge
 * drops the records of a session that is still present in the {@link SsoSessionRegistry}, that
 * session is ended — AWS-side {@code RevokeToken} first (best-effort), then
 * {@link SsoSessionRegistry#end} — so a stale browser cannot keep using credentials whose audit
 * trail has been erased. That {@code LOGOUT} is itself recorded and ages out on the same 48h clock.
 */
@Service
public class AdminAuditService {

    private static final Logger log = LoggerFactory.getLogger(AdminAuditService.class);

    /** How many recent events are kept in memory for API reads (and restored at startup). */
    public static final int MAX_TAIL = 5000;

    /** Audit history retention: records older than this are purged. */
    public static final long RETENTION_MS = TimeUnit.HOURS.toMillis(48);

    /** Purges are guarded lazily on the append path; at most this often. */
    private static final long PURGE_INTERVAL_MS = TimeUnit.HOURS.toMillis(1);

    /** Written under {data}/admin/. */
    private static final String AUDIT_DIR = "admin";
    private static final String AUDIT_FILE = "audit.jsonl";

    private final String dataDir;

    /**
     * A private mapper rather than the injected bean: the serialised shape is this class's contract
     * with its own reader (the startup tail load), so it must not drift with whatever global Jackson
     * customisation a deployment happens to apply.
     */
    private final ObjectMapper json = new ObjectMapper();

    private final Object tailLock = new Object();
    private final ArrayDeque<AuditEntry> tail = new ArrayDeque<>();

    private final ConcurrentHashMap<String, UserRollup> rollups = new ConcurrentHashMap<>();

    /**
     * Admin-set display-name corrections, keyed by rollup key. Held apart from the rollups so a
     * retention rebuild (which reconstructs rollups from the surviving tail) never drops them.
     */
    private final ConcurrentHashMap<String, String> displayNames = new ConcurrentHashMap<>();

    /** Epoch millis of the last purge, so the O(n) trim is amortised. */
    private final AtomicLong lastPurgeEpochMs = new AtomicLong(System.currentTimeMillis());

    /** Lazily resolved so this bean can be constructed without eager registry/service wiring. */
    private final ObjectProvider<SsoSessionRegistry> registryProvider;
    private final ObjectProvider<AwsSsoLoginService> loginServiceProvider;

    public AdminAuditService(@Value("${app.data-dir:data}") String dataDir,
                             ObjectProvider<SsoSessionRegistry> registryProvider,
                             ObjectProvider<AwsSsoLoginService> loginServiceProvider) {
        this.dataDir = (dataDir == null || dataDir.isBlank()) ? "data" : dataDir.trim();
        this.registryProvider = registryProvider;
        this.loginServiceProvider = loginServiceProvider;
    }

    /** Absolute path of the audit log; created lazily on first append. */
    public Path auditPath() {
        return Path.of(dataDir).resolve(AUDIT_DIR).resolve(AUDIT_FILE);
    }

    // ── event ingestion ─────────────────────────────────────────────────────────

    /**
     * One audit line per SSO transition. Best-effort by design: a failed append is logged and
     * swallowed so that writing the audit can never change the outcome of a login or a connect.
     */
    @EventListener
    public void onAuditEvent(SsoAuditEvent event) {
        if (event == null) return;
        AuditEntry entry = AuditEntry.from(event);
        appendLine(entry);
        synchronized (tailLock) {
            tail.addLast(entry);
            while (tail.size() > MAX_TAIL) tail.removeFirst();
        }
        rollupFor(entry.key()).record(entry);
        // A login that AWS could not name inherits the name this person is already known by, so a
        // chosen display name survives across sessions. Runs after the rollup record, so the value
        // it reads is what earlier events established.
        backfillDisplayName(event, entry);
        // Lazy retention: at most once an hour, on the same path every event already takes.
        purgeIfDue(false);
    }

    // ── display-name backfill ───────────────────────────────────────────────────

    /**
     * Re-applies the name a person is already known by to a fresh, nameless login.
     *
     * <h2>The gap this closes</h2>
     *
     * <p>Identity Center's device flow frequently yields no name and no email, so a login lands as an
     * anonymous account id. The person (or an admin) then gives themselves a display name — at
     * {@code /connect}, or through the admin Users tab — which is held here, keyed by the account id
     * that <em>is</em> their identity key while AWS names them nothing. That name lives on the
     * rollup, and on the session it was set in, but a <em>later</em> login starts from AWS's blank
     * identity again and shows nothing. So a name that is durable in the audit trail is not durable
     * in the UI, which is the inconsistency this fixes.
     *
     * <h2>What it does</h2>
     *
     * <p>On a {@link SsoAuditEvent#LOGIN} whose identity has neither a name nor an email, it looks up
     * the display name already recorded for that person and writes it onto the live
     * {@link SsoSessionState} for the event's session, so {@code /session} (and every other surface
     * that renders {@link SsoIdentity#shownName()}) reports it for this login too.
     *
     * <h2>Why a real name always wins</h2>
     *
     * <p>The backfill only runs when AWS supplied nothing. The moment Identity Center resolves a
     * genuine name or email — via the id_token or the Identity Store lookup — {@link SsoIdentity#name}
     * is non-null, this method stands down, and the AWS-derived name is what is shown. The display
     * name is a placeholder until a real one exists, never an override of one.
     *
     * <p>Best-effort and listener-local: any failure leaves the login exactly as it was, and no
     * token or credential value is read on this path.
     */
    private void backfillDisplayName(SsoAuditEvent event, AuditEntry entry) {
        if (event == null || !SsoAuditEvent.LOGIN.equals(event.type())) return;
        SsoIdentity identity = event.identity();
        if (identity == null) return;
        // A real name/email from AWS (or Identity Store) outranks anything remembered here.
        if (blankToNull(identity.name()) != null || blankToNull(identity.email()) != null) return;
        // Nothing to do when this login already carries a chosen name (only reachable if a caller
        // set one before the event, which /connect does).
        if (blankToNull(identity.displayName()) != null) return;
        String accountId = blankToNull(identity.accountId());
        if (accountId == null || event.sessionId() == null || event.sessionId().isBlank()) return;

        String remembered = storedDisplayNameFor(entry.key(), accountId);
        if (remembered == null) return;

        SsoSessionRegistry registry = registryProvider.getIfAvailable();
        if (registry == null) return;
        SsoSessionState state = registry.get(event.sessionId());
        if (state == null || !state.loggedIn()) return;
        SsoIdentity live = state.identity();
        // Re-check on what the session actually holds now: Identity Store may have resolved a real
        // name between the event being built and this listener running.
        if (live == null || blankToNull(live.name()) != null || blankToNull(live.email()) != null) return;
        if (blankToNull(live.displayName()) != null) return;

        state.identity(live.withDisplayName(remembered));
        // The one identity-source line for a login AWS could not name at all: the source is the
        // remembered display name. Never a value.
        log.info("[aws-sso] identity source: backfill (a stored display name was applied to this "
                + "login's session)");
    }

    /**
     * The name this person is already known by, or {@code null} when none was ever recorded.
     *
     * <p>Three sources, in order of how specific they are:
     *
     * <ol>
     *   <li>an admin-set override on the event's own rollup key (the account id, while AWS names
     *       them nothing) — {@code displayNames};</li>
     *   <li>the snapshot name on that same rollup, which is what an earlier event carried;</li>
     *   <li>a name recorded against any <em>other</em> rollup for the same AWS account. A person's
     *       rollup key moves from the account id to their chosen name the moment one exists (see
     *       {@link #identityKey}), so the name set at {@code /connect} lives under a name-keyed
     *       rollup that the account id alone would never find. Accepted only when every such rollup
     *       agrees on the name, so a shared AWS account with several named people can never cause
     *       one person's name to be handed to another.</li>
     * </ol>
     */
    private String storedDisplayNameFor(String key, String accountId) {
        String direct = key == null ? null : displayNames.get(key);
        if (blankToNull(direct) != null) return direct.trim();
        UserRollup own = key == null ? null : rollups.get(key);
        if (own != null && blankToNull(own.name()) != null) return own.name().trim();

        String found = null;
        for (UserRollup r : rollups.values()) {
            if (!accountId.equals(r.accountId()) || blankToNull(r.name()) == null) continue;
            String override = displayNames.get(r.key());
            String candidate = blankToNull(override) != null ? override.trim() : r.name().trim();
            if (found == null) found = candidate;
            else if (!found.equals(candidate)) return null; // several names for one account: stay out
        }
        return found;
    }

    /**
     * Rebuilds the recent history from the file so a restart does not present an empty portal before
     * the first new event arrives. Only the last {@link #MAX_TAIL} lines are retained; a malformed
     * line (a torn write from a previous crash) is skipped rather than aborting the load.
     */
    @PostConstruct
    void loadTail() {
        Path path = auditPath();
        if (!Files.isRegularFile(path)) return;

        ArrayDeque<AuditEntry> loaded = new ArrayDeque<>();
        long malformed = 0;
        try (Stream<String> lines = Files.lines(path, StandardCharsets.UTF_8)) {
            Iterator<String> it = lines.iterator();
            while (it.hasNext()) {
                String line = it.next();
                if (line.isBlank()) continue;
                AuditEntry entry = parse(line);
                if (entry == null) {
                    malformed++;
                    continue;
                }
                loaded.addLast(entry);
                if (loaded.size() > MAX_TAIL) loaded.removeFirst();
            }
        } catch (IOException | RuntimeException ex) {
            log.warn("[admin-audit] could not read {}: {}", path, ex.getMessage());
            return;
        }

        synchronized (tailLock) {
            tail.clear();
            tail.addAll(loaded);
        }
        for (AuditEntry entry : loaded) rollupFor(entry.key()).record(entry);
        if (malformed > 0) log.warn("[admin-audit] skipped {} malformed line(s) in {}", malformed, path);
        log.info("[admin-audit] restored {} recent event(s) from {}", loaded.size(), path);
        // Enforce retention once at startup, so a long downtime's worth of expired records does not
        // linger until the first new event arrives.
        purgeIfDue(true);
    }

    private void appendLine(AuditEntry entry) {
        Path path = auditPath();
        try {
            Files.createDirectories(path.getParent());
            Files.writeString(path, toJson(entry) + "\n", StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE);
        } catch (IOException ex) {
            log.warn("[admin-audit] append to {} failed: {}", path, ex.getMessage());
        }
    }

    private String toJson(AuditEntry e) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("type", e.type());
        m.put("timestampEpochMs", e.timestampEpochMs());
        m.put("sessionId", e.sessionId());
        m.put("name", e.name());
        m.put("email", e.email());
        m.put("accountId", e.accountId());
        m.put("accountName", e.accountName());
        m.put("roleName", e.roleName());
        m.put("bucket", e.bucket());
        m.put("region", e.region());
        m.put("capabilities", capabilitiesToMap(e.capabilities()));
        try {
            return json.writeValueAsString(m);
        } catch (Exception ex) {
            // Cannot happen for this map of primitives/strings, but never let it escape the listener.
            log.warn("[admin-audit] serialisation failed: {}", ex.getMessage());
            return "{}";
        }
    }

    private static Map<String, Object> capabilitiesToMap(SsoStoreSwapper.Capabilities caps) {
        if (caps == null) return null;
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("read", caps.read());
        m.put("upload", caps.upload());
        m.put("write", caps.write());
        m.put("ok", caps.ok());
        m.put("error", caps.error());
        return m;
    }

    private AuditEntry parse(String line) {
        try {
            JsonNode n = json.readTree(line);
            if (n == null || !n.isObject()) return null;
            SsoStoreSwapper.Capabilities caps = null;
            JsonNode c = n.get("capabilities");
            if (c != null && c.isObject()) {
                caps = new SsoStoreSwapper.Capabilities(
                        c.path("read").asBoolean(false),
                        c.path("upload").asBoolean(false),
                        c.path("write").asBoolean(false),
                        c.path("ok").asBoolean(false),
                        textOrNull(c.get("error")));
            }
            return new AuditEntry(
                    textOrNull(n.get("type")),
                    textOrNull(n.get("sessionId")),
                    textOrNull(n.get("name")),
                    textOrNull(n.get("email")),
                    textOrNull(n.get("accountId")),
                    textOrNull(n.get("accountName")),
                    textOrNull(n.get("roleName")),
                    textOrNull(n.get("bucket")),
                    textOrNull(n.get("region")),
                    caps,
                    n.path("timestampEpochMs").asLong(0L));
        } catch (Exception ex) {
            return null;
        }
    }

    private static String textOrNull(JsonNode node) {
        return (node == null || node.isNull()) ? null : node.asText();
    }

    // ── read side ───────────────────────────────────────────────────────────────

    /**
     * Newest-first events, optionally narrowed by exact {@code type} and a case-insensitive
     * substring {@code user} match over the identity fields and the rollup key.
     */
    public List<AuditEntry> query(int limit, String type, String user) {
        List<AuditEntry> snapshot;
        synchronized (tailLock) {
            snapshot = new ArrayList<>(tail);
        }
        String wantedType = blankToNull(type);
        String wantedUser = blankToNull(user);
        List<AuditEntry> out = new ArrayList<>();
        if (limit <= 0) return out;
        for (int i = snapshot.size() - 1; i >= 0 && out.size() < limit; i--) {
            AuditEntry e = snapshot.get(i);
            if (wantedType != null && !wantedType.equalsIgnoreCase(e.type())) continue;
            if (wantedUser != null && !e.matchesUser(wantedUser)) continue;
            out.add(e);
        }
        return out;
    }

    /** Every user rollup, most recently seen first. */
    public List<UserRollup> rollups() {
        List<UserRollup> out = new ArrayList<>(rollups.values());
        out.sort(Comparator.comparingLong(UserRollup::lastSeenEpochMs).reversed());
        return out;
    }

    public UserRollup rollup(String key) {
        return key == null ? null : rollups.get(key);
    }

    private UserRollup rollupFor(String key) {
        return rollups.computeIfAbsent(key, UserRollup::new);
    }

    // ── mutations ───────────────────────────────────────────────────────────────

    /** Truncates the file and drops the in-memory tail and rollups, so the two stay consistent. */
    public void clear() {
        Path path = auditPath();
        try {
            Files.createDirectories(path.getParent());
            Files.writeString(path, "", StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        } catch (IOException ex) {
            log.warn("[admin-audit] truncate of {} failed: {}", path, ex.getMessage());
        }
        synchronized (tailLock) {
            tail.clear();
        }
        rollups.clear();
        displayNames.clear();
    }

    /**
     * Forgets one user: drops their rollup and rewrites the file without their lines.
     *
     * <p>The rewrite is line-preserving for everyone else — every line is parsed only to learn whose
     * it is, then re-emitted verbatim, so unrelated events (and even malformed lines, which belong to
     * no identifiable user) survive byte for byte.
     *
     * @return true when something was removed from memory or the file
     */
    public boolean forget(String key) {
        if (key == null || key.isBlank()) return false;
        boolean removed = rollups.remove(key) != null;
        removed |= displayNames.remove(key) != null;

        synchronized (tailLock) {
            removed |= tail.removeIf(e -> key.equals(e.key()));
        }

        Path path = auditPath();
        if (!Files.isRegularFile(path)) return removed;

        try {
            List<String> kept = new ArrayList<>();
            try (Stream<String> lines = Files.lines(path, StandardCharsets.UTF_8)) {
                Iterator<String> it = lines.iterator();
                while (it.hasNext()) {
                    String line = it.next();
                    if (line.isBlank()) continue;
                    AuditEntry e = parse(line);
                    if (e != null && key.equals(e.key())) {
                        removed = true;
                        continue;
                    }
                    kept.add(line);
                }
            }
            Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
            Files.write(tmp, kept, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
            try {
                Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException atomicUnsupported) {
                Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException ex) {
            log.warn("[admin-audit] forget rewrite of {} failed: {}", path, ex.getMessage());
        }
        return removed;
    }

    // ── display-name overrides ──────────────────────────────────────────────────

    /** The admin-set display name for a rollup key, or {@code null} when none is set. */
    public String displayNameFor(String key) {
        return key == null ? null : displayNames.get(key);
    }

    /**
     * Sets — or, with a blank value, clears — the admin display-name override for a known user
     * rollup. Stored apart from the rollups so it survives a retention rebuild.
     *
     * @return false when the key names no known user, so the caller can answer "not found"
     */
    public boolean setDisplayName(String key, String displayName) {
        if (key == null || key.isBlank()) return false;
        if (rollups.get(key) == null) return false;
        if (displayName == null || displayName.isBlank()) displayNames.remove(key);
        else displayNames.put(key, displayName.trim());
        return true;
    }

    // ── retention ───────────────────────────────────────────────────────────────

    /**
     * Runs the 48h trim if due (or always when {@code force}). Single-flight: a losing thread simply
     * returns, because a concurrent purge already covers the same window.
     */
    private void purgeIfDue(boolean force) {
        long now = System.currentTimeMillis();
        long last = lastPurgeEpochMs.get();
        if (!force && now - last < PURGE_INTERVAL_MS) return;
        if (!lastPurgeEpochMs.compareAndSet(last, now)) return;
        try {
            purgeOlderThan(now - RETENTION_MS);
        } catch (RuntimeException e) {
            log.warn("[admin-audit] retention purge failed ({}); will retry later", e.getClass().getSimpleName());
        }
    }

    /**
     * Drops every record older than {@code cutoffMs} from the file, the tail and the rollups, then
     * revokes any still-live session whose records were fully cleaned.
     */
    private void purgeOlderThan(long cutoffMs) {
        Path path = auditPath();
        Set<String> purgedSessions = new HashSet<>();
        int removed = 0;
        synchronized (tailLock) {
            // File first, rewritten atomically. Malformed lines belong to no identifiable record
            // and cannot be aged, so they are preserved verbatim, as in forget().
            if (Files.isRegularFile(path)) {
                List<String> kept = new ArrayList<>();
                try (Stream<String> lines = Files.lines(path, StandardCharsets.UTF_8)) {
                    Iterator<String> it = lines.iterator();
                    while (it.hasNext()) {
                        String line = it.next();
                        if (line.isBlank()) continue;
                        AuditEntry e = parse(line);
                        if (e != null && expired(e, cutoffMs)) {
                            if (e.sessionId() != null) purgedSessions.add(e.sessionId());
                            removed++;
                            continue;
                        }
                        kept.add(line);
                    }
                    if (removed > 0) writeAtomically(path, kept);
                } catch (IOException ex) {
                    log.warn("[admin-audit] retention read of {} failed: {}", path, ex.getMessage());
                }
            }

            Iterator<AuditEntry> it = tail.iterator();
            while (it.hasNext()) {
                AuditEntry e = it.next();
                if (expired(e, cutoffMs)) {
                    if (e.sessionId() != null) purgedSessions.add(e.sessionId());
                    it.remove();
                    removed++;
                }
            }

            // Rebuild rollups from the surviving tail — the same source they were first built from —
            // so a user whose only records expired disappears. Display-name overrides live apart and
            // are re-applied by the read side, not lost here.
            rollups.clear();
            Set<String> survivors = new HashSet<>();
            for (AuditEntry e : tail) {
                rollupFor(e.key()).record(e);
                if (e.sessionId() != null) survivors.add(e.sessionId());
            }
            // A session is only "cleaned" when none of its records survive; otherwise an active
            // session whose old LOGIN aged out but whose CONNECT is recent would be wrongly revoked.
            purgedSessions.removeAll(survivors);
        }

        if (removed > 0) {
            log.info("[admin-audit] retention removed {} record(s) older than {}h",
                    removed, RETENTION_MS / 3_600_000L);
        }
        // Outside the lock and best-effort: revoke the fully-cleaned sessions that are still live.
        revokeCleanedSessions(purgedSessions);
    }

    private void revokeCleanedSessions(Set<String> sessionIds) {
        if (sessionIds == null || sessionIds.isEmpty()) return;
        SsoSessionRegistry registry = registryProvider.getIfAvailable();
        if (registry == null) return;
        AwsSsoLoginService login = loginServiceProvider.getIfAvailable();
        for (String sessionId : sessionIds) {
            if (sessionId == null) continue;
            SsoSessionState state = registry.get(sessionId);
            if (state == null || !state.loggedIn()) continue; // not live: nothing to revoke
            if (login != null) login.revokeAwsToken(state);
            // The LOGOUT it publishes is itself retained for the same 48h window.
            registry.end(sessionId);
            log.info("[admin-audit] revoked a live session whose audit records aged out");
        }
    }

    private static boolean expired(AuditEntry e, long cutoffMs) {
        return e.timestampEpochMs() > 0 && e.timestampEpochMs() < cutoffMs;
    }

    /** Writes {@code lines} to {@code path} via a sibling temp file and an atomic move. */
    private void writeAtomically(Path path, List<String> lines) throws IOException {
        Path tmp = path.resolveSibling(path.getFileName() + ".retention.tmp");
        Files.write(tmp, lines, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        try {
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException atomicUnsupported) {
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    // ── shared helpers ──────────────────────────────────────────────────────────

    /**
     * The per-user key. Email first, then name, then account id — the most human-identifying claim
     * available, falling back to the always-present account id. Lower-cased for email/name so two
     * spellings of the same address do not fork a user in two.
     */
    public static String identityKey(SsoIdentity identity) {
        if (identity == null) return null;
        return identityKey(identity.shownName(), identity.email(), identity.accountId());
    }

    public static String identityKey(String name, String email, String accountId) {
        if (email != null && !email.isBlank()) return email.trim().toLowerCase();
        if (name != null && !name.isBlank()) return name.trim().toLowerCase();
        if (accountId != null && !accountId.isBlank()) return accountId.trim();
        return null;
    }

    /** First eight characters of a session id plus an ellipsis; ids shorter than that pass through. */
    public static String maskSession(String sessionId) {
        if (sessionId == null || sessionId.isBlank()) return null;
        return sessionId.length() <= 8 ? sessionId : sessionId.substring(0, 8) + "…";
    }

    private static String blankToNull(String s) {
        return (s == null || s.isBlank()) ? null : s.trim();
    }

    // ── value types ─────────────────────────────────────────────────────────────

    /**
     * One flattened audit record. Flattened rather than keeping the {@link SsoIdentity} nested so the
     * JSON line is flat and the identity key is computable from a single object whether it came from
     * a live event or from re-reading the file.
     */
    public record AuditEntry(String type,
                             String sessionId,
                             String name,
                             String email,
                             String accountId,
                             String accountName,
                             String roleName,
                             String bucket,
                             String region,
                             SsoStoreSwapper.Capabilities capabilities,
                             long timestampEpochMs) {

        static AuditEntry from(SsoAuditEvent event) {
            SsoIdentity id = event.identity();
            return new AuditEntry(
                    event.type(),
                    event.sessionId(),
                    // The name the person chose to be shown as, when they set one; else the
                    // AWS-derived name. This is what the admin Users tab and audit "User" column
                    // render, so a display-name override reaches the audit trail too.
                    id == null ? null : id.shownName(),
                    id == null ? null : id.email(),
                    id == null ? null : id.accountId(),
                    id == null ? null : id.accountName(),
                    id == null ? null : id.roleName(),
                    event.bucket(),
                    event.region(),
                    event.capabilities(),
                    event.timestampEpochMs());
        }

        /**
         * The rollup key this entry belongs to. Identity-less events (a logout whose state was
         * already cleared, a denial before login) still need a home, so they key on the session.
         */
        public String key() {
            String k = identityKey(name, email, accountId);
            if (k != null) return k;
            String masked = maskSession(sessionId);
            return masked == null ? "unknown" : "session:" + masked;
        }

        boolean matchesUser(String needle) {
            String n = needle.toLowerCase();
            return contains(name, n) || contains(email, n) || contains(accountId, n)
                    || contains(accountName, n) || contains(roleName, n)
                    || contains(key(), n) || contains(sessionId, n);
        }

        private static boolean contains(String haystack, String lowerNeedle) {
            return haystack != null && haystack.toLowerCase().contains(lowerNeedle);
        }
    }

    /**
     * The per-user rollup: first/last seen, the latest identity snapshot, the latest verified target
     * and a count per event type. All mutation is synchronised on the instance; reads hand back
     * copies so a concurrent event cannot tear a snapshot mid-serialisation.
     */
    public static final class UserRollup {

        private final String key;
        private String name;
        private String email;
        private String accountId;
        private String accountName;
        private String roleName;
        private String bucket;
        private String region;
        private long firstSeenEpochMs;
        private long lastSeenEpochMs;
        private final Map<String, Integer> events = new LinkedHashMap<>();

        UserRollup(String key) {
            this.key = key;
        }

        synchronized void record(AuditEntry e) {
            if (firstSeenEpochMs == 0L || (e.timestampEpochMs() > 0 && e.timestampEpochMs() < firstSeenEpochMs)) {
                firstSeenEpochMs = e.timestampEpochMs();
            }
            if (e.timestampEpochMs() > lastSeenEpochMs) lastSeenEpochMs = e.timestampEpochMs();
            // Snapshot fields are only ever replaced by a non-null value: an event without an
            // identity (a logout) must not blank out what an earlier event already knew.
            if (e.name() != null) name = e.name();
            if (e.email() != null) email = e.email();
            if (e.accountId() != null) accountId = e.accountId();
            if (e.accountName() != null) accountName = e.accountName();
            if (e.roleName() != null) roleName = e.roleName();
            if (e.bucket() != null) bucket = e.bucket();
            if (e.region() != null) region = e.region();
            if (e.type() != null) events.merge(e.type(), 1, Integer::sum);
        }

        public synchronized String key() { return key; }
        public synchronized String name() { return name; }
        public synchronized String email() { return email; }
        public synchronized String accountId() { return accountId; }
        public synchronized String accountName() { return accountName; }
        public synchronized String roleName() { return roleName; }
        public synchronized String bucket() { return bucket; }
        public synchronized String region() { return region; }
        public synchronized long firstSeenEpochMs() { return firstSeenEpochMs; }
        public synchronized long lastSeenEpochMs() { return lastSeenEpochMs; }

        /** A copy, so callers serialise a consistent snapshot. */
        public synchronized Map<String, Integer> eventCounts() {
            return new LinkedHashMap<>(events);
        }
    }
}
