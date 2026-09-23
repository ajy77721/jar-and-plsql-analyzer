package com.analyzer.admin;

import com.analyzer.storage.sso.SsoAuditEvent;
import com.analyzer.storage.sso.SsoIdentity;
import com.analyzer.storage.sso.SsoStoreSwapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
 */
@Service
public class AdminAuditService {

    private static final Logger log = LoggerFactory.getLogger(AdminAuditService.class);

    /** How many recent events are kept in memory for API reads (and restored at startup). */
    public static final int MAX_TAIL = 5000;

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

    public AdminAuditService(@Value("${app.data-dir:data}") String dataDir) {
        this.dataDir = (dataDir == null || dataDir.isBlank()) ? "data" : dataDir.trim();
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

    // ── shared helpers ──────────────────────────────────────────────────────────

    /**
     * The per-user key. Email first, then name, then account id — the most human-identifying claim
     * available, falling back to the always-present account id. Lower-cased for email/name so two
     * spellings of the same address do not fork a user in two.
     */
    public static String identityKey(SsoIdentity identity) {
        if (identity == null) return null;
        return identityKey(identity.name(), identity.email(), identity.accountId());
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
                    id == null ? null : id.name(),
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
