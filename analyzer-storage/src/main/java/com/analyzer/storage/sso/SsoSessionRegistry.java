package com.analyzer.storage.sso;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The per-user registry of AWS SSO sessions, keyed by {@code HttpSession.getId()}.
 *
 * <h2>The frozen contract</h2>
 *
 * <p>This is the one collaborator the web layer and the credential/store routing talk to. Its public
 * surface is fixed so the pieces wired around it can be written independently of the flow internals:
 *
 * <ul>
 *   <li>{@link #get(String)} — the state for a session, or {@code null}. Creates nothing.</li>
 *   <li>{@link #isAuthenticated(String)} — has that session completed the device flow?</li>
 *   <li>{@link #identity(String)} — who that session is, or {@code null} when not logged in.</li>
 *   <li>{@link #end(String)} — drop the session (logout).</li>
 * </ul>
 *
 * <p>{@link #getOrCreate(String)} is the one extra method the SSO controller needs: {@code /start}
 * must be able to attach flow state to a session that does not have any yet. {@link #listActive()}
 * is the admin-portal view of who is connected to what.
 *
 * <h2>Why a map keyed by HttpSession id</h2>
 *
 * <p>The tokens, the in-progress device code and the verified bucket/region all belong to one
 * browser, so one map keyed by the container-issued session id is the natural home. The id is an
 * opaque, server-generated, unguessable string, so it is safe as a map key and never needs to be
 * parsed or logged. The map itself is a {@link ConcurrentHashMap}: reads happen on the S3 request
 * path from many threads at once, while {@code /start}/{@code /logout} mutate it.
 *
 * <h2>Idle expiry</h2>
 *
 * <p>A browser that simply disappears never calls {@code /logout}, so entries are swept lazily:
 * every {@link #get(String)} first checks whether a sweep is due (at most once a minute) and, if so,
 * removes entries untouched for {@link #IDLE_TTL_MS}. Lazy-on-access is chosen over a scheduled
 * task deliberately — it needs no {@code @EnableScheduling} and it cannot silently stop running if
 * scheduling is not enabled in some deployment. A session that is being used is touched on every
 * access, so it can never be swept out from under an active user; a sweep emits {@code LOGOUT} audit
 * events just as an explicit logout does.
 */
@Component
public class SsoSessionRegistry {

    private static final Logger log = LoggerFactory.getLogger(SsoSessionRegistry.class);

    /** How long a session may sit untouched before it is dropped. */
    static final long IDLE_TTL_MS = 24L * 60 * 60 * 1000;

    /** Sweeps are cheap but unnecessary per request; run at most this often. */
    private static final long SWEEP_INTERVAL_MS = 60L * 1000;

    private final ConcurrentHashMap<String, SsoSessionState> sessions = new ConcurrentHashMap<>();

    /** Epoch millis of the last sweep, so the O(n) pass is amortised. */
    private final AtomicLong lastSweepEpochMs = new AtomicLong(System.currentTimeMillis());

    private final ApplicationEventPublisher events;

    public SsoSessionRegistry(ApplicationEventPublisher events) {
        this.events = events;
    }

    /**
     * The state for {@code sessionId}, or {@code null} when there is none.
     *
     * <p>Reading a live session counts as activity, so it is touched and cannot be expired while in
     * use. A null/blank id — a request thread with no session, which is the normal state for a
     * background job — yields {@code null} rather than throwing, because "no session" is a legal
     * answer the caller routes to the default credentials on.
     */
    public SsoSessionState get(String sessionId) {
        sweepIfDue();
        if (sessionId == null || sessionId.isBlank()) return null;
        SsoSessionState state = sessions.get(sessionId);
        if (state != null) state.touch();
        return state;
    }

    /**
     * The state for {@code sessionId}, creating an empty one if absent. Only the SSO controller's
     * {@code /start}/connect path calls this; every read-only consumer uses {@link #get(String)} so
     * that merely reading a status endpoint never allocates state.
     */
    public SsoSessionState getOrCreate(String sessionId) {
        if (sessionId == null || sessionId.isBlank()) {
            throw new IllegalArgumentException("A session id is required to track an AWS SSO login");
        }
        sweepIfDue();
        return sessions.computeIfAbsent(sessionId, SsoSessionState::new);
    }

    /** True when this session has completed a device-flow login and has not been logged out. */
    public boolean isAuthenticated(String sessionId) {
        SsoSessionState state = get(sessionId);
        return state != null && state.loggedIn();
    }

    /**
     * Who this session is, or {@code null} when it is not logged in.
     *
     * <p>Returns the full {@link SsoIdentity} including the best-effort {@code name}/{@code email}
     * claims; individual fields inside it may themselves be {@code null}.
     */
    public SsoIdentity identity(String sessionId) {
        SsoSessionState state = get(sessionId);
        return (state == null || !state.loggedIn()) ? null : state.identity();
    }

    /**
     * Which mechanism authenticated this session — {@code "iam"} for classic access keys,
     * {@code "sso"} otherwise — or {@code null} when the session is unknown. Carried onto the audit
     * trail so a CONNECT/LOGOUT can be attributed to the credential source behind it.
     */
    public String credentialSource(String sessionId) {
        SsoSessionState state = get(sessionId);
        return state == null ? null : state.credentialSource();
    }

    /**
     * A snapshot of every tracked session, for the admin portal: who each session is (when logged
     * in), which bucket/region it verified, and when it was last active.
     *
     * <p>Sessions that have started a flow but not completed it are included, with a {@code null}
     * {@code identity}; a consumer wanting only fully-joined users filters on that. The list is a
     * copy, so iterating it while users log in or out is safe.
     */
    public List<ActiveUserInfo> listActive() {
        sweepIfDue();
        List<ActiveUserInfo> out = new ArrayList<>(sessions.size());
        for (SsoSessionState state : sessions.values()) {
            out.add(new ActiveUserInfo(
                    state.sessionId(),
                    state.identity(),
                    state.bucket(),
                    state.region(),
                    state.capabilities(),
                    state.lastAccessEpochMs()));
        }
        return out;
    }

    /**
     * Ends a session: clears its tokens and drops it from the registry, emitting a {@code LOGOUT}
     * audit event. Safe to call for an unknown or already-ended session, and for a {@code null} id,
     * so {@code /logout} never has to check.
     */
    public void end(String sessionId) {
        if (sessionId == null || sessionId.isBlank()) return;
        SsoSessionState removed = sessions.remove(sessionId);
        if (removed != null) {
            removed.clear();
            log.info("[aws-sso] session {} ended", shortId(sessionId));
            publishAudit(SsoAuditEvent.LOGOUT, removed);
        }
    }

    /** Number of tracked sessions; diagnostics and tests only. */
    public int size() {
        return sessions.size();
    }

    /**
     * Removes sessions untouched for longer than {@link #IDLE_TTL_MS}, at most once per
     * {@link #SWEEP_INTERVAL_MS}. Called from {@link #get(String)} — see the class Javadoc for why
     * this is lazy rather than scheduled.
     */
    private void sweepIfDue() {
        long now = System.currentTimeMillis();
        long last = lastSweepEpochMs.get();
        if (now - last < SWEEP_INTERVAL_MS) return;
        if (!lastSweepEpochMs.compareAndSet(last, now)) return; // another thread is sweeping
        int removed = 0;
        Iterator<Map.Entry<String, SsoSessionState>> it = sessions.entrySet().iterator();
        while (it.hasNext()) {
            SsoSessionState state = it.next().getValue();
            if (now - state.lastAccessEpochMs() > IDLE_TTL_MS) {
                it.remove();
                state.clear();
                publishAudit(SsoAuditEvent.LOGOUT, state);
                removed++;
            }
        }
        if (removed > 0) {
            log.info("[aws-sso] swept {} idle SSO session(s) (idle > {}h)",
                    removed, IDLE_TTL_MS / 3_600_000L);
        }
    }

    /**
     * Publishes a {@code LOGOUT} for {@code state} using whatever identity/target it still held.
     * Kept here so both the explicit logout and the idle sweep report the same way.
     */
    private void publishAudit(String type, SsoSessionState state) {
        try {
            events.publishEvent(new SsoAuditEvent(type, state.sessionId(), state.identity(),
                    state.bucket(), state.region(), state.capabilities(),
                    System.currentTimeMillis(), state.credentialSource()));
        } catch (RuntimeException e) {
            // Never let an audit listener break logout.
            log.debug("[aws-sso] audit event {} could not be published ({})",
                    type, e.getClass().getSimpleName());
        }
    }

    /**
     * A short, non-reversible suffix of a session id for a log line — enough to correlate without
     * writing a value that could be replayed as a session identifier.
     */
    private static String shortId(String sessionId) {
        return sessionId.length() <= 8 ? "…" : "…" + sessionId.substring(sessionId.length() - 8);
    }
}
