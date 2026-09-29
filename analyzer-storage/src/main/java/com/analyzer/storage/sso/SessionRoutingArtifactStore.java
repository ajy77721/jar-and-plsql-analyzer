package com.analyzer.storage.sso;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.LocalHandle;
import com.analyzer.storage.RequestScopedArtifactStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * The {@link ArtifactStore} bean an S3-backed feature injects. On every operation it resolves the
 * calling user's session and delegates to the store that user connected; a request that carries an
 * unauthenticated session is refused rather than served from the wrong bucket.
 *
 * <h2>The routing rule</h2>
 *
 * <ul>
 *   <li><b>A session, logged in and connected</b> — delegate to that user's store, built for their
 *       verified bucket/region and signed with their role. This is the per-user path.</li>
 *   <li><b>A session that is not logged in</b> — throw {@link NotAuthenticatedException}. The web
 *       layer maps it to 401, so an unauthenticated browser cannot read or write storage at all.</li>
 *   <li><b>A session that is logged in but has not connected a bucket</b> — throw
 *       {@link NotAuthenticatedException} with a "connect your account first" message. Serving the
 *       shared default store here silently wrote the user's artifacts to the shared bucket under the
 *       wrong principal (the live F1 failure); per-user isolation is not a thing the user can opt
 *       out of by not connecting.</li>
 *   <li><b>A job snapshot whose identity is resolvable but incomplete</b> — a {@link SsoJobContext}
 *       is installed with a session id, yet it holds no store for this feature (a credentials-only
 *       snapshot, F3). Same throw: a resolvable identity must never be handed the shared default
 *       store.</li>
 *   <li><b>No request-bound session and no job snapshot</b> — delegate to the startup store, which
 *       signs with the default AWS chain and the configured bucket/region. This is a background job
 *       thread without a snapshot, a CLI, or the pre-SSO local workflow, all of which must keep
 *       working exactly as before. A throttled WARN is logged so this path can never masquerade as
 *       correct behaviour during diagnosis. A job that wants to run under its submitter instead
 *       should be handed a {@link CapturedStores} snapshot taken on the request thread.</li>
 * </ul>
 *
 * <h2>Why a stable façade</h2>
 *
 * <p>Every consumer injects the bean once and holds it for the life of the context, so "swap the
 * store" is not possible — the injection already happened. Routing the value they hold keeps that
 * contract: callers still see one {@link ArtifactStore}, and cannot tell whether they were served a
 * per-user store or the default one.
 *
 * <p>One instance exists per registered feature ({@code parser}, {@code jar}); {@link #name} selects
 * which half of the user's {@link SsoStoreSwapper.BuiltStores} to delegate to.
 *
 * <p>Delegation is deliberately a pure forward for every method — no caching of the resolved store
 * beyond the single call — because the resolution is a map lookup and a volatile read, and caching a
 * per-thread delegate would pin a user's target across a reconnect.
 */
public final class SessionRoutingArtifactStore implements RequestScopedArtifactStore {

    private static final Logger log = LoggerFactory.getLogger(SessionRoutingArtifactStore.class);

    /**
     * How often the "no session at all" fallback may emit its WARN. Long enough that a background
     * job's per-artifact store calls do not flood the log, short enough that the fallback is always
     * visible in a fresh diagnostic window.
     */
    private static final long NO_SESSION_WARN_INTERVAL_MS = 60_000L;

    /**
     * The message for "the identity resolved, but it has no bucket to route to". Kept as a constant
     * so the audit's expected wording is exact; the web layer's 401 body still uses
     * {@link NotAuthenticatedException#MESSAGE}, but this text is what a log or a direct caller sees.
     */
    static final String NO_BUCKET_CONNECTED =
            "Logged in but no AWS bucket connected — connect your account first "
                    + "(was going to fall back to the shared default store)";

    /** Which registered half of a user's built stores this façade fronts. */
    private final String name;

    /** What a request with no session-bound state falls back to; see the class Javadoc. */
    private final ArtifactStore defaultStore;

    private final SsoStoreSwapper swapper;
    private final SsoSessionRegistry registry;
    private final CurrentHttpSession currentSession;

    /** Wall-clock time of the last "no session" WARN; only ever read/written by {@link #warnNoSessionFallback()}. */
    private final AtomicLong lastNoSessionWarnMs = new AtomicLong(0L);

    public SessionRoutingArtifactStore(String name,
                                       ArtifactStore defaultStore,
                                       SsoStoreSwapper swapper,
                                       SsoSessionRegistry registry,
                                       CurrentHttpSession currentSession) {
        if (defaultStore == null) throw new IllegalArgumentException("defaultStore is required");
        this.name = name;
        this.defaultStore = defaultStore;
        this.swapper = swapper;
        this.registry = registry;
        this.currentSession = currentSession;
    }

    /**
     * The store to serve this operation from — the user's when there is a connected session, the
     * default store only when there is no session identity at all (warned, throttled), and a thrown
     * {@link NotAuthenticatedException} when an identity is resolvable but incomplete. See the class
     * Javadoc for the exact rule.
     */
    private ArtifactStore delegate() {
        // A background job running with its submitter's snapshot installed on this thread takes
        // precedence over request discovery: the queue thread has no HttpSession, so without this
        // the job would write to the default bucket under the default chain. See SsoJobContext.
        ArtifactStore jobStore = SsoJobContext.storeFor(name);
        if (jobStore != null) return jobStore;
        // F3: a snapshot is installed and its identity is resolvable (it carries a session id) but
        // it holds no store for this feature — a credentials-only snapshot, or a feature the
        // capture did not build. Serving the shared default store here would sign the job's work as
        // the wrong principal on a pod with no ambient credentials; refuse instead.
        if (SsoJobContext.current() != null && SsoJobContext.sessionId() != null) {
            throw new NotAuthenticatedException(NO_BUCKET_CONNECTED);
        }
        String sessionId = currentSession.currentSessionId();
        if (sessionId == null) {
            // No session-bound state at all: a background job without a snapshot, a CLI, or the
            // pre-SSO local workflow. The legacy default-store path is kept, but it is never allowed
            // to be silent — a throttled WARN records every window in which it is taken.
            warnNoSessionFallback();
            return defaultStore;
        }
        SsoSessionState state = registry.get(sessionId);
        if (state == null || !state.loggedIn()) {
            throw new NotAuthenticatedException();
        }
        SsoStoreSwapper.BuiltStores stores = swapper.storesFor(sessionId);
        ArtifactStore perUser = stores == null ? null : stores.storeFor(name);
        if (perUser == null) {
            // F1(b), the live failure: logged in but no bucket connected. This used to serve the
            // shared default store, silently writing the user's artifacts to the shared bucket (or
            // failing to sign at all on a pod with no ambient credentials). A resolvable identity
            // with no connected bucket is an incomplete identity, not a licence to use the shared
            // target — tell the user to connect, and surface it as a 401 through the web advice.
            throw new NotAuthenticatedException(NO_BUCKET_CONNECTED);
        }
        return perUser;
    }

    /**
     * Emits at most one WARN per {@link #NO_SESSION_WARN_INTERVAL_MS} for the no-session default
     * fallback. Uses a CAS on {@link #lastNoSessionWarnMs} so a burst of concurrent store calls
     * produces one line, not one per call.
     */
    private void warnNoSessionFallback() {
        long now = System.currentTimeMillis();
        long last = lastNoSessionWarnMs.get();
        if (now - last < NO_SESSION_WARN_INTERVAL_MS) return;
        if (lastNoSessionWarnMs.compareAndSet(last, now)) {
            log.warn("[storage] {} routing store has no session identity (background job without a "
                    + "snapshot, CLI, or pre-SSO local workflow) — serving the shared default store "
                    + "and default AWS credentials. If this is a background job that should run as its "
                    + "submitter, it was not handed a captured SSO snapshot.", name);
        }
    }

    /**
     * Snapshots the current request user's stores for a background job, delegating to the swapper.
     * Returns {@code null} when the thread has no session or the user never connected.
     */
    public CapturedStores captureCurrent() {
        return swapper.captureCurrent();
    }

    /**
     * The session id this thread resolves to, re-installable on a pool worker, so a fan-out of store
     * calls routes every worker as the requesting user rather than falling through to
     * {@link #defaultStore} on threads that have no request context of their own.
     *
     * <p>Only the session id is carried, not credentials or a resolved store: on a worker the same
     * {@code registry -> swapper} lookup the request thread would have done then runs, so the worker
     * gets exactly the user's store — and, for a user who is logged in but has not connected a
     * bucket yet, exactly the same refusal ({@link NotAuthenticatedException}) the request thread
     * would have raised. The id is opaque and server-generated and never rendered; no credential
     * material crosses threads.
     *
     * <p>A thread with no session (a background job, a CLI) returns {@link Scope#NONE}, so those paths
     * keep the shared default target unchanged.
     */
    @Override
    public Scope captureScope() {
        // A job context installed on this thread (a queue worker, or the login warm-up sweep's
        // coordinator/workers) has no request context, so request discovery would answer null and
        // the fan-out would fall through to the shared default target — which on a pod with no
        // ambient credentials fails. Carrying the job snapshot's session id onto the pool workers
        // lets each resolve the capture's user store and credentials from the registry. See
        // SsoJobContext#sessionId().
        String jobSession = SsoJobContext.sessionId();
        String sessionId = jobSession != null ? jobSession : currentSession.currentSessionId();
        if (sessionId == null) return Scope.NONE;
        return () -> currentSession.propagate(sessionId);
    }

    // ── pure delegation — no other behaviour lives here ────────────────────────

    @Override
    public OutputStream write(ArtifactKey key) throws IOException {
        return delegate().write(key);
    }

    @Override
    public OutputStream write(ArtifactKey key, long contentLength) throws IOException {
        return delegate().write(key, contentLength);
    }

    @Override
    public InputStream read(ArtifactKey key) throws IOException {
        return delegate().read(key);
    }

    @Override
    public InputStream read(ArtifactKey key, long offset, long length) throws IOException {
        return delegate().read(key, offset, length);
    }

    @Override
    public boolean exists(ArtifactKey key) {
        return delegate().exists(key);
    }

    @Override
    public long size(ArtifactKey key) {
        return delegate().size(key);
    }

    @Override
    public long lastModified(ArtifactKey key) {
        return delegate().lastModified(key);
    }

    @Override
    public boolean delete(ArtifactKey key) {
        return delegate().delete(key);
    }

    @Override
    public int deletePrefix(ArtifactKey prefix) throws IOException {
        return delegate().deletePrefix(prefix);
    }

    @Override
    public Stream<ArtifactEntry> list(ArtifactKey prefix, boolean recursive) throws IOException {
        return delegate().list(prefix, recursive);
    }

    @Override
    public void copy(ArtifactKey from, ArtifactKey to) throws IOException {
        delegate().copy(from, to);
    }

    @Override
    public void move(ArtifactKey from, ArtifactKey to) throws IOException {
        delegate().move(from, to);
    }

    @Override
    public LocalHandle materialize(ArtifactKey key) throws IOException {
        return delegate().materialize(key);
    }

    @Override
    public Map<ArtifactKey, ArtifactEntry> statPrefix(ArtifactKey prefix) throws IOException {
        return delegate().statPrefix(prefix);
    }
}
