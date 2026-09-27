package com.analyzer.storage.sso;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.LocalHandle;
import com.analyzer.storage.RequestScopedArtifactStore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
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
 *   <li><b>No request-bound session</b> — delegate to the startup store, which signs with the
 *       default AWS chain and the configured bucket/region. This is a background job thread, a CLI,
 *       or the pre-SSO local workflow, all of which must keep working exactly as before. A job that
 *       wants to run under its submitter instead should be handed a {@link CapturedStores} snapshot
 *       taken on the request thread.</li>
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

    /** Which registered half of a user's built stores this façade fronts. */
    private final String name;

    /** What a request with no session-bound state falls back to; see the class Javadoc. */
    private final ArtifactStore defaultStore;

    private final SsoStoreSwapper swapper;
    private final SsoSessionRegistry registry;
    private final CurrentHttpSession currentSession;

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
     * default store when there is no session at all, and a thrown {@link NotAuthenticatedException}
     * otherwise. See the class Javadoc for the exact rule.
     */
    private ArtifactStore delegate() {
        // A background job running with its submitter's snapshot installed on this thread takes
        // precedence over request discovery: the queue thread has no HttpSession, so without this
        // the job would write to the default bucket under the default chain. See SsoJobContext.
        ArtifactStore jobStore = SsoJobContext.storeFor(name);
        if (jobStore != null) return jobStore;
        String sessionId = currentSession.currentSessionId();
        if (sessionId == null) {
            return defaultStore;
        }
        SsoSessionState state = registry.get(sessionId);
        if (state == null || !state.loggedIn()) {
            throw new NotAuthenticatedException();
        }
        SsoStoreSwapper.BuiltStores stores = swapper.storesFor(sessionId);
        ArtifactStore perUser = stores == null ? null : stores.storeFor(name);
        if (perUser == null) {
            // Logged in but not connected (or this feature is not S3-backed for them): serve the
            // default/configured store rather than failing — the configured bucket IS the shared
            // target, and throwing made every listing/read 500 for a signed-in user who hadn't
            // connected a bucket yet. Writes the user makes after connecting still land in their
            // own per-user store, so per-user isolation is unaffected.
            return defaultStore;
        }
        return perUser;
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
     * gets exactly the user's store — and, for a user who is logged in but has not connected a bucket
     * yet, exactly the same {@code defaultStore} fallback with the same user-signed credentials. The
     * id is opaque and server-generated and never rendered; no credential material crosses threads.
     *
     * <p>A thread with no session (a background job, a CLI) returns {@link Scope#NONE}, so those paths
     * keep the shared default target unchanged.
     */
    @Override
    public Scope captureScope() {
        String sessionId = currentSession.currentSessionId();
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
