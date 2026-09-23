package com.analyzer.storage.sso;

import com.analyzer.storage.ArtifactStore;

/**
 * A per-thread override of the SSO store and credential resolution, in force only for the duration
 * of one background job.
 *
 * <h2>Why this exists</h2>
 *
 * <p>{@link SsoCredentialsResolver} and {@link SessionRoutingArtifactStore} both discover the calling
 * user from the <em>current request thread</em> ({@link CurrentHttpSession}). An analysis job runs
 * later, on a queue worker with no request bound, so by the time it touches storage the request
 * context is gone and both would fall back to the shared default chain — which is exactly the
 * "a job must never fall back to shared/default credentials" rule this class exists to keep.
 *
 * <p>The submit-time snapshot ({@link CapturedStores} / {@link CapturedCredentials}) is carried on
 * the in-memory job. Installing it here for the job's duration lets the <em>same</em> routing store
 * and credential resolver every injected component already holds resolve the submitter's stores and
 * role, with no per-call plumbing. That matters because a job's writes do not all flow through the
 * executor itself: {@code PersistenceService}, {@code ClaudeAnalysisService} and friends each hold
 * their own reference to the shared {@code ArtifactStore} bean, so swapping the executor's own
 * field would leave every artifact those services write signed by the wrong principal.
 *
 * <h2>Scope and lifetime</h2>
 *
 * <p>The override is a {@link ThreadLocal}, set and cleared around the executor call in
 * {@code AnalysisQueueService}. A nested install restores the previous value on {@link #close()}, so
 * a follow-up job cannot leak its predecessor's identity. Only the job's own thread is covered: work
 * handed to a <em>pooled</em> helper thread (e.g. a Claude chunk executor) starts with no request
 * context and therefore still resolves the default path; such work is expected to run on the job
 * thread or be handed the snapshot explicitly.
 *
 * <p>Nothing here is ever logged: the snapshot holds a live credentials triple, and this class never
 * renders it.
 */
public final class SsoJobContext implements AutoCloseable {

    /** The override in force on the current thread, or {@code null} when there is none. */
    private static final ThreadLocal<SsoJobContext> CURRENT = new ThreadLocal<>();

    private final CapturedStores stores;
    private final CapturedCredentials credentials;
    private final SsoJobContext previous;

    private SsoJobContext(CapturedStores stores, CapturedCredentials credentials, SsoJobContext previous) {
        this.stores = stores;
        this.credentials = credentials;
        this.previous = previous;
    }

    /**
     * Installs {@code stores}/{@code credentials} as the current thread's SSO context and returns a
     * handle whose {@link #close()} restores whatever was in force before.
     *
     * <p>Returns {@code null} — and installs nothing — when both snapshots are {@code null}, which is
     * the no-SSO-session case ({@code app.auth.sso.required=false}, a CLI, a test). The legacy
     * shared-bean / default-credentials path is then left exactly as it was.
     */
    public static SsoJobContext install(CapturedStores stores, CapturedCredentials credentials) {
        if (stores == null && credentials == null) return null;
        SsoJobContext context = new SsoJobContext(stores, credentials, CURRENT.get());
        CURRENT.set(context);
        return context;
    }

    /** The store captured for a registered feature, or {@code null} when none is in force. */
    static ArtifactStore storeFor(String name) {
        SsoJobContext context = CURRENT.get();
        return context == null || context.stores == null ? null : context.stores.storeFor(name);
    }

    /** The credentials captured for the running job, or {@code null} when none are in force. */
    static CapturedCredentials credentials() {
        SsoJobContext context = CURRENT.get();
        return context == null ? null : context.credentials;
    }

    @Override
    public void close() {
        if (previous == null) CURRENT.remove();
        else CURRENT.set(previous);
    }
}
