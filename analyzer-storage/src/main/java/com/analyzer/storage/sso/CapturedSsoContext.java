package com.analyzer.storage.sso;

/**
 * A re-installable snapshot of the SSO context in force on one thread, carried to a pooled worker
 * so the work that runs there signs as the same principal.
 *
 * <h2>The gap this closes</h2>
 *
 * <p>{@link SsoJobContext} already lets a job's submitter be carried onto a worker thread, and
 * {@link SessionRoutingArtifactStore#captureScope()} does the same for a listing fan-out. Both are
 * forms of the same rule: <b>a credential context must follow the work, not the thread that
 * happened to start it</b>. Work that is buffered on one thread and flushed on a pool thread —
 * {@code WriteBehindStore}'s S3 uploads — was the one place the rule was not applied: the write
 * ran on the request/job thread where the identity was resolvable, but the upload ran on a
 * {@code s3-write-behind} pool thread with no request and no installed job context, so the S3
 * client fell through to the default AWS chain. On a pod with no ambient credentials that fails
 * <b>every</b> upload, and the buffered artifacts are lost.
 *
 * <h2>Two sources, one snapshot</h2>
 *
 * <p>{@link #capture(SsoCredentialsResolver)} must be called on the thread that owns the answer —
 * the write's own thread. It prefers, in order:
 *
 * <ol>
 *   <li>the {@link SsoJobContext} installed on this thread (a queue worker or the warm sweep),
 *       whose captured stores <em>and</em> credentials are the job's identity; then</li>
 *   <li>the request-bound session's credentials ({@link SsoCredentialsResolver#captureCurrent()}),
 *       for a plain request thread that has no job context installed.</li>
 * </ol>
 *
 * <p>Returns {@code null} when neither applies, which is the CLI / no-SSO-session case: installing
 * nothing preserves the legacy default-chain behaviour exactly.
 *
 * <h2>Lifetime and secrecy</h2>
 *
 * <p>The snapshot holds a live credentials triple and never renders it — no field here reaches a
 * log line, an exception message, or a response. {@link #install()} binds it to whatever thread it
 * is called on (the same bounded install as {@link SsoJobContext#install}) and the returned handle
 * restores the previous binding on close, so one flushed entry cannot leak its identity into the
 * next on a shared pool thread.
 */
public final class CapturedSsoContext {

    private final CapturedStores stores;
    private final CapturedCredentials credentials;

    private CapturedSsoContext(CapturedStores stores, CapturedCredentials credentials) {
        this.stores = stores;
        this.credentials = credentials;
    }

    /**
     * Snapshots the SSO context in force on the calling thread, or {@code null} when there is none.
     *
     * <p>Must be called on the thread whose identity the work belongs to (the write/request/job
     * thread); a snapshot taken later on a pool worker would capture that worker's empty context.
     *
     * @param resolver the resolver to fall back to for a plain request thread's session credentials,
     *                 or {@code null} when the caller has none (a per-user store already carries its
     *                 own fixed credentials)
     */
    public static CapturedSsoContext capture(SsoCredentialsResolver resolver) {
        SsoJobContext job = SsoJobContext.current();
        if (job != null) {
            CapturedStores jobStores = job.capturedStores();
            CapturedCredentials jobCredentials = job.capturedCredentials();
            if (jobStores != null || jobCredentials != null) {
                return new CapturedSsoContext(jobStores, jobCredentials);
            }
        }
        if (resolver != null) {
            CapturedCredentials captured = resolver.captureCurrent();
            if (captured != null) return new CapturedSsoContext(null, captured);
        }
        return null;
    }

    /**
     * Binds this snapshot to the <em>current</em> thread and returns a handle whose {@code close()}
     * restores whatever was bound before. A single snapshot may be installed concurrently on many
     * pool threads, because each install acts only on its own calling thread. Never returns a handle
     * that leaks the snapshot beyond its close.
     */
    public AutoCloseable install() {
        return SsoJobContext.install(stores, credentials);
    }
}
