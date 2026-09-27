package com.analyzer.storage;

/**
 * An {@link ArtifactStore} whose behaviour depends on the request <em>thread</em> it is called on.
 *
 * <p>The per-user routing store ({@code SessionRoutingArtifactStore}) resolves the calling user from
 * thread-bound request context: on a plain request thread it serves that user's store, and on a
 * thread with no request bound (a background job, a CLI) it serves the shared default target. Both
 * are correct — but only when the thread calling it actually is the thread that owns the answer.
 *
 * <h2>The hole this closes</h2>
 *
 * <p>A listing fans its per-directory and per-analysis reads out onto a fixed pool
 * ({@link ShallowTree}). Those pool threads are created once and have <b>no request context</b> —
 * Spring's {@code RequestContextHolder} is a plain {@link ThreadLocal}, set on the servlet thread
 * and never inherited by a pre-existing pool worker. So a logged-in, connected user's listing
 * silently resolved to the default store on every worker: the store call left with the default AWS
 * chain, and on a pod with no ambient credentials it failed (an IMDS-timeout
 * {@code SdkClientException}) — which the caller then surfaced as an <em>empty</em> list. A fan-out
 * must therefore carry the requesting thread's scope across to its workers.
 *
 * <h2>Contract</h2>
 *
 * <p>{@link #captureScope()} is called <b>on the request thread</b>, before any task is submitted.
 * The returned {@link Scope} is then installed inside each worker task; {@link Scope#install()}
 * binds the captured scope to whatever thread it is called on and returns a handle whose
 * {@code close()} unbinds it there. A single captured {@code Scope} may be installed concurrently
 * on many worker threads, because each {@code install()} acts only on its own calling thread.
 *
 * <p>A store that has nothing to carry (a thread with no request-bound session at all) returns
 * {@link Scope#NONE}; installing that does nothing, which preserves the existing fallback — an
 * unauthenticated or background thread still routes to the default store exactly as before.
 */
public interface RequestScopedArtifactStore extends ArtifactStore {

    /**
     * A snapshot of the calling thread's routing, re-installable on a worker thread.
     *
     * <p>Deliberately has no {@code close()} of its own: the binding is per-thread, so only the
     * handle returned by {@link #install()} can unwrap it, and only on the thread that installed it.
     */
    interface Scope {

        /**
         * Binds this scope to the <em>current</em> thread. Close the returned handle on the same
         * thread to unbind; a bounded install (see {@code SsoJobContext}) restores whatever was bound
         * before on that thread.
         */
        AutoCloseable install();

        /** A scope that carries nothing and whose {@code install()} is a no-op. */
        Scope NONE = () -> () -> {
            // nothing to bind or unbind
        };
    }

    /**
     * Snapshot the routing in force on this (the request) thread. Must be called before fanning out;
     * a snapshot taken on a worker thread would capture that worker's — empty — context.
     */
    Scope captureScope();
}
