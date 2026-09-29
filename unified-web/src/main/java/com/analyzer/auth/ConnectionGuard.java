package com.analyzer.auth;

import com.analyzer.admin.AdminAuditService;
import com.analyzer.storage.StorageMode;
import com.analyzer.storage.sso.CurrentHttpSession;
import com.analyzer.storage.sso.SsoIdentity;
import com.analyzer.storage.sso.SsoSessionRegistry;
import com.analyzer.storage.sso.SsoStoreSwapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

/**
 * The one place the web layer asks three related questions about the calling request, so neither
 * auth rule is re-implemented (and re-forgotten) at each of the six submit points:
 *
 * <ol>
 *   <li><b>Is the caller connected?</b> A request that is logged in via AWS SSO but has not yet
 *       connected a bucket must fail <em>before</em> a background job starts. Otherwise the job is
 *       queued, the UI shows a spinner, and the failure only surfaces later — or worse, the job
 *       silently writes to the shared default target. {@link #requireConnected(String)} turns that
 *       into an immediate {@code 409} the UI can act on.</li>
 *   <li><b>Who is the caller, for cache scoping?</b> The per-user routed store keeps each user's
 *       bytes apart, but the in-process caches that hold <em>parsed</em> documents are keyed by
 *       analysis name alone, so the same name in two users' buckets collides.
 *       {@link #routingIdentityKey()} adds the routing identity to those keys (see
 *       {@code ParserAnalysisReader}/{@code StaticAnalysisReader}).</li>
 *   <li><b>Does the caller own this job?</b> Sequential job/run ids are guessable, so a poll of
 *       another user's id must not return their report. {@link #owns(String)} is the check.</li>
 * </ol>
 *
 * <h2>The rule for "connected"</h2>
 *
 * <p>Mirrors {@code LogArchiveService.connectedSessionId()}: a session is usable only once it both
 * logged in and had a bucket verified. Here that is asked of the store swapper —
 * {@link SsoStoreSwapper#storesFor(String)} is non-null exactly when a connect probe succeeded — so
 * the check is "the stores this session will actually write through exist". A request with no
 * session at all (a queue worker, the CLI, the pre-SSO local workflow) is left alone: it routes to
 * the default store and the default credentials, which is behaviour that must keep working.
 *
 * <p>The guard only fires for an S3/HYBRID feature: a local or pure-MongoDB deployment has no
 * bucket to connect, so the rule is a no-op there.
 */
@Component
public class ConnectionGuard {

    /** Feature name for the JAR analyzer's store; matches {@link SsoStoreSwapper#JAR}. */
    public static final String JAR = "jar";

    /** Feature name for the PL/SQL parser's store; matches {@link SsoStoreSwapper#PARSER}. */
    public static final String PARSER = "parser";

    /** The user-facing message on a {@code 409} when a logged-in user has not connected a bucket. */
    public static final String CONNECT_MESSAGE = "Connect your AWS account first";

    /** Cache-scoping identity used when no routing identity can be resolved. */
    public static final String SHARED = "shared";

    private final SsoSessionRegistry registry;
    private final SsoStoreSwapper swapper;
    private final CurrentHttpSession currentSession;

    private final boolean jarS3Backed;
    private final boolean parserS3Backed;

    public ConnectionGuard(SsoSessionRegistry registry,
                           SsoStoreSwapper swapper,
                           CurrentHttpSession currentSession,
                           @Value("${app.storage.jar.mode:${app.storage.mode:local}}") String jarMode,
                           @Value("${app.storage.parser.mode:${app.storage.mode:local}}") String parserMode) {
        this.registry = registry;
        this.swapper = swapper;
        this.currentSession = currentSession;
        this.jarS3Backed = isS3Mode(jarMode);
        this.parserS3Backed = isS3Mode(parserMode);
    }

    // ── Connection requirement ────────────────────────────────────────────────

    /** Rejects a JAR-analyzer submit from a logged-in-but-unconnected session when JAR storage is S3. */
    public void requireJarConnected() {
        requireConnected(JAR);
    }

    /** Rejects a parser submit from a logged-in-but-unconnected session when parser storage is S3. */
    public void requireParserConnected() {
        requireConnected(PARSER);
    }

    /**
     * Throws {@code 409 Conflict} when the calling request belongs to a session that has completed
     * an AWS login but has not connected a bucket, and {@code feature}'s storage is S3/HYBRID.
     *
     * <p>Returns quietly in every other case — no session, not logged in, already connected, or a
     * non-S3 feature — so the legacy default-path behaviour is untouched.
     */
    public void requireConnected(String feature) {
        if (!isS3Backed(feature)) return;
        String sessionId = currentSession.currentSessionId();
        if (sessionId == null) return;                                 // no request-bound session
        if (swapper.storesFor(sessionId) != null) return;              // connected: verified stores exist
        if (!registry.isAuthenticated(sessionId)) return;              // not logged in: interceptor's job
        throw new ResponseStatusException(HttpStatus.CONFLICT, CONNECT_MESSAGE);
    }

    // ── Identity ──────────────────────────────────────────────────────────────

    /**
     * The calling session's routing identity (email, else name, else account id), or {@code null}
     * when this thread serves no authenticated session. Never throws.
     *
     * <p>This is the same "who is this" chain as {@code UserConnectionStore.currentIdentityKey()} and
     * {@link AdminAuditService#identityKey}, so a job's recorded owner and a cache key agree on the
     * spelling of one user.
     */
    public String currentIdentityKey() {
        try {
            String sessionId = currentSession.currentSessionId();
            if (sessionId == null) return null;
            SsoIdentity identity = registry.identity(sessionId);
            return AdminAuditService.identityKey(identity);
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * {@link #currentIdentityKey()} with a {@link #SHARED} fallback, for cache keys that must always
     * have a non-null component. Two requests that both resolve to no identity share one cache
     * partition, exactly as they shared one before this existed.
     */
    public String routingIdentityKey() {
        String key = currentIdentityKey();
        return key != null ? key : SHARED;
    }

    /**
     * True when the calling request may read a resource recorded with {@code owner}. An unowned
     * resource ({@code owner} null/blank) — a job persisted before ownership was recorded, or one
     * created with no SSO session — stays readable, so this cannot lock a local deployment out of
     * its own history.
     */
    public boolean owns(String owner) {
        if (owner == null || owner.isBlank()) return true;
        String current = currentIdentityKey();
        return current != null && current.equals(owner);
    }

    // ── Internals ─────────────────────────────────────────────────────────────

    private boolean isS3Backed(String feature) {
        return PARSER.equals(feature) ? parserS3Backed : jarS3Backed;
    }

    private static boolean isS3Mode(String modeProp) {
        StorageMode mode = StorageMode.parse(modeProp);
        return mode == StorageMode.S3 || mode == StorageMode.HYBRID;
    }
}
