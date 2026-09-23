package com.analyzer.storage.sso;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.sso.model.RoleCredentials;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Supplies the S3 client with credentials: the <em>current request's</em> SSO role credentials
 * while that user is logged in, and the ordinary AWS default chain the rest of the time.
 *
 * <h2>Per-user, discovered from the request</h2>
 *
 * <p>This used to answer from one shared login session. In the per-user model it asks
 * {@link CurrentHttpSession} which {@code HttpSession} the calling thread belongs to, looks that
 * user's state up in {@link SsoSessionRegistry}, and mints/caches role credentials for that user
 * alone. Credentials are cached in a map keyed by session id, so one user's refresh never touches
 * another's, and each key's check-and-fill is single-flight on that user's {@link SsoSessionState#lock()}.
 *
 * <h2>Two lifetimes, two layers of cache</h2>
 *
 * <ul>
 *   <li>An <b>access token</b> is renewed (refresh-token grant) by {@link AwsSsoLoginService} when it
 *       nears expiry — single-flight per user, on the same lock.</li>
 *   <li><b>Role credentials</b> (the short-lived STS-style triple S3 signs with) are minted by
 *       {@code sso:GetRoleCredentials} and cached here until their own expiry. They are far more
 *       expensive to fetch than they are to store.</li>
 * </ul>
 *
 * <h2>Never a hard failure on the request path</h2>
 *
 * <p>No session, not logged in, or the AWS round trip failing all fall back to
 * {@link DefaultCredentialsProvider} rather than throwing, so SSO state can never take down the
 * default workflow. The one place "no session" is treated as an error is the per-user store router
 * ({@link SessionRoutingArtifactStore}), which owns that policy explicitly.
 *
 * <p><b>Capture for background work.</b> {@link #captureCurrent()} snapshots the caller's
 * credentials while their request thread still exists, so a queued job can run under the submitter's
 * identity instead of silently reverting to the default chain.
 *
 * <p>No access token, refresh token, secret access key or session token ever appears in a log line
 * or an exception message.
 */
@Component
public class SsoCredentialsResolver implements AwsCredentialsProvider {

    private static final Logger log = LoggerFactory.getLogger(SsoCredentialsResolver.class);

    /** Renew role credentials this long before their stated expiry, to beat clock skew. */
    private static final long REFRESH_SKEW_MS = TimeUnit.MINUTES.toMillis(5);

    /** Fallback lifetime when the service omits an expiry — AWS default role credential life. */
    private static final long DEFAULT_CREDENTIAL_LIFETIME_MS = TimeUnit.HOURS.toMillis(1);

    private final AwsSsoLoginService login;
    private final SsoSessionRegistry registry;
    private final CurrentHttpSession currentSession;
    private final AwsCredentialsProvider fallback;

    /** Per-user role-credential cache; entries are dropped when their session ends or goes invalid. */
    private final ConcurrentHashMap<String, Cached> cache = new ConcurrentHashMap<>();

    public SsoCredentialsResolver(AwsSsoLoginService login, SsoSessionRegistry registry,
                                  CurrentHttpSession currentSession) {
        this.login = login;
        this.registry = registry;
        this.currentSession = currentSession;
        this.fallback = DefaultCredentialsProvider.create();
    }

    /**
     * The credentials for the current request's user, or the default chain when the thread has no
     * SSO-bound session or its credentials cannot be obtained.
     *
     * <p>Called on every S3 operation, possibly concurrently.
     */
    @Override
    public AwsCredentials resolveCredentials() {
        // A background job's captured role credentials win over request discovery: the queue thread
        // has no HttpSession, so without this the job would sign with the default chain. See
        // SsoJobContext and captureCurrent().
        CapturedCredentials captured = SsoJobContext.credentials();
        if (captured != null) return captured.credentials();
        String sessionId = currentSession.currentSessionId();
        if (sessionId == null) {
            return fallback.resolveCredentials();
        }
        AwsSessionCredentials credentials = credentialsFor(sessionId);
        return credentials != null ? credentials : fallback.resolveCredentials();
    }

    /** The current thread's session id, or {@code null}; exposed so callers do not re-derive it. */
    public String currentSessionId() {
        return currentSession.currentSessionId();
    }

    /**
     * A provider fixed to {@code sessionId}'s credentials, for building an S3 client that belongs to
     * one user rather than to whatever thread happens to use it. Falls back to the default chain if
     * that user's credentials cannot be resolved, so a client built this way is never left with no
     * answer at all.
     */
    public AwsCredentialsProvider resolveFor(String sessionId) {
        if (sessionId == null) return fallback;
        return () -> {
            AwsSessionCredentials credentials = credentialsFor(sessionId);
            return credentials != null ? credentials : fallback.resolveCredentials();
        };
    }

    /** A provider that always returns a captured snapshot, ignoring the request context entirely. */
    public AwsCredentialsProvider resolveFor(CapturedCredentials captured) {
        return captured == null ? fallback : captured.asProvider();
    }

    /**
     * Snapshots the current request's user and credentials for hand-off to a background job, or
     * {@code null} when the thread has no SSO-bound session or no credentials could be obtained.
     *
     * <p>Must be called on the request thread — that is the whole point: the job it is passed to has
     * no request context of its own.
     */
    public CapturedCredentials captureCurrent() {
        String sessionId = currentSession.currentSessionId();
        if (sessionId == null) return null;
        AwsSessionCredentials credentials = credentialsFor(sessionId);
        return credentials == null ? null : new CapturedCredentials(sessionId, credentials);
    }

    /** Drops {@code sessionId}'s cached credentials; called on logout so a stale triple cannot linger. */
    public void forget(String sessionId) {
        if (sessionId != null) cache.remove(sessionId);
    }

    /** Returns this user's cached-or-freshly-minted role credentials, or {@code null} if none. */
    private AwsSessionCredentials credentialsFor(String sessionId) {
        SsoSessionState state = registry.get(sessionId);
        // No state, or a state between logins: there is nothing to sign with. Drop any cached
        // triple so a later login for the same session id cannot accidentally reuse the old one.
        if (state == null || !state.loggedIn()) {
            cache.remove(sessionId);
            return null;
        }
        long now = System.currentTimeMillis();
        Cached current = cache.get(sessionId);
        if (current != null && current.expiresAtEpochMs - REFRESH_SKEW_MS > now) {
            return current.credentials;
        }
        synchronized (state.lock()) {
            // Re-check under the per-user lock: another thread may have refreshed while we waited.
            now = System.currentTimeMillis();
            current = cache.get(sessionId);
            if (current != null && current.expiresAtEpochMs - REFRESH_SKEW_MS > now) {
                return current.credentials;
            }
            String token = login.accessToken(state);
            SsoIdentity identity = state.identity();
            if (token == null || identity == null
                    || identity.accountId() == null || identity.roleName() == null) {
                return null;
            }
            try {
                RoleCredentials role = login.ssoClient().getRoleCredentials(b -> b
                        .accessToken(token)
                        .accountId(identity.accountId())
                        .roleName(identity.roleName())).roleCredentials();
                if (role == null || role.accessKeyId() == null || role.secretAccessKey() == null) {
                    log.warn("[aws-sso] GetRoleCredentials returned no usable credentials; using the default chain");
                    return null;
                }
                AwsSessionCredentials minted = AwsSessionCredentials.create(
                        role.accessKeyId(), role.secretAccessKey(), role.sessionToken());
                long expiresAt = role.expiration() != null
                        ? role.expiration()
                        : now + DEFAULT_CREDENTIAL_LIFETIME_MS;
                cache.put(sessionId, new Cached(minted, expiresAt));
                log.info("[aws-sso] minted S3 role credentials for account {} role {}",
                        identity.accountId(), identity.roleName());
                return minted;
            } catch (RuntimeException e) {
                // Class name only: the SDK exception may carry the request, and the request carries
                // the bearer token.
                log.warn("[aws-sso] GetRoleCredentials failed ({}); using the default AWS chain",
                        e.getClass().getSimpleName());
                cache.remove(sessionId);
                return null;
            }
        }
    }

    /** Cached role credentials plus their absolute expiry. Never logged. */
    private record Cached(AwsSessionCredentials credentials, long expiresAtEpochMs) {
    }
}
