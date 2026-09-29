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
 * <h2>Falling back to the default chain only when it can actually answer</h2>
 *
 * <p>When the request has no SSO-bound session, or a session whose credentials cannot be resolved,
 * this used to fall back to {@link DefaultCredentialsProvider} unconditionally — which on an
 * S3-backed pod with no ambient credentials does not silently degrade, it fails <em>or</em> signs as
 * the wrong principal. The fallback is now only taken when the default chain can actually resolve
 * credentials (a CLI, a local run, an instance role); when it cannot, an S3 operation has no
 * principal to sign with, so this throws {@link NotAuthenticatedException} rather than pretending.
 * Nothing about the CLI/local path changes: ambient credentials that resolve are still used exactly
 * as before, and a session-less local run with no ambient credentials already failed at the default
 * chain — it now simply fails with a clear type instead of an SDK exception.
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

    @org.springframework.beans.factory.annotation.Autowired
    public SsoCredentialsResolver(AwsSsoLoginService login, SsoSessionRegistry registry,
                                  CurrentHttpSession currentSession) {
        this(login, registry, currentSession, DefaultCredentialsProvider.create());
    }

    /**
     * Test seam: same resolver with an explicit ambient provider, so the "the default chain cannot
     * resolve" branch can be exercised without depending on the host's real AWS environment.
     */
    SsoCredentialsResolver(AwsSsoLoginService login, SsoSessionRegistry registry,
                           CurrentHttpSession currentSession, AwsCredentialsProvider fallback) {
        this.login = login;
        this.registry = registry;
        this.currentSession = currentSession;
        this.fallback = fallback;
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
            // No session-bound identity: the CLI/local path. The default chain is used only if it
            // can actually answer; see ambientOrThrow.
            return ambientOrThrow();
        }
        AwsCredentials credentials = credentialsFor(sessionId);
        return credentials != null ? credentials : ambientOrThrow();
    }

    /**
     * The ambient (default-chain) credentials, when they resolve — otherwise a
     * {@link NotAuthenticatedException}. This is the only route to {@code fallback}, so the default
     * chain can never sign an operation it has no principal for. The SDK failure is deliberately not
     * wrapped: it can echo request material, and credential material must never reach a message.
     */
    private AwsCredentials ambientOrThrow() {
        try {
            return fallback.resolveCredentials();
        } catch (RuntimeException e) {
            throw new NotAuthenticatedException(
                    "No AWS SSO session and no ambient AWS credentials are available — "
                            + "authenticate via AWS SSO first");
        }
    }

    /** The current thread's session id, or {@code null}; exposed so callers do not re-derive it. */
    public String currentSessionId() {
        return currentSession.currentSessionId();
    }

    /**
     * A provider fixed to {@code sessionId}'s credentials, for building an S3 client that belongs to
     * one user rather than to whatever thread happens to use it.
     *
     * <p>When that user's credentials cannot be resolved — the session ended or the user logged out
     * mid-job — this refuses rather than degrading to the default chain: the client was built for a
     * specific user, so signing as the ambient principal would write that user's data under the
     * wrong identity. No ambient provider is consulted here, because there is always a session id to
     * hold to. {@code resolveFor(null)} (the CLI/local path) still returns the ambient provider
     * directly, so a store built without a session is unchanged.
     */
    public AwsCredentialsProvider resolveFor(String sessionId) {
        if (sessionId == null) return fallback;
        return () -> {
            AwsCredentials credentials = credentialsFor(sessionId);
            if (credentials != null) return credentials;
            throw new NotAuthenticatedException(
                    "The AWS SSO session backing this S3 store has ended — sign in again");
        };
    }

    /** A provider that always returns a captured snapshot, ignoring the request context entirely. */
    public AwsCredentialsProvider resolveFor(CapturedCredentials captured) {
        return captured == null ? fallback : captured.asProvider();
    }

    /**
     * Credentials for a user who has <b>just</b> been authenticated by the device flow but whose
     * state is not yet marked {@code loggedIn} — the window in which {@link AwsSsoLoginService}
     * resolves the display identity through Identity Store.
     *
     * <p>{@link #resolveCredentials()} cannot help here: it deliberately refuses to mint for a state
     * that is not logged in. This method is handed the freshly issued access token plus the
     * account/role the login settled on, mints the same short-lived role credentials, and caches
     * them under {@code sessionId} so the very next S3 call — after {@code loggedIn} is set — reuses
     * them instead of asking AWS again.
     *
     * <p>Returns {@code null} (never throws) when any input is missing or {@code GetRoleCredentials}
     * fails, so the caller falls back to the default chain exactly as before. No credential value is
     * ever logged.
     */
    public AwsCredentialsProvider resolveFor(String sessionId, String accessToken,
                                             String accountId, String roleName) {
        if (accessToken == null || accessToken.isBlank()
                || accountId == null || accountId.isBlank()
                || roleName == null || roleName.isBlank()) {
            return null;
        }
        try {
            RoleCredentials role = login.ssoClient().getRoleCredentials(b -> b
                    .accessToken(accessToken)
                    .accountId(accountId)
                    .roleName(roleName)).roleCredentials();
            if (role == null || role.accessKeyId() == null || role.secretAccessKey() == null) {
                log.warn("[aws-sso] GetRoleCredentials returned no usable credentials for the identity lookup; using the default chain");
                return null;
            }
            AwsSessionCredentials minted = AwsSessionCredentials.create(
                    role.accessKeyId(), role.secretAccessKey(), role.sessionToken());
            long expiresAt = role.expiration() != null
                    ? role.expiration()
                    : System.currentTimeMillis() + DEFAULT_CREDENTIAL_LIFETIME_MS;
            if (sessionId != null) {
                cache.put(sessionId, new Cached(minted, expiresAt));
            }
            return () -> minted;
        } catch (RuntimeException e) {
            // Class name only: the request carries the freshly issued access token.
            log.warn("[aws-sso] GetRoleCredentials failed ({}) during the identity lookup; using the default AWS chain",
                    e.getClass().getSimpleName());
            return null;
        }
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
        AwsCredentials credentials = credentialsFor(sessionId);
        return credentials == null ? null : new CapturedCredentials(sessionId, credentials);
    }

    /** Drops {@code sessionId}'s cached credentials; called on logout so a stale triple cannot linger. */
    public void forget(String sessionId) {
        if (sessionId != null) cache.remove(sessionId);
    }

    /**
     * Returns this user's credentials — their fixed IAM access keys when the session authenticated
     * with {@code /api/auth/aws/iam-connect}, otherwise their cached-or-freshly-minted SSO role
     * credentials — or {@code null} when the session has none.
     *
     * <p>A session is either IAM or SSO (see {@link SsoSessionState#connectIam}), so the IAM check
     * comes first and short-circuits: there is no token to refresh and nothing to cache for keys that
     * are constant for the life of the session.
     */
    private AwsCredentials credentialsFor(String sessionId) {
        SsoSessionState state = registry.get(sessionId);
        // No state, or a state between logins: there is nothing to sign with. Drop any cached
        // triple so a later login for the same session id cannot accidentally reuse the old one.
        if (state == null || !state.loggedIn()) {
            cache.remove(sessionId);
            return null;
        }
        // IAM access keys are fixed for the session — return them as-is; never logged or cached.
        AwsCredentials iam = state.iamCredentials();
        if (iam != null) {
            cache.remove(sessionId);
            return iam;
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
                // The SSO session has lapsed (the refresh-token grant failed / tokens gone) — this
                // used to return null, which silently downgraded every store call to the default
                // chain: on a pod with no ambient credentials that meant a multi-second credential
                // timeout PER READ (a 115-node detail view crawled at ~2.5s/read = ~2 minutes).
                // A logged-in user whose session died must be told to sign in again — end the
                // session and surface it as 401 (the UI's handler opens the login modal).
                log.warn("[aws-sso] session {} has no usable SSO token — ending it and requiring re-login",
                        sessionId.substring(0, Math.min(8, sessionId.length())) + "…");
                registry.end(sessionId);
                throw new NotAuthenticatedException();
            }
            try {
                RoleCredentials role = login.ssoClient().getRoleCredentials(b -> b
                        .accessToken(token)
                        .accountId(identity.accountId())
                        .roleName(identity.roleName())).roleCredentials();
                if (role == null || role.accessKeyId() == null || role.secretAccessKey() == null) {
                    log.warn("[aws-sso] GetRoleCredentials returned no usable credentials — ending the session and requiring re-login");
                    registry.end(sessionId);
                    throw new NotAuthenticatedException();
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
            } catch (NotAuthenticatedException e) {
                throw e;
            } catch (RuntimeException e) {
                // Class name only: the SDK exception may carry the request, and the request carries
                // the bearer token.
                log.warn("[aws-sso] GetRoleCredentials failed ({}); ending the session and requiring re-login",
                        e.getClass().getSimpleName());
                cache.remove(sessionId);
                registry.end(sessionId);
                throw new NotAuthenticatedException();
            }
        }
    }

    /** Cached role credentials plus their absolute expiry. Never logged. */
    private record Cached(AwsSessionCredentials credentials, long expiresAtEpochMs) {
    }
}
