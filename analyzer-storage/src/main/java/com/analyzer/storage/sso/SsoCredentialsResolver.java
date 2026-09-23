package com.analyzer.storage.sso;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.sso.model.RoleCredentials;

import java.util.concurrent.TimeUnit;

/**
 * Supplies the S3 client with credentials: the logged-in user's SSO role credentials while a
 * session is active, and the ordinary AWS default chain the rest of the time.
 *
 * <h2>Why one provider, swapped answers</h2>
 *
 * <p>The {@code S3Client} is built once and holds a credentials provider for its lifetime — an S3
 * client cannot be told to change provider without rebuilding it, and rebuilding it on every login
 * would mean rebuilding every store that holds it. So the client holds <em>this</em> provider from
 * the start, and this provider decides per call. That is why {@code StorageConfig} can pass it in at
 * startup: before any login it simply reports the default chain, which is exactly today's
 * behaviour, and no existing deployment changes.
 *
 * <h2>Layering of the caches (both are needed)</h2>
 *
 * <ul>
 *   <li>An <b>access token</b> is renewed (refresh-token grant) by {@link AwsSsoLoginService} when it
 *       nears expiry — that call is already single-flight there.</li>
 *   <li><b>Role credentials</b> (the short-lived STS-style triple S3 actually signs with) are minted
 *       by {@code sso:GetRoleCredentials} and are cached here until their own expiry. They are a
 *       separate lifetime from the access token and are much more expensive to fetch, so caching
 *       them matters even when the token is fresh.</li>
 * </ul>
 *
 * <p>{@link #resolveCredentials()} is called on the S3 request path, possibly concurrently, so the
 * cache check-and-fill runs under a lock. Two threads that both find the cache cold will thus do
 * one fetch, not two — the single-flight requirement.
 *
 * <h2>Never a hard failure</h2>
 *
 * <p>If there is no session, or the SSO round trip fails for any reason, this resolver falls back
 * to {@link DefaultCredentialsProvider} rather than throwing. That keeps a logged-out or
 * misconfigured SSO state from taking down every S3 operation the application performs; the switch
 * between the two modes is logged at INFO (and the failure at WARN) so it is never silent. No token,
 * secret key or session token ever appears in a log line.
 */
@Component
public class SsoCredentialsResolver implements AwsCredentialsProvider {

    private static final Logger log = LoggerFactory.getLogger(SsoCredentialsResolver.class);

    /** Renew role credentials this long before their stated expiry, to beat clock skew. */
    private static final long REFRESH_SKEW_MS = TimeUnit.MINUTES.toMillis(5);

    /** Fallback lifetime when the service omits an expiry — AWS default role credential life. */
    private static final long DEFAULT_CREDENTIAL_LIFETIME_MS = TimeUnit.HOURS.toMillis(1);

    private final AwsSsoLoginService login;
    private final AwsCredentialsProvider fallback;

    private final Object lock = new Object();

    private volatile AwsSessionCredentials cached;
    private volatile long cachedExpiresAtEpochMs;

    /** Tracks the last mode reported, so the switch is logged once rather than per request. */
    private volatile boolean servingSso;

    public SsoCredentialsResolver(AwsSsoLoginService login) {
        this.login = login;
        this.fallback = DefaultCredentialsProvider.create();
    }

    @Override
    public AwsCredentials resolveCredentials() {
        if (!login.isLoggedIn()) {
            logSwitch(false);
            cached = null;
            return fallback.resolveCredentials();
        }
        AwsSessionCredentials credentials = currentSsoCredentials();
        if (credentials != null) {
            logSwitch(true);
            return credentials;
        }
        // A live session that could not produce role credentials: fall through rather than fail.
        logSwitch(false);
        return fallback.resolveCredentials();
    }

    /** Returns cached-or-freshly-minted role credentials, or {@code null} if none can be obtained. */
    private AwsSessionCredentials currentSsoCredentials() {
        long now = System.currentTimeMillis();
        AwsSessionCredentials current = cached;
        if (current != null && cachedExpiresAtEpochMs - REFRESH_SKEW_MS > now) {
            return current;
        }
        synchronized (lock) {
            // Re-check under the lock: another thread may have refreshed while we waited.
            now = System.currentTimeMillis();
            current = cached;
            if (current != null && cachedExpiresAtEpochMs - REFRESH_SKEW_MS > now) {
                return current;
            }
            String token = login.accessToken();
            AwsSsoLoginService.Identity identity = login.identity();
            if (token == null || identity == null) {
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
                cached = minted;
                cachedExpiresAtEpochMs = role.expiration() != null
                        ? role.expiration()
                        : now + DEFAULT_CREDENTIAL_LIFETIME_MS;
                return minted;
            } catch (RuntimeException e) {
                // Class name only: the SDK exception may carry the request, and the request carries
                // the bearer token.
                log.warn("[aws-sso] GetRoleCredentials failed ({}); using the default AWS chain",
                        e.getClass().getSimpleName());
                cached = null;
                return null;
            }
        }
    }

    /** Logs the user-SSO <-> default-chain switch exactly once per transition. No account/role names
     * beyond the ones already exposed by the status endpoints, and no credential material. */
    private void logSwitch(boolean sso) {
        if (sso == servingSso) return;
        servingSso = sso;
        if (sso) {
            AwsSsoLoginService.Identity id = login.identity();
            log.info("[aws-sso] S3 credentials now come from the user's SSO session"
                    + (id != null ? " (account " + id.accountId() + ", role " + id.roleName() + ")" : ""));
        } else {
            log.info("[aws-sso] S3 credentials reverted to the default AWS provider chain");
        }
    }
}
