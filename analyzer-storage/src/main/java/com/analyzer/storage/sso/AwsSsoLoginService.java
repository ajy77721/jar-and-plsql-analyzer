package com.analyzer.storage.sso;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sso.SsoClient;
import software.amazon.awssdk.services.sso.model.AccountInfo;
import software.amazon.awssdk.services.sso.model.ListAccountRolesResponse;
import software.amazon.awssdk.services.sso.model.RoleInfo;
import software.amazon.awssdk.services.ssooidc.SsoOidcClient;
import software.amazon.awssdk.services.ssooidc.model.AuthorizationPendingException;
import software.amazon.awssdk.services.ssooidc.model.CreateTokenResponse;
import software.amazon.awssdk.services.ssooidc.model.RegisterClientResponse;
import software.amazon.awssdk.services.ssooidc.model.SlowDownException;
import software.amazon.awssdk.services.ssooidc.model.StartDeviceAuthorizationResponse;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Drives the AWS IAM Identity Center (successor to SSO) device-authorization flow and holds the
 * resulting tokens for exactly as long as they are valid.
 *
 * <h2>The flow, end to end</h2>
 *
 * <ol>
 *   <li>{@link #startLogin} calls {@code ssooidc:RegisterClient} to obtain a client id/secret, then
 *       {@code StartDeviceAuthorization} against the user-supplied Identity Center portal URL. The
 *       user is shown a verification URL and a short code; nothing else has happened yet.</li>
 *   <li>{@link #poll} is called repeatedly (the UI paces itself by {@link DeviceFlowStart#intervalSeconds()}).
 *       {@code ssooidc:CreateToken} with {@code grant_type=authorization_code} either returns
 *       {@code authorization_pending}/{@code slow_down} — surfaced as {@code "pending"} — or the
 *       tokens. On success the identity is then resolved through the {@code sso} service:
 *       {@code ListAccounts} picks the first account, {@code ListAccountRoles} prefers a
 *       PowerUser/Administrator role.</li>
 *   <li>{@link #accessToken()} transparently renews an expired access token with
 *       {@code grant_type=refresh_token}, so a long analysis run does not die mid-flight.</li>
 * </ol>
 *
 * <h2>Tokens are secret; this class treats them that way</h2>
 *
 * <p>Access tokens, refresh tokens and the OIDC client secret are never written to a log line,
 * never returned in an API response beyond the fields the flow itself defines, and never placed in
 * an exception message. The one secret that legitimately reaches a browser is the device
 * authorization user code — which is by design meant to be typed into the AWS verification page.
 *
 * <h2>One session, not a map of sessions</h2>
 *
 * <p>This deployment authenticates a single operator (the existing API-key gate is likewise a
 * single credential), so the login state is held directly rather than keyed per {@code HttpSession}.
 * The web layer still marks the {@code HttpSession} so a browser reload reflects connection state,
 * but the tokens live here, where the credential resolver can reach them without a request
 * context. A second concurrent login simply replaces the first, which is the correct behaviour for
 * a single-user tool.
 */
@Service
public class AwsSsoLoginService {

    private static final Logger log = LoggerFactory.getLogger(AwsSsoLoginService.class);

    /** Registration metadata; the values are fixed by what the identity device flow requires. */
    private static final String CLIENT_NAME = "amp-analyzer";
    private static final String GRANT_AUTHORIZATION_CODE = "authorization_code";
    private static final String GRANT_REFRESH_TOKEN = "refresh_token";
    private static final String SCOPE_SSO_ACCOUNT_ACCESS = "sso:account:access";

    /** Renew this long before real expiry, so an in-flight request never races the deadline. */
    private static final long REFRESH_SKEW_MS = TimeUnit.MINUTES.toMillis(5);

    /** Fallback device-flow poll interval when the service omits one. */
    private static final int DEFAULT_INTERVAL_SECONDS = 5;

    private final SsoOidcClient oidc;
    private final SsoClient sso;
    private final Region region;

    /** Guards every state transition; token refresh happens under it, so it is single-flight. */
    private final Object lock = new Object();

    private volatile ClientRegistration registration;
    private volatile PendingFlow pending;
    private volatile TokenState tokens;
    private volatile Identity identity;
    private volatile boolean loggedIn;

    /**
     * @param regionProperty the Identity Center / IAM region the OIDC and SSO endpoints live in.
     *                       It is distinct from the S3 bucket region a user may connect to, but in
     *                       the common case both are the same and {@code app.storage.s3.region} is
     *                       the natural default.
     */
    public AwsSsoLoginService(
            @Value("${app.storage.sso.region:${app.storage.s3.region:us-east-1}}") String regionProperty) {
        this.region = Region.of((regionProperty == null || regionProperty.isBlank())
                ? "us-east-1" : regionProperty.trim());
        // Built once: both clients are thread-safe and stateless, and rebuilding one per call would
        // create a fresh HTTP connection pool each time. Neither carries credentials of its own —
        // the OIDC calls are unsigned and the SSO calls are authorised by the bearer token we pass.
        this.oidc = SsoOidcClient.builder().region(region).build();
        this.sso = SsoClient.builder().region(region).build();
        log.info("[aws-sso] Identity Center device flow configured against region {}", region);
    }

    /**
     * Begins a login. Clears any previous session first, so a new portal URL cannot inherit stale
     * tokens from the old one.
     *
     * @param startUrl the Identity Center portal URL, e.g. {@code https://d-xxxx.awsapps.com/start}
     */
    public DeviceFlowStart startLogin(String startUrl) {
        if (startUrl == null || startUrl.isBlank()) {
            throw new IllegalArgumentException("An Identity Center start URL is required, e.g. https://d-xxxx.awsapps.com/start");
        }
        synchronized (lock) {
            clearState();

            RegisterClientResponse reg = oidc.registerClient(b -> b
                    .clientName(CLIENT_NAME)
                    .grantTypes(GRANT_AUTHORIZATION_CODE)
                    .scopes(SCOPE_SSO_ACCOUNT_ACCESS));
            registration = new ClientRegistration(reg.clientId(), reg.clientSecret());
            // The client id is not secret; the secret is, and is deliberately not logged.
            log.info("[aws-sso] registered OIDC client {} for start URL {}", reg.clientId(), startUrl);

            StartDeviceAuthorizationResponse dev = oidc.startDeviceAuthorization(b -> b
                    .clientId(reg.clientId())
                    .clientSecret(reg.clientSecret())
                    .startUrl(startUrl));

            int interval = dev.interval() != null ? dev.interval() : DEFAULT_INTERVAL_SECONDS;
            long expiresAt = System.currentTimeMillis()
                    + TimeUnit.SECONDS.toMillis(dev.expiresIn() != null ? dev.expiresIn() : 600);
            pending = new PendingFlow(dev.deviceCode(), interval, expiresAt);
            log.info("[aws-sso] device authorization started; user code expires in {}s",
                    dev.expiresIn() != null ? dev.expiresIn() : 600);
            return new DeviceFlowStart(dev.verificationUri(), dev.verificationUriComplete(),
                    dev.userCode(), interval, expiresAt);
        }
    }

    /**
     * One poll of the device-authorization endpoint.
     *
     * @return {@code "pending"} while the user has not yet approved, {@code "complete"} with the
     *         resolved account/role on success, {@code "expired"} once the device code lapses, or
     *         {@code "none"} when no login is in progress.
     */
    public PollResult poll() {
        synchronized (lock) {
            PendingFlow flow = pending;
            ClientRegistration reg = registration;
            if (flow == null || reg == null) {
                return new PollResult("none", null, null, null);
            }
            if (System.currentTimeMillis() > flow.expiresAtEpochMs()) {
                pending = null;
                log.info("[aws-sso] device authorization expired before approval");
                return new PollResult("expired", null, null, null);
            }
            try {
                CreateTokenResponse token = oidc.createToken(b -> b
                        .clientId(reg.clientId())
                        .clientSecret(reg.clientSecret())
                        .grantType(GRANT_AUTHORIZATION_CODE)
                        .deviceCode(flow.deviceCode()));
                storeTokens(token);
                pending = null;
                Identity id = resolveIdentity(token.accessToken());
                identity = id;
                loggedIn = true;
                log.info("[aws-sso] login complete — account {} role {}", id.accountId(), id.roleName());
                return new PollResult("complete", id.accountId(), id.accountName(), id.roleName());
            } catch (AuthorizationPendingException e) {
                return new PollResult("pending", null, null, null);
            } catch (SlowDownException e) {
                // The service is asking us to back off. Report pending; the caller already waits at
                // least the advertised interval before the next poll.
                return new PollResult("pending", null, null, null);
            } catch (software.amazon.awssdk.services.ssooidc.model.ExpiredTokenException e) {
                pending = null;
                log.info("[aws-sso] device code rejected as expired");
                return new PollResult("expired", null, null, null);
            }
        }
    }

    /**
     * A currently-valid access token, refreshing through the refresh-token grant if it is within
     * {@link #REFRESH_SKEW_MS} of expiry. Returns {@code null} when there is no session or the
     * refresh itself fails — the credential resolver interprets that as "fall back to the default
     * chain", not as an error.
     */
    public String accessToken() {
        synchronized (lock) {
            if (!loggedIn || tokens == null) return null;
            if (tokens.expiresAtEpochMs() - REFRESH_SKEW_MS > System.currentTimeMillis()) {
                return tokens.accessToken();
            }
            ClientRegistration reg = registration;
            String refresh = tokens.refreshToken();
            if (reg == null || refresh == null) {
                log.warn("[aws-sso] access token expired and no refresh token is available; clearing session");
                clearState();
                return null;
            }
            try {
                CreateTokenResponse renewed = oidc.createToken(b -> b
                        .clientId(reg.clientId())
                        .clientSecret(reg.clientSecret())
                        .grantType(GRANT_REFRESH_TOKEN)
                        .refreshToken(refresh));
                storeTokens(renewed);
                log.info("[aws-sso] access token refreshed");
                return tokens.accessToken();
            } catch (RuntimeException e) {
                // Message only — an SDK exception on this path would carry the request, and the
                // request carries the refresh token.
                log.warn("[aws-sso] token refresh failed ({}); clearing session and falling back to the default AWS chain",
                        e.getClass().getSimpleName());
                clearState();
                return null;
            }
        }
    }

    /** True once {@link #poll} has completed at least one login and no logout/refresh failure cleared it. */
    public boolean isLoggedIn() {
        return loggedIn;
    }

    /** The resolved account/role, or {@code null} when not logged in. */
    public Identity identity() {
        return identity;
    }

    /** The region the Identity Center endpoints are addressed in. */
    public Region region() {
        return region;
    }

    /** The shared {@code sso} client, for the credential resolver's GetRoleCredentials calls. */
    public SsoClient ssoClient() {
        return sso;
    }

    /** Discards every token and the resolved identity. Safe to call when not logged in. */
    public void logout() {
        synchronized (lock) {
            boolean wasLoggedIn = loggedIn;
            clearState();
            if (wasLoggedIn) log.info("[aws-sso] session cleared by logout");
        }
    }

    /** Resolves identity from a fresh access token: first account, then a PowerUser/Administrator role. */
    private Identity resolveIdentity(String accessToken) {
        List<AccountInfo> accounts = sso.listAccounts(b -> b.accessToken(accessToken)).accountList();
        if (accounts.isEmpty()) {
            throw new IllegalStateException(
                    "The SSO login succeeded but the identity exposes no AWS accounts; assign the user to an account in Identity Center.");
        }
        AccountInfo account = accounts.get(0);
        List<RoleInfo> roles = sso.listAccountRoles(b -> b
                .accessToken(accessToken)
                .accountId(account.accountId())).roleList();
        if (roles.isEmpty()) {
            throw new IllegalStateException(
                    "The SSO login succeeded but account " + account.accountId()
                            + " exposes no roles; assign a permission set in Identity Center.");
        }
        RoleInfo role = roles.stream()
                .filter(r -> r.roleName() != null
                        && (r.roleName().contains("PowerUser") || r.roleName().contains("Administrator")))
                .findFirst()
                .orElse(roles.get(0));
        return new Identity(account.accountId(), account.accountName(), role.roleName());
    }

    private void storeTokens(CreateTokenResponse token) {
        long expiresAt = System.currentTimeMillis()
                + TimeUnit.SECONDS.toMillis(token.expiresIn() != null ? token.expiresIn() : 0);
        String refresh = token.refreshToken() != null
                ? token.refreshToken()
                : (tokens != null ? tokens.refreshToken() : null);
        tokens = new TokenState(token.accessToken(), refresh, expiresAt);
    }

    private void clearState() {
        tokens = null;
        identity = null;
        pending = null;
        registration = null;
        loggedIn = false;
    }

    /**
     * What the front end needs to finish the device flow. Never carries the device code or any
     * token — those stay server-side in {@link PendingFlow}.
     *
     * @param verificationUri         where the user navigates, without the code embedded
     * @param verificationUriComplete the same URL with the user code pre-filled; show this if possible
     * @param userCode                the short code to type if the complete URL cannot be opened
     * @param intervalSeconds         how long to wait between {@link #poll()} calls
     * @param expiresAtEpochMs        when the code stops working, so the UI can time the attempt out
     */
    public record DeviceFlowStart(String verificationUri, String verificationUriComplete,
                                  String userCode, int intervalSeconds, long expiresAtEpochMs) {
    }

    /**
     * One poll outcome.
     *
     * @param status {@code pending} | {@code complete} | {@code expired} | {@code none}
     */
    public record PollResult(String status, String accountId, String accountName, String roleName) {
    }

    /** The identity the credential resolver mints role credentials for. */
    public record Identity(String accountId, String accountName, String roleName) {
    }

    /** OIDC client credentials from RegisterClient; the secret never leaves this class as a log. */
    private record ClientRegistration(String clientId, String clientSecret) {
    }

    /** An in-progress device authorization. */
    private record PendingFlow(String deviceCode, int intervalSeconds, long expiresAtEpochMs) {
    }

    /** Live tokens with their absolute expiry, so refresh is a clock comparison. */
    private record TokenState(String accessToken, String refreshToken, long expiresAtEpochMs) {
    }
}
