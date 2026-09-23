package com.analyzer.storage.sso;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sso.SsoClient;
import software.amazon.awssdk.services.sso.model.AccountInfo;
import software.amazon.awssdk.services.sso.model.RoleInfo;
import software.amazon.awssdk.services.ssooidc.SsoOidcClient;
import software.amazon.awssdk.services.ssooidc.model.AuthorizationPendingException;
import software.amazon.awssdk.services.ssooidc.model.CreateTokenResponse;
import software.amazon.awssdk.services.ssooidc.model.RegisterClientResponse;
import software.amazon.awssdk.services.ssooidc.model.SlowDownException;
import software.amazon.awssdk.services.ssooidc.model.StartDeviceAuthorizationResponse;

import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Drives the AWS IAM Identity Center (successor to SSO) device-authorization flow for one user at a
 * time, operating on the {@link SsoSessionState} it is handed.
 *
 * <h2>The flow, end to end</h2>
 *
 * <ol>
 *   <li>{@link #startLogin(SsoSessionState, String)} calls {@code ssooidc:RegisterClient} to obtain
 *       a client id/secret, then {@code StartDeviceAuthorization} against the user-supplied Identity
 *       Center portal URL. The user is shown a verification URL and a short code.</li>
 *   <li>{@link #poll(SsoSessionState)} is called repeatedly (the UI paces itself by
 *       {@link DeviceFlowStart#intervalSeconds()}). {@code ssooidc:CreateToken} with the device-code
 *       grant either returns {@code authorization_pending}/{@code slow_down} — surfaced as
 *       {@code "pending"} — or the tokens. On success the identity is resolved through the {@code sso}
 *       service ({@code ListAccounts}, then {@code ListAccountRoles} preferring a
 *       PowerUser/Administrator role) and the display name/email are decoded from the {@code id_token}.</li>
 *   <li>{@link #accessToken(SsoSessionState)} transparently renews an expired access token with the
 *       refresh-token grant, so a long analysis run does not die mid-flight.</li>
 * </ol>
 *
 * <h2>Stateless with respect to users</h2>
 *
 * <p>This bean holds only the AWS clients and the region — both thread-safe and shared. Every
 * per-user value (tokens, device code, identity, verified target) lives in the caller's
 * {@link SsoSessionState}, and each method synchronises on that state's own lock. Two users logging
 * in concurrently therefore never contend on each other's flow, and a second login can no longer
 * overwrite the first's tokens.
 *
 * <h2>Tokens are secret; this class treats them that way</h2>
 *
 * <p>Access tokens, refresh tokens and the OIDC client secret are never written to a log line, never
 * returned in an API response beyond the fields the flow itself defines, and never placed in an
 * exception message. The one secret that legitimately reaches a browser is the device authorization
 * user code — which is by design meant to be typed into the AWS verification page.
 */
@Service
public class AwsSsoLoginService {

    private static final Logger log = LoggerFactory.getLogger(AwsSsoLoginService.class);

    /** Registration metadata; the values are fixed by what the identity device flow requires. */
    private static final String CLIENT_NAME = "amp-analyzer";

    /**
     * The only client type Identity Center's OIDC endpoint accepts. It is NOT optional in practice:
     * when {@code clientType} is omitted from {@code RegisterClient}, the service rejects the call
     * with HTTP 400 {@code InvalidClientMetadataException} — the exact symptom this app hit, while
     * the AWS CLI and standalone probes passed because they both send {@code clientType=public}.
     * The SDK does not default it, so it must be set explicitly here.
     */
    private static final String CLIENT_TYPE_PUBLIC = "public";

    /**
     * The URN AWS requires as {@code grant_type} whenever a device code is exchanged for tokens.
     *
     * <p>This is deliberately NOT the bare {@code "authorization_code"} string: the Identity Center
     * OIDC endpoint rejects {@code RegisterClient} outright (HTTP 400,
     * {@code InvalidClientMetadataException}/{@code InvalidRequestException}) when a client is
     * registered for {@code authorization_code}, and it rejects {@code CreateToken} when a device
     * code is presented under that grant. The device-code grant is its own registered grant type
     * under RFC 8628.
     */
    private static final String GRANT_DEVICE_CODE = "urn:ietf:params:oauth:grant-type:device_code";

    /** The grant used to renew an access token from the refresh token, no device code involved. */
    private static final String GRANT_REFRESH_TOKEN = "refresh_token";

    private static final String SCOPE_SSO_ACCOUNT_ACCESS = "sso:account:access";

    /**
     * The standard OIDC scope. Requesting it makes {@code CreateToken} return an {@code id_token},
     * whose claims are the only source of a human-readable name/email — Identity Center's own
     * {@code ListAccounts} response carries neither.
     */
    private static final String SCOPE_OPENID = "openid";

    /** Renew this long before real expiry, so an in-flight request never races the deadline. */
    private static final long REFRESH_SKEW_MS = TimeUnit.MINUTES.toMillis(5);

    /** Fallback device-flow poll interval when the service omits one. */
    private static final int DEFAULT_INTERVAL_SECONDS = 5;

    /** Shared, thread-safe JSON reader for the id_token payload. */
    private static final ObjectMapper JSON = new ObjectMapper();

    private final SsoOidcClient oidc;
    private final SsoClient sso;
    private final Region region;
    private final ApplicationEventPublisher events;

    /**
     * @param regionProperty the Identity Center / IAM region the OIDC and SSO endpoints live in.
     *                       It is distinct from the S3 bucket region a user may connect to, but in
     *                       the common case both are the same and {@code app.storage.s3.region} is
     *                       the natural default.
     */
    public AwsSsoLoginService(
            @Value("${app.storage.sso.region:${app.storage.s3.region:us-east-1}}") String regionProperty,
            ApplicationEventPublisher events) {
        this.events = events;
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
     * Begins a login for {@code state}, clearing that user's previous state first so a new portal
     * URL cannot inherit stale tokens from the old one.
     *
     * @param state    the caller's per-session state; never null
     * @param startUrl the Identity Center portal URL, e.g. {@code https://d-xxxx.awsapps.com/start}
     */
    public DeviceFlowStart startLogin(SsoSessionState state, String startUrl) {
        if (startUrl == null || startUrl.isBlank()) {
            throw new IllegalArgumentException("An Identity Center start URL is required, e.g. https://d-xxxx.awsapps.com/start");
        }
        synchronized (state.lock()) {
            state.clear();

            // Both grant types are registered up front: the device-code grant is what /poll uses,
            // and refresh_token is what long-run renewal uses in accessToken(). AWS accepts exactly
            // this pair and rejects an authorization_code registration. `openid` is requested so the
            // CreateToken response carries an id_token with name/email claims.
            // TEMP-DIAGNOSTIC: the request built here carries no secret (clientName/type/grants/
            // scopes are all public metadata), so logging it is safe; it exists only until the
            // registerClient rejection is root-caused.
            log.warn("[aws-sso][diag] registerClient request: clientName={} clientType={} grantTypes={} scopes={} region={} oidc={} endpointOverride={}",
                    CLIENT_NAME, CLIENT_TYPE_PUBLIC, GRANT_DEVICE_CODE + "," + GRANT_REFRESH_TOKEN,
                    SCOPE_SSO_ACCOUNT_ACCESS + "," + SCOPE_OPENID, region, oidc.getClass().getName(),
                    oidc.serviceClientConfiguration().endpointOverride());
            RegisterClientResponse reg;
            try {
                reg = oidc.registerClient(b -> b
                        .clientName(CLIENT_NAME)
                        // Required by the endpoint; omitting it is the InvalidClientMetadataException.
                        .clientType(CLIENT_TYPE_PUBLIC)
                        .grantTypes(GRANT_DEVICE_CODE, GRANT_REFRESH_TOKEN)
                        .scopes(SCOPE_SSO_ACCOUNT_ACCESS, SCOPE_OPENID));
            } catch (RuntimeException diag) {
                log.warn("[aws-sso][diag] registerClient threw {} cause={} message={}",
                        diag.getClass().getName(), diag.getCause(), diag.getMessage());
                throw diag;
            }
            state.registration(new SsoSessionState.ClientRegistration(reg.clientId(), reg.clientSecret()));
            // The client id is not secret; the secret is, and is deliberately not logged.
            log.info("[aws-sso] registered OIDC client {} for start URL {}", reg.clientId(), startUrl);

            StartDeviceAuthorizationResponse dev = oidc.startDeviceAuthorization(b -> b
                    .clientId(reg.clientId())
                    .clientSecret(reg.clientSecret())
                    .startUrl(startUrl));

            int interval = dev.interval() != null ? dev.interval() : DEFAULT_INTERVAL_SECONDS;
            long expiresAt = System.currentTimeMillis()
                    + TimeUnit.SECONDS.toMillis(dev.expiresIn() != null ? dev.expiresIn() : 600);
            state.pendingFlow(new SsoSessionState.PendingFlow(dev.deviceCode(), interval, expiresAt));
            log.info("[aws-sso] device authorization started; user code expires in {}s",
                    dev.expiresIn() != null ? dev.expiresIn() : 600);
            return new DeviceFlowStart(dev.verificationUri(), dev.verificationUriComplete(),
                    dev.userCode(), interval, expiresAt);
        }
    }

    /**
     * One poll of the device-authorization endpoint for {@code state}.
     *
     * @return {@code "pending"} while the user has not yet approved, {@code "complete"} with the
     *         resolved account/role on success, {@code "expired"} once the device code lapses, or
     *         {@code "none"} when no login is in progress for that user.
     */
    public PollResult poll(SsoSessionState state) {
        if (state == null) return new PollResult("none", null, null, null);
        synchronized (state.lock()) {
            SsoSessionState.PendingFlow flow = state.pendingFlow();
            SsoSessionState.ClientRegistration reg = state.registration();
            if (flow == null || reg == null) {
                return new PollResult("none", null, null, null);
            }
            if (System.currentTimeMillis() > flow.expiresAtEpochMs()) {
                state.pendingFlow(null);
                log.info("[aws-sso] device authorization expired before approval");
                return new PollResult("expired", null, null, null);
            }
            try {
                CreateTokenResponse token = oidc.createToken(b -> b
                        .clientId(reg.clientId())
                        .clientSecret(reg.clientSecret())
                        // A device code is only legal under the RFC 8628 device-code grant; AWS
                        // rejects it under any other grant_type.
                        .grantType(GRANT_DEVICE_CODE)
                        .deviceCode(flow.deviceCode()));
                storeTokens(state, token);
                state.pendingFlow(null);
                SsoIdentity id = resolveIdentity(token.accessToken(), token.idToken());
                state.identity(id);
                state.loggedIn(true);
                log.info("[aws-sso] login complete — account {} role {}", id.accountId(), id.roleName());
                publishLogin(state, id);
                return new PollResult("complete", id.accountId(), id.accountName(), id.roleName());
            } catch (AuthorizationPendingException e) {
                return new PollResult("pending", null, null, null);
            } catch (SlowDownException e) {
                // The service is asking us to back off. Report pending; the caller already waits at
                // least the advertised interval before the next poll.
                return new PollResult("pending", null, null, null);
            } catch (software.amazon.awssdk.services.ssooidc.model.ExpiredTokenException e) {
                state.pendingFlow(null);
                log.info("[aws-sso] device code rejected as expired");
                return new PollResult("expired", null, null, null);
            }
        }
    }

    /**
     * A currently-valid access token for {@code state}, refreshing through the refresh-token grant
     * if it is within {@link #REFRESH_SKEW_MS} of expiry. Returns {@code null} when there is no
     * session or the refresh itself fails — the credential resolver interprets that as "fall back to
     * the default chain", not as an error.
     */
    public String accessToken(SsoSessionState state) {
        if (state == null) return null;
        synchronized (state.lock()) {
            SsoSessionState.TokenState tokens = state.tokens();
            if (!state.loggedIn() || tokens == null) return null;
            if (tokens.expiresAtEpochMs() - REFRESH_SKEW_MS > System.currentTimeMillis()) {
                return tokens.accessToken();
            }
            SsoSessionState.ClientRegistration reg = state.registration();
            String refresh = tokens.refreshToken();
            if (reg == null || refresh == null) {
                log.warn("[aws-sso] access token expired and no refresh token is available; clearing session");
                state.clear();
                return null;
            }
            try {
                CreateTokenResponse renewed = oidc.createToken(b -> b
                        .clientId(reg.clientId())
                        .clientSecret(reg.clientSecret())
                        .grantType(GRANT_REFRESH_TOKEN)
                        .refreshToken(refresh));
                storeTokens(state, renewed);
                log.info("[aws-sso] access token refreshed");
                return state.tokens().accessToken();
            } catch (RuntimeException e) {
                // Message only — an SDK exception on this path would carry the request, and the
                // request carries the refresh token.
                log.warn("[aws-sso] token refresh failed ({}); clearing session and falling back to the default AWS chain",
                        e.getClass().getSimpleName());
                state.clear();
                return null;
            }
        }
    }

    /** The region the Identity Center endpoints are addressed in. */
    public Region region() {
        return region;
    }

    /** The shared {@code sso} client, for the credential resolver's GetRoleCredentials calls. */
    public SsoClient ssoClient() {
        return sso;
    }

    /** Resolves identity from a fresh access token: first account, then a PowerUser/Administrator role. */
    private SsoIdentity resolveIdentity(String accessToken, String idToken) {
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
        IdentityClaims claims = identityClaims(idToken);
        return new SsoIdentity(claims.name(), claims.email(),
                account.accountId(), account.accountName(), role.roleName());
    }

    /**
     * Decodes the {@code name}/{@code email} display claims out of a {@code CreateToken} id_token.
     *
     * <p>An id_token is a JWT: {@code header.payload.signature}, the payload being base64url-encoded
     * JSON. Only the payload is read here, and it is <b>not</b> signature-verified — deliberately:
     * the token was just received directly from AWS over TLS in the same request that minted it, so
     * it is not an untrusted third party's claim, and verifying it would require fetching and
     * rotating Identity Center's JWKS for a value used only as a display hint. Nothing security-
     * relevant is derived from these claims (authorisation keys off the account id and role name from
     * the {@code sso} calls, which are themselves token-authenticated).
     *
     * <p>Every fallback is null-safe: a missing id_token, a malformed one, or absent claims yield a
     * {@link IdentityClaims} with null fields rather than an exception, because "no display name" is
     * a normal Identity Center configuration, not a login failure.
     */
    private static IdentityClaims identityClaims(String idToken) {
        if (idToken == null || idToken.isBlank()) return new IdentityClaims(null, null);
        try {
            String[] parts = idToken.split("\\.");
            if (parts.length < 2) return new IdentityClaims(null, null);
            JsonNode claims = JSON.readTree(Base64.getUrlDecoder().decode(parts[1]));
            String email = firstNonBlank(
                    text(claims, "email"), text(claims, "preferred_username"), text(claims, "sub"));
            String name = firstNonBlank(
                    text(claims, "name"), text(claims, "preferred_username"),
                    text(claims, "email"), text(claims, "sub"));
            return new IdentityClaims(name, email);
        } catch (RuntimeException | IOException e) {
            // Class name only; the token itself is never logged.
            log.debug("[aws-sso] id_token claims could not be decoded ({})", e.getClass().getSimpleName());
            return new IdentityClaims(null, null);
        }
    }

    private static String text(JsonNode node, String field) {
        if (node == null) return null;
        JsonNode value = node.get(field);
        if (value == null || value.isNull() || !value.isTextual()) return null;
        String s = value.asText();
        return (s == null || s.isBlank()) ? null : s;
    }

    private static String firstNonBlank(String... values) {
        for (String v : values) {
            if (v != null && !v.isBlank()) return v;
        }
        return null;
    }

    private void storeTokens(SsoSessionState state, CreateTokenResponse token) {
        long expiresAt = System.currentTimeMillis()
                + TimeUnit.SECONDS.toMillis(token.expiresIn() != null ? token.expiresIn() : 0);
        SsoSessionState.TokenState previous = state.tokens();
        String refresh = token.refreshToken() != null
                ? token.refreshToken()
                : (previous != null ? previous.refreshToken() : null);
        state.tokens(new SsoSessionState.TokenState(token.accessToken(), refresh, expiresAt));
    }

    /** Publishes the {@code LOGIN} audit event; a listener failure must not fail the login. */
    private void publishLogin(SsoSessionState state, SsoIdentity identity) {
        try {
            events.publishEvent(new SsoAuditEvent(SsoAuditEvent.LOGIN, state.sessionId(), identity,
                    null, null, null, System.currentTimeMillis()));
        } catch (RuntimeException e) {
            log.debug("[aws-sso] LOGIN audit event could not be published ({})", e.getClass().getSimpleName());
        }
    }

    /**
     * What the front end needs to finish the device flow. Never carries the device code or any
     * token — those stay server-side in {@link SsoSessionState}.
     *
     * @param verificationUri         where the user navigates, without the code embedded
     * @param verificationUriComplete the same URL with the user code pre-filled; show this if possible
     * @param userCode                the short code to type if the complete URL cannot be opened
     * @param intervalSeconds         how long to wait between {@link #poll(SsoSessionState)} calls
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

    /** The {@code name}/{@code email} claims decoded from an id_token; either or both may be null. */
    private record IdentityClaims(String name, String email) {
    }
}
