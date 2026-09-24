package com.analyzer.storage.sso;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
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
import java.util.ArrayList;
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

    /**
     * Standard OIDC profile/email scopes, requested alongside {@code openid} on the same reasoning:
     * some Identity Center instances then put {@code email}/{@code name}/{@code preferred_username}
     * straight into the {@code id_token}, which resolves the display identity without needing an
     * Identity Store call (and therefore without needing {@code identitystore:DescribeUser} or
     * {@code sso:ListInstances} on the user's role). They are ordinary OIDC scopes and are simply
     * ignored by an instance that does not honour them, so requesting them is harmless; when they do
     * work, the lookup below is not reached at all because name/email are already present.
     */
    private static final String SCOPE_PROFILE = "profile";

    private static final String SCOPE_EMAIL = "email";

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
    private final AwsIdentityStoreLookup identityLookup;
    /**
     * Lazy so the identity lookup can sign with the <em>user's</em> credentials without a bean
     * cycle: {@link SsoCredentialsResolver} depends on this service, so this service can only hold
     * a provider for the resolver, not the resolver itself.
     */
    private final ObjectProvider<SsoCredentialsResolver> credentialsResolver;

    /**
     * @param regionProperty the Identity Center / IAM region the OIDC and SSO endpoints live in.
     *                       It is distinct from the S3 bucket region a user may connect to, but in
     *                       the common case both are the same and {@code app.storage.s3.region} is
     *                       the natural default.
     */
    public AwsSsoLoginService(
            @Value("${app.storage.sso.region:${app.storage.s3.region:us-east-1}}") String regionProperty,
            ApplicationEventPublisher events,
            AwsIdentityStoreLookup identityLookup,
            ObjectProvider<SsoCredentialsResolver> credentialsResolver) {
        this.events = events;
        this.identityLookup = identityLookup;
        this.credentialsResolver = credentialsResolver;
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
            // this pair and rejects an authorization_code registration. `openid`, `profile` and
            // `email` are requested so the CreateToken response's id_token carries name/email
            // claims where the instance supports them; `sso:account:access` is what grants the
            // ListAccounts/ListAccountRoles/GetRoleCredentials calls the flow itself needs.
            // TEMP-DIAGNOSTIC: the request built here carries no secret (clientName/type/grants/
            // scopes are all public metadata), so logging it is safe; it exists only until the
            // registerClient rejection is root-caused.
            log.warn("[aws-sso][diag] registerClient request: clientName={} clientType={} grantTypes={} scopes={} region={} oidc={} endpointOverride={}",
                    CLIENT_NAME, CLIENT_TYPE_PUBLIC, GRANT_DEVICE_CODE + "," + GRANT_REFRESH_TOKEN,
                    SCOPE_SSO_ACCOUNT_ACCESS + "," + SCOPE_OPENID + "," + SCOPE_PROFILE + "," + SCOPE_EMAIL,
                    region, oidc.getClass().getName(),
                    oidc.serviceClientConfiguration().endpointOverride());
            RegisterClientResponse reg;
            try {
                reg = oidc.registerClient(b -> b
                        .clientName(CLIENT_NAME)
                        // Required by the endpoint; omitting it is the InvalidClientMetadataException.
                        .clientType(CLIENT_TYPE_PUBLIC)
                        .grantTypes(GRANT_DEVICE_CODE, GRANT_REFRESH_TOKEN)
                        .scopes(SCOPE_SSO_ACCOUNT_ACCESS, SCOPE_OPENID, SCOPE_PROFILE, SCOPE_EMAIL));
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
                SsoIdentity id = resolveIdentity(state, token.accessToken(), token.idToken());
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

    /**
     * Revokes this session <b>at AWS</b>, so a logout or an admin revoke kills the server-side
     * Identity Center sign-in session rather than only forgetting local state.
     *
     * <p>Uses the {@code sso:Logout} operation, which is Identity Center's server-side revocation:
     * it invalidates the access token and the sign-in session it belongs to, so the refresh token can
     * no longer mint new credentials either. (The AWS SDK at the pinned 2.29.24 has no
     * {@code ssooidc:RevokeToken} model — that operation does not exist on the SSO OIDC endpoint —
     * so {@code sso:Logout} is the correct and available primitive.)
     *
     * <p>Deliberately non-fatal and best-effort: a missing session, an already-expired token, or an
     * endpoint failure are all normal and log one INFO line — the caller must still proceed to drop
     * local state, because a live local session is worse than a failed AWS logout. No token or secret
     * is ever logged.
     *
     * @return true when AWS accepted the logout, false when there was nothing to revoke or it could
     *         not be completed (the caller ignores this and clears local state regardless)
     */
    public boolean revokeAwsToken(SsoSessionState state) {
        if (state == null) return false;
        synchronized (state.lock()) {
            SsoSessionState.TokenState tokens = state.tokens();
            if (tokens == null || tokens.accessToken() == null) {
                // Nothing was ever minted for this session (or it was already cleared); there is no
                // AWS-side session to end.
                log.info("[aws-sso] no AWS-side session token to revoke for this session");
                return false;
            }
            try {
                sso.logout(b -> b.accessToken(tokens.accessToken()));
                log.info("[aws-sso] AWS-side Identity Center session revoked (sso:Logout)");
                return true;
            } catch (RuntimeException e) {
                // Class name only: the request carries the access token and must not be logged.
                log.info("[aws-sso] AWS-side logout did not complete ({}); "
                        + "clearing the local session anyway", e.getClass().getSimpleName());
                return false;
            }
        }
    }

    /**
     * Resolves identity from a fresh access token: first account, then a PowerUser/Administrator role.
     *
     * <p>{@code state} is the just-authenticated user's own session state; it is used to mint that
     * user's role credentials for the Identity Store display-name lookup below, so the lookup does
     * not need ambient credentials on the pod.
     */
    private SsoIdentity resolveIdentity(SsoSessionState state, String accessToken, String idToken) {
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
        String name = claims.name();
        String email = claims.email();
        // What the id_token actually contained, as KEYS and presence flags only — never the claim
        // values (an id_token is bearer material, and a `sub`/email is personal data even when it is
        // not a secret). Without this, "the session has no name/email" is indistinguishable from
        // "the lookup never ran", which is exactly the ambiguity this line removes.
        log.info("[aws-sso] id_token claim keys present: {} (hasName={} hasEmail={} hasUserId={} hasStoreId={})",
                claims.keys(), claims.name() != null, claims.email() != null,
                claims.userId() != null, claims.storeId() != null);
        // A bare `sub` is NOT a human name: Identity Center routinely returns only that. Prefer a
        // real name/email claim, and when there is none, ask Identity Store to turn the sub (which
        // for Identity Center is the Identity Store UserId) into a person. Best-effort — an empty
        // result just leaves the identity anonymous as before.
        if (name == null || email == null) {
            // The lookup runs inside this user's own poll request, and the access token above was
            // just issued to them: mint their role credentials and sign the Identity Store call with
            // them. On a pod with no ambient credentials this is the only chain that can work;
            // a null here (minting failed) degrades to the default chain inside the lookup.
            SsoCredentialsResolver resolver = credentialsResolver.getIfAvailable();
            AwsCredentialsProvider userCredentials = resolver == null ? null
                    : resolver.resolveFor(state == null ? null : state.sessionId(),
                            accessToken, account.accountId(), role.roleName());
            AwsIdentityStoreLookup.ResolvedIdentity resolved;
            if (claims.userId() == null) {
                // The single silent path in the lookup: with no `sub`/`userId` there is nothing to
                // describe, and it returns empty without a word. Say so here instead, together with
                // what the token did carry, so "no Identity Store log line" is never the only clue.
                resolved = null;
                log.warn("[aws-sso] the id_token carried no sub/userId claim (keys present: {}), so the "
                        + "Identity Store lookup was skipped; this session keeps a null name/email",
                        claims.keys());
            } else {
                log.info("[aws-sso] asking Identity Store to resolve userId={} for the display name "
                        + "(userCredentials={})", shortId(claims.userId()), userCredentials != null);
                resolved = identityLookup
                        .lookup(claims.userId(), claims.storeId(), userCredentials)
                        .orElse(null);
                if (resolved == null) {
                    log.warn("[aws-sso] Identity Store resolved nothing for this user; the session "
                            + "keeps a null name/email (see the [identity-store] line above for why)");
                }
            }
            if (resolved != null) {
                name = firstNonBlank(name, resolved.name());
                email = firstNonBlank(email, resolved.email());
            }
        }
        return new SsoIdentity(name, email,
                account.accountId(), account.accountName(), role.roleName(), null);
    }

    /**
     * Decodes the display and identity claims out of a {@code CreateToken} id_token.
     *
     * <p>An id_token is a JWT: {@code header.payload.signature}, the payload being base64url-encoded
     * JSON. Only the payload is read here, and it is <b>not</b> signature-verified — deliberately:
     * the token was just received directly from AWS over TLS in the same request that minted it, so
     * it is not an untrusted third party's claim, and verifying it would require fetching and
     * rotating Identity Center's JWKS for a value used only as a display hint. Nothing security-
     * relevant is derived from these claims (authorisation keys off the account id and role name from
     * the {@code sso} calls, which are themselves token-authenticated).
     *
     * <p>The {@code sub} is deliberately <b>not</b> used as a name or email. It is an opaque
     * identifier, and rendering it as a person's name is exactly the "anonymous account id" symptom
     * this class exists to avoid; instead it is surfaced as {@code userId} for the Identity Store
     * lookup. Email/name fall back only to genuinely human claims.
     *
     * <p>Every fallback is null-safe: a missing id_token, a malformed one, or absent claims yield a
     * {@link IdentityClaims} with null fields rather than an exception, because "no display name" is
     * a normal Identity Center configuration, not a login failure.
     */
    private static IdentityClaims identityClaims(String idToken) {
        if (idToken == null || idToken.isBlank()) {
            log.info("[aws-sso] CreateToken returned no id_token, so there are no display claims to read");
            return new IdentityClaims(null, null, null, null, List.of());
        }
        try {
            String[] parts = idToken.split("\\.");
            if (parts.length < 2) return new IdentityClaims(null, null, null, null, List.of());
            JsonNode claims = JSON.readTree(Base64.getUrlDecoder().decode(parts[1]));
            String email = firstNonBlank(
                    text(claims, "email"), text(claims, "preferred_username"));
            String name = firstNonBlank(
                    text(claims, "name"), text(claims, "preferred_username"), email);
            // Identity Center puts the Identity Store UserId in `sub`; some instances also name the
            // store outright, which saves the ListInstances round trip.
            String userId = firstNonBlank(
                    text(claims, "sub"), text(claims, "userId"), text(claims, "user_id"));
            String storeId = firstNonBlank(
                    text(claims, "identity_store_id"), text(claims, "identityStoreId"));
            return new IdentityClaims(name, email, userId, storeId, claimKeys(claims));
        } catch (RuntimeException | IOException e) {
            // Class name only; the token itself is never logged.
            log.debug("[aws-sso] id_token claims could not be decoded ({})", e.getClass().getSimpleName());
            return new IdentityClaims(null, null, null, null, List.of());
        }
    }

    /**
     * The names — never the values — of the top-level fields in a decoded id_token payload, sorted
     * for a stable log line. This is the diagnostic that distinguishes "Identity Center sent us only
     * {@code aud}/{@code exp}/{@code iat}/{@code iss}" from "we failed to read the claims at all".
     */
    private static List<String> claimKeys(JsonNode claims) {
        if (claims == null || !claims.isObject()) return List.of();
        List<String> keys = new ArrayList<>();
        claims.fieldNames().forEachRemaining(keys::add);
        keys.sort(String::compareTo);
        return keys;
    }

    /** Short, non-reversible suffix of an id for a log line — as in {@link AwsIdentityStoreLookup}. */
    private static String shortId(String id) {
        return id == null ? "null" : (id.length() <= 8 ? "…" : "…" + id.substring(id.length() - 8));
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

    /**
     * The display/identity claims decoded from an id_token; any field may be null.
     *
     * @param name       a human name claim ({@code name}/{@code preferred_username}), or null
     * @param email      an email claim ({@code email}/{@code preferred_username}), or null
     * @param userId     the Identity Store UserId ({@code sub}), or null when absent
     * @param storeId    the IdentityStoreId named by the token, or null
     * @param keys       the top-level claim names the token carried, for diagnostics only — never
     *                   the values; empty when there was no decodable id_token
     */
    private record IdentityClaims(String name, String email, String userId, String storeId,
                                  List<String> keys) {
    }
}
