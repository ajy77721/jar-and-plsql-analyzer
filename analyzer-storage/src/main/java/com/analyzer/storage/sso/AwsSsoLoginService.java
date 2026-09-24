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
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;

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
 *       {@code "pending"} — or the tokens. On success every (account, role) pair is enumerated
 *       through the {@code sso} service ({@code ListAccounts}, then one {@code ListAccountRoles} per
 *       account) and stored on the state so the UI can offer the choice; the identity auto-picks the
 *       configured/preferred pair (else the first account, preferring a PowerUser/Administrator
 *       role), and the display name/email are decoded from the {@code id_token}.</li>
 *   <li>{@link #chooseIdentity(SsoSessionState, String, String)} lets the caller switch a completed
 *       login to a specific (account, role) pair — the modal's chooser step — validating it against
 *       the enumerated choices, minting that pair's credentials and re-running the display-name
 *       lookup. A later {@code sso:GetRoleCredentials} then signs as the chosen pair.</li>
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
     * Deployment-wide preferred AWS account, from {@code app.storage.sso.account-id}, or blank. When
     * set and the user's login exposes it, the automatic pick lands there instead of on the arbitrary
     * first account — the fix for "everyone lands in the wrong account". A user's own choice at
     * {@code /connect} (and a per-login request from {@code /start}) still wins over it, and a
     * configured account the user cannot see is simply ignored (they get the chooser / first).
     */
    private final String configuredAccountId;

    /** Deployment-wide preferred role within {@link #configuredAccountId}; blank means any/auto. */
    private final String configuredRoleName;

    /** Cap on {@code sso:ListAccountRoles} calls made at poll-complete, so a many-account login
     *  cannot fan out into an unbounded burst against the Identity Center API. */
    private static final int MAX_ACCOUNT_ROLE_LOOKUPS = 50;

    /**
     * @param regionProperty the Identity Center / IAM region the OIDC and SSO endpoints live in.
     *                       It is distinct from the S3 bucket region a user may connect to, but in
     *                       the common case both are the same and {@code app.storage.s3.region} is
     *                       the natural default.
     * @param configuredAccountId the account the auto-pick should prefer, or blank for "first".
     * @param configuredRoleName  the role the auto-pick should prefer within that account, or blank.
     */
    public AwsSsoLoginService(
            @Value("${app.storage.sso.region:${app.storage.s3.region:us-east-1}}") String regionProperty,
            @Value("${app.storage.sso.account-id:}") String configuredAccountId,
            @Value("${app.storage.sso.role-name:}") String configuredRoleName,
            ApplicationEventPublisher events,
            AwsIdentityStoreLookup identityLookup,
            ObjectProvider<SsoCredentialsResolver> credentialsResolver) {
        this.events = events;
        this.identityLookup = identityLookup;
        this.credentialsResolver = credentialsResolver;
        this.configuredAccountId = configuredAccountId == null ? "" : configuredAccountId.trim();
        this.configuredRoleName = configuredRoleName == null ? "" : configuredRoleName.trim();
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
                // Enumerate every (account, roles) pair the login exposes — the UI offers this as a
                // choice, and the auto-pick below lands on the configured/preferred one when it can.
                List<SsoSessionState.AccountChoice> choices = listChoices(token.accessToken());
                state.accountChoices(choices);
                SsoIdentity id = resolveIdentity(state, token.accessToken(), token.idToken(), choices);
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
     * Enumerates every (account, roles) pair {@code accessToken} can see:
     * {@code sso:ListAccounts}, then one {@code sso:ListAccountRoles} per account.
     *
     * <p>The per-account role call is <b>tolerant</b>: an account whose role listing fails is kept in
     * the result with an empty role list (visible, but not selectable) rather than aborting the whole
     * login, and the calls are capped at {@link #MAX_ACCOUNT_ROLE_LOOKUPS} so a login that exposes
     * hundreds of accounts cannot fan out into an unbounded burst. A failure of {@code ListAccounts}
     * itself still propagates — with no accounts there is nothing to log in <em>as</em>.
     *
     * @return one {@link SsoSessionState.AccountChoice} per visible account, in AWS's order
     */
    public List<SsoSessionState.AccountChoice> listChoices(String accessToken) {
        List<AccountInfo> accounts = sso.listAccounts(b -> b.accessToken(accessToken)).accountList();
        List<SsoSessionState.AccountChoice> out = new ArrayList<>(accounts.size());
        int calls = 0;
        for (AccountInfo account : accounts) {
            List<String> roles = new ArrayList<>();
            if (account.accountId() != null && calls < MAX_ACCOUNT_ROLE_LOOKUPS) {
                calls++;
                try {
                    for (RoleInfo r : sso.listAccountRoles(b -> b
                            .accessToken(accessToken)
                            .accountId(account.accountId())).roleList()) {
                        if (r.roleName() != null && !r.roleName().isBlank()) roles.add(r.roleName());
                    }
                } catch (RuntimeException e) {
                    // Class name only — the request carries the bearer token. The account stays in the
                    // list with no roles, so the UI can show it but cannot let the user pick it.
                    log.warn("[aws-sso] could not list roles for one account ({}); keeping it with no roles",
                            e.getClass().getSimpleName());
                }
            }
            out.add(new SsoSessionState.AccountChoice(account.accountId(), account.accountName(),
                    List.copyOf(roles)));
        }
        return out;
    }

    /**
     * The (account, role) the automatic pick lands on, honouring the preference order
     * <b>per-login request &gt; deployment property &gt; first account / preferred role</b>.
     *
     * <p>Returns {@code null} when {@code choices} is empty. A preferred account that the user cannot
     * see is ignored — no error — so a mis-set property degrades to today's behaviour and the UI's
     * chooser still lets the user override it.
     */
    private SsoSessionState.AccountChoice preferredChoice(SsoSessionState state,
                                                          List<SsoSessionState.AccountChoice> choices) {
        if (choices.isEmpty()) return null;
        // The per-login request (a per-user pre-target from /start) outweighs the deployment default.
        String wantAccount = firstNonBlank(
                state == null ? null : state.requestedAccountId(), configuredAccountId);
        String wantRole = firstNonBlank(
                state == null ? null : state.requestedRoleName(), configuredRoleName);
        if (wantAccount != null) {
            for (SsoSessionState.AccountChoice c : choices) {
                if (wantAccount.equals(c.accountId())) {
                    return new SsoSessionState.AccountChoice(c.accountId(), c.accountName(),
                            orderRoles(c.roles(), wantRole));
                }
            }
            // Configured/requested account is not among the user's accounts: fall back (do not error).
            log.info("[aws-sso] preferred account {} is not exposed by this login; falling back to the "
                    + "first account (the user can still choose)", wantAccount);
        }
        SsoSessionState.AccountChoice first = choices.get(0);
        return new SsoSessionState.AccountChoice(first.accountId(), first.accountName(),
                orderRoles(first.roles(), wantRole));
    }

    /**
     * Reorders {@code roles} so the preferred one (when present) is first, then a PowerUser/
     * Administrator role, then AWS's original order. The first element is what the auto-pick uses;
     * the list order is also what the UI shows, so the emphasis is consistent across both.
     */
    private static List<String> orderRoles(List<String> roles, String preferredRole) {
        List<String> out = new ArrayList<>(roles);
        if (preferredRole != null && out.remove(preferredRole)) {
            out.add(0, preferredRole);
            return out;
        }
        for (int i = 0; i < out.size(); i++) {
            String r = out.get(i);
            if (r.contains("PowerUser") || r.contains("Administrator")) {
                out.remove(i);
                out.add(0, r);
                break;
            }
        }
        return out;
    }

    /**
     * Resolves identity from a fresh access token using the pre-listed {@code choices}: the preferred
     * account/{@code role} when one is configured or requested, else the first account with a
     * PowerUser/Administrator role preferred.
     *
     * <p><b>How a person is named (the resolution chain).</b> The id_token's own claims win when they
     * exist; when they do not, this deployment's identity is read from {@code sts:GetCallerIdentity}
     * — Identity Center puts the user's email in the role-session name, and that call needs no
     * permission of its own, so it is a self-managed, zero-setup source of who the user is. Identity
     * Store may still add a display name (which outranks the raw email local part); a stored
     * display name (the audit rollup's backfill) is the last resort, applied by a listener after this
     * returns. So: <b>id_token claims &gt; STS email &gt; Identity Store &gt; rollup backfill &gt;
     * anonymous</b>, applied per field on the name/email precedence described at each step below.
     *
     * <p>{@code state} is the just-authenticated user's own session state; it is used to mint that
     * user's role credentials (once) for the STS probe and the Identity Store lookup below, so
     * neither needs ambient credentials on the pod.
     */
    private SsoIdentity resolveIdentity(SsoSessionState state, String accessToken, String idToken,
                                        List<SsoSessionState.AccountChoice> choices) {
        SsoSessionState.AccountChoice account = preferredChoice(state, choices);
        if (account == null) {
            throw new IllegalStateException(
                    "The SSO login succeeded but the identity exposes no AWS accounts; assign the user to an account in Identity Center.");
        }
        if (account.roles().isEmpty()) {
            throw new IllegalStateException(
                    "The SSO login succeeded but account " + account.accountId()
                            + " exposes no roles; assign a permission set in Identity Center.");
        }
        String role = account.roles().get(0);
        IdentityClaims claims = identityClaims(idToken);
        // Keep the Identity Store coordinates so a later chosen account can re-run this lookup.
        state.idTokenClaims(new SsoSessionState.IdTokenClaims(claims.userId(), claims.storeId()));
        String name = claims.name();
        String email = claims.email();
        // Mint the user's role credentials AT MOST ONCE per login and share the provider between the
        // STS probe and the Identity Store lookup: both sign as this user, and on a pod with no
        // ambient credentials theirs is the only chain that can.
        AwsCredentialsProvider userCredentials = (name == null || email == null)
                ? userCredentials(state, accessToken, account.accountId(), role)
                : null;
        // STS email: the mechanism this feature exists for. It fills `email` only when the id_token
        // carried none — a real token claim always wins — but it IS the person, so it also outranks
        // a remembered display name from the audit rollup, which is why the backfill stands down as
        // soon as this populates email.
        String stsEmail = null;
        if (email == null) {
            stsEmail = stsCallerEmail(userCredentials);
            if (stsEmail != null) email = stsEmail;
        }
        // A bare `sub` is NOT a human name: Identity Center routinely returns only that. Prefer a
        // real name claim, then Identity Store's display name — which beats the raw STS email local
        // part below — and when that is absent, fall back to the local part. Best-effort: an empty
        // result simply leaves that field null, exactly as before.
        AwsIdentityStoreLookup.ResolvedIdentity resolved = null;
        if (name == null || email == null) {
            resolved = lookupIdentity(claims, userCredentials);
            if (resolved != null) {
                name = firstNonBlank(name, resolved.name());
                // STS email already set above outranks the Identity Store email here.
                email = firstNonBlank(email, resolved.email());
            }
        }
        // Nothing named this person anywhere: a minimally humanised local part of the STS email is
        // still the person, and far better than the bare account id.
        if (name == null && email != null) {
            name = nameFromEmail(email);
        }
        // One INFO line per login saying which source named this session — never a value, only which
        // source won (token-claims | sts-email | identity-store | anonymous; `backfill` is logged by
        // the audit listener when it applies a remembered name to an otherwise-anonymous login).
        log.info("[aws-sso] identity source: {}", identitySource(claims, stsEmail, resolved));
        return new SsoIdentity(name, email,
                account.accountId(), account.accountName(), role, null);
    }

    /**
     * The requesting user's own short-lived role credentials for {@code accountId}/{@code roleName},
     * or {@code null} when none can be obtained (no resolver bean, or AWS refused the mint). Both the
     * STS probe and the Identity Store lookup sign with these; on a pod with no ambient credentials
     * theirs is the only chain that can work. No credential value is ever logged.
     */
    private AwsCredentialsProvider userCredentials(SsoSessionState state, String accessToken,
                                                   String accountId, String roleName) {
        SsoCredentialsResolver resolver = credentialsResolver.getIfAvailable();
        return resolver == null ? null
                : resolver.resolveFor(state == null ? null : state.sessionId(),
                        accessToken, accountId, roleName);
    }

    /**
     * The email this login is actually for, read from {@code sts:GetCallerIdentity}.
     *
     * <p>Identity Center derives the role-session name from the signed-in user, and for this
     * deployment that name is the user's email; {@code GetCallerIdentity} returns it as
     * {@code <principalId>:<role-session-name>} in {@code UserId}, e.g.
     * {@code AROAU2LKKDAMTWBDT3YUB:ajaysingh.thakur@mongodb.com}. The call requires no permission of
     * its own — every AWS principal may make it — so a person is resolved with zero user effort and
     * zero admin setup.
     *
     * <p>Best-effort: no credentials, a malformed UserId (no colon, or no {@code @} after it) or an
     * SDK failure all yield {@code null}. The address is read but never logged.
     */
    private String stsCallerEmail(AwsCredentialsProvider userCredentials) {
        if (userCredentials == null) return null;
        try (StsClient sts = StsClient.builder()
                .region(region)
                .credentialsProvider(userCredentials)
                .build()) {
            GetCallerIdentityResponse caller = sts.getCallerIdentity();
            return emailFromCallerUserId(caller.userId());
        } catch (RuntimeException e) {
            // Class name only — an SDK exception can echo the request, and the request carries the
            // role credentials.
            log.info("[aws-sso] STS GetCallerIdentity failed ({}); continuing without an STS email",
                    e.getClass().getSimpleName());
            return null;
        }
    }

    /**
     * The email encoded in a {@code GetCallerIdentity} {@code UserId}, or {@code null}.
     *
     * <p>The value is {@code <principalId>:<role-session-name>}: the part after the <em>first</em>
     * colon is the role-session name, which Identity Center sets to the user's email. A plain IAM
     * user's UserId carries no colon, and a non-email session name carries no {@code @} — both yield
     * {@code null}, so a non-Identity-Center principal simply has no STS email.
     */
    private static String emailFromCallerUserId(String userId) {
        if (userId == null) return null;
        int colon = userId.indexOf(':');
        if (colon < 0) return null;
        String sessionName = userId.substring(colon + 1).trim();
        return sessionName.contains("@") ? sessionName : null;
    }

    /**
     * A minimally humanised name from an email address: the local part with its separators turned
     * into spaces. The local part is kept as-is otherwise — no letter's case is fabricated — so
     * {@code ajaysingh.thakur@mongodb.com} yields {@code ajaysingh thakur}. Returns {@code null} for
     * an empty local part.
     */
    private static String nameFromEmail(String email) {
        if (email == null) return null;
        int at = email.indexOf('@');
        String local = (at > 0 ? email.substring(0, at) : email).trim();
        if (local.isEmpty()) return null;
        return local.replaceAll("[._-]+", " ").trim();
    }

    /**
     * Which source named this login — the highest-priority contributor in the resolution order.
     * Returns a bare label only, never a value; this is the one INFO line per login the identity
     * work is allowed to emit.
     */
    private static String identitySource(IdentityClaims claims, String stsEmail,
                                         AwsIdentityStoreLookup.ResolvedIdentity resolved) {
        if (claims.name() != null || claims.email() != null) return "token-claims";
        if (stsEmail != null) return "sts-email";
        if (resolved != null) return "identity-store";
        return "anonymous";
    }

    /**
     * Applies a user's explicit (account, role) choice — the modal's new step — to a session that has
     * already completed the device flow.
     *
     * <p>Validates the pair against the choices the login actually exposed (so a stale or forged
     * value cannot send {@code GetRoleCredentials} at an account the user does not have), mints the
     * <b>chosen</b> pair's credentials and caches them under this session (so the identity lookup
     * below and every later S3 call sign as the chosen pair), and re-runs the display-name lookup for
     * that account. The returned {@link SsoIdentity} carries the chosen account/role, the identity
     * name/email (from the id_token, backfilled through Identity Store when it can), and any display
     * name already set on the session.
     *
     * @throws IllegalArgumentException when the pair is not one this login exposed
     * @throws IllegalStateException    when the session has no live access token
     */
    public SsoIdentity chooseIdentity(SsoSessionState state, String accountId, String roleName) {
        if (state == null) return null;
        synchronized (state.lock()) {
            SsoSessionState.AccountChoice chosen = null;
            for (SsoSessionState.AccountChoice c : state.accountChoices()) {
                if (c.accountId() != null && c.accountId().equals(accountId)) { chosen = c; break; }
            }
            if (chosen == null) {
                throw new IllegalArgumentException(
                        "Account " + accountId + " is not one this AWS SSO login can use.");
            }
            if (roleName == null || !chosen.roles().contains(roleName)) {
                throw new IllegalArgumentException(
                        "Role " + roleName + " is not available in account " + accountId + ".");
            }
            String token = accessToken(state);
            if (token == null) {
                throw new IllegalStateException(
                        "The AWS SSO session has no live access token; log in again.");
            }
            SsoIdentity current = state.identity();
            String name = current == null ? null : current.name();
            String email = current == null ? null : current.email();
            String displayName = current == null ? null : current.displayName();
            if (name == null || email == null) {
                // Re-run the display-name lookup for the CHOSEN account: the credentials minted here
                // (and cached under this session) are what the Identity Store call — and every later
                // S3 call — signs with.
                SsoSessionState.IdTokenClaims stored = state.idTokenClaims();
                IdentityClaims claims = new IdentityClaims(name, email,
                        stored == null ? null : stored.userId(),
                        stored == null ? null : stored.storeId(), List.of());
                AwsCredentialsProvider userCredentials =
                        userCredentials(state, token, accountId, roleName);
                AwsIdentityStoreLookup.ResolvedIdentity resolved =
                        lookupIdentity(claims, userCredentials);
                if (resolved != null) {
                    name = firstNonBlank(name, resolved.name());
                    email = firstNonBlank(email, resolved.email());
                }
            }
            return new SsoIdentity(name, email, accountId, chosen.accountName(), roleName, displayName);
        }
    }

    /**
     * The Identity Store lookup, signed with this user's credentials for {@code accountId}/
     * {@code roleName} when the id_token carried a {@code sub}. Best-effort: a missing sub, missing
     * credentials or an empty Identity Store answer yields {@code null} rather than failing the login.
     *
     * @param userCredentials this user's freshly minted role credentials (see
     *                        {@link #userCredentials}), or {@code null} to let the lookup fall back to
     *                        the process default chain
     */
    private AwsIdentityStoreLookup.ResolvedIdentity lookupIdentity(
            IdentityClaims claims, AwsCredentialsProvider userCredentials) {
        // What the id_token actually contained, as KEYS and presence flags only — never the claim
        // values (an id_token is bearer material, and a `sub`/email is personal data even when it is
        // not a secret). Without this, "the session has no name/email" is indistinguishable from
        // "the lookup never ran", which is exactly the ambiguity this line removes.
        log.info("[aws-sso] id_token claim keys present: {} (hasName={} hasEmail={} hasUserId={} hasStoreId={})",
                claims.keys(), claims.name() != null, claims.email() != null,
                claims.userId() != null, claims.storeId() != null);
        if (claims.userId() == null) {
            // The single silent path in the lookup: with no `sub`/`userId` there is nothing to
            // describe, and it returns empty without a word. Say so here instead, together with
            // what the token did carry, so "no Identity Store log line" is never the only clue.
            log.warn("[aws-sso] the id_token carried no sub/userId claim (keys present: {}), so the "
                    + "Identity Store lookup was skipped; this session keeps a null name/email",
                    claims.keys());
            return null;
        }
        log.info("[aws-sso] asking Identity Store to resolve userId={} for the display name "
                + "(userCredentials={})", shortId(claims.userId()), userCredentials != null);
        AwsIdentityStoreLookup.ResolvedIdentity resolved = identityLookup
                .lookup(claims.userId(), claims.storeId(), userCredentials)
                .orElse(null);
        if (resolved == null) {
            log.warn("[aws-sso] Identity Store resolved nothing for this user; the session "
                    + "keeps a null name/email (see the [identity-store] line above for why)");
        }
        return resolved;
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
                    null, null, null, System.currentTimeMillis(), SsoSessionState.SOURCE_SSO));
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
