package com.analyzer.storage.sso;

import java.util.List;

/**
 * All mutable AWS SSO state belonging to exactly one browser session.
 *
 * <h2>Why state moved out of the service</h2>
 *
 * <p>The flow logic in {@link AwsSsoLoginService} used to mutate fields on the service bean itself,
 * which made the application single-tenant: a second operator's login overwrote the first's tokens.
 * The service is now stateless with respect to users — it holds only the AWS clients and the region
 * — and every per-user value lives here, keyed by {@code HttpSession.getId()} in
 * {@link SsoSessionRegistry}.
 *
 * <h2>Locking</h2>
 *
 * <p>Each instance owns its own {@link #lock()}. The service synchronises device-flow transitions
 * (register/start/poll/refresh/clear) on it, and {@link SsoCredentialsResolver} synchronises its
 * role-credential mint-and-cache on the same object, so "one user, one in-flight token refresh" and
 * "one user, one in-flight GetRoleCredentials" are the same single-flight guarantee. {@code synchronized}
 * is reentrant, so a refresh nestling inside a credential mint on the same thread cannot deadlock.
 *
 * <h2>Visibility</h2>
 *
 * <p>Every mutable field is {@code volatile}. Writers mutate under {@link #lock()}, but readers on
 * the S3 request path — {@link SsoSessionRegistry} answering "is this session authenticated?" and
 * {@code /session} — read without taking the lock, and a stale {@code loggedIn=false} there would
 * wrongly send a request down the default-credentials path.
 *
 * <p>No method here returns or logs token material to a caller that could put it in a response; the
 * token records are reachable only from the SSO package, which is what keeps the "tokens never
 * appear in a response" invariant checkable.
 */
public final class SsoSessionState {

    /** The {@code HttpSession} id this state belongs to; its key in {@link SsoSessionRegistry}. */
    private final String sessionId;

    /** Per-user single-flight lock — see the class Javadoc. Never exposed beyond the sso package. */
    private final Object lock = new Object();

    private volatile ClientRegistration registration;
    private volatile PendingFlow pendingFlow;
    private volatile TokenState tokens;
    private volatile SsoIdentity identity;
    private volatile boolean loggedIn;

    /**
     * Every (account, roles) pair the completed login exposed, so the UI can offer the choice the
     * flow deliberately does not make for the user. Populated at poll-complete from
     * {@code sso:ListAccounts} plus one {@code sso:ListAccountRoles} per account; empty before then.
     * Contains no token material — account ids, account names and role names only.
     */
    private volatile List<AccountChoice> accountChoices = List.of();

    /**
     * The Identity Store coordinates ({@code sub}/{@code identity_store_id}) decoded from the
     * id_token at poll time. Kept so a <em>later</em> chosen account can re-run the display-name
     * lookup with that account's credentials. Both fields may be null; neither is a secret.
     */
    private volatile IdTokenClaims idTokenClaims;

    /**
     * A per-login requested target from {@code /start} (a per-user pre-selection supplied by the
     * UI), or nulls when the caller supplied none. The login's auto-pick prefers this over the
     * deployment-wide {@code app.storage.sso.account-id} property, and the user's chooser over both.
     */
    private volatile String requestedAccountId;
    private volatile String requestedRoleName;

    /** The verified S3 target for this user, set only by a successful {@code /connect} probe. */
    private volatile String bucket;
    private volatile String region;
    private volatile SsoStoreSwapper.Capabilities capabilities;

    /** Last time the registry handed this state out; drives the idle-TTL sweep. */
    private volatile long lastAccessEpochMs;

    public SsoSessionState(String sessionId) {
        this.sessionId = sessionId;
        this.lastAccessEpochMs = System.currentTimeMillis();
    }

    public String sessionId() {
        return sessionId;
    }

    /** The per-user lock; see the class Javadoc. */
    public Object lock() {
        return lock;
    }

    public long lastAccessEpochMs() {
        return lastAccessEpochMs;
    }

    /** Marks the session as freshly used, so an active session is never swept for idleness. */
    public void touch() {
        this.lastAccessEpochMs = System.currentTimeMillis();
    }

    public ClientRegistration registration() {
        return registration;
    }

    public void registration(ClientRegistration registration) {
        this.registration = registration;
    }

    public PendingFlow pendingFlow() {
        return pendingFlow;
    }

    public void pendingFlow(PendingFlow pendingFlow) {
        this.pendingFlow = pendingFlow;
    }

    public TokenState tokens() {
        return tokens;
    }

    public void tokens(TokenState tokens) {
        this.tokens = tokens;
    }

    public SsoIdentity identity() {
        return identity;
    }

    public void identity(SsoIdentity identity) {
        this.identity = identity;
    }

    public boolean loggedIn() {
        return loggedIn;
    }

    public void loggedIn(boolean loggedIn) {
        this.loggedIn = loggedIn;
    }

    /** The (account, roles) choices this login exposed; empty until the device flow completes. */
    public List<AccountChoice> accountChoices() {
        return accountChoices;
    }

    public void accountChoices(List<AccountChoice> accountChoices) {
        this.accountChoices = accountChoices == null ? List.of() : List.copyOf(accountChoices);
    }

    /** The Identity Store coordinates from the id_token, or null when none were decoded. */
    public IdTokenClaims idTokenClaims() {
        return idTokenClaims;
    }

    public void idTokenClaims(IdTokenClaims idTokenClaims) {
        this.idTokenClaims = idTokenClaims;
    }

    /** A per-login requested account from {@code /start}, or null when the caller supplied none. */
    public String requestedAccountId() {
        return requestedAccountId;
    }

    /** A per-login requested role from {@code /start}, or null when the caller supplied none. */
    public String requestedRoleName() {
        return requestedRoleName;
    }

    /** Records the optional per-login pre-selection the auto-pick should prefer. */
    public void requestedTarget(String accountId, String roleName) {
        this.requestedAccountId = accountId == null || accountId.isBlank() ? null : accountId.trim();
        this.requestedRoleName = roleName == null || roleName.isBlank() ? null : roleName.trim();
    }

    public String bucket() {
        return bucket;
    }

    public String region() {
        return region;
    }

    public SsoStoreSwapper.Capabilities capabilities() {
        return capabilities;
    }

    /**
     * Records the bucket/region/capabilities a successful {@code /connect} verified for this user.
     * This is what {@code /session} reports and what tells {@link SessionRoutingArtifactStore} that
     * the user has a target to route to, as distinct from merely being logged in.
     */
    public void verifiedTarget(String bucket, String region, SsoStoreSwapper.Capabilities capabilities) {
        this.bucket = bucket;
        this.region = region;
        this.capabilities = capabilities;
    }

    /**
     * Discards every token, the resolved identity and the verified target — the state a logout or a
     * failed refresh leaves behind. The instance stays valid (and in the registry) so a following
     * {@code /start} can reuse it; {@link SsoSessionRegistry#end(String)} is what removes it.
     */
    public void clear() {
        synchronized (lock) {
            registration = null;
            pendingFlow = null;
            tokens = null;
            identity = null;
            loggedIn = false;
            accountChoices = List.of();
            idTokenClaims = null;
            requestedAccountId = null;
            requestedRoleName = null;
            bucket = null;
            region = null;
            capabilities = null;
        }
    }

    /**
     * OIDC client credentials from {@code RegisterClient}. The secret is deliberately never logged
     * and never leaves the sso package.
     */
    public record ClientRegistration(String clientId, String clientSecret) {
    }

    /** An in-progress device authorization: the device code plus when it lapses. */
    public record PendingFlow(String deviceCode, int intervalSeconds, long expiresAtEpochMs) {
    }

    /** Live tokens with their absolute expiry, so refresh is a clock comparison. */
    public record TokenState(String accessToken, String refreshToken, long expiresAtEpochMs) {
    }

    /**
     * One selectable AWS account and the permission-set roles it exposes to this login.
     *
     * @param accountId   the 12-digit AWS account id (also the audit rollup key when no email claim
     *                    exists); never null once the flow has listed accounts
     * @param accountName a human name for the account, or null when AWS supplied none
     * @param roles       the role names available in this account; empty when the per-account role
     *                    listing failed (visible, but not connectable)
     */
    public record AccountChoice(String accountId, String accountName, List<String> roles) {
    }

    /**
     * The Identity Store coordinates decoded from the id_token — the inputs to a display-name
     * lookup, kept so a chosen account can re-run it. Not secret.
     *
     * @param userId  the Identity Store UserId ({@code sub}), or null
     * @param storeId the IdentityStoreId named by the token, or null
     */
    public record IdTokenClaims(String userId, String storeId) {
    }
}
