package com.analyzer.storage.sso;

/**
 * A fire-and-forget record of an AWS SSO lifecycle transition, published as a Spring
 * {@code ApplicationEvent} for listeners outside this module (an admin portal, an audit sink).
 *
 * <h2>Why events rather than direct calls</h2>
 *
 * <p>The SSO flow must not grow a dependency on whatever wants to observe it — an audit log today,
 * a metrics counter tomorrow. Publishing a plain application event inverts that: this module names
 * the transition and the data, and any number of listeners can react without the flow knowing they
 * exist. Publishing is best-effort and must never affect the login or connect outcome.
 *
 * <h2>What is deliberately absent</h2>
 *
 * <p>No access token, refresh token, client secret, secret access key or session token is a field
 * here, so an audit listener cannot leak one even by accident. The event carries identity and target
 * metadata only — the same information the status endpoints already expose to the logged-in user.
 *
 * @param type          {@code LOGIN} | {@code CONNECT} | {@code SWAP-DENIED} | {@code LOGOUT}
 * @param sessionId     the {@code HttpSession} id the transition belongs to
 * @param identity      who the session is, when known; {@code null} for a logout after state was
 *                      cleared or a denial before login
 * @param bucket        the verified target bucket, when the type has one; otherwise {@code null}
 * @param region        the verified target region, when the type has one; otherwise {@code null}
 * @param capabilities  the probe outcome for {@code CONNECT}/{@code SWAP-DENIED}; otherwise {@code null}
 * @param timestampEpochMs when the transition happened, epoch millis
 */
public record SsoAuditEvent(String type,
                            String sessionId,
                            SsoIdentity identity,
                            String bucket,
                            String region,
                            SsoStoreSwapper.Capabilities capabilities,
                            long timestampEpochMs) {

    /** A completed device-flow login. */
    public static final String LOGIN = "LOGIN";
    /** A successful {@code /connect}: the session now has a verified bucket/region. */
    public static final String CONNECT = "CONNECT";
    /** A {@code /connect} whose capability probe failed; nothing was swapped for that user. */
    public static final String SWAP_DENIED = "SWAP-DENIED";
    /** A session ended, by {@code /logout} or by the idle sweep. */
    public static final String LOGOUT = "LOGOUT";
}
