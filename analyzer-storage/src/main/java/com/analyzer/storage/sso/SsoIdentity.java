package com.analyzer.storage.sso;

/**
 * Who an AWS SSO session is: the AWS account and permission-set role the user logged in as, plus
 * the human-identifying claims carried by the OIDC {@code id_token}.
 *
 * <p>The {@code account*}/{@code role*} fields answer "what is this session allowed to touch?" and
 * are always present once the device flow completes — they come from {@code sso:ListAccounts} /
 * {@code sso:ListAccountRoles}, which the flow already has to call to mint role credentials.
 *
 * <p>The {@code name}/{@code email} fields answer "who is this?" and are strictly best-effort: they
 * are decoded from the {@code id_token} returned by {@code CreateToken}, and an Identity Center
 * instance configured without the {@code openid} scope (or with a profile that omits those claims)
 * legitimately yields {@code null} for either. Every consumer must therefore treat them as
 * nullable display hints, never as identity keys — the account id and role name are the keys.
 *
 * <p>No token, secret or credential material is ever part of this record; it is safe to serialise
 * into an API response, which is exactly what the status endpoints do.
 *
 * @param name        display name from the id_token ({@code name}, else {@code preferred_username},
 *                    ...), or {@code null}
 * @param email       email address from the id_token ({@code email}, else
 *                    {@code preferred_username}, ...), or {@code null}
 * @param accountId   the AWS account the session resolved to; non-null once logged in
 * @param accountName a human name for that account, or {@code null} when AWS did not supply one
 * @param roleName    the permission-set role the session uses; non-null once logged in
 * @param displayName a name the person (or an admin) chose to be shown as, overriding the AWS-
 *                    derived {@code name} for every rendering; {@code null} when none was set.
 *                    It is display-only and never participates in identity keys, so setting it
 *                    cannot fork a user's audit rollup.
 */
public record SsoIdentity(String name, String email,
                          String accountId, String accountName, String roleName,
                          String displayName) {

    /**
     * The name to render for this identity: the chosen {@code displayName} when set, otherwise the
     * AWS-derived {@code name}. Every surface that shows a person — the home pill, the SSO modal,
     * the admin Users tab and the audit "User" column — uses this rather than {@link #name()} so a
     * display-name override takes effect everywhere at once.
     */
    public String shownName() {
        return (displayName != null && !displayName.isBlank()) ? displayName.trim() : name;
    }

    /** A copy of this identity carrying a different display name; {@code null} clears the override. */
    public SsoIdentity withDisplayName(String displayName) {
        String v = (displayName == null || displayName.isBlank()) ? null : displayName.trim();
        return new SsoIdentity(name, email, accountId, accountName, roleName, v);
    }
}
