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
 */
public record SsoIdentity(String name, String email,
                          String accountId, String accountName, String roleName) {
}
