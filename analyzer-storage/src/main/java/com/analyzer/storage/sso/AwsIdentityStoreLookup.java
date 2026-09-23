package com.analyzer.storage.sso;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.identitystore.IdentitystoreClient;
import software.amazon.awssdk.services.identitystore.model.DescribeUserResponse;
import software.amazon.awssdk.services.identitystore.model.Email;
import software.amazon.awssdk.services.ssoadmin.SsoAdminClient;
import software.amazon.awssdk.services.ssoadmin.model.InstanceMetadata;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Best-effort resolution of the human behind an IAM Identity Center login.
 *
 * <h2>Why this exists</h2>
 *
 * <p>The device flow's {@code id_token} (scopes {@code openid} + {@code sso:account:access}) is
 * frequently anonymous: Identity Center puts only a {@code sub} in it, and {@code sso:ListAccounts}/
 * {@code ListAccountRoles} return account metadata, not a person. Without this, every surface falls
 * back to the raw account id. Identity Store is the one AWS API that maps that {@code sub} — for
 * Identity Center the id_token {@code sub} is the Identity Store {@code UserId} — to a
 * {@code UserName}/{@code DisplayName}/email.
 *
 * <h2>Credentials and failure policy</h2>
 *
 * <p>The lookups run server-side with the process's <b>own default credential chain</b>, not the
 * logged-in user's bearer token: Identity Store is an admin API, not something an SSO user token
 * can call. Both clients are built once (thread-safe) against the Identity Center region.
 *
 * <p>Every failure is non-fatal and expected in the common case where the deployer has not granted
 * {@code identitystore:DescribeUser}/{@code sso:ListInstances}: the method logs one INFO line and
 * returns empty, never a stack trace, so a missing permission degrades to "anonymous" rather than
 * breaking login. Results (including a negative result) are cached per {@code UserId}, so the
 * lookup — and any failure — is attempted at most once per user per JVM.
 *
 * <p>No token, secret or credential value is read, logged or returned here; only the display
 * identity. The {@link IdentityStoreId} is cached for the process lifetime because an Identity
 * Center instance does not change during a run.
 */
@Service
public class AwsIdentityStoreLookup {

    private static final Logger log = LoggerFactory.getLogger(AwsIdentityStoreLookup.class);

    /** UserId → resolved identity (empty when none could be resolved, so we never re-ask). */
    private final ConcurrentHashMap<String, Optional<ResolvedIdentity>> cache = new ConcurrentHashMap<>();

    private final IdentitystoreClient identityStore;
    private final SsoAdminClient ssoAdmin;

    /** Resolved once; an Identity Center instance is fixed for the life of the process. */
    private volatile String identityStoreId;

    public AwsIdentityStoreLookup(
            @Value("${app.storage.sso.region:${app.storage.s3.region:us-east-1}}") String regionProperty) {
        Region region = Region.of((regionProperty == null || regionProperty.isBlank())
                ? "us-east-1" : regionProperty.trim());
        // Neither client carries credentials of its own; the SDK default provider chain supplies
        // them at call time, which keeps this class safe to instantiate in deployments without any.
        this.identityStore = IdentitystoreClient.builder().region(region).build();
        this.ssoAdmin = SsoAdminClient.builder().region(region).build();
    }

    /**
     * The human identity for an Identity Center {@code UserId}, or empty when it cannot be resolved.
     *
     * <p>Never throws: an Identity Store without the needed grants, an unknown user id, or a
     * network failure all degrade to {@link Optional#empty()}.
     *
     * @param userId        the id_token {@code sub} (falling back to {@code userId}/{@code user_id})
     * @param storeIdHint   an IdentityStoreId taken from an id_token claim, when present
     */
    public Optional<ResolvedIdentity> lookup(String userId, String storeIdHint) {
        if (userId == null || userId.isBlank()) return Optional.empty();
        String key = userId.trim();
        // computeIfAbsent guards the single-flight per user: the first caller resolves, the rest
        // reuse the cached Optional (empty included).
        return cache.computeIfAbsent(key, k -> resolve(k, storeIdHint));
    }

    private Optional<ResolvedIdentity> resolve(String userId, String storeIdHint) {
        // Resolve the store id lazily: a token claim or an already-cached id avoids the
        // ListInstances round trip entirely.
        String resolved = firstNonBlank(storeIdHint, identityStoreId);
        final String storeId = resolved != null ? resolved : discoverIdentityStoreId();
        if (storeId == null) {
            log.info("[identity-store] no IdentityStoreId available; skipping lookup (grant sso:ListInstances to enable it)");
            return Optional.empty();
        }
        try {
            DescribeUserResponse user = identityStore.describeUser(b -> b
                    .identityStoreId(storeId)
                    .userId(userId));
            String email = firstNonBlank(user.userName(), primaryEmail(user.emails()));
            String name = firstNonBlank(user.displayName(), user.userName(), email);
            if (name == null && email == null) {
                log.info("[identity-store] user {} resolved to no name/email", shortId(userId));
                return Optional.empty();
            }
            log.info("[identity-store] resolved display identity for user {}", shortId(userId));
            return Optional.of(new ResolvedIdentity(userId, name, email));
        } catch (RuntimeException e) {
            // Class name only — a message here could echo request material and there is no secret
            // to protect beyond what the SDK already excludes.
            log.info("[identity-store] lookup unavailable ({}); continuing anonymously "
                    + "(grant identitystore:DescribeUser to enable it)", e.getClass().getSimpleName());
            return Optional.empty();
        }
    }

    /**
     * {@code sso:ListInstances} through the default chain, cached for the process. Any failure
     * (typically a missing grant) yields {@code null} and is remembered by the caller's cache path
     * only indirectly — a repeated call is cheap and rare, so it is not itself cached.
     */
    private String discoverIdentityStoreId() {
        try {
            List<InstanceMetadata> instances = ssoAdmin.listInstances(r -> {
            }).instances();
            if (instances == null || instances.isEmpty()) {
                log.info("[identity-store] sso:ListInstances returned no Identity Center instance");
                return null;
            }
            String id = instances.get(0).identityStoreId();
            if (id != null && !id.isBlank()) {
                identityStoreId = id;
                return id;
            }
            return null;
        } catch (RuntimeException e) {
            log.info("[identity-store] could not discover the IdentityStoreId ({}); "
                    + "grant sso:ListInstances to enable the lookup", e.getClass().getSimpleName());
            return null;
        }
    }

    private static String primaryEmail(List<Email> emails) {
        if (emails == null || emails.isEmpty()) return null;
        for (Email e : emails) {
            if (e != null && e.primary() != null && e.primary() && e.value() != null) return e.value();
        }
        Email first = emails.get(0);
        return first == null ? null : first.value();
    }

    private static String firstNonBlank(String... values) {
        for (String v : values) {
            if (v != null && !v.isBlank()) return v;
        }
        return null;
    }

    /** Short, non-reversible suffix of an id for a log line. */
    private static String shortId(String id) {
        return id.length() <= 8 ? "…" : "…" + id.substring(id.length() - 8);
    }

    /** A resolved person: their Identity Store {@code UserId} plus the best name/email available. */
    public record ResolvedIdentity(String userId, String name, String email) {
    }
}
