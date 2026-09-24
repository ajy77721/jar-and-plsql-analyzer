package com.analyzer.storage.sso;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
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
 * <p>Identity Store is called with SigV4, so the lookup needs real AWS credentials — a bearer token
 * cannot call it. The lookup is made <b>with the requesting user's own credentials</b> whenever they
 * are supplied by the caller ({@link #lookup(String, String, AwsCredentialsProvider)}): a user who
 * just completed the device flow holds short-lived role credentials minted by
 * {@link SsoCredentialsResolver}, and the lookup runs inside that same user's login/poll request.
 * This matters on deployments where the pod itself has <b>no</b> ambient credentials at all: the
 * process default chain is empty, so only the user's credentials can resolve a name/email.
 *
 * <p>When no user credentials are supplied (or none could be minted), the two shared clients built
 * once against the Identity Center region are used, and they sign through the ordinary SDK default
 * chain. That is also the correct behaviour for callers with no request context.
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
    private final Region region;

    /** Resolved once; an Identity Center instance is fixed for the life of the process. */
    private volatile String identityStoreId;

    public AwsIdentityStoreLookup(
            @Value("${app.storage.sso.region:${app.storage.s3.region:us-east-1}}") String regionProperty) {
        this.region = Region.of((regionProperty == null || regionProperty.isBlank())
                ? "us-east-1" : regionProperty.trim());
        // Neither client carries credentials of its own; the SDK default provider chain supplies
        // them at call time, which keeps this class safe to instantiate in deployments without any.
        // The user-credential path builds a short-lived client per lookup instead (see resolve()).
        this.identityStore = IdentitystoreClient.builder().region(region).build();
        this.ssoAdmin = SsoAdminClient.builder().region(region).build();
    }

    /**
     * The human identity for an Identity Center {@code UserId}, resolved with the process's default
     * credential chain. Prefer {@link #lookup(String, String, AwsCredentialsProvider)} when the
     * requesting user's own credentials are available.
     */
    public Optional<ResolvedIdentity> lookup(String userId, String storeIdHint) {
        return lookup(userId, storeIdHint, null);
    }

    /**
     * The human identity for an Identity Center {@code UserId}, or empty when it cannot be resolved.
     *
     * <p>Never throws: an Identity Store without the needed grants, an unknown user id, or a
     * network failure all degrade to {@link Optional#empty()}.
     *
     * @param userId          the id_token {@code sub} (falling back to {@code userId}/{@code user_id})
     * @param storeIdHint     an IdentityStoreId taken from an id_token claim, when present
     * @param userCredentials the requesting user's credentials, or {@code null} to sign with the
     *                        process default chain. On a pod with no ambient credentials this is the
     *                        <em>only</em> chain that can work, so callers on a user's request thread
     *                        should pass it.
     */
    public Optional<ResolvedIdentity> lookup(String userId, String storeIdHint,
                                             AwsCredentialsProvider userCredentials) {
        if (userId == null || userId.isBlank()) return Optional.empty();
        String key = userId.trim();
        // computeIfAbsent guards the single-flight per user: the first caller resolves, the rest
        // reuse the cached Optional (empty included). A UserId uniquely identifies one person, so
        // the first attempt's outcome — success or failure — stays valid for every later caller.
        return cache.computeIfAbsent(key, k -> resolve(k, storeIdHint, userCredentials));
    }

    private Optional<ResolvedIdentity> resolve(String userId, String storeIdHint,
                                               AwsCredentialsProvider userCredentials) {
        // Build a client bound to the user's credentials when we have them; it is short-lived
        // because a lookup happens at most once per user, so the builder + connection pool cost is
        // paid once and the client is closed in the finally. With no user credentials the shared
        // default-chain client is used.
        final boolean temporary = userCredentials != null;
        IdentitystoreClient client = temporary
                ? IdentitystoreClient.builder().region(region).credentialsProvider(userCredentials).build()
                : identityStore;
        try {
            // Resolve the store id lazily: a token claim or an already-cached id avoids the
            // ListInstances round trip entirely.
            String resolved = firstNonBlank(storeIdHint, identityStoreId);
            final String storeId = resolved != null ? resolved : discoverIdentityStoreId(userCredentials);
            if (storeId == null) {
                log.info("[identity-store] no IdentityStoreId available; skipping lookup (grant sso:ListInstances to enable it)");
                return Optional.empty();
            }
            DescribeUserResponse user = client.describeUser(b -> b
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
        } finally {
            if (temporary) closeQuietly(client);
        }
    }

    /**
     * {@code sso:ListInstances}, cached for the process once resolved. Any failure (typically a
     * missing grant) yields {@code null} and is remembered by the caller's cache path only
     * indirectly — a repeated call is cheap and rare, so it is not itself cached.
     *
     * @param userCredentials the requesting user's credentials, or {@code null} for the default
     *                        chain (see {@link #lookup(String, String, AwsCredentialsProvider)})
     */
    private String discoverIdentityStoreId(AwsCredentialsProvider userCredentials) {
        final boolean temporary = userCredentials != null;
        SsoAdminClient admin = temporary
                ? SsoAdminClient.builder().region(region).credentialsProvider(userCredentials).build()
                : ssoAdmin;
        try {
            List<InstanceMetadata> instances = admin.listInstances(r -> {
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
        } finally {
            if (temporary) closeQuietly(admin);
        }
    }

    /** Best-effort close of a per-lookup client; a failure here must never surface as an error. */
    private static void closeQuietly(AutoCloseable client) {
        try {
            client.close();
        } catch (Exception e) {
            log.debug("[identity-store] short-lived client did not close cleanly ({})",
                    e.getClass().getSimpleName());
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
