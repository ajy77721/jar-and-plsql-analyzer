package com.analyzer.storage.sso;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;

/**
 * Resolves who a caller with classic IAM access keys is, by asking {@code sts:GetCallerIdentity} with
 * <em>their</em> credentials.
 *
 * <h2>Why STS and not Identity Store</h2>
 *
 * <p>The SSO path names a person through Identity Center (id_token claims, then the Identity Store).
 * A user whose account is not in any Identity Center has none of that: there is no portal, no
 * {@code sub}, no store. What they do have is the principal their access key belongs to, and every
 * AWS principal — including a plain IAM user — may call {@code GetCallerIdentity} on itself. So the
 * identity comes from that call's ARN, and the Identity Store lookup is deliberately skipped.
 *
 * <h2>The identity derived</h2>
 *
 * <p>An IAM user ARN is {@code arn:aws:iam::<account-id>:user/<name>} (a path may intervene, e.g.
 * {@code user/division/bob}, in which case the user name is the last segment). From it:
 *
 * <ul>
 *   <li>{@code name} — the IAM user name;</li>
 *   <li>{@code accountId} — the ARN's account;</li>
 *   <li>{@code email} — {@code null}: an IAM user has no email claim, and inventing one would fork
 *       the audit rollup, which keys on email first, then name, then account id. A null email falls
 *       through to the IAM user name, which is stable and human-readable.</li>
 *   <li>{@code roleName}/{@code accountName} — {@code null}: there is no permission-set role and no
 *       account name in an ARN.</li>
 * </ul>
 *
 * <p>For an ARN that is not an IAM user (e.g. an {@code assumed-role} ARN produced by a federated
 * principal) the last resource segment names the principal and the ARN's account is still used, so
 * the call never fails merely because the principal is not a bare IAM user.
 *
 * <p>No access key, secret or session token is ever logged: on failure callers see only the exception
 * class name, and {@code GetCallerIdentity} returns no credential material.
 */
@Component
public class AwsIamIdentityResolver {

    private static final Logger log = LoggerFactory.getLogger(AwsIamIdentityResolver.class);

    private final Region region;

    /**
     * @param regionProperty the region the STS call is made in. STS has a global endpoint, so the
     *                       configured SSO/S3 region is a fine default and keeps one knob for all
     *                       AWS calls the app makes.
     */
    public AwsIamIdentityResolver(
            @Value("${app.storage.sso.region:${app.storage.s3.region:us-east-1}}") String regionProperty) {
        this.region = Region.of((regionProperty == null || regionProperty.isBlank())
                ? "us-east-1" : regionProperty.trim());
    }

    /**
     * The identity the supplied access keys belong to.
     *
     * @param credentials the caller's own credentials (basic, or session when a token was given)
     * @return the resolved {@link SsoIdentity}; never {@code null}
     * @throws IllegalArgumentException when {@code credentials} is null, or STS refused the call —
     *         the caller reports this as an {@code ok:false} reply
     */
    public SsoIdentity resolve(AwsCredentials credentials) {
        if (credentials == null) {
            throw new IllegalArgumentException("IAM credentials are required.");
        }
        GetCallerIdentityResponse caller;
        try (StsClient sts = StsClient.builder()
                .region(region)
                .credentialsProvider(() -> credentials)
                .build()) {
            caller = sts.getCallerIdentity();
        } catch (RuntimeException e) {
            // Class name only: an SDK exception message can echo request material.
            throw new IllegalArgumentException(
                    "AWS STS could not verify these access keys (" + e.getClass().getSimpleName() + ").");
        }
        String arn = caller == null ? null : caller.arn();
        String accountId = accountIdFromArn(arn);
        if (accountId == null) {
            // A malformed/absent ARN still yields the account from the response itself.
            accountId = blankToNull(caller == null ? null : caller.account());
        }
        String name = iamUserName(arn);
        // Account id only — no credential material, and no name (a name is personal data, not needed
        // in the log; the audit trail records it where an operator can find it).
        log.info("[aws-iam] STS GetCallerIdentity resolved an IAM principal for account {}",
                accountId == null ? "?" : accountId);
        return new SsoIdentity(name, null, accountId, null, null, null);
    }

    /**
     * The account id out of an ARN ({@code arn:partition:service:region:account:resource}), or
     * {@code null} when the ARN is absent or too short to carry one.
     */
    static String accountIdFromArn(String arn) {
        if (arn == null) return null;
        // Limit to 6 so a resource containing ':' (e.g. an assumed-role path) is not split apart.
        String[] parts = arn.split(":", 6);
        return parts.length >= 5 ? blankToNull(parts[4]) : null;
    }

    /**
     * The IAM user name out of an ARN, or {@code null} when the ARN carries no usable resource.
     *
     * <p>For {@code user/<name>} (with an optional path) this is the final path segment. For any other
     * resource (e.g. {@code assumed-role/<role>/<session>}) the last segment names the principal, so
     * that is used rather than discarding the identity entirely.
     */
    static String iamUserName(String arn) {
        if (arn == null) return null;
        String[] parts = arn.split(":", 6);
        if (parts.length < 6) return null;
        String resource = parts[5];
        if (resource == null || resource.isBlank()) return null;
        String path = resource.startsWith("user/") ? resource.substring("user/".length()) : resource;
        int slash = path.lastIndexOf('/');
        String name = slash >= 0 ? path.substring(slash + 1) : path;
        return blankToNull(name);
    }

    private static String blankToNull(String s) {
        return (s == null || s.isBlank()) ? null : s.trim();
    }
}
