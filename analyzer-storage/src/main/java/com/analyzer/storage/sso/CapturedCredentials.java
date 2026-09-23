package com.analyzer.storage.sso;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * A snapshot of the role credentials of the user who submitted a job, taken while their request
 * thread is still alive.
 *
 * <h2>The problem this solves</h2>
 *
 * <p>{@link SsoCredentialsResolver} normally discovers the user from the current request thread.
 * An analysis job runs later, on a queue thread with no request bound, so by the time it needs S3
 * the resolver would find no session and fall back to the default chain — writing the job's output
 * as the wrong principal, or failing. Capturing the caller's credentials at submit time and handing
 * the snapshot to the job keeps the job's storage operations under the submitter's identity.
 *
 * <h2>Lifetime</h2>
 *
 * <p>The credentials are the short-lived STS-style triple from {@code sso:GetRoleCredentials}, so a
 * long job can outlive them. Callers should therefore either re-check freshness or accept that a
 * very long run may need re-submission; capturing the <em>session id</em> alongside the triple lets
 * a job ask the registry for a refreshed set where that is worth doing. No token or secret key is
 * ever logged, and this record never reaches an API response — it is an in-process hand-off.
 *
 * @param sessionId   the {@code HttpSession} id the credentials were resolved for; never null
 * @param credentials the role credentials themselves; never null
 */
public record CapturedCredentials(String sessionId, AwsCredentials credentials) {

    /**
     * A provider that always answers with the captured credentials — the shape an S3 client builder
     * wants, with no dependency on the request context.
     */
    public AwsCredentialsProvider asProvider() {
        return () -> credentials;
    }
}
