package com.analyzer.storage.sso;

import com.analyzer.storage.ArtifactStore;

/**
 * A snapshot of the per-user parser/JAR stores a connected session was routed to, taken on the
 * request thread so a background job can keep using the right bucket, region and credentials.
 *
 * <h2>Why capture stores and not just credentials</h2>
 *
 * <p>A job that only had credentials would still have to be told the bucket and region, and would
 * have to build its own S3 client. The session's {@code BuiltStores} already <em>are</em> that pair
 * of clients — built for the user's verified bucket/region and signed with their role — so capturing
 * the stores captures all three at once and the job needs no request context and no extra wiring.
 *
 * <p>Only the stores the project actually registered are non-null: a deployment running the parser
 * against S3 but the JAR analyzer locally captures a {@code parser} store and a {@code null}
 * {@code jar} store, exactly mirroring what {@code /connect} built. A job must therefore ask for the
 * feature it needs and tolerate {@code null} rather than assume both.
 *
 * <h2>Lifetime</h2>
 *
 * <p>The snapshot is immutable and holds live store delegates; a later {@code /connect} by the same
 * user builds a fresh pair and does <em>not</em> mutate this snapshot, so a job in flight keeps the
 * target it started with. If the user logs out, {@link SsoStoreSwapper#forget(String)} drops the
 * cache but an already-captured snapshot keeps working for the remainder of the job.
 *
 * @param sessionId   the {@code HttpSession} id the stores were built for
 * @param parser      the PL/SQL parser's store, or {@code null} when that feature is not S3-backed
 * @param jar         the JAR analyzer's store, or {@code null} when that feature is not S3-backed
 * @param bucket      the verified bucket, or the configured default
 * @param region      the verified region, or the configured default
 * @param capabilities what the connect probe verified, or {@code null} when the user never connected
 */
public record CapturedStores(String sessionId,
                             ArtifactStore parser,
                             ArtifactStore jar,
                             String bucket,
                             String region,
                             SsoStoreSwapper.Capabilities capabilities) {

    /**
     * The store for a registered target by its registration name ({@code "parser"} or {@code "jar"}),
     * or {@code null} when that feature is not S3-backed in this deployment.
     */
    public ArtifactStore storeFor(String name) {
        if ("parser".equals(name)) return parser;
        if ("jar".equals(name)) return jar;
        return null;
    }
}
