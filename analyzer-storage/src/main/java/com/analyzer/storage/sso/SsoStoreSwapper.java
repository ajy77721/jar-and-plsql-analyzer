package com.analyzer.storage.sso;

import com.analyzer.storage.ArtifactStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Builds and caches a private set of S3-backed stores for each user who has connected a bucket,
 * gated by a capability probe run with that user's own credentials.
 *
 * <h2>From one swapped store to one store per user</h2>
 *
 * <p>The single-tenant version re-pointed the shared {@code parserArtifactStore}/{@code jarArtifactStore}
 * beans at the connecting user's bucket, which meant a second user's connect silently moved the
 * first user's data. Now every connect is keyed by {@code HttpSession} id: this component keeps a
 * {@code sessionId -> BuiltStores} map, and {@link SessionRoutingArtifactStore} — the bean every
 * consumer injects — looks up the calling user's entry on each operation. One user's connect cannot
 * affect another's stores.
 *
 * <h2>What a connect has to build</h2>
 *
 * <p>Two things determine which bytes an S3 store touches: the bucket/region baked into its
 * {@code S3Client}, and the credentials that client signs with. Neither can be changed on a live
 * client, so a connect rebuilds the stores against the new target using a credential provider fixed
 * to that user ({@link SsoCredentialsResolver#resolveFor(String)}).
 *
 * <h2>Capability verification gates the swap</h2>
 *
 * <p>Before any store is built this class runs a three-step probe against the target bucket with a
 * dedicated {@link S3Client} signed by that user's credentials:
 *
 * <ol>
 *   <li>{@code listObjectsV2} with {@code maxKeys=1} — can we <b>read</b> the bucket?</li>
 *   <li>{@code putObject} of a tiny marker under a reserved {@code _amp-probe/} prefix — can we
 *       <b>upload</b>?</li>
 *   <li>{@code deleteObject} of that same marker — can we <b>write</b> (mutate) the bucket?</li>
 * </ol>
 *
 * <p>Each step is independent and its outcome is recorded, so the UI can show which permission is
 * missing. Only a fully green probe builds and caches stores: a failing probe returns immediately,
 * leaving any previously connected target for that user intact. The probe never logs credential
 * material — on failure it reports the AWS error <em>code</em> (e.g. {@code AccessDenied}) and HTTP
 * status only.
 *
 * <h2>Why a builder registry rather than the config calling us back</h2>
 *
 * <p>{@code StorageConfig} owns how to build each store (mode, prefix, endpoint, hybrid thresholds,
 * the shared Mongo pool). Rather than duplicate that here, each S3-backed bean registers a
 * {@link StoreBuilder} at construction time. This class stays ignorant of storage modes and simply
 * re-invokes the factory the config already trusts, once per user per connect.
 */
@Component
public class SsoStoreSwapper {

    private static final Logger log = LoggerFactory.getLogger(SsoStoreSwapper.class);

    /** Registration name for the PL/SQL parser's store; matches the bean qualifier. */
    public static final String PARSER = "parser";
    /** Registration name for the JAR analyzer's store; matches the bean qualifier. */
    public static final String JAR = "jar";

    /**
     * Prefix reserved for probe objects. Deliberately unlike any artifact key
     * ({@code <prefix>/…}), so a probe object can never collide with, or be mistaken for, real
     * stored data — and so an operator can spot-and-sweep stray probes from one known location if a
     * delete ever fails mid-probe.
     */
    private static final String PROBE_PREFIX = "_amp-probe/";

    /** Content of the probe object; small, inert, and self-describing if it is ever found left over. */
    private static final String PROBE_CONTENT = "amp-analyzer connect probe";

    /**
     * Builds one store for a user's target. Supplied by {@code StorageConfig}, which owns every
     * other setting the build needs; the per-user credential provider is passed in so the built
     * client signs as that user.
     */
    @FunctionalInterface
    public interface StoreBuilder {
        ArtifactStore build(String bucket, String region, AwsCredentialsProvider credentials);
    }

    /**
     * The stores built for one user's verified target.
     *
     * @param parser       the PL/SQL parser's store, or {@code null} when that feature is not S3-backed
     * @param jar          the JAR analyzer's store, or {@code null} when that feature is not S3-backed
     * @param bucket       the verified bucket
     * @param region       the verified region
     * @param capabilities the probe outcome backing this build
     */
    public record BuiltStores(ArtifactStore parser, ArtifactStore jar,
                              String bucket, String region, Capabilities capabilities) {

        /** The store for a registration name ({@link #PARSER} or {@link #JAR}), or {@code null}. */
        public ArtifactStore storeFor(String name) {
            if (PARSER.equals(name)) return parser;
            if (JAR.equals(name)) return jar;
            return null;
        }
    }

    /** name -> builder, registered once per S3-backed bean at startup. */
    private final Map<String, StoreBuilder> builders = new ConcurrentHashMap<>();

    /** sessionId -> that user's built stores; written on connect, read on every routed operation. */
    private final ConcurrentHashMap<String, BuiltStores> storesBySession = new ConcurrentHashMap<>();

    /**
     * sessionId -> monitor that serialises {@link #connect} for that user. A manual connect from the
     * modal and an {@link #autoReconnect} fired by a login completing can race; serialising on one
     * monitor per session means the second entrant sees the stores the first just built (and for an
     * auto-reconnect to the same target, {@link #autoReconnect} skips the rebuild entirely) instead
     * of two probes and two builds interleaving.
     */
    private final ConcurrentHashMap<String, Object> connectLocks = new ConcurrentHashMap<>();

    /** Effective target when a user supplies none, and what {@code /session} reports before connect. */
    private final String defaultBucket;
    private final String defaultRegion;

    /** Resolves per-session credential providers; never inspected or logged here. */
    private final SsoCredentialsResolver credentials;

    /** Lets a background job capture the current user's stores on the request thread. */
    private final CurrentHttpSession currentSession;

    /** The registry, consulted only to name the identity on a CONNECT audit event. */
    private final SsoSessionRegistry registry;

    private final ApplicationEventPublisher events;

    public SsoStoreSwapper(@Value("${app.storage.s3.bucket:}") String defaultBucket,
                           @Value("${app.storage.s3.region:}") String defaultRegion,
                           SsoCredentialsResolver credentials,
                           CurrentHttpSession currentSession,
                           SsoSessionRegistry registry,
                           ApplicationEventPublisher events) {
        this.defaultBucket = defaultBucket == null ? "" : defaultBucket.trim();
        this.defaultRegion = defaultRegion == null ? "" : defaultRegion.trim();
        this.credentials = credentials;
        this.currentSession = currentSession;
        this.registry = registry;
        this.events = events;
    }

    /**
     * Called once per S3-backed store bean during context startup. Not synchronised against
     * {@link #connect(String, String, String)}: Spring publishes all singletons before any request
     * can arrive, so registration is always complete before a connect is possible.
     */
    public void register(String name, StoreBuilder builder) {
        builders.put(name, builder);
        log.info("[storage] {} store registered as SSO-connectable", name);
    }

    /**
     * Probes {@code bucket}/{@code region} for {@code sessionId}, then — only if every capability
     * passed — builds that user's stores against it and caches them.
     *
     * <p>A blank bucket or region falls back to the configured default, so a UI that only lets the
     * user override one of the two does not accidentally clear the other.
     *
     * <p>The probe runs first and is the sole gate: on any failed capability this method returns a
     * {@link ConnectResult} whose {@link ConnectResult#ok()} is {@code false}, having touched
     * nothing — the user's previously connected stores (if any) stay cached, and no other user is
     * affected. On success the built pair replaces this user's cached pair atomically.
     */
    public ConnectResult connect(String sessionId, String bucket, String region) {
        if (sessionId == null || sessionId.isBlank()) {
            throw new NotAuthenticatedException();
        }
        // One monitor per session: a manual connect from the modal and an autoReconnect fired by a
        // login completing cannot probe and build for the same user at the same time. See connectLocks.
        synchronized (lockFor(sessionId)) {
            return connectLocked(sessionId, bucket, region);
        }
    }

    /** The body of {@link #connect}, run under that session's monitor. See {@link #connectLocks}. */
    private ConnectResult connectLocked(String sessionId, String bucket, String region) {
        String effectiveBucket = (bucket == null || bucket.isBlank()) ? defaultBucket : bucket.trim();
        String effectiveRegion = (region == null || region.isBlank()) ? defaultRegion : region.trim();
        if (effectiveBucket.isBlank()) {
            throw new IllegalArgumentException(
                    "A bucket is required to connect S3 storage. Pass one, or set app.storage.s3.bucket.");
        }

        BuiltStores previous = storesBySession.get(sessionId);
        String previousBucket = previous != null ? previous.bucket() : defaultBucket;
        String previousRegion = previous != null ? previous.region() : defaultRegion;

        Capabilities probed = probe(effectiveBucket, effectiveRegion, credentials.resolveFor(sessionId));
        if (!probed.ok()) {
            // Do not build: the user's previous target (if any) stays live, and the failure (with
            // its per-capability breakdown) is returned for the UI to show.
            log.warn("[storage] capability probe failed for session {} on s3://{} (region {}): {}",
                    stateLabel(sessionId), effectiveBucket,
                    effectiveRegion.isBlank() ? "default" : effectiveRegion, probed.error());
            publishAudit(SsoAuditEvent.SWAP_DENIED, sessionId, effectiveBucket, effectiveRegion, probed);
            return new ConnectResult(previousBucket, previousRegion, 0, probed);
        }

        ArtifactStore parser = build(PARSER, effectiveBucket, effectiveRegion, sessionId);
        ArtifactStore jar = build(JAR, effectiveBucket, effectiveRegion, sessionId);
        int built = (parser == null ? 0 : 1) + (jar == null ? 0 : 1);
        BuiltStores stores = new BuiltStores(parser, jar, effectiveBucket, effectiveRegion, probed);
        storesBySession.put(sessionId, stores);
        log.info("[storage] session {} connected to s3://{}/ (region {}) across {} store(s); "
                        + "probe verified read/upload/write",
                stateLabel(sessionId), effectiveBucket,
                effectiveRegion.isBlank() ? "default" : effectiveRegion, built);
        publishAudit(SsoAuditEvent.CONNECT, sessionId, effectiveBucket, effectiveRegion, probed);
        return new ConnectResult(effectiveBucket, effectiveRegion, built, probed);
    }

    /**
     * Runs the exact probe {@link #connect} gates on — {@code listObjectsV2}, {@code putObject} and
     * {@code deleteObject} under the reserved probe prefix, signed with this user's credentials —
     * but <b>builds nothing and swaps nothing</b>. It neither caches stores nor publishes a
     * CONNECT/SWAP-DENIED audit event, so the user's current target is untouched and no history is
     * written for a mere test.
     *
     * <p>This backs the modal's "Test access" button: the same three capabilities the user is about
     * to rely on, checked independently of the connect that follows.
     */
    public Capabilities testCapabilities(String sessionId, String bucket, String region) {
        if (sessionId == null || sessionId.isBlank()) {
            throw new NotAuthenticatedException();
        }
        String effectiveBucket = (bucket == null || bucket.isBlank()) ? defaultBucket : bucket.trim();
        String effectiveRegion = (region == null || region.isBlank()) ? defaultRegion : region.trim();
        if (effectiveBucket.isBlank()) {
            throw new IllegalArgumentException(
                    "A bucket is required to test S3 access. Pass one, or set app.storage.s3.bucket.");
        }
        return probe(effectiveBucket, effectiveRegion, credentials.resolveFor(sessionId));
    }

    /** The stores {@code sessionId} connected, or {@code null} when that user has none. */
    public BuiltStores storesFor(String sessionId) {
        return sessionId == null ? null : storesBySession.get(sessionId);
    }

    /**
     * Reconnects a session that has just completed login to the target it previously verified,
     * <b>without any user action</b> — the fix for a pod redeploy wiping the in-memory binding while
     * the user's SSO login silently re-establishes.
     *
     * <p>The saved target is read from the durable per-identity config (by identity, never by session)
     * and passed here; this method runs the <em>same</em> connect path a manual connect does, so the
     * three-step capability probe still gates the build. A probe failure changes nothing and the user
     * falls back to the manual connect, exactly as today.
     *
     * <p>Idempotent and race-safe: if the session is already connected to the same bucket/region this
     * is a no-op, and the actual connect runs under this session's monitor (see {@link #connectLocks}),
     * so it can never double-build against a manual connect happening at the same instant.
     *
     * @return true when the session ends up connected to the saved target (or already was); false when
     *         there is nothing to do or the probe did not pass. Never throws.
     */
    public boolean autoReconnect(String sessionId, String bucket, String region) {
        if (sessionId == null || sessionId.isBlank() || bucket == null || bucket.isBlank()) {
            return false;
        }
        String effectiveBucket = bucket.trim();
        String effectiveRegion = (region == null || region.isBlank()) ? defaultRegion : region.trim();
        try {
            synchronized (lockFor(sessionId)) {
                BuiltStores current = storesBySession.get(sessionId);
                if (current != null
                        && effectiveBucket.equals(current.bucket())
                        && (effectiveRegion.isBlank() || effectiveRegion.equals(current.region()))) {
                    log.debug("[auto-reconnect] session {} already connected to s3://{} (region {}); "
                                    + "nothing to do",
                            stateLabel(sessionId), current.bucket(),
                            current.region().isBlank() ? "default" : current.region());
                    return true;
                }
                ConnectResult result = connectLocked(sessionId, effectiveBucket, effectiveRegion);
                if (result != null && result.ok()) {
                    // ONE INFO line per auto-reconnect, naming the masked session and the target only.
                    log.info("[auto-reconnect] session {} reconnected to s3://{} (region {}) from saved config",
                            stateLabel(sessionId), result.bucket(),
                            result.region().isBlank() ? "default" : result.region());
                    return true;
                }
                log.debug("[auto-reconnect] session {} could not reconnect to saved s3://{} "
                                + "(capability probe did not pass); manual connect required",
                        stateLabel(sessionId), effectiveBucket);
                return false;
            }
        } catch (RuntimeException e) {
            // Never let auto-reconnect fail a login; the user can always connect manually.
            log.debug("[auto-reconnect] session {} skipped ({})",
                    stateLabel(sessionId), e.getClass().getSimpleName());
            return false;
        }
    }

    /**
     * A store over the configured <b>default</b> bucket/region, signed with {@code sessionId}'s own
     * credentials, for the small per-identity config document that has to be readable <em>before</em>
     * a session has connected a bucket (see {@code UserConnectionStore}'s AWS-target persistence).
     *
     * <p>Returns {@code null} when there is no session, no default bucket, or no registered S3-backed
     * builder (local/Mongo modes, where there is no per-user S3 target to remember) — callers treat
     * {@code null} as "no durable target available" and degrade silently. Never throws.
     */
    public ArtifactStore sharedStoreFor(String name, String sessionId) {
        if (sessionId == null || sessionId.isBlank() || defaultBucket.isBlank()) return null;
        StoreBuilder builder = builders.get(name);
        if (builder == null) return null;
        try {
            return builder.build(defaultBucket, defaultRegion, credentials.resolveFor(sessionId));
        } catch (RuntimeException e) {
            log.debug("[storage] no shared {} store available for per-identity config ({})",
                    name, e.getClass().getSimpleName());
            return null;
        }
    }

    /**
     * Drops and forgets {@code sessionId}'s stores — called on logout so the user's S3 clients do
     * not linger. Safe for an unknown session.
     */
    public void forget(String sessionId) {
        if (sessionId == null) return;
        storesBySession.remove(sessionId);
        connectLocks.remove(sessionId);
    }

    /** The per-session monitor {@link #connect} and {@link #autoReconnect} serialise on. */
    private Object lockFor(String sessionId) {
        return connectLocks.computeIfAbsent(sessionId, k -> new Object());
    }

    /**
     * Snapshots the current request user's stores for hand-off to a background job, or {@code null}
     * when the thread has no session or that user never connected. Must be called on the request
     * thread; see {@link CapturedStores}.
     */
    public CapturedStores captureCurrent() {
        String sessionId = currentSession.currentSessionId();
        if (sessionId == null) return null;
        BuiltStores stores = storesBySession.get(sessionId);
        if (stores == null) return null;
        return new CapturedStores(sessionId, stores.parser(), stores.jar(),
                stores.bucket(), stores.region(), stores.capabilities());
    }

    /** Effective default bucket, used when a request has no session-bound target. */
    public String bucket() {
        return defaultBucket;
    }

    /** Effective default region, used when a request has no session-bound target. */
    public String region() {
        return defaultRegion;
    }

    private ArtifactStore build(String name, String bucket, String region, String sessionId) {
        StoreBuilder builder = builders.get(name);
        if (builder == null) return null;
        return builder.build(bucket, region, credentials.resolveFor(sessionId));
    }

    /**
     * Runs the three-step capability probe against {@code bucket} with a fresh {@link S3Client}
     * built for {@code region} and {@code credentials}.
     *
     * <p>Written so it cannot throw: any SDK or transport failure is folded into the returned
     * {@link Capabilities}, because a probe that cannot even connect is exactly the outcome the
     * caller needs reported rather than propagated. The probe object is best-effort deleted; if the
     * delete fails the object is left under {@link #PROBE_PREFIX}, which is why that prefix exists.
     *
     * @return per-capability outcomes plus a top-level {@code ok} and, when not ok, an error naming
     *         the first failed operation and its AWS error code. Never contains credentials.
     */
    private Capabilities probe(String bucket, String region, AwsCredentialsProvider credentials) {
        boolean read = false;
        boolean upload = false;
        boolean write = false;
        String firstError = null;
        try (S3Client s3 = probeClient(region, credentials)) {
            try {
                s3.listObjectsV2(b -> b.bucket(bucket).maxKeys(1));
                read = true;
            } catch (RuntimeException e) {
                firstError = describe("read (listObjectsV2)", e);
            }

            String key = PROBE_PREFIX + "write-test-" + System.currentTimeMillis() + ".txt";
            try {
                s3.putObject(b -> b.bucket(bucket).key(key), RequestBody.fromString(PROBE_CONTENT));
                upload = true;
            } catch (RuntimeException e) {
                if (firstError == null) firstError = describe("upload (putObject)", e);
            }

            // Only attempt the delete if the object exists to delete; otherwise a 204 from a
            // no-op delete would falsely report write access on a bucket we cannot upload to.
            if (upload) {
                try {
                    s3.deleteObject(b -> b.bucket(bucket).key(key));
                    write = true;
                } catch (RuntimeException e) {
                    if (firstError == null) firstError = describe("write/delete (deleteObject)", e);
                }
            }
        } catch (RuntimeException e) {
            // Client construction or close failed — there is nothing to report but the reason.
            if (firstError == null) firstError = describe("connect to S3", e);
        }
        boolean ok = read && upload && write;
        if (!ok && firstError == null) {
            firstError = "Bucket capability probe did not pass read/upload/write; no AWS error was reported.";
        }
        return new Capabilities(read, upload, write, ok, ok ? null : firstError);
    }

    /**
     * Builds the one throwaway client the probe signs with. It uses the supplied per-user credential
     * provider, so the probe tests the credentials that will actually serve the bucket.
     */
    private S3Client probeClient(String region, AwsCredentialsProvider credentials) {
        S3ClientBuilder b = S3Client.builder()
                .httpClientBuilder(ApacheHttpClient.builder())
                .credentialsProvider(credentials);
        if (region != null && !region.isBlank()) {
            b.region(Region.of(region.trim()));
        }
        return b.build();
    }

    /**
     * A one-line description of a failed operation, carrying the AWS error code (e.g.
     * {@code AccessDenied}) and HTTP status when the SDK exposed them.
     *
     * <p>Deliberately built only from the error <em>code</em> and numeric status — never the SDK
     * exception's message or request, which on some paths can echo request material. No credential,
     * token or account identifier appears in the result.
     */
    private static String describe(String operation, RuntimeException e) {
        String code = null;
        int status = -1;
        if (e instanceof AwsServiceException ase) {
            if (ase.awsErrorDetails() != null) code = ase.awsErrorDetails().errorCode();
            status = ase.statusCode();
        }
        StringBuilder sb = new StringBuilder(operation).append(" failed: ");
        sb.append(code != null && !code.isBlank() ? code : e.getClass().getSimpleName());
        if (status > 0) sb.append(" (HTTP ").append(status).append(')');
        return sb.toString();
    }

    /** Publishes a CONNECT/SWAP-DENIED audit event; a listener failure must not break the connect. */
    private void publishAudit(String type, String sessionId, String bucket, String region,
                              Capabilities capabilities) {
        try {
            events.publishEvent(new SsoAuditEvent(type, sessionId, registry.identity(sessionId),
                    bucket, region, capabilities, System.currentTimeMillis(),
                    registry.credentialSource(sessionId)));
        } catch (RuntimeException e) {
            log.debug("[aws-sso] {} audit event could not be published ({})",
                    type, e.getClass().getSimpleName());
        }
    }

    /** Short, non-reversible session suffix for log lines. */
    private static String stateLabel(String sessionId) {
        return sessionId.length() <= 8 ? "…" : "…" + sessionId.substring(sessionId.length() - 8);
    }

    /**
     * Outcome of a {@link #connect}.
     *
     * @param bucket       the bucket now in effect for that user, or the previous one when {@code ok}
     *                     is false
     * @param region       the region now in effect for that user, or the previous one when {@code ok}
     *                     is false
     * @param storesSwapped how many registered stores were built; {@code 0} when the probe failed
     * @param capabilities the probe outcome backing this result, always non-null
     */
    public record ConnectResult(String bucket, String region, int storesSwapped,
                                Capabilities capabilities) {
        /** True only when the probe passed every capability and the stores were built. */
        public boolean ok() {
            return capabilities != null && capabilities.ok();
        }
    }

    /**
     * What the connect probe found on the target bucket.
     *
     * @param read   {@code listObjectsV2} succeeded
     * @param upload {@code putObject} of the probe marker succeeded
     * @param write  {@code deleteObject} of that marker succeeded
     * @param ok     all three passed
     * @param error  human-readable name of the first failed operation plus its AWS error code, or
     *               {@code null} when {@code ok}. Never contains credentials.
     */
    public record Capabilities(boolean read, boolean upload, boolean write, boolean ok, String error) {
    }
}
