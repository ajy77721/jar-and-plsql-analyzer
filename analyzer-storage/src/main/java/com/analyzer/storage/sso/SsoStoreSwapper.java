package com.analyzer.storage.sso;

import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.SwappableArtifactStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

/**
 * Re-points the already-wired S3-backed {@link ArtifactStore} beans at a user-supplied bucket and
 * region after a successful AWS SSO login.
 *
 * <h2>What actually has to change at connect time</h2>
 *
 * <p>Two things determine which bytes an S3 store touches: the bucket/region baked into its
 * {@code S3Client}, and the credentials that client signs with. The credentials half needs <em>no
 * swap</em>: {@link SsoCredentialsResolver} is a single Spring bean passed into
 * {@code StorageFactory} when the store is first built, and it decides per-request whether to mint
 * user-SSO role credentials or fall through to the default AWS chain. The client therefore already
 * holds the right provider; "logging in" simply flips the resolver's answer. That leaves only the
 * bucket/region, which are constructor arguments and cannot change on a live client — so a connect
 * rebuilds the store against the new target and swaps it in behind the stable
 * {@link SwappableArtifactStore} façade every consumer already holds.
 *
 * <h2>Why a registry rather than the config calling us back</h2>
 *
 * <p>{@code StorageConfig} owns the two store beans and knows how to rebuild each one (it holds the
 * mode, prefix, endpoint, hybrid thresholds and the shared Mongo pool). Rather than duplicate that
 * knowledge here, each bean registers itself with this component at construction time, handing over
 * a rebuild function that closes over its own settings. This class stays ignorant of storage modes
 * and simply re-invokes the factory the config already trusts.
 *
 * <p>Only S3-backed beans register — a LOCAL or pure-MONGODB store has no bucket to re-point, and
 * registering it would only produce a confusing no-op rebuild. A connect with no registered
 * targets is therefore harmless, and the effective bucket/region are still reported so the UI can
 * show the configured defaults.
 */
@Component
public class SsoStoreSwapper {

    private static final Logger log = LoggerFactory.getLogger(SsoStoreSwapper.class);

    /**
     * One re-pointable bean: its stable façade plus the function that rebuilds a fresh store for a
     * given bucket/region. The function is supplied by {@code StorageConfig}, which owns every other
     * setting the rebuild needs.
     */
    public record Target(String name,
                         SwappableArtifactStore store,
                         BiFunction<String, String, ArtifactStore> rebuild) {
    }

    private final List<Target> targets = new ArrayList<>();

    /** Effective target, initialised from the configured defaults and replaced on each connect. */
    private volatile String bucket;
    private volatile String region;

    /**
     * @param defaultBucket {@code app.storage.s3.bucket} — what a connect with no explicit bucket
     *                      keeps, and what {@code /session} reports before any connect
     * @param defaultRegion {@code app.storage.s3.region} — same role for the region
     */
    public SsoStoreSwapper(@Value("${app.storage.s3.bucket:}") String defaultBucket,
                           @Value("${app.storage.s3.region:}") String defaultRegion) {
        this.bucket = defaultBucket == null ? "" : defaultBucket.trim();
        this.region = defaultRegion == null ? "" : defaultRegion.trim();
    }

    /**
     * Called once per S3-backed store bean during context startup. Not synchronised against
     * {@link #connect}: Spring publishes all singletons before any request can arrive, so
     * registration is always complete before a connect is possible.
     */
    public synchronized void register(String name, SwappableArtifactStore store,
                                      BiFunction<String, String, ArtifactStore> rebuild) {
        targets.add(new Target(name, store, rebuild));
        log.info("[storage] {} store registered as SSO-swappable", name);
    }

    /**
     * Rebuilds every registered store against {@code bucket}/{@code region} and swaps it in.
     *
     * <p>A blank bucket or region falls back to the configured default, so a UI that only lets the
     * user override one of the two does not accidentally clear the other. Each store is rebuilt
     * before anything is swapped: if any rebuild throws — most likely a misconfigured hybrid mode
     * with no Mongo URI — no store has been touched, so a failed connect leaves the previous target
     * fully intact rather than half-migrated.
     */
    public synchronized ConnectResult connect(String bucket, String region) {
        String effectiveBucket = (bucket == null || bucket.isBlank()) ? this.bucket : bucket.trim();
        String effectiveRegion = (region == null || region.isBlank()) ? this.region : region.trim();
        if (effectiveBucket.isBlank()) {
            throw new IllegalArgumentException(
                    "A bucket is required to connect S3 storage. Pass one, or set app.storage.s3.bucket.");
        }

        List<Target> snapshot = List.copyOf(targets);
        List<ArtifactStore> rebuilt = new ArrayList<>(snapshot.size());
        for (Target target : snapshot) {
            // Build first, swap after the whole list is built — see the atomicity note above.
            rebuilt.add(target.rebuild().apply(effectiveBucket, effectiveRegion));
        }
        for (int i = 0; i < snapshot.size(); i++) {
            snapshot.get(i).store().swapTo(rebuilt.get(i));
        }
        this.bucket = effectiveBucket;
        this.region = effectiveRegion;
        log.info("[storage] switched to s3://{}/ (region {}) across {} store(s)",
                effectiveBucket, effectiveRegion.isBlank() ? "default" : effectiveRegion, snapshot.size());
        return new ConnectResult(effectiveBucket, effectiveRegion, snapshot.size());
    }

    /** Effective bucket after the last connect, or the configured default before any connect. */
    public String bucket() {
        return bucket;
    }

    /** Effective region after the last connect, or the configured default before any connect. */
    public String region() {
        return region;
    }

    /** Outcome of a {@link #connect}: what the stores are now pointed at. */
    public record ConnectResult(String bucket, String region, int storesSwapped) {
    }
}
