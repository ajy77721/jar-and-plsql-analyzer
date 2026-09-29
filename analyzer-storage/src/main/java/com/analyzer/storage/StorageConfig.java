package com.analyzer.storage;

import com.analyzer.storage.sso.CurrentHttpSession;
import com.analyzer.storage.sso.SessionRoutingArtifactStore;
import com.analyzer.storage.sso.SsoCredentialsResolver;
import com.analyzer.storage.sso.SsoSessionRegistry;
import com.analyzer.storage.sso.SsoStoreSwapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.nio.file.Path;

/**
 * Wires the two {@link ArtifactStore} beans — {@code parserArtifactStore} (PL/SQL parser) and
 * {@code jarArtifactStore} (JAR analyzer) — every component reads and writes through, and selects
 * which {@link AnalysisRepository} implementation answers queries.
 *
 * <pre>
 * app.storage.mode=local               # local | s3 | mongodb | hybrid — where the bytes live,
 *                                       # shared default for both features below
 * app.storage.parser.mode=             # optional override for the PL/SQL parser feature only;
 *                                       # falls back to app.storage.mode when unset
 * app.storage.jar.mode=                # optional override for the JAR analyzer feature only;
 *                                       # falls back to app.storage.mode when unset
 * app.storage.repository=artifact-scan # artifact-scan | mongo — what answers queries
 *
 * # mongodb mode / mongo repository (either or both):
 * app.storage.mongodb.uri=mongodb://localhost:27017
 * app.storage.mongodb.database=analyzer
 * app.storage.mongodb.prefix=data      # same default as app.storage.s3.prefix, deliberately
 * app.storage.mongodb.inline-threshold=1048576   # 0 = default 1 MiB; above this, GridFS
 *
 * # hybrid mode: both app.storage.s3.* and app.storage.mongodb.* above are read together (the
 * # same bucket/prefix/region/endpoint and uri/database/prefix/inline-threshold as s3 mode and
 * # mongodb mode use on their own), plus one extra property for the Mongo-vs-S3 split itself:
 * app.storage.hybrid.big-threshold=1048576   # 0 = default 1 MiB; at/above this, S3 instead of Mongo
 *
 * # housekeeping (MongoHousekeepingService) — orphaned GridFS payloads, abandoned runs and
 * # unreferenced repo-overflow/ blobs. On by default; inert outside mongodb/hybrid mode.
 * app.storage.mongodb.sweep.enabled=true
 * app.storage.mongodb.sweep.interval-ms=900000   # 15 minutes
 * app.storage.mongodb.sweep.min-age-ms=3600000   # 1 hour; must exceed the longest single write
 *
 * # disk read-through cache (on by default; wraps BOTH routed stores):
 * app.cache.disk.enabled=true
 * app.cache.disk.max-mb=2048                     # LRU-by-last-access cap under {data}/cache/store
 * </pre>
 *
 * <p>Two properties, because they are two capabilities (PLAN-00): {@code ArtifactStore} is a blob
 * interface and {@code AnalysisRepository} is a query interface, and PLAN-09's rollout needs to
 * move them one at a time.
 *
 * <p><b>Two {@code ArtifactStore} beans, not one.</b> The PL/SQL parser and the JAR analyzer are
 * independent features that happened to share a single bean and a single {@code app.storage.mode}
 * property; every consumer now depends on {@code @Qualifier("parserArtifactStore")} or
 * {@code @Qualifier("jarArtifactStore")} rather than the bare type. Both bean methods otherwise do
 * exactly what the single {@code artifactStore()} bean used to do — same {@link StorageMode#parse},
 * same switch, same {@link StorageFactory#create} call — parameterised only by which mode property
 * they resolve. {@code app.storage.parser.mode}/{@code app.storage.jar.mode} each fall back to the
 * shared {@code app.storage.mode} (via Spring's nested {@code ${a:${b:c}}} placeholder syntax, in
 * use since Spring 3.1 and unconditionally supported by this app's Spring Boot 3.2.5/Spring
 * Framework 6.1 baseline), so a deployment that never sets either new property keeps behaving
 * exactly as before — one property, one effective backend, for both features.
 */
@Configuration
public class StorageConfig {

    private static final Logger log = LoggerFactory.getLogger(StorageConfig.class);

    /**
     * The PL/SQL parser's {@link ArtifactStore}.
     *
     * <p>Rooted at {@code app.data-dir.base}, NOT {@code app.data-dir}. See {@link #jarArtifactStore}
     * for why the store spans the whole data directory rather than a feature-specific subtree.
     *
     * <p>{@code app.storage.parser.mode} defaults to {@code app.storage.mode} when unset, so every
     * existing single-mode deployment keeps working with zero migration.
     */
    @Bean("parserArtifactStore")
    public ArtifactStore parserArtifactStore(
            com.analyzer.storage.mongo.MongoConnection mongoConnection,
            SsoStoreSwapper ssoStoreSwapper,
            SsoCredentialsResolver ssoCredentialsResolver,
            SsoSessionRegistry ssoSessionRegistry,
            CurrentHttpSession currentHttpSession,
            @Value("${app.storage.parser.mode:${app.storage.mode:local}}") String modeProp,
            @Value("${app.data-dir.base:data}") String dataDir,
            @Value("${app.storage.s3.bucket:}") String bucket,
            @Value("${app.storage.s3.prefix:data}") String prefix,
            @Value("${app.storage.s3.region:}") String region,
            @Value("${app.storage.s3.endpoint:}") String endpoint,
            @Value("${app.storage.mongodb.uri:}") String mongoUri,
            @Value("${app.storage.mongodb.database:}") String mongoDatabase,
            @Value("${app.storage.mongodb.prefix:data}") String mongoPrefix,
            @Value("${app.storage.mongodb.inline-threshold:0}") int inlineThreshold,
            @Value("${app.storage.hybrid.big-threshold:0}") long hybridBigThreshold,
            @Value("${app.storage.hybrid.s3-always-paths:}") String s3AlwaysPaths,
            @Value("${app.storage.s3.write-behind:false}") boolean s3WriteBehind,
            @Value("${app.cache.disk.enabled:true}") boolean cacheEnabled,
            @Value("${app.cache.disk.max-mb:2048}") long cacheMaxMb) {
        return buildAndRegister("parser", mongoConnection, ssoStoreSwapper, ssoCredentialsResolver,
                ssoSessionRegistry, currentHttpSession,
                modeProp, dataDir, bucket, prefix, region, endpoint, mongoUri, mongoDatabase,
                mongoPrefix, inlineThreshold, hybridBigThreshold, s3AlwaysPaths, s3WriteBehind,
                cacheEnabled, cacheMaxMb);
    }

    /**
     * The JAR analyzer's {@link ArtifactStore}.
     *
     * <p>DirPropertyResolver rewrites {@code app.data-dir} to {@code {base}/jar} before injection
     * because the JAR services expect their own subtree as root. This store spans the WHOLE data
     * directory — {@code jar/}, {@code plsql-parse/}, {@code config/} — so it must sit above that
     * split, or every jar key would land at {@code data/jar/jar/...}.
     *
     * <p>{@code app.storage.jar.mode} defaults to {@code app.storage.mode} when unset, so every
     * existing single-mode deployment keeps working with zero migration.
     */
    @Bean("jarArtifactStore")
    public ArtifactStore jarArtifactStore(
            com.analyzer.storage.mongo.MongoConnection mongoConnection,
            SsoStoreSwapper ssoStoreSwapper,
            SsoCredentialsResolver ssoCredentialsResolver,
            SsoSessionRegistry ssoSessionRegistry,
            CurrentHttpSession currentHttpSession,
            @Value("${app.storage.jar.mode:${app.storage.mode:local}}") String modeProp,
            @Value("${app.data-dir.base:data}") String dataDir,
            @Value("${app.storage.s3.bucket:}") String bucket,
            @Value("${app.storage.s3.prefix:data}") String prefix,
            @Value("${app.storage.s3.region:}") String region,
            @Value("${app.storage.s3.endpoint:}") String endpoint,
            @Value("${app.storage.mongodb.uri:}") String mongoUri,
            @Value("${app.storage.mongodb.database:}") String mongoDatabase,
            @Value("${app.storage.mongodb.prefix:data}") String mongoPrefix,
            @Value("${app.storage.mongodb.inline-threshold:0}") int inlineThreshold,
            @Value("${app.storage.hybrid.big-threshold:0}") long hybridBigThreshold,
            @Value("${app.storage.hybrid.s3-always-paths:}") String s3AlwaysPaths,
            @Value("${app.storage.s3.write-behind:false}") boolean s3WriteBehind,
            @Value("${app.cache.disk.enabled:true}") boolean cacheEnabled,
            @Value("${app.cache.disk.max-mb:2048}") long cacheMaxMb) {
        return buildAndRegister("jar", mongoConnection, ssoStoreSwapper, ssoCredentialsResolver,
                ssoSessionRegistry, currentHttpSession,
                modeProp, dataDir, bucket, prefix, region, endpoint, mongoUri, mongoDatabase,
                mongoPrefix, inlineThreshold, hybridBigThreshold, s3AlwaysPaths, s3WriteBehind,
                cacheEnabled, cacheMaxMb);
    }

    /**
     * Builds a store and, for an S3-backed mode, wraps it in the per-session routing façade and
     * registers a per-user builder so a connect can build that user's own store for their bucket.
     *
     * <p>Only S3/HYBRID stores are wrapped and registered: a LOCAL or pure-MONGODB store has no
     * bucket, so there is nothing for a connect to change. Registering the builder here (rather than
     * duplicating the mode/prefix/threshold logic in {@code SsoStoreSwapper}) keeps the knowledge of
     * how a store is constructed in exactly one place.
     *
     * <p>The disk cache wraps the <b>outermost</b> store — for S3/HYBRID that is the per-session
     * router, so one cache serves a feature uniformly whether a request resolves to a user's own
     * store or the startup store. The cache sits above routing, so every read and write the router
     * would have served goes through it. See {@link CachingArtifactStore}.
     */
    private ArtifactStore buildAndRegister(
            String name, com.analyzer.storage.mongo.MongoConnection mongoConnection,
            SsoStoreSwapper ssoStoreSwapper, SsoCredentialsResolver ssoCredentialsResolver,
            SsoSessionRegistry ssoSessionRegistry, CurrentHttpSession currentHttpSession,
            String modeProp, String dataDir, String bucket, String prefix, String region,
            String endpoint, String mongoUri, String mongoDatabase, String mongoPrefix,
            int inlineThreshold, long hybridBigThreshold, String s3AlwaysPaths,
            boolean s3WriteBehind, boolean cacheEnabled, long cacheMaxMb) {
        // The startup store: what a request with no session-bound state reads and writes through,
        // signed with the credential resolver (which answers the default chain when there is no
        // session). See SessionRoutingArtifactStore for the routing rule.
        ArtifactStore store = buildArtifactStore(mongoConnection, ssoCredentialsResolver, modeProp,
                dataDir, bucket, prefix, region, endpoint, mongoUri, mongoDatabase, mongoPrefix,
                inlineThreshold, hybridBigThreshold, s3AlwaysPaths, s3WriteBehind);
        StorageMode mode = StorageMode.parse(modeProp);
        ArtifactStore effective = store;
        if (mode == StorageMode.S3 || mode == StorageMode.HYBRID) {
            // Register how to build THIS bean's store for an arbitrary bucket/region, using the
            // per-user credential provider the swapper passes in, so a connect changes where the
            // bytes go (and who signs for them) and nothing else. The builder calls
            // buildArtifactStore directly (not buildAndRegister), so it neither re-registers nor
            // recurses.
            ssoStoreSwapper.register(name, (newBucket, newRegion, credentials) ->
                    buildArtifactStore(mongoConnection, credentials, modeProp, dataDir,
                            newBucket, prefix, newRegion, endpoint, mongoUri, mongoDatabase, mongoPrefix,
                            inlineThreshold, hybridBigThreshold, s3AlwaysPaths, s3WriteBehind));
            // The bean handed to consumers is the per-session router, not the startup store: it
            // forwards to the calling user's own store once they have connected, and to `store` only
            // when the request has no session-bound state.
            effective = new SessionRoutingArtifactStore(name, store, ssoStoreSwapper,
                    ssoSessionRegistry, currentHttpSession);
        }
        if (!cacheEnabled) {
            log.info("[storage] {}: disk cache disabled (app.cache.disk.enabled=false)", name);
            return effective;
        }
        // {app.data-dir}/cache/store/ — local disk only, never heap. One shared root; the store name
        // is the first segment, so parser and jar entries never collide.
        Path cacheRoot = Path.of(dataDir).resolve("cache").resolve("store");
        long maxBytes = cacheMaxMb <= 0 ? -1 : cacheMaxMb * 1024L * 1024L;
        log.info("[storage] {}: disk cache at {} (max {} MB, 24h revalidation)",
                name, cacheRoot.resolve(name), cacheMaxMb);
        return new CachingArtifactStore(name, effective, cacheRoot, maxBytes);
    }

    /**
     * Shared construction logic for both {@link #parserArtifactStore} and {@link #jarArtifactStore}
     * — identical to the single {@code artifactStore()} bean this replaced, modulo which mode
     * property resolved {@code modeProp}.
     *
     * <p>The credential provider is handed to {@link StorageFactory} for S3/HYBRID modes. For the
     * startup store that is {@link SsoCredentialsResolver}, which answers the current request's SSO
     * role and falls back to the default chain; for a per-user store built by a connect it is the
     * provider fixed to that one user. It is unused in local/mongodb modes, matching the "no
     * credentials in properties" guarantee below.
     */
    private ArtifactStore buildArtifactStore(
            com.analyzer.storage.mongo.MongoConnection mongoConnection,
            AwsCredentialsProvider credentialsProvider,
            String modeProp, String dataDir, String bucket, String prefix, String region,
            String endpoint, String mongoUri, String mongoDatabase, String mongoPrefix,
            int inlineThreshold, long hybridBigThreshold, String s3AlwaysPaths,
            boolean s3WriteBehind) {
        StorageMode mode = StorageMode.parse(modeProp);
        // Credentials are NOT read from properties: S3 uses the default AWS provider chain, and
        // MongoDB takes authentication from the operator-supplied URI (redacted before logging) —
        // so no secret lands in a config file this application writes or serves. There is
        // deliberately no app.storage.mongodb.username/password pair.
        ArtifactStore store = StorageFactory.create(switch (mode) {
            case LOCAL -> StorageFactory.Settings.local(Path.of(dataDir));
            case S3 -> StorageFactory.Settings.s3(bucket, prefix, region, endpoint);
            case MONGODB -> StorageFactory.Settings.mongodb(
                    mongoUri, mongoDatabase, mongoPrefix, inlineThreshold);
            // Both the S3 fields and the Mongo fields are populated at once here — the one mode
            // where that is intentional rather than the mode-specific fields simply being unused.
            // mongoPrefix (not the S3 prefix) is reused for both halves, matching the "identical
            // prefixes need no key rewriting" reasoning app.storage.mongodb.prefix already relies
            // on against app.storage.s3.prefix.
            case HYBRID -> StorageFactory.Settings.hybrid(
                    bucket, mongoPrefix, region, endpoint, mongoUri, mongoDatabase,
                    inlineThreshold, hybridBigThreshold);
        }, mongoConnection, credentialsProvider);
        // Path-based S3 routing (hybrid only): keys carrying one of the listed path segments —
        // e.g. the parser's `sources/` SQL files — go to S3 regardless of size, so the size
        // threshold keeps applying only to everything else. Empty/unset means no routing.
        if (mode == StorageMode.HYBRID && s3AlwaysPaths != null && !s3AlwaysPaths.isBlank()) {
            java.util.Set<String> segments = java.util.Arrays.stream(s3AlwaysPaths.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(java.util.stream.Collectors.toUnmodifiableSet());
            log.info("[storage] hybrid path routing: writes under segments {} always go to S3", segments);
            store = new S3AlwaysPathsStore(store, segments);
        }
        // Write-behind (S3 only): small writes buffer on close and upload on a bounded pool, so a
        // few-hundred-object analysis stops paying one remote round trip per artifact, serialized.
        // Read semantics are unchanged — every read-shaped call drains pending uploads first (see
        // WriteBehindStore). Hybrid is left alone: its small writes already go to local MongoDB.
        if (mode == StorageMode.S3 && s3WriteBehind) {
            // The upload pool threads have no request context of their own, so the write-behind
            // must capture the writer's credentials at write time and re-install them on the flush
            // thread — see CapturedSsoContext. Only the startup store (signed by the shared
            // resolver) needs this: a per-user store's client already carries fixed credentials.
            SsoCredentialsResolver resolver =
                    credentialsProvider instanceof SsoCredentialsResolver r ? r : null;
            log.info("[storage] S3 write-behind enabled (bounded upload pool, read barriers on, "
                    + "caller context carried to flush threads: {})", resolver != null);
            store = new WriteBehindStore(store, WriteBehindStore.DEFAULT_THREADS, resolver);
        }
        return store;
    }

    /**
     * The single MongoDB connection pool, or the unconfigured instance when Mongo is not in use.
     *
     * <p>Opened here rather than inside either consumer because {@code app.storage.mode} (or its
     * per-feature {@code app.storage.parser.mode}/{@code app.storage.jar.mode} overrides) and
     * {@code app.storage.repository} select the store(s) and the repository <b>independently</b> —
     * the whole reason those are separate properties (PLAN-09 §5: Increment F is {@code mode=s3}
     * with {@code repository=mongo}, so rollback is a property change rather than a restore). Any
     * of them may want Mongo; a driver client is a pool of sockets and monitor threads, so wanting
     * it from multiple places must not mean opening multiple pools — which is exactly why
     * {@link #parserArtifactStore} and {@link #jarArtifactStore} are both handed this same bean and
     * both reuse it (via {@code StorageFactory.create(settings, mongoConnection)}) whenever their
     * own resolved mode is {@code mongodb}/{@code hybrid}.
     *
     * <p>Always a bean, {@code configured() == false} in {@code local}/{@code s3} with the scan
     * repository. A bean that is simply absent in the default configuration would turn "Mongo is not
     * in use" into an unsatisfied-dependency failure at every injection point, which is a confusing
     * error for the configuration every existing deployment runs.
     *
     * <p>{@code destroyMethod} returns the pool on shutdown, and is a no-op for the unconfigured
     * instance.
     */
    @Bean(destroyMethod = "close")
    public com.analyzer.storage.mongo.MongoConnection mongoConnection(
            @Value("${app.storage.parser.mode:${app.storage.mode:local}}") String parserModeProp,
            @Value("${app.storage.jar.mode:${app.storage.mode:local}}") String jarModeProp,
            @Value("${app.storage.repository:artifact-scan}") String repositoryProp,
            @Value("${app.storage.mongodb.uri:}") String uri,
            @Value("${app.storage.mongodb.database:}") String database) {
        boolean storeWantsMongo = wantsMongo(parserModeProp) || wantsMongo(jarModeProp);
        boolean repoWantsMongo = com.analyzer.storage.repo.RepositoryMode.parse(repositoryProp)
                == com.analyzer.storage.repo.RepositoryMode.MONGO;
        if (!storeWantsMongo && !repoWantsMongo) {
            return com.analyzer.storage.mongo.MongoConnection.unconfigured();
        }
        // Pings before returning, so an unreachable server fails the context at boot with the
        // property to fix rather than on the first analysis (PLAN-09 §5, §8 R1).
        return com.analyzer.storage.mongo.MongoConnection.open(uri, database);
    }

    private static boolean wantsMongo(String modeProp) {
        StorageMode mode = StorageMode.parse(modeProp);
        return mode == StorageMode.MONGODB || mode == StorageMode.HYBRID;
    }

    /**
     * The selected repository implementation, parsed and logged once at startup.
     *
     * <p>Exposed as a bean rather than read with {@code @Value} at each use site so an unknown
     * value fails the context at boot with the valid list, instead of being discovered on the
     * first request to whichever screen happens to be opened first — PLAN-09 §8 R1 is a green test
     * suite hiding a startup failure, and a silently-defaulted storage property is one way to get
     * one.
     *
     * <p>The implementation itself is constructed where its collaborators live: the
     * {@code artifact-scan} implementation delegates to the readers that already parse these
     * artifacts, and those readers are in {@code unified-web}. Re-implementing them here would put
     * a second parser on the same bytes, which is the one thing that can make a response differ
     * without any test noticing. So this module owns the interface and the selection; the wiring
     * switch sits next to the implementations and has exactly one arm per value of this enum.
     */
    @Bean
    public com.analyzer.storage.repo.RepositoryMode repositoryMode(
            @Value("${app.storage.repository:artifact-scan}") String repositoryProp) {
        com.analyzer.storage.repo.RepositoryMode mode =
                com.analyzer.storage.repo.RepositoryMode.parse(repositoryProp);
        log.info("[storage] repository={} ({})", mode.wireName(),
                mode == com.analyzer.storage.repo.RepositoryMode.ARTIFACT_SCAN
                        ? "queries answered by scanning today's JSON artifacts"
                        : "queries answered from MongoDB indexes");
        return mode;
    }

    /**
     * The operator handle onto the disk cache, so the admin portal and the ops script can report
     * and clear it without knowing where either store put it.
     *
     * <p>The path is {@code {app.data-dir}/cache/store/} — the same root {@link CachingArtifactStore}
     * derives — computed here once from the already-resolved {@code app.data-dir.base}. A single bean
     * rather than one per feature because clearing the cache is a deployment-wide action and both
     * features share the root.
     */
    @Bean
    public DiskCacheAdmin diskCacheAdmin(@Value("${app.data-dir.base:data}") String dataDir) {
        return new DiskCacheAdmin(Path.of(dataDir).resolve("cache").resolve("store"));
    }
}
