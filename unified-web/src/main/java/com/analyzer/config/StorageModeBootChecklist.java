package com.analyzer.config;

import com.analyzer.storage.StorageMode;
import com.analyzer.storage.mongo.MongoArtifactStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Prints, once at startup, exactly what the active storage mode requires and whether each piece is
 * present — so "why is my S3 run failing?" is answered by one boot log block instead of a hunt
 * through {@code application.properties}, {@code StorageConfig}, {@code SsoStoreSwapper} and the
 * AWS docs.
 *
 * <p>This is the operator-facing half of the single-assignment switch: {@code app.storage.mode=X}
 * is meant to be the <em>only</em> thing you set, and this block proves that every layer cohered
 * around it — the bucket/region for {@code s3}, the URI/database for {@code mongodb}/{@code hybrid},
 * the SSO login properties for the user-scoped modes, the caches and the entity-ingest toggle.
 * Per-feature {@code app.storage.parser.mode}/{@code app.storage.jar.mode} are optional refinements
 * and are shown side by side with the shared mode so a divergence is visible rather than silent.
 *
 * <p><b>Severity.</b> A {@code REQUIRED} requirement that is missing is a boot failure
 * ({@link #run} throws). In practice the store beans fail first ({@code StorageFactory.validate}),
 * so this throw is the backstop that keeps the contract in one place. A {@code RECOMMENDED} miss is
 * a loud WARN — chiefly {@code app.storage.sso.start-url}, without which the login modal opens
 * empty and each user must type the Identity Center portal URL. Everything else is OPTIONAL and is
 * printed for confirmation.
 *
 * <p>The evaluation is a pure function ({@link #evaluate}) so the whole checklist is unit-testable
 * without booting Spring.
 */
@Component
public class StorageModeBootChecklist implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(StorageModeBootChecklist.class);

    /** How badly a requirement is needed. */
    public enum Severity { REQUIRED, RECOMMENDED, OPTIONAL }

    /** One property (or small property group) and whether the active mode needs it. */
    public record Requirement(String label, boolean present, Severity severity, String hint) {
    }

    /** The evaluated checklist: the modes in play, the requirements, and the resolved auth line. */
    public record Report(Set<StorageMode> modes, List<Requirement> requirements,
                         boolean authRequired, String authReason) {

        /** True when a REQUIRED requirement is missing — the caller must refuse to start. */
        public boolean criticalMissing() {
            return requirements.stream().anyMatch(r -> r.severity() == Severity.REQUIRED && !r.present());
        }

        /** True when a RECOMMENDED requirement is missing — a loud WARN, not a failure. */
        public boolean hasRecommendedMiss() {
            return requirements.stream()
                    .anyMatch(r -> r.severity() == Severity.RECOMMENDED && !r.present());
        }

        /** The single block an operator reads at boot. */
        public String render() {
            StringBuilder sb = new StringBuilder();
            sb.append("[storage] mode check — active mode(s): ")
              .append(modes.isEmpty() ? "local" : modes)
              .append("; auth required=").append(authRequired)
              .append(" (").append(authReason).append(")");
            for (Requirement r : requirements) {
                sb.append("\n[storage]   ")
                  .append(r.severity() == Severity.REQUIRED ? "requires: "
                          : r.severity() == Severity.RECOMMENDED ? "recommends: " : "optional: ")
                  .append(r.label()).append(' ')
                  .append(r.present() ? "✓" : "MISSING")
                  .append(r.present() || r.hint() == null || r.hint().isBlank() ? "" : " — " + r.hint());
            }
            sb.append("\n[storage] ")
              .append(criticalMissing() ? "MISSING REQUIRED PROPERTY — fix the line(s) marked MISSING above"
                      : hasRecommendedMiss() ? "OK, but a recommended property is missing (see WARN above)"
                      : "OK — all requirements present");
            return sb.toString();
        }
    }

    /** Raw inputs, all strings/booleans so the evaluation has no Spring dependency. */
    public record Facts(String sharedMode, String parserMode, String jarMode, String dataDir,
                        String bucket, String prefix, String region, String endpoint,
                        String ssoRegion, String ssoStartUrl,
                        String mongoUri, String mongoDatabase, String mongoPrefix,
                        int inlineThreshold, long bigThreshold, String s3AlwaysPaths,
                        boolean writeBehind, boolean cacheDiskEnabled, long cacheDiskMaxMb,
                        long cacheHeapMaxMb, boolean entityIngestEnabled,
                        String entityIngestUri, String entityIngestDatabase,
                        boolean sharedMongoConfigured, boolean authRequired, String authReason) {
    }

    /**
     * The whole checklist as a pure function: which modes are active, what each needs, and whether
     * each is present. No logging, no throwing — {@link #run} does both.
     */
    public static Report evaluate(Facts f) {
        Set<StorageMode> modes = new LinkedHashSet<>();
        modes.add(StorageMode.parse(f.parserMode()));
        modes.add(StorageMode.parse(f.jarMode()));
        List<Requirement> reqs = new ArrayList<>();

        boolean s3 = modes.contains(StorageMode.S3) || modes.contains(StorageMode.HYBRID);
        boolean mongo = modes.contains(StorageMode.MONGODB) || modes.contains(StorageMode.HYBRID);
        boolean local = modes.contains(StorageMode.LOCAL);

        if (local) {
            reqs.add(new Requirement("app.data-dir=" + f.dataDir(), true, Severity.OPTIONAL,
                    "local filesystem root (default: data)"));
        }
        if (s3) {
            reqs.add(new Requirement("app.storage.s3.bucket=" + show(f.bucket()), !blank(f.bucket()),
                    Severity.REQUIRED, "set app.storage.s3.bucket"));
            reqs.add(new Requirement("app.storage.s3.region=" + show(f.region()), !blank(f.region()),
                    Severity.RECOMMENDED, "set app.storage.s3.region (blank = AWS SDK default region)"));
            reqs.add(new Requirement("app.storage.s3.prefix=" + show(f.prefix()), true,
                    Severity.OPTIONAL, "default: data"));
            reqs.add(new Requirement("app.storage.s3.endpoint=" + show(f.endpoint()), true,
                    Severity.OPTIONAL, "blank = real S3; set for MinIO/LocalStack"));
            reqs.add(new Requirement("app.storage.s3.write-behind=" + f.writeBehind(), true,
                    Severity.OPTIONAL, "S3 only; ignored in hybrid"));
            reqs.add(new Requirement("app.storage.sso.start-url=" + show(f.ssoStartUrl()),
                    !blank(f.ssoStartUrl()), Severity.RECOMMENDED,
                    "set app.storage.sso.start-url so the login modal is prefilled"));
            reqs.add(new Requirement("app.storage.sso.region=" + show(f.ssoRegion()), !blank(f.ssoRegion()),
                    Severity.RECOMMENDED, "set app.storage.sso.region (defaults to s3.region, then us-east-1)"));
        }
        if (mongo) {
            reqs.add(new Requirement("app.storage.mongodb.uri=" + show(f.mongoUri()), !blank(f.mongoUri()),
                    Severity.REQUIRED, "set app.storage.mongodb.uri"));
            reqs.add(new Requirement("app.storage.mongodb.database=" + show(f.mongoDatabase()),
                    !blank(f.mongoDatabase()), Severity.REQUIRED,
                    "set app.storage.mongodb.database"));
            reqs.add(new Requirement("app.storage.mongodb.prefix=" + show(f.mongoPrefix()), true,
                    Severity.OPTIONAL, "default: data (same default as the S3 prefix)"));
            reqs.add(new Requirement("app.storage.mongodb.inline-threshold=" + f.inlineThreshold()
                    + " (0 = 1 MiB default, max " + MongoArtifactStore.MAX_INLINE_THRESHOLD + ")",
                    f.inlineThreshold() >= 0
                            && f.inlineThreshold() <= MongoArtifactStore.MAX_INLINE_THRESHOLD,
                    Severity.OPTIONAL, "must be 0.." + MongoArtifactStore.MAX_INLINE_THRESHOLD + " bytes"));
        }
        if (modes.contains(StorageMode.HYBRID)) {
            reqs.add(new Requirement("app.storage.hybrid.big-threshold=" + f.bigThreshold()
                    + " (0 = 1 MiB default)", f.bigThreshold() >= 0, Severity.OPTIONAL,
                    "at/above this size an artifact goes to S3 instead of Mongo"));
            reqs.add(new Requirement("app.storage.hybrid.s3-always-paths=" + show(f.s3AlwaysPaths()), true,
                    Severity.OPTIONAL, "comma-separated path segments always routed to S3"));
        }
        if (f.entityIngestEnabled()) {
            boolean resolvable = (!blank(f.entityIngestUri()) && !blank(f.entityIngestDatabase()))
                    || f.sharedMongoConfigured()
                    || (mongo && !blank(f.mongoUri()) && !blank(f.mongoDatabase()));
            reqs.add(new Requirement("app.storage.entity-ingest.mongo.uri/database"
                    + (resolvable ? " (resolved)" : ""), resolvable, Severity.REQUIRED,
                    "set the entity-ingest Mongo URI/database, or point a storage feature at mongodb"));
        }
        reqs.add(new Requirement("app.cache.disk.enabled=" + f.cacheDiskEnabled()
                + ", app.cache.disk.max-mb=" + f.cacheDiskMaxMb()
                + ", app.cache.heap.max-mb=" + f.cacheHeapMaxMb(), true, Severity.OPTIONAL,
                "read-through caches; both wrap every routed store"));

        return new Report(modes, reqs, f.authRequired(), f.authReason());
    }

    private final Facts facts;

    public StorageModeBootChecklist(
            @Value("${app.storage.mode:local}") String sharedMode,
            @Value("${app.storage.parser.mode:${app.storage.mode:local}}") String parserMode,
            @Value("${app.storage.jar.mode:${app.storage.mode:local}}") String jarMode,
            @Value("${app.data-dir.base:data}") String dataDir,
            @Value("${app.storage.s3.bucket:}") String bucket,
            @Value("${app.storage.s3.prefix:data}") String prefix,
            @Value("${app.storage.s3.region:}") String region,
            @Value("${app.storage.s3.endpoint:}") String endpoint,
            @Value("${app.storage.sso.region:}") String ssoRegion,
            @Value("${app.storage.sso.start-url:}") String ssoStartUrl,
            @Value("${app.storage.mongodb.uri:}") String mongoUri,
            @Value("${app.storage.mongodb.database:}") String mongoDatabase,
            @Value("${app.storage.mongodb.prefix:data}") String mongoPrefix,
            @Value("${app.storage.mongodb.inline-threshold:0}") int inlineThreshold,
            @Value("${app.storage.hybrid.big-threshold:0}") long bigThreshold,
            @Value("${app.storage.hybrid.s3-always-paths:}") String s3AlwaysPaths,
            @Value("${app.storage.s3.write-behind:false}") boolean writeBehind,
            @Value("${app.cache.disk.enabled:true}") boolean cacheDiskEnabled,
            @Value("${app.cache.disk.max-mb:2048}") long cacheDiskMaxMb,
            @Value("${app.cache.heap.max-mb:64}") long cacheHeapMaxMb,
            @Value("${app.storage.entity-ingest.enabled:false}") boolean entityIngestEnabled,
            @Value("${app.storage.entity-ingest.mongo.uri:}") String entityIngestUri,
            @Value("${app.storage.entity-ingest.mongo.database:}") String entityIngestDatabase,
            @Value("${app.storage.repository:artifact-scan}") String repository,
            @Value("${app.auth.required:}") String authRequiredProp,
            @Value("${app.auth.sso.required:}") String legacySsoRequiredProp) {
        boolean authRequired = com.analyzer.auth.ApiKeyAuthInterceptor.resolveAuthRequired(
                authRequiredProp, legacySsoRequiredProp, jarMode, parserMode);
        boolean sharedMongoConfigured = !blank(mongoUri) && !blank(mongoDatabase)
                && (wantsMongo(parserMode) || wantsMongo(jarMode)
                    || repository.trim().equalsIgnoreCase("mongo"));
        String authReason = authRequired
                ? (authRequiredProp != null && !authRequiredProp.isBlank()
                        ? "explicit app.auth.required" : "auto from s3/hybrid storage mode")
                : (authRequiredProp != null && !authRequiredProp.isBlank()
                        ? "explicit app.auth.required" : "no s3/hybrid feature — nothing user-scoped");
        this.facts = new Facts(sharedMode, parserMode, jarMode, dataDir, bucket, prefix, region,
                endpoint, ssoRegion, ssoStartUrl, mongoUri, mongoDatabase, mongoPrefix,
                inlineThreshold, bigThreshold, s3AlwaysPaths, writeBehind, cacheDiskEnabled,
                cacheDiskMaxMb, cacheHeapMaxMb, entityIngestEnabled, entityIngestUri,
                entityIngestDatabase, sharedMongoConfigured, authRequired, authReason);
    }

    @Override
    public void run(ApplicationArguments args) {
        Report report = evaluate(facts);
        log.info(report.render());
        if (report.hasRecommendedMiss()) {
            log.warn("[storage] a recommended property is missing (see the [storage] block above); "
                    + "the app will start, but the affected feature degrades");
        }
        if (report.criticalMissing()) {
            throw new IllegalStateException("Storage mode is missing a required property:\n" + report.render());
        }
    }

    private static boolean wantsMongo(String modeProp) {
        StorageMode m = StorageMode.parse(modeProp);
        return m == StorageMode.MONGODB || m == StorageMode.HYBRID;
    }

    private static boolean blank(String s) {
        return s == null || s.isBlank();
    }

    private static String show(String s) {
        return blank(s) ? "(unset)" : s;
    }
}
