package com.analyzer.storage.repo;

import java.util.Locale;

/**
 * Which {@link com.analyzer.storage.AnalysisRepository} implementation serves queries, from
 * {@code app.storage.repository}.
 *
 * <p><b>Separate from {@code app.storage.mode} on purpose.</b> The store and the repository are two
 * capabilities, not one (PLAN-00), and the rollout needs them to move independently: PLAN-09
 * Increment F is exactly "{@code mode=s3} with {@code repository=mongo}" — writes still go to the
 * bucket, so a divergence is reversible with zero loss, which is impossible if one property
 * decides both. Rollback is then a property change rather than a restore (C5).
 */
public enum RepositoryMode {

    /**
     * Read today's JSON artifacts through {@code ArtifactStore}. The default, and the only mode
     * that works in every deployment, because it is written against the frozen interface alone and
     * therefore cannot tell {@code local} from {@code s3}.
     */
    ARTIFACT_SCAN,

    /** Answer from MongoDB indexes. Arrives with PLAN-09 Increment D. */
    MONGO;

    /** Accepts the hyphenated wire spelling ({@code artifact-scan}) as well as the enum name. */
    public static RepositoryMode parse(String value) {
        if (value == null || value.isBlank()) return ARTIFACT_SCAN;
        String normalized = value.trim().toUpperCase(Locale.ROOT).replace('-', '_');
        try {
            return valueOf(normalized);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Unknown app.storage.repository '" + value + "'. Valid: artifact-scan, mongo");
        }
    }

    /** The wire spelling, for logs and for echoing a selection back. */
    public String wireName() {
        return name().toLowerCase(Locale.ROOT).replace('_', '-');
    }
}
