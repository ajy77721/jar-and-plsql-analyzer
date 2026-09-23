package com.analyzer.storage;

/**
 * Where analysis output is stored. Four modes.
 *
 * <p>All four keep the same layout, the same key names and the same artifact contents — only the
 * medium differs. {@code data/jar/<id>/analysis.json} on local disk is
 * {@code s3://<bucket>/data/jar/<id>/analysis.json} in S3, and the document
 * {@code _id: "data/jar/<id>/analysis.json"} in MongoDB — and in {@code hybrid} mode it is one or
 * the other of those two, depending on size. Nothing else about the application changes, and the
 * UI cannot tell which is active.
 *
 * <p>The identical key string is not a coincidence: {@link ArtifactKey#s3Key(String)} produces it
 * and both remote backends reuse it verbatim, so {@code app.storage.s3.prefix} and
 * {@code app.storage.mongodb.prefix} both default to {@code data} and a mode flip needs no key
 * rewriting (PLAN-09 §5 — the property that makes rollback free).
 */
public enum StorageMode {

    /** Local filesystem under {@code app.data-dir.base}. Default; today's behaviour. */
    LOCAL,

    /** S3 bucket under the {@code data/} prefix. Object storage is the source of truth. */
    S3,

    /**
     * MongoDB: a manifest collection for small artifacts, GridFS for large ones. PLAN-07 §1.
     *
     * <p>For the deployment where Mongo is the only available storage — PLAN-06's stated exception
     * to "GridFS is not used". GridFS stays an implementation detail of
     * {@code MongoArtifactStore}; no caller and no entity document ever sees it.
     */
    MONGODB,

    /**
     * Both at once, split by size: small artifacts in MongoDB, big ones in S3. See
     * {@code com.analyzer.storage.hybrid.HybridArtifactStore} for the routing rule
     * ({@code app.storage.hybrid.big-threshold}, defaulting to the same 1 MiB as
     * {@code app.storage.mongodb.inline-threshold}) and how a rewrite that crosses the threshold in
     * either direction is kept from leaving a stale copy in the backend it left behind.
     */
    HYBRID;

    public static StorageMode parse(String s) {
        if (s == null || s.isBlank()) return LOCAL;
        return switch (s.trim().toLowerCase()) {
            case "local", "internal" -> LOCAL;   // "internal" accepted as a legacy alias
            case "s3", "external" -> S3;         // "external" accepted as a legacy alias
            case "mongodb", "mongo" -> MONGODB;  // "mongo" accepted, matching app.storage.repository
            case "hybrid", "mongo-s3" -> HYBRID; // "mongo-s3" accepted as a descriptive alias
            default -> throw new IllegalArgumentException(
                    "Unknown app.storage.mode '" + s + "'. Expected: local | s3 | mongodb | hybrid");
        };
    }
}
