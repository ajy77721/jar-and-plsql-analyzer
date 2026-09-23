package com.codeanalyzer.ingest.mongo.model;

import com.codeanalyzer.ingest.mongo.id.Escaping;
import com.codeanalyzer.ingest.mongo.id.Identity;
import org.bson.Document;

/**
 * Joins one entity to one run — the row that makes cross-run deduplication possible.
 *
 * <h2>Why membership is a separate collection</h2>
 *
 * <p>Without it, an entity has to carry its run inside itself, which means the same unchanged
 * procedure is stored once per run. Measured, that is the dominant cost at scale: ten runs of a
 * 100M-line codebase would write ~450M documents where most content never changed, and the archived
 * runs show three of five dialects re-running byte-identically.
 *
 * <p>Separating them inverts that. The entity is written once, keyed by its content hash, and each
 * run contributes one small membership row. Re-ingesting an unchanged analysis writes N membership
 * rows and <em>zero</em> entity documents.
 *
 * <h2>What membership carries that the entity cannot</h2>
 *
 * <p>Anything true of the entity <em>in this run</em> rather than of the entity itself — the run
 * reference, and where it was found. Those fields are excluded from the content hash precisely
 * because they vary per run; if they lived on the entity, its hash would change every run and the
 * deduplication would silently never fire. Nothing would break, storage would simply stay ten times
 * larger than it should be, which is the kind of failure nobody notices.
 *
 * <h2>Its own identity</h2>
 *
 * <p>A membership row is uniquely identified by the pair it joins, so its {@code _id} is
 * {@code runRef ~ entityRef}. Re-ingesting the same run upserts the same row rather than adding a
 * duplicate, which is what keeps ingest idempotent without a read-back.
 */
public final class RunMember {

    public static final String COLLECTION = "am_run_member";

    private RunMember() {}

    /**
     * @param runRef      the run's key
     * @param entityRef   the entity's {@code entityKey} — NOT its {@code _id}, because the hash
     *                    changes when content changes and a membership row must survive that
     * @param contentHash which version of the entity this run saw
     * @param collection  which collection the entity lives in, so a reader can resolve it
     */
    public static Document of(String runRef, String entityRef, String contentHash,
                              String collection, Document runScopedFacts) {
        Document d = runScopedFacts == null ? new Document() : new Document(runScopedFacts);
        String key = Escaping.join(runRef, entityRef);
        d.put("_id", key);
        d.put("key", key);
        d.put("runRef", runRef);
        d.put("entityRef", entityRef);
        d.put("contentHash", contentHash);
        d.put("entityCollection", collection);
        return d;
    }

    /** Convenience for an entity that has just been stamped. */
    public static Document of(String runRef, Identity identity, String collection,
                              Document runScopedFacts) {
        return of(runRef, identity.entityKey(), identity.id(), collection, runScopedFacts);
    }
}
