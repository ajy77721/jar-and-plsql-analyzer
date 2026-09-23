package com.codeanalyzer.ingest.mongo.write;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Updates;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import org.bson.Document;

import java.time.Instant;

/**
 * The run lifecycle: the only thing standing in for a transaction on this server.
 *
 * <h2>Why a marker rather than a transaction</h2>
 *
 * <p>The MongoDB instance here is <b>standalone</b> ({@code NoReplicationEnabled}), so
 * multi-document transactions are unavailable — a fact established against the running server, not
 * assumed. An ingest therefore cannot be atomic across collections, and a crash mid-run leaves
 * documents behind.
 *
 * <p>What can be made atomic is a <em>single</em> document. So a run is written
 * {@code IN_PROGRESS}, its data is ingested, and only then is the marker flipped to
 * {@code COMPLETE}. Every reader filters on {@code COMPLETE}, which makes a partial run
 * <b>invisible</b> rather than inconsistent. The distinction matters: invisible data can be
 * cleaned up later at leisure; inconsistent data that readers can see is a wrong answer.
 *
 * <h2>The ordering that is easy to get backwards</h2>
 *
 * <p>Publishing a new run means demoting the previous one. The partial unique index permits only
 * one {@code COMPLETE + isCurrent} run per entry point, so the old run must be marked
 * {@code SUPERSEDED} <b>before</b> the new one is published. Doing it the intuitive way round —
 * publish then demote — makes the index reject the publish, and the run that just succeeded
 * appears to have failed.
 */
public final class RunMarker {

    public static final String IN_PROGRESS = "IN_PROGRESS";
    public static final String COMPLETE    = "COMPLETE";
    public static final String SUPERSEDED  = "SUPERSEDED";
    public static final String FAILED      = "FAILED";

    private final MongoDatabase db;

    public RunMarker(MongoDatabase db) {
        this.db = db;
    }

    /**
     * Allocates the next version for an entry point.
     *
     * <p>The one place a value is generated rather than derived. Versions must be dense and
     * ordered, which no function of the content can provide — two runs of identical source are
     * legitimately different runs. {@code findOneAndUpdate($inc)} is atomic on a single document,
     * so this is safe even without transactions.
     */
    public int nextVersion(String dialect, String project, String env, String entryPoint) {
        String key = String.join("~", dialect, project, env, entryPoint);
        Document counter = db.getCollection("am_counter").findOneAndUpdate(
                Filters.eq("_id", key),
                Updates.inc("seq", 1),
                new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));
        return counter == null ? 1 : counter.getInteger("seq", 1);
    }

    /**
     * How long an {@code IN_PROGRESS} run may sit untouched before it is treated as abandoned.
     *
     * <p>{@code fail()} clears the marker on a graceful error, but a hard kill leaves it set with
     * no one to clear it. Without reclamation the entry point is locked forever and the only
     * recovery is editing the database — a worse failure than the concurrent-ingest waste this
     * guard exists to prevent, and one that arrives via an ordinary process kill.
     *
     * <p>Two hours: comfortably longer than any observed ingest, short enough that a crash does not
     * block the next scheduled run. The threshold is a guess about duration, so it is generous —
     * reclaiming a <em>live</em> ingest would resurrect the exact problem being solved.
     */
    public static final long ABANDONED_AFTER_MS = 2 * 60 * 60 * 1000L;

    /** Raised when another ingest of the same analysis is already running. */
    public static final class ConcurrentIngestException extends RuntimeException {
        public ConcurrentIngestException(String entryPoint) {
            super("An ingest of '" + entryPoint + "' is already in progress. Two concurrent ingests "
                    + "of one analysis both finish, and the second silently supersedes the first — "
                    + "one machine's work discarded with nothing but a SUPERSEDED marker to show "
                    + "for it. Wait for the running ingest, or fail it first.");
        }
    }

    /**
     * Writes the marker that makes an in-flight run invisible to readers.
     *
     * <p>Refused when another ingest of the same analysis is already in progress. The refusal comes
     * from a partial unique index rather than a read-then-write check, because a check has a window
     * between the two and concurrency is precisely what this guards against.
     */
    public void begin(Document run) {
        // The run is the one document whose _id IS its business key: it is not content-addressed,
        // because two runs of identical source are legitimately different runs.
        run.put("key", run.get("_id"));
        run.put("status", IN_PROGRESS);
        Instant startedAt = Instant.now();
        run.put("startedAt", startedAt.toString());
        // Also as epoch millis, because the string is what a human reads and NOT what a range
        // query can use. Instant.toString() omits the fractional part when nanos are exactly zero,
        // so "…T10:00:00Z" sorts lexicographically AFTER "…T10:00:00.500Z" — a later instant
        // comparing as earlier. Any reclamation driven by the string would misjudge exactly the
        // runs it exists to judge, rarely and unrepeatably.
        run.put("startedAtEpochMs", startedAt.toEpochMilli());
        run.put("committedAt", null);
        reclaimAbandoned(run);
        try {
            db.getCollection(Collections.RUN).replaceOne(
                    Filters.eq("_id", run.get("_id")), run,
                    new com.mongodb.client.model.ReplaceOptions().upsert(true));
        } catch (com.mongodb.MongoWriteException | com.mongodb.MongoWriteConcernException e) {
            if (isDuplicateKey(e)) {
                throw new ConcurrentIngestException(String.valueOf(run.get("entryPoint")));
            }
            throw e;
        }
    }

    /**
     * Fails any {@code IN_PROGRESS} run for this entry point that is too old to be alive.
     *
     * <p>Marked {@code FAILED} rather than deleted: the documents it wrote are still there, and a
     * marker saying the run was abandoned is what makes them explicable later. Deleting the marker
     * would leave orphaned data with nothing to attribute it to.
     */
    private void reclaimAbandoned(Document run) {
        long cutoff = Instant.now().toEpochMilli() - ABANDONED_AFTER_MS;
        db.getCollection(Collections.RUN).updateMany(
                Filters.and(
                        Filters.eq("dialect", run.get("dialect")),
                        Filters.eq("project", run.get("project")),
                        Filters.eq("environment", run.get("environment")),
                        Filters.eq("entryPoint", run.get("entryPoint")),
                        Filters.eq("status", IN_PROGRESS),
                        // Numeric comparison. A run written before this field existed has no
                        // startedAtEpochMs and is therefore never reclaimed automatically — safer
                        // than guessing its age from a string whose ordering cannot be trusted.
                        Filters.lt("startedAtEpochMs", cutoff)),
                Updates.combine(
                        Updates.set("status", FAILED),
                        Updates.set("isCurrent", false),
                        Updates.set("failureReason",
                                "abandoned: no progress for " + (ABANDONED_AFTER_MS / 60000)
                                        + " minutes; reclaimed so the analysis can be re-ingested"),
                        Updates.set("finishedAt", Instant.now().toString())));
    }

    /**
     * A duplicate-key rejection, as opposed to any other write failure.
     *
     * <p>Checked by error code rather than by message: the text varies between server versions and
     * a message match would quietly stop recognising the case it exists for.
     */
    private static boolean isDuplicateKey(RuntimeException e) {
        if (e instanceof com.mongodb.MongoWriteException w) {
            return w.getError().getCategory() == com.mongodb.ErrorCategory.DUPLICATE_KEY;
        }
        return e instanceof com.mongodb.MongoWriteConcernException;
    }

    /** Demotes the previous current run, then publishes this one. Order is load-bearing. */
    public void complete(String runId, String dialect, String project, String env, String entryPoint) {
        db.getCollection(Collections.RUN).updateMany(
                Filters.and(
                        Filters.eq("dialect", dialect), Filters.eq("project", project),
                        Filters.eq("environment", env), Filters.eq("entryPoint", entryPoint),
                        Filters.eq("isCurrent", true), Filters.ne("_id", runId)),
                Updates.combine(Updates.set("status", SUPERSEDED), Updates.set("isCurrent", false)));

        db.getCollection(Collections.RUN).updateOne(
                Filters.eq("_id", runId),
                Updates.combine(
                        Updates.set("status", COMPLETE),
                        Updates.set("isCurrent", true),
                        Updates.set("committedAt", Instant.now().toString()),
                        Updates.set("finishedAt", Instant.now().toString())));
    }

    /** Marks a run failed. The documents stay for diagnosis; no reader will see them. */
    public void fail(String runId, String reason) {
        db.getCollection(Collections.RUN).updateOne(
                Filters.eq("_id", runId),
                Updates.combine(
                        Updates.set("status", FAILED),
                        Updates.set("isCurrent", false),
                        Updates.set("failureReason", reason),
                        Updates.set("finishedAt", Instant.now().toString())));
    }
}
