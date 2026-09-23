package com.analyzer.storage.mongo;

import java.util.concurrent.TimeUnit;
import com.analyzer.storage.AnalysisRepository;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.repo.AnalysisQuery;
import com.analyzer.storage.repo.AnalysisRef;
import com.analyzer.storage.repo.RowStream;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.function.Function;

/**
 * The {@code mongo} {@link AnalysisRepository}: PLAN-09 Increment D's read side.
 *
 * <p>Implements the same five methods {@code ArtifactScanRepository} implements, from MongoDB
 * collections instead of by scanning JSON artifacts. {@link #indexed()} returns {@code true}, which
 * is the interface's own stated way for a screen to know whether an answer costs milliseconds or
 * seconds — and the one assertion that distinguishes the two implementations in a conformance test
 * that otherwise requires them to agree exactly.
 *
 * <h2>How the collections get populated — read this before changing anything</h2>
 *
 * <p><b>By ingesting from the scan implementation on first touch, not by the analyzer writing
 * entities directly.</b> PLAN-09 §2.3 describes the eventual shape — the analyzer and the migrator
 * both producing through one mapper — but that mapper is Increment E's deliverable and it needs the
 * entity model of PLAN-04/05, which no code implements yet. Increment D is scoped to "a user can
 * run a new analysis into Mongo"; it is not scoped to rewriting the analyzer's write path.
 *
 * <p>So this class takes the scan implementation as its {@code source} and treats it as the
 * authority on <em>content</em> while owning <em>where the answer comes from</em>. A query whose run
 * is absent is answered from the source, and the rows are written to Mongo as they stream past;
 * every later query for the same run is served from the index. Three properties follow, and they are
 * why this shape was chosen over a stub:
 *
 * <ol>
 *   <li><b>C3 cannot break.</b> The bytes originate from the same readers the controllers use, so a
 *       response cannot differ from {@code artifact-scan}'s — the acceptance criterion Increment C
 *       established and Increment D must not spend.</li>
 *   <li><b>The write path is real</b>, so the 12 MB guard, the run marker and the bulk-insert
 *       batching are exercised by ordinary use rather than by a test that pretends. Increment E
 *       replaces the <em>producer</em> and reuses all of it.</li>
 *   <li><b>It degrades to correct.</b> With Mongo empty, every answer is still right, just not yet
 *       fast. A repository that returned nothing until a backfill ran would make {@code mongodb}
 *       mode look like data loss.</li>
 * </ol>
 *
 * <p>The honest cost: the <em>first</em> query for an analysis is a scan plus an insert, so it is
 * slower than {@code artifact-scan} rather than faster. Only the second onwards wins. That is
 * stated here rather than in a commit message because it is the number anyone measuring this will
 * hit first.
 *
 * <h2>The run marker, not a transaction</h2>
 *
 * <p>An ingest of a 5,000-node analysis writes 5,000 documents and can fail at 3,000. So a
 * {@code run} document is inserted with {@code status=IN_PROGRESS} before any child row, and flipped
 * to {@code COMPLETE} only once the last row is acknowledged; <b>every read filters on
 * {@code status=COMPLETE}</b>, so a partial run is invisible rather than corrupt. Object storage has
 * the same exposure today and handles it worse — a half-written analysis there is
 * indistinguishable from a whole one.
 *
 * <p>A multi-document transaction is rejected deliberately, per PLAN-06: it requires a replica set,
 * holds resources for the whole crawl, and risks the 16 MB oplog entry limit on large batches. The
 * marker gives the same visible atomicity on a standalone server — which is what this was developed
 * against, MongoDB 6.0.26 standalone, so the choice is validated rather than assumed.
 *
 * <h2>Why a row is stored as a JSON string and not as a BSON sub-document</h2>
 *
 * <p>This looks like the wrong choice and is the right one for this increment. Three reasons:
 *
 * <ol>
 *   <li><b>Byte-exactness.</b> A round trip through BSON is exactly where a numeric widening
 *       (int → long → double), a field reordering, or an absent-vs-null distinction changes. The
 *       acceptance criterion is that no response changes byte for byte, so the payload is stored as
 *       the bytes themselves.</li>
 *   <li><b>Field names.</b> BSON has historically rejected {@code .} and {@code $} in keys, and
 *       these documents carry real ones — fully-qualified class names, JSON pointers. The rules
 *       differ by server version, which is a portability cliff with no upside here.</li>
 *   <li><b>Nothing needs the fields yet.</b> No method on this interface filters, sorts or
 *       aggregates on a row's <em>contents</em> — {@code JsonFacetPager} owns that, deliberately, and
 *       it consumes {@link RowStream}. So field-level indexes would be built for no query. PLAN-08
 *       is where a screen's query arrives, and that is the moment to promote the fields it needs
 *       into indexed columns beside the payload rather than guessing now.</li>
 * </ol>
 *
 * <p>Stated plainly: <b>this is a document store used as an indexed row store, not yet as a query
 * engine.</b> It removes the repeated full scan, which is the cost PLAN-02 §10 measured. It does not
 * yet answer "which endpoints call table X" from an index, because the entity model that query needs
 * is PLAN-04/05's and is not built.
 */
public class MongoAnalysisRepository implements AnalysisRepository {

    private static final Logger log = LoggerFactory.getLogger(MongoAnalysisRepository.class);

    static final String C_RUNS = "analysis_run";
    static final String C_FACETS = "analysis_facet";
    static final String C_ROWS = "analysis_row";

    static final String IN_PROGRESS = "IN_PROGRESS";
    static final String COMPLETE = "COMPLETE";

    /**
     * Rows per bulk insert, sized by bytes rather than by count.
     *
     * <p>PLAN-09 §2.2: "a count-based batch of 1,000 works fine for 2 KB nodes and blows up on
     * 370 KB nodes" — the measured extremes are exactly that far apart, so the buffer is flushed on
     * accumulated payload bytes and the count is only a backstop against a pathological number of
     * tiny rows.
     */
    private static final int BATCH_TARGET_BYTES = 4 * 1024 * 1024;
    private static final int BATCH_MAX_ROWS = 2_000;

    private final MongoDatabase db;
    private final MongoCollection<Document> runs;
    private final MongoCollection<Document> facets;
    private final MongoCollection<Document> rows;
    private final ObjectMapper mapper;
    private final MongoDocumentGuard guard;
    private final AnalysisRepository source;

    /**
     * @param blobs  where an oversized payload is diverted. The credential-guarded store, so C4
     *               holds for the diversion too
     * @param source the scan implementation, consulted for a run that has not been ingested yet
     */
    public MongoAnalysisRepository(MongoDatabase db, ArtifactStore blobs, ObjectMapper mapper,
                                   AnalysisRepository source) {
        this(db, mapper, source, new MongoDocumentGuard(blobs));
    }

    /** @param guard overridable so a test can drive the diversion without 12 MB of input */
    public MongoAnalysisRepository(MongoDatabase db, ObjectMapper mapper,
                                   AnalysisRepository source, MongoDocumentGuard guard) {
        if (db == null) throw new IllegalArgumentException("mongo database is required");
        if (source == null) {
            throw new IllegalArgumentException(
                    "a source AnalysisRepository is required: the mongo repository ingests from it "
                    + "on first touch (PLAN-09 Increment D), so without one it can only answer for "
                    + "runs that happen to be present already");
        }
        this.db = db;
        this.runs = db.getCollection(C_RUNS);
        this.facets = db.getCollection(C_FACETS);
        this.rows = db.getCollection(C_ROWS);
        this.mapper = mapper == null ? new ObjectMapper() : mapper;
        this.source = source;
        this.guard = guard;
    }

    /**
     * Creates the indexes the five read methods use, and no others.
     *
     * <p>Speculative indexes are not free: each one slows every insert and the ingest path is
     * insert-heavy by construction. So there are exactly three, one per access pattern:
     * the run lookup, the facet lookup, and the row scan.
     */
    public void ensureIndexes() {
        try {
            // The run lookup. `table` is part of the run key (see runKey) and was missing from this
            // index, so completeRunId's filter was only partly index-covered — the fifth field is
            // the one the query actually equality-matches on most often.
            createIndex(runs, "run_lookup",
                    Indexes.ascending("dataset", "analysis", "version", "table", "status"),
                    new IndexOptions().name("run_lookup").background(true));

            // THE INVARIANT, made real. Two COMPLETE runs for one key is the ambiguity the whole
            // marker scheme exists to prevent, and until this index existed nothing prevented it:
            // two concurrent ingests of the same query each inserted IN_PROGRESS and each flipped
            // to COMPLETE, and the second one to finish retired the first *after* both were
            // visible. A plain unique index on the five fields including `status` cannot be used —
            // it would also forbid two concurrent IN_PROGRESS runs, which are legal and expected
            // (the loser's rows are garbage, not a conflict). So it is a PARTIAL unique index over
            // the four key fields, scoped to status=COMPLETE: any number of runs may be in flight,
            // exactly one may be published.
            //
            // The arbitration point therefore becomes the single-document update that flips the
            // marker: the winner's update succeeds, the loser's fails with a duplicate key and is
            // discarded (see publish). That is what makes the run-marker scheme correct under
            // concurrency without a transaction, which a standalone server cannot offer anyway.
            createIndex(runs, "run_one_complete",
                    Indexes.ascending("dataset", "analysis", "version", "table"),
                    new IndexOptions().name("run_one_complete").unique(true).background(true)
                            .partialFilterExpression(Filters.eq("status", COMPLETE)));

            createIndex(facets, "facet_lookup", Indexes.ascending("runId", "table"),
                    new IndexOptions().name("facet_lookup").background(true));
            // (runId, seq): the row scan is always "all rows of one run, in insertion order", so
            // this index serves both the filter and the sort and the scan never touches a document
            // out of order.
            createIndex(rows, "row_scan", Indexes.ascending("runId", "seq"),
                    new IndexOptions().name("row_scan").background(true));
            log.info("[repo] mongo repository indexes ensured on {}", db.getName());
        } catch (MongoException e) {
            log.warn("[repo] could not ensure repository indexes on {}: {}", db.getName(), e.getMessage());
        }
    }

    /**
     * Creates an index, replacing one of the same name whose specification has changed.
     *
     * <p>Needed because {@code run_lookup} shipped with a different key pattern. MongoDB rejects a
     * {@code createIndex} that reuses a name with a different spec
     * ({@code IndexKeySpecsConflict}), so a plain call would fail on every database created by the
     * previous build — and it would fail at boot, in the one code path PLAN-09 §8 R1 says must not
     * surprise an operator. Dropping and rebuilding is safe: these indexes carry no uniqueness that
     * data depends on except {@code run_one_complete}, and a duplicate that predates it is reported
     * below rather than silently tolerated.
     */
    private void createIndex(MongoCollection<Document> collection, String name,
                             Bson keys, IndexOptions options) {
        try {
            collection.createIndex(keys, options);
        } catch (MongoCommandException e) {
            // 85 IndexOptionsConflict, 86 IndexKeySpecsConflict.
            if (e.getErrorCode() != 85 && e.getErrorCode() != 86) throw e;
            log.info("[repo] index {}.{} has an outdated specification; rebuilding it",
                    collection.getNamespace().getCollectionName(), name);
            try {
                collection.dropIndex(name);
                collection.createIndex(keys, options);
            } catch (MongoException retry) {
                // The interesting case: a database that already holds two COMPLETE runs for one key
                // from before the constraint existed. Say so precisely — a warning about "could not
                // create an index" would send an operator looking for a permissions problem.
                log.warn("[repo] could not rebuild index {}.{}: {}. If this is {} the database "
                         + "already contains more than one COMPLETE run for a single "
                         + "(dataset, analysis, version, table) key from before this constraint "
                         + "existed; drop the {} collection to re-ingest, or remove the duplicates.",
                        collection.getNamespace().getCollectionName(), name, retry.getMessage(),
                        "run_one_complete", C_RUNS);
            }
        }
    }

    /**
     * True. The honest signal of which implementation answered — and, after the first touch of a
     * given analysis, an accurate one.
     */
    @Override
    public boolean indexed() {
        return true;
    }

    /* ─────────────────────────── run markers ─────────────────────────── */

    private static String versionOf(AnalysisQuery q) {
        // Normalised, never null: null and "" would otherwise be two distinct keys for
        // best-available, and the run for one would be invisible to a query written the other way.
        return q.version() == null ? "" : q.version();
    }

    private static String analysisOf(AnalysisQuery q) {
        return q.analysis() == null ? "" : q.analysis();
    }

    /**
     * The run key. <b>{@code table} is part of it</b>, and leaving it out was a real bug caught by
     * {@code tableScopedFacetsAreKeyedSeparately}: the two table-detail datasets are per-table, so
     * a run ingested for {@code EMP} would otherwise satisfy a query for {@code DEPT} and answer it
     * with a missing facet — the "analysis has no such facet" signal, for a table that has one.
     */
    private Bson runKey(AnalysisQuery q) {
        return Filters.and(
                Filters.eq("dataset", q.dataset().name()),
                Filters.eq("analysis", analysisOf(q)),
                Filters.eq("version", versionOf(q)),
                Filters.eq("table", q.table()));
    }

    private Bson completeRun(AnalysisQuery q) {
        return Filters.and(runKey(q), Filters.eq("status", COMPLETE));
    }

    /** @return the run id of the COMPLETE run for this query, or null when there is not one. */
    private String completeRunId(AnalysisQuery q) {
        try {
            Document d = QueryBudget.request(runs).find(completeRun(q)).projection(Projections.include("_id")).first();
            return d == null ? null : d.getString("_id");
        } catch (MongoException e) {
            log.warn("[repo] run lookup failed for {}: {}", q.dataset(), e.getMessage());
            return null;
        }
    }

    /**
     * Opens a run, ingests through {@code body}, and flips the marker to COMPLETE.
     *
     * <p>The marker is the unit of atomicity, so the ordering is not incidental: insert
     * {@code IN_PROGRESS}, write every child, then one single-document update to {@code COMPLETE}.
     * If {@code body} throws, the marker is left {@code IN_PROGRESS} and the run stays invisible to
     * every read; the rows it did write are garbage a sweeper can remove by age. It is deliberately
     * <b>not</b> rolled back inline: a failed ingest is often a failing Mongo, and a cleanup that
     * needs the database that just failed will fail too — leaving the marker is what makes the
     * outcome safe without needing anything to succeed.
     *
     * <p>A previous COMPLETE run for the same key is retired at the end rather than deleted first,
     * so a re-ingest never has a window where neither run is visible.
     */
    private String ingest(AnalysisQuery q, IngestBody body) throws IOException {
        String runId = UUID.randomUUID().toString();
        Document marker = new Document("_id", runId)
                .append("dataset", q.dataset().name())
                .append("analysis", analysisOf(q))
                .append("version", versionOf(q))
                .append("table", q.table())
                .append("status", IN_PROGRESS)
                .append("createdAt", new Date());
        try {
            runs.insertOne(marker);
            long written = body.run(runId);
            if (publish(q, runId, written)) {
                log.info("[repo] ingested {} for '{}' as run {} ({} rows)",
                        q.dataset(), analysisOf(q), runId, written);
            }
            return runId;
        } catch (MongoException e) {
            throw new IOException("MongoDB ingest of " + q.dataset() + " failed: " + e.getMessage(), e);
        }
    }

    /**
     * Flips a run's marker to COMPLETE, or discards it if another ingest published first.
     *
     * <p>This is the arbitration point of the whole scheme, and it is one single-document update —
     * which is atomic on a standalone server, where a transaction is not available. Two outcomes,
     * both correct:
     *
     * <ul>
     *   <li><b>Won.</b> The update lands, {@code run_one_complete} accepted it, this run is the one
     *       COMPLETE run for the key. Superseded runs are retired afterwards.</li>
     *   <li><b>Lost.</b> The unique partial index rejects the update with a duplicate key, because a
     *       concurrent ingest of the same query got there first. Nothing is wrong: the loser's rows
     *       are a second copy of the same content (both ingests read the same source), so the
     *       winner's run is a complete and correct answer. The loser's rows and marker are removed
     *       and <b>the caller is not told</b> — it has already been handed the rows it asked for,
     *       byte-identical to what the source produced, and an exception here would turn a
     *       successful read into a 500 for a race the user did not cause.</li>
     * </ul>
     *
     * <p>A third outcome is folded into "lost": {@code matchedCount == 0}, meaning this run's marker
     * is already gone — retired by the winner, or removed by the sweeper. Without this check the
     * update would silently match nothing and the ingest would report success for a run that no
     * longer exists.
     *
     * @return true when this run became the visible COMPLETE run
     */
    private boolean publish(AnalysisQuery q, String runId, long rowCount) {
        try {
            long matched = runs.updateOne(Filters.eq("_id", runId),
                    Updates.combine(Updates.set("status", COMPLETE),
                                    Updates.set("rowCount", rowCount),
                                    Updates.set("completedAt", new Date()))).getMatchedCount();
            if (matched == 0) {
                log.info("[repo] run {} for {} '{}' vanished before it could be published "
                         + "(retired or swept); discarding its {} row(s)",
                        runId, q.dataset(), analysisOf(q), rowCount);
                discardRun(runId);
                return false;
            }
        } catch (MongoWriteException | MongoWriteConcernException | MongoBulkWriteException e) {
            if (!isDuplicateKey(e)) throw e;
            log.info("[repo] concurrent ingest of {} '{}' already published a COMPLETE run; "
                     + "discarding this one ({}, {} rows). The published run has the same content.",
                    q.dataset(), analysisOf(q), runId, rowCount);
            discardRun(runId);
            return false;
        }
        retireSupersededRuns(q, runId);
        return true;
    }

    /** True when a write error is a unique-index violation (11000) rather than anything else. */
    private static boolean isDuplicateKey(MongoException e) {
        if (e instanceof MongoWriteException w) {
            return w.getError().getCategory() == ErrorCategory.DUPLICATE_KEY;
        }
        if (e instanceof MongoBulkWriteException b) {
            return b.getWriteErrors().stream()
                    .anyMatch(err -> ErrorCategory.fromErrorCode(err.getCode())
                            == ErrorCategory.DUPLICATE_KEY);
        }
        return e.getCode() == 11000;
    }

    /**
     * Removes a run that will never be published, with its children.
     *
     * <p>Children first, marker last — the same ordering {@link #retireSupersededRuns} uses and for
     * the same reason: a failure in between leaves rows with no marker, which the sweeper collects,
     * rather than a marker with no rows, which nothing would notice. Best-effort, because the
     * caller's read has already succeeded and a failure to reclaim space must not surface as one.
     */
    private void discardRun(String runId) {
        try {
            rows.deleteMany(Filters.eq("runId", runId));
            facets.deleteMany(Filters.eq("runId", runId));
            runs.deleteOne(Filters.eq("_id", runId));
        } catch (MongoException e) {
            log.warn("[repo] could not discard run {}: {}. The sweeper will collect it.",
                    runId, e.getMessage());
        }
    }

    /**
     * Removes older runs for the same key once the new one is COMPLETE.
     *
     * <p>Rows are removed before the marker, which is the same ordering {@link MongoArtifactStore}
     * uses for delete: the marker is what makes a run visible, so a crash in between leaves rows
     * with no marker (invisible garbage) rather than a marker with no rows (a run that reads as
     * empty). PLAN-06 notes a re-run gives history for free; retention of superseded runs is
     * PLAN-09 Increment G's decision, so this deletes rather than accumulating without a policy.
     *
     * <p><b>This can and does delete a concurrent ingest's in-flight run</b>, and that is safe only
     * because of the {@code matchedCount == 0} branch in {@link #publish}. The interleaving: two
     * ingests of one query run at once, the first to finish publishes and retires the second's
     * {@code IN_PROGRESS} marker and rows while it is still writing. The second then finds its own
     * marker gone, recognises it as a lost race and deletes whatever it wrote after the retire. Any
     * row that outlives the retire is therefore cleaned up by the loser itself, and the rows it
     * loses mid-write were never visible to anything — a run is only ever reachable through a
     * COMPLETE marker. Without that branch the loser's update would match nothing, report success,
     * and leave the caller believing an empty run had been published; that was the bug, not this
     * delete.
     */
    private void retireSupersededRuns(AnalysisQuery q, String keepRunId) {
        List<String> stale = new ArrayList<>();
        try (MongoCursor<Document> c = QueryBudget.request(runs).find(
                Filters.and(runKey(q), Filters.ne("_id", keepRunId)))
                .projection(Projections.include("_id")).iterator()) {
            while (c.hasNext()) stale.add(c.next().getString("_id"));
        }
        if (stale.isEmpty()) return;
        rows.deleteMany(Filters.in("runId", stale));
        facets.deleteMany(Filters.in("runId", stale));
        runs.deleteMany(Filters.in("_id", stale));
    }

    /**
     * Deletes runs left {@code IN_PROGRESS} by a failed ingest, along with their orphaned rows.
     *
     * <p>An operator action, not something a read path triggers, and it takes an age threshold
     * because a run that is still being written looks exactly like one that was abandoned. Nothing
     * here is a correctness issue — an {@code IN_PROGRESS} run is invisible to every read — so this
     * reclaims space and nothing more.
     */
    public int sweepIncompleteRuns(long olderThanMillis) {
        Date cutoff = new Date(System.currentTimeMillis() - Math.max(0, olderThanMillis));
        List<String> stale = new ArrayList<>();
        try (MongoCursor<Document> c = QueryBudget.request(runs).find(Filters.and(
                Filters.eq("status", IN_PROGRESS),
                Filters.lt("createdAt", cutoff))).projection(Projections.include("_id")).iterator()) {
            while (c.hasNext()) stale.add(c.next().getString("_id"));
        }
        int swept = stale.size();
        if (!stale.isEmpty()) {
            rows.deleteMany(Filters.in("runId", stale));
            facets.deleteMany(Filters.in("runId", stale));
            runs.deleteMany(Filters.in("_id", stale));
        }
        swept += sweepRowsWithNoRun();
        if (swept > 0) log.info("[repo] swept {} incomplete or orphaned run(s)", swept);
        return swept;
    }

    /**
     * Deletes rows and facets whose run marker is gone entirely.
     *
     * <p>A second orphan class, and one the {@code IN_PROGRESS} sweep above cannot see: if a marker
     * is removed while its children remain — a partially-failed retire, an operator deleting a run,
     * a restore of {@code analysis_run} alone — the children are unreachable through every read
     * (they are only ever found via a run id) and would otherwise sit there forever. Exactly the
     * shape of the {@code MongoArtifactStore} chunk orphan whose collector had to be written
     * against the chunks rather than against the {@code .files} documents.
     *
     * @return the number of distinct orphaned run ids removed
     */
    private int sweepRowsWithNoRun() {
        int removed = 0;
        for (String runId : QueryBudget.request(rows).distinct("runId", String.class)) {
            if (runId == null || hasRun(runId)) continue;
            rows.deleteMany(Filters.eq("runId", runId));
            removed++;
        }
        for (String runId : QueryBudget.request(facets).distinct("runId", String.class)) {
            if (runId == null || hasRun(runId)) continue;
            facets.deleteMany(Filters.eq("runId", runId));
            removed++;
        }
        return removed;
    }

    /**
     * Deletes diverted payloads under {@code repo-overflow/} that no document references any more.
     *
     * <p>The third orphan class, and the one nothing collected at all. {@link MongoDocumentGuard}
     * writes an oversized element to the {@link ArtifactStore} and stores only its key; when the row
     * holding that key is deleted — a retired superseded run, a swept incomplete run, a discarded
     * loser of an ingest race — the artifact stays. On a re-analysed codebase with a handful of
     * indivisible 12 MB+ elements that is tens of megabytes leaked per run, forever, in the backend
     * that can least afford it (GridFS, where the bytes are also chunk documents).
     *
     * <p>Content addressing is what makes this safe to do by reference-count rather than by
     * ownership: the key <em>is</em> the SHA-256 of the payload, so two runs referencing the same
     * bytes reference the same key, and "no document mentions this key" is exactly "nothing can read
     * it". The referenced set is read from {@code analysis_row} and {@code analysis_facet} <b>before</b>
     * the listing, so a document written during the sweep is either already counted or belongs to a
     * blob too new to be eligible.
     *
     * <p><b>The age threshold is not a nicety; it is the only thing protecting an ingest in
     * flight.</b> A row's payload is diverted by {@code guard.store()} the moment the row streams
     * past, but the row <em>document</em> sits in {@link IngestingRowStream}'s batch buffer until
     * 4 MB or 2,000 rows accumulate. So for the whole of a large ingest there are live payloads on
     * disk that no document in Mongo references yet — they are referenced by an {@code ArrayList} in
     * another thread's heap, which no query can see. The reference check cannot help here and does
     * not pretend to. What makes the sweep safe is that nothing younger than
     * {@code olderThanMillis} is ever considered, so the threshold must exceed the duration of the
     * longest single ingest — hence the one-hour default and the refusal of a non-positive value in
     * {@code MongoHousekeepingService}. {@code MongoOverflowSweepTest} asserts both halves of this.
     *
     * @return the number of artifacts removed
     */
    public int sweepUnreferencedOverflowBlobs(long olderThanMillis) {
        ArtifactStore blobs = guard.blobs();
        ArtifactKey root = overflowRoot();
        long cutoff = System.currentTimeMillis() - Math.max(0, olderThanMillis);

        java.util.Set<String> referenced = new java.util.HashSet<>();
        try {
            for (String k : QueryBudget.request(rows).distinct(MongoDocumentGuard.F_BLOB_KEY, String.class)) {
                if (k != null) referenced.add(k);
            }
            for (String k : QueryBudget.request(facets).distinct(MongoDocumentGuard.F_BLOB_KEY, String.class)) {
                if (k != null) referenced.add(k);
            }
        } catch (MongoException e) {
            // Bail out rather than sweep against an incomplete reference set: a partial answer here
            // would delete live payloads, which is the one outcome worse than never running.
            log.warn("[repo] could not read the overflow reference set; skipping the sweep: {}",
                    e.getMessage());
            return 0;
        }

        List<ArtifactKey> victims = new ArrayList<>();
        try (java.util.stream.Stream<com.analyzer.storage.ArtifactEntry> s = blobs.list(root, true)) {
            s.filter(e -> !e.directory())
             .filter(e -> e.lastModified() > 0 && e.lastModified() < cutoff)
             .filter(e -> !referenced.contains(e.key().toString()))
             .forEach(e -> victims.add(e.key()));
        } catch (IOException | RuntimeException e) {
            log.warn("[repo] could not list {}: {}", root, e.getMessage());
            return 0;
        }

        int removed = 0;
        for (ArtifactKey key : victims) {
            try {
                if (blobs.delete(key)) removed++;
            } catch (RuntimeException e) {
                log.warn("[repo] could not delete unreferenced overflow blob {}: {}", key, e.getMessage());
            }
        }
        if (removed > 0) log.info("[repo] collected {} unreferenced overflow payload(s)", removed);
        return removed;
    }

    private boolean hasRun(String runId) {
        return QueryBudget.request(runs).find(Filters.eq("_id", runId)).projection(Projections.include("_id")).first() != null;
    }

    private interface IngestBody {
        /** @return the number of rows written */
        long run(String runId) throws IOException;
    }

    /* ─────────────────────────── analyses ─────────────────────────── */

    /**
     * Every analysis of one kind, newest first.
     *
     * <p>Delegated to the source, not answered from a collection, and this is the one method where
     * that is the right permanent answer rather than a stepping stone: the question is "which
     * analyses exist in the store", and the store — not this index — is the authority on that. An
     * analysis produced while this process was not looking, or removed underneath it, would make a
     * cached list wrong in the one place a user would immediately notice, because the picker is the
     * first screen they see. The interface documents it as bounded by the analysis count rather than
     * by artifact size for exactly this reason, so there is no scan to avoid here.
     */
    @Override
    public List<AnalysisRef> analyses(AnalysisQuery.Dataset dataset) throws IOException {
        return source.analyses(dataset);
    }

    /* ─────────────────────────── exists ─────────────────────────── */

    /**
     * Whether the artifact backing this query exists at all.
     *
     * <p>Delegated, deliberately. The interface is explicit that this is a different answer from an
     * empty result — "this analysis recorded no bundled JARs" is a 204 and "the filter matched
     * nothing" is an empty page, and the tree renders them differently. An ingested run cannot
     * distinguish them: both produce zero rows. So the question is answered where it has an answer,
     * against the artifact, and this method deliberately does not become "is there a run".
     */
    @Override
    public boolean exists(AnalysisQuery query) {
        return source.exists(query);
    }

    /* ─────────────────────────── documents ─────────────────────────── */

    /**
     * The whole facet document for a query, in exactly the shape it has on the wire today.
     *
     * <p>Served from the index after the first touch. Bounded per analysis — the largest observed is
     * ~307 KB ({@code api/tables/index.json}) — but "bounded by today's measurement" is not a
     * guarantee, so the payload goes through {@link MongoDocumentGuard} like everything else and a
     * document past the soft limit is diverted rather than failing the write.
     */
    @Override
    public JsonNode document(AnalysisQuery query) throws IOException {
        String runId = completeRunId(query);
        if (runId == null) {
            JsonNode doc = source.document(query);
            String json = doc == null || doc.isMissingNode() ? null : writeJson(doc);
            ingest(query, id -> {
                if (json == null) return 0;
                Document payload = guard.store(json);
                facets.replaceOne(
                        Filters.and(Filters.eq("runId", id), Filters.eq("table", query.table())),
                        payload.append("runId", id).append("table", query.table()),
                        new ReplaceOptions().upsert(true));
                return 1;
            });
            return doc;
        }
        Document stored = QueryBudget.request(facets).find(Filters.and(
                Filters.eq("runId", runId), Filters.eq("table", query.table()))).first();
        String json = guard.load(stored);
        // A COMPLETE run with no facet document means the source had none — an analysis with no such
        // facet. The interface says to answer with a null/missing node, so the absence is preserved
        // rather than turned into an empty object.
        return json == null ? null : mapper.readTree(json);
    }

    /* ─────────────────────────── rows ─────────────────────────── */

    /**
     * The rows a query matches, streamed one at a time, off a real Mongo cursor.
     *
     * <p>Peak heap is one row plus the pager's bounded heap, the same guarantee the scan
     * implementation gives and for the same reason — a real {@code analysis.json} reached 912 MB and
     * the class tree is projected out of it without ever holding it. Nothing here materialises a
     * result list: the cursor is consumed as the caller iterates and closed when the caller closes.
     *
     * <p>On the ingesting path the rows are written <em>as they stream past</em> and handed straight
     * on to the caller, so a first touch costs one pass rather than a pass to store plus a pass to
     * serve.
     */
    @Override
    public RowStream rows(AnalysisQuery query, Function<JsonNode, JsonNode> projection)
            throws IOException {
        String runId = completeRunId(query);
        if (runId == null) return ingestingRows(query, projection);

        MongoCursor<Document> cursor = QueryBudget.request(rows).find(Filters.eq("runId", runId))
                .sort(Sorts.ascending("seq")).iterator();
        return new RowStream() {
            @Override public boolean hasNext() { return cursor.hasNext(); }

            @Override
            public JsonNode next() {
                if (!cursor.hasNext()) throw new NoSuchElementException();
                try {
                    JsonNode row = mapper.readTree(guard.load(cursor.next()));
                    // The projection is applied on the way out, not stored applied: which fields a
                    // screen wants is a presentation decision and two screens can want different
                    // ones from the same run. A null return drops the row, per the interface.
                    return projection == null ? row : projection.apply(row);
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            }

            @Override public void close() { cursor.close(); }
        };
    }

    /**
     * First touch: stream the source's rows, write each one, and pass it to the caller.
     *
     * <p>The run is completed when the source stream is exhausted — that is, in {@link #close()} of
     * the returned stream, which the caller is already required to call. <b>A caller that abandons
     * the stream half way therefore leaves the run {@code IN_PROGRESS}</b>, which is exactly right:
     * a partial page-1 read must not publish a partial run as though it were the whole analysis.
     * The next query re-ingests from the start, and the sweeper collects the abandoned rows.
     */
    private RowStream ingestingRows(AnalysisQuery query, Function<JsonNode, JsonNode> projection)
            throws IOException {
        String runId = UUID.randomUUID().toString();
        runs.insertOne(new Document("_id", runId)
                .append("dataset", query.dataset().name())
                .append("analysis", analysisOf(query))
                .append("version", versionOf(query))
                .append("table", query.table())
                .append("status", IN_PROGRESS)
                .append("createdAt", new Date()));

        // The projection is NOT pushed into the source: the row is stored unprojected so a second
        // screen with a different projection is still served from the index rather than re-scanning.
        RowStream upstream = source.rows(query, null);
        return new IngestingRowStream(runId, query, upstream, projection);
    }

    /**
     * Writes rows to Mongo as they pass through to the caller, batching by accumulated bytes.
     *
     * <p>Unordered bulk inserts, so one bad document does not abort the batch (PLAN-06 "Write
     * patterns"). Batched by bytes rather than by count because the row sizes in play span two
     * orders of magnitude — 2.05 KB average for {@code node_<n>.json} against 370 KB for
     * {@code api/nodes/<id>.json} — so a fixed count is either wasteful or oversized.
     */
    private final class IngestingRowStream implements RowStream {

        private final String runId;
        private final AnalysisQuery query;
        private final RowStream upstream;
        private final Function<JsonNode, JsonNode> projection;
        private final List<Document> batch = new ArrayList<>();
        private long batchBytes;
        private long seq;
        private boolean completed;

        IngestingRowStream(String runId, AnalysisQuery query, RowStream upstream,
                           Function<JsonNode, JsonNode> projection) {
            this.runId = runId;
            this.query = query;
            this.upstream = upstream;
            this.projection = projection;
        }

        @Override
        public boolean hasNext() {
            return upstream.hasNext();
        }

        @Override
        public JsonNode next() {
            JsonNode row = upstream.next();
            try {
                String json = writeJson(row);
                Document payload = guard.store(json)
                        .append("runId", runId)
                        .append("seq", seq++);
                batch.add(payload);
                batchBytes += json.length();
                if (batchBytes >= BATCH_TARGET_BYTES || batch.size() >= BATCH_MAX_ROWS) flush();
            } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
            return projection == null ? row : projection.apply(row);
        }

        private void flush() {
            if (batch.isEmpty()) return;
            rows.insertMany(batch, new com.mongodb.client.model.InsertManyOptions().ordered(false));
            batch.clear();
            batchBytes = 0;
        }

        @Override
        public void close() throws IOException {
            try {
                // Only a fully-consumed source produces a complete run. hasNext() being false is
                // the only evidence available that the caller reached the end rather than giving up.
                if (!upstream.hasNext()) {
                    flush();
                    // publish() is the arbiter: it either makes this the one COMPLETE run for the
                    // key or discards it because a concurrent ingest published first. Either way
                    // the caller has already received every row, so neither outcome is an error
                    // here.
                    completed = publish(query, runId, seq);
                    if (completed) {
                        log.info("[repo] ingested {} rows for {} '{}' as run {}",
                                seq, query.dataset(), analysisOf(query), runId);
                    }
                } else {
                    log.info("[repo] row stream for {} '{}' abandoned after {} rows; run {} left "
                             + "IN_PROGRESS and therefore invisible",
                            query.dataset(), analysisOf(query), seq, runId);
                }
            } finally {
                upstream.close();
            }
        }

        /** For tests: whether close() published the run. */
        boolean completed() {
            return completed;
        }
    }

    /**
     * Serialises a row exactly as the wire would.
     *
     * <p>{@code writeValueAsString} on the tree, not a re-encoding through a model: the tree came
     * from the source's parser and this puts the same bytes back, which is what keeps a stored row
     * byte-identical to the one {@code artifact-scan} would have returned.
     */
    private String writeJson(JsonNode node) throws JsonProcessingException {
        return mapper.writeValueAsString(node);
    }

    /** For diagnostics and tests: how many COMPLETE runs this database holds. */
    public long completeRunCount() {
        return runs.countDocuments(Filters.eq("status", COMPLETE));
    }

    /** For diagnostics and tests: how many rows are stored for a query's COMPLETE run. */
    public long storedRowCount(AnalysisQuery query) {
        String runId = completeRunId(query);
        return runId == null ? -1 : rows.countDocuments(Filters.eq("runId", runId));
    }

    /* ─────────────────── PLAN-09 Increment E — entity queries ─────────────────── */

    /**
     * The PL/SQL entity index, when one has been attached. Null keeps this class working exactly as
     * Increment D left it.
     *
     * <p>Attached rather than constructed here, and mutable rather than a constructor parameter, for
     * one reason: the index needs the PL/SQL parse root, which is owned by
     * {@code AnalysisPaths.PARSE_ROOT} in the web module. Copying that literal into this module is
     * the drift PLAN-09 §8 R2 records as having silently broken WAR storage once already, so the
     * value comes from its owner at wiring time. Every existing constructor keeps its signature.
     */
    private volatile MongoPlsqlEntityIndex entities;

    /**
     * Attaches the entity index. Called once at wiring time.
     *
     * <p>Without it the three entity methods throw the interface's default
     * {@code UnsupportedOperationException} — which is the honest answer, not a silent scan. A
     * repository that quietly fell back to reading every node document while reporting
     * {@link #indexed()} {@code == true} is PLAN-09 §8 R1 in a different costume.
     */
    public void attachEntityIndex(MongoPlsqlEntityIndex index) {
        this.entities = index;
    }

    /** The attached index, or null. For {@code MongoHousekeepingService} and for tests. */
    public MongoPlsqlEntityIndex entityIndex() {
        return entities;
    }

    private MongoPlsqlEntityIndex requireEntities() {
        MongoPlsqlEntityIndex e = entities;
        if (e == null) {
            throw new UnsupportedOperationException(
                    "no PL/SQL entity index is attached to this repository; call "
                    + "attachEntityIndex() at wiring time (PLAN-09 Increment E)");
        }
        return e;
    }

    /** PLAN-08 Q43b, from {@code table_usage}'s {@code tableusage_key} index. */
    @Override
    public List<com.analyzer.storage.entity.TableUsageRow> nodesUsingTable(
            String analysis, String table, String usage) throws IOException {
        return requireEntities().nodesUsingTable(analysis, table, usage);
    }

    /** PLAN-08 Q58, from {@code flow_node}'s unique {@code flownode_key}. */
    @Override
    public java.util.Optional<com.analyzer.storage.entity.FlowNodeRow> flowNode(
            String analysis, String nodeId) throws IOException {
        return requireEntities().flowNode(analysis, nodeId);
    }

    /** PLAN-08 Q60, from {@code flow_edge}'s {@code edge_from} index. */
    @Override
    public List<com.analyzer.storage.entity.FlowEdgeRow> edgesFrom(String analysis, String nodeId)
            throws IOException {
        return requireEntities().edgesFrom(analysis, nodeId);
    }

    /** The guard, so a caller can report the soft limit it is enforcing. */
    public MongoDocumentGuard guard() {
        return guard;
    }

    /** The overflow root, so an operator can find diverted payloads. */
    public static ArtifactKey overflowRoot() {
        return ArtifactKey.of(MongoDocumentGuard.OVERFLOW_ROOT);
    }
}
