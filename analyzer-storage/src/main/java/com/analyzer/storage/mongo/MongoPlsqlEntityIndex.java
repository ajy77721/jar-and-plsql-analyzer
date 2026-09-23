package com.analyzer.storage.mongo;

import java.util.concurrent.TimeUnit;
import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.entity.FlowEdgeRow;
import com.analyzer.storage.entity.FlowNodeRow;
import com.analyzer.storage.entity.PlsqlEntityMapper;
import com.analyzer.storage.entity.TableUsageRow;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * The PL/SQL flow-node entity spine as real MongoDB collections with real indexes.
 * <b>PLAN-09 Increment E.</b>
 *
 * <h2>What this is, against what was there before</h2>
 *
 * {@link MongoAnalysisRepository} stores each row as an <em>opaque JSON payload</em> — deliberately,
 * because Increment C's acceptance criterion was that no response change byte for byte and a round
 * trip through typed records is exactly where field order and absent-vs-null shift. That gets you a
 * fast {@code document(key)} and nothing else. It is a document store used as an indexed row store,
 * which is a thing an object store already does.
 *
 * <p>This class is the part that makes MongoDB worth having: {@code flow_node}, {@code flow_edge} and
 * {@code table_usage} as queryable documents, so <b>"which nodes touch table X"</b> is an index seek
 * instead of a read of every node document in the analysis.
 *
 * <p><b>It is additive and it changes nothing.</b> The opaque-payload path is untouched and remains
 * the answer for all five original methods; no existing response can move by a byte because no
 * existing response is served from here. These collections are a second, parallel index over the
 * same artifacts, populated from them and discardable — drop all four collections and the
 * application still answers every question it answered before, just without the three entity
 * queries.
 *
 * <h2>Scope: the flow-node spine, and what is deliberately left</h2>
 *
 * PLAN-04 models 65 entities and PLAN-05 71 tables. Three are implemented here, chosen because real
 * data exists for them ({@code data/plsql-parse}, 16 analyses, 712 node documents) and because they
 * carry the query that justifies the work. <b>Deliberately not implemented:</b>
 *
 * <ul>
 *   <li>{@code table_join}, {@code db_column}, {@code cursor}, {@code dynamic_sql},
 *       {@code sequence} (PLAN-08 Q59/Q59b/Q61) — the four node-scan facets and the two table-detail
 *       lists. They have live endpoints already served whole from
 *       {@link MongoAnalysisRepository#document}, and converting a served endpoint is a
 *       response-changing act that belongs with the screen that validates it (PLAN-08 §3), not with
 *       the mapper.</li>
 *   <li>The whole JAR endpoint/class spine — {@code endpoint}, {@code java_class},
 *       {@code annotation_attribute_value}. Its field rules are the ones PLAN-11 §2 marks most
 *       hazardous (the scalar-or-array {@code attributes.value}, the {@code endpoints} and
 *       {@code modules} count/collection collisions). The normaliser those need is written and
 *       tested here ({@link com.analyzer.storage.entity.OrdinalValues}); the mapper that uses it is
 *       not, and would be a <b>separate class with its own {@code ARTIFACT} constant</b> — PLAN-11
 *       §2.4's rule is that a field means what it means in its artifact, so a shared mapper with a
 *       branch per artifact is the wrong shape, not a shortcut.</li>
 *   <li>{@code claude_*}, {@code cross_ref_*}, {@code conversion_*}, {@code chat_*} — PLAN-11 §5:
 *       those trees do not exist in any snapshot on this machine, so their field shapes are
 *       code-derived and unverified. Mapping them would be writing a schema against no data, which
 *       is what produced the six invalidated plan steps.</li>
 * </ul>
 *
 * <h2>The run marker again, not a transaction</h2>
 *
 * An ingest of one analysis writes tens of thousands of documents and can fail part way. So the same
 * scheme {@link MongoAnalysisRepository} uses is used here, for the same reason (MongoDB 6.0.26
 * standalone; multi-document transactions need a replica set, and PLAN-06 rejects them anyway):
 * insert an {@code entity_run} marker as {@code IN_PROGRESS}, write every child, flip to
 * {@code COMPLETE} last, and <b>filter every read on the COMPLETE run id</b>, so a partial ingest is
 * invisible rather than corrupt. A partial-unique index scoped to {@code status=COMPLETE} makes
 * "exactly one published run per analysis" an invariant the server enforces rather than a convention.
 *
 * <p>It is a <b>separate</b> marker collection from {@code analysis_run}, not a reuse of it. The two
 * have different lifecycles: {@code analysis_run} is keyed per
 * {@code (dataset, analysis, version, table)} facet and is created on the first touch of one screen,
 * while an entity run covers a whole analysis and is created on the first entity query. Sharing the
 * collection would mean a facet ingest could retire an entity run, and the retire is a
 * {@code deleteMany} on the children.
 */
public class MongoPlsqlEntityIndex {

    private static final Logger log = LoggerFactory.getLogger(MongoPlsqlEntityIndex.class);

    public static final String C_ENTITY_RUNS = "entity_run";
    public static final String C_FLOW_NODE = "flow_node";
    public static final String C_FLOW_EDGE = "flow_edge";
    public static final String C_TABLE_USAGE = "table_usage";

    private static final String IN_PROGRESS = "IN_PROGRESS";
    private static final String COMPLETE = "COMPLETE";

    /** Sized like {@link MongoAnalysisRepository}'s, and for the same reason: entity rows are small
     *  and numerous, so the count backstop is what actually fires here, not the byte target. */
    private static final int BATCH_MAX_ROWS = 2_000;

    private final MongoDatabase db;
    private final MongoCollection<Document> entityRuns;
    private final MongoCollection<Document> flowNodes;
    private final MongoCollection<Document> flowEdges;
    private final MongoCollection<Document> tableUsages;
    private final ArtifactStore store;
    private final PlsqlEntityMapper entityMapper;
    private final String parseRoot;

    /**
     * @param store     where the node documents are read from. The {@link ArtifactStore} and nothing
     *                  else, so this works identically against {@code local}, {@code s3} and
     *                  {@code mongodb-gridfs} — the property that kept the S3 backend to one class
     * @param parseRoot the PL/SQL parse tree's root segment, supplied by the caller rather than
     *                  duplicated as a constant here. {@code AnalysisPaths.PARSE_ROOT} lives in the
     *                  web module and is the single owner of this arithmetic; a copy of the literal
     *                  in this module is precisely the drift PLAN-09 §8 R2 records as having
     *                  silently broken WAR storage once already
     */
    public MongoPlsqlEntityIndex(MongoDatabase db, ArtifactStore store, ObjectMapper mapper,
                                 String parseRoot) {
        if (db == null) throw new IllegalArgumentException("mongo database is required");
        if (store == null) throw new IllegalArgumentException("an artifact store is required");
        if (parseRoot == null || parseRoot.isBlank()) {
            throw new IllegalArgumentException("the PL/SQL parse root is required");
        }
        this.db = db;
        this.store = store;
        this.parseRoot = parseRoot;
        this.entityMapper = new PlsqlEntityMapper(mapper);
        this.entityRuns = db.getCollection(C_ENTITY_RUNS);
        this.flowNodes = db.getCollection(C_FLOW_NODE);
        this.flowEdges = db.getCollection(C_FLOW_EDGE);
        this.tableUsages = db.getCollection(C_TABLE_USAGE);
    }

    /* ─────────────────────────── indexes (PLAN-08 §1) ─────────────────────────── */

    /**
     * The indexes the three entity queries use, and no others.
     *
     * <p>Every one is justified by a query in PLAN-08 §1. Speculative indexes are not free — the
     * ingest path writes tens of thousands of documents per analysis and each index is a write
     * amplification on all of them — so an index with no query is a cost with no return.
     */
    public void ensureIndexes() {
        try {
            createIndex(entityRuns, "entity_run_lookup",
                    Indexes.ascending("analysis", "status"),
                    new IndexOptions().name("entity_run_lookup").background(true));
            // Exactly one published run per analysis, enforced by the server. Partial and scoped to
            // COMPLETE for the same reason MongoAnalysisRepository's is: any number of ingests may
            // be in flight (the loser's rows are garbage, not a conflict), exactly one may be
            // published, and the single-document update that flips the marker becomes the
            // arbitration point.
            createIndex(entityRuns, "entity_run_one_complete",
                    Indexes.ascending("analysis"),
                    new IndexOptions().name("entity_run_one_complete").unique(true).background(true)
                            .partialFilterExpression(Filters.eq("status", COMPLETE)));

            // PLAN-08 Q58 / L21: one node by (analysis, nodeId). Unique, because two rows for one
            // node inside a run is the duplicate the mapper must not produce and nothing else would
            // catch. Also serves Q29's (analysis_id, node_uid) paging prefix.
            createIndex(flowNodes, "flownode_key",
                    Indexes.ascending("runId", "nodeId"),
                    new IndexOptions().name("flownode_key").unique(true).background(true));
            // PLAN-08 Q29 ix_flownode_type: the object-type facet of the node list.
            createIndex(flowNodes, "flownode_type",
                    Indexes.ascending("runId", "objectType", "nodeId"),
                    new IndexOptions().name("flownode_type").background(true));

            // PLAN-08 Q60 ix_edge_from / ix_edge_to. Both arms, because the call graph is walked
            // downward from a node and upward to find its callers, and an index that serves only
            // one direction turns the other into a collection scan.
            createIndex(flowEdges, "edge_from",
                    Indexes.ascending("runId", "fromNodeId"),
                    new IndexOptions().name("edge_from").background(true));
            createIndex(flowEdges, "edge_to",
                    Indexes.ascending("runId", "toNodeId"),
                    new IndexOptions().name("edge_to").background(true));

            // ── THE ONE THAT PAYS FOR THE DATABASE ──
            // PLAN-08 Q43b ix_nodecoll_name. (runId, tableKey, usage, nodeId): the filter is
            // (runId, tableKey) with an optional usage, the sort is nodeId, and the three fields
            // are in that order so a query with or without `usage` is still a prefix seek and the
            // sort is index-ordered rather than a blocking SORT stage. Adding the projected fields
            // would make it covered, but the fetch is bounded by the nodes touching one table and
            // an index carrying every column is a second copy of the collection.
            createIndex(tableUsages, "tableusage_key",
                    Indexes.ascending("runId", "tableKey", "usage", "nodeId"),
                    new IndexOptions().name("tableusage_key").background(true));
            // The reverse question — "which tables does this node touch" — is the table list of one
            // node, and it is the same collection read the other way round.
            createIndex(tableUsages, "tableusage_node",
                    Indexes.ascending("runId", "nodeId", "tableKey"),
                    new IndexOptions().name("tableusage_node").background(true));

            log.info("[entity] PL/SQL entity indexes ensured on {}", db.getName());
        } catch (MongoException e) {
            log.warn("[entity] could not ensure entity indexes on {}: {}", db.getName(), e.getMessage());
        }
    }

    /** Same rebuild-on-spec-change handling as {@link MongoAnalysisRepository}, and for the same
     *  reason: a {@code createIndex} that reuses a name with a changed spec fails at boot. */
    private void createIndex(MongoCollection<Document> collection, String name,
                             Bson keys, IndexOptions options) {
        try {
            collection.createIndex(keys, options);
        } catch (MongoCommandException e) {
            if (e.getErrorCode() != 85 && e.getErrorCode() != 86) throw e;
            log.info("[entity] index {}.{} has an outdated specification; rebuilding it",
                    collection.getNamespace().getCollectionName(), name);
            try {
                collection.dropIndex(name);
                collection.createIndex(keys, options);
            } catch (MongoException retry) {
                log.warn("[entity] could not rebuild index {}.{}: {}. Drop the {} collections to "
                         + "re-ingest.", collection.getNamespace().getCollectionName(), name,
                        retry.getMessage(), C_FLOW_NODE);
            }
        }
    }

    /* ─────────────────────────── queries ─────────────────────────── */

    /**
     * PLAN-08 Q43b. The nodes that use a table, from {@code tableusage_key}.
     *
     * <p>{@code table} is normalised through {@link TableUsageRow#keyOf} exactly as the stored key
     * was, so {@code ERP_AUDIT_LOG}, {@code erp_audit_log} and {@code ANALYZERDB.ERP_AUDIT_LOG} all
     * find the same rows. When the caller gives no schema the match is on the bare name against a
     * schema-qualified key, so it falls back to a suffix match on the same index — an
     * index-bounded regex anchored at {@code .}, not a scan of the collection.
     */
    public List<TableUsageRow> nodesUsingTable(String analysis, String table, String usage)
            throws IOException {
        String runId = ensureRun(analysis);
        if (runId == null) return List.of();

        String key = TableUsageRow.keyOf(null, table);
        if (key == null) return List.of();
        Bson tableFilter = key.indexOf('.') >= 0
                ? Filters.eq("tableKey", key)
                // No schema given. The stored keys are schema-qualified, so an equality match would
                // find nothing; ".NAME" anchored at the end is the same question. Not a leading
                // wildcard on purpose — that would be unindexable — but honestly a scan of the
                // (runId, tableKey) index range for this run rather than of the collection.
                : Filters.or(Filters.eq("tableKey", key),
                             Filters.regex("tableKey", "\\." + java.util.regex.Pattern.quote(key) + "$"));

        List<Bson> and = new ArrayList<>();
        and.add(Filters.eq("runId", runId));
        and.add(tableFilter);
        if (usage != null && !usage.isBlank()) and.add(Filters.eq("usage", usage));

        List<TableUsageRow> out = new ArrayList<>();
        try (MongoCursor<Document> c = QueryBudget.request(tableUsages).find(Filters.and(and))
                .sort(Sorts.ascending("nodeId")).iterator()) {
            while (c.hasNext()) out.add(toUsage(analysis, c.next()));
        }
        return out;
    }

    /** PLAN-08 Q58. One node, from the unique {@code flownode_key}. */
    public Optional<FlowNodeRow> flowNode(String analysis, String nodeId) throws IOException {
        String runId = ensureRun(analysis);
        if (runId == null) return Optional.empty();
        Document d = QueryBudget.request(flowNodes).find(Filters.and(
                Filters.eq("runId", runId), Filters.eq("nodeId", nodeId))).first();
        return Optional.ofNullable(d).map(doc -> toNode(analysis, doc));
    }

    /** PLAN-08 Q60. The calls out of one node, from {@code edge_from}. */
    public List<FlowEdgeRow> edgesFrom(String analysis, String nodeId) throws IOException {
        String runId = ensureRun(analysis);
        if (runId == null) return List.of();
        List<FlowEdgeRow> out = new ArrayList<>();
        try (MongoCursor<Document> c = QueryBudget.request(flowEdges).find(Filters.and(
                Filters.eq("runId", runId), Filters.eq("fromNodeId", nodeId)))
                .sort(Sorts.ascending("toNodeId")).iterator()) {
            while (c.hasNext()) out.add(toEdge(analysis, c.next()));
        }
        return out;
    }

    /**
     * The {@code explain} of the hot query, so the report can show a plan rather than assert one.
     *
     * <p>Exposed as a method rather than run by hand because "no collection scan on a hot path" is a
     * property that can regress silently — an index dropped, a filter field renamed — and the only
     * way to keep it true is for a test to be able to read the winning plan.
     */
    public Document explainNodesUsingTable(String analysis, String table, String usage)
            throws IOException {
        String runId = ensureRun(analysis);
        if (runId == null) return new Document();
        String key = TableUsageRow.keyOf(null, table);
        List<Bson> and = new ArrayList<>();
        and.add(Filters.eq("runId", runId));
        and.add(key != null && key.indexOf('.') >= 0
                ? Filters.eq("tableKey", key)
                : Filters.regex("tableKey", "\\." + java.util.regex.Pattern.quote(String.valueOf(key)) + "$"));
        if (usage != null && !usage.isBlank()) and.add(Filters.eq("usage", usage));
        return QueryBudget.request(tableUsages).find(Filters.and(and)).sort(Sorts.ascending("nodeId")).explain();
    }

    /* ─────────────────────────── ingest ─────────────────────────── */

    /**
     * The run id of the analysis's COMPLETE entity run, ingesting it first if there is not one.
     *
     * <p>Lazy on first touch, like Increment D's facet ingest and for the same three reasons: the
     * bytes originate from the same artifacts so nothing can diverge, the write path is exercised by
     * ordinary use rather than by a test that pretends, and it <b>degrades to correct</b> — with the
     * collections empty every answer is still right, just not yet fast. The honest cost is that the
     * <em>first</em> entity query for an analysis pays the ingest and is therefore slower than the
     * scan, not faster. Only the second onwards wins. That is the number anyone measuring this hits
     * first, so it is stated here rather than in a commit message.
     *
     * @return null when the analysis has no node documents at all — not an error, just an analysis
     *         with nothing to index, and the caller returns an empty result rather than a 500
     */
    private String ensureRun(String analysis) throws IOException {
        String existing = completeRunId(analysis);
        if (existing != null) return existing;
        return ingest(analysis);
    }

    private String completeRunId(String analysis) {
        try {
            Document d = QueryBudget.request(entityRuns).find(Filters.and(
                    Filters.eq("analysis", analysis), Filters.eq("status", COMPLETE)))
                    .projection(Projections.include("_id")).first();
            return d == null ? null : d.getString("_id");
        } catch (MongoException e) {
            log.warn("[entity] run lookup failed for '{}': {}", analysis, e.getMessage());
            return null;
        }
    }

    /**
     * Streams every {@code api/nodes/*.json} of one analysis through the mapper and writes the rows.
     *
     * <p>One artifact at a time, one JSON element at a time inside it — see
     * {@link PlsqlEntityMapper}. Peak heap is one batch of small entity documents, so a 5,000-node
     * analysis costs the same as a 69-node one.
     */
    private String ingest(String analysis) throws IOException {
        ArtifactKey nodesDir = ArtifactKey.of(parseRoot).resolve(analysis, "api", "nodes");
        List<ArtifactKey> keys = new ArrayList<>();
        try (java.util.stream.Stream<ArtifactEntry> s = store.list(nodesDir, false)) {
            s.filter(e -> !e.directory())
             .filter(e -> e.key().toString().endsWith(".json"))
             .forEach(e -> keys.add(e.key()));
        } catch (IOException e) {
            log.warn("[entity] cannot list {}: {}", nodesDir, e.getMessage());
            return null;
        }
        if (keys.isEmpty()) {
            log.info("[entity] analysis '{}' has no node documents under {}; nothing to index",
                    analysis, nodesDir);
            return null;
        }

        String runId = UUID.randomUUID().toString();
        long started = System.nanoTime();
        entityRuns.insertOne(new Document("_id", runId)
                .append("analysis", analysis)
                .append("status", IN_PROGRESS)
                .append("nodeFiles", keys.size())
                .append("createdAt", new Date()));

        Batch nodeBatch = new Batch(flowNodes);
        Batch edgeBatch = new Batch(flowEdges);
        Batch usageBatch = new Batch(tableUsages);
        long rows = 0;
        try {
            PlsqlEntityMapper.RowSink sink = PlsqlEntityMapper.sinkOf(
                    n -> nodeBatch.add(nodeDoc(runId, n)),
                    e -> edgeBatch.add(edgeDoc(runId, e)),
                    u -> usageBatch.add(usageDoc(runId, u)));
            for (ArtifactKey key : keys) {
                try (InputStream in = store.read(key)) {
                    rows += entityMapper.map(analysis, in, sink);
                }
            }
            nodeBatch.flush();
            edgeBatch.flush();
            usageBatch.flush();
        } catch (RuntimeException | IOException e) {
            // The marker is left IN_PROGRESS deliberately, exactly as MongoAnalysisRepository#ingest
            // does: a failed ingest is often a failing Mongo, and a cleanup that needs the database
            // that just failed will fail too. The run stays invisible to every read and
            // sweepIncompleteEntityRuns reclaims it by age.
            log.warn("[entity] ingest of '{}' failed after {} rows; run {} left IN_PROGRESS and "
                     + "therefore invisible: {}", analysis, rows, runId, e.toString());
            throw e instanceof IOException io ? io : new IOException(e);
        }

        if (!publish(analysis, runId, rows)) return completeRunId(analysis);
        log.info("[entity] indexed '{}' as run {} — {} node file(s), {} entity row(s), {} ms",
                analysis, runId, keys.size(), rows, (System.nanoTime() - started) / 1_000_000);
        return runId;
    }

    /**
     * Flips the marker to COMPLETE, or discards this run because a concurrent ingest published
     * first. The single-document update is the arbitration point — atomic on a standalone server,
     * where a transaction is not available.
     *
     * @return true when this run became the visible one
     */
    private boolean publish(String analysis, String runId, long rows) {
        try {
            long matched = entityRuns.updateOne(Filters.eq("_id", runId),
                    Updates.combine(Updates.set("status", COMPLETE),
                                    Updates.set("rowCount", rows),
                                    Updates.set("completedAt", new Date()))).getMatchedCount();
            if (matched == 0) {
                discard(runId);
                return false;
            }
        } catch (MongoWriteException | MongoBulkWriteException e) {
            if (!isDuplicateKey(e)) throw e;
            log.info("[entity] a concurrent ingest of '{}' already published; discarding run {} "
                     + "({} rows). Both read the same artifacts, so the published run is the same "
                     + "content.", analysis, runId, rows);
            discard(runId);
            return false;
        }
        retireSuperseded(analysis, runId);
        return true;
    }

    private static boolean isDuplicateKey(MongoException e) {
        if (e instanceof MongoWriteException w) {
            return w.getError().getCategory() == ErrorCategory.DUPLICATE_KEY;
        }
        if (e instanceof MongoBulkWriteException b) {
            return b.getWriteErrors().stream().anyMatch(
                    err -> ErrorCategory.fromErrorCode(err.getCode()) == ErrorCategory.DUPLICATE_KEY);
        }
        return e.getCode() == 11000;
    }

    /** Children first, marker last — a crash between them leaves invisible garbage a sweeper
     *  collects, rather than a marker whose run reads as empty. */
    private void discard(String runId) {
        try {
            deleteChildren(List.of(runId));
            entityRuns.deleteOne(Filters.eq("_id", runId));
        } catch (MongoException e) {
            log.warn("[entity] could not discard run {}: {}. The sweeper will collect it.",
                    runId, e.getMessage());
        }
    }

    private void retireSuperseded(String analysis, String keepRunId) {
        List<String> stale = new ArrayList<>();
        try (MongoCursor<Document> c = QueryBudget.request(entityRuns).find(Filters.and(
                Filters.eq("analysis", analysis), Filters.ne("_id", keepRunId)))
                .projection(Projections.include("_id")).iterator()) {
            while (c.hasNext()) stale.add(c.next().getString("_id"));
        }
        if (stale.isEmpty()) return;
        deleteChildren(stale);
        entityRuns.deleteMany(Filters.in("_id", stale));
    }

    private void deleteChildren(List<String> runIds) {
        flowNodes.deleteMany(Filters.in("runId", runIds));
        flowEdges.deleteMany(Filters.in("runId", runIds));
        tableUsages.deleteMany(Filters.in("runId", runIds));
    }

    /**
     * Deletes entity runs abandoned {@code IN_PROGRESS}, with their orphaned rows.
     *
     * <p>An operator action, driven from {@code MongoHousekeepingService} beside the two sweeps that
     * already exist. Nothing here is a correctness issue — an {@code IN_PROGRESS} run is invisible to
     * every read — so it reclaims space and nothing more, and it takes an age threshold because a run
     * being written looks exactly like one that was abandoned.
     */
    public int sweepIncompleteEntityRuns(long olderThanMillis) {
        Date cutoff = new Date(System.currentTimeMillis() - Math.max(0, olderThanMillis));
        List<String> stale = new ArrayList<>();
        try (MongoCursor<Document> c = QueryBudget.request(entityRuns).find(Filters.and(
                Filters.eq("status", IN_PROGRESS), Filters.lt("createdAt", cutoff)))
                .projection(Projections.include("_id")).iterator()) {
            while (c.hasNext()) stale.add(c.next().getString("_id"));
        }
        if (stale.isEmpty()) return 0;
        deleteChildren(stale);
        entityRuns.deleteMany(Filters.in("_id", stale));
        log.info("[entity] swept {} incomplete entity run(s)", stale.size());
        return stale.size();
    }

    /** Forces a re-index of one analysis. For a test, and for an operator whose artifacts changed
     *  underneath a published run. */
    public void invalidate(String analysis) {
        List<String> ids = new ArrayList<>();
        try (MongoCursor<Document> c = QueryBudget.request(entityRuns).find(Filters.eq("analysis", analysis))
                .projection(Projections.include("_id")).iterator()) {
            while (c.hasNext()) ids.add(c.next().getString("_id"));
        }
        if (ids.isEmpty()) return;
        deleteChildren(ids);
        entityRuns.deleteMany(Filters.in("_id", ids));
    }

    /** For diagnostics and tests. */
    public long entityRowCount(String analysis) {
        String runId = completeRunId(analysis);
        if (runId == null) return -1;
        return flowNodes.countDocuments(Filters.eq("runId", runId))
               + flowEdges.countDocuments(Filters.eq("runId", runId))
               + tableUsages.countDocuments(Filters.eq("runId", runId));
    }

    /** True when this analysis already has a published entity run — so a measurement can say which
     *  side of the first-touch cost it is on. */
    public boolean isIndexed(String analysis) {
        return completeRunId(analysis) != null;
    }

    /* ─────────────────────────── documents ─────────────────────────── */

    /**
     * Batches inserts, unordered so one bad document does not abort the batch (PLAN-06 "Write
     * patterns").
     *
     * <p>Count-based here rather than byte-based, unlike the payload rows in
     * {@link MongoAnalysisRepository}: those span two orders of magnitude in size (2 KB to 370 KB),
     * which is what made a fixed count either wasteful or oversized. Entity documents are uniformly
     * small — a {@code table_usage} row is a handful of scalars plus a line array — so the count is
     * the meaningful bound and a byte accumulator would be arithmetic with no effect.
     */
    private static final class Batch {
        private final MongoCollection<Document> collection;
        private final List<Document> buffer = new ArrayList<>(BATCH_MAX_ROWS);

        Batch(MongoCollection<Document> collection) {
            this.collection = collection;
        }

        void add(Document d) {
            buffer.add(d);
            if (buffer.size() >= BATCH_MAX_ROWS) flush();
        }

        void flush() {
            if (buffer.isEmpty()) return;
            collection.insertMany(buffer, new InsertManyOptions().ordered(false));
            buffer.clear();
        }
    }

    /**
     * Entity rows are stored as <b>real BSON</b>, not as an opaque JSON string.
     *
     * <p>That is the whole point of this class and it is the exact opposite of the choice
     * {@link MongoAnalysisRepository} documents at length — correctly, for its own case. The two
     * reasons that made a JSON string right there do not apply here:
     *
     * <ul>
     *   <li><b>Byte-exactness is not at stake.</b> These rows are not any response. No endpoint
     *       serves them, so a numeric widening or a field reordering has nothing to break. The
     *       responses that must stay byte-identical are still served from the payload path,
     *       untouched.</li>
     *   <li><b>The fields are the point.</b> A BSON sub-document is what an index can be built on. A
     *       JSON string cannot answer "which nodes touch table X" any better than the artifact
     *       could.</li>
     * </ul>
     *
     * <p>The third reason — BSON's historical rejection of {@code .} and {@code $} in keys — is real
     * and is handled rather than dodged: no mapped value is ever used as a <em>key</em>. A node id
     * is {@code ANALYZERDB$ERP_USP_RUN_CYCLE} and a table key is {@code SCHEMA.TABLE}; both contain
     * exactly those characters, and both are stored as <em>values</em> of fixed field names, which
     * has never been restricted. The one place the corpus would have forced a dynamic key —
     * {@code statementSummary}, whose keys are 43 dialect tokens — is stored as an array of
     * {@code {statementType, count}} pairs instead, which is also what PLAN-11 §3 asks for
     * ("child table {@code (node_id, statement_type, count)} — never an enum").
     */
    private static Document nodeDoc(String runId, FlowNodeRow n) {
        List<Document> params = new ArrayList<>(n.parameters().size());
        for (FlowNodeRow.Parameter p : n.parameters()) {
            params.add(putIfPresent(new Document("ordinal", p.ordinal()), "name", p.name())
                    .append("dataType", p.dataType())
                    .append("direction", p.direction())
                    .append("line", p.line()));
        }
        List<Document> stmts = new ArrayList<>(n.statements().size());
        for (FlowNodeRow.StatementCount s : n.statements()) {
            stmts.add(new Document("statementType", s.statementType()).append("count", s.count()));
        }
        return new Document("runId", runId)
                .append("nodeId", n.nodeId())
                .append("name", n.name())
                .append("schema", n.schema())
                .append("objectType", n.objectType())
                .append("depth", n.depth())
                .append("lineStart", n.lineStart())
                .append("lineEnd", n.lineEnd())
                .append("linesOfCode", n.linesOfCode())
                .append("sourceFile", n.sourceFile())
                .append("parameters", params)
                .append("statements", stmts);
    }

    private static Document putIfPresent(Document d, String field, Object value) {
        // PLAN-11 rule 4 at the storage boundary: an absent field and a null field must not become
        // the same thing, and a document that carries `name: null` makes an existence filter on
        // `name` match a row that has none.
        if (value != null) d.append(field, value);
        return d;
    }

    private static Document edgeDoc(String runId, FlowEdgeRow e) {
        return new Document("runId", runId)
                .append("fromNodeId", e.fromNodeId())
                .append("toNodeId", e.toNodeId())
                .append("toSchema", e.toSchema())
                .append("toObject", e.toObject())
                .append("packageName", e.packageName())
                .append("callType", e.callType())
                .append("argCount", e.argCount())
                .append("count", e.count())
                .append("lines", e.lines());
    }

    private static Document usageDoc(String runId, TableUsageRow u) {
        return new Document("runId", runId)
                .append("nodeId", u.nodeId())
                .append("nodeName", u.nodeName())
                .append("tableKey", u.tableKey())
                .append("tableSchema", u.tableSchema())
                .append("tableName", u.tableName())
                .append("objectType", u.objectType())
                .append("operation", u.operation())
                .append("usage", u.usage())
                .append("totalUsageCount", u.totalUsageCount())
                .append("directUsageCount", u.directUsageCount())
                .append("lines", u.lines());
    }

    private static FlowNodeRow toNode(String analysis, Document d) {
        List<FlowNodeRow.Parameter> params = new ArrayList<>();
        for (Document p : d.getList("parameters", Document.class, List.of())) {
            params.add(new FlowNodeRow.Parameter(p.getInteger("ordinal", 0), p.getString("name"),
                    p.getString("dataType"), p.getString("direction"), p.getInteger("line")));
        }
        List<FlowNodeRow.StatementCount> stmts = new ArrayList<>();
        for (Document s : d.getList("statements", Document.class, List.of())) {
            stmts.add(new FlowNodeRow.StatementCount(s.getString("statementType"),
                    s.getInteger("count", 0)));
        }
        return new FlowNodeRow(analysis, d.getString("nodeId"), d.getString("name"),
                d.getString("schema"), d.getString("objectType"), d.getInteger("depth"),
                d.getInteger("lineStart"), d.getInteger("lineEnd"), d.getInteger("linesOfCode"),
                d.getString("sourceFile"), List.copyOf(params), List.copyOf(stmts));
    }

    private static FlowEdgeRow toEdge(String analysis, Document d) {
        return new FlowEdgeRow(analysis, d.getString("fromNodeId"), d.getString("toNodeId"),
                d.getString("toSchema"), d.getString("toObject"), d.getString("packageName"),
                d.getString("callType"), d.getInteger("argCount"), d.getInteger("count"),
                List.copyOf(d.getList("lines", Integer.class, List.of())));
    }

    private static TableUsageRow toUsage(String analysis, Document d) {
        return new TableUsageRow(analysis, d.getString("nodeId"), d.getString("nodeName"),
                d.getString("tableKey"), d.getString("tableSchema"), d.getString("tableName"),
                d.getString("objectType"), d.getString("operation"), d.getString("usage"),
                d.getInteger("totalUsageCount"), d.getInteger("directUsageCount"),
                List.copyOf(d.getList("lines", Integer.class, List.of())));
    }
}
