package com.codeanalyzer.ingest.mongo.write;

import com.codeanalyzer.ingest.mongo.map.Splitter;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Batches upserts keyed on the business key, not on {@code _id}.
 *
 * <h2>Why the match is on {@code key} rather than {@code _id}</h2>
 *
 * <p>{@code _id} is left to MongoDB, which generates an {@code ObjectId}. That is deliberate: it
 * keeps the surrogate identity (where a row lives) separate from the business identity (what the
 * row is), so the natural key can change without rewriting every reference in the database.
 *
 * <p>Idempotency therefore comes from {@code key}, which is computed from natural keys before the
 * write and carries a unique index. Re-ingesting matches the existing document and replaces it;
 * nothing duplicates, and no read-back is needed for a child to reference its parent — the child
 * stores the parent's {@code key}, which it can compute itself.
 *
 * <p>{@code key} must be run-scoped where the natural key is not globally unique. A {@code nodeId}
 * repeats across analysis runs, so keying on it alone would make two runs overwrite each other.
 */
public final class BulkWriter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(BulkWriter.class);

    static final int BATCH_TARGET_BYTES = 4 * 1024 * 1024;
    static final int BATCH_MAX_ROWS = 2_000;

    /**
     * Refuse any single document beyond this.
     *
     * <p>With arrays split into their own collections no document should come close. If one does,
     * an array has crept back in — so this fails loudly and names the collection, rather than
     * waiting for MongoDB's 16 MiB rejection which names neither the cause nor the decision.
     */
    static final int MAX_DOC_BYTES = 1024 * 1024;

    private final MongoDatabase db;
    private final WriteStats stats;
    private final Map<String, List<WriteModel<Document>>> pending = new HashMap<>();
    private final Map<String, Integer> pendingBytes = new HashMap<>();
    private final CodecRegistry registry;
    /** Collections already given their indexes this session. */
    private final java.util.Set<String> prepared = new java.util.HashSet<>();

    public BulkWriter(MongoDatabase db, WriteStats stats) {
        this.db = db;
        this.stats = stats;
        this.registry = db.getCodecRegistry();
    }

    /**
     * Ensures the indexes a split-out child collection needs, once per collection.
     *
     * <p>Child collections are named from the schema at ingest time, so {@code Catalog} cannot
     * enumerate them ahead of time and {@code SchemaInstaller} never sees them. Without this they
     * are created implicitly by the first write with only the default {@code _id} index — and
     * every upsert, which matches on {@code key}, then scans the whole collection. That is
     * quadratic: the M1 ingest went from 2 seconds to <b>18 minutes</b> before this was added, on
     * a few thousand documents. At the target scale it would never finish.
     *
     * <p>{@code parentRef} is indexed for the same reason it exists: "the children of X" must be
     * one seek, or the relation chain is only nominally traversable.
     */
    private void prepare(String collection) {
        if (!prepared.add(collection)) return;
        var coll = db.getCollection(collection);
        try {
            // Partial, and that is not a detail. Entity collections are content-addressed and
            // their documents carry no `key` at all — a plain unique index would see every one of
            // them as `key: null` and reject the second insert. Indexing only the documents that
            // have a key lets one index serve both shapes: unique where identity is a business
            // key, absent where identity is the content hash.
            coll.createIndex(new Document("key", 1),
                    new com.mongodb.client.model.IndexOptions().name("uk_key").unique(true)
                            .partialFilterExpression(
                                    new Document("key", new Document("$exists", true))));
            coll.createIndex(new Document("parentRef", 1),
                    new com.mongodb.client.model.IndexOptions().name("ix_parent"));
            coll.createIndex(new Document("runId", 1),
                    new com.mongodb.client.model.IndexOptions().name("ix_run"));
        } catch (RuntimeException e) {
            // An index that already exists with different options is not fatal here: the
            // collection is still writable, and SchemaInstaller owns reconciliation for the
            // collections it declares.
            log.debug("index prepare on {}: {}", collection, e.toString());
        }
    }

    public void upsert(String collection, Document doc) {
        // Content-addressed documents match on _id; run-scoped rows (membership, split array
        // children) match on their business key. Both are computed before the write, so neither
        // needs a read-back.
        Object matchField = doc.get("_id") != null ? "_id" : "key";
        Object matchValue = doc.get(matchField.toString());
        if (matchValue == null) {
            throw new IllegalArgumentException(
                    "document for " + collection + " has neither _id nor key; both are computed "
                    + "up front, so a missing one is a mapper bug rather than a database concern");
        }
        Object key = matchValue;
        if (Splitter.containsArray(doc)) {
            throw new IllegalStateException(
                    "document " + key + " in " + collection + " contains an array. Arrays grow "
                    + "without bound and belong in their own collection — split it in the mapper "
                    + "rather than relaxing this check.");
        }
        String dataKeyed = Splitter.dataKeyedMap(doc);
        if (dataKeyed != null) {
            throw new IllegalStateException(
                    "document " + key + " in " + collection + ": " + dataKeyed + ". Field names "
                    + "made out of data are the same unbounded-growth failure the no-arrays rule "
                    + "prevents, and a map is simply not an array — which is how two of these "
                    + "reached the database before the rule was extended.");
        }
        int size = sizeOf(doc);
        if (size > MAX_DOC_BYTES) {
            throw new IllegalStateException(String.format(
                    "document %s in %s is %,d bytes, over the %,d limit", key, collection, size, MAX_DOC_BYTES));
        }
        prepare(collection);
        pending.computeIfAbsent(collection, k -> new ArrayList<>())
               .add(new ReplaceOneModel<>(Filters.eq(matchField.toString(), matchValue), doc,
                       new ReplaceOptions().upsert(true)));
        int total = pendingBytes.merge(collection, size, Integer::sum);
        if (total >= BATCH_TARGET_BYTES || pending.get(collection).size() >= BATCH_MAX_ROWS) {
            flush(collection);
        }
    }

    private int sizeOf(Document doc) {
        return doc.toBsonDocument(BsonDocument.class, registry).asDocument().toString().length();
    }

    public void flush(String collection) {
        List<WriteModel<Document>> batch = pending.remove(collection);
        pendingBytes.remove(collection);
        if (batch == null || batch.isEmpty()) return;
        db.getCollection(collection).bulkWrite(batch, new BulkWriteOptions().ordered(false));
        stats.record(collection, batch.size());
        log.debug("flushed {} docs to {}", batch.size(), collection);
    }

    public void flushAll() {
        for (String c : List.copyOf(pending.keySet())) flush(c);
    }

    @Override
    public void close() {
        flushAll();
    }
}
