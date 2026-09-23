package com.codeanalyzer.ingest.mongo.write;

import com.codeanalyzer.ingest.mongo.id.Escaping;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Facet counts, computed once per run instead of on every request.
 *
 * <h2>The cost being avoided</h2>
 *
 * <p>A facet panel asks "how many statements per operation, per schema, per table". Answered with
 * {@code $group}, that reads every statement row — unnoticeable at the 2,436 rows measured here and
 * not viable at the 1,000,000 this model is sized for, on a panel that opens constantly.
 *
 * <h2>Why the run id belongs in this key, when it did not in {@code am_call_edge}</h2>
 *
 * <p>Removing {@code runId} from the call-edge key was what took dedup from 40.9% to 75%, so the
 * question is fair. The difference is what the row asserts. A call edge states a fact about the
 * source — procedure A calls B at line 30 — which is true in every run that parsed that source, so
 * scoping it by run manufactures duplicates of one fact.
 *
 * <p>A facet count states a fact about a run: "this run saw 412 SELECTs". Two runs that saw
 * different numbers hold genuinely different facts, and collapsing them would leave whichever was
 * written last. These rows are per-run by nature, and they are cheap: a few hundred rows against
 * the hundreds of thousands they summarise.
 */
public final class FacetCounter {

    /** Collection → the fields worth faceting on it. */
    private static final Map<String, List<String>> FACETS = new LinkedHashMap<>();
    static {
        FACETS.put(Collections.STATEMENT, List.of("operation", "schemaName", "tableName"));
        FACETS.put(Collections.DB_OBJECT, List.of("objectType", "schema"));
        FACETS.put(Collections.CHUNK_STATEMENT, List.of("statementType"));
        FACETS.put(Collections.PROC_NODE, List.of("objectType", "schema"));
    }

    /**
     * A value appearing in more rows than this is stored; rarer ones are not.
     *
     * <p>Zero, deliberately — every distinct value is kept. A threshold would make the counts a
     * sample rather than a total, and a facet panel that silently omits rare values is worse than
     * one that is slow: the reader cannot tell "no rows match" from "too few rows to mention".
     * The constant exists so that choice is visible rather than implied by its absence.
     */
    public static final int MIN_COUNT = 0;

    private final MongoDatabase db;

    public FacetCounter(MongoDatabase db) {
        this.db = db;
    }

    /**
     * Computes and stores every facet count for one run.
     *
     * <p>Uses {@code $group} exactly once, here, rather than once per request. The aggregation runs
     * against rows already resident from the ingest that just wrote them.
     *
     * @return how many facet rows were written
     */
    public int computeFor(String runId) {
        List<WriteModel<Document>> writes = new ArrayList<>();
        for (var entry : FACETS.entrySet()) {
            String collection = entry.getKey();
            for (String field : entry.getValue()) {
                var grouped = db.getCollection(collection).aggregate(List.of(
                        new Document("$match", new Document("runId", runId)
                                .append(field, new Document("$ne", null))),
                        new Document("$group", new Document("_id", "$" + field)
                                .append("n", new Document("$sum", 1)))));
                for (Document g : grouped) {
                    Object value = g.get("_id");
                    if (value == null) continue;
                    // Kept as int, matching what $sum returns for a count. Appending a long here
                    // stores a BSON Long, and every reader doing getInteger("count") then throws a
                    // ClassCastException on data that is otherwise perfectly correct.
                    int n = g.getInteger("n", 0);
                    if (n < MIN_COUNT) continue;
                    String key = Escaping.join(runId, collection, field, String.valueOf(value));
                    Document row = new Document("_id", key)
                            .append("key", key)
                            .append("runId", runId)
                            .append("entityCollection", collection)
                            .append("field", field)
                            .append("value", String.valueOf(value))
                            .append("count", n);
                    writes.add(new ReplaceOneModel<>(Filters.eq("_id", key), row,
                            new ReplaceOptions().upsert(true)));
                }
            }
        }
        if (writes.isEmpty()) return 0;
        db.getCollection(Collections.FACET_COUNT).bulkWrite(writes);
        return writes.size();
    }

    /** The counts for one facet, as the panel reads them: a single indexed lookup. */
    public List<Document> read(String runId, String collection, String field) {
        return db.getCollection(Collections.FACET_COUNT)
                .find(Filters.and(Filters.eq("runId", runId),
                        Filters.eq("entityCollection", collection),
                        Filters.eq("field", field)))
                .sort(new Document("count", -1))
                .into(new ArrayList<>());
    }
}
