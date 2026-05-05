package com.flowpoc.layer2;

import org.bson.Document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CollectingQueryInterceptor implements QueryInterceptor {

    /**
     * One captured DB interaction — covers both MongoDB and SQL.
     *
     * For MongoDB: mongoOp is set, sql is null.
     * For SQL:     mongoOp is UNKNOWN, sql carries the statement.
     *
     * impactCount – for write ops: how many real-DB documents matched the filter
     *               (pre-seeded into shadow before the write was applied)
     * snapshotBefore – copies of those documents as they existed in the real DB
     *                  before the write; useful for test-data generation and
     *                  verifying what exactly would have been mutated
     */
    public record CapturedCall(
            String         collection,      // Mongo collection name, or null for SQL
            MongoOp        mongoOp,         // operation type; UNKNOWN for SQL
            Object         input,           // filter / document / pipeline stages
            String         sql,             // SQL statement (null for Mongo)
            List<Object>   params,          // SQL bind params (empty for Mongo)
            List<Object>   results,         // returned documents / rows
            long           impactCount,     // writes: matched real-DB doc count
            List<Document> snapshotBefore   // writes: pre-write state of matched docs
    ) {
        public boolean isMongo()     { return mongoOp != MongoOp.UNKNOWN; }
        public boolean isSql()       { return sql != null; }
        public boolean isWrite()     { return mongoOp != null && mongoOp.isMutation(); }
        public boolean hasImpact()   { return impactCount > 0; }

        /** Convenience ctor for reads / SQL (no impact). */
        public CapturedCall(String collection, MongoOp mongoOp, Object input,
                            String sql, List<Object> params, List<Object> results) {
            this(collection, mongoOp, input, sql, params, results,
                 0L, Collections.emptyList());
        }
    }

    private final List<CapturedCall>  captured    = new ArrayList<>();
    private final ShadowMongoStore    shadowStore = new ShadowMongoStore();

    @Override
    public void onMongoOperation(String collection, MongoOp op, Object input, List<Object> results) {
        captured.add(new CapturedCall(collection, op, input, null, Collections.emptyList(), results));
    }

    /** Called for write operations — carries real-DB impact metadata. */
    public void onMongoWrite(String collection, MongoOp op, Object input,
                             ShadowMongoStore.WriteImpact impact) {
        captured.add(new CapturedCall(
                collection, op, input, null, Collections.emptyList(),
                Collections.emptyList(),
                impact.matchedCount(), impact.snapshotBefore()));
    }

    public ShadowMongoStore getShadowStore() { return shadowStore; }

    @Override
    @SuppressWarnings("unchecked")
    public void onSqlQuery(String sql, List<Object> params, List<Map<String, Object>> results) {
        captured.add(new CapturedCall(null, MongoOp.UNKNOWN, null, sql, params,
                (List<Object>) (List<?>) results));
    }

    public List<CapturedCall> getCaptured() {
        return Collections.unmodifiableList(captured);
    }

    /** Clears only the captured calls — shadow store persists across steps within a flow. */
    public void clear() {
        captured.clear();
    }

    /** Clears both captured calls and the shadow store — call between FlowResult executions. */
    public void clearAll() {
        captured.clear();
        shadowStore.clear();
    }
}
