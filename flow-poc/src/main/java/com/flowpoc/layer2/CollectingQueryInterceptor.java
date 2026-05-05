package com.flowpoc.layer2;

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
     */
    public record CapturedCall(
            String     collection,    // Mongo collection name, or null for SQL
            MongoOp    mongoOp,       // operation type; UNKNOWN for SQL
            Object     input,         // filter / document / pipeline stages / SQL params
            String     sql,           // SQL statement (null for Mongo)
            List<Object> params,      // SQL bind params (empty for Mongo)
            List<Object> results      // returned documents / rows
    ) {
        public boolean isMongo() { return mongoOp != MongoOp.UNKNOWN; }
        public boolean isSql()   { return sql != null; }
    }

    private final List<CapturedCall>  captured    = new ArrayList<>();
    private final ShadowMongoStore    shadowStore = new ShadowMongoStore();

    @Override
    public void onMongoOperation(String collection, MongoOp op, Object input, List<Object> results) {
        captured.add(new CapturedCall(collection, op, input, null, Collections.emptyList(), results));
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
