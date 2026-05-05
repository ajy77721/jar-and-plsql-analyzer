package com.flowpoc.layer2;

import java.util.List;
import java.util.Map;

/**
 * Unified callback interface for all captured DB interactions.
 * One method per DB type — every MongoDB op (find/aggregate/insert/update/delete/bulk/command)
 * funnels through onMongoOperation; SQL through onSqlQuery.
 */
public interface QueryInterceptor {

    /**
     * Called for every MongoDB operation intercepted on MongoTemplate,
     * raw MongoCollection, or BulkOperations.
     *
     * @param collection  collection name (may be "unknown" for command ops)
     * @param op          operation type enum
     * @param input       filter doc / insert doc / pipeline stages / update spec / raw command
     * @param results     result documents (empty for writes)
     */
    void onMongoOperation(String collection, MongoOp op, Object input, List<Object> results);

    void onSqlQuery(String sql, List<Object> params, List<Map<String, Object>> results);
}
