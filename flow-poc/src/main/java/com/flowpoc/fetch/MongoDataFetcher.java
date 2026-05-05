package com.flowpoc.fetch;

import com.flowpoc.model.*;
import com.flowpoc.testdata.TestDataBuilder;
import org.bson.Document;
import org.bson.conversions.Bson;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;

import java.util.*;

/**
 * Fetches representative MongoDB documents matching the predicates of an ExtractedQuery.
 *
 * Chain fetch (fetchChain) implements the "predicate propagation" idea:
 *   Step 1: fetch with no filter (or known predicates) → get sample docs
 *   Step 2: take a specific field from step-1 docs, bind it into step-2's predicate,
 *           re-run step-2 with real values → downstream data that actually exists
 *
 * This mirrors the "lob value → next query" scenario described in the POC brief.
 */
public class MongoDataFetcher implements DataFetcher {

    private final MongoClient mongoClient;
    private final String      databaseName;

    public MongoDataFetcher(MongoClient mongoClient, String databaseName) {
        this.mongoClient  = mongoClient;
        this.databaseName = databaseName;
    }

    @Override
    public boolean supports(ExtractedQuery query) {
        // Supports any query that targets a non-null collection name
        return query.getTableName() != null && !query.getTableName().isBlank()
                && query.getType() == ExtractedQuery.QueryType.SELECT;
    }

    @Override
    public TestDataSet fetch(ExtractedQuery query, int sampleSize) {
        String collection = query.getTableName();
        Bson   filter     = buildFilter(query.getPredicates());

        List<Map<String, Object>> rows = executeFind(collection, filter, sampleSize);

        TestDataSet tds = TestDataBuilder.from(query, rows);
        generateEdgeCases(tds, rows);
        return tds;
    }

    /**
     * Chain fetch: walks ordered queries propagating values from one result to the next.
     *
     * Algorithm per step:
     *  1. Build Bson filter from known static predicates
     *  2. If a bind param exists whose value was resolved in a prior step, substitute it
     *  3. Execute find → capture sample
     *  4. Expose all result columns so the next step can pick up values
     */
    @Override
    public List<TestDataSet> fetchChain(List<ExtractedQuery> orderedQueries, int sampleSize) {
        List<TestDataSet> chain = new ArrayList<>();
        Map<String, Object> resolvedValues = new LinkedHashMap<>(); // col → value from prior step

        for (ExtractedQuery query : orderedQueries) {
            if (!supports(query)) {
                chain.add(TestDataBuilder.empty(query));
                continue;
            }

            // Resolve bind parameters from prior step's output
            resolveBindParams(query, resolvedValues);

            Bson   filter = buildFilter(query.getPredicates());
            List<Map<String, Object>> rows = executeFind(query.getTableName(), filter, sampleSize);

            TestDataSet tds = TestDataBuilder.from(query, rows);
            generateEdgeCases(tds, rows);

            // Expose this step's column values for the next step
            if (!rows.isEmpty()) {
                rows.get(0).forEach((col, val) -> resolvedValues.put(col, val));
                // Also expose with collection-prefix for disambiguation
                String prefix = query.getTableName() + ".";
                rows.get(0).forEach((col, val) -> resolvedValues.put(prefix + col, val));
            }

            chain.add(tds);
        }
        return chain;
    }

    // ---- helpers ----

    private void resolveBindParams(ExtractedQuery query, Map<String, Object> resolvedValues) {
        for (Predicate pred : query.getPredicates()) {
            if (!pred.isBindParam() || pred.getResolvedValue() != null) continue;

            String col = pred.getColumn();
            if (col == null) continue;

            // Try direct match, then case-insensitive
            Object val = resolvedValues.get(col);
            if (val == null) {
                val = resolvedValues.entrySet().stream()
                        .filter(e -> e.getKey().equalsIgnoreCase(col)
                                || e.getKey().endsWith("." + col))
                        .map(Map.Entry::getValue)
                        .findFirst().orElse(null);
            }
            if (val != null) pred.setResolvedValue(String.valueOf(val));
        }
    }

    private Bson buildFilter(List<Predicate> predicates) {
        if (predicates.isEmpty()) return new Document();

        List<Bson> conditions = new ArrayList<>();
        for (Predicate pred : predicates) {
            String col = pred.getColumn();
            String val = pred.getResolvedValue() != null ? pred.getResolvedValue() : pred.getRawValue();
            if (col == null || val == null || val.startsWith("?") || val.startsWith(":")) continue;

            Bson cond = switch (pred.getOp()) {
                case EQ      -> Filters.eq(col, val);
                case NEQ     -> Filters.ne(col, val);
                case GT      -> Filters.gt(col, val);
                case GTE     -> Filters.gte(col, val);
                case LT      -> Filters.lt(col, val);
                case LTE     -> Filters.lte(col, val);
                case LIKE    -> Filters.regex(col, val.replace("%", ".*"));
                case IS_NULL -> Filters.eq(col, (Object) null);
                default      -> Filters.eq(col, val);
            };
            conditions.add(cond);
        }

        if (conditions.isEmpty()) return new Document();
        return conditions.size() == 1 ? conditions.get(0) : Filters.and(conditions);
    }

    private List<Map<String, Object>> executeFind(String collection, Bson filter, int limit) {
        List<Map<String, Object>> rows = new ArrayList<>();
        try {
            MongoDatabase db = mongoClient.getDatabase(databaseName);
            MongoCollection<Document> col = db.getCollection(collection);
            FindIterable<Document> it = col.find(filter).limit(limit);
            for (Document doc : it) {
                rows.add(new LinkedHashMap<>(doc));
            }
        } catch (Exception e) {
            // Non-fatal: collection may not exist in dev DB
        }
        return rows;
    }

    private void generateEdgeCases(TestDataSet tds, List<Map<String, Object>> rows) {
        TestDataBuilder.appendEdgeCases(tds, rows, tds.getAppliedPredicates());
    }

    @Override
    public void close() {
        if (mongoClient != null) mongoClient.close();
    }
}
