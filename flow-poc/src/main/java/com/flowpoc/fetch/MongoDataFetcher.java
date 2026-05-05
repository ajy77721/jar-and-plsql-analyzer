package com.flowpoc.fetch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowpoc.model.*;
import com.flowpoc.testdata.TestDataBuilder;
import org.bson.Document;
import org.bson.conversions.Bson;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;

import java.util.*;

/**
 * Fetches representative MongoDB documents for both find and aggregate operations.
 *
 * Chain fetch propagates values across steps:
 *   Step 1: fetch with known predicates/pipeline → sample docs
 *   Step 2: bind field values from step-1 docs into step-2 filter or $match stage
 *
 * Aggregation pipelines are re-executed as-is against the real collection.
 * Bind parameters (?0 / :name) in $match stages are substituted with
 * values resolved from prior step results before execution.
 */
public class MongoDataFetcher implements DataFetcher {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final MongoClient mongoClient;
    private final String      databaseName;

    public MongoDataFetcher(MongoClient mongoClient, String databaseName) {
        this.mongoClient  = mongoClient;
        this.databaseName = databaseName;
    }

    @Override
    public boolean supports(ExtractedQuery query) {
        return query.getTableName() != null && !query.getTableName().isBlank()
                && (query.getType() == ExtractedQuery.QueryType.SELECT
                    || query.isAggregation());
    }

    @Override
    public TestDataSet fetch(ExtractedQuery query, int sampleSize) {
        List<Map<String, Object>> rows = query.isAggregation()
                ? executeAggregate(query.getTableName(), query.getAggregationPipeline(), sampleSize)
                : executeFind(query.getTableName(), buildFilter(query.getPredicates()), sampleSize);

        TestDataSet tds = TestDataBuilder.from(query, rows);
        generateEdgeCases(tds, rows);
        return tds;
    }

    @Override
    public List<TestDataSet> fetchChain(List<ExtractedQuery> orderedQueries, int sampleSize) {
        List<TestDataSet> chain = new ArrayList<>();
        Map<String, Object> resolvedValues = new LinkedHashMap<>();

        for (ExtractedQuery query : orderedQueries) {
            if (!supports(query)) {
                chain.add(TestDataBuilder.empty(query));
                continue;
            }

            List<Map<String, Object>> rows;
            if (query.isAggregation()) {
                String resolvedPipeline = substitutePipelineBindParams(
                        query.getAggregationPipeline(), resolvedValues);
                rows = executeAggregate(query.getTableName(), resolvedPipeline, sampleSize);
            } else {
                resolveBindParams(query, resolvedValues);
                rows = executeFind(query.getTableName(), buildFilter(query.getPredicates()), sampleSize);
            }

            TestDataSet tds = TestDataBuilder.from(query, rows);
            generateEdgeCases(tds, rows);

            if (!rows.isEmpty()) {
                rows.get(0).forEach((col, val) -> resolvedValues.put(col, val));
                String prefix = query.getTableName() + ".";
                rows.get(0).forEach((col, val) -> resolvedValues.put(prefix + col, val));
            }

            chain.add(tds);
        }
        return chain;
    }

    // ---- aggregation ----

    private List<Map<String, Object>> executeAggregate(String collection,
                                                        String pipelineJson,
                                                        int limit) {
        List<Map<String, Object>> rows = new ArrayList<>();
        try {
            List<Bson> stages = parsePipelineStages(pipelineJson);
            stages.add(new Document("$limit", limit));

            MongoDatabase              db  = mongoClient.getDatabase(databaseName);
            MongoCollection<Document>  col = db.getCollection(collection);
            for (Document doc : col.aggregate(stages)) {
                rows.add(new LinkedHashMap<>(doc));
            }
        } catch (Exception e) {
            // Non-fatal — collection may not exist or pipeline may contain unresolved params
        }
        return rows;
    }

    private List<Bson> parsePipelineStages(String pipelineJson) {
        List<Bson> stages = new ArrayList<>();
        if (pipelineJson == null || pipelineJson.isBlank()) return stages;
        try {
            JsonNode root = MAPPER.readTree(pipelineJson.trim());
            if (root.isArray()) {
                for (JsonNode stage : root) {
                    stages.add(Document.parse(stage.toString()));
                }
            }
        } catch (Exception ignored) {
        }
        return stages;
    }

    /**
     * Substitutes bind-param placeholders (?0, ?1, :name) inside a pipeline JSON string
     * with values resolved from prior fetch steps.
     */
    private String substitutePipelineBindParams(String pipelineJson,
                                                 Map<String, Object> resolvedValues) {
        if (pipelineJson == null || resolvedValues.isEmpty()) return pipelineJson;
        String result = pipelineJson;
        // Replace positional ?0/?1 with first available resolved value
        List<Object> vals = new ArrayList<>(resolvedValues.values());
        for (int i = 0; i < vals.size(); i++) {
            Object v = vals.get(i);
            if (v == null) continue;
            result = result.replace("\"?" + i + "\"", jsonValue(v));
        }
        // Replace named :param with matching column value
        for (Map.Entry<String, Object> entry : resolvedValues.entrySet()) {
            String col = entry.getKey();
            Object val = entry.getValue();
            if (val == null) continue;
            result = result.replace("\":" + col + "\"", jsonValue(val));
        }
        return result;
    }

    private String jsonValue(Object val) {
        if (val instanceof Number) return val.toString();
        if (val instanceof Boolean) return val.toString();
        return "\"" + val + "\"";
    }

    // ---- find helpers ----

    private void resolveBindParams(ExtractedQuery query, Map<String, Object> resolvedValues) {
        for (Predicate pred : query.getPredicates()) {
            if (!pred.isBindParam() || pred.getResolvedValue() != null) continue;
            String col = pred.getColumn();
            if (col == null) continue;
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
            for (Document doc : col.find(filter).limit(limit)) {
                rows.add(new LinkedHashMap<>(doc));
            }
        } catch (Exception e) {
            // Non-fatal
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
