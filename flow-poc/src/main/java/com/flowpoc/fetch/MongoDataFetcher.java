/*
 * Decompiled with CFR 0.152.
 * 
 * Could not load the following classes:
 *  com.fasterxml.jackson.databind.JsonNode
 *  com.fasterxml.jackson.databind.ObjectMapper
 *  com.mongodb.client.MongoClient
 *  com.mongodb.client.MongoCollection
 *  com.mongodb.client.MongoDatabase
 *  com.mongodb.client.model.Filters
 *  org.bson.Document
 *  org.bson.conversions.Bson
 */
package com.flowpoc.fetch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowpoc.fetch.DataFetcher;
import com.flowpoc.model.ExtractedQuery;
import com.flowpoc.model.Predicate;
import com.flowpoc.model.TestDataSet;
import com.flowpoc.testdata.TestDataBuilder;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.bson.conversions.Bson;

public class MongoDataFetcher
implements DataFetcher {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final MongoClient mongoClient;
    private final String databaseName;

    public MongoDataFetcher(MongoClient mongoClient, String databaseName) {
        this.mongoClient = mongoClient;
        this.databaseName = databaseName;
    }

    @Override
    public boolean supports(ExtractedQuery query) {
        if (query.getTableName() == null || query.getTableName().isBlank()) {
            return false;
        }
        ExtractedQuery.QueryType t = query.getType();
        return t == ExtractedQuery.QueryType.SELECT || t == ExtractedQuery.QueryType.UNKNOWN || query.isAggregation();
    }

    @Override
    public TestDataSet fetch(ExtractedQuery query, int sampleSize) {
        List<Map<String, Object>> rows = query.isAggregation() ? this.executeAggregate(query.getTableName(), query.getAggregationPipeline(), sampleSize) : this.executeFind(query.getTableName(), this.buildFilter(query.getPredicates()), sampleSize);
        TestDataSet tds = TestDataBuilder.from(query, rows);
        this.generateEdgeCases(tds, rows);
        return tds;
    }

    @Override
    public List<TestDataSet> fetchChain(List<ExtractedQuery> orderedQueries, int sampleSize) {
        ArrayList<TestDataSet> chain = new ArrayList<TestDataSet>();
        LinkedHashMap<String, Object> resolvedValues = new LinkedHashMap<String, Object>();
        for (ExtractedQuery query : orderedQueries) {
            List<Map<String, Object>> rows;
            if (!this.supports(query)) {
                chain.add(TestDataBuilder.empty(query));
                continue;
            }
            if (query.isAggregation()) {
                String resolvedPipeline = this.substitutePipelineBindParams(query.getAggregationPipeline(), resolvedValues);
                rows = this.executeAggregate(query.getTableName(), resolvedPipeline, sampleSize);
            } else {
                this.resolveBindParams(query, resolvedValues);
                rows = this.executeFind(query.getTableName(), this.buildFilter(query.getPredicates()), sampleSize);
            }
            TestDataSet tds = TestDataBuilder.from(query, rows);
            this.generateEdgeCases(tds, rows);
            if (!rows.isEmpty()) {
                rows.get(0).forEach((col, val) -> resolvedValues.put((String)col, val));
                String prefix = query.getTableName() + ".";
                rows.get(0).forEach((col, val) -> resolvedValues.put(prefix + col, val));
            }
            chain.add(tds);
        }
        return chain;
    }

    private List<Map<String, Object>> executeAggregate(String collection, String pipelineJson, int limit) {
        ArrayList<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        try {
            List<Bson> stages = this.parsePipelineStages(pipelineJson);
            stages.add((Bson)new Document("$limit", (Object)limit));
            MongoDatabase db = this.mongoClient.getDatabase(this.databaseName);
            MongoCollection<Document> col = db.getCollection(collection);
            for (Document doc : col.aggregate(stages)) {
                rows.add(new LinkedHashMap(doc));
            }
        }
        catch (Exception exception) {
            // empty catch block
        }
        return rows;
    }

    private List<Bson> parsePipelineStages(String pipelineJson) {
        ArrayList<Bson> stages = new ArrayList<Bson>();
        if (pipelineJson == null || pipelineJson.isBlank()) {
            return stages;
        }
        try {
            JsonNode root = MAPPER.readTree(pipelineJson.trim());
            if (root.isArray()) {
                for (JsonNode stage : root) {
                    stages.add((Bson)Document.parse((String)stage.toString()));
                }
            }
        }
        catch (Exception exception) {
            // empty catch block
        }
        return stages;
    }

    private String substitutePipelineBindParams(String pipelineJson, Map<String, Object> resolvedValues) {
        if (pipelineJson == null || resolvedValues.isEmpty()) {
            return pipelineJson;
        }
        String result = pipelineJson;
        ArrayList<Object> vals = new ArrayList<Object>(resolvedValues.values());
        for (int i = 0; i < vals.size(); ++i) {
            Object v = vals.get(i);
            if (v == null) continue;
            result = result.replace("\"?" + i + "\"", this.jsonValue(v));
        }
        for (Map.Entry<String, Object> entry : resolvedValues.entrySet()) {
            String col = entry.getKey();
            Object val = entry.getValue();
            if (val == null) continue;
            result = result.replace("\":" + col + "\"", this.jsonValue(val));
        }
        return result;
    }

    private String jsonValue(Object val) {
        if (val instanceof Number) {
            return val.toString();
        }
        if (val instanceof Boolean) {
            return val.toString();
        }
        return "\"" + String.valueOf(val) + "\"";
    }

    private void resolveBindParams(ExtractedQuery query, Map<String, Object> resolvedValues) {
        for (Predicate pred : query.getPredicates()) {
            String col;
            if (!pred.isBindParam() || pred.getResolvedValue() != null || (col = pred.getColumn()) == null) continue;
            Object val = resolvedValues.get(col);
            if (val == null) {
                val = resolvedValues.entrySet().stream().filter(e -> ((String)e.getKey()).equalsIgnoreCase(col) || ((String)e.getKey()).endsWith("." + col)).map(Map.Entry::getValue).findFirst().orElse(null);
            }
            if (val == null) continue;
            pred.setResolvedValue(String.valueOf(val));
        }
    }

    private Bson buildFilter(List<Predicate> predicates) {
        if (predicates.isEmpty()) {
            return new Document();
        }
        ArrayList<Bson> conditions = new ArrayList<Bson>();
        for (Predicate pred : predicates) {
            String val;
            String col = pred.getColumn();
            String string = val = pred.getResolvedValue() != null ? pred.getResolvedValue() : pred.getRawValue();
            if (col == null || val == null || val.startsWith("?") || val.startsWith(":")) continue;
            Bson cond = switch (pred.getOp()) {
                case EQ -> Filters.eq((String)col, (Object)val);
                case NEQ -> Filters.ne((String)col, (Object)val);
                case GT -> Filters.gt((String)col, (Object)val);
                case GTE -> Filters.gte((String)col, (Object)val);
                case LT -> Filters.lt((String)col, (Object)val);
                case LTE -> Filters.lte((String)col, (Object)val);
                case LIKE -> Filters.regex((String)col, (String)val.replace("%", ".*"));
                case IS_NULL -> Filters.eq((String)col, null);
                default -> Filters.eq((String)col, (Object)val);
            };
            conditions.add(cond);
        }
        if (conditions.isEmpty()) {
            return new Document();
        }
        return conditions.size() == 1 ? (Bson)conditions.get(0) : Filters.and(conditions);
    }

    private List<Map<String, Object>> executeFind(String collection, Bson filter, int limit) {
        ArrayList<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        try {
            MongoDatabase db = this.mongoClient.getDatabase(this.databaseName);
            MongoCollection<Document> col = db.getCollection(collection);
            for (Document doc : col.find(filter).limit(limit)) {
                rows.add(new LinkedHashMap(doc));
            }
        }
        catch (Exception exception) {
            // empty catch block
        }
        return rows;
    }

    private void generateEdgeCases(TestDataSet tds, List<Map<String, Object>> rows) {
        TestDataBuilder.appendEdgeCases(tds, rows, tds.getAppliedPredicates());
    }

    @Override
    public void close() {
        if (this.mongoClient != null) {
            this.mongoClient.close();
        }
    }
}

