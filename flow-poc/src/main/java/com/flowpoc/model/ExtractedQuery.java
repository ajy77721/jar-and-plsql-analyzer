package com.flowpoc.model;

import java.util.ArrayList;
import java.util.List;

/**
 * A single SQL statement extracted from a flow step, with its parsed predicates.
 */
public class ExtractedQuery {

    public enum QueryType { SELECT, INSERT, UPDATE, DELETE, CALL, UNKNOWN }

    private final String rawSql;
    private final QueryType type;
    private final String tableName;          // primary table (best-effort)
    private final List<Predicate> predicates = new ArrayList<>();
    private final List<String> columns      = new ArrayList<>(); // projected or written columns
    private final String sourceClass;
    private final String sourceMethod;

    // set when the operation is a MongoDB aggregation pipeline
    private String aggregationPipeline;

    // set during data-fetch phase
    private List<java.util.Map<String, Object>> fetchedSample;

    public ExtractedQuery(String rawSql, QueryType type, String tableName,
                          String sourceClass, String sourceMethod) {
        this.rawSql       = rawSql;
        this.type         = type;
        this.tableName    = tableName;
        this.sourceClass  = sourceClass;
        this.sourceMethod = sourceMethod;
    }

    public String getRawSql()        { return rawSql; }
    public QueryType getType()       { return type; }
    public String getTableName()     { return tableName; }
    public List<Predicate> getPredicates() { return predicates; }
    public List<String> getColumns() { return columns; }
    public String getSourceClass()   { return sourceClass; }
    public String getSourceMethod()  { return sourceMethod; }

    public boolean isAggregation()                              { return aggregationPipeline != null; }
    public String  getAggregationPipeline()                     { return aggregationPipeline; }
    public void    setAggregationPipeline(String p)             { this.aggregationPipeline = p; }

    public List<java.util.Map<String, Object>> getFetchedSample() { return fetchedSample; }
    public void setFetchedSample(List<java.util.Map<String, Object>> s) { this.fetchedSample = s; }

    public void addPredicate(Predicate p) { predicates.add(p); }
    public void addColumn(String c)       { if (c != null && !c.isBlank()) columns.add(c); }
}
