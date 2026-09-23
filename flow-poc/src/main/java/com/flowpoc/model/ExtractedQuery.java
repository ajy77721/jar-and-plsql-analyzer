/*
 * Decompiled with CFR 0.152.
 */
package com.flowpoc.model;

import com.flowpoc.model.Predicate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ExtractedQuery {
    private final String rawSql;
    private final QueryType type;
    private final String tableName;
    private final List<Predicate> predicates = new ArrayList<Predicate>();
    private final List<String> columns = new ArrayList<String>();
    private final String sourceClass;
    private final String sourceMethod;
    private String aggregationPipeline;
    private DataClass dataClass = DataClass.UNKNOWN;
    private List<Map<String, Object>> fetchedSample;

    public ExtractedQuery(String rawSql, QueryType type, String tableName, String sourceClass, String sourceMethod) {
        this.rawSql = rawSql;
        this.type = type;
        this.tableName = tableName;
        this.sourceClass = sourceClass;
        this.sourceMethod = sourceMethod;
    }

    public String getRawSql() {
        return this.rawSql;
    }

    public QueryType getType() {
        return this.type;
    }

    public String getTableName() {
        return this.tableName;
    }

    public List<Predicate> getPredicates() {
        return this.predicates;
    }

    public List<String> getColumns() {
        return this.columns;
    }

    public String getSourceClass() {
        return this.sourceClass;
    }

    public String getSourceMethod() {
        return this.sourceMethod;
    }

    public boolean isAggregation() {
        return this.aggregationPipeline != null;
    }

    public String getAggregationPipeline() {
        return this.aggregationPipeline;
    }

    public void setAggregationPipeline(String p) {
        this.aggregationPipeline = p;
    }

    public DataClass getDataClass() {
        return this.dataClass;
    }

    public void setDataClass(DataClass dc) {
        this.dataClass = dc;
    }

    public List<Map<String, Object>> getFetchedSample() {
        return this.fetchedSample;
    }

    public void setFetchedSample(List<Map<String, Object>> s) {
        this.fetchedSample = s;
    }

    public void addPredicate(Predicate p) {
        this.predicates.add(p);
    }

    public void addColumn(String c) {
        if (c != null && !c.isBlank()) {
            this.columns.add(c);
        }
    }

    public static enum DataClass {
        STATIC,
        TRANSACTIONAL,
        UNKNOWN;

    }

    public static enum QueryType {
        SELECT,
        INSERT,
        UPDATE,
        DELETE,
        CALL,
        UNKNOWN;

    }
}

