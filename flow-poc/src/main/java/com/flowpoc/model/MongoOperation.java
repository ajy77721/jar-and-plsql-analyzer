/*
 * Decompiled with CFR 0.152.
 */
package com.flowpoc.model;

import com.flowpoc.model.ExtractedQuery;
import com.flowpoc.model.Predicate;
import java.util.ArrayList;
import java.util.List;

public class MongoOperation {
    private final String collection;
    private final Op op;
    private final String rawFilter;
    private final String sourceClass;
    private final String sourceMethod;
    private final List<Predicate> predicates = new ArrayList<Predicate>();
    private String updateSpec;
    private String pipeline;

    public MongoOperation(String collection, Op op, String rawFilter, String sourceClass, String sourceMethod) {
        this.collection = collection;
        this.op = op;
        this.rawFilter = rawFilter;
        this.sourceClass = sourceClass;
        this.sourceMethod = sourceMethod;
    }

    public String getCollection() {
        return this.collection;
    }

    public Op getOp() {
        return this.op;
    }

    public String getRawFilter() {
        return this.rawFilter;
    }

    public String getSourceClass() {
        return this.sourceClass;
    }

    public String getSourceMethod() {
        return this.sourceMethod;
    }

    public List<Predicate> getPredicates() {
        return this.predicates;
    }

    public String getUpdateSpec() {
        return this.updateSpec;
    }

    public void setUpdateSpec(String u) {
        this.updateSpec = u;
    }

    public String getPipeline() {
        return this.pipeline;
    }

    public void setPipeline(String p) {
        this.pipeline = p;
    }

    public void addPredicate(Predicate p) {
        this.predicates.add(p);
    }

    public ExtractedQuery toExtractedQuery() {
        String sql = this.op.name() + " " + this.collection + (String)(this.rawFilter != null && !this.rawFilter.isBlank() ? " FILTER " + this.rawFilter : "");
        ExtractedQuery eq = new ExtractedQuery(sql, this.op.toQueryType(), this.collection, this.sourceClass, this.sourceMethod);
        this.predicates.forEach(eq::addPredicate);
        if (this.op == Op.AGGREGATE && this.pipeline != null) {
            eq.setAggregationPipeline(this.pipeline);
        }
        return eq;
    }

    public String toString() {
        return "db." + this.collection + "." + this.op.name().toLowerCase() + (String)(this.rawFilter != null ? "(" + this.rawFilter + ")" : "()");
    }

    public static enum Op {
        FIND,
        INSERT,
        UPDATE,
        DELETE,
        AGGREGATE,
        COUNT,
        EXISTS,
        REPLACE,
        UNKNOWN;


        public static Op fromString(String s) {
            if (s == null) {
                return UNKNOWN;
            }
            return switch (s.toUpperCase()) {
                case "FIND", "SELECT", "READ", "GET", "QUERY" -> FIND;
                case "INSERT", "INSERTONE", "INSERTMANY", "SAVE" -> INSERT;
                case "UPDATE", "UPDATEONE", "UPDATEMANY", "MODIFY", "PATCH" -> UPDATE;
                case "DELETE", "DELETEONE", "DELETEMANY", "REMOVE" -> DELETE;
                case "AGGREGATE", "PIPELINE" -> AGGREGATE;
                case "COUNT", "COUNTDOCUMENTS" -> COUNT;
                case "EXISTS" -> EXISTS;
                case "REPLACE", "REPLACEONE" -> REPLACE;
                default -> UNKNOWN;
            };
        }

        public ExtractedQuery.QueryType toQueryType() {
            return switch (this) {
                case FIND, COUNT, EXISTS, AGGREGATE -> ExtractedQuery.QueryType.SELECT;
                case INSERT -> ExtractedQuery.QueryType.INSERT;
                case UPDATE, REPLACE -> ExtractedQuery.QueryType.UPDATE;
                case DELETE -> ExtractedQuery.QueryType.DELETE;
                default -> ExtractedQuery.QueryType.UNKNOWN;
            };
        }
    }
}

