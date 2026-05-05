package com.flowpoc.model;

import java.util.ArrayList;
import java.util.List;

/**
 * A MongoDB operation extracted from the call tree.
 * Wraps collection name, operation type, raw filter expression, and parsed predicates.
 */
public class MongoOperation {

    public enum Op {
        FIND, INSERT, UPDATE, DELETE, AGGREGATE, COUNT, EXISTS, REPLACE, UNKNOWN;

        public static Op fromString(String s) {
            if (s == null) return UNKNOWN;
            return switch (s.toUpperCase()) {
                case "FIND", "SELECT"            -> FIND;
                case "INSERT", "INSERTONE",
                     "INSERTMANY", "SAVE"        -> INSERT;
                case "UPDATE", "UPDATEONE",
                     "UPDATEMANY", "MODIFY",
                     "PATCH"                     -> UPDATE;
                case "DELETE", "DELETEONE",
                     "DELETEMANY", "REMOVE"      -> DELETE;
                case "AGGREGATE", "PIPELINE"     -> AGGREGATE;
                case "COUNT", "COUNTDOCUMENTS"   -> COUNT;
                case "EXISTS"                    -> EXISTS;
                case "REPLACE", "REPLACEONE"     -> REPLACE;
                default                          -> UNKNOWN;
            };
        }

        public ExtractedQuery.QueryType toQueryType() {
            return switch (this) {
                case FIND, COUNT, EXISTS, AGGREGATE -> ExtractedQuery.QueryType.SELECT;
                case INSERT                         -> ExtractedQuery.QueryType.INSERT;
                case UPDATE, REPLACE                -> ExtractedQuery.QueryType.UPDATE;
                case DELETE                         -> ExtractedQuery.QueryType.DELETE;
                default                             -> ExtractedQuery.QueryType.UNKNOWN;
            };
        }
    }

    private final String collection;
    private final Op     op;
    private final String rawFilter;      // JSON filter string if available (from @Query etc.)
    private final String sourceClass;
    private final String sourceMethod;
    private final List<Predicate> predicates = new ArrayList<>();

    // For UPDATE/DELETE: the update document or push/pull spec
    private String updateSpec;

    // For AGGREGATE: pipeline stages as raw string
    private String pipeline;

    public MongoOperation(String collection, Op op, String rawFilter,
                          String sourceClass, String sourceMethod) {
        this.collection   = collection;
        this.op           = op;
        this.rawFilter    = rawFilter;
        this.sourceClass  = sourceClass;
        this.sourceMethod = sourceMethod;
    }

    public String getCollection()      { return collection; }
    public Op     getOp()              { return op; }
    public String getRawFilter()       { return rawFilter; }
    public String getSourceClass()     { return sourceClass; }
    public String getSourceMethod()    { return sourceMethod; }
    public List<Predicate> getPredicates() { return predicates; }
    public String getUpdateSpec()      { return updateSpec; }
    public void   setUpdateSpec(String u)  { this.updateSpec = u; }
    public String getPipeline()        { return pipeline; }
    public void   setPipeline(String p)    { this.pipeline = p; }

    public void addPredicate(Predicate p) { predicates.add(p); }

    public ExtractedQuery toExtractedQuery() {
        String sql = op.name() + " " + collection
                + (rawFilter != null && !rawFilter.isBlank() ? " FILTER " + rawFilter : "");
        ExtractedQuery eq = new ExtractedQuery(sql, op.toQueryType(),
                collection, sourceClass, sourceMethod);
        predicates.forEach(eq::addPredicate);
        return eq;
    }

    @Override
    public String toString() {
        return "db." + collection + "." + op.name().toLowerCase()
                + (rawFilter != null ? "(" + rawFilter + ")" : "()");
    }
}
