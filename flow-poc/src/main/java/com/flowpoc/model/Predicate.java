package com.flowpoc.model;

/**
 * A single WHERE-clause condition extracted from a SQL statement.
 * In static mode the value may be a bind placeholder (?/:name).
 * In dynamic mode it will be the actual runtime value.
 */
public class Predicate {

    public enum Op { EQ, NEQ, GT, GTE, LT, LTE, LIKE, IN, IS_NULL, IS_NOT_NULL, BETWEEN, OTHER }

    private final String column;
    private final Op     op;
    private final String rawValue;     // raw token from SQL (? / :param / literal)
    private String       resolvedValue; // filled in by data-fetch phase or runtime intercept

    public Predicate(String column, Op op, String rawValue) {
        this.column   = column;
        this.op       = op;
        this.rawValue = rawValue;
    }

    public String getColumn()          { return column; }
    public Op     getOp()              { return op; }
    public String getRawValue()        { return rawValue; }
    public String getResolvedValue()   { return resolvedValue; }
    public void   setResolvedValue(String v) { this.resolvedValue = v; }

    public boolean isBindParam() {
        return rawValue != null && (rawValue.startsWith("?") || rawValue.startsWith(":"));
    }

    @Override
    public String toString() {
        String val = resolvedValue != null ? resolvedValue : rawValue;
        return column + " " + op.name() + " " + val;
    }
}
