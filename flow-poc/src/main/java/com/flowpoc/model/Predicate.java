/*
 * Decompiled with CFR 0.152.
 */
package com.flowpoc.model;

public class Predicate {
    private final String column;
    private final Op op;
    private final String rawValue;
    private String resolvedValue;

    public Predicate(String column, Op op, String rawValue) {
        this.column = column;
        this.op = op;
        this.rawValue = rawValue;
    }

    public String getColumn() {
        return this.column;
    }

    public Op getOp() {
        return this.op;
    }

    public String getRawValue() {
        return this.rawValue;
    }

    public String getResolvedValue() {
        return this.resolvedValue;
    }

    public void setResolvedValue(String v) {
        this.resolvedValue = v;
    }

    public boolean isBindParam() {
        return this.rawValue != null && (this.rawValue.startsWith("?") || this.rawValue.startsWith(":"));
    }

    public String toString() {
        String val = this.resolvedValue != null ? this.resolvedValue : this.rawValue;
        return this.column + " " + this.op.name() + " " + val;
    }

    public static enum Op {
        EQ,
        NEQ,
        GT,
        GTE,
        LT,
        LTE,
        LIKE,
        IN,
        IS_NULL,
        IS_NOT_NULL,
        BETWEEN,
        OTHER;

    }
}

