/*
 * Decompiled with CFR 0.152.
 */
package com.flowpoc.model;

import com.flowpoc.model.ExtractedQuery;
import java.util.ArrayList;
import java.util.List;

public class FlowStep {
    private final String className;
    private final String methodName;
    private final StepKind kind;
    private final int depth;
    private final List<ExtractedQuery> queries = new ArrayList<ExtractedQuery>();
    private final List<FlowStep> children = new ArrayList<FlowStep>();
    private String boundSql;
    private List<Object> boundParams;

    public FlowStep(String className, String methodName, StepKind kind, int depth) {
        this.className = className;
        this.methodName = methodName;
        this.kind = kind;
        this.depth = depth;
    }

    public String getClassName() {
        return this.className;
    }

    public String getMethodName() {
        return this.methodName;
    }

    public StepKind getKind() {
        return this.kind;
    }

    public int getDepth() {
        return this.depth;
    }

    public List<ExtractedQuery> getQueries() {
        return this.queries;
    }

    public List<FlowStep> getChildren() {
        return this.children;
    }

    public String getBoundSql() {
        return this.boundSql;
    }

    public void setBoundSql(String s) {
        this.boundSql = s;
    }

    public List<Object> getBoundParams() {
        return this.boundParams;
    }

    public void setBoundParams(List<Object> p) {
        this.boundParams = p;
    }

    public void addQuery(ExtractedQuery q) {
        this.queries.add(q);
    }

    public void addChild(FlowStep s) {
        this.children.add(s);
    }

    public String label() {
        return this.className.contains(".") ? this.className.substring(this.className.lastIndexOf(46) + 1) : this.className;
    }

    public static enum StepKind {
        CONTROLLER,
        SERVICE,
        REPOSITORY,
        EXTERNAL,
        OTHER;

    }
}

