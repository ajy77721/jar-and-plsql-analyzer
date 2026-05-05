package com.flowpoc.model;

import java.util.ArrayList;
import java.util.List;

/**
 * One node in the call-tree walk: a single method invocation with its
 * extracted SQL statements and the predicates parsed from each.
 */
public class FlowStep {

    public enum StepKind { CONTROLLER, SERVICE, REPOSITORY, EXTERNAL, OTHER }

    private final String className;
    private final String methodName;
    private final StepKind kind;
    private final int depth;

    private final List<ExtractedQuery> queries = new ArrayList<>();
    private final List<FlowStep> children = new ArrayList<>();

    // Layer-2 runtime fields (null in static-only mode)
    private String boundSql;          // SQL with actual parameter values substituted
    private List<Object> boundParams; // parameter values captured at runtime

    public FlowStep(String className, String methodName, StepKind kind, int depth) {
        this.className = className;
        this.methodName = methodName;
        this.kind = kind;
        this.depth = depth;
    }

    public String getClassName()         { return className; }
    public String getMethodName()        { return methodName; }
    public StepKind getKind()            { return kind; }
    public int getDepth()                { return depth; }
    public List<ExtractedQuery> getQueries() { return queries; }
    public List<FlowStep> getChildren()  { return children; }
    public String getBoundSql()          { return boundSql; }
    public void setBoundSql(String s)    { this.boundSql = s; }
    public List<Object> getBoundParams() { return boundParams; }
    public void setBoundParams(List<Object> p) { this.boundParams = p; }

    public void addQuery(ExtractedQuery q) { queries.add(q); }
    public void addChild(FlowStep s)       { children.add(s); }

    public String label() {
        return className.contains(".") ? className.substring(className.lastIndexOf('.') + 1) : className;
    }
}
