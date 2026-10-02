package com.plsql.parser.model;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Lightweight summary of one subprogram inside a package that was crawled as a single,
 * aggregated {@link FlowNode} (a bare package name given as the entry point — see
 * {@code DependencyCrawler}'s package-as-entry-point aggregation branch). One instance per
 * member, listed on {@link FlowNode#getMembers()} so the UI can still drill into individual
 * procedures/functions of the package without a separate per-procedure crawl/output folder.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class SubprogramSummaryInfo {
    private String name;
    private String type;
    private int lineStart;
    private int lineEnd;
    /**
     * Task #29: true when this member is a nested/local subprogram (declared inside another
     * member's body). Additive; false for ordinary package members.
     */
    private boolean nested;
    /** Task #29: nesting level (0 = package member, 1 = one body deep, …). */
    private int scopeDepth;

    public SubprogramSummaryInfo() {
    }

    public SubprogramSummaryInfo(String name, String type, int lineStart, int lineEnd) {
        this(name, type, lineStart, lineEnd, false, 0);
    }

    public SubprogramSummaryInfo(String name, String type, int lineStart, int lineEnd,
                                 boolean nested, int scopeDepth) {
        this.name = name;
        this.type = type;
        this.lineStart = lineStart;
        this.lineEnd = lineEnd;
        this.nested = nested;
        this.scopeDepth = scopeDepth;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public int getLineStart() { return lineStart; }
    public void setLineStart(int lineStart) { this.lineStart = lineStart; }
    public int getLineEnd() { return lineEnd; }
    public void setLineEnd(int lineEnd) { this.lineEnd = lineEnd; }
    public boolean isNested() { return nested; }
    public void setNested(boolean nested) { this.nested = nested; }
    public int getScopeDepth() { return scopeDepth; }
    public void setScopeDepth(int scopeDepth) { this.scopeDepth = scopeDepth; }
}
