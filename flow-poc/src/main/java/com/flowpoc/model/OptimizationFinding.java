/*
 * Decompiled with CFR 0.152.
 */
package com.flowpoc.model;

public class OptimizationFinding {
    private final Category category;
    private final Severity severity;
    private final String table;
    private final String column;
    private final String description;
    private final String location;
    private final String evidence;
    private final String suggestedCode;
    private String claudeInsight;

    public OptimizationFinding(Category category, Severity severity, String table, String column, String description, String location, String evidence, String suggestedCode) {
        this.category = category;
        this.severity = severity;
        this.table = table;
        this.column = column;
        this.description = description;
        this.location = location;
        this.evidence = evidence;
        this.suggestedCode = suggestedCode != null ? suggestedCode : "";
    }

    public OptimizationFinding(Category category, String table, String column, String description, String location, String evidence) {
        this(category, Severity.MEDIUM, table, column, description, location, evidence, null);
    }

    public Category getCategory() {
        return this.category;
    }

    public Severity getSeverity() {
        return this.severity;
    }

    public String getTable() {
        return this.table;
    }

    public String getColumn() {
        return this.column;
    }

    public String getDescription() {
        return this.description;
    }

    public String getLocation() {
        return this.location;
    }

    public String getEvidence() {
        return this.evidence;
    }

    public String getSuggestedCode() {
        return this.suggestedCode;
    }

    public String getClaudeInsight() {
        return this.claudeInsight;
    }

    public void setClaudeInsight(String insight) {
        this.claudeInsight = insight;
    }

    public String toString() {
        return "[" + String.valueOf((Object)this.severity) + "/" + String.valueOf((Object)this.category) + "] " + this.table + (String)(this.column != null ? "." + this.column : "") + " @ " + this.location + " \u2014 " + this.description;
    }

    public static enum Category {
        MISSING_INDEX,
        N_PLUS_ONE,
        PREFETCH_CANDIDATE,
        BULK_WRITE,
        BULK_READ,
        FULL_TABLE_SCAN,
        SELECT_STAR,
        UNBOUNDED_RESULT,
        STATIC_CACHEABLE,
        AGGREGATION_REWRITE,
        PIPELINE_MISSING_PROJECT,
        PIPELINE_UNBOUNDED_SORT,
        CONTROLLER_DB_ACCESS,
        CROSS_LAYER_QUERY,
        UNBOUNDED_DEEP_READ,
        OTHER;

    }

    public static enum Severity {
        HIGH,
        MEDIUM,
        LOW;

    }
}

