package com.flowpoc.model;

/**
 * A single optimization opportunity identified during flow analysis.
 */
public class OptimizationFinding {

    public enum Severity { HIGH, MEDIUM, LOW }

    public enum Category {
        // existing
        MISSING_INDEX,
        N_PLUS_ONE,
        PREFETCH_CANDIDATE,
        BULK_WRITE,
        BULK_READ,
        FULL_TABLE_SCAN,
        SELECT_STAR,
        UNBOUNDED_RESULT,
        // new
        STATIC_CACHEABLE,          // query result never changes — safe to cache at startup
        AGGREGATION_REWRITE,       // two queries can be merged into one aggregation pipeline
        PIPELINE_MISSING_PROJECT,  // aggregation returns all fields — add $project
        PIPELINE_UNBOUNDED_SORT,   // $sort without $limit on large result set
        CONTROLLER_DB_ACCESS,      // controller method directly queries DB
        CROSS_LAYER_QUERY,         // same collection queried at multiple call-tree depths
        UNBOUNDED_DEEP_READ,       // no limit + no filter at deep call-tree level
        OTHER
    }

    private final Category category;
    private final Severity severity;
    private final String   table;
    private final String   column;
    private final String   description;
    private final String   location;
    private final String   evidence;
    private final String   suggestedCode;   // short actionable code snippet / command

    public OptimizationFinding(Category category, Severity severity,
                               String table, String column,
                               String description, String location,
                               String evidence, String suggestedCode) {
        this.category      = category;
        this.severity      = severity;
        this.table         = table;
        this.column        = column;
        this.description   = description;
        this.location      = location;
        this.evidence      = evidence;
        this.suggestedCode = suggestedCode != null ? suggestedCode : "";
    }

    /** Backward-compat constructor (no severity, no suggestion). */
    public OptimizationFinding(Category category, String table, String column,
                               String description, String location, String evidence) {
        this(category, Severity.MEDIUM, table, column, description, location, evidence, null);
    }

    public Category getCategory()      { return category; }
    public Severity getSeverity()      { return severity; }
    public String   getTable()         { return table; }
    public String   getColumn()        { return column; }
    public String   getDescription()   { return description; }
    public String   getLocation()      { return location; }
    public String   getEvidence()      { return evidence; }
    public String   getSuggestedCode() { return suggestedCode; }

    @Override
    public String toString() {
        return "[" + severity + "/" + category + "] "
                + table + (column != null ? "." + column : "")
                + " @ " + location + " — " + description;
    }
}
