package com.flowpoc.model;

/**
 * A single optimization opportunity identified during flow analysis.
 */
public class OptimizationFinding {

    public enum Category {
        MISSING_INDEX,       // WHERE predicate on column with no apparent index
        N_PLUS_ONE,          // SELECT inside a loop / repeated same-table query
        PREFETCH_CANDIDATE,  // child data always loaded after parent — can be joined
        BULK_WRITE,          // multiple INSERTs/UPDATEs to same table in same flow
        BULK_READ,           // multiple SELECTs on same table, different predicates
        FULL_TABLE_SCAN,     // SELECT without WHERE (or WHERE on non-indexed col)
        SELECT_STAR,         // SELECT * — fetches unused columns
        UNBOUNDED_RESULT,    // SELECT without LIMIT/ROWNUM
        OTHER
    }

    private final Category category;
    private final String   table;
    private final String   column;      // may be null
    private final String   description;
    private final String   location;    // className.methodName
    private final String   evidence;    // the SQL or call pattern that triggered this

    public OptimizationFinding(Category category, String table, String column,
                               String description, String location, String evidence) {
        this.category    = category;
        this.table       = table;
        this.column      = column;
        this.description = description;
        this.location    = location;
        this.evidence    = evidence;
    }

    public Category getCategory()    { return category; }
    public String   getTable()       { return table; }
    public String   getColumn()      { return column; }
    public String   getDescription() { return description; }
    public String   getLocation()    { return location; }
    public String   getEvidence()    { return evidence; }

    @Override
    public String toString() {
        return "[" + category + "] " + table
                + (column != null ? "." + column : "")
                + " @ " + location + " — " + description;
    }
}
