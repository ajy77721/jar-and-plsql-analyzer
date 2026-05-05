package com.flowpoc.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Test-data records for one query in the flow, keyed by predicate conditions.
 * Represents: "for this step to execute with predicate X=Y, these are real DB records
 * that satisfy it, and this is what the next step receives."
 */
public class TestDataSet {

    private final String queryStep;      // className.methodName
    private final String sql;
    private final String table;
    private final List<Predicate> appliedPredicates;

    /** Rows fetched from the real DB matching these predicates (sample, up to 10). */
    private final List<Map<String, Object>> sampleRows = new ArrayList<>();

    /** If this step's output feeds the next step's predicate, the downstream binding. */
    private final Map<String, String> downstreamBindings = new LinkedHashMap<>();

    /** Edge-case variants: empty result, max-value, null column, etc. */
    private final List<TestCase> testCases = new ArrayList<>();

    public TestDataSet(String queryStep, String sql, String table,
                       List<Predicate> appliedPredicates) {
        this.queryStep        = queryStep;
        this.sql              = sql;
        this.table            = table;
        this.appliedPredicates = appliedPredicates;
    }

    public String             getQueryStep()          { return queryStep; }
    public String             getSql()                { return sql; }
    public String             getTable()              { return table; }
    public List<Predicate>    getAppliedPredicates()  { return appliedPredicates; }
    public List<Map<String, Object>> getSampleRows()  { return sampleRows; }
    public Map<String, String> getDownstreamBindings(){ return downstreamBindings; }
    public List<TestCase>     getTestCases()          { return testCases; }

    public void addRow(Map<String, Object> row)      { sampleRows.add(row); }
    public void addDownstreamBinding(String col, String nextParam) {
        downstreamBindings.put(col, nextParam);
    }
    public void addTestCase(TestCase tc)             { testCases.add(tc); }

    // ---- inner ----

    public static class TestCase {
        public enum Variant { HAPPY_PATH, EMPTY_RESULT, NULL_VALUE, MAX_VALUE, MIN_VALUE, BOUNDARY }

        private final Variant variant;
        private final String  description;
        private final Map<String, Object> inputData;
        private final Map<String, Object> expectedOutput;

        public TestCase(Variant variant, String description,
                        Map<String, Object> inputData, Map<String, Object> expectedOutput) {
            this.variant        = variant;
            this.description    = description;
            this.inputData      = inputData;
            this.expectedOutput = expectedOutput;
        }

        public Variant getVariant()                     { return variant; }
        public String  getDescription()                 { return description; }
        public Map<String, Object> getInputData()       { return inputData; }
        public Map<String, Object> getExpectedOutput()  { return expectedOutput; }
    }
}
