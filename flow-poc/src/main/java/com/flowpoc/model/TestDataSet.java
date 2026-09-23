/*
 * Decompiled with CFR 0.152.
 */
package com.flowpoc.model;

import com.flowpoc.model.Predicate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestDataSet {
    private final String queryStep;
    private final String sql;
    private final String table;
    private final List<Predicate> appliedPredicates;
    private final List<Map<String, Object>> sampleRows = new ArrayList<Map<String, Object>>();
    private final Map<String, String> downstreamBindings = new LinkedHashMap<String, String>();
    private final List<TestCase> testCases = new ArrayList<TestCase>();

    public TestDataSet(String queryStep, String sql, String table, List<Predicate> appliedPredicates) {
        this.queryStep = queryStep;
        this.sql = sql;
        this.table = table;
        this.appliedPredicates = appliedPredicates;
    }

    public String getQueryStep() {
        return this.queryStep;
    }

    public String getSql() {
        return this.sql;
    }

    public String getTable() {
        return this.table;
    }

    public List<Predicate> getAppliedPredicates() {
        return this.appliedPredicates;
    }

    public List<Map<String, Object>> getSampleRows() {
        return this.sampleRows;
    }

    public Map<String, String> getDownstreamBindings() {
        return this.downstreamBindings;
    }

    public List<TestCase> getTestCases() {
        return this.testCases;
    }

    public void addRow(Map<String, Object> row) {
        this.sampleRows.add(row);
    }

    public void addDownstreamBinding(String col, String nextParam) {
        this.downstreamBindings.put(col, nextParam);
    }

    public void addTestCase(TestCase tc) {
        this.testCases.add(tc);
    }

    public static class TestCase {
        private final Variant variant;
        private final String description;
        private final Map<String, Object> inputData;
        private final Map<String, Object> expectedOutput;

        public TestCase(Variant variant, String description, Map<String, Object> inputData, Map<String, Object> expectedOutput) {
            this.variant = variant;
            this.description = description;
            this.inputData = inputData;
            this.expectedOutput = expectedOutput;
        }

        public Variant getVariant() {
            return this.variant;
        }

        public String getDescription() {
            return this.description;
        }

        public Map<String, Object> getInputData() {
            return this.inputData;
        }

        public Map<String, Object> getExpectedOutput() {
            return this.expectedOutput;
        }

        public static enum Variant {
            HAPPY_PATH,
            EMPTY_RESULT,
            NULL_VALUE,
            MAX_VALUE,
            MIN_VALUE,
            BOUNDARY;

        }
    }
}

