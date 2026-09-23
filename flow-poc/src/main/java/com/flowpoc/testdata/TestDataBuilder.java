/*
 * Decompiled with CFR 0.152.
 */
package com.flowpoc.testdata;

import com.flowpoc.model.ExtractedQuery;
import com.flowpoc.model.Predicate;
import com.flowpoc.model.TestDataSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public final class TestDataBuilder {
    private TestDataBuilder() {
    }

    public static TestDataSet from(ExtractedQuery query, List<Map<String, Object>> rows) {
        TestDataSet tds = new TestDataSet(query.getSourceClass() + "." + query.getSourceMethod(), query.getRawSql(), query.getTableName(), query.getPredicates());
        rows.forEach(tds::addRow);
        return tds;
    }

    public static TestDataSet empty(ExtractedQuery query) {
        return new TestDataSet(query.getSourceClass() + "." + query.getSourceMethod(), query.getRawSql(), query.getTableName(), query.getPredicates());
    }

    public static void appendStaticEdgeCases(TestDataSet tds, List<Predicate> predicates) {
        TestDataBuilder.appendStaticEdgeCases(tds, predicates, null);
    }

    public static void appendStaticEdgeCases(TestDataSet tds, List<Predicate> predicates, Map<String, Object> entitySample) {
        String happyDesc;
        LinkedHashMap<String, Object> input;
        if (!tds.getTestCases().isEmpty()) {
            return;
        }
        if (predicates.isEmpty()) {
            if (entitySample != null && !entitySample.isEmpty()) {
                input = new LinkedHashMap<String, Object>(entitySample);
                happyDesc = "Sample document for '" + tds.getTable() + "' (derived from entity schema)";
            } else {
                input = new LinkedHashMap<String, Object>(Collections.singletonMap("_any_", "any record"));
                happyDesc = "Ensure '" + tds.getTable() + "' has at least one document";
            }
        } else {
            input = new LinkedHashMap();
            for (Predicate p : predicates) {
                if (p.getColumn() == null) continue;
                Object val = null;
                val = p.getResolvedValue() != null ? p.getResolvedValue() : (entitySample != null && entitySample.containsKey(p.getColumn()) ? entitySample.get(p.getColumn()) : (p.getRawValue() != null && !p.getRawValue().startsWith("?") && !p.getRawValue().startsWith(":") ? p.getRawValue() : "<" + p.getColumn() + ">"));
                input.put(p.getColumn(), val);
            }
            happyDesc = "Insert records with: " + TestDataBuilder.predicateSummary(predicates);
        }
        tds.addTestCase(new TestDataSet.TestCase(TestDataSet.TestCase.Variant.HAPPY_PATH, happyDesc, input, Collections.emptyMap()));
        LinkedHashMap<String, Object> noMatch = new LinkedHashMap<String, Object>();
        for (Predicate p : predicates) {
            if (p.getColumn() == null) continue;
            noMatch.put(p.getColumn(), "__no_match_" + UUID.randomUUID().toString().substring(0, 8));
        }
        tds.addTestCase(new TestDataSet.TestCase(TestDataSet.TestCase.Variant.EMPTY_RESULT, (String)(predicates.isEmpty() ? "Empty '" + tds.getTable() + "' collection \u2192 result is empty list" : "No records match predicates \u2192 downstream must handle empty"), noMatch.isEmpty() ? Collections.<String, Object>singletonMap("_any_", "empty collection") : noMatch, Collections.emptyMap()));
        for (Predicate p : predicates) {
            if (p.getColumn() == null || "_id".equals(p.getColumn())) continue;
            tds.addTestCase(new TestDataSet.TestCase(TestDataSet.TestCase.Variant.NULL_VALUE, "Null value for '" + p.getColumn() + "' \u2014 verify null-handling", Collections.singletonMap(p.getColumn(), null), Collections.emptyMap()));
        }
    }

    private static String predicateSummary(List<Predicate> predicates) {
        return predicates.stream().filter(p -> p.getColumn() != null).map(p -> p.getColumn() + " " + p.getOp().name() + " " + p.getRawValue()).collect(Collectors.joining(", "));
    }

    public static void appendEdgeCases(TestDataSet tds, List<Map<String, Object>> rows, List<Predicate> predicates) {
        if (!rows.isEmpty()) {
            tds.addTestCase(TestDataBuilder.happyPath(rows.get(0), predicates));
            tds.addTestCase(TestDataBuilder.emptyResult(predicates));
            for (Predicate pred : predicates) {
                Object minVal;
                String col = pred.getColumn();
                if (col == null) continue;
                Object maxVal = rows.stream().map(r -> r.get(col)).filter(v -> v instanceof Number).max(Comparator.comparingDouble(v -> ((Number)v).doubleValue())).orElse(null);
                if (maxVal != null) {
                    tds.addTestCase(TestDataBuilder.boundary(col, maxVal, predicates, TestDataSet.TestCase.Variant.MAX_VALUE));
                }
                if ((minVal = rows.stream().map(r -> r.get(col)).filter(v -> v instanceof Number).min(Comparator.comparingDouble(v -> ((Number)v).doubleValue())).orElse(null)) != null) {
                    tds.addTestCase(TestDataBuilder.boundary(col, minVal, predicates, TestDataSet.TestCase.Variant.MIN_VALUE));
                }
                if (col.equals("_id")) continue;
                tds.addTestCase(TestDataBuilder.nullCase(col, predicates));
            }
        }
    }

    private static TestDataSet.TestCase happyPath(Map<String, Object> row, List<Predicate> predicates) {
        LinkedHashMap<String, Object> input = new LinkedHashMap<String, Object>();
        for (Predicate p : predicates) {
            if (p.getColumn() == null) continue;
            input.put(p.getColumn(), row.getOrDefault(p.getColumn(), "?"));
        }
        return new TestDataSet.TestCase(TestDataSet.TestCase.Variant.HAPPY_PATH, "Normal case \u2014 row exists matching all predicates", input, new LinkedHashMap<String, Object>(row));
    }

    private static TestDataSet.TestCase emptyResult(List<Predicate> predicates) {
        LinkedHashMap<String, Object> input = new LinkedHashMap<String, Object>();
        for (Predicate p : predicates) {
            if (p.getColumn() == null) continue;
            input.put(p.getColumn(), "__NO_MATCH_" + String.valueOf(UUID.randomUUID()));
        }
        return new TestDataSet.TestCase(TestDataSet.TestCase.Variant.EMPTY_RESULT, "No records match \u2014 downstream must handle empty result", input, Collections.emptyMap());
    }

    private static TestDataSet.TestCase boundary(String col, Object val, List<Predicate> predicates, TestDataSet.TestCase.Variant variant) {
        LinkedHashMap<String, Object> input = new LinkedHashMap<String, Object>();
        for (Predicate p : predicates) {
            input.put(p.getColumn(), col.equals(p.getColumn()) ? val : p.getRawValue());
        }
        return new TestDataSet.TestCase(variant, String.valueOf((Object)variant) + " value (" + String.valueOf(val) + ") for column " + col, input, Collections.singletonMap(col, val));
    }

    private static TestDataSet.TestCase nullCase(String col, List<Predicate> predicates) {
        LinkedHashMap<String, Object> input = new LinkedHashMap<String, Object>();
        for (Predicate p : predicates) {
            input.put(p.getColumn(), col.equals(p.getColumn()) ? null : p.getRawValue());
        }
        return new TestDataSet.TestCase(TestDataSet.TestCase.Variant.NULL_VALUE, "Null value for column " + col + " \u2014 test null-handling path", input, Collections.singletonMap(col, null));
    }
}

