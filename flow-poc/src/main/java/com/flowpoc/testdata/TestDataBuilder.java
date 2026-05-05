package com.flowpoc.testdata;

import com.flowpoc.model.*;

import java.util.*;

/**
 * Builder (Builder pattern + Factory methods) for constructing TestDataSet instances
 * from raw DB rows. Also generates edge-case TestCase variants automatically.
 *
 * Separated from DataFetcher so the test-case generation logic is reusable
 * regardless of which DB driver is in use (SRP).
 */
public final class TestDataBuilder {

    private TestDataBuilder() {}

    public static TestDataSet from(ExtractedQuery query, List<Map<String, Object>> rows) {
        TestDataSet tds = new TestDataSet(
                query.getSourceClass() + "." + query.getSourceMethod(),
                query.getRawSql(),
                query.getTableName(),
                query.getPredicates());

        rows.forEach(tds::addRow);
        return tds;
    }

    public static TestDataSet empty(ExtractedQuery query) {
        return new TestDataSet(
                query.getSourceClass() + "." + query.getSourceMethod(),
                query.getRawSql(),
                query.getTableName(),
                query.getPredicates());
    }

    /**
     * Appends standard edge-case variants based on the fetched sample rows and predicates.
     *
     * Generated variants:
     *  HAPPY_PATH   — first real row that satisfies all predicates
     *  EMPTY_RESULT — modify a predicate to a value that won't match anything
     *  NULL_VALUE   — set a nullable predicate column to null
     *  MAX_VALUE    — use the maximum observed value for numeric/date columns
     *  MIN_VALUE    — use the minimum observed value for numeric/date columns
     *  BOUNDARY     — value one step above/below an EQ predicate
     */
    public static void appendEdgeCases(TestDataSet tds,
                                        List<Map<String, Object>> rows,
                                        List<Predicate> predicates) {
        if (!rows.isEmpty()) {
            tds.addTestCase(happyPath(rows.get(0), predicates));
            tds.addTestCase(emptyResult(predicates));
        }

        // Numeric/date boundary cases
        for (Predicate pred : predicates) {
            String col = pred.getColumn();
            if (col == null) continue;

            Object maxVal = rows.stream()
                    .map(r -> r.get(col))
                    .filter(v -> v instanceof Number)
                    .max(Comparator.comparingDouble(v -> ((Number) v).doubleValue()))
                    .orElse(null);

            if (maxVal != null) {
                tds.addTestCase(boundary(col, maxVal, predicates, TestDataSet.TestCase.Variant.MAX_VALUE));
            }

            Object minVal = rows.stream()
                    .map(r -> r.get(col))
                    .filter(v -> v instanceof Number)
                    .min(Comparator.comparingDouble(v -> ((Number) v).doubleValue()))
                    .orElse(null);

            if (minVal != null) {
                tds.addTestCase(boundary(col, minVal, predicates, TestDataSet.TestCase.Variant.MIN_VALUE));
            }

            // Null value case if column is not _id
            if (!col.equals("_id")) {
                tds.addTestCase(nullCase(col, predicates));
            }
        }
    }

    private static TestDataSet.TestCase happyPath(Map<String, Object> row,
                                                   List<Predicate> predicates) {
        Map<String, Object> input = new LinkedHashMap<>();
        for (Predicate p : predicates) {
            if (p.getColumn() != null) input.put(p.getColumn(), row.getOrDefault(p.getColumn(), "?"));
        }
        return new TestDataSet.TestCase(
                TestDataSet.TestCase.Variant.HAPPY_PATH,
                "Normal case — row exists matching all predicates",
                input, new LinkedHashMap<>(row));
    }

    private static TestDataSet.TestCase emptyResult(List<Predicate> predicates) {
        Map<String, Object> input = new LinkedHashMap<>();
        for (Predicate p : predicates) {
            if (p.getColumn() != null) input.put(p.getColumn(), "__NO_MATCH_" + UUID.randomUUID());
        }
        return new TestDataSet.TestCase(
                TestDataSet.TestCase.Variant.EMPTY_RESULT,
                "No records match — downstream must handle empty result",
                input, Collections.emptyMap());
    }

    private static TestDataSet.TestCase boundary(String col, Object val,
                                                  List<Predicate> predicates,
                                                  TestDataSet.TestCase.Variant variant) {
        Map<String, Object> input = new LinkedHashMap<>();
        for (Predicate p : predicates) {
            input.put(p.getColumn(), col.equals(p.getColumn()) ? val : p.getRawValue());
        }
        return new TestDataSet.TestCase(variant,
                variant + " value (" + val + ") for column " + col,
                input, Collections.singletonMap(col, val));
    }

    private static TestDataSet.TestCase nullCase(String col, List<Predicate> predicates) {
        Map<String, Object> input = new LinkedHashMap<>();
        for (Predicate p : predicates) {
            input.put(p.getColumn(), col.equals(p.getColumn()) ? null : p.getRawValue());
        }
        return new TestDataSet.TestCase(TestDataSet.TestCase.Variant.NULL_VALUE,
                "Null value for column " + col + " — test null-handling path",
                input, Collections.singletonMap(col, null));
    }
}
