package com.flowpoc.analyzer;

import com.flowpoc.model.*;

import java.util.*;

/**
 * Detects patterns in a flow where two or more separate queries can be
 * collapsed into a single aggregation pipeline, and where existing pipelines
 * are missing important stages.
 *
 * Patterns detected:
 *   AGGREGATION_REWRITE      – FIND A then FIND B by FK → suggest $lookup
 *   AGGREGATION_REWRITE      – multiple FINDs on same collection → suggest $in
 *   PIPELINE_MISSING_PROJECT – aggregation has no $project stage
 *   PIPELINE_UNBOUNDED_SORT  – aggregation has $sort but no $limit
 */
public class AggregationRewriteAnalyzer implements OptimizationAnalyzer {

    @Override
    public List<OptimizationFinding> analyze(FlowResult flowResult) {
        List<OptimizationFinding> findings = new ArrayList<>();
        findings.addAll(detectLookupOpportunities(flowResult));
        findings.addAll(detectInQueryOpportunities(flowResult));
        findings.addAll(detectPipelineIssues(flowResult));
        return findings;
    }

    // ── FIND A then FIND B by FK → suggest $lookup ────────────────────────────

    private List<OptimizationFinding> detectLookupOpportunities(FlowResult flowResult) {
        List<OptimizationFinding> findings = new ArrayList<>();
        List<FlowStep> allSteps = flowResult.allSteps();

        for (int i = 0; i < allSteps.size() - 1; i++) {
            FlowStep stepA = allSteps.get(i);
            FlowStep stepB = allSteps.get(i + 1);

            String colA = firstSelectTable(stepA);
            String colB = firstSelectTable(stepB);
            if (colA == null || colB == null || colA.equals(colB)) continue;

            // B queries by a field that looks like a FK into A
            boolean bHasFkFilter = stepB.getQueries().stream()
                    .flatMap(q -> q.getPredicates().stream())
                    .anyMatch(p -> {
                        String col = p.getColumn();
                        if (col == null) return false;
                        String c = col.toLowerCase();
                        // FK pattern: references other collection name or ends with Id/_id
                        return c.endsWith("id") || c.endsWith("_id")
                                || c.contains(colA.toLowerCase().replaceAll("s$", "")); // e.g. orderId on orders
                    });

            if (bHasFkFilter) {
                String locA = stepA.getClassName() + "." + stepA.getMethodName();
                findings.add(new OptimizationFinding(
                        OptimizationFinding.Category.AGGREGATION_REWRITE,
                        OptimizationFinding.Severity.HIGH,
                        colA + " + " + colB, null,
                        "'" + colA + "' is fetched, then '" + colB + "' is queried by its FK — "
                            + "two round-trips where one $lookup aggregation suffices.",
                        locA,
                        "FIND " + colA + " → FIND " + colB + " by FK",
                        "db." + colA + ".aggregate([\n"
                            + "  { $lookup: { from: \"" + colB + "\", "
                            + "localField: \"_id\", foreignField: \"" + singularize(colA) + "Id\", "
                            + "as: \"" + colB + "\" } }\n"
                            + "])"));
            }
        }
        return findings;
    }

    // ── Multiple FINDs on same collection → suggest $in ───────────────────────

    private List<OptimizationFinding> detectInQueryOpportunities(FlowResult flowResult) {
        List<OptimizationFinding> findings = new ArrayList<>();

        // collect FIND queries grouped by collection
        Map<String, List<ExtractedQuery>> byCollection = new LinkedHashMap<>();
        Map<String, FlowStep> firstStepForCollection = new LinkedHashMap<>();

        for (FlowStep step : flowResult.allSteps()) {
            for (ExtractedQuery eq : step.getQueries()) {
                if (eq.getType() != ExtractedQuery.QueryType.SELECT) continue;
                String col = eq.getTableName();
                if (col == null) continue;
                byCollection.computeIfAbsent(col, k -> new ArrayList<>()).add(eq);
                firstStepForCollection.putIfAbsent(col, step);
            }
        }

        for (Map.Entry<String, List<ExtractedQuery>> entry : byCollection.entrySet()) {
            List<ExtractedQuery> queries = entry.getValue();
            if (queries.size() < 2) continue;

            // Check if all queries filter on the same field (different values) — $in candidate
            Set<String> filterFields = new HashSet<>();
            for (ExtractedQuery eq : queries) {
                eq.getPredicates().stream()
                        .map(Predicate::getColumn)
                        .filter(Objects::nonNull)
                        .forEach(filterFields::add);
            }

            FlowStep first = firstStepForCollection.get(entry.getKey());
            String loc = first != null ? first.getClassName() + "." + first.getMethodName() : "unknown";
            String commonField = filterFields.size() == 1 ? filterFields.iterator().next() : null;

            findings.add(new OptimizationFinding(
                    OptimizationFinding.Category.AGGREGATION_REWRITE,
                    OptimizationFinding.Severity.MEDIUM,
                    entry.getKey(), commonField,
                    queries.size() + " separate FIND operations on '" + entry.getKey()
                        + "' — consolidate into a single query"
                        + (commonField != null ? " using { " + commonField + ": { $in: [...] } }" : ""),
                    loc,
                    queries.size() + " FINDs on " + entry.getKey(),
                    commonField != null
                        ? "db." + entry.getKey() + ".find({ " + commonField + ": { $in: [val1, val2, ...] } })"
                        : "Use a single aggregation or $in query to batch " + queries.size() + " reads into one"));
        }
        return findings;
    }

    // ── Existing pipeline issues ───────────────────────────────────────────────

    private List<OptimizationFinding> detectPipelineIssues(FlowResult flowResult) {
        List<OptimizationFinding> findings = new ArrayList<>();

        for (FlowStep step : flowResult.allSteps()) {
            for (ExtractedQuery eq : step.getQueries()) {
                if (!eq.isAggregation() || eq.getAggregationPipeline() == null) continue;

                String pipeline = eq.getAggregationPipeline().toLowerCase();
                String loc = step.getClassName() + "." + step.getMethodName();
                String table = eq.getTableName() != null ? eq.getTableName() : "unknown";

                // No $project → returns all fields
                if (!pipeline.contains("$project")) {
                    findings.add(new OptimizationFinding(
                            OptimizationFinding.Category.PIPELINE_MISSING_PROJECT,
                            OptimizationFinding.Severity.LOW,
                            table, null,
                            "Aggregation on '" + table + "' has no $project stage — "
                                + "all document fields are returned. "
                                + "Add $project to return only the fields the caller needs.",
                            loc,
                            eq.getAggregationPipeline(),
                            "Add { $project: { field1: 1, field2: 1, _id: 0 } } as the last stage"));
                }

                // $sort present but no $limit
                if (pipeline.contains("$sort") && !pipeline.contains("$limit")) {
                    findings.add(new OptimizationFinding(
                            OptimizationFinding.Category.PIPELINE_UNBOUNDED_SORT,
                            OptimizationFinding.Severity.HIGH,
                            table, null,
                            "Aggregation on '" + table + "' has $sort but no $limit — "
                                + "MongoDB must sort the entire result set in memory before "
                                + "returning. This can cause OOM on large collections.",
                            loc,
                            eq.getAggregationPipeline(),
                            "Add { $limit: N } immediately after { $sort: ... }"));
                }
            }
        }
        return findings;
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private String firstSelectTable(FlowStep step) {
        return step.getQueries().stream()
                .filter(q -> q.getType() == ExtractedQuery.QueryType.SELECT)
                .map(ExtractedQuery::getTableName)
                .filter(Objects::nonNull)
                .findFirst().orElse(null);
    }

    private String singularize(String name) {
        if (name == null) return "id";
        String n = name.toLowerCase();
        if (n.endsWith("ies")) return n.substring(0, n.length() - 3) + "y";
        if (n.endsWith("ses") || n.endsWith("xes") || n.endsWith("zes")) return n.substring(0, n.length() - 2);
        if (n.endsWith("s") && n.length() > 1) return n.substring(0, n.length() - 1);
        return n;
    }
}
