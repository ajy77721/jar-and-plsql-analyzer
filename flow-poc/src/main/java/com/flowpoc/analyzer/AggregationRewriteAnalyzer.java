/*
 * Decompiled with CFR 0.152.
 */
package com.flowpoc.analyzer;

import com.flowpoc.analyzer.OptimizationAnalyzer;
import com.flowpoc.model.ExtractedQuery;
import com.flowpoc.model.FlowResult;
import com.flowpoc.model.FlowStep;
import com.flowpoc.model.OptimizationFinding;
import com.flowpoc.model.Predicate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class AggregationRewriteAnalyzer
implements OptimizationAnalyzer {
    @Override
    public List<OptimizationFinding> analyze(FlowResult flowResult) {
        ArrayList<OptimizationFinding> findings = new ArrayList<OptimizationFinding>();
        findings.addAll(this.detectLookupOpportunities(flowResult));
        findings.addAll(this.detectInQueryOpportunities(flowResult));
        findings.addAll(this.detectPipelineIssues(flowResult));
        return findings;
    }

    private List<OptimizationFinding> detectLookupOpportunities(FlowResult flowResult) {
        ArrayList<OptimizationFinding> findings = new ArrayList<OptimizationFinding>();
        List<FlowStep> allSteps = flowResult.allSteps();
        for (int i = 0; i < allSteps.size() - 1; ++i) {
            boolean bHasFkFilter;
            FlowStep stepA = allSteps.get(i);
            FlowStep stepB = allSteps.get(i + 1);
            String colA = this.firstSelectTable(stepA);
            String colB = this.firstSelectTable(stepB);
            if (colA == null || colB == null || colA.equals(colB) || !(bHasFkFilter = stepB.getQueries().stream().flatMap(q -> q.getPredicates().stream()).anyMatch(p -> {
                String col = p.getColumn();
                if (col == null) {
                    return false;
                }
                String c = col.toLowerCase();
                return c.endsWith("id") || c.endsWith("_id") || c.contains(colA.toLowerCase().replaceAll("s$", ""));
            }))) continue;
            String locA = stepA.getClassName() + "." + stepA.getMethodName();
            findings.add(new OptimizationFinding(OptimizationFinding.Category.AGGREGATION_REWRITE, OptimizationFinding.Severity.HIGH, colA + " + " + colB, null, "'" + colA + "' is fetched, then '" + colB + "' is queried by its FK \u2014 two round-trips where one $lookup aggregation suffices.", locA, "FIND " + colA + " \u2192 FIND " + colB + " by FK", "db." + colA + ".aggregate([\n  { $lookup: { from: \"" + colB + "\", localField: \"_id\", foreignField: \"" + this.singularize(colA) + "Id\", as: \"" + colB + "\" } }\n])"));
        }
        return findings;
    }

    private List<OptimizationFinding> detectInQueryOpportunities(FlowResult flowResult) {
        ArrayList<OptimizationFinding> findings = new ArrayList<OptimizationFinding>();
        LinkedHashMap<String, List<ExtractedQuery>> byCollection = new LinkedHashMap<>();
        LinkedHashMap<Object, FlowStep> firstStepForCollection = new LinkedHashMap<Object, FlowStep>();
        for (FlowStep flowStep : flowResult.allSteps()) {
            for (ExtractedQuery eq : flowStep.getQueries()) {
                Object col;
                if (eq.getType() != ExtractedQuery.QueryType.SELECT || (col = eq.getTableName()) == null) continue;
                byCollection.computeIfAbsent((String)col, k -> new ArrayList<>()).add(eq);
                firstStepForCollection.putIfAbsent(col, flowStep);
            }
        }
        for (Map.Entry<String, List<ExtractedQuery>> entry : byCollection.entrySet()) {
            List<ExtractedQuery> queries = entry.getValue();
            if (queries.size() < 2) continue;
            HashSet<String> filterFields = new HashSet<>();
            for (ExtractedQuery eq : queries) {
                eq.getPredicates().stream().map(Predicate::getColumn).filter(Objects::nonNull).forEach(filterFields::add);
            }
            FlowStep first = firstStepForCollection.get(entry.getKey());
            String loc = first != null ? first.getClassName() + "." + first.getMethodName() : "unknown";
            String commonField = filterFields.size() == 1 ? (String)filterFields.iterator().next() : null;
            findings.add(new OptimizationFinding(OptimizationFinding.Category.AGGREGATION_REWRITE, OptimizationFinding.Severity.MEDIUM, (String)entry.getKey(), commonField, queries.size() + " separate FIND operations on '" + (String)entry.getKey() + "' \u2014 consolidate into a single query" + (String)(commonField != null ? " using { " + commonField + ": { $in: [...] } }" : ""), loc, queries.size() + " FINDs on " + (String)entry.getKey(), commonField != null ? "db." + (String)entry.getKey() + ".find({ " + commonField + ": { $in: [val1, val2, ...] } })" : "Use a single aggregation or $in query to batch " + queries.size() + " reads into one"));
        }
        return findings;
    }

    private List<OptimizationFinding> detectPipelineIssues(FlowResult flowResult) {
        ArrayList<OptimizationFinding> findings = new ArrayList<OptimizationFinding>();
        for (FlowStep step : flowResult.allSteps()) {
            for (ExtractedQuery eq : step.getQueries()) {
                String table;
                if (!eq.isAggregation() || eq.getAggregationPipeline() == null) continue;
                String pipeline = eq.getAggregationPipeline().toLowerCase();
                String loc = step.getClassName() + "." + step.getMethodName();
                String string = table = eq.getTableName() != null ? eq.getTableName() : "unknown";
                if (!pipeline.contains("$project")) {
                    findings.add(new OptimizationFinding(OptimizationFinding.Category.PIPELINE_MISSING_PROJECT, OptimizationFinding.Severity.LOW, table, null, "Aggregation on '" + table + "' has no $project stage \u2014 all document fields are returned. Add $project to return only the fields the caller needs.", loc, eq.getAggregationPipeline(), "Add { $project: { field1: 1, field2: 1, _id: 0 } } as the last stage"));
                }
                if (!pipeline.contains("$sort") || pipeline.contains("$limit")) continue;
                findings.add(new OptimizationFinding(OptimizationFinding.Category.PIPELINE_UNBOUNDED_SORT, OptimizationFinding.Severity.HIGH, table, null, "Aggregation on '" + table + "' has $sort but no $limit \u2014 MongoDB must sort the entire result set in memory before returning. This can cause OOM on large collections.", loc, eq.getAggregationPipeline(), "Add { $limit: N } immediately after { $sort: ... }"));
            }
        }
        return findings;
    }

    private String firstSelectTable(FlowStep step) {
        return step.getQueries().stream().filter(q -> q.getType() == ExtractedQuery.QueryType.SELECT).map(ExtractedQuery::getTableName).filter(Objects::nonNull).findFirst().orElse(null);
    }

    private String singularize(String name) {
        if (name == null) {
            return "id";
        }
        String n = name.toLowerCase();
        if (n.endsWith("ies")) {
            return n.substring(0, n.length() - 3) + "y";
        }
        if (n.endsWith("ses") || n.endsWith("xes") || n.endsWith("zes")) {
            return n.substring(0, n.length() - 2);
        }
        if (n.endsWith("s") && n.length() > 1) {
            return n.substring(0, n.length() - 1);
        }
        return n;
    }
}

