/*
 * Decompiled with CFR 0.152.
 */
package com.flowpoc.analyzer;

import com.flowpoc.analyzer.OptimizationAnalyzer;
import com.flowpoc.model.ExtractedQuery;
import com.flowpoc.model.FlowResult;
import com.flowpoc.model.FlowStep;
import com.flowpoc.model.OptimizationFinding;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class HierarchyFlowAnalyzer
implements OptimizationAnalyzer {
    @Override
    public List<OptimizationFinding> analyze(FlowResult flowResult) {
        ArrayList<OptimizationFinding> findings = new ArrayList<OptimizationFinding>();
        Map<Integer, List<FlowStep>> byDepth = flowResult.getStepsByDepth();
        findings.addAll(this.detectControllerDbAccess(byDepth));
        findings.addAll(this.detectCrossLayerQueries(byDepth));
        findings.addAll(this.detectUnboundedDeepReads(byDepth));
        return findings;
    }

    private List<OptimizationFinding> detectControllerDbAccess(Map<Integer, List<FlowStep>> byDepth) {
        ArrayList<OptimizationFinding> findings = new ArrayList<OptimizationFinding>();
        List<FlowStep> controllers = byDepth.getOrDefault(0, Collections.emptyList());
        for (FlowStep step : controllers) {
            if (step.getKind() != FlowStep.StepKind.CONTROLLER) continue;
            for (ExtractedQuery eq : step.getQueries()) {
                if (eq.getType() != ExtractedQuery.QueryType.SELECT && eq.getType() != ExtractedQuery.QueryType.INSERT && eq.getType() != ExtractedQuery.QueryType.UPDATE && eq.getType() != ExtractedQuery.QueryType.DELETE) continue;
                findings.add(new OptimizationFinding(OptimizationFinding.Category.CONTROLLER_DB_ACCESS, OptimizationFinding.Severity.HIGH, eq.getTableName(), null, "Controller '" + step.label() + "." + step.getMethodName() + "' directly accesses the database. DB calls should be delegated to a @Service or @Repository.", step.getClassName() + "." + step.getMethodName(), eq.getRawSql(), "Extract DB call into a @Service method, inject the service into the controller"));
            }
        }
        return findings;
    }

    private List<OptimizationFinding> detectCrossLayerQueries(Map<Integer, List<FlowStep>> byDepth) {
        ArrayList<OptimizationFinding> findings = new ArrayList<OptimizationFinding>();
        LinkedHashMap<String, Set<Integer>> collectionDepths = new LinkedHashMap<String, Set<Integer>>();
        LinkedHashMap<String, FlowStep> collectionFirstStep = new LinkedHashMap<String, FlowStep>();
        for (Map.Entry<Integer, List<FlowStep>> entry : byDepth.entrySet()) {
            int depth = entry.getKey();
            for (FlowStep step : entry.getValue()) {
                for (ExtractedQuery eq : step.getQueries()) {
                    String col;
                    if (eq.getType() != ExtractedQuery.QueryType.SELECT || (col = eq.getTableName()) == null) continue;
                    collectionDepths.computeIfAbsent(col, k -> new TreeSet<Integer>()).add(depth);
                    collectionFirstStep.putIfAbsent(col, step);
                }
            }
        }
        for (Map.Entry<String, Set<Integer>> entry : collectionDepths.entrySet()) {
            Set<Integer> depths = entry.getValue();
            if (depths.size() < 2) continue;
            int minDepth = depths.iterator().next();
            int maxDepth = ((TreeSet<Integer>)depths).last();
            if (maxDepth - minDepth < 1) continue;
            FlowStep first = collectionFirstStep.get(entry.getKey());
            String location = first != null ? first.getClassName() + "." + first.getMethodName() : "unknown";
            findings.add(new OptimizationFinding(OptimizationFinding.Category.CROSS_LAYER_QUERY, OptimizationFinding.Severity.MEDIUM, entry.getKey(), null, "Collection '" + entry.getKey() + "' is queried at depths " + String.valueOf(depths) + " \u2014 reading the same data at multiple call-tree layers. Consolidate reads into the deepest layer (repository) and pass results up via return values.", location, "queried at depths " + String.valueOf(depths), "Move all reads of '" + entry.getKey() + "' into a single repository method; service layer should use the returned value, not query again"));
        }
        return findings;
    }

    private List<OptimizationFinding> detectUnboundedDeepReads(Map<Integer, List<FlowStep>> byDepth) {
        ArrayList<OptimizationFinding> findings = new ArrayList<OptimizationFinding>();
        for (Map.Entry<Integer, List<FlowStep>> entry : byDepth.entrySet()) {
            if (entry.getKey() < 2) continue;
            for (FlowStep step : entry.getValue()) {
                if (this.isModelAccessor(step)) continue;
                for (ExtractedQuery eq : step.getQueries()) {
                    boolean noFilter;
                    if (eq.getType() != ExtractedQuery.QueryType.SELECT || !(noFilter = eq.getPredicates().isEmpty() && (eq.getAggregationPipeline() == null || !eq.getAggregationPipeline().contains("$match"))) || HierarchyFlowAnalyzer.rawSqlHasFilter(eq.getRawSql())) continue;
                    String loc = step.getClassName() + "." + step.getMethodName();
                    boolean isAgg = eq.getRawSql() != null && eq.getRawSql().startsWith("AGGREGATE");
                    String fix = isAgg ? "Add a $match stage as the first pipeline stage:\n  db." + eq.getTableName() + ".aggregate([{ $match: { field: value } }, ...])" : "Add a filter predicate or .limit(n) to the query; or if truly a full load, move to startup with @EventListener(ApplicationReadyEvent.class)";
                    findings.add(new OptimizationFinding(OptimizationFinding.Category.UNBOUNDED_DEEP_READ, OptimizationFinding.Severity.HIGH, eq.getTableName(), null, "Repository-level method '" + step.label() + "." + step.getMethodName() + "' performs an unbounded read on '" + eq.getTableName() + "' (no filter, no $match). This will scan the entire collection at depth " + String.valueOf(entry.getKey()) + ".", loc, eq.getRawSql(), fix));
                }
            }
        }
        return findings;
    }

    private static boolean rawSqlHasFilter(String rawSql) {
        if (rawSql == null) {
            return false;
        }
        return rawSql.contains("FILTER") || rawSql.contains("{");
    }

    private boolean isModelAccessor(FlowStep step) {
        if (step.getKind() != FlowStep.StepKind.OTHER) {
            return false;
        }
        String method = step.getMethodName();
        if (method == null) {
            return true;
        }
        if (method.startsWith("get") || method.startsWith("set") || method.startsWith("is") || method.startsWith("lambda") || method.startsWith("build") || method.equals("equals") || method.equals("hashCode") || method.equals("toString")) {
            return true;
        }
        String cls = step.getClassName();
        return cls != null && (cls.contains("$") || cls.endsWith("Builder"));
    }
}

