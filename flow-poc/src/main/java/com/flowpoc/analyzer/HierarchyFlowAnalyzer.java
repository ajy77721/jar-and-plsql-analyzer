package com.flowpoc.analyzer;

import com.flowpoc.model.*;

import java.util.*;

/**
 * Analyzes the call-tree level by level (top → bottom) and emits findings about:
 *
 *   CONTROLLER_DB_ACCESS   – a controller method directly calls DB (depth 0 with queries)
 *   CROSS_LAYER_QUERY      – same collection queried at both service and repo depths
 *   UNBOUNDED_DEEP_READ    – no LIMIT + no filter at depth ≥ 2 (inside repo / deep service)
 *
 * Reading order recommendation: the report also lists the deepest-to-shallowest
 * dependency chain so a consumer can decide load order (deepest ref data first,
 * transactional data on demand at request time).
 */
public class HierarchyFlowAnalyzer implements OptimizationAnalyzer {

    @Override
    public List<OptimizationFinding> analyze(FlowResult flowResult) {
        List<OptimizationFinding> findings = new ArrayList<>();
        Map<Integer, List<FlowStep>> byDepth = flowResult.getStepsByDepth();

        findings.addAll(detectControllerDbAccess(byDepth));
        findings.addAll(detectCrossLayerQueries(byDepth));
        findings.addAll(detectUnboundedDeepReads(byDepth));

        return findings;
    }

    // ── Controller directly queries DB ────────────────────────────────────────

    private List<OptimizationFinding> detectControllerDbAccess(
            Map<Integer, List<FlowStep>> byDepth) {
        List<OptimizationFinding> findings = new ArrayList<>();
        List<FlowStep> controllers = byDepth.getOrDefault(0, Collections.emptyList());

        for (FlowStep step : controllers) {
            if (step.getKind() != FlowStep.StepKind.CONTROLLER) continue;
            for (ExtractedQuery eq : step.getQueries()) {
                if (eq.getType() == ExtractedQuery.QueryType.SELECT
                        || eq.getType() == ExtractedQuery.QueryType.INSERT
                        || eq.getType() == ExtractedQuery.QueryType.UPDATE
                        || eq.getType() == ExtractedQuery.QueryType.DELETE) {
                    findings.add(new OptimizationFinding(
                            OptimizationFinding.Category.CONTROLLER_DB_ACCESS,
                            OptimizationFinding.Severity.HIGH,
                            eq.getTableName(), null,
                            "Controller '" + step.label() + "." + step.getMethodName()
                                + "' directly accesses the database. "
                                + "DB calls should be delegated to a @Service or @Repository.",
                            step.getClassName() + "." + step.getMethodName(),
                            eq.getRawSql(),
                            "Extract DB call into a @Service method, inject the service into the controller"));
                }
            }
        }
        return findings;
    }

    // ── Same collection queried at multiple depths ────────────────────────────

    private List<OptimizationFinding> detectCrossLayerQueries(
            Map<Integer, List<FlowStep>> byDepth) {
        List<OptimizationFinding> findings = new ArrayList<>();

        // Map: collection → set of depths where it is queried
        Map<String, Set<Integer>> collectionDepths = new LinkedHashMap<>();
        Map<String, FlowStep>     collectionFirstStep = new LinkedHashMap<>();

        for (Map.Entry<Integer, List<FlowStep>> entry : byDepth.entrySet()) {
            int depth = entry.getKey();
            for (FlowStep step : entry.getValue()) {
                for (ExtractedQuery eq : step.getQueries()) {
                    if (eq.getType() != ExtractedQuery.QueryType.SELECT) continue;
                    String col = eq.getTableName();
                    if (col == null) continue;
                    collectionDepths.computeIfAbsent(col, k -> new TreeSet<>()).add(depth);
                    collectionFirstStep.putIfAbsent(col, step);
                }
            }
        }

        for (Map.Entry<String, Set<Integer>> entry : collectionDepths.entrySet()) {
            Set<Integer> depths = entry.getValue();
            if (depths.size() < 2) continue;

            int minDepth = depths.iterator().next();
            int maxDepth = ((TreeSet<Integer>) depths).last();
            if (maxDepth - minDepth < 1) continue;

            FlowStep first = collectionFirstStep.get(entry.getKey());
            String location = first != null ? first.getClassName() + "." + first.getMethodName() : "unknown";

            findings.add(new OptimizationFinding(
                    OptimizationFinding.Category.CROSS_LAYER_QUERY,
                    OptimizationFinding.Severity.MEDIUM,
                    entry.getKey(), null,
                    "Collection '" + entry.getKey() + "' is queried at depths "
                        + depths + " — reading the same data at multiple call-tree layers. "
                        + "Consolidate reads into the deepest layer (repository) and pass "
                        + "results up via return values.",
                    location,
                    "queried at depths " + depths,
                    "Move all reads of '" + entry.getKey() + "' into a single repository method; "
                        + "service layer should use the returned value, not query again"));
        }
        return findings;
    }

    // ── Unbounded reads deep in call tree ─────────────────────────────────────

    private List<OptimizationFinding> detectUnboundedDeepReads(
            Map<Integer, List<FlowStep>> byDepth) {
        List<OptimizationFinding> findings = new ArrayList<>();

        for (Map.Entry<Integer, List<FlowStep>> entry : byDepth.entrySet()) {
            if (entry.getKey() < 2) continue; // only flag at depth 2+ (repository level)
            for (FlowStep step : entry.getValue()) {
                for (ExtractedQuery eq : step.getQueries()) {
                    if (eq.getType() != ExtractedQuery.QueryType.SELECT) continue;
                    boolean noFilter = eq.getPredicates().isEmpty()
                            && (eq.getAggregationPipeline() == null
                                || !eq.getAggregationPipeline().contains("$match"));
                    if (noFilter) {
                        String loc = step.getClassName() + "." + step.getMethodName();
                        findings.add(new OptimizationFinding(
                                OptimizationFinding.Category.UNBOUNDED_DEEP_READ,
                                OptimizationFinding.Severity.HIGH,
                                eq.getTableName(), null,
                                "Repository-level method '" + step.label() + "." + step.getMethodName()
                                    + "' performs an unbounded read on '" + eq.getTableName()
                                    + "' (no filter, no $match). This will scan the entire "
                                    + "collection at depth " + entry.getKey() + ".",
                                loc,
                                eq.getRawSql(),
                                "Add a filter predicate or .limit(n) to the query; "
                                    + "or if truly a full load, move to startup with @EventListener(ApplicationReadyEvent.class)"));
                    }
                }
            }
        }
        return findings;
    }
}
