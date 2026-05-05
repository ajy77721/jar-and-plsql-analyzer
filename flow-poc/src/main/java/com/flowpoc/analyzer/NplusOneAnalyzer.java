package com.flowpoc.analyzer;

import com.flowpoc.model.*;

import java.util.*;

/**
 * Detects N+1 query patterns in a flow.
 *
 * Pattern: a FIND (parent) is followed by one or more FIND calls on a different
 * (child) collection at a deeper depth, with the child query's predicate referencing
 * an _id / foreign-key that likely comes from the parent's result set.
 *
 * Recommendation: replace with a $lookup aggregation (single round-trip).
 */
public class NplusOneAnalyzer implements OptimizationAnalyzer {

    @Override
    public List<OptimizationFinding> analyze(FlowResult flowResult) {
        List<OptimizationFinding> findings = new ArrayList<>();

        // Group FIND steps by collection
        Map<String, List<FlowStep>> findsByCollection = new LinkedHashMap<>();
        collectFinds(flowResult.getRootStep(), findsByCollection);

        // A collection appearing in multiple steps at depth > 0 inside a parent FIND is N+1
        for (Map.Entry<String, List<FlowStep>> entry : findsByCollection.entrySet()) {
            List<FlowStep> steps = entry.getValue();
            if (steps.size() < 2) continue;

            Set<Integer> depths = new HashSet<>();
            for (FlowStep s : steps) depths.add(s.getDepth());
            if (depths.size() == 1) continue; // all at same depth — not N+1

            String collection = entry.getKey();
            FlowStep first = steps.get(0);
            findings.add(new OptimizationFinding(
                    OptimizationFinding.Category.N_PLUS_ONE,
                    collection, null,
                    "Collection '" + collection + "' is queried " + steps.size()
                            + " times across different call depths — possible N+1. "
                            + "Consider $lookup or batch findAllById.",
                    first.getClassName() + "." + first.getMethodName(),
                    "Repeated FIND on " + collection));
        }
        return findings;
    }

    private void collectFinds(FlowStep step,
                               Map<String, List<FlowStep>> findsByCollection) {
        if (step == null) return;
        for (ExtractedQuery eq : step.getQueries()) {
            if (eq.getType() == ExtractedQuery.QueryType.SELECT && eq.getTableName() != null) {
                findsByCollection.computeIfAbsent(eq.getTableName(), k -> new ArrayList<>()).add(step);
            }
        }
        for (FlowStep child : step.getChildren()) {
            collectFinds(child, findsByCollection);
        }
    }
}
