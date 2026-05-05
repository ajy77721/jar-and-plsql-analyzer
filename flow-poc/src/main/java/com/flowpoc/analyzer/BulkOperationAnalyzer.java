package com.flowpoc.analyzer;

import com.flowpoc.model.*;

import java.util.*;

/**
 * Detects opportunities to replace multiple single-document writes/reads
 * with bulk operations (insertMany, bulkWrite, $in queries).
 *
 * Pattern — Bulk Write:  ≥2 INSERT or UPDATE on same collection in same flow
 * Pattern — Bulk Read:   ≥2 FIND on same collection with different predicates at same depth
 * Pattern — Prefetch:    child FIND happens directly after parent FIND with matching _id field
 */
public class BulkOperationAnalyzer implements OptimizationAnalyzer {

    @Override
    public List<OptimizationFinding> analyze(FlowResult flowResult) {
        List<OptimizationFinding> findings = new ArrayList<>();
        findings.addAll(detectBulkWrites(flowResult));
        findings.addAll(detectBulkReads(flowResult));
        findings.addAll(detectPrefetchCandidates(flowResult));
        return findings;
    }

    private List<OptimizationFinding> detectBulkWrites(FlowResult flowResult) {
        List<OptimizationFinding> findings = new ArrayList<>();
        Map<String, List<FlowStep>> writesByCollection = new LinkedHashMap<>();

        for (FlowStep step : flowResult.allSteps()) {
            for (ExtractedQuery eq : step.getQueries()) {
                if (eq.getType() == ExtractedQuery.QueryType.INSERT
                        || eq.getType() == ExtractedQuery.QueryType.UPDATE) {
                    writesByCollection.computeIfAbsent(eq.getTableName(), k -> new ArrayList<>())
                            .add(step);
                }
            }
        }

        for (Map.Entry<String, List<FlowStep>> entry : writesByCollection.entrySet()) {
            List<FlowStep> steps = entry.getValue();
            if (steps.size() < 2) continue;
            FlowStep first = steps.get(0);
            findings.add(new OptimizationFinding(
                    OptimizationFinding.Category.BULK_WRITE,
                    entry.getKey(), null,
                    steps.size() + " write operations on '" + entry.getKey()
                            + "' in this flow — consolidate into insertMany / bulkWrite",
                    first.getClassName() + "." + first.getMethodName(),
                    steps.size() + " writes to " + entry.getKey()));
        }
        return findings;
    }

    private List<OptimizationFinding> detectBulkReads(FlowResult flowResult) {
        List<OptimizationFinding> findings = new ArrayList<>();

        // group FIND queries on the same collection at the same depth
        Map<String, Map<Integer, List<ExtractedQuery>>> byCollectionDepth = new LinkedHashMap<>();
        for (FlowStep step : flowResult.allSteps()) {
            for (ExtractedQuery eq : step.getQueries()) {
                if (eq.getType() != ExtractedQuery.QueryType.SELECT) continue;
                byCollectionDepth
                        .computeIfAbsent(eq.getTableName(), k -> new LinkedHashMap<>())
                        .computeIfAbsent(step.getDepth(), k -> new ArrayList<>())
                        .add(eq);
            }
        }

        for (Map.Entry<String, Map<Integer, List<ExtractedQuery>>> ce : byCollectionDepth.entrySet()) {
            for (Map.Entry<Integer, List<ExtractedQuery>> de : ce.getValue().entrySet()) {
                List<ExtractedQuery> queries = de.getValue();
                if (queries.size() < 2) continue;
                findings.add(new OptimizationFinding(
                        OptimizationFinding.Category.BULK_READ,
                        ce.getKey(), null,
                        queries.size() + " separate FIND operations on '" + ce.getKey()
                                + "' at depth " + de.getKey()
                                + " — consider $in query or single aggregation pipeline",
                        ce.getKey(),
                        queries.size() + " FINDs on " + ce.getKey()));
            }
        }
        return findings;
    }

    private List<OptimizationFinding> detectPrefetchCandidates(FlowResult flowResult) {
        List<OptimizationFinding> findings = new ArrayList<>();
        detectPrefetch(flowResult.getRootStep(), findings);
        return findings;
    }

    private void detectPrefetch(FlowStep step, List<OptimizationFinding> findings) {
        for (int i = 0; i < step.getChildren().size() - 1; i++) {
            FlowStep a = step.getChildren().get(i);
            FlowStep b = step.getChildren().get(i + 1);

            String colA = firstSelectCollection(a);
            String colB = firstSelectCollection(b);
            if (colA == null || colB == null || colA.equals(colB)) continue;

            // If B's predicate references an _id-like field, it's likely fed from A's result
            boolean bHasIdFilter = b.getQueries().stream()
                    .flatMap(q -> q.getPredicates().stream())
                    .anyMatch(p -> p.getColumn() != null
                            && (p.getColumn().endsWith("Id") || p.getColumn().endsWith("_id")
                                || p.getColumn().equalsIgnoreCase("id")));

            if (bHasIdFilter) {
                findings.add(new OptimizationFinding(
                        OptimizationFinding.Category.PREFETCH_CANDIDATE,
                        colA + "+" + colB, null,
                        "'" + colA + "' is fetched then '" + colB + "' is queried by its id — "
                                + "consider $lookup (single aggregation) or a combined projection",
                        a.getClassName() + " → " + b.getClassName(),
                        "FIND " + colA + " → FIND " + colB + " by id"));
            }
        }
        for (FlowStep child : step.getChildren()) detectPrefetch(child, findings);
    }

    private String firstSelectCollection(FlowStep step) {
        return step.getQueries().stream()
                .filter(q -> q.getType() == ExtractedQuery.QueryType.SELECT)
                .map(ExtractedQuery::getTableName)
                .filter(Objects::nonNull)
                .findFirst().orElse(null);
    }
}
