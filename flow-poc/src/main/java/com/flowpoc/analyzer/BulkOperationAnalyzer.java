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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class BulkOperationAnalyzer
implements OptimizationAnalyzer {
    @Override
    public List<OptimizationFinding> analyze(FlowResult flowResult) {
        ArrayList<OptimizationFinding> findings = new ArrayList<OptimizationFinding>();
        findings.addAll(this.detectBulkWrites(flowResult));
        findings.addAll(this.detectBulkReads(flowResult));
        findings.addAll(this.detectPrefetchCandidates(flowResult));
        return findings;
    }

    private List<OptimizationFinding> detectBulkWrites(FlowResult flowResult) {
        ArrayList<OptimizationFinding> findings = new ArrayList<OptimizationFinding>();
        LinkedHashMap<String, List<FlowStep>> writesByCollection = new LinkedHashMap<String, List<FlowStep>>();
        for (FlowStep flowStep : flowResult.allSteps()) {
            for (ExtractedQuery eq : flowStep.getQueries()) {
                if (eq.getType() != ExtractedQuery.QueryType.INSERT && eq.getType() != ExtractedQuery.QueryType.UPDATE) continue;
                writesByCollection.computeIfAbsent(eq.getTableName(), k -> new ArrayList<FlowStep>()).add(flowStep);
            }
        }
        for (Map.Entry<String, List<FlowStep>> entry : writesByCollection.entrySet()) {
            List<FlowStep> steps = entry.getValue();
            if (steps.size() < 2) continue;
            FlowStep first = steps.get(0);
            findings.add(new OptimizationFinding(OptimizationFinding.Category.BULK_WRITE, OptimizationFinding.Severity.MEDIUM, entry.getKey(), null, steps.size() + " write operations on '" + entry.getKey() + "' in this flow \u2014 consolidate into insertMany / bulkWrite", first.getClassName() + "." + first.getMethodName(), steps.size() + " writes to " + entry.getKey(), "Replace individual inserts with: mongoTemplate.insertAll(List.of(doc1, doc2, ...)) or bulkOps.insert(...).execute()"));
        }
        return findings;
    }

    private List<OptimizationFinding> detectBulkReads(FlowResult flowResult) {
        ArrayList<OptimizationFinding> findings = new ArrayList<OptimizationFinding>();
        LinkedHashMap<String, Map<Integer, List<ExtractedQuery>>> byCollectionDepth = new LinkedHashMap<String, Map<Integer, List<ExtractedQuery>>>();
        for (FlowStep flowStep : flowResult.allSteps()) {
            for (ExtractedQuery extractedQuery : flowStep.getQueries()) {
                if (extractedQuery.getType() != ExtractedQuery.QueryType.SELECT) continue;
                byCollectionDepth.computeIfAbsent(extractedQuery.getTableName(), k -> new LinkedHashMap<Integer, List<ExtractedQuery>>()).computeIfAbsent(flowStep.getDepth(), k -> new ArrayList<ExtractedQuery>()).add(extractedQuery);
            }
        }
        for (Map.Entry<String, Map<Integer, List<ExtractedQuery>>> entry : byCollectionDepth.entrySet()) {
            for (Map.Entry<Integer, List<ExtractedQuery>> entry2 : entry.getValue().entrySet()) {
                List<ExtractedQuery> queries = entry2.getValue();
                if (queries.size() < 2) continue;
                findings.add(new OptimizationFinding(OptimizationFinding.Category.BULK_READ, OptimizationFinding.Severity.MEDIUM, entry.getKey(), null, queries.size() + " separate FIND operations on '" + entry.getKey() + "' at depth " + String.valueOf(entry2.getKey()) + " \u2014 consider $in query or single aggregation pipeline", entry.getKey(), queries.size() + " FINDs on " + entry.getKey(), "Replace separate FINDs with: mongoTemplate.find(Query.query(Criteria.where(\"field\").in(val1, val2)), ...)"));
            }
        }
        return findings;
    }

    private List<OptimizationFinding> detectPrefetchCandidates(FlowResult flowResult) {
        ArrayList<OptimizationFinding> findings = new ArrayList<OptimizationFinding>();
        this.detectPrefetch(flowResult.getRootStep(), findings);
        return findings;
    }

    private void detectPrefetch(FlowStep step, List<OptimizationFinding> findings) {
        for (int i = 0; i < step.getChildren().size() - 1; ++i) {
            boolean bHasIdFilter;
            FlowStep a = step.getChildren().get(i);
            FlowStep b = step.getChildren().get(i + 1);
            String colA = this.firstSelectCollection(a);
            String colB = this.firstSelectCollection(b);
            if (colA == null || colB == null || colA.equals(colB) || !(bHasIdFilter = b.getQueries().stream().flatMap(q -> q.getPredicates().stream()).anyMatch(p -> p.getColumn() != null && (p.getColumn().endsWith("Id") || p.getColumn().endsWith("_id") || p.getColumn().equalsIgnoreCase("id"))))) continue;
            findings.add(new OptimizationFinding(OptimizationFinding.Category.PREFETCH_CANDIDATE, OptimizationFinding.Severity.MEDIUM, colA + "+" + colB, null, "'" + colA + "' is fetched then '" + colB + "' is queried by its id \u2014 consider $lookup (single aggregation) or a combined projection", a.getClassName() + " \u2192 " + b.getClassName(), "FIND " + colA + " \u2192 FIND " + colB + " by id", "db." + colA + ".aggregate([{ $lookup: { from: \"" + colB + "\", localField: \"_id\", foreignField: \"<fk>\", as: \"" + colB + "\" } }])"));
        }
        for (FlowStep child : step.getChildren()) {
            this.detectPrefetch(child, findings);
        }
    }

    private String firstSelectCollection(FlowStep step) {
        return step.getQueries().stream().filter(q -> q.getType() == ExtractedQuery.QueryType.SELECT).map(ExtractedQuery::getTableName).filter(Objects::nonNull).findFirst().orElse(null);
    }
}

