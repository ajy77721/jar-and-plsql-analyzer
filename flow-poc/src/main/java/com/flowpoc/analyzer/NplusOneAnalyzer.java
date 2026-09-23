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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class NplusOneAnalyzer
implements OptimizationAnalyzer {
    @Override
    public List<OptimizationFinding> analyze(FlowResult flowResult) {
        ArrayList<OptimizationFinding> findings = new ArrayList<OptimizationFinding>();
        findings.addAll(this.detectSameCollectionMultiDepth(flowResult));
        findings.addAll(this.detectListPlusSingleLookup(flowResult));
        findings.addAll(this.detectNPlusOneWrites(flowResult));
        return findings;
    }

    private List<OptimizationFinding> detectSameCollectionMultiDepth(FlowResult flowResult) {
        ArrayList<OptimizationFinding> findings = new ArrayList<OptimizationFinding>();
        LinkedHashMap<String, List<FlowStep>> findsByCollection = new LinkedHashMap<String, List<FlowStep>>();
        this.collectQuerySteps(flowResult.getRootStep(), findsByCollection);
        for (Map.Entry<String, List<FlowStep>> entry : findsByCollection.entrySet()) {
            List<FlowStep> steps = entry.getValue();
            if (steps.size() < 2) continue;
            HashSet<Integer> depths = new HashSet<Integer>();
            for (FlowStep s : steps) {
                depths.add(s.getDepth());
            }
            if (depths.size() == 1) continue;
            String collection = entry.getKey();
            FlowStep first = steps.get(0);
            findings.add(new OptimizationFinding(OptimizationFinding.Category.N_PLUS_ONE, OptimizationFinding.Severity.HIGH, collection, null, "Collection '" + collection + "' is queried " + steps.size() + " times across different call depths \u2014 possible N+1. Consider $lookup or batch findAllById.", first.getClassName() + "." + first.getMethodName(), "Repeated FIND on " + collection, "Replace repeated FIND with: db." + collection + ".aggregate([{ $lookup: { from: \"<child>\", localField: \"_id\", foreignField: \"<fk>\", as: \"items\" } }])"));
        }
        return findings;
    }

    private void collectQuerySteps(FlowStep step, Map<String, List<FlowStep>> byCollection) {
        if (step == null) {
            return;
        }
        if (this.isModelAccessor(step)) {
            for (FlowStep child : step.getChildren()) {
                this.collectQuerySteps(child, byCollection);
            }
            return;
        }
        for (ExtractedQuery eq : step.getQueries()) {
            if (eq.getType() != ExtractedQuery.QueryType.SELECT && eq.getType() != ExtractedQuery.QueryType.UNKNOWN || eq.getTableName() == null) continue;
            byCollection.computeIfAbsent(eq.getTableName(), k -> new ArrayList<FlowStep>()).add(step);
        }
        for (FlowStep child : step.getChildren()) {
            this.collectQuerySteps(child, byCollection);
        }
    }

    private List<OptimizationFinding> detectListPlusSingleLookup(FlowResult flowResult) {
        ArrayList<OptimizationFinding> findings = new ArrayList<OptimizationFinding>();
        this.scanForPattern(flowResult.getRootStep(), findings);
        return findings;
    }

    private void scanForPattern(FlowStep step, List<OptimizationFinding> findings) {
        if (step == null) {
            return;
        }
        ArrayList<String> listReadCollections = new ArrayList<String>();
        ArrayList<String[]> singleLookups = new ArrayList<String[]>();
        for (FlowStep child : step.getChildren()) {
            String col;
            String method;
            if (this.isModelAccessor(child) || (method = child.getMethodName()) == null || (col = this.firstCollection(child)) == null) continue;
            if (this.isSingleLookup(method)) {
                singleLookups.add(new String[]{col, child.getClassName() + "." + child.getMethodName()});
                continue;
            }
            if (!this.isListRead(method)) continue;
            listReadCollections.add(col);
        }
        if (!listReadCollections.isEmpty() && !singleLookups.isEmpty()) {
            for (String[] lookup : singleLookups) {
                String lookupColl = lookup[0];
                String lookupLoc = lookup[1];
                if (listReadCollections.contains(lookupColl)) continue;
                String parentLoc = step.getClassName() + "." + step.getMethodName();
                findings.add(new OptimizationFinding(OptimizationFinding.Category.N_PLUS_ONE, OptimizationFinding.Severity.HIGH, lookupColl, null, "'" + parentLoc + "' performs a list-read then calls '" + lookupLoc + "' once per result \u2014 classic N+1 read. Replace individual lookups with a single $lookup / findAllById.", parentLoc, "list-read " + String.valueOf(listReadCollections) + " \u2192 per-item lookup " + lookupColl, "Replace with: mongoTemplate.find(Query.query(Criteria.where(\"_id\").in(ids)), " + lookupColl.substring(0, 1).toUpperCase() + lookupColl.substring(1) + ".class) OR use $lookup in the aggregation pipeline"));
            }
        }
        for (FlowStep child : step.getChildren()) {
            this.scanForPattern(child, findings);
        }
    }

    private List<OptimizationFinding> detectNPlusOneWrites(FlowResult flowResult) {
        ArrayList<OptimizationFinding> findings = new ArrayList<OptimizationFinding>();
        this.detectWriteNPlusOne(flowResult.getRootStep(), findings);
        return findings;
    }

    private void detectWriteNPlusOne(FlowStep step, List<OptimizationFinding> findings) {
        if (step == null) {
            return;
        }
        if (step.getKind() == FlowStep.StepKind.CONTROLLER || step.getKind() == FlowStep.StepKind.REPOSITORY) {
            for (FlowStep child : step.getChildren()) {
                this.detectWriteNPlusOne(child, findings);
            }
            return;
        }
        boolean hasListReadChild = step.getChildren().stream().filter(c -> !this.isModelAccessor((FlowStep)c)).anyMatch(c -> this.isListRead(c.getMethodName()));
        if (!hasListReadChild) {
            for (FlowStep child : step.getChildren()) {
                this.detectWriteNPlusOne(child, findings);
            }
            return;
        }
        for (ExtractedQuery eq : step.getQueries()) {
            boolean writeIsDifferentCollection;
            String writeColl;
            if (eq.getType() != ExtractedQuery.QueryType.INSERT && eq.getType() != ExtractedQuery.QueryType.UPDATE || (writeColl = eq.getTableName()) == null || !(writeIsDifferentCollection = step.getChildren().stream().filter(c -> !this.isModelAccessor((FlowStep)c) && this.isListRead(c.getMethodName())).map(c -> this.firstCollection((FlowStep)c)).filter(Objects::nonNull).noneMatch(writeColl::equals))) continue;
            String loc = step.getClassName() + "." + step.getMethodName();
            findings.add(new OptimizationFinding(OptimizationFinding.Category.BULK_WRITE, OptimizationFinding.Severity.HIGH, writeColl, null, "'" + loc + "' writes to '" + writeColl + "' once per item inside a loop over a list-read result \u2014 N+1 write. Collect all documents first, then insertAll / bulkWrite in one call.", loc, "per-item INSERT to " + writeColl + " inside list-read loop", "Replace per-item insert with: mongoTemplate.insertAll(List.of(doc1, doc2, ...)) or bulkOps(BulkMode.UNORDERED, \"" + writeColl + "\").insert(...).execute()"));
        }
        for (FlowStep child : step.getChildren()) {
            this.detectWriteNPlusOne(child, findings);
        }
    }

    private boolean isListRead(String method) {
        if (method == null) {
            return false;
        }
        if (this.isSingleLookup(method)) {
            return false;
        }
        String m = method.toLowerCase();
        return m.startsWith("findall") || m.startsWith("findby") || m.startsWith("getall") || m.startsWith("getby") || m.startsWith("fetchall") || m.startsWith("fetchby") || m.startsWith("listall") || m.startsWith("listby") || m.startsWith("searchby") || m.startsWith("queryby") || m.startsWith("findin") || m.startsWith("findbetween") || m.startsWith("find") && !this.isSingleLookup(method) || m.startsWith("get") && !this.isSingleLookup(method);
    }

    private boolean isSingleLookup(String method) {
        if (method == null) {
            return false;
        }
        String m = method.toLowerCase();
        return m.startsWith("findbyid") || m.startsWith("findone") || m.startsWith("getbyid") || m.startsWith("getone") || m.startsWith("fetchbyid") || m.equals("findbyid") || m.equals("getbyid");
    }

    private String firstCollection(FlowStep step) {
        for (ExtractedQuery eq : step.getQueries()) {
            if (eq.getTableName() == null) continue;
            return eq.getTableName();
        }
        return null;
    }

    private boolean isModelAccessor(FlowStep step) {
        if (step.getKind() != FlowStep.StepKind.OTHER) {
            return false;
        }
        String m = step.getMethodName();
        if (m == null) {
            return true;
        }
        if (m.startsWith("get") || m.startsWith("set") || m.startsWith("is") || m.startsWith("lambda") || m.startsWith("build") || m.equals("equals") || m.equals("hashCode") || m.equals("toString")) {
            return true;
        }
        String cls = step.getClassName();
        return cls != null && (cls.contains("$") || cls.endsWith("Builder"));
    }
}

