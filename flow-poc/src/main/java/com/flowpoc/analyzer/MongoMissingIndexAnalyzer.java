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
import java.util.List;
import java.util.Set;

public class MongoMissingIndexAnalyzer
implements OptimizationAnalyzer {
    private static final Set<String> AUTO_INDEXED = Set.of("_id", "id");

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

    @Override
    public List<OptimizationFinding> analyze(FlowResult flowResult) {
        ArrayList<OptimizationFinding> findings = new ArrayList<OptimizationFinding>();
        for (FlowStep step : flowResult.allSteps()) {
            if (this.isModelAccessor(step)) continue;
            for (ExtractedQuery eq : step.getQueries()) {
                if (eq.getType() != ExtractedQuery.QueryType.SELECT) continue;
                String table = eq.getTableName();
                String location = step.getClassName() + "." + step.getMethodName();
                if (eq.getPredicates().isEmpty() && !MongoMissingIndexAnalyzer.rawSqlHasFilter(eq.getRawSql())) {
                    boolean isAgg = eq.getRawSql() != null && eq.getRawSql().startsWith("AGGREGATE");
                    Object fix = isAgg ? "Add a $match stage as the first pipeline stage to leverage indexes:\n  db." + table + ".aggregate([{ $match: { field: value } }, /* rest */])" : "Add a filter predicate or paginate with .limit(n)";
                    findings.add(new OptimizationFinding(OptimizationFinding.Category.FULL_TABLE_SCAN, OptimizationFinding.Severity.HIGH, table, null, (isAgg ? "AGGREGATE with no $match stage \u2014 full collection scan on " : "FIND with no filter \u2014 full collection scan on ") + table, location, eq.getRawSql(), (String)fix));
                    continue;
                }
                for (Predicate pred : eq.getPredicates()) {
                    String col = pred.getColumn();
                    if (col == null || AUTO_INDEXED.contains(col.toLowerCase())) continue;
                    findings.add(new OptimizationFinding(OptimizationFinding.Category.MISSING_INDEX, OptimizationFinding.Severity.MEDIUM, table, col, "Field '" + col + "' used in FIND predicate \u2014 verify index exists on " + table + "." + col, location, eq.getRawSql(), "db." + table + ".createIndex({ " + col + ": 1 })"));
                }
            }
        }
        return findings;
    }
}

