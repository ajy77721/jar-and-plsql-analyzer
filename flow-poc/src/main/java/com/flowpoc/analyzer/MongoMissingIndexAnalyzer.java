package com.flowpoc.analyzer;

import com.flowpoc.model.*;

import java.util.*;

/**
 * Detects MongoDB fields used in query predicates that have no apparent index.
 *
 * Heuristic: if the same field appears in a FIND/COUNT predicate across multiple
 * flow steps, or if the query has no filter at all (full collection scan), flag it.
 *
 * A real implementation would compare against db.collection.getIndexes() output;
 * here we flag all filter fields as candidates since we have no index metadata yet.
 */
public class MongoMissingIndexAnalyzer implements OptimizationAnalyzer {

    // Well-known fields that MongoDB always indexes
    private static final Set<String> AUTO_INDEXED = Set.of("_id", "id");

    @Override
    public List<OptimizationFinding> analyze(FlowResult flowResult) {
        List<OptimizationFinding> findings = new ArrayList<>();

        for (FlowStep step : flowResult.allSteps()) {
            for (ExtractedQuery eq : step.getQueries()) {
                if (eq.getType() != ExtractedQuery.QueryType.SELECT) continue;

                String table   = eq.getTableName();
                String location = step.getClassName() + "." + step.getMethodName();

                // No filter at all → full collection scan
                if (eq.getPredicates().isEmpty()) {
                    findings.add(new OptimizationFinding(
                            OptimizationFinding.Category.FULL_TABLE_SCAN, OptimizationFinding.Severity.HIGH,
                            table, null,
                            "FIND with no filter — full collection scan on " + table,
                            location, eq.getRawSql(),
                            "Add a filter predicate or paginate with .limit(n)"));
                    continue;
                }

                for (Predicate pred : eq.getPredicates()) {
                    String col = pred.getColumn();
                    if (col == null || AUTO_INDEXED.contains(col.toLowerCase())) continue;

                    findings.add(new OptimizationFinding(
                            OptimizationFinding.Category.MISSING_INDEX, OptimizationFinding.Severity.MEDIUM,
                            table, col,
                            "Field '" + col + "' used in FIND predicate — verify index exists on "
                                    + table + "." + col,
                            location, eq.getRawSql(),
                            "db." + table + ".createIndex({ " + col + ": 1 })"));
                }
            }
        }
        return findings;
    }
}
