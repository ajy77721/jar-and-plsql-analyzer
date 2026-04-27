package com.plsqlanalyzer.analyzer.service;

import com.plsqlanalyzer.analyzer.model.CallGraphNode;
import com.plsqlanalyzer.analyzer.model.JoinOperationSummary;
import com.plsqlanalyzer.analyzer.model.JoinOperationSummary.JoinAccessDetail;
import com.plsqlanalyzer.parser.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Collects all JOIN operations from parsed units — which tables are joined,
 * with what join type, and what ON predicate. Produces per-join-pair summaries.
 */
public class JoinOperationCollector {

    private static final Logger log = LoggerFactory.getLogger(JoinOperationCollector.class);

    public Map<String, JoinOperationSummary> collect(List<PlsqlUnit> units) {
        log.info("Collecting join operations from {} units", units.size());
        Map<String, JoinOperationSummary> summaries = new LinkedHashMap<>();

        for (PlsqlUnit unit : units) {
            for (PlsqlProcedure proc : unit.getProcedures()) {
                String procId = CallGraphNode.buildId(unit.getSchemaName(), unit.getName(), proc.getName());
                Set<String> seen = new HashSet<>();

                // From procedure-level join infos (captured by ANTLR visitor → JSqlParser)
                for (JoinInfo ji : proc.getJoinInfos()) {
                    addJoin(summaries, seen, procId, proc.getName(), unit.getSourceFile(), ji);
                }

                // Also from SqlAnalysisResult join infos
                for (SqlAnalysisResult sql : proc.getSqlStatements()) {
                    if (sql.getJoinInfos() != null) {
                        for (JoinInfo ji : sql.getJoinInfos()) {
                            addJoin(summaries, seen, procId, proc.getName(), unit.getSourceFile(), ji);
                        }
                    }
                }
            }
        }

        // Set access counts
        for (JoinOperationSummary s : summaries.values()) {
            s.setAccessCount(s.getAccessDetails().size());
        }

        int totalJoins = summaries.values().stream()
                .mapToInt(s -> s.getAccessDetails().size()).sum();
        log.info("Join operations collected: {} unique join pairs, {} total join accesses", summaries.size(), totalJoins);
        return summaries;
    }

    private void addJoin(Map<String, JoinOperationSummary> summaries, Set<String> seen,
                          String procId, String procName, String sourceFile, JoinInfo ji) {
        String left = (ji.getLeftTable() != null ? ji.getLeftTable() : "?").toUpperCase();
        String right = (ji.getRightTable() != null ? ji.getRightTable() : "?").toUpperCase();
        String joinType = (ji.getJoinType() != null ? ji.getJoinType() : "INNER").toUpperCase();

        String dedupKey = (left + "|" + right + "|" + procName + "|"
                + joinType + "|" + ji.getLineNumber()).toUpperCase();
        if (!seen.add(dedupKey)) return;

        // Normalize key: always alphabetical to group A-B and B-A
        String key = left.compareTo(right) <= 0 ? left + "|" + right : right + "|" + left;
        JoinOperationSummary summary = summaries.computeIfAbsent(key,
                k -> new JoinOperationSummary(left, right));

        summary.getJoinTypes().add(joinType);

        JoinAccessDetail detail = new JoinAccessDetail();
        detail.setProcedureId(procId);
        detail.setProcedureName(procName);
        detail.setJoinType(joinType);
        detail.setOnPredicate(ji.getOnPredicate());
        detail.setLineNumber(ji.getLineNumber());
        detail.setSourceFile(sourceFile);
        summary.getAccessDetails().add(detail);
    }
}
