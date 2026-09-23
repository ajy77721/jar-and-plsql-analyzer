package com.plsqlanalyzer.analyzer.service;

import com.plsqlanalyzer.analyzer.model.CallGraphNode;
import com.plsqlanalyzer.analyzer.model.TableOperationSummary;
import com.plsqlanalyzer.analyzer.model.TableOperationSummary.TableAccessDetail;
import com.plsqlanalyzer.parser.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TableOperationCollector {

    private static final Logger log = LoggerFactory.getLogger(TableOperationCollector.class);

    public Map<String, TableOperationSummary> collect(List<PlsqlUnit> units) {
        log.info("Collecting table operations from {} units", units.size());
        Map<String, TableOperationSummary> summaries = new LinkedHashMap<>();

        for (PlsqlUnit unit : units) {
            for (PlsqlProcedure proc : unit.getProcedures()) {
                String procId = CallGraphNode.buildId(unit.getSchemaName(), unit.getName(), proc.getName());

                // Track already-added entries to avoid duplicates between ANTLR visitor and JSqlParser
                // Key: "TABLE_NAME|PROC_NAME|OPERATION|LINE"
                Set<String> seen = new HashSet<>();

                // From table references extracted by ANTLR4 visitor
                for (TableReference ref : proc.getTableReferences()) {
                    String dedupKey = (ref.getTableName() + "|" + proc.getName() + "|"
                            + ref.getOperation() + "|" + ref.getLineNumber()).toUpperCase();
                    if (!seen.add(dedupKey)) continue;

                    String key = ref.getFullTableName().toUpperCase();
                    log.debug("Found table reference: {} in {}.{}", key, unit.getName(), proc.getName());
                    TableOperationSummary summary = summaries.computeIfAbsent(key,
                            k -> new TableOperationSummary(ref.getTableName(), ref.getSchemaName()));

                    if (ref.getOperation() != null) {
                        summary.getOperations().add(ref.getOperation());
                    }

                    TableAccessDetail detail = new TableAccessDetail();
                    detail.setProcedureId(procId);
                    detail.setProcedureName(proc.getName());
                    detail.setOperation(ref.getOperation());
                    detail.setLineNumber(ref.getLineNumber());
                    detail.setSourceFile(unit.getSourceFile());
                    summary.getAccessDetails().add(detail);
                }

                // From SQL analysis results (JSqlParser) — adds WHERE filter info
                // Only add if not already seen (avoids duplicates with ANTLR visitor)
                for (SqlAnalysisResult sql : proc.getSqlStatements()) {
                    for (TableReference ref : sql.getTableReferences()) {
                        String dedupKey = (ref.getTableName() + "|" + proc.getName() + "|"
                                + ref.getOperation() + "|" + ref.getLineNumber()).toUpperCase();

                        String key = ref.getFullTableName().toUpperCase();
                        TableOperationSummary summary = summaries.computeIfAbsent(key,
                                k -> new TableOperationSummary(ref.getTableName(), ref.getSchemaName()));

                        if (ref.getOperation() != null) {
                            summary.getOperations().add(ref.getOperation());
                        }

                        if (!seen.add(dedupKey)) {
                            // Already added by ANTLR — but if JSqlParser has WHERE filters,
                            // find the existing detail and attach filters to it
                            if (sql.getWhereFilters() != null && !sql.getWhereFilters().isEmpty()) {
                                for (TableAccessDetail existing : summary.getAccessDetails()) {
                                    if (existing.getProcedureName() != null
                                            && existing.getProcedureName().equalsIgnoreCase(proc.getName())
                                            && existing.getLineNumber() == ref.getLineNumber()
                                            && (existing.getWhereFilters() == null || existing.getWhereFilters().isEmpty())) {
                                        existing.setWhereFilters(sql.getWhereFilters());
                                        break;
                                    }
                                }
                            }
                            continue;
                        }

                        TableAccessDetail detail = new TableAccessDetail();
                        detail.setProcedureId(procId);
                        detail.setProcedureName(proc.getName());
                        detail.setOperation(ref.getOperation());
                        detail.setLineNumber(ref.getLineNumber());
                        detail.setSourceFile(unit.getSourceFile());
                        detail.setWhereFilters(sql.getWhereFilters());
                        summary.getAccessDetails().add(detail);
                    }
                }
            }
        }

        int totalAccesses = summaries.values().stream()
                .mapToInt(s -> s.getAccessDetails().size()).sum();
        log.info("Table operations collected: {} tables, {} total accesses", summaries.size(), totalAccesses);
        return summaries;
    }
}
