package com.plsqlanalyzer.analyzer.service;

import com.plsqlanalyzer.analyzer.model.CallGraphNode;
import com.plsqlanalyzer.analyzer.model.SequenceOperationSummary;
import com.plsqlanalyzer.analyzer.model.SequenceOperationSummary.SequenceAccessDetail;
import com.plsqlanalyzer.parser.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Collects all Oracle SEQUENCE references (NEXTVAL/CURRVAL) from parsed units,
 * deduplicates, and produces per-sequence summaries. Mirrors TableOperationCollector.
 */
public class SequenceOperationCollector {

    private static final Logger log = LoggerFactory.getLogger(SequenceOperationCollector.class);

    public Map<String, SequenceOperationSummary> collect(List<PlsqlUnit> units) {
        log.info("Collecting sequence operations from {} units", units.size());
        Map<String, SequenceOperationSummary> summaries = new LinkedHashMap<>();

        for (PlsqlUnit unit : units) {
            for (PlsqlProcedure proc : unit.getProcedures()) {
                String procId = CallGraphNode.buildId(unit.getSchemaName(), unit.getName(), proc.getName());
                Set<String> seen = new HashSet<>();

                for (SequenceReference ref : proc.getSequenceReferences()) {
                    String dedupKey = (ref.getSequenceName() + "|" + proc.getName() + "|"
                            + ref.getOperation() + "|" + ref.getLineNumber()).toUpperCase();
                    if (!seen.add(dedupKey)) continue;

                    String key = ref.getFullName().toUpperCase();
                    SequenceOperationSummary summary = summaries.computeIfAbsent(key,
                            k -> new SequenceOperationSummary(ref.getSequenceName(), ref.getSchemaName()));

                    summary.getOperations().add(ref.getOperation().toUpperCase());

                    SequenceAccessDetail detail = new SequenceAccessDetail();
                    detail.setProcedureId(procId);
                    detail.setProcedureName(proc.getName());
                    detail.setOperation(ref.getOperation().toUpperCase());
                    detail.setLineNumber(ref.getLineNumber());
                    detail.setSourceFile(unit.getSourceFile());
                    summary.getAccessDetails().add(detail);
                }
            }
        }

        // Set access counts
        for (SequenceOperationSummary s : summaries.values()) {
            s.setAccessCount(s.getAccessDetails().size());
        }

        int totalAccesses = summaries.values().stream()
                .mapToInt(s -> s.getAccessDetails().size()).sum();
        log.info("Sequence operations collected: {} sequences, {} total accesses", summaries.size(), totalAccesses);
        return summaries;
    }
}
