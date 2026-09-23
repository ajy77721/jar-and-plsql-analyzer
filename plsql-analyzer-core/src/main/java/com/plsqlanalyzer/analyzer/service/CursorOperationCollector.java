package com.plsqlanalyzer.analyzer.service;

import com.plsqlanalyzer.analyzer.model.CallGraphNode;
import com.plsqlanalyzer.analyzer.model.CursorOperationSummary;
import com.plsqlanalyzer.analyzer.model.CursorOperationSummary.CursorAccessDetail;
import com.plsqlanalyzer.parser.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Collects all cursor references (DECLARE, OPEN, FETCH, CLOSE, FOR_LOOP) from parsed units,
 * deduplicates, and produces per-cursor summaries. Mirrors SequenceOperationCollector.
 */
public class CursorOperationCollector {

    private static final Logger log = LoggerFactory.getLogger(CursorOperationCollector.class);

    public Map<String, CursorOperationSummary> collect(List<PlsqlUnit> units) {
        log.info("Collecting cursor operations from {} units", units.size());
        Map<String, CursorOperationSummary> summaries = new LinkedHashMap<>();

        for (PlsqlUnit unit : units) {
            for (PlsqlProcedure proc : unit.getProcedures()) {
                String procId = CallGraphNode.buildId(unit.getSchemaName(), unit.getName(), proc.getName());
                Set<String> seen = new HashSet<>();

                for (CursorInfo ci : proc.getCursorInfos()) {
                    String cursorName = (ci.getCursorName() != null ? ci.getCursorName() : "UNKNOWN").toUpperCase();
                    String dedupKey = (cursorName + "|" + proc.getName() + "|"
                            + ci.getOperation() + "|" + ci.getLineNumber()).toUpperCase();
                    if (!seen.add(dedupKey)) continue;

                    CursorOperationSummary summary = summaries.computeIfAbsent(cursorName,
                            k -> new CursorOperationSummary(cursorName));

                    summary.getOperations().add(ci.getOperation().toUpperCase());

                    if ("DECLARE".equals(ci.getOperation())) {
                        if (summary.getCursorType() == null) {
                            summary.setCursorType(ci.getCursorType());
                        }
                        if (summary.getQueryText() == null && ci.getQueryText() != null) {
                            summary.setQueryText(ci.getQueryText());
                        }
                    }
                    if ("FOR_LOOP".equals(ci.getOperation()) && ci.getQueryText() != null
                            && summary.getQueryText() == null) {
                        summary.setQueryText(ci.getQueryText());
                        if (summary.getCursorType() == null) summary.setCursorType("FOR_LOOP");
                    }
                    if ("OPEN".equals(ci.getOperation()) && "OPEN_FOR".equals(ci.getCursorType())
                            && ci.getQueryText() != null && summary.getQueryText() == null) {
                        summary.setQueryText(ci.getQueryText());
                        if (summary.getCursorType() == null) summary.setCursorType("OPEN_FOR");
                    }
                    if ("REF_CURSOR".equals(ci.getCursorType()) && summary.getCursorType() == null) {
                        summary.setCursorType("REF_CURSOR");
                    }

                    CursorAccessDetail detail = new CursorAccessDetail();
                    detail.setProcedureId(procId);
                    detail.setProcedureName(proc.getName());
                    detail.setOperation(ci.getOperation().toUpperCase());
                    detail.setCursorType(ci.getCursorType());
                    detail.setQueryText(ci.getQueryText());
                    detail.setLineNumber(ci.getLineNumber());
                    detail.setSourceFile(unit.getSourceFile());
                    summary.getAccessDetails().add(detail);
                }
            }
        }

        for (CursorOperationSummary s : summaries.values()) {
            s.setAccessCount(s.getAccessDetails().size());
            if (s.getCursorType() == null) s.setCursorType("EXPLICIT");
        }

        int totalAccesses = summaries.values().stream()
                .mapToInt(CursorOperationSummary::getAccessCount).sum();
        log.info("Cursor operations collected: {} cursors, {} total accesses", summaries.size(), totalAccesses);
        return summaries;
    }
}
