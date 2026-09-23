package com.plsqlanalyzer.web.controller.detail;

import com.plsqlanalyzer.analyzer.model.AnalysisResult;
import com.plsqlanalyzer.analyzer.model.CursorOperationSummary;
import com.plsqlanalyzer.analyzer.model.JoinOperationSummary;
import com.plsqlanalyzer.analyzer.model.SequenceOperationSummary;
import com.plsqlanalyzer.analyzer.model.TableOperationSummary;
import com.plsqlanalyzer.analyzer.service.AnalysisService;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Collections;

/** Tables / sequences / joins / cursors listing endpoints (extracted from AnalyzerController). */
@Service
public class TableQueryService {

    /*
     * These three used to answer an empty list both when no analysis was loaded and when the
     * analysis genuinely had none of that operation. The first is 404 now: an absent analysis is not
     * evidence that a procedure contains no joins, and the screens reading these render "none found"
     * either way.
     */

    private final AnalysisService analysisService;

    public TableQueryService(AnalysisService analysisService) {
        this.analysisService = analysisService;
    }

    public ResponseEntity<Collection<TableOperationSummary>> getTableOperations() {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(result.getTableOperations().values());
    }

    public ResponseEntity<TableOperationSummary> getTableDetail(String tableName) {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) return ResponseEntity.notFound().build();

        TableOperationSummary summary = result.getTableOperations().get(tableName.toUpperCase());
        if (summary == null) {
            for (var entry : result.getTableOperations().entrySet()) {
                if (entry.getKey().contains(tableName.toUpperCase())) {
                    summary = entry.getValue();
                    break;
                }
            }
        }
        if (summary == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(summary);
    }

    public ResponseEntity<Collection<SequenceOperationSummary>> getSequenceOperations() {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) return ResponseEntity.notFound().build();
        if (result.getSequenceOperations() == null) return ResponseEntity.ok(Collections.emptyList());
        return ResponseEntity.ok(result.getSequenceOperations().values());
    }

    public ResponseEntity<Collection<JoinOperationSummary>> getJoinOperations() {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) return ResponseEntity.notFound().build();
        if (result.getJoinOperations() == null) return ResponseEntity.ok(Collections.emptyList());
        return ResponseEntity.ok(result.getJoinOperations().values());
    }

    public ResponseEntity<Collection<CursorOperationSummary>> getCursorOperations() {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) return ResponseEntity.notFound().build();
        if (result.getCursorOperations() == null) return ResponseEntity.ok(Collections.emptyList());
        return ResponseEntity.ok(result.getCursorOperations().values());
    }
}
