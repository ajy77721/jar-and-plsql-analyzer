package com.plsqlanalyzer.web.controller.detail;

import com.plsqlanalyzer.analyzer.model.AnalysisResult;
import com.plsqlanalyzer.analyzer.service.AnalysisService;
import com.plsqlanalyzer.web.service.PersistenceService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** History / latest-result endpoints (extracted from AnalyzerController). */
@Service
public class HistoryQueryService {

    private final AnalysisService analysisService;
    private final PersistenceService persistenceService;
    private final SummaryService summaryService;

    public HistoryQueryService(AnalysisService analysisService,
                               @Qualifier("plsqlPersistenceService") PersistenceService persistenceService,
                               SummaryService summaryService) {
        this.analysisService = analysisService;
        this.persistenceService = persistenceService;
        this.summaryService = summaryService;
    }

    public ResponseEntity<Map<String, Object>> getLatestResult() {
        AnalysisResult result = analysisService.getLatestResult();
        if (result == null) {
            return ResponseEntity.ok(Map.of("status", "no_analysis"));
        }
        Map<String, Object> summary = summaryService.buildSummary(result);
        return ResponseEntity.ok(summary);
    }

    public ResponseEntity<List<Map<String, Object>>> listHistory() {
        return ResponseEntity.ok(persistenceService.listHistory());
    }

    public ResponseEntity<Map<String, Object>> loadAnalysis(String name) {
        AnalysisResult result = persistenceService.loadByName(name);
        if (result == null) return ResponseEntity.notFound().build();
        analysisService.setLatestResult(result);
        Map<String, Object> summary = summaryService.buildSummary(result);
        return ResponseEntity.ok(summary);
    }

    public ResponseEntity<Map<String, Object>> deleteAnalysis(String name) {
        boolean deleted = persistenceService.delete(name);
        if (!deleted) return ResponseEntity.notFound().build();

        AnalysisResult current = analysisService.getLatestResult();
        if (current != null && name.equals(current.getName())) {
            analysisService.setLatestResult(null);
        }

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("deleted", true);
        response.put("name", name);
        return ResponseEntity.ok(response);
    }

    public ResponseEntity<Map<String, Object>> getAnalysisConnections(String name) {
        Map<String, Object> info = persistenceService.loadConnections(name);
        if (info == null) return ResponseEntity.ok(Map.of("available", false));
        info.put("available", true);
        return ResponseEntity.ok(info);
    }

    public ResponseEntity<List<Map<String, Object>>> listVersions() {
        return ResponseEntity.ok(persistenceService.listHistory());
    }
}
