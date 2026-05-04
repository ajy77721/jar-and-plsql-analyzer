package com.plsqlanalyzer.web.parser.controller;

import com.plsqlanalyzer.web.parser.config.ComplexityConfig;
import com.plsqlanalyzer.web.parser.config.JoinComplexityConfig;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/parser/config")
public class ParserConfigController {

    private final ComplexityConfig complexityConfig;
    private final JoinComplexityConfig joinConfig;

    public ParserConfigController(ComplexityConfig complexityConfig,
                                  JoinComplexityConfig joinConfig) {
        this.complexityConfig = complexityConfig;
        this.joinConfig = joinConfig;
    }

    @GetMapping("/complexity")
    public ResponseEntity<Map<String, Object>> getComplexityConfig() {
        return ResponseEntity.ok(Map.of(
                "weights", Map.of(
                        "loc", complexityConfig.getLoc(),
                        "tables", complexityConfig.getTables(),
                        "callsOut", complexityConfig.getCallsOut(),
                        "cursors", complexityConfig.getCursors(),
                        "exceptionHandlers", complexityConfig.getExceptionHandlers(),
                        "totalStatements", complexityConfig.getTotalStatements(),
                        "dynamicSql", complexityConfig.getDynamicSql(),
                        "depth", complexityConfig.getDepth()
                ),
                "thresholds", Map.of(
                        "medium", complexityConfig.getThresholdMedium(),
                        "high", complexityConfig.getThresholdHigh()
                )
        ));
    }

    @GetMapping("/join-complexity")
    public ResponseEntity<Map<String, Object>> getJoinComplexityConfig() {
        return ResponseEntity.ok(Map.of(
                "weights", Map.of(
                        "baseScore", joinConfig.getBaseScore(),
                        "outerJoinPenalty", joinConfig.getOuterJoinPenalty(),
                        "crossFullJoinPenalty", joinConfig.getCrossFullJoinPenalty(),
                        "multiPredBonus", joinConfig.getMultiPredBonus(),
                        "highPredBonus", joinConfig.getHighPredBonus(),
                        "noPredPenalty", joinConfig.getNoPredPenalty(),
                        "multiPredThreshold", joinConfig.getMultiPredThreshold(),
                        "highPredThreshold", joinConfig.getHighPredThreshold()
                ),
                "thresholds", Map.of(
                        "medium", joinConfig.getThresholdMedium(),
                        "high", joinConfig.getThresholdHigh()
                )
        ));
    }
}
