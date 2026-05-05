package com.flowpoc.analyzer;

import com.flowpoc.model.FlowResult;
import com.flowpoc.model.OptimizationFinding;

import java.util.List;

/**
 * Strategy interface (Strategy pattern) for a single category of optimization detection.
 * Each analyzer is responsible for exactly one concern (SRP).
 * New analyzers are plugged in without touching existing code (OCP).
 */
public interface OptimizationAnalyzer {
    List<OptimizationFinding> analyze(FlowResult flowResult);
}
