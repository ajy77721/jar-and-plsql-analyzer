/*
 * Decompiled with CFR 0.152.
 */
package com.flowpoc.model;

import com.flowpoc.model.ClaudeCallRecord;
import com.flowpoc.model.FlowStep;
import com.flowpoc.model.OptimizationFinding;
import com.flowpoc.model.TestDataSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class FlowResult {
    private final String endpointMethod;
    private final String endpointPath;
    private final FlowStep rootStep;
    private final List<OptimizationFinding> optimizations = new ArrayList<OptimizationFinding>();
    private final List<TestDataSet> testDataSets = new ArrayList<TestDataSet>();
    private final List<ClaudeCallRecord> claudeCallLogs = new ArrayList<ClaudeCallRecord>();
    private final Map<String, Integer> optimizationKeys = new HashMap<String, Integer>();

    public FlowResult(String endpointMethod, String endpointPath, FlowStep rootStep) {
        this.endpointMethod = endpointMethod;
        this.endpointPath = endpointPath;
        this.rootStep = rootStep;
    }

    public String getEndpointMethod() {
        return this.endpointMethod;
    }

    public String getEndpointPath() {
        return this.endpointPath;
    }

    public FlowStep getRootStep() {
        return this.rootStep;
    }

    public List<OptimizationFinding> getOptimizations() {
        return this.optimizations;
    }

    public List<TestDataSet> getTestDataSets() {
        return this.testDataSets;
    }

    public void addOptimization(OptimizationFinding f) {
        String raw = f.getTable() != null ? f.getTable() : "";
        String key = String.valueOf((Object)f.getCategory()) + "|" + f.getLocation() + "|" + raw.toUpperCase();
        boolean isUpper = raw.equals(raw.toUpperCase()) && !raw.isEmpty();
        Integer existingIdx = this.optimizationKeys.get(key);
        if (existingIdx == null) {
            this.optimizationKeys.put(key, this.optimizations.size());
            this.optimizations.add(f);
        } else if (isUpper) {
            this.optimizations.set(existingIdx, f);
        }
    }

    public void addTestDataSet(TestDataSet t) {
        this.testDataSets.add(t);
    }

    public List<ClaudeCallRecord> getClaudeCallLogs() {
        return this.claudeCallLogs;
    }

    public void addClaudeCallLog(ClaudeCallRecord r) {
        this.claudeCallLogs.add(r);
    }

    public List<FlowStep> allSteps() {
        ArrayList<FlowStep> out = new ArrayList<FlowStep>();
        this.collect(this.rootStep, out);
        return out;
    }

    public Map<Integer, List<FlowStep>> getStepsByDepth() {
        TreeMap<Integer, List<FlowStep>> map = new TreeMap<Integer, List<FlowStep>>();
        for (FlowStep s : this.allSteps()) {
            map.computeIfAbsent(s.getDepth(), k -> new ArrayList()).add(s);
        }
        return map;
    }

    private void collect(FlowStep step, List<FlowStep> out) {
        if (step == null) {
            return;
        }
        out.add(step);
        for (FlowStep child : step.getChildren()) {
            this.collect(child, out);
        }
    }
}

