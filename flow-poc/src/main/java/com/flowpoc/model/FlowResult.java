package com.flowpoc.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Complete result for one endpoint's flow walk —
 * the ordered step chain plus all analysis findings.
 */
public class FlowResult {

    private final String endpointMethod;
    private final String endpointPath;
    private final FlowStep rootStep;

    private final List<OptimizationFinding> optimizations = new ArrayList<>();
    private final List<TestDataSet>         testDataSets  = new ArrayList<>();

    public FlowResult(String endpointMethod, String endpointPath, FlowStep rootStep) {
        this.endpointMethod = endpointMethod;
        this.endpointPath   = endpointPath;
        this.rootStep       = rootStep;
    }

    public String getEndpointMethod()  { return endpointMethod; }
    public String getEndpointPath()    { return endpointPath; }
    public FlowStep getRootStep()      { return rootStep; }

    public List<OptimizationFinding> getOptimizations() { return optimizations; }
    public List<TestDataSet>         getTestDataSets()  { return testDataSets; }

    public void addOptimization(OptimizationFinding f) { optimizations.add(f); }
    public void addTestDataSet(TestDataSet t)           { testDataSets.add(t); }

    /** Flat ordered list of all steps (BFS). */
    public List<FlowStep> allSteps() {
        List<FlowStep> out = new ArrayList<>();
        collect(rootStep, out);
        return out;
    }

    /** Steps grouped by call-tree depth (0 = controller, 1 = service, 2+ = repository). */
    public java.util.Map<Integer, java.util.List<FlowStep>> getStepsByDepth() {
        java.util.Map<Integer, java.util.List<FlowStep>> map = new java.util.TreeMap<>();
        for (FlowStep s : allSteps()) {
            map.computeIfAbsent(s.getDepth(), k -> new java.util.ArrayList<>()).add(s);
        }
        return map;
    }

    private void collect(FlowStep step, List<FlowStep> out) {
        if (step == null) return;
        out.add(step);
        for (FlowStep child : step.getChildren()) collect(child, out);
    }
}
