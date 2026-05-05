package com.flowpoc.analyzer;

import com.flowpoc.model.FlowResult;
import com.flowpoc.model.OptimizationFinding;

import java.util.ArrayList;
import java.util.List;

/**
 * Chain of Responsibility: runs all registered analyzers in order and aggregates findings.
 * New analyzers are added here without touching FlowResult or the engine (OCP).
 */
public class AnalyzerPipeline {

    private final List<OptimizationAnalyzer> analyzers;

    private AnalyzerPipeline(Builder builder) {
        this.analyzers = List.copyOf(builder.analyzers);
    }

    public List<OptimizationFinding> run(FlowResult flowResult) {
        List<OptimizationFinding> all = new ArrayList<>();
        for (OptimizationAnalyzer analyzer : analyzers) {
            all.addAll(analyzer.analyze(flowResult));
        }
        return all;
    }

    public void runAndAttach(FlowResult flowResult) {
        run(flowResult).forEach(flowResult::addOptimization);
    }

    // --- Builder ---

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private final List<OptimizationAnalyzer> analyzers = new ArrayList<>();

        public Builder add(OptimizationAnalyzer a) { analyzers.add(a); return this; }

        /** Default MongoDB-focused pipeline. */
        public Builder withMongoDefaults() {
            return add(new MongoMissingIndexAnalyzer())
                    .add(new NplusOneAnalyzer())
                    .add(new BulkOperationAnalyzer());
        }

        public AnalyzerPipeline build() { return new AnalyzerPipeline(this); }
    }
}
