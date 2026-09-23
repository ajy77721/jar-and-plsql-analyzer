/*
 * Decompiled with CFR 0.152.
 */
package com.flowpoc.analyzer;

import com.flowpoc.ClaudeInvoker;
import com.flowpoc.ProgressCallback;
import com.flowpoc.analyzer.AggregationRewriteAnalyzer;
import com.flowpoc.analyzer.BulkOperationAnalyzer;
import com.flowpoc.analyzer.ClaudeFlowAnalyzer;
import com.flowpoc.analyzer.HierarchyFlowAnalyzer;
import com.flowpoc.analyzer.MongoMissingIndexAnalyzer;
import com.flowpoc.analyzer.NplusOneAnalyzer;
import com.flowpoc.analyzer.OptimizationAnalyzer;
import com.flowpoc.analyzer.QueryClassificationAnalyzer;
import com.flowpoc.model.FlowResult;
import com.flowpoc.model.OptimizationFinding;
import java.util.ArrayList;
import java.util.List;

public class AnalyzerPipeline {
    private final List<OptimizationAnalyzer> analyzers;

    private AnalyzerPipeline(Builder builder) {
        this.analyzers = List.copyOf(builder.analyzers);
    }

    public void runAndAttach(FlowResult flowResult) {
        for (OptimizationAnalyzer analyzer : this.analyzers) {
            analyzer.analyze(flowResult).forEach(flowResult::addOptimization);
        }
    }

    public List<OptimizationFinding> run(FlowResult flowResult) {
        ArrayList<OptimizationFinding> all = new ArrayList<OptimizationFinding>();
        for (OptimizationAnalyzer analyzer : this.analyzers) {
            List<OptimizationFinding> batch = analyzer.analyze(flowResult);
            batch.forEach(flowResult::addOptimization);
            all.addAll(batch);
        }
        return all;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final List<OptimizationAnalyzer> analyzers = new ArrayList<OptimizationAnalyzer>();

        public Builder add(OptimizationAnalyzer a) {
            this.analyzers.add(a);
            return this;
        }

        public Builder withMongoDefaults() {
            return this.add(new QueryClassificationAnalyzer()).add(new HierarchyFlowAnalyzer()).add(new MongoMissingIndexAnalyzer()).add(new NplusOneAnalyzer()).add(new BulkOperationAnalyzer()).add(new AggregationRewriteAnalyzer());
        }

        public Builder withMongoDefaultsAndClaude(String claudeApiKey) {
            return this.withMongoDefaults().add(new ClaudeFlowAnalyzer(claudeApiKey, null));
        }

        public Builder withMongoDefaultsAndClaude(String claudeApiKey, ProgressCallback callback) {
            return this.withMongoDefaults().add(new ClaudeFlowAnalyzer(claudeApiKey, callback));
        }

        public Builder withMongoDefaultsAndClaude(ClaudeInvoker invoker, ProgressCallback callback) {
            return this.withMongoDefaults().add(new ClaudeFlowAnalyzer(invoker, callback));
        }

        public AnalyzerPipeline build() {
            return new AnalyzerPipeline(this);
        }
    }
}

