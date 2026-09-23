/*
 * Decompiled with CFR 0.152.
 */
package com.flowpoc.analyzer;

import com.flowpoc.model.FlowResult;
import com.flowpoc.model.OptimizationFinding;
import java.util.List;

public interface OptimizationAnalyzer {
    public List<OptimizationFinding> analyze(FlowResult var1);
}

