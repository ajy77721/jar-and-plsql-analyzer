/*
 * Decompiled with CFR 0.152.
 */
package com.flowpoc;

import com.flowpoc.model.FlowResult;

public interface ProgressCallback {
    default public void onEndpointStart(int index, int total, String endpoint) {
    }

    default public void onEndpointDone(int index, int total, String endpoint, FlowResult result) {
    }

    default public void onClaudePlanReady(String endpoint, int totalNodes) {
    }

    default public void onClaudeNodeStart(String endpoint, String node, int nodeIndex, int totalNodes) {
    }

    default public void onClaudeNodeDone(String endpoint, String node, int enrichedFindings) {
    }
}

