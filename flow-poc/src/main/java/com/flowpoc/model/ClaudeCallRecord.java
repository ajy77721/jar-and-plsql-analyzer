/*
 * Decompiled with CFR 0.152.
 */
package com.flowpoc.model;

public class ClaudeCallRecord {
    private final String nodeLocation;
    private final int chunkIndex;
    private final int totalChunks;
    private final String prompt;
    private final String rawResponse;

    public ClaudeCallRecord(String nodeLocation, int chunkIndex, int totalChunks, String prompt, String rawResponse) {
        this.nodeLocation = nodeLocation;
        this.chunkIndex = chunkIndex;
        this.totalChunks = totalChunks;
        this.prompt = prompt;
        this.rawResponse = rawResponse;
    }

    public String getNodeLocation() {
        return this.nodeLocation;
    }

    public int getChunkIndex() {
        return this.chunkIndex;
    }

    public int getTotalChunks() {
        return this.totalChunks;
    }

    public String getPrompt() {
        return this.prompt;
    }

    public String getRawResponse() {
        return this.rawResponse;
    }
}

