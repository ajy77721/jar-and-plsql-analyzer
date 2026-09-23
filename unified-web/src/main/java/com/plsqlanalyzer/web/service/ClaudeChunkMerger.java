package com.plsqlanalyzer.web.service;

import com.plsqlanalyzer.analyzer.graph.CallGraph;
import com.plsqlanalyzer.analyzer.model.AnalysisResult;
import com.plsqlanalyzer.analyzer.model.TableOperationSummary;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Merges small verification chunks into larger batches and estimates prompt sizes.
 * Extracted from ClaudeVerificationService to keep files focused; logic unchanged.
 */
class ClaudeChunkMerger {

    /** Max tables per chunk — prevents mega-chunks */
    private static final int MAX_TABLES_PER_CHUNK = 20;

    /** Target content size when merging small chunks together */
    private static final int MERGE_TARGET_CHARS = 40_000;

    /**
     * Merge small chunks (no source window, few tables, short estimated prompt) into larger batches
     * to reduce the number of Claude CLI invocations. Large/windowed chunks are kept as-is.
     */
    List<VerificationChunk> mergeSmallChunks(List<VerificationChunk> chunks, AnalysisResult result) {
        List<VerificationChunk> large = new ArrayList<>();
        List<VerificationChunk> small = new ArrayList<>();

        for (VerificationChunk chunk : chunks) {
            boolean isWindowed = chunk.sourceWindowStart > 0 && chunk.sourceWindowEnd > chunk.sourceWindowStart;
            int estimated = estimatePromptSize(chunk, result);
            if (isWindowed || estimated > MERGE_TARGET_CHARS || chunk.tables.size() > MAX_TABLES_PER_CHUNK) {
                large.add(chunk);
            } else {
                small.add(chunk);
            }
        }

        if (small.size() <= 1) {
            // Nothing to merge
            List<VerificationChunk> all = new ArrayList<>(large);
            all.addAll(small);
            return all;
        }

        // Greedy bin-packing: add small chunks to current batch until target size exceeded
        List<VerificationChunk> merged = new ArrayList<>(large);
        VerificationChunk current = null;
        int currentSize = 0;

        for (VerificationChunk sc : small) {
            int scSize = estimatePromptSize(sc, result);

            if (current == null) {
                current = copyChunk(sc);
                currentSize = scSize;
                continue;
            }

            int combinedTables = current.tables.size() + sc.tables.size();
            if (currentSize + scSize > MERGE_TARGET_CHARS || combinedTables > MAX_TABLES_PER_CHUNK) {
                // Finalize current batch
                finalizeMergedName(current);
                merged.add(current);
                current = copyChunk(sc);
                currentSize = scSize;
            } else {
                // Add to current batch
                current.procedureIds = new ArrayList<>(current.procedureIds);
                for (String pid : sc.procedureIds) {
                    if (!current.procedureIds.contains(pid)) {
                        current.procedureIds.add(pid);
                    }
                }
                current.tables.putAll(sc.tables);
                currentSize += scSize;
            }
        }

        if (current != null) {
            finalizeMergedName(current);
            merged.add(current);
        }

        return merged;
    }

    private int estimatePromptSize(VerificationChunk chunk, AnalysisResult result) {
        int size = 0;

        // Estimate findings section size
        for (var entry : chunk.tables.entrySet()) {
            size += entry.getKey().length() + 30; // TABLE: header
            TableOperationSummary summary = entry.getValue();
            if (summary != null) {
                size += summary.getOperations().size() * 40;
                size += summary.getAccessDetails().size() * 80;
            }
        }

        // Estimate source code size
        for (String procId : chunk.procedureIds) {
            String[] parts = procId.toUpperCase().split("\\.");
            String sourceKey = parts.length >= 2 ? parts[0] + "." + parts[1] : procId.toUpperCase();
            // MID-027 — deliberately NOT converted to PersistenceService.getSourceLines' streamed
            // single-key read: this loop asks for a different key per procedureId, potentially many
            // per chunk across many chunks, so the existing "load the whole map once, every
            // subsequent get() is free" behavior is already the right amortized strategy for this
            // access pattern. A streamed read re-parses the file per key; converting this specific
            // call site would trade one full load for N partial re-scans, which loses for any N
            // beyond ~2 keys. See MiscQueryService.getSource for the genuinely single-key consumer
            // this fix was built for.
            List<String> lines = result.getSourceMap() != null ? result.getSourceMap().get(sourceKey) : null;
            if (lines == null) continue;

            CallGraph graph = result.getCallGraph();
            if (graph != null) {
                com.plsqlanalyzer.analyzer.model.CallGraphNode cgNode = graph.getNode(procId);
                if (cgNode != null && cgNode.getStartLine() > 0 && cgNode.getEndLine() > cgNode.getStartLine()) {
                    int lineCount = cgNode.getEndLine() - cgNode.getStartLine() + 1;
                    size += lineCount * 60; // average ~60 chars per line with line numbers
                    continue;
                }
            }
            size += Math.min(lines.size(), 200) * 60;
        }

        size += 1500; // template overhead (instructions, response format)
        return size;
    }

    private VerificationChunk copyChunk(VerificationChunk src) {
        VerificationChunk copy = new VerificationChunk();
        copy.procedureIds = new ArrayList<>(src.procedureIds);
        copy.tables = new LinkedHashMap<>(src.tables);
        copy.name = src.name;
        copy.sourceWindowStart = src.sourceWindowStart;
        copy.sourceWindowEnd = src.sourceWindowEnd;
        return copy;
    }

    private void finalizeMergedName(VerificationChunk chunk) {
        if (chunk.procedureIds.size() <= 1) return; // single-proc chunk, keep original name
        List<String> shortNames = new ArrayList<>();
        for (String pid : chunk.procedureIds) {
            String s = pid.contains(".") ? pid.substring(pid.lastIndexOf('.') + 1) : pid;
            shortNames.add(s);
        }
        String joined = shortNames.size() <= 3
                ? String.join(" + ", shortNames)
                : shortNames.get(0) + " + " + shortNames.get(1) + " + ... (" + shortNames.size() + " procs)";
        chunk.name = joined + " → " + chunk.tables.size() + " tables";
    }
}
