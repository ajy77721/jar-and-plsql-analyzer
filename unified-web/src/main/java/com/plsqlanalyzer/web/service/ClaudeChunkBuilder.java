package com.plsqlanalyzer.web.service;

import com.plsqlanalyzer.analyzer.graph.CallGraph;
import com.plsqlanalyzer.analyzer.model.AnalysisResult;
import com.plsqlanalyzer.analyzer.model.TableOperationSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds verification chunks from a static analysis result. Extracted from
 * ClaudeVerificationService to keep files focused; logic unchanged.
 */
class ClaudeChunkBuilder {

    private static final Logger log = LoggerFactory.getLogger(ClaudeChunkBuilder.class);

    /** Max tables per chunk — prevents mega-chunks */
    private static final int MAX_TABLES_PER_CHUNK = 20;

    /**
     * Max lines of source code per chunk sent to Claude.
     * Keeps prompts focused and within Claude's effective processing range.
     * Large procs are split into overlapping source windows.
     */
    private static final int MAX_SOURCE_LINES_PER_CHUNK = 500;

    /** Overlap between source windows so cross-boundary SQL isn't missed */
    private static final int SOURCE_WINDOW_OVERLAP = 100;

    private final ClaudeChunkMerger chunkMerger;

    ClaudeChunkBuilder(ClaudeChunkMerger chunkMerger) {
        this.chunkMerger = chunkMerger;
    }

    List<VerificationChunk> buildChunks(AnalysisResult result) {
        List<VerificationChunk> chunks = new ArrayList<>();

        Map<String, TableOperationSummary> allTables = result.getTableOperations();
        if (allTables == null || allTables.isEmpty()) return chunks;

        // Build: procId → list of {tableName, accessDetail} only for that proc
        Map<String, Map<String, List<TableOperationSummary.TableAccessDetail>>> procAccess = new LinkedHashMap<>();
        for (var entry : allTables.entrySet()) {
            String tableName = entry.getKey();
            TableOperationSummary summary = entry.getValue();
            for (var detail : summary.getAccessDetails()) {
                String procId = detail.getProcedureId();
                if (procId != null) {
                    procAccess
                        .computeIfAbsent(procId, k -> new LinkedHashMap<>())
                        .computeIfAbsent(tableName, k -> new ArrayList<>())
                        .add(detail);
                }
            }
        }

        int chunkNum = 0;
        int largeProcCount = 0;
        for (var entry : procAccess.entrySet()) {
            String procId = entry.getKey();
            Map<String, List<TableOperationSummary.TableAccessDetail>> tablesForProc = entry.getValue();

            // Build filtered table summaries for this proc
            Map<String, TableOperationSummary> filteredTables = new LinkedHashMap<>();
            for (var tableEntry : tablesForProc.entrySet()) {
                String tbl = tableEntry.getKey();
                TableOperationSummary original = allTables.get(tbl);
                if (original != null) {
                    TableOperationSummary filtered = new TableOperationSummary();
                    filtered.setTableName(original.getTableName());
                    filtered.setSchemaName(original.getSchemaName());
                    filtered.setTableType(original.getTableType());
                    filtered.setExternal(original.isExternal());
                    for (var d : tableEntry.getValue()) {
                        filtered.getAccessDetails().add(d);
                        if (d.getOperation() != null) filtered.getOperations().add(d.getOperation());
                    }
                    filtered.setAccessCount(filtered.getAccessDetails().size());
                    filteredTables.put(tbl, filtered);
                }
            }

            // Determine proc size from CallGraph node
            int procStartLine = 0;
            int procEndLine = 0;
            CallGraph graph = result.getCallGraph();
            if (graph != null) {
                com.plsqlanalyzer.analyzer.model.CallGraphNode cgNode = graph.getNode(procId);
                if (cgNode != null && cgNode.getStartLine() > 0 && cgNode.getEndLine() > cgNode.getStartLine()) {
                    procStartLine = cgNode.getStartLine();
                    procEndLine = cgNode.getEndLine();
                }
            }
            int procLines = (procEndLine > procStartLine) ? (procEndLine - procStartLine + 1) : 0;

            String shortProc = procId.contains(".") ? procId.substring(procId.lastIndexOf('.') + 1) : procId;

            if (procLines > MAX_SOURCE_LINES_PER_CHUNK) {
                // ---- LARGE PROC: split into source-window sub-chunks ----
                largeProcCount++;
                int step = MAX_SOURCE_LINES_PER_CHUNK - SOURCE_WINDOW_OVERLAP;

                for (int winStart = procStartLine; winStart < procEndLine; winStart += step) {
                    int winEnd = Math.min(winStart + MAX_SOURCE_LINES_PER_CHUNK, procEndLine);

                    // Only include tables with operations in this line range
                    Map<String, TableOperationSummary> windowTables = new LinkedHashMap<>();
                    for (var te : filteredTables.entrySet()) {
                        TableOperationSummary ts = te.getValue();
                        for (var ad : ts.getAccessDetails()) {
                            int ln = ad.getLineNumber();
                            // Include if line is in window, or if line is unknown (0)
                            if (ln == 0 || (ln >= winStart && ln <= winEnd)) {
                                windowTables.put(te.getKey(), ts);
                                break;
                            }
                        }
                    }

                    if (windowTables.isEmpty()) continue;

                    VerificationChunk chunk = new VerificationChunk();
                    chunk.id = "chunk_" + (++chunkNum);
                    chunk.procedureIds = List.of(procId);
                    chunk.tables = windowTables;
                    chunk.sourceWindowStart = winStart;
                    chunk.sourceWindowEnd = winEnd;
                    chunk.name = shortProc + " [L" + winStart + "-" + winEnd + "] → " + windowTables.size() + " tables";
                    chunks.add(chunk);
                }
            } else {
                // ---- NORMAL PROC: split by table count if >10 tables ----
                List<List<String>> tableBatches = new ArrayList<>();
                List<String> tableNames = new ArrayList<>(filteredTables.keySet());
                for (int i = 0; i < tableNames.size(); i += MAX_TABLES_PER_CHUNK) {
                    tableBatches.add(tableNames.subList(i, Math.min(i + MAX_TABLES_PER_CHUNK, tableNames.size())));
                }

                for (List<String> batch : tableBatches) {
                    VerificationChunk chunk = new VerificationChunk();
                    chunk.id = "chunk_" + (++chunkNum);
                    chunk.procedureIds = List.of(procId);
                    chunk.tables = new LinkedHashMap<>();
                    for (String tbl : batch) {
                        if (filteredTables.containsKey(tbl)) {
                            chunk.tables.put(tbl, filteredTables.get(tbl));
                        }
                    }
                    chunk.name = shortProc + " → " + batch.size() + " tables";
                    chunks.add(chunk);
                }
            }
        }

        log.info("Built {} initial chunks from {} procedures ({} large) across {} tables",
                chunks.size(), procAccess.size(), largeProcCount, allTables.size());

        // Merge small chunks to reduce Claude CLI invocations.
        // Small procs with few tables produce tiny prompts (~5-10KB).
        // Batching them into ~40KB groups cuts total chunks significantly.
        int beforeMerge = chunks.size();
        chunks = chunkMerger.mergeSmallChunks(chunks, result);
        if (chunks.size() < beforeMerge) {
            log.info("Merged small chunks: {} → {} (saved {} Claude calls)",
                    beforeMerge, chunks.size(), beforeMerge - chunks.size());
        }

        // Re-number chunk IDs sequentially
        for (int i = 0; i < chunks.size(); i++) {
            chunks.get(i).id = "chunk_" + (i + 1);
        }

        return chunks;
    }
}
