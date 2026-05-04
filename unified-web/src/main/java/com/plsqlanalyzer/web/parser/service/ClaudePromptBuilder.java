package com.plsqlanalyzer.web.parser.service;

import com.plsqlanalyzer.web.parser.service.ClaudeVerificationModels.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Builds verification prompts for the Claude CLI.
 * Separated from ClaudeVerificationService to keep files focused.
 */
class ClaudePromptBuilder {

    private static final Logger log = LoggerFactory.getLogger(ClaudePromptBuilder.class);

    private static final int MAX_CHUNK_CHARS = 50_000;
    private static final int MAX_PROMPT_CHARS = 80_000;

    private final AnalysisDataReader analysisReader;

    ClaudePromptBuilder(AnalysisDataReader analysisReader) {
        this.analysisReader = analysisReader;
    }

    /**
     * Build the full prompt for a verification chunk.
     */
    String buildPrompt(VerificationChunk chunk, AnalysisData data) {
        String findings = buildFindingsSection(chunk);
        String callChain = buildCallChainSection(chunk, data);
        String sourceWindowNote = buildSourceWindowNote(chunk);
        String sourceCode = buildSourceSection(chunk, data);

        String prompt = buildFromTemplate(findings, callChain, sourceCode, sourceWindowNote);
        if (prompt.length() > MAX_PROMPT_CHARS) {
            prompt = prompt.substring(0, MAX_PROMPT_CHARS - 200) + "\n... [prompt truncated due to size]\n";
        }
        return prompt;
    }

    private String buildFindingsSection(VerificationChunk chunk) {
        StringBuilder sb = new StringBuilder();
        Map<String, List<TableOp>> byTable = ChunkingUtils.groupByTable(chunk.tables);

        for (var entry : byTable.entrySet()) {
            String tableKey = entry.getKey();
            List<TableOp> ops = entry.getValue();

            sb.append("TABLE: ").append(tableKey).append("\n");
            Set<String> opTypes = new LinkedHashSet<>();
            for (TableOp op : ops) opTypes.add(op.operation);
            sb.append("  Operations: ").append(String.join(", ", opTypes)).append("\n");

            for (TableOp op : ops) {
                sb.append("  - ").append(op.operation);
                if (op.line > 0) sb.append(" at line ").append(op.line);
                sb.append("\n");
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    /**
     * Build a concise call chain section showing which procedures each chunk node calls
     * and what tables those callees touch. Gives Claude context about indirect table access.
     * Returns empty string if no call graph data exists for the chunk's nodes.
     */
    private String buildCallChainSection(VerificationChunk chunk, AnalysisData data) {
        if (data.callGraph.isEmpty()) return "";

        StringBuilder sb = new StringBuilder();
        for (String nodeId : chunk.nodeIds) {
            List<CallEdge> calls = data.callGraph.get(nodeId);
            if (calls == null || calls.isEmpty()) continue;

            NodeData callerNode = data.nodes.get(nodeId);
            String callerName = (callerNode != null && callerNode.name != null)
                    ? callerNode.name : nodeId;

            sb.append(callerName).append(" calls:\n");
            for (CallEdge edge : calls) {
                String calleeName = edge.toName != null ? edge.toName : edge.toNodeId;
                sb.append("  -> ").append(calleeName);

                // Append tables the callee touches (concise, one line)
                NodeData calleeNode = data.nodes.get(edge.toNodeId);
                if (calleeNode != null && calleeNode.tableOps != null && !calleeNode.tableOps.isEmpty()) {
                    Set<String> tableNames = new LinkedHashSet<>();
                    for (TableOp op : calleeNode.tableOps) {
                        tableNames.add(op.tableName);
                    }
                    sb.append(" (touches: ").append(String.join(", ", tableNames)).append(")");
                }
                sb.append("\n");
            }
            sb.append("\n");
        }

        return sb.toString();
    }

    private String buildSourceWindowNote(VerificationChunk chunk) {
        boolean isWindow = chunk.sourceWindowStart > 0 && chunk.sourceWindowEnd > chunk.sourceWindowStart;
        if (!isWindow) return "";
        return "-- NOTE: This is a source window chunk for a large procedure. "
                + "Only lines " + chunk.sourceWindowStart + "-" + chunk.sourceWindowEnd
                + " are shown. Verify table operations within this range. --";
    }

    private String buildSourceSection(VerificationChunk chunk, AnalysisData data) {
        boolean isWindowChunk = chunk.sourceWindowStart > 0 && chunk.sourceWindowEnd > chunk.sourceWindowStart;
        StringBuilder sb = new StringBuilder();
        int charsRemaining = MAX_CHUNK_CHARS;
        Set<String> includedSources = new HashSet<>();

        for (String nodeId : chunk.nodeIds) {
            if (charsRemaining <= 0) break;

            NodeData node = data.nodes.get(nodeId);
            if (node == null) continue;

            String sourceCode = loadSourceCode(data.analysisName, node);
            if (sourceCode == null) continue;

            String[] lines = sourceCode.split("\n", -1);
            String sourceKey = node.sourceFile != null ? node.sourceFile : nodeId;

            int includeFrom, includeTo;
            if (isWindowChunk) {
                includeFrom = Math.max(0, chunk.sourceWindowStart - 1);
                includeTo = Math.min(lines.length, chunk.sourceWindowEnd);
            } else if (node.lineStart > 0 && node.lineEnd > node.lineStart) {
                includeFrom = Math.max(0, node.lineStart - 3);
                includeTo = Math.min(lines.length, node.lineEnd + 1);
            } else {
                if (includedSources.contains(sourceKey)) continue;
                includeFrom = 0;
                includeTo = Math.min(lines.length, 200);
            }

            includedSources.add(sourceKey + ":" + nodeId);
            int lineCount = includeTo - includeFrom;

            sb.append("-- Source: ").append(sourceKey)
                    .append(" > ").append(node.name != null ? node.name : nodeId)
                    .append(" (lines ").append(includeFrom + 1).append("-").append(includeTo)
                    .append(", ").append(lineCount).append(" lines) --\n");

            for (int i = includeFrom; i < includeTo && charsRemaining > 0; i++) {
                String formatted = String.format("%5d: %s\n", i + 1, i < lines.length ? lines[i] : "");
                sb.append(formatted);
                charsRemaining -= formatted.length();
            }
            if (charsRemaining <= 0) {
                sb.append("... [source truncated]\n");
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    private String loadSourceCode(String analysisName, NodeData node) {
        if (node.sourceFile == null) return null;
        try {
            return analysisReader.getSource(analysisName, node.sourceFile);
        } catch (Exception e) {
            log.debug("Cannot load source for {}: {}", node.nodeId, e.getMessage());
            return null;
        }
    }

    private String buildFromTemplate(String findings, String callChain,
                                     String sourceCode, String sourceWindowNote) {
        return "You are verifying the results of a PL/SQL static code analysis tool.\n"
                + "The tool analyzed Oracle PL/SQL packages and extracted table operations "
                + "(SELECT, INSERT, UPDATE, DELETE, MERGE, TRUNCATE).\n"
                + "Your task is to verify whether these table operations are correct "
                + "by examining the actual PL/SQL source code.\n\n"
                + "For each table operation found by static analysis, determine:\n"
                + "- CONFIRMED: The operation is present in the code and correctly identified\n"
                + "- REMOVED: The operation is NOT actually in the code (false positive)\n"
                + "- NEW: You found table operations that the static analysis MISSED\n\n"
                + "=== STATIC ANALYSIS FINDINGS ===\n\n"
                + findings + "\n"
                + (callChain.isEmpty() ? ""
                        : "=== CALL CHAIN (indirect table access via called procedures) ===\n\n"
                        + callChain + "\n")
                + (sourceWindowNote.isEmpty() ? "" : sourceWindowNote + "\n\n")
                + "=== PL/SQL SOURCE CODE ===\n\n"
                + sourceCode + "\n"
                + "=== RESPONSE FORMAT ===\n\n"
                + "Respond ONLY with a JSON object in this exact format "
                + "(no markdown, no explanation outside JSON):\n"
                + "```json\n"
                + "{\n"
                + "  \"tables\": [\n"
                + "    {\n"
                + "      \"tableName\": \"TABLE_NAME\",\n"
                + "      \"operations\": [\n"
                + "        {\n"
                + "          \"operation\": \"SELECT|INSERT|UPDATE|DELETE|MERGE|TRUNCATE\",\n"
                + "          \"status\": \"CONFIRMED|REMOVED|NEW\",\n"
                + "          \"procedureName\": \"procedure that performs this\",\n"
                + "          \"lineNumber\": 123,\n"
                + "          \"reason\": \"brief explanation\"\n"
                + "        }\n"
                + "      ]\n"
                + "    }\n"
                + "  ],\n"
                + "  \"summary\": \"brief overall summary\"\n"
                + "}\n"
                + "```\n";
    }
}
