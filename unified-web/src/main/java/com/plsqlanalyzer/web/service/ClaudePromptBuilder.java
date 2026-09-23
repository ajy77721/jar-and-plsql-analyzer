package com.plsqlanalyzer.web.service;

import com.plsqlanalyzer.analyzer.graph.CallGraph;
import com.plsqlanalyzer.analyzer.model.AnalysisResult;
import com.plsqlanalyzer.analyzer.model.TableOperationSummary;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Builds the Claude verification prompt for a single chunk. Extracted from
 * ClaudeVerificationService to keep files focused; logic unchanged.
 */
class ClaudePromptBuilder {

    /** Max characters of source code per chunk — each chunk now has at most 500 lines of source */
    private static final int MAX_CHUNK_CHARS = 50_000;

    /** Max total prompt size including context + tables + response format instructions */
    private static final int MAX_PROMPT_CHARS = 80_000;

    String buildPrompt(VerificationChunk chunk, AnalysisResult result) {
        StringBuilder findingsSb = new StringBuilder();

        // Build static analysis findings section
        for (var entry : chunk.tables.entrySet()) {
            String tableName = entry.getKey();
            TableOperationSummary summary = entry.getValue();
            if (summary == null) continue;

            findingsSb.append("TABLE: ").append(tableName);
            if (summary.getSchemaName() != null) findingsSb.append(" (Schema: ").append(summary.getSchemaName()).append(")");
            findingsSb.append("\n");

            Set<?> ops = summary.getOperations();
            if (ops != null && !ops.isEmpty()) {
                findingsSb.append("  Operations: ");
                findingsSb.append(ops.stream().map(Object::toString).collect(Collectors.joining(", ")));
                findingsSb.append("\n");
            }

            for (var detail : summary.getAccessDetails()) {
                findingsSb.append("  - ").append(detail.getOperation()).append(" in ");
                findingsSb.append(detail.getProcedureName() != null ? detail.getProcedureName() : "?");
                if (detail.getLineNumber() > 0) findingsSb.append(" at line ").append(detail.getLineNumber());
                if (detail.getWhereFilters() != null && !detail.getWhereFilters().isEmpty()) {
                    findingsSb.append(" WHERE: ");
                    for (var wf : detail.getWhereFilters()) {
                        findingsSb.append(wf.getColumnName()).append(" ").append(wf.getOperator()).append(" ").append(wf.getValue()).append(", ");
                    }
                }
                findingsSb.append("\n");
            }
            findingsSb.append("\n");
        }

        // Build source window note
        boolean isWindowChunk = chunk.sourceWindowStart > 0 && chunk.sourceWindowEnd > chunk.sourceWindowStart;
        String sourceWindowNote = "";
        if (isWindowChunk) {
            sourceWindowNote = "-- NOTE: This is a source window chunk for a large procedure. " +
                    "Only lines " + chunk.sourceWindowStart + "-" + chunk.sourceWindowEnd +
                    " are shown. Verify table operations within this range. --";
        }

        // Build source code section
        StringBuilder sourceSb = new StringBuilder();
        int sourceCharsRemaining = MAX_CHUNK_CHARS;
        Set<String> includedSources = new HashSet<>();

        for (String procId : chunk.procedureIds) {
            if (sourceCharsRemaining <= 0) break;

            String[] parts = procId.toUpperCase().split("\\.");
            String sourceKey = parts.length >= 2 ? parts[0] + "." + parts[1] : procId.toUpperCase();
            String procName = parts.length >= 3 ? parts[2] : (parts.length >= 2 ? parts[1] : parts[0]);

            // MID-027 — same reasoning as ClaudeChunkMerger's identical loop: many different keys
            // per call, so the full-map-load-once path already amortizes correctly; a streamed
            // single-key read would re-scan the file per key instead. Left unconverted deliberately.
            List<String> lines = result.getSourceMap() != null ? result.getSourceMap().get(sourceKey) : null;
            if (lines == null) continue;

            int includeFrom;
            int includeTo;

            if (isWindowChunk) {
                includeFrom = Math.max(0, chunk.sourceWindowStart - 1);
                includeTo = Math.min(lines.size(), chunk.sourceWindowEnd);
            } else {
                int procStart = -1;
                int procEnd = -1;

                CallGraph graph = result.getCallGraph();
                if (graph != null) {
                    com.plsqlanalyzer.analyzer.model.CallGraphNode cgNode = graph.getNode(procId);
                    if (cgNode != null && cgNode.getStartLine() > 0 && cgNode.getEndLine() > 0) {
                        procStart = Math.max(0, cgNode.getStartLine() - 3);
                        procEnd = Math.min(lines.size(), cgNode.getEndLine() + 1);
                    }
                }

                if (procStart < 0) {
                    String upperProcName = procName.toUpperCase();
                    for (int i = 0; i < lines.size(); i++) {
                        String lineUpper = lines.get(i).toUpperCase().trim();
                        if ((lineUpper.contains("PROCEDURE") || lineUpper.contains("FUNCTION"))
                                && lineUpper.contains(upperProcName)) {
                            procStart = Math.max(0, i - 2);
                            for (int j = i + 1; j < lines.size(); j++) {
                                String lu = lines.get(j).toUpperCase().trim();
                                if ((lu.startsWith("PROCEDURE ") || lu.startsWith("FUNCTION "))
                                        && !lu.contains(upperProcName)) {
                                    procEnd = j;
                                    break;
                                }
                                if (lu.matches("END\\s+" + upperProcName + "\\s*;.*")) {
                                    procEnd = j + 1;
                                    break;
                                }
                            }
                            if (procEnd < 0) procEnd = lines.size();
                            break;
                        }
                    }
                }

                if (procStart < 0) {
                    if (includedSources.contains(sourceKey)) continue;
                    procStart = 0;
                    procEnd = Math.min(lines.size(), 200);
                }

                includeFrom = procStart;
                includeTo = procEnd;
            }

            includedSources.add(sourceKey + ":" + procName);
            int lineCount = includeTo - includeFrom;

            sourceSb.append("-- Source: ").append(sourceKey).append(" > ").append(procName)
              .append(" (lines ").append(includeFrom + 1).append("-").append(includeTo)
              .append(", ").append(lineCount).append(" lines) --\n");

            for (int i = includeFrom; i < includeTo && sourceCharsRemaining > 0; i++) {
                String line = lines.get(i);
                String formatted = String.format("%5d: %s\n", i + 1, line);
                sourceSb.append(formatted);
                sourceCharsRemaining -= formatted.length();
            }
            if (sourceCharsRemaining <= 0) {
                sourceSb.append("... [source truncated — chunk char limit reached]\n");
            }
            sourceSb.append("\n");
        }

        // Load template and fill placeholders
        String prompt = com.jaranalyzer.service.PromptTemplates.buildPlsqlVerificationPrompt(
                findingsSb.toString(), sourceSb.toString(), sourceWindowNote);

        if (prompt.length() > MAX_PROMPT_CHARS) {
            prompt = prompt.substring(0, MAX_PROMPT_CHARS - 200) + "\n... [prompt truncated due to size]\n";
        }

        return prompt;
    }
}
