package com.plsqlanalyzer.web.parser.service;

import com.plsqlanalyzer.web.parser.service.ClaudeVerificationModels.*;
import com.plsqlanalyzer.web.promptconfig.PromptConfigService;
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
    /** Optional — supplies additive global + per-analysis context/instructions. May be null. */
    private final PromptConfigService promptConfig;

    ClaudePromptBuilder(AnalysisDataReader analysisReader) {
        this(analysisReader, null);
    }

    ClaudePromptBuilder(AnalysisDataReader analysisReader, PromptConfigService promptConfig) {
        this.analysisReader = analysisReader;
        this.promptConfig = promptConfig;
    }

    /**
     * Build the full prompt for a verification chunk.
     */
    String buildPrompt(VerificationChunk chunk, AnalysisData data) {
        String findings = buildFindingsSection(chunk)
                + buildJoinsSection(chunk)
                + buildSequencesSection(chunk)
                + buildDynamicSqlSection(chunk)
                + buildPredicatesSection(chunk);
        String callChain = buildCallChainSection(chunk, data);
        String sourceWindowNote = buildSourceWindowNote(chunk);
        String sourceCode = buildSourceSection(chunk, data);

        String prompt = buildFromTemplate(findings, callChain, sourceCode, sourceWindowNote, data.dialect, data.analysisName);
        // Additively append optional global + per-analysis context/instructions.
        prompt = prompt + buildAdditionalBlocks(data.analysisName);
        if (prompt.length() > MAX_PROMPT_CHARS) {
            prompt = prompt.substring(0, MAX_PROMPT_CHARS - 200) + "\n... [prompt truncated due to size]\n";
        }
        return prompt;
    }

    /**
     * Build the optional, clearly-delimited additive blocks appended after the base prompt.
     * Combines global extras with per-analysis context/append. Returns "" when nothing is set
     * or when no PromptConfigService is wired in. Never alters the base template text.
     */
    private String buildAdditionalBlocks(String analysisName) {
        if (promptConfig == null) return "";
        String globalContext = "", globalAppend = "", perContext = "", perAppend = "";
        try {
            Map<String, String> extras = promptConfig.getExtras();
            if (extras != null) {
                globalContext = nz(extras.get("globalContext"));
                globalAppend = nz(extras.get("globalAppend"));
            }
            if (analysisName != null && !analysisName.isBlank()) {
                Map<String, Object> a = promptConfig.getAnalysis(analysisName);
                if (a != null) {
                    perContext = nz(a.get("context") == null ? null : a.get("context").toString());
                    perAppend = nz(a.get("append") == null ? null : a.get("append").toString());
                }
            }
        } catch (Exception e) {
            log.debug("Could not resolve prompt extras: {}", e.getMessage());
            return "";
        }
        String context = joinBlocks(globalContext, perContext);
        String append = joinBlocks(globalAppend, perAppend);
        StringBuilder sb = new StringBuilder();
        if (!context.isBlank()) {
            sb.append("\n=== ADDITIONAL CONTEXT ===\n").append(context).append("\n");
        }
        if (!append.isBlank()) {
            sb.append("\n=== ADDITIONAL INSTRUCTIONS ===\n").append(append).append("\n");
        }
        return sb.toString();
    }

    private static String nz(String s) { return s == null ? "" : s; }

    private static String joinBlocks(String a, String b) {
        String x = nz(a).trim(), y = nz(b).trim();
        if (x.isEmpty()) return y;
        if (y.isEmpty()) return x;
        return x + "\n" + y;
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

    private String buildJoinsSection(VerificationChunk chunk) {
        if (chunk.joins == null || chunk.joins.isEmpty()) return "";
        StringBuilder sb = new StringBuilder("JOINS:\n");
        for (JoinOp j : chunk.joins) {
            sb.append("  - ").append(j.leftTable).append(" ")
                    .append(j.joinType != null ? j.joinType : "?").append(" JOIN ")
                    .append(j.rightTable);
            if (j.condition != null && !j.condition.isBlank()) sb.append(" ON ").append(j.condition);
            if (j.line > 0) sb.append(" at line ").append(j.line);
            sb.append("\n");
        }
        sb.append("\n");
        return sb.toString();
    }

    private String buildSequencesSection(VerificationChunk chunk) {
        if (chunk.sequences == null || chunk.sequences.isEmpty()) return "";
        StringBuilder sb = new StringBuilder("SEQUENCES:\n");
        for (SequenceOp s : chunk.sequences) {
            sb.append("  - ").append(s.sequenceName).append(".").append(s.operation);
            if (s.line > 0) sb.append(" at line ").append(s.line);
            sb.append("\n");
        }
        sb.append("\n");
        return sb.toString();
    }

    private String buildDynamicSqlSection(VerificationChunk chunk) {
        if (chunk.dynamicSql == null || chunk.dynamicSql.isEmpty()) return "";
        StringBuilder sb = new StringBuilder("DYNAMIC SQL:\n");
        for (DynamicSqlOp d : chunk.dynamicSql) {
            sb.append("  - ").append(d.type != null ? d.type : "?");
            if (d.line > 0) sb.append(" at line ").append(d.line);
            if (d.sqlExpression != null && !d.sqlExpression.isBlank()) {
                String snippet = d.sqlExpression.length() > 200 ? d.sqlExpression.substring(0, 200) + "..." : d.sqlExpression;
                sb.append(": ").append(snippet.replace("\n", " "));
            }
            sb.append("\n");
        }
        sb.append("\n");
        return sb.toString();
    }

    private String buildPredicatesSection(VerificationChunk chunk) {
        if (chunk.predicates == null || chunk.predicates.isEmpty()) return "";
        StringBuilder sb = new StringBuilder("PREDICATES (WHERE clause filters):\n");
        for (PredicateOp p : chunk.predicates) {
            sb.append("  - ").append(p.tableName).append(".").append(p.columnName)
                    .append(" ").append(p.operator != null ? p.operator : "?")
                    .append(" ").append(p.value != null ? p.value : "?");
            if (p.line > 0) sb.append(" at line ").append(p.line);
            sb.append("\n");
        }
        sb.append("\n");
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

    /** Human-readable procedural-SQL language name for the given dialect. */
    private static String languageName(String dialect) {
        if (dialect == null) return "SQL";
        switch (dialect.toUpperCase()) {
            case "ORACLE":    return "Oracle PL/SQL";
            case "SQLSERVER": return "SQL Server T-SQL";
            case "MYSQL":     return "MySQL SQL";
            case "DB2":       return "IBM Db2 SQL PL";
            case "POSTGRES":  return "PostgreSQL PL/pgSQL";
            default:          return "SQL";
        }
    }

    /** dialect → editable-template slug (oracle/mysql/sqlserver/db2). */
    private static String dialectSlug(String dialect) {
        if (dialect == null) return "oracle";
        switch (dialect.toUpperCase()) {
            case "SQLSERVER": return "sqlserver";
            case "MYSQL":     return "mysql";
            case "DB2":       return "db2";
            case "POSTGRES":  return "postgres";
            case "ORACLE":    default: return "oracle";
        }
    }

    private String buildFromTemplate(String findings, String callChain,
                                     String sourceCode, String sourceWindowNote,
                                     String dialect, String analysisName) {
        // Prefer the editable per-dialect template (data/prompts/plsql-<dialect>-verification.txt),
        // honoring per-analysis overrides. Falls back to the inline default below.
        if (promptConfig != null) {
            String tmplName = "plsql-" + dialectSlug(dialect) + "-verification";
            String tmpl = promptConfig.effectiveTemplate(tmplName, analysisName);
            if (tmpl != null && tmpl.contains("{{SOURCE_CODE}}")) {
                String callChainBlock = callChain.isEmpty() ? ""
                        : "=== CALL CHAIN (indirect table access via called procedures) ===\n\n" + callChain + "\n";
                String noteBlock = sourceWindowNote.isEmpty() ? "" : sourceWindowNote + "\n\n";
                return tmpl.replace("{{FINDINGS}}", findings)
                        .replace("{{CALL_CHAIN}}", callChainBlock)
                        .replace("{{SOURCE_WINDOW_NOTE}}", noteBlock)
                        .replace("{{SOURCE_CODE}}", sourceCode);
            }
        }
        String lang = languageName(dialect);
        return "You are verifying the results of a " + lang + " static code analysis tool.\n"
                + "The tool analyzed " + lang + " stored procedures/functions and extracted table operations "
                + "(SELECT, INSERT, UPDATE, DELETE, MERGE, TRUNCATE).\n"
                + "Your task is to verify whether these table operations are correct "
                + "by examining the actual " + lang + " source code.\n\n"
                + "For each table operation found by static analysis, determine:\n"
                + "- CONFIRMED: The operation is present in the code and correctly identified\n"
                + "- REMOVED: The operation is NOT actually in the code (false positive)\n"
                + "- NEW: You found table operations that the static analysis MISSED\n\n"
                + "ALSO verify the CALL relationships (which procedures/functions each unit calls). "
                + "Using the CALL CHAIN section and the source, classify each call edge as "
                + "CONFIRMED (the call really exists), REMOVED (false-positive edge not in the code), "
                + "or NEW (a call the static analysis MISSED, e.g. inside dynamic SQL / EXEC).\n\n"
                + "ALSO verify, using the same CONFIRMED/REMOVED/NEW classification, any JOINS, "
                + "SEQUENCES, DYNAMIC SQL, and PREDICATES (WHERE-clause filters) listed below: "
                + "confirm each is really present in the source, flag any that are false positives "
                + "(REMOVED), and report any the static analysis missed (NEW).\n\n"
                + "=== STATIC ANALYSIS FINDINGS ===\n\n"
                + findings + "\n"
                + (callChain.isEmpty() ? ""
                        : "=== CALL CHAIN (indirect table access via called procedures) ===\n\n"
                        + callChain + "\n")
                + (sourceWindowNote.isEmpty() ? "" : sourceWindowNote + "\n\n")
                + "=== " + lang + " SOURCE CODE ===\n\n"
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
                + "  \"calls\": [\n"
                + "    {\n"
                + "      \"fromProcedure\": \"caller name\",\n"
                + "      \"toProcedure\": \"callee name\",\n"
                + "      \"status\": \"CONFIRMED|REMOVED|NEW\",\n"
                + "      \"lineNumber\": 123,\n"
                + "      \"reason\": \"brief explanation\"\n"
                + "    }\n"
                + "  ],\n"
                + "  \"joins\": [\n"
                + "    {\n"
                + "      \"leftTable\": \"LEFT_TABLE\",\n"
                + "      \"rightTable\": \"RIGHT_TABLE\",\n"
                + "      \"joinType\": \"INNER|LEFT|RIGHT|CROSS|FULL\",\n"
                + "      \"status\": \"CONFIRMED|REMOVED|NEW\",\n"
                + "      \"lineNumber\": 123,\n"
                + "      \"reason\": \"brief explanation\"\n"
                + "    }\n"
                + "  ],\n"
                + "  \"sequences\": [\n"
                + "    {\n"
                + "      \"sequenceName\": \"SEQUENCE_NAME\",\n"
                + "      \"operation\": \"NEXTVAL|CURRVAL\",\n"
                + "      \"status\": \"CONFIRMED|REMOVED|NEW\",\n"
                + "      \"lineNumber\": 123,\n"
                + "      \"reason\": \"brief explanation\"\n"
                + "    }\n"
                + "  ],\n"
                + "  \"dynamicSql\": [\n"
                + "    {\n"
                + "      \"type\": \"EXECUTE_IMMEDIATE|EXEC|SET_SQL|...\",\n"
                + "      \"sqlExpression\": \"the dynamic SQL text\",\n"
                + "      \"status\": \"CONFIRMED|REMOVED|NEW\",\n"
                + "      \"lineNumber\": 123,\n"
                + "      \"reason\": \"brief explanation\"\n"
                + "    }\n"
                + "  ],\n"
                + "  \"predicates\": [\n"
                + "    {\n"
                + "      \"tableName\": \"TABLE_NAME\",\n"
                + "      \"columnName\": \"COLUMN_NAME\",\n"
                + "      \"operator\": \"=|>|<|LIKE|IN|...\",\n"
                + "      \"value\": \"literal or bind value\",\n"
                + "      \"status\": \"CONFIRMED|REMOVED|NEW\",\n"
                + "      \"lineNumber\": 123,\n"
                + "      \"reason\": \"brief explanation\"\n"
                + "    }\n"
                + "  ],\n"
                + "  \"summary\": \"brief overall summary\"\n"
                + "}\n"
                + "```\n";
    }
}
