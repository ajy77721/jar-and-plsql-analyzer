package com.plsqlanalyzer.web.parser.service;

import com.plsqlanalyzer.web.parser.service.ClaudePurposeSummaryModels.PurposeChunk;
import com.plsqlanalyzer.web.parser.service.ClaudePurposeSummaryModels.PurposeTarget;

/**
 * Builds prompts for §267's purpose-summarization Claude calls — one call per batch of procedures,
 * modeled on {@link ClaudePromptBuilder}'s structure (findings section + source section + a strict
 * JSON response format) but asking a different question: not "is this correct" but "what does this
 * do, in one sentence".
 */
class ClaudePurposePromptBuilder {

    private static final int MAX_SOURCE_CHARS_PER_PROC = 6_000;
    private static final int MAX_PROMPT_CHARS = 60_000;

    /** Build the batch prompt for one chunk of procedures. */
    String buildPrompt(PurposeChunk chunk) {
        StringBuilder sb = new StringBuilder();
        sb.append("You are summarizing the PURPOSE of PL/SQL-family stored procedures/functions for a ")
          .append("code-comprehension tool. You are NOT verifying correctness and you are NOT listing ")
          .append("table operations again — those are already known and given to you as context only.\n\n")
          .append("For EACH procedure below, write ONE short sentence (max ~20 words) describing what it ")
          .append("DOES from a business/functional point of view (e.g. \"writes an audit log entry for a ")
          .append("sales order change\" — not \"selects from and inserts into a table\"). Optionally, for a ")
          .append("few of its most meaningful individual statements, add a one-line intent.\n\n");

        for (PurposeTarget t : chunk.targets) {
            sb.append("=== PROCEDURE nodeId=\"").append(t.nodeId).append("\" ===\n");
            String qualified = String.join(".", nonBlank(t.schema), nonBlank(t.packageName), nonBlank(t.name));
            sb.append("Name: ").append(qualified.isBlank() ? t.name : qualified).append("\n");
            if (t.objectType != null) sb.append("Type: ").append(t.objectType).append("\n");
            if (!t.tableSummaries.isEmpty()) {
                sb.append("Known table operations: ").append(String.join("; ", t.tableSummaries)).append("\n");
            }
            if (!t.callSummaries.isEmpty()) {
                sb.append("Known calls out: ").append(String.join("; ", t.callSummaries)).append("\n");
            }
            sb.append("Source:\n");
            String src = t.sourceSnippet != null ? t.sourceSnippet : "";
            if (src.length() > MAX_SOURCE_CHARS_PER_PROC) {
                src = src.substring(0, MAX_SOURCE_CHARS_PER_PROC) + "\n... [truncated]";
            }
            sb.append(src).append("\n\n");
        }

        sb.append("=== RESPONSE FORMAT ===\n\n")
          .append("Respond ONLY with a JSON object (no markdown, no explanation outside JSON):\n")
          .append("```json\n{\n  \"procedures\": [\n    {\n")
          .append("      \"nodeId\": \"<the exact nodeId from above>\",\n")
          .append("      \"purpose\": \"one short sentence\",\n")
          .append("      \"perStatement\": [ {\"line\": 123, \"intent\": \"short phrase\"} ]\n")
          .append("    }\n  ]\n}\n```\n")
          .append("\"perStatement\" is optional and best-effort — include it only for a few genuinely ")
          .append("meaningful lines, or omit/empty it. Every procedure given above MUST appear once in ")
          .append("\"procedures\", matched by its exact nodeId.\n");

        String prompt = sb.toString();
        if (prompt.length() > MAX_PROMPT_CHARS) {
            prompt = prompt.substring(0, MAX_PROMPT_CHARS - 200) + "\n... [prompt truncated due to size]\n";
        }
        return prompt;
    }

    private static String nonBlank(String s) { return s == null ? "" : s; }
}
