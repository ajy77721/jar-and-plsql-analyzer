package com.flowpoc.analyzer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowpoc.model.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;

/**
 * Sends the EXISTING analyzer findings (already computed by QueryClassificationAnalyzer,
 * HierarchyFlowAnalyzer, MongoMissingIndexAnalyzer, NplusOneAnalyzer, BulkOperationAnalyzer,
 * AggregationRewriteAnalyzer) to Claude and asks for:
 *   1. Natural-language explanation of each finding's business/performance impact
 *   2. Fix priority order
 *   3. Overall endpoint health assessment
 *
 * This analyzer does NOT detect new issues — it only enriches the existing ones.
 * Run it LAST in AnalyzerPipeline so all prior findings are already attached.
 *
 * API key: pass explicitly or set ANTHROPIC_API_KEY environment variable.
 * If neither is available the analyzer silently returns empty list.
 */
public class ClaudeFlowAnalyzer implements OptimizationAnalyzer {

    private static final String API_URL    = "https://api.anthropic.com/v1/messages";
    private static final String MODEL      = "claude-haiku-4-5-20251001";
    private static final int    MAX_TOKENS = 1024;
    private static final int    TIMEOUT_SEC = 30;

    private final String       apiKey;
    private final HttpClient   http;
    private final ObjectMapper mapper;

    public ClaudeFlowAnalyzer(String apiKey) {
        String key = apiKey != null ? apiKey : System.getenv("ANTHROPIC_API_KEY");
        this.apiKey = key != null ? key : "";
        this.http   = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(TIMEOUT_SEC))
                .build();
        this.mapper = new ObjectMapper();
    }

    @Override
    public List<OptimizationFinding> analyze(FlowResult flowResult) {
        if (apiKey.isBlank()) return Collections.emptyList();

        // Only run if there are existing findings to enrich
        List<OptimizationFinding> existing = flowResult.getOptimizations();
        if (existing.isEmpty()) return Collections.emptyList();

        try {
            String prompt   = buildPrompt(flowResult, existing);
            String body     = buildRequestBody(prompt);
            String response = callClaude(body);
            return parseFindings(response, flowResult.getEndpointPath());
        } catch (Exception ignored) {
            return Collections.emptyList();
        }
    }

    // ── Prompt — passes EXISTING findings, asks Claude to explain + prioritize ─

    private String buildPrompt(FlowResult flow, List<OptimizationFinding> existing) {
        StringBuilder sb = new StringBuilder();
        sb.append("You are a MongoDB + Spring Boot performance consultant reviewing automated analysis results.\n");
        sb.append("The tools have already found these issues for endpoint ")
          .append(flow.getEndpointMethod()).append(" ").append(flow.getEndpointPath())
          .append(":\n\n");

        // List existing findings grouped by severity
        for (OptimizationFinding.Severity sev : OptimizationFinding.Severity.values()) {
            List<OptimizationFinding> bySev = existing.stream()
                    .filter(f -> f.getSeverity() == sev
                              && f.getCategory() != OptimizationFinding.Category.OTHER)
                    .toList();
            if (bySev.isEmpty()) continue;
            sb.append(sev).append(" issues:\n");
            for (OptimizationFinding f : bySev) {
                sb.append("  - [").append(f.getCategory().name().replace("_"," ")).append("] ")
                  .append("collection=").append(f.getTable()).append(": ")
                  .append(f.getDescription()).append("\n");
                if (f.getSuggestedCode() != null && !f.getSuggestedCode().isBlank()) {
                    sb.append("    suggestion: ").append(f.getSuggestedCode()).append("\n");
                }
            }
            sb.append("\n");
        }

        // Query classification summary
        long staticQ = flow.allSteps().stream().flatMap(s -> s.getQueries().stream())
                .filter(q -> q.getDataClass() == ExtractedQuery.DataClass.STATIC).count();
        long transQ  = flow.allSteps().stream().flatMap(s -> s.getQueries().stream())
                .filter(q -> q.getDataClass() == ExtractedQuery.DataClass.TRANSACTIONAL).count();
        sb.append("Query classification: ").append(staticQ).append(" static (cacheable), ")
          .append(transQ).append(" transactional.\n\n");

        sb.append("Tasks:\n");
        sb.append("1. Explain each issue's real-world performance/business impact in one sentence.\n");
        sb.append("2. Give a prioritized fix order (which to fix first).\n");
        sb.append("3. State overall health: GOOD | NEEDS_WORK | CRITICAL\n\n");
        sb.append("Format each output line as:\n");
        sb.append("SUGGESTION [HIGH|MEDIUM|LOW]: <impact explanation + fix order> | CODE: <concrete command or code if different from above>\n");
        return sb.toString();
    }

    // ── HTTP ──────────────────────────────────────────────────────────────────

    private String buildRequestBody(String prompt) throws Exception {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("model", MODEL);
        body.put("max_tokens", MAX_TOKENS);
        body.put("messages", List.of(Map.of("role", "user", "content", prompt)));
        return mapper.writeValueAsString(body);
    }

    private String callClaude(String requestBody) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(API_URL))
                .timeout(Duration.ofSeconds(TIMEOUT_SEC))
                .header("content-type",      "application/json")
                .header("x-api-key",         apiKey)
                .header("anthropic-version", "2023-06-01")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        return resp.statusCode() == 200 ? resp.body() : "";
    }

    // ── Parse ─────────────────────────────────────────────────────────────────

    private List<OptimizationFinding> parseFindings(String responseBody, String endpointPath) {
        List<OptimizationFinding> findings = new ArrayList<>();
        if (responseBody == null || responseBody.isBlank()) return findings;
        try {
            JsonNode root = mapper.readTree(responseBody);
            String text = root.path("content").get(0).path("text").asText("");
            for (String line : text.split("\n")) {
                line = line.trim();
                if (!line.startsWith("SUGGESTION")) continue;

                OptimizationFinding.Severity sev = parseSeverity(line);
                String desc = extractBetween(line, ":", "|").trim();
                String code = extractAfter(line, "CODE:").trim();

                findings.add(new OptimizationFinding(
                        OptimizationFinding.Category.OTHER, sev,
                        endpointPath, null,
                        "[Claude AI] " + desc,
                        "Claude AI", line, code));
            }
        } catch (Exception ignored) {}
        return findings;
    }

    private OptimizationFinding.Severity parseSeverity(String line) {
        String u = line.toUpperCase();
        if (u.contains("[HIGH]"))   return OptimizationFinding.Severity.HIGH;
        if (u.contains("[LOW]"))    return OptimizationFinding.Severity.LOW;
        return OptimizationFinding.Severity.MEDIUM;
    }

    private String extractBetween(String s, String from, String to) {
        int a = s.indexOf(from);
        int b = s.indexOf(to, a + 1);
        if (a < 0) return s;
        return b > a ? s.substring(a + 1, b) : s.substring(a + 1);
    }

    private String extractAfter(String s, String marker) {
        int i = s.toUpperCase().indexOf(marker.toUpperCase());
        return i < 0 ? "" : s.substring(i + marker.length());
    }
}
