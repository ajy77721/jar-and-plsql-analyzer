/*
 * Decompiled with CFR 0.152.
 * 
 * Could not load the following classes:
 *  com.fasterxml.jackson.databind.JsonNode
 *  com.fasterxml.jackson.databind.ObjectMapper
 */
package com.flowpoc.analyzer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowpoc.ClaudeInvoker;
import com.flowpoc.ProgressCallback;
import com.flowpoc.analyzer.OptimizationAnalyzer;
import com.flowpoc.model.ClaudeCallRecord;
import com.flowpoc.model.FlowResult;
import com.flowpoc.model.FlowStep;
import com.flowpoc.model.OptimizationFinding;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ClaudeFlowAnalyzer
implements OptimizationAnalyzer {
    private static final String API_URL = "https://api.anthropic.com/v1/messages";
    private static final String MODEL = "claude-haiku-4-5-20251001";
    private static final int MAX_TOKENS = 2048;
    private static final int TIMEOUT_SEC = 120;
    private static final int CHUNK_SIZE = 15;
    private static final int MAX_FINDINGS_PER_NODE = 20;
    private static final int NODE_PARALLELISM = 8;
    private final ClaudeInvoker invoker;
    private final ProgressCallback callback;
    private final ObjectMapper mapper;
    private final String apiKey;
    private final HttpClient http;

    public ClaudeFlowAnalyzer(ClaudeInvoker invoker, ProgressCallback callback) {
        this.invoker = invoker;
        this.callback = callback;
        this.mapper = new ObjectMapper();
        this.apiKey = null;
        this.http = null;
    }

    public ClaudeFlowAnalyzer(String apiKey, ProgressCallback callback) {
        String key = apiKey != null ? apiKey : System.getenv("ANTHROPIC_API_KEY");
        this.apiKey = key != null ? key : "";
        this.invoker = null;
        this.callback = callback;
        this.mapper = new ObjectMapper();
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(120L)).build();
    }

    public ClaudeFlowAnalyzer(String apiKey) {
        this(apiKey, null);
    }

    @Override
    public List<OptimizationFinding> analyze(FlowResult flowResult) {
        boolean hasApiKey;
        boolean hasInvoker = this.invoker != null;
        boolean bl = hasApiKey = this.apiKey != null && !this.apiKey.isBlank();
        if (!hasInvoker && !hasApiKey) {
            return Collections.emptyList();
        }
        List<OptimizationFinding> candidates = flowResult.getOptimizations().stream().filter(f -> f.getCategory() != OptimizationFinding.Category.OTHER).collect(Collectors.toList());
        if (candidates.isEmpty()) {
            return Collections.emptyList();
        }
        Map<String, FlowStep> stepIndex = this.buildStepIndex(flowResult.getRootStep());
        LinkedHashMap<String, List<OptimizationFinding>> byNode = new LinkedHashMap<String, List<OptimizationFinding>>();
        for (OptimizationFinding f2 : candidates) {
            String loc = f2.getLocation() != null ? f2.getLocation() : "unknown";
            byNode.computeIfAbsent(loc, k -> new ArrayList<OptimizationFinding>()).add(f2);
        }
        String ep = flowResult.getEndpointMethod() + " " + flowResult.getEndpointPath();
        int totalNodes = byNode.size();
        if (this.callback != null) {
            this.callback.onClaudePlanReady(ep, totalNodes);
        }
        ExecutorService nodePool = Executors.newFixedThreadPool(Math.min(8, totalNodes));
        AtomicInteger nodeCounter = new AtomicInteger(0);
        List<Future<?>> futures = new ArrayList<Future<?>>();
        for (Map.Entry<String, List<OptimizationFinding>> entry : byNode.entrySet()) {
            String location = entry.getKey();
            List<OptimizationFinding> nodeFindings = this.capByPriority(entry.getValue());
            FlowStep step = stepIndex.get(location);
            int nodeIdx = nodeCounter.incrementAndGet();
            futures.add(nodePool.submit(() -> {
                if (this.callback != null) {
                    this.callback.onClaudeNodeStart(ep, location, nodeIdx, totalNodes);
                }
                int totalChunks = (nodeFindings.size() + 15 - 1) / 15;
                for (int i = 0; i < nodeFindings.size(); i += 15) {
                    int chunkIdx = i / 15 + 1;
                    ArrayList<OptimizationFinding> chunk = new ArrayList<OptimizationFinding>(nodeFindings.subList(i, Math.min(i + 15, nodeFindings.size())));
                    this.enrichChunk(flowResult, step, chunk, location, chunkIdx, totalChunks);
                }
                long enriched = nodeFindings.stream().filter(f -> f.getClaudeInsight() != null).count();
                if (this.callback != null) {
                    this.callback.onClaudeNodeDone(ep, location, (int)enriched);
                }
            }));
        }
        for (Future<?> future : futures) {
            try {
                future.get();
            }
            catch (Exception exception) {}
        }
        nodePool.shutdown();
        return Collections.emptyList();
    }

    private List<OptimizationFinding> capByPriority(List<OptimizationFinding> findings) {
        if (findings.size() <= 20) {
            return findings;
        }
        return findings.stream().sorted(Comparator.comparingInt(f -> switch (f.getSeverity()) {
            default -> throw new IncompatibleClassChangeError();
            case HIGH -> 0;
            case MEDIUM -> 1;
            case LOW -> 2;
        })).limit(20L).collect(Collectors.toList());
    }

    /*
     * WARNING - Removed try catching itself - possible behaviour change.
     */
    private void enrichChunk(FlowResult flow, FlowStep step, List<OptimizationFinding> chunk, String location, int chunkIdx, int totalChunks) {
        String prompt = "";
        String response = "";
        try {
            prompt = this.buildPrompt(flow, step, chunk, location, chunkIdx, totalChunks);
            response = this.callClaude(prompt);
            if (response != null) {
                this.applyInsights(chunk, response);
            }
        }
        catch (Exception rawText) {
        }
        finally {
            String rawText = this.extractRawText(response);
            flow.addClaudeCallLog(new ClaudeCallRecord(location, chunkIdx, totalChunks, prompt, rawText));
        }
    }

    private String callClaude(String prompt) throws Exception {
        if (this.invoker != null) {
            return this.invoker.invoke(prompt);
        }
        String reqBody = this.buildApiRequestBody(prompt);
        return this.callClaudeApi(reqBody);
    }

    private String extractRawText(String response) {
        if (response == null || response.isBlank()) {
            return "(no response)";
        }
        try {
            JsonNode root = this.mapper.readTree(response);
            if (root.has("content") && root.path("content").isArray()) {
                return root.path("content").get(0).path("text").asText(response);
            }
        }
        catch (Exception exception) {
            // empty catch block
        }
        return response;
    }

    private String buildPrompt(FlowResult flow, FlowStep step, List<OptimizationFinding> chunk, String location, int chunkIdx, int totalChunks) {
        StringBuilder sb = new StringBuilder();
        sb.append("You are a senior MongoDB + Spring Boot performance consultant.\n");
        sb.append("Provide concise, actionable insights for these specific findings.\n\n");
        sb.append("## Context\n");
        sb.append("Endpoint: `").append(flow.getEndpointMethod()).append(" ").append(flow.getEndpointPath()).append("`\n");
        sb.append("Node: `").append(location).append("`");
        if (step != null) {
            sb.append("  [").append(step.getKind().name()).append(" layer, depth=").append(step.getDepth()).append("]");
            List<FlowStep> children = step.getChildren().stream().filter(c -> c.getKind() != FlowStep.StepKind.OTHER).collect(Collectors.toList());
            if (!children.isEmpty()) {
                sb.append("\nCalls: ");
                sb.append(children.stream().map(c -> c.getClassName().replaceAll(".*\\.", "") + "." + c.getMethodName() + "()").collect(Collectors.joining(", ")));
            }
        }
        sb.append("\n");
        if (totalChunks > 1) {
            sb.append("Chunk: ").append(chunkIdx).append(" / ").append(totalChunks).append("\n");
        }
        sb.append("\n");
        sb.append("## Findings\n\n");
        for (int i = 0; i < chunk.size(); ++i) {
            OptimizationFinding f = chunk.get(i);
            sb.append(i + 1).append(". **[").append(f.getCategory().name().replace("_", " ")).append(" / ").append((Object)f.getSeverity()).append("]**");
            if (f.getTable() != null) {
                sb.append("  collection=`").append(f.getTable()).append("`");
            }
            sb.append("\n");
            sb.append("   ").append(f.getDescription()).append("\n");
            if (f.getSuggestedCode() != null && !f.getSuggestedCode().isBlank()) {
                Object hint = f.getSuggestedCode();
                if (((String)hint).length() > 200) {
                    hint = ((String)hint).substring(0, 200) + "\u2026";
                }
                sb.append("   Current hint: `").append((String)hint).append("`\n");
            }
            sb.append("\n");
        }
        sb.append("## Required response format\n\n");
        sb.append("Return ONLY valid JSON \u2014 no markdown fences, no explanation outside JSON:\n");
        sb.append("{\n");
        sb.append("  \"nodeHealth\": \"CRITICAL|NEEDS_WORK|ACCEPTABLE|GOOD\",\n");
        sb.append("  \"findings\": [\n");
        sb.append("    {\n");
        sb.append("      \"index\": 1,\n");
        sb.append("      \"impact\": \"one-sentence production impact if this is not fixed\",\n");
        sb.append("      \"priority\": 1,\n");
        sb.append("      \"codeExample\": \"concrete Spring Data / MongoTemplate Java snippet or MongoDB shell command to fix this \u2014 show before/after if replacing a query\"\n");
        sb.append("    }\n");
        sb.append("  ]\n");
        sb.append("}\n\n");
        sb.append("Rules for codeExample:\n");
        sb.append("- For FULL_TABLE_SCAN on AGGREGATE: show adding $match as first stage\n");
        sb.append("- For FULL_TABLE_SCAN on FIND: show adding a filter predicate or .limit(n)\n");
        sb.append("- For MISSING_INDEX: show db.collection.createIndex() shell command\n");
        sb.append("- For N_PLUS_ONE: show replacing a loop with findAllById() or $lookup aggregation\n");
        sb.append("- For CROSS_LAYER_QUERY: show moving the read to the repository method and returning the result\n");
        sb.append("- For CONTROLLER_DB_ACCESS: show extracting DB call into a @Service method\n");
        return sb.toString();
    }

    private String buildApiRequestBody(String prompt) throws Exception {
        LinkedHashMap<String, Object> body = new LinkedHashMap<String, Object>();
        body.put("model", MODEL);
        body.put("max_tokens", 2048);
        body.put("messages", List.of(Map.of("role", "user", "content", prompt)));
        return this.mapper.writeValueAsString(body);
    }

    private String callClaudeApi(String requestBody) throws Exception {
        HttpRequest req = HttpRequest.newBuilder().uri(URI.create(API_URL)).timeout(Duration.ofSeconds(120L)).header("content-type", "application/json").header("x-api-key", this.apiKey).header("anthropic-version", "2023-06-01").POST(HttpRequest.BodyPublishers.ofString(requestBody)).build();
        HttpResponse<String> resp = this.http.send(req, HttpResponse.BodyHandlers.ofString());
        return resp.statusCode() == 200 ? resp.body() : "";
    }

    private void applyInsights(List<OptimizationFinding> chunk, String responseText) {
        if (responseText == null || responseText.isBlank()) {
            return;
        }
        try {
            String text = responseText;
            try {
                JsonNode apiRoot = this.mapper.readTree(responseText);
                if (apiRoot.has("content") && apiRoot.path("content").isArray()) {
                    text = apiRoot.path("content").get(0).path("text").asText(responseText);
                }
            }
            catch (Exception apiRoot) {
                // empty catch block
            }
            String jsonText = this.extractJsonObject(text);
            if (jsonText == null) {
                return;
            }
            JsonNode parsed = this.mapper.readTree(jsonText);
            String nodeHealth = parsed.path("nodeHealth").asText("");
            JsonNode findingsArr = parsed.path("findings");
            if (!findingsArr.isArray()) {
                return;
            }
            for (JsonNode fn : findingsArr) {
                int zeroIdx = fn.path("index").asInt(1) - 1;
                if (zeroIdx < 0 || zeroIdx >= chunk.size()) continue;
                String impact = fn.path("impact").asText("").trim();
                String codeExample = fn.path("codeExample").asText("").trim();
                int priority = fn.path("priority").asInt(0);
                StringBuilder insight = new StringBuilder();
                if (!impact.isBlank()) {
                    insight.append(impact);
                }
                if (priority > 0) {
                    insight.append(" [Fix priority: ").append(priority).append("]");
                }
                if (!codeExample.isBlank() && !"null".equals(codeExample)) {
                    insight.append("\nExample fix: ").append(codeExample);
                }
                if (!nodeHealth.isBlank() && zeroIdx == 0) {
                    insight.append("\nNode health: ").append(nodeHealth);
                }
                if (insight.length() <= 0) continue;
                chunk.get(zeroIdx).setClaudeInsight(insight.toString().trim());
            }
        }
        catch (Exception exception) {
            // empty catch block
        }
    }

    private String extractJsonObject(String text) {
        if (text == null) {
            return null;
        }
        int start = text.indexOf(123);
        int end = text.lastIndexOf(125);
        if (start >= 0 && end > start) {
            return text.substring(start, end + 1);
        }
        return null;
    }

    private Map<String, FlowStep> buildStepIndex(FlowStep root) {
        LinkedHashMap<String, FlowStep> index = new LinkedHashMap<String, FlowStep>();
        this.indexStep(root, index);
        return index;
    }

    private void indexStep(FlowStep step, Map<String, FlowStep> index) {
        if (step == null) {
            return;
        }
        String key = step.getClassName() + "." + step.getMethodName();
        index.putIfAbsent(key, step);
        for (FlowStep child : step.getChildren()) {
            this.indexStep(child, index);
        }
    }
}

