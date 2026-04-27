package com.jaranalyzer.service;

import com.jaranalyzer.model.EndpointInfo;
import com.jaranalyzer.model.JarAnalysis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class ChatboxService {

    private static final Logger log = LoggerFactory.getLogger(ChatboxService.class);

    private static final int MAX_ENDPOINTS_IN_CONTEXT = 60;
    private static final int MAX_CONTEXT_CHARS = 40_000;

    private final PersistenceService persistenceService;
    private final ClaudeProcessRunner processRunner;
    private final ChatHistoryService historyService;

    public ChatboxService(PersistenceService persistenceService,
                          @Qualifier("jarClaudeProcessRunner") ClaudeProcessRunner processRunner,
                          ChatHistoryService historyService) {
        this.persistenceService = persistenceService;
        this.processRunner = processRunner;
        this.historyService = historyService;
    }

    public String chat(String jarId, String userMessage) throws Exception {
        JarAnalysis analysis = persistenceService.load(jarId);
        if (analysis == null) {
            throw new IllegalArgumentException("No analysis found for JAR: " + jarId);
        }

        String jarKey = JarNameUtil.normalizeKey(jarId);

        historyService.append(jarKey, "user", userMessage);

        List<Map<String, String>> history = historyService.load(jarKey);

        String context = buildContext(analysis);
        String prompt = buildPrompt(context, history);

        log.info("[Chatbox] JAR={} historySize={} contextLen={} historyBytes={}",
                jarId, history.size(), context.length(), historyService.sizeBytes(jarKey));

        List<String> command = List.of("claude", "-p", "--no-session-persistence");
        String rawOutput = processRunner.runClaudeProcess(command, prompt, null, null);

        if (rawOutput == null || rawOutput.isBlank()) {
            throw new RuntimeException("Claude returned an empty response");
        }
        String reply = rawOutput.trim();

        historyService.append(jarKey, "assistant", reply);

        return reply;
    }

    public List<Map<String, String>> getHistory(String jarId) throws Exception {
        return historyService.load(JarNameUtil.normalizeKey(jarId));
    }

    public void clearHistory(String jarId) throws Exception {
        historyService.clear(JarNameUtil.normalizeKey(jarId));
    }

    private String buildContext(JarAnalysis analysis) {
        StringBuilder sb = new StringBuilder();

        sb.append("=== JAR ANALYSIS CONTEXT ===\n");
        sb.append("JAR: ").append(analysis.getJarName()).append("\n");
        if (analysis.getProjectName() != null) {
            sb.append("Project: ").append(analysis.getProjectName()).append("\n");
        }
        sb.append("Analyzed: ").append(analysis.getAnalyzedAt()).append("\n");
        sb.append("Mode: ").append(analysis.getAnalysisMode()).append("\n");
        sb.append("Total classes: ").append(analysis.getTotalClasses()).append("\n");
        sb.append("Total endpoints: ").append(analysis.getTotalEndpoints()).append("\n\n");

        List<EndpointInfo> endpoints = analysis.getEndpoints();
        if (endpoints == null || endpoints.isEmpty()) {
            sb.append("No endpoints found.\n");
        } else {
            sb.append("=== ENDPOINTS (").append(endpoints.size()).append(") ===\n");
            int shown = Math.min(endpoints.size(), MAX_ENDPOINTS_IN_CONTEXT);
            for (int i = 0; i < shown; i++) {
                appendEndpoint(sb, endpoints.get(i));
            }
            if (endpoints.size() > shown) {
                sb.append("... and ").append(endpoints.size() - shown)
                  .append(" more endpoints (not shown to stay within context limit)\n");
            }
        }

        Set<String> allCollections = new LinkedHashSet<>();
        if (endpoints != null) {
            for (EndpointInfo ep : endpoints) {
                if (ep.getAggregatedCollections() != null) {
                    allCollections.addAll(ep.getAggregatedCollections());
                }
            }
        }
        if (!allCollections.isEmpty()) {
            sb.append("\n=== MONGODB COLLECTIONS (").append(allCollections.size()).append(") ===\n");
            sb.append(String.join(", ", allCollections)).append("\n");
        }

        String result = sb.toString();
        if (result.length() > MAX_CONTEXT_CHARS) {
            result = result.substring(0, MAX_CONTEXT_CHARS)
                    + "\n\n[Context truncated at " + MAX_CONTEXT_CHARS + " chars]\n";
        }
        return result;
    }

    private void appendEndpoint(StringBuilder sb, EndpointInfo ep) {
        sb.append("\n--- ").append(ep.getHttpMethod()).append(" ").append(ep.getFullPath()).append(" ---\n");
        sb.append("Controller: ").append(ep.getControllerSimpleName())
          .append(".").append(ep.getMethodName()).append("\n");

        if (ep.getAggregatedCollections() != null && !ep.getAggregatedCollections().isEmpty()) {
            sb.append("Collections: ").append(String.join(", ", ep.getAggregatedCollections())).append("\n");
        }
        if (ep.getDerivedOperationType() != null) {
            sb.append("Operation: ").append(ep.getDerivedOperationType()).append("\n");
        }

        if (ep.getCallTree() != null) {
            var root = ep.getCallTree();
            if (root.getDomain() != null) sb.append("Domain: ").append(root.getDomain()).append("\n");
            if (root.getModule() != null) sb.append("Module: ").append(root.getModule()).append("\n");
            appendTreeEnrichment(sb, root, 0);
        }
    }

    private void appendTreeEnrichment(StringBuilder sb, com.jaranalyzer.model.CallNode node, int depth) {
        if (node == null || depth > 5) return;

        String indent = "  ".repeat(depth);
        sb.append(indent)
          .append(node.getSimpleClassName() != null ? node.getSimpleClassName() : node.getClassName())
          .append(".").append(node.getMethodName());

        if (node.getStereotype() != null) sb.append(" [").append(node.getStereotype()).append("]");
        if (node.getCollectionsAccessed() != null && !node.getCollectionsAccessed().isEmpty()) {
            sb.append(" -> ").append(String.join(", ", node.getCollectionsAccessed()));
            if (node.getOperationType() != null) sb.append(" (").append(node.getOperationType()).append(")");
        }
        sb.append("\n");

        if (node.getChildren() != null) {
            for (var child : node.getChildren()) {
                appendTreeEnrichment(sb, child, depth + 1);
            }
        }
    }

    private String buildPrompt(String context, List<Map<String, String>> messages) {
        StringBuilder sb = new StringBuilder();

        sb.append("You are an expert Java/Spring Boot code analyst assistant embedded inside the JAR Analyzer tool.\n");
        sb.append("The user has uploaded and analyzed a Spring Boot JAR file. ");
        sb.append("Use the analysis data below to answer questions accurately and concisely.\n\n");
        sb.append(context);
        sb.append("\n=== CONVERSATION ===\n");

        for (Map<String, String> msg : messages) {
            String role = msg.getOrDefault("role", "user");
            String content = msg.getOrDefault("content", "");
            if ("user".equals(role)) {
                sb.append("User: ").append(content).append("\n");
            } else if ("assistant".equals(role)) {
                sb.append("Assistant: ").append(content).append("\n");
            }
        }

        sb.append("Assistant:");
        return sb.toString();
    }
}
