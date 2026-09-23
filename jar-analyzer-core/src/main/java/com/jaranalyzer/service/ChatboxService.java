package com.jaranalyzer.service;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.*;

@Service
public class ChatboxService {

    private static final Logger log = LoggerFactory.getLogger(ChatboxService.class);

    private static final int MAX_ENDPOINTS_IN_CONTEXT = 60;
    private static final int MAX_CONTEXT_CHARS = 40_000;
    private static final int MAX_TREE_DEPTH = 5;

    private final PersistenceService persistenceService;
    private final ClaudeProcessRunner processRunner;
    private final ChatHistoryService historyService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ChatboxService(PersistenceService persistenceService,
                          @Qualifier("jarClaudeProcessRunner") ClaudeProcessRunner processRunner,
                          ChatHistoryService historyService) {
        this.persistenceService = persistenceService;
        this.processRunner = processRunner;
        this.historyService = historyService;
    }

    public String chat(String jarId, String userMessage) throws Exception {
        // Was persistenceService.load(jarId) — the whole JarAnalysis object graph, up to 912 MB
        // measured elsewhere in this registry, to build a context capped at MAX_CONTEXT_CHARS.
        // §16 audited 8 load()/loadStatic() callers and cleared all of them as legitimate
        // whole-analysis operations; this one was missed, and unlike those eight it runs
        // synchronously per chat message rather than once in a background job. See §30 and
        // ChatContextBuilder.
        Map<String, Object> header = persistenceService.readHeader(jarId);
        if (header == null) {
            throw new IllegalArgumentException("No analysis found for JAR: " + jarId);
        }

        String jarKey = JarNameUtil.normalizeKey(jarId);

        historyService.append(jarKey, "user", userMessage);

        List<Map<String, String>> history = historyService.load(jarKey);

        InputStream in = persistenceService.openDocumentStream(jarId);
        if (in == null) {
            // The header sidecar exists but the document it describes does not — an
            // inconsistency the old code could not have detected either, since load() would
            // simply have returned null and hit the same "No analysis found" path.
            throw new IllegalArgumentException("No analysis found for JAR: " + jarId);
        }
        String context;
        try (in; JsonParser parser = objectMapper.getFactory().createParser(in)) {
            context = ChatContextBuilder.build(parser, objectMapper, header,
                    MAX_ENDPOINTS_IN_CONTEXT, MAX_CONTEXT_CHARS, MAX_TREE_DEPTH);
        }
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

    // buildContext/appendEndpoint/appendTreeEnrichment were here, operating on the fully
    // materialised JarAnalysis object graph. Replaced by ChatContextBuilder.build, which reads
    // the same three things — header, first MAX_ENDPOINTS_IN_CONTEXT endpoints in full, every
    // endpoint's aggregatedCollections — by streaming, so nothing but the header sidecar and one
    // endpoint at a time is ever held in heap.

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
