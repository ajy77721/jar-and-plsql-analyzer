package com.analyzer.chat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.plsqlanalyzer.web.service.ClaudeProcessRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ChatService {

    private static final Logger log = LoggerFactory.getLogger(ChatService.class);
    private static final int MAX_CONTEXT_MESSAGES = 20;
    private static final int CLAUDE_TIMEOUT_SECONDS = 300;

    private final ClaudeProcessRunner processRunner;
    private final Path chatDataDir;
    private final ObjectMapper mapper;
    private final ConcurrentHashMap<String, ReentrantLock> sessionLocks = new ConcurrentHashMap<>();

    private AnalysisContextProvider jarContextProvider;
    private AnalysisContextProvider plsqlContextProvider;

    private static final List<String> CHAT_TYPES = List.of("classic", "new");

    public ChatService(ClaudeProcessRunner processRunner, String dataDir) {
        this.processRunner = processRunner;
        this.chatDataDir = Path.of(dataDir);
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        try {
            Files.createDirectories(chatDataDir.resolve("classic"));
            Files.createDirectories(chatDataDir.resolve("new"));
        } catch (IOException e) {
            log.error("Failed to create chat data dirs: {}", e.getMessage());
        }
    }

    public ChatSession createSession(String name, String scope, String chatType, Map<String, String> scopeContext) {
        ChatSession session = new ChatSession(name, scope, chatType, scopeContext);
        saveSession(session);
        log.info("Created chat session: {} [{}] scope={} type={}", session.getId(), name, scope, session.getChatType());
        return session;
    }

    public List<Map<String, Object>> listSessions(String scopeFilter, String chatType) {
        List<Map<String, Object>> summaries = new ArrayList<>();
        List<Path> dirs = chatType != null
                ? List.of(chatDataDir.resolve(chatType))
                : List.of(chatDataDir.resolve("classic"), chatDataDir.resolve("new"));

        for (Path dir : dirs) {
            if (!Files.isDirectory(dir)) continue;
            try (Stream<Path> files = Files.list(dir)) {
                files.filter(p -> p.toString().endsWith(".json"))
                        .forEach(p -> {
                            try {
                                ChatSession s = mapper.readValue(p.toFile(), ChatSession.class);
                                if (scopeFilter != null && !scopeFilter.equalsIgnoreCase(s.getScope())) return;
                                Map<String, Object> summary = new LinkedHashMap<>();
                                summary.put("id", s.getId());
                                summary.put("name", s.getName());
                                summary.put("scope", s.getScope());
                                summary.put("chatType", s.getChatType());
                                summary.put("messageCount", s.getMessages().size());
                                summary.put("createdAt", s.getCreatedAt());
                                summary.put("updatedAt", s.getUpdatedAt());
                                summaries.add(summary);
                            } catch (IOException e) {
                                log.debug("Failed to read session file {}: {}", p, e.getMessage());
                            }
                        });
            } catch (IOException e) {
                log.error("Failed to list sessions in {}: {}", dir, e.getMessage());
            }
        }

        summaries.sort((a, b) -> {
            Instant ua = (Instant) b.get("updatedAt");
            Instant ub = (Instant) a.get("updatedAt");
            if (ua == null || ub == null) return 0;
            return ua.compareTo(ub);
        });
        return summaries;
    }

    public ChatSession getSession(String id) {
        for (String type : CHAT_TYPES) {
            Path file = chatDataDir.resolve(type).resolve(id + ".json");
            if (Files.exists(file)) {
                try {
                    return mapper.readValue(file.toFile(), ChatSession.class);
                } catch (IOException e) {
                    log.error("Failed to read session {}: {}", id, e.getMessage());
                    return null;
                }
            }
        }
        Path legacy = chatDataDir.resolve(id + ".json");
        if (Files.exists(legacy)) {
            try {
                return mapper.readValue(legacy.toFile(), ChatSession.class);
            } catch (IOException e) {
                log.error("Failed to read legacy session {}: {}", id, e.getMessage());
            }
        }
        return null;
    }

    public boolean deleteSession(String id) {
        sessionLocks.remove(id);
        for (String type : CHAT_TYPES) {
            Path file = chatDataDir.resolve(type).resolve(id + ".json");
            try {
                if (Files.deleteIfExists(file)) return true;
            } catch (IOException e) {
                log.error("Failed to delete session {}: {}", id, e.getMessage());
            }
        }
        try {
            return Files.deleteIfExists(chatDataDir.resolve(id + ".json"));
        } catch (IOException e) {
            return false;
        }
    }

    public ChatMessage sendMessage(String sessionId, String userMessage, String pageContext) {
        ChatSession session = getSession(sessionId);
        if (session == null) return null;

        ReentrantLock lock = sessionLocks.computeIfAbsent(sessionId, k -> new ReentrantLock());
        if (!lock.tryLock()) {
            return new ChatMessage("assistant", "A message is already being processed for this session. Please wait.");
        }
        try {
            session.getMessages().add(new ChatMessage("user", userMessage));

            String prompt = buildPrompt(session, userMessage, pageContext);
            String response = processRunner.run(prompt, null, CLAUDE_TIMEOUT_SECONDS);

            if (response == null || response.isBlank()) {
                response = "I'm sorry, I couldn't generate a response. Please try again.";
            }

            ChatMessage assistantMsg = new ChatMessage("assistant", response);
            session.getMessages().add(assistantMsg);
            session.setUpdatedAt(Instant.now());
            saveSession(session);

            return assistantMsg;
        } finally {
            lock.unlock();
        }
    }

    public String generateMarkdownReport(String sessionId) {
        ChatSession session = getSession(sessionId);
        if (session == null) return null;

        StringBuilder md = new StringBuilder();
        md.append("# Chat Session: ").append(session.getName()).append("\n\n");
        md.append("**Scope:** ").append(session.getScope()).append("  \n");
        md.append("**Created:** ").append(session.getCreatedAt()).append("  \n");
        md.append("**Messages:** ").append(session.getMessages().size()).append("\n\n");
        md.append("---\n\n");

        for (ChatMessage msg : session.getMessages()) {
            if ("user".equals(msg.getRole())) {
                md.append("## User\n\n").append(msg.getContent()).append("\n\n");
            } else {
                md.append("## Assistant\n\n").append(msg.getContent()).append("\n\n");
            }
            md.append("---\n\n");
        }

        return md.toString();
    }

    public void setJarContextProvider(AnalysisContextProvider provider) {
        this.jarContextProvider = provider;
    }

    public void setPlsqlContextProvider(AnalysisContextProvider provider) {
        this.plsqlContextProvider = provider;
    }

    private static final int MAX_PAGE_CONTEXT_CHARS = 8000;

    private String buildPrompt(ChatSession session, String latestMessage, String pageContext) {
        StringBuilder prompt = new StringBuilder();

        prompt.append("You are an AI assistant for a code analysis platform. ");

        String contextSummary = getContextSummary(session);
        if (contextSummary != null && !contextSummary.isBlank()) {
            prompt.append("\n\nHere is context about the current analysis:\n");
            prompt.append(contextSummary);
        }

        if (pageContext != null && !pageContext.isBlank()) {
            String trimmed = pageContext.length() > MAX_PAGE_CONTEXT_CHARS
                    ? pageContext.substring(0, MAX_PAGE_CONTEXT_CHARS) + "\n...(truncated)"
                    : pageContext;
            prompt.append("\n\nThe user is currently viewing this page context:\n");
            prompt.append(trimmed);
        }

        List<ChatMessage> messages = session.getMessages();
        int start = Math.max(0, messages.size() - MAX_CONTEXT_MESSAGES);
        if (start > 0) {
            prompt.append("\n\n[Earlier conversation messages omitted for brevity]\n");
        }
        for (int i = start; i < messages.size(); i++) {
            ChatMessage m = messages.get(i);
            prompt.append("\n\n").append("user".equals(m.getRole()) ? "Human" : "Assistant")
                    .append(": ").append(m.getContent());
        }

        return prompt.toString();
    }

    private String getContextSummary(ChatSession session) {
        String scope = session.getScope();
        Map<String, String> ctx = session.getScopeContext();

        if ("PLSQL".equalsIgnoreCase(scope) && plsqlContextProvider != null) {
            String analysisName = ctx != null ? ctx.get("analysisName") : null;
            return plsqlContextProvider.getContextSummary(analysisName);
        }
        if ("JAR".equalsIgnoreCase(scope) && jarContextProvider != null) {
            String jarName = ctx != null ? ctx.get("jarName") : null;
            return jarContextProvider.getContextSummary(jarName);
        }
        if ("GLOBAL".equalsIgnoreCase(scope)) {
            StringBuilder global = new StringBuilder();
            if (plsqlContextProvider != null) {
                String plsqlCtx = plsqlContextProvider.getContextSummary(null);
                if (plsqlCtx != null) global.append("## PL/SQL Analysis\n").append(plsqlCtx).append("\n\n");
            }
            if (jarContextProvider != null) {
                String jarCtx = jarContextProvider.getContextSummary(null);
                if (jarCtx != null) global.append("## JAR Analysis\n").append(jarCtx).append("\n\n");
            }
            return global.toString();
        }
        return null;
    }

    private void saveSession(ChatSession session) {
        String type = session.getChatType() != null ? session.getChatType() : "classic";
        Path dir = chatDataDir.resolve(type);
        try {
            Files.createDirectories(dir);
            mapper.writerWithDefaultPrettyPrinter()
                    .writeValue(dir.resolve(session.getId() + ".json").toFile(), session);
        } catch (IOException e) {
            log.error("Failed to save session {}: {}", session.getId(), e.getMessage());
        }
    }

    @FunctionalInterface
    public interface AnalysisContextProvider {
        String getContextSummary(String identifier);
    }
}
