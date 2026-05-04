package com.analyzer.chat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ChatSession {
    private String id;
    private String name;
    private String scope; // JAR, PLSQL, GLOBAL
    private String chatType; // "classic" or "new"
    private Map<String, String> scopeContext;
    private String claudeSessionId;
    private Instant createdAt;
    private Instant updatedAt;
    private List<ChatMessage> messages = new ArrayList<>();

    public ChatSession() {}

    public ChatSession(String name, String scope, String chatType, Map<String, String> scopeContext) {
        this.id = UUID.randomUUID().toString();
        this.name = name;
        this.scope = scope;
        this.chatType = chatType != null ? chatType : "classic";
        this.scopeContext = scopeContext;
        this.claudeSessionId = UUID.randomUUID().toString();
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getScope() { return scope; }
    public void setScope(String scope) { this.scope = scope; }

    public String getChatType() { return chatType; }
    public void setChatType(String chatType) { this.chatType = chatType; }

    public Map<String, String> getScopeContext() { return scopeContext; }
    public void setScopeContext(Map<String, String> scopeContext) { this.scopeContext = scopeContext; }

    public String getClaudeSessionId() { return claudeSessionId; }
    public void setClaudeSessionId(String claudeSessionId) { this.claudeSessionId = claudeSessionId; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }

    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }

    public List<ChatMessage> getMessages() { return messages; }
    public void setMessages(List<ChatMessage> messages) { this.messages = messages; }
}
