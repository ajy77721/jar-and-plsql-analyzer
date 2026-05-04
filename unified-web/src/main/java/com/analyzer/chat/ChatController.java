package com.analyzer.chat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/chat")
public class ChatController {

    private static final Logger log = LoggerFactory.getLogger(ChatController.class);

    private final ChatService chatService;

    @org.springframework.beans.factory.annotation.Value("${chat.classic.enabled:true}")
    private boolean classicEnabled;

    @org.springframework.beans.factory.annotation.Value("${chat.chatbox.enabled:true}")
    private boolean chatboxEnabled;

    public ChatController(ChatService chatService) {
        this.chatService = chatService;
    }

    @GetMapping("/config")
    public Map<String, Object> getChatConfig() {
        return Map.of(
                "classicEnabled", classicEnabled,
                "chatboxEnabled", chatboxEnabled
        );
    }

    @PostMapping("/sessions")
    public ResponseEntity<?> createSession(@RequestBody Map<String, Object> body) {
        String name = (String) body.get("name");
        String scope = (String) body.getOrDefault("scope", "GLOBAL");
        String chatType = (String) body.getOrDefault("chatType", "classic");
        @SuppressWarnings("unchecked")
        Map<String, String> scopeContext = (Map<String, String>) body.get("scopeContext");

        if (name == null || name.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Session name is required"));
        }

        ChatSession session = chatService.createSession(name, scope.toUpperCase(), chatType, scopeContext);
        return ResponseEntity.ok(Map.of(
                "id", session.getId(),
                "name", session.getName(),
                "scope", session.getScope(),
                "chatType", session.getChatType(),
                "createdAt", session.getCreatedAt().toString()
        ));
    }

    @GetMapping("/sessions")
    public ResponseEntity<List<Map<String, Object>>> listSessions(
            @RequestParam(required = false) String scope,
            @RequestParam(required = false) String chatType) {
        return ResponseEntity.ok(chatService.listSessions(scope, chatType));
    }

    @GetMapping("/sessions/{id}")
    public ResponseEntity<?> getSession(@PathVariable String id) {
        ChatSession session = chatService.getSession(id);
        if (session == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(session);
    }

    @DeleteMapping("/sessions/{id}")
    public ResponseEntity<?> deleteSession(@PathVariable String id) {
        boolean deleted = chatService.deleteSession(id);
        if (!deleted) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(Map.of("deleted", id));
    }

    @PostMapping("/sessions/{id}/messages")
    public ResponseEntity<?> sendMessage(@PathVariable String id, @RequestBody Map<String, String> body) {
        String message = body.get("message");
        if (message == null || message.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Message is required"));
        }

        String pageContext = body.get("pageContext");
        log.info("Chat message to session {}: {} chars, context: {}", id, message.length(),
                pageContext != null ? pageContext.length() + " chars" : "none");
        ChatMessage response = chatService.sendMessage(id, message, pageContext);
        if (response == null) {
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok(Map.of(
                "role", response.getRole(),
                "content", response.getContent(),
                "timestamp", response.getTimestamp().toString()
        ));
    }

    @GetMapping("/sessions/{id}/report")
    public ResponseEntity<?> downloadReport(@PathVariable String id) {
        String markdown = chatService.generateMarkdownReport(id);
        if (markdown == null) return ResponseEntity.notFound().build();

        ChatSession session = chatService.getSession(id);
        String fileName = (session != null ? session.getName() : id)
                .replaceAll("[^a-zA-Z0-9_-]", "_") + "_report.md";

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + "\"")
                .contentType(MediaType.TEXT_MARKDOWN)
                .body(markdown);
    }

    private static final long MAX_READ_SIZE = 512 * 1024;

    @PostMapping("/files/read")
    public ResponseEntity<?> readFile(@RequestBody Map<String, String> body) {
        String filePath = body.get("path");
        if (filePath == null || filePath.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Path is required"));
        }

        filePath = filePath.replace("\\", "/");
        if (filePath.contains("..")) {
            return ResponseEntity.badRequest().body(Map.of("error", "Path traversal not allowed"));
        }

        Path path = Path.of(filePath);
        if (!Files.exists(path)) {
            return ResponseEntity.ok(Map.of("error", "File not found: " + filePath));
        }
        if (Files.isDirectory(path)) {
            return ResponseEntity.ok(Map.of("error", "Path is a directory, not a file"));
        }

        try {
            long size = Files.size(path);
            if (size > MAX_READ_SIZE) {
                return ResponseEntity.ok(Map.of("error", "File too large (" + (size / 1024) + "KB). Max: " + (MAX_READ_SIZE / 1024) + "KB"));
            }
            String content = Files.readString(path, StandardCharsets.UTF_8);
            log.info("Chat file read: {} ({} bytes)", filePath, size);
            return ResponseEntity.ok(Map.of("content", content, "path", filePath, "size", size));
        } catch (IOException e) {
            log.error("Failed to read file {}: {}", filePath, e.getMessage());
            return ResponseEntity.ok(Map.of("error", "Failed to read: " + e.getMessage()));
        }
    }

    @PostMapping("/files/write")
    public ResponseEntity<?> writeFile(@RequestBody Map<String, String> body) {
        String filePath = body.get("path");
        String content = body.get("content");

        if (filePath == null || filePath.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Path is required"));
        }
        if (content == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "Content is required"));
        }

        filePath = filePath.replace("\\", "/");
        if (filePath.contains("..")) {
            return ResponseEntity.badRequest().body(Map.of("error", "Path traversal not allowed"));
        }

        Path path = Path.of(filePath);
        try {
            Files.createDirectories(path.getParent());
            Files.writeString(path, content, StandardCharsets.UTF_8);
            log.info("Chat file written: {} ({} bytes)", filePath, content.length());
            return ResponseEntity.ok(Map.of("success", true, "path", filePath, "size", content.length()));
        } catch (IOException e) {
            log.error("Failed to write file {}: {}", filePath, e.getMessage());
            return ResponseEntity.ok(Map.of("error", "Failed to write: " + e.getMessage()));
        }
    }
}
