package com.jaranalyzer.controller;

import com.jaranalyzer.service.ChatboxService;
import com.jaranalyzer.service.ChatHistoryService;
import com.jaranalyzer.service.JarNameUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/jars")
public class ChatboxController {

    private static final Logger log = LoggerFactory.getLogger(ChatboxController.class);

    private final ChatboxService chatboxService;
    private final ChatHistoryService historyService;

    public ChatboxController(ChatboxService chatboxService, ChatHistoryService historyService) {
        this.chatboxService = chatboxService;
        this.historyService = historyService;
    }

    @PostMapping("/{id}/chat")
    public Map<String, Object> chat(
            @PathVariable("id") String jarId,
            @RequestBody Map<String, String> body) {

        String message = body.get("message");
        if (message == null || message.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "'message' must not be empty");
        }

        try {
            String reply = chatboxService.chat(jarId, message);
            String jarKey = JarNameUtil.normalizeKey(jarId);
            long historyBytes = historyService.sizeBytes(jarKey);
            List<Map<String, String>> history = historyService.load(jarKey);

            return Map.of(
                    "reply", reply,
                    "historyBytes", historyBytes,
                    "messageCount", history.size()
            );
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, e.getMessage());
        } catch (Exception e) {
            log.error("Chatbox error for JAR {}: {}", jarId, e.getMessage(), e);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
                    "Claude returned an error: " + e.getMessage());
        }
    }

    @GetMapping("/{id}/chat/history")
    public Map<String, Object> getHistory(@PathVariable("id") String jarId) {
        try {
            String jarKey = JarNameUtil.normalizeKey(jarId);
            List<Map<String, String>> messages = historyService.load(jarKey);
            long bytes = historyService.sizeBytes(jarKey);
            return Map.of(
                    "messages", messages,
                    "messageCount", messages.size(),
                    "historyBytes", bytes
            );
        } catch (Exception e) {
            log.error("Failed to load chatbox history for {}: {}", jarId, e.getMessage());
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to load history");
        }
    }

    @DeleteMapping("/{id}/chat/history")
    public Map<String, String> clearHistory(@PathVariable("id") String jarId) {
        try {
            chatboxService.clearHistory(jarId);
            return Map.of("status", "cleared", "jarId", jarId);
        } catch (Exception e) {
            log.error("Failed to clear chatbox history for {}: {}", jarId, e.getMessage());
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to clear history");
        }
    }
}
