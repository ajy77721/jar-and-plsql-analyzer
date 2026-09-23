package com.jaranalyzer.service;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

@Service
public class ChatHistoryService {

    private static final Logger log = LoggerFactory.getLogger(ChatHistoryService.class);

    private static final long MAX_HISTORY_BYTES = 10L * 1024 * 1024;
    private static final TypeReference<Map<String, String>> MSG_TYPE = new TypeReference<>() {};

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final JarDataPaths jarDataPaths;
    private final ArtifactStore store;

    public ChatHistoryService(JarDataPaths jarDataPaths, @Qualifier("jarArtifactStore") ArtifactStore store) {
        this.jarDataPaths = jarDataPaths;
        this.store = store;
    }

    /**
     * Appends one message.
     *
     * <p>Object storage has no append, so this is a read-modify-write of the whole history. That
     * is affordable only because the history is hard-capped at {@link #MAX_HISTORY_BYTES} (10 MB)
     * — the same cap that previously drove {@code purgeIfNeeded}, now applied on the way out
     * instead of as a separate rewrite pass.
     */
    public synchronized void append(String jarKey, String role, String content) throws IOException {
        ArtifactKey key = historyKey(jarKey);

        Map<String, String> entry = new LinkedHashMap<>();
        entry.put("role", role);
        entry.put("content", content);
        entry.put("ts", Instant.now().toString());

        List<byte[]> lines = readLines(key);
        lines.add((objectMapper.writeValueAsString(entry) + "\n").getBytes(StandardCharsets.UTF_8));

        long total = 0;
        for (byte[] b : lines) total += b.length;
        int dropped = 0;
        while (total > MAX_HISTORY_BYTES && lines.size() > 1) {
            total -= lines.remove(0).length;
            dropped++;
        }
        if (dropped > 0) {
            log.info("[ChatHistory] Purged {} line(s) from {}; remaining {} bytes ({} messages)",
                    dropped, key, total, lines.size());
        }

        try (OutputStream os = new BufferedOutputStream(store.write(key, total), 64 * 1024)) {
            for (byte[] b : lines) os.write(b);
        }
    }

    public synchronized List<Map<String, String>> load(String jarKey) throws IOException {
        ArtifactKey key = historyKey(jarKey);
        if (!store.exists(key)) return Collections.emptyList();

        List<Map<String, String>> result = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(store.read(key), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.strip();
                if (line.isEmpty()) continue;
                try {
                    Map<String, String> msg = objectMapper.readValue(line, MSG_TYPE);
                    Map<String, String> clean = new LinkedHashMap<>();
                    clean.put("role", msg.getOrDefault("role", "user"));
                    clean.put("content", msg.getOrDefault("content", ""));
                    result.add(clean);
                } catch (Exception e) {
                    log.warn("Skipping malformed history line: {}", e.getMessage());
                }
            }
        }
        return result;
    }

    public synchronized void clear(String jarKey) throws IOException {
        store.delete(historyKey(jarKey));
        log.info("[ChatHistory] Cleared history for {}", jarKey);
    }

    public long sizeBytes(String jarKey) {
        long size = store.size(historyKey(jarKey));
        return size < 0 ? 0L : size;
    }

    /** Existing history as raw lines (each keeping its trailing newline); empty when absent. */
    private List<byte[]> readLines(ArtifactKey key) throws IOException {
        List<byte[]> lines = new ArrayList<>();
        if (!store.exists(key)) return lines;
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(store.read(key), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add((line + "\n").getBytes(StandardCharsets.UTF_8));
            }
        }
        return lines;
    }

    private ArtifactKey historyKey(String jarKey) {
        return jarDataPaths.chatHistoryKey(jarKey);
    }
}
