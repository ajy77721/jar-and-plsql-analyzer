package com.jaranalyzer.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;

@Service
public class ChatHistoryService {

    private static final Logger log = LoggerFactory.getLogger(ChatHistoryService.class);

    private static final long MAX_HISTORY_BYTES = 10L * 1024 * 1024;
    private static final TypeReference<Map<String, String>> MSG_TYPE = new TypeReference<>() {};

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final JarDataPaths jarDataPaths;

    public ChatHistoryService(JarDataPaths jarDataPaths) {
        this.jarDataPaths = jarDataPaths;
    }

    public synchronized void append(String jarKey, String role, String content) throws IOException {
        Path file = historyFile(jarKey);
        Files.createDirectories(file.getParent());

        Map<String, String> entry = new LinkedHashMap<>();
        entry.put("role", role);
        entry.put("content", content);
        entry.put("ts", Instant.now().toString());

        String line = objectMapper.writeValueAsString(entry) + "\n";

        try (OutputStream os = Files.newOutputStream(file,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            os.write(line.getBytes(StandardCharsets.UTF_8));
        }

        purgeIfNeeded(file);
    }

    public synchronized List<Map<String, String>> load(String jarKey) throws IOException {
        Path file = historyFile(jarKey);
        if (!Files.exists(file)) return Collections.emptyList();

        List<Map<String, String>> result = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
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
        Path file = historyFile(jarKey);
        Files.deleteIfExists(file);
        log.info("[ChatHistory] Cleared history for {}", jarKey);
    }

    public long sizeBytes(String jarKey) {
        try {
            Path file = historyFile(jarKey);
            return Files.exists(file) ? Files.size(file) : 0L;
        } catch (IOException e) {
            return 0L;
        }
    }

    private void purgeIfNeeded(Path file) throws IOException {
        long size = Files.size(file);
        if (size <= MAX_HISTORY_BYTES) return;

        log.info("[ChatHistory] File {} is {} bytes (>{} MB) — starting incremental purge",
                file.getFileName(), size, MAX_HISTORY_BYTES / (1024 * 1024));

        List<byte[]> lineBytes = new ArrayList<>();
        long totalBytes = 0;
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                byte[] b = (line + "\n").getBytes(StandardCharsets.UTF_8);
                lineBytes.add(b);
                totalBytes += b.length;
            }
        }

        int dropped = 0;
        while (totalBytes > MAX_HISTORY_BYTES && !lineBytes.isEmpty()) {
            totalBytes -= lineBytes.get(0).length;
            lineBytes.remove(0);
            dropped++;
        }

        try (OutputStream os = Files.newOutputStream(file, StandardOpenOption.TRUNCATE_EXISTING)) {
            for (byte[] b : lineBytes) {
                os.write(b);
            }
        }

        log.info("[ChatHistory] Purged {} lines from {}; remaining: {} bytes ({} messages)",
                dropped, file.getFileName(), totalBytes, lineBytes.size());
    }

    private Path historyFile(String jarKey) {
        return jarDataPaths.chatHistoryFile(jarKey);
    }
}
