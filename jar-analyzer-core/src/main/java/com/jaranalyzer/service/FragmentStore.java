package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
class FragmentStore {

    private static final Logger log = LoggerFactory.getLogger(FragmentStore.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final JarDataPaths jarDataPaths;

    FragmentStore(JarDataPaths jarDataPaths) {
        this.jarDataPaths = jarDataPaths;
    }

    // ─── Fragment Index ──────────────────────────────────────────────────────

    static class FragmentIndex {
        private final Map<String, Map<String, Object>> entries = new ConcurrentHashMap<>();
        private final AtomicInteger counter = new AtomicInteger(0);

        String nextEndpointId() {
            return String.format("E%03d", counter.incrementAndGet());
        }

        String chunkId(String endpointTraceId, int chunkIndex) {
            return endpointTraceId + "_C" + String.format("%03d", chunkIndex);
        }

        void register(String traceId, String fileName, Map<String, Object> details) {
            Map<String, Object> entry = new LinkedHashMap<>(details);
            entry.put("file", fileName);
            entry.put("traceId", traceId);
            entries.put(traceId, entry);
        }

        Map<String, Map<String, Object>> getEntries() {
            return new LinkedHashMap<>(entries);
        }
    }

    FragmentIndex createIndex() {
        return new FragmentIndex();
    }

    void writeIndex(Path workDir, FragmentIndex index) {
        try {
            Map<String, Object> indexData = new LinkedHashMap<>();
            indexData.put("generatedAt", LocalDateTime.now().toString());
            indexData.put("totalFragments", index.getEntries().size());
            indexData.put("fragments", index.getEntries());
            Files.writeString(workDir.resolve("_index.json"),
                    objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(indexData),
                    StandardCharsets.UTF_8);
        } catch (IOException e) {
            log.debug("Failed to write index: {}", e.getMessage());
        }
    }

    void writeFragment(Path dir, String fileName, String content) throws IOException {
        Files.writeString(dir.resolve(fileName), content, StandardCharsets.UTF_8);
    }

    void writeFragmentSafe(Path dir, String fileName, String content) {
        try {
            writeFragment(dir, fileName, content != null ? content : "unknown error");
        } catch (IOException e) {
            log.warn("Failed to write fragment {}: {}", fileName, e.getMessage());
        }
    }

    void writeMetaSafe(Path dir, String jarName, int total, int done, String mode) {
        try {
            Map<String, Object> meta = new LinkedHashMap<>();
            meta.put("jarName", jarName);
            meta.put("mode", mode);
            meta.put("totalEndpoints", total);
            meta.put("processedEndpoints", done);
            meta.put("timestamp", LocalDateTime.now().toString());
            writeFragment(dir, "_meta.json",
                    objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(meta));
        } catch (IOException e) {
            log.debug("Failed to write meta: {}", e.getMessage());
        }
    }

    public String sanitizeFragment(String name) {
        return JarNameUtil.sanitize(name);
    }

    public Path getWorkDir(String jarName) {
        return jarDataPaths.claudeDir(jarName);
    }

    public List<Map<String, Object>> listFragments(String jarName) {
        Path workDir = getWorkDir(jarName);
        List<Map<String, Object>> fragments = new ArrayList<>();
        if (!Files.isDirectory(workDir)) return fragments;

        try (var stream = Files.list(workDir)) {
            stream.filter(p -> p.toString().endsWith(".json") || p.toString().endsWith(".txt"))
                    .sorted()
                    .forEach(p -> {
                        Map<String, Object> info = new LinkedHashMap<>();
                        info.put("name", p.getFileName().toString());
                        try {
                            info.put("size", Files.size(p));
                        } catch (IOException e) {
                            info.put("size", -1);
                        }
                        fragments.add(info);
                    });
        } catch (IOException e) {
            log.debug("Failed to list fragments for {}: {}", jarName, e.getMessage());
        }
        return fragments;
    }
}
