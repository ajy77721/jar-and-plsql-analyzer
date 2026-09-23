package com.plsqlanalyzer.web.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.plsqlanalyzer.web.parser.service.AnalysisPaths;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Read-only access to persisted chunk fragments/index files under the claude data dir.
 * Extracted from ClaudeVerificationService to keep files focused; logic unchanged.
 */
class ClaudeChunkReader {

    private static final Logger log = LoggerFactory.getLogger(ClaudeChunkReader.class);

    private final ObjectMapper mapper;
    private final ArtifactStore store;

    ClaudeChunkReader(ObjectMapper mapper, ArtifactStore store) {
        this.mapper = mapper;
        this.store = java.util.Objects.requireNonNull(store, "ArtifactStore is required");
    }

    /** {@code plsql/<name>/claude-legacy/} — must match ClaudeVerificationRunner's writer path
     *  (see that class's own comment: this "legacy" web.service stack used to share the exact
     *  same "claude" subfolder as the unrelated web.parser.service.ClaudeVerificationService,
     *  which could silently clobber that stack's chunk fragments — renamed here to match). */
    private ArtifactKey claudeDir(String analysisName) {
        return AnalysisPaths.claudeFileKey(analysisName, "claude-legacy");
    }

    private String readString(ArtifactKey key) throws IOException {
        try (InputStream in = store.read(key)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /** Every artifact in one chunk folder, in a single round trip. */
    private Map<ArtifactKey, ArtifactEntry> stat(ArtifactKey dir) {
        try {
            return store.statPrefix(dir);
        } catch (IOException e) {
            return Map.of();
        }
    }

    Map<String, Object> getChunkFragment(String analysisName, String chunkId) {
        ArtifactKey dir = claudeDir(analysisName);
        Map<String, Object> fragment = new LinkedHashMap<>();
        fragment.put("chunkId", chunkId);

        // ONE listing covers all three fragment probes below.
        Map<ArtifactKey, ArtifactEntry> stat = stat(dir);

        // Load input
        ArtifactKey inputFile = dir.resolve(chunkId + "_input.json");
        if (stat.containsKey(inputFile)) {
            try (InputStream in = store.read(inputFile)) {
                fragment.put("input", mapper.readValue(in, Object.class));
            } catch (IOException e) {
                fragment.put("input", "Error reading: " + e.getMessage());
            }
        }

        // Load output
        ArtifactKey outputFile = dir.resolve(chunkId + "_output.json");
        if (stat.containsKey(outputFile)) {
            try {
                String raw = readString(outputFile);
                // Try to parse as JSON first
                try {
                    fragment.put("output", mapper.readValue(raw, Object.class));
                } catch (Exception e) {
                    fragment.put("output", raw); // raw text
                }
            } catch (IOException e) {
                fragment.put("output", "Error reading: " + e.getMessage());
            }
        }

        // Load error if exists
        ArtifactKey errorFile = dir.resolve(chunkId + "_error.txt");
        if (stat.containsKey(errorFile)) {
            try {
                fragment.put("error", readString(errorFile));
            } catch (IOException e) { log.debug("IO operation failed: {}", e.getMessage()); }
        }

        return fragment;
    }

    Map<String, List<String>> getTableChunkMapping(String analysisName) {
        ArtifactKey dir = claudeDir(analysisName);
        Map<String, List<String>> mapping = new LinkedHashMap<>();

        // ONE listing rather than an exists() per chunk.
        Map<ArtifactKey, ArtifactEntry> stat = stat(dir);
        List<String> chunkIds = listChunks(analysisName);
        for (String chunkId : chunkIds) {
            ArtifactKey outputFile = dir.resolve(chunkId + "_output.json");
            if (!stat.containsKey(outputFile)) continue;
            try {
                String raw = readString(outputFile);
                // Try to parse as JSON to extract table names
                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> output = mapper.readValue(raw, LinkedHashMap.class);
                    Object tablesObj = output.get("tables");
                    if (tablesObj instanceof List<?> tablesList) {
                        for (Object item : tablesList) {
                            if (item instanceof Map<?, ?> tableMap) {
                                Object nameObj = tableMap.get("tableName");
                                if (nameObj != null) {
                                    String name = nameObj.toString().toUpperCase();
                                    mapping.computeIfAbsent(name, k -> new ArrayList<>()).add(chunkId);
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    // Not JSON — scan raw text for table names (best effort)
                    // Look for patterns like "tableName": "XXX" in the raw text
                    java.util.regex.Pattern pat = java.util.regex.Pattern.compile("\"tableName\"\\s*:\\s*\"([^\"]+)\"");
                    java.util.regex.Matcher m = pat.matcher(raw);
                    while (m.find()) {
                        String name = m.group(1).toUpperCase();
                        mapping.computeIfAbsent(name, k -> new ArrayList<>()).add(chunkId);
                    }
                }
            } catch (IOException e) { log.debug("IO operation failed: {}", e.getMessage()); }
        }
        return mapping;
    }

    List<String> listChunks(String analysisName) {
        ArtifactKey dir = claudeDir(analysisName);
        ArtifactKey indexFile = dir.resolve("_index.json");
        if (!store.exists(indexFile)) return Collections.emptyList();
        try (InputStream in = store.read(indexFile)) {
            @SuppressWarnings("unchecked")
            Map<String, Object> index = mapper.readValue(in, LinkedHashMap.class);
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> chunks = (List<Map<String, Object>>) index.get("chunks");
            if (chunks == null) return Collections.emptyList();
            return chunks.stream().map(c -> (String) c.get("id")).collect(Collectors.toList());
        } catch (IOException e) {
            return Collections.emptyList();
        }
    }

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> listChunkSummaries(String analysisName) {
        ArtifactKey dir = claudeDir(analysisName);
        ArtifactKey indexFile = dir.resolve("_index.json");
        // ONE listing covers the index plus the two status probes per chunk.
        Map<ArtifactKey, ArtifactEntry> stat = stat(dir);
        if (!stat.containsKey(indexFile)) return Collections.emptyList();
        try {
            Map<String, Object> index;
            try (InputStream in = store.read(indexFile)) {
                index = mapper.readValue(in, LinkedHashMap.class);
            }
            List<Map<String, Object>> chunks = (List<Map<String, Object>>) index.get("chunks");
            if (chunks == null) return Collections.emptyList();

            List<Map<String, Object>> result = new ArrayList<>();
            for (Map<String, Object> chunk : chunks) {
                String id = (String) chunk.get("id");
                Map<String, Object> summary = new LinkedHashMap<>();
                summary.put("id", id);
                summary.put("name", chunk.get("name"));
                summary.put("procedures", chunk.get("procedures"));
                summary.put("tableCount", chunk.get("tableCount"));

                ArtifactKey outputFile = dir.resolve(id + "_output.json");
                ArtifactKey errorFile = dir.resolve(id + "_error.txt");
                if (stat.containsKey(outputFile)) {
                    summary.put("status", "COMPLETE");
                } else if (stat.containsKey(errorFile)) {
                    summary.put("status", "ERROR");
                    try { summary.put("error", readString(errorFile).trim()); } catch (IOException ignored) {}
                } else {
                    summary.put("status", "PENDING");
                }
                result.add(summary);
            }
            return result;
        } catch (IOException e) {
            return Collections.emptyList();
        }
    }
}
