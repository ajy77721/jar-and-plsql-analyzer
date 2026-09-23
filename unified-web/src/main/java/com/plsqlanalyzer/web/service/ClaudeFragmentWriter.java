package com.plsqlanalyzer.web.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Persists chunk fragments, the chunk index, and the resume checkpoint under the claude
 * data dir. Extracted from ClaudeVerificationService to keep files focused; logic unchanged.
 */
class ClaudeFragmentWriter {

    private static final Logger log = LoggerFactory.getLogger(ClaudeFragmentWriter.class);

    private final ObjectMapper mapper;
    private final ArtifactStore store;

    ClaudeFragmentWriter(ObjectMapper mapper, ArtifactStore store) {
        this.mapper = mapper;
        this.store = java.util.Objects.requireNonNull(store, "ArtifactStore is required");
    }

    void saveFragment(ArtifactKey dir, String filename, Object data) {
        ArtifactKey key = dir.resolve(filename);
        try {
            if (data instanceof String str) {
                byte[] body = str.getBytes(StandardCharsets.UTF_8);
                try (OutputStream out = store.write(key, body.length)) {
                    out.write(body);
                }
            } else {
                try (OutputStream out = store.write(key)) {
                    mapper.writeValue(out, data);
                }
            }
        } catch (IOException e) {
            log.error("Failed to save fragment {}: {}", filename, e.getMessage());
        }
    }

    void saveIndex(ArtifactKey dir, List<VerificationChunk> chunks) {
        Map<String, Object> index = new LinkedHashMap<>();
        List<Map<String, Object>> chunkList = new ArrayList<>();
        for (VerificationChunk chunk : chunks) {
            Map<String, Object> cm = new LinkedHashMap<>();
            cm.put("id", chunk.id);
            cm.put("name", chunk.name);
            cm.put("procedures", chunk.procedureIds);
            cm.put("procedureCount", chunk.procedureIds.size());
            cm.put("tableCount", chunk.tables.size());
            chunkList.add(cm);
        }
        index.put("chunks", chunkList);
        index.put("timestamp", LocalDateTime.now().toString());
        saveFragment(dir, "_index.json", index);
    }

    void saveCheckpoint(ArtifactKey dir, Set<String> completed) {
        saveFragment(dir, "_checkpoint.json", new ArrayList<>(completed));
    }
}
