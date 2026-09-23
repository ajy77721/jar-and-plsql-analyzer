package com.plsqlanalyzer.web.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.plsqlanalyzer.web.service.ClaudeVerificationService.OperationVerification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Parses Claude CLI responses into ChunkResult objects and reloads persisted chunk
 * outputs. Extracted from ClaudeVerificationService to keep files focused; logic unchanged.
 */
class ClaudeResponseParser {

    private static final Logger log = LoggerFactory.getLogger(ClaudeResponseParser.class);

    private final ObjectMapper mapper;

    ClaudeResponseParser(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    ChunkResult parseResponse(VerificationChunk chunk, String response) {
        ChunkResult cr = new ChunkResult();
        cr.chunkId = chunk.id;
        cr.procedureIds = chunk.procedureIds;
        cr.tableVerifications = new ArrayList<>();

        // Extract JSON from response (might be wrapped in markdown code blocks)
        String json = extractJson(response);
        if (json == null) {
            cr.error = "Could not extract JSON from Claude response";
            log.warn("Chunk {}: no JSON in response (first 200 chars: {})",
                    chunk.id, response.substring(0, Math.min(200, response.length())));
            return cr;
        }

        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> parsed = mapper.readValue(json, LinkedHashMap.class);

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> tables = (List<Map<String, Object>>) parsed.get("tables");
            if (tables != null) {
                for (Map<String, Object> table : tables) {
                    TableVerification tv = new TableVerification();
                    tv.tableName = (String) table.get("tableName");
                    tv.operations = new ArrayList<>();

                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> ops = (List<Map<String, Object>>) table.get("operations");
                    if (ops != null) {
                        for (Map<String, Object> op : ops) {
                            OperationVerification ov = new OperationVerification();
                            ov.operation = (String) op.get("operation");
                            ov.status = (String) op.get("status");
                            ov.procedureName = (String) op.get("procedureName");
                            ov.lineNumber = op.get("lineNumber") instanceof Number n ? n.intValue() : 0;
                            ov.reason = (String) op.get("reason");
                            tv.operations.add(ov);
                        }
                    }
                    cr.tableVerifications.add(tv);
                }
            }

            cr.summary = (String) parsed.get("summary");
        } catch (Exception e) {
            cr.error = "Failed to parse JSON: " + e.getMessage();
            log.warn("Chunk {}: JSON parse error: {}", chunk.id, e.getMessage());
        }

        return cr;
    }

    private String extractJson(String response) {
        if (response == null) return null;
        String trimmed = response.trim();

        // Try: response is already valid JSON
        if (trimmed.startsWith("{")) return trimmed;

        // Try: extract from ```json ... ``` code block
        int jsonStart = trimmed.indexOf("```json");
        if (jsonStart >= 0) {
            int codeStart = trimmed.indexOf('\n', jsonStart);
            int codeEnd = trimmed.indexOf("```", codeStart + 1);
            if (codeStart >= 0 && codeEnd > codeStart) {
                return trimmed.substring(codeStart + 1, codeEnd).trim();
            }
        }

        // Try: extract from ``` ... ```
        int btStart = trimmed.indexOf("```");
        if (btStart >= 0) {
            int start = trimmed.indexOf('\n', btStart);
            int end = trimmed.indexOf("```", start + 1);
            if (start >= 0 && end > start) {
                String content = trimmed.substring(start + 1, end).trim();
                if (content.startsWith("{")) return content;
            }
        }

        // Try: find first { and last }
        int first = trimmed.indexOf('{');
        int last = trimmed.lastIndexOf('}');
        if (first >= 0 && last > first) {
            return trimmed.substring(first, last + 1);
        }

        return null;
    }

    ChunkResult loadChunkResult(ArtifactStore store, ArtifactKey dir, String chunkId) {
        ArtifactKey outputFile = dir.resolve(chunkId + "_output.json");
        if (!store.exists(outputFile)) return null;
        try {
            String response;
            try (InputStream in = store.read(outputFile)) {
                response = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
            VerificationChunk fakeChunk = new VerificationChunk();
            fakeChunk.id = chunkId;
            // Recover procedureIds from saved input for disambiguation
            ArtifactKey inputFile = dir.resolve(chunkId + "_input.json");
            if (store.exists(inputFile)) {
                try (InputStream in = store.read(inputFile)) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> input = mapper.readValue(in, LinkedHashMap.class);
                    Object procs = input.get("procedures");
                    if (procs instanceof List<?> procList) {
                        fakeChunk.procedureIds = new ArrayList<>();
                        for (Object p : procList) fakeChunk.procedureIds.add(p.toString());
                    }
                } catch (IOException e) {
                    log.debug("Could not load chunk input for {}: {}", chunkId, e.getMessage());
                }
            }
            return parseResponse(fakeChunk, response);
        } catch (IOException e) {
            return null;
        }
    }
}
