package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.analyzer.storage.ArtifactKey;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static com.plsqlanalyzer.web.parser.service.StaticAnalysisReader.*;

/** Sequence + dynamic-SQL facet reads plus source-line scanning helpers. */
class SequenceFacetReader {

    private final StaticAnalysisReader r;

    SequenceFacetReader(StaticAnalysisReader r) {
        this.r = r;
    }

    JsonNode getSequences(String name) throws IOException {
        ObjectMapper mapper = r.mapper;
        String key = r.cacheKey(name, "sequences");
        JsonNode cached = r.synthesizedCache.get(key);
        if (cached != null) return cached;

        List<JsonNode> nodes = r.nodeDocuments(name);
        if (nodes.isEmpty()) return mapper.createArrayNode();

        Map<String, ObjectNode> seqMap = new LinkedHashMap<>();
        Map<String, String[]> srcCache = new HashMap<>();  // sourceFile -> source lines
        // Built once, on first need, and only if some node actually carries a sourceFile: one LIST
        // of sources/ replaces every per-source exact→.sql→.prc HEAD probe in this view.
        AtomicReference<Map<String, ArtifactKey>> indexRef = new AtomicReference<>();
        {
            for (JsonNode node : nodes) {
                String nodeId = textOrNull(node, "nodeId");
                String nodeName = textOrNull(node, "name");
                String sourceFile = textOrNull(node, "sourceFile");
                JsonNode sequences = node.get("sequences");
                if (sequences == null || !sequences.isArray()) continue;

                String[] srcLines = loadSourceLines(name, sourceFile, srcCache, indexRef);

                for (JsonNode seq : sequences) {
                    String seqName = textOrNull(seq, "name");
                    if (seqName == null) continue;
                    String seqSchema = textOrNull(seq, "schema");
                    String mapKey = (seqSchema != null ? seqSchema + "." : "") + seqName;

                    ObjectNode entry = seqMap.get(mapKey);
                    if (entry == null) {
                        entry = mapper.createObjectNode();
                        entry.put("sequenceName", seqName);
                        entry.put("sequenceSchema", seqSchema != null ? seqSchema : "");
                        entry.set("operations", mapper.createArrayNode());
                        entry.put("accessCount", 0);
                        entry.set("accessDetails", mapper.createArrayNode());
                        seqMap.put(mapKey, entry);
                    }

                    JsonNode ops = seq.get("operations");
                    if (ops != null && ops.isObject()) {
                        ArrayNode entryOps = (ArrayNode) entry.get("operations");
                        Iterator<String> fieldIter = ops.fieldNames();
                        while (fieldIter.hasNext()) {
                            String opType = fieldIter.next();
                            boolean found = false;
                            for (JsonNode existOp : entryOps) {
                                if (existOp.asText().equals(opType)) { found = true; break; }
                            }
                            if (!found) entryOps.add(opType);

                            // Prefer real line numbers scanned from source (older analyses stored
                            // the body-start line for every usage); fall back to stored lines.
                            List<Integer> scanned = findSequenceLines(srcLines, seqName, opType);
                            List<Integer> useLines = new ArrayList<>();
                            if (!scanned.isEmpty()) {
                                useLines = scanned;
                            } else {
                                JsonNode lines = ops.get(opType);
                                if (lines != null && lines.isArray()) {
                                    for (JsonNode lineNode : lines) useLines.add(lineNode.asInt());
                                }
                            }
                            for (int ln : useLines) {
                                entry.put("accessCount", entry.get("accessCount").asInt() + 1);
                                ObjectNode detail = mapper.createObjectNode();
                                detail.put("operation", opType);
                                detail.put("procedureId", nodeId != null ? nodeId : "");
                                detail.put("procedureName", nodeName != null ? nodeName : "");
                                detail.put("lineNumber", ln);
                                detail.put("sourceFile", sourceFile != null ? sourceFile : "");
                                ((ArrayNode) entry.get("accessDetails")).add(detail);
                            }
                        }
                    }
                }
            }
        }

        ArrayNode result = mapper.createArrayNode();
        seqMap.values().forEach(result::add);
        r.synthesizedCache.put(key, result);
        return result;
    }

    /**
     * Aggregate all dynamic-SQL executions (EXECUTE IMMEDIATE / OPEN FOR / EXEC /
     * sp_executesql / SET @sql=…) across every procedure into a flat report.
     * Dialect-agnostic: both Oracle and SQL Server write node-detail "dynamicSql".
     */
    JsonNode getDynamicSql(String name) throws IOException {
        ObjectMapper mapper = r.mapper;
        String key = r.cacheKey(name, "dynamicSql");
        JsonNode cached = r.synthesizedCache.get(key);
        if (cached != null) return cached;

        ArrayNode result = mapper.createArrayNode();
        for (JsonNode node : r.nodeDocuments(name)) {
            JsonNode dyn = node.get("dynamicSql");
            if (dyn == null || !dyn.isArray()) continue;
            String nodeId = textOrNull(node, "nodeId");
            String nodeName = textOrNull(node, "name");
            String sourceFile = textOrNull(node, "sourceFile");
            String schema = textOrNull(node, "schema");
            for (JsonNode d : dyn) {
                ObjectNode e = mapper.createObjectNode();
                e.put("procedureId", nodeId != null ? nodeId : "");
                e.put("procedureName", nodeName != null ? nodeName : "");
                e.put("schema", schema != null ? schema : "");
                e.put("sourceFile", sourceFile != null ? sourceFile : "");
                e.put("type", textOrDefault(d, "type", "DYNAMIC"));
                e.put("lineNumber", intOrDefault(d, "line", 0));
                e.put("sqlExpression", textOrDefault(d, "sqlExpression", ""));
                JsonNode uv = d.get("usingVariables");
                if (uv != null && uv.isArray()) e.set("usingVariables", uv);
                result.add(e);
            }
        }
        r.synthesizedCache.put(key, result);
        return result;
    }

    /** Load + cache source lines for a file; returns null if unavailable. */
    private String[] loadSourceLines(String name, String sourceFile, Map<String, String[]> cache,
                                     AtomicReference<Map<String, ArtifactKey>> indexRef) {
        if (sourceFile == null || sourceFile.isBlank()) return null;
        if (cache.containsKey(sourceFile)) return cache.get(sourceFile);
        String[] lines = null;
        try {
            Map<String, ArtifactKey> index = indexRef.get();
            if (index == null) {
                index = r.sourceKeyIndex(name);
                indexRef.set(index);
            }
            String src = r.getSourceFromIndex(index, sourceFile);
            if (src != null && !src.isEmpty()) lines = src.split("\n", -1);
        } catch (Exception e) {
            /* source unavailable (e.g. encrypted) — fall back to stored lines */
        }
        cache.put(sourceFile, lines);
        return lines;
    }

    /**
     * Find the 1-based source line(s) where a sequence is used for a given op type.
     * Handles Oracle ({@code seq.NEXTVAL}/{@code seq.CURRVAL}) and SQL Server
     * ({@code NEXT VALUE FOR seq}) syntaxes. Returns empty when nothing is found.
     */
    private List<Integer> findSequenceLines(String[] srcLines, String seqName, String opType) {
        List<Integer> out = new ArrayList<>();
        if (srcLines == null || seqName == null) return out;
        String seq = seqName.toUpperCase();
        boolean currval = "CURRVAL".equalsIgnoreCase(opType);
        for (int i = 0; i < srcLines.length; i++) {
            String u = srcLines[i].toUpperCase();
            if (!u.contains(seq)) continue;
            boolean match = currval
                    ? u.contains(seq + ".CURRVAL")
                    : (u.contains(seq + ".NEXTVAL") || u.contains("NEXT VALUE FOR"));
            if (match) out.add(i + 1);
        }
        return out;
    }
}
