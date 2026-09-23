package com.jaranalyzer.crossref;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.crossref.model.CrossRefResult;
import com.jaranalyzer.service.JarDataPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Artifact I/O for cross-reference run data, through {@link ArtifactStore}.
 *
 * Layout:
 *   jar/crossref/{jarId}/{crossRefId}/
 *       meta.json    — request params + timestamp (small, always readable)
 *       result.json  — full CrossRefResult JSON
 *       report.md    — human-readable Markdown comparison report
 *
 * This class is completely independent of FlowRunStore — no shared state, no inheritance.
 */
@Component
public class CrossRefStore {

    private static final Logger log = LoggerFactory.getLogger(CrossRefStore.class);

    private final ArtifactKey baseCrossRefDir;  // jar/crossref/
    private final ArtifactStore store;
    private final ObjectMapper objectMapper;

    public CrossRefStore(JarDataPaths jarDataPaths, @Qualifier("jarArtifactStore") ArtifactStore store,
                         ObjectMapper objectMapper) {
        // keyForPath is the sanctioned bridge from the JAR subtree's Path root to its key root,
        // so this stays correct if the data root moves. Rebuilding it from a String would pin the
        // location to the local filesystem.
        this.baseCrossRefDir = jarDataPaths.keyForPath(jarDataPaths.getBaseDir()).resolve("crossref");
        this.store = java.util.Objects.requireNonNull(store, "ArtifactStore is required");
        this.objectMapper = objectMapper;
    }

    // ── Location helpers ──────────────────────────────────────────────────────

    /**
     * The run "directory". The store creates parents implicitly on write and object storage has
     * no directories, so there is nothing to create — the key is enough.
     */
    public ArtifactKey createRunDir(String jarId, String crossRefId) throws IOException {
        return runDir(jarId, crossRefId);
    }

    public ArtifactKey runDir(String jarId, String crossRefId) {
        return baseCrossRefDir.resolve(safe(jarId)).resolve(safe(crossRefId));
    }

    public ArtifactKey jarCrossRefDir(String jarId) {
        return baseCrossRefDir.resolve(safe(jarId));
    }

    // ── Store plumbing ────────────────────────────────────────────────────────

    private void writeBytes(ArtifactKey key, byte[] body) throws IOException {
        try (OutputStream out = store.write(key, body.length)) {
            out.write(body);
        }
    }

    private byte[] readBytes(ArtifactKey key) throws IOException {
        try (InputStream in = store.read(key)) {
            return in.readAllBytes();
        }
    }

    // ── meta.json ─────────────────────────────────────────────────────────────

    public void writeMeta(ArtifactKey runDir, String crossRefId, String jarId,
                          String endpointPath, String procName, String schema, String mode) {
        try {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("crossRefId", crossRefId);
            m.put("jarId", jarId);
            m.put("endpointPath", endpointPath);
            m.put("procName", procName);
            m.put("schema", schema);
            m.put("mode", mode);
            m.put("timestamp", System.currentTimeMillis());
            writeBytes(metaFile(runDir), objectMapper.writeValueAsBytes(m));
        } catch (Exception e) {
            log.warn("CrossRefStore: failed to write meta.json for {}: {}", crossRefId, e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> readMeta(ArtifactKey runDir) {
        ArtifactKey f = metaFile(runDir);
        if (!store.exists(f)) return null;
        try (InputStream in = store.read(f)) {
            return objectMapper.readValue(in, Map.class);
        } catch (Exception e) {
            log.debug("CrossRefStore: cannot read meta.json at {}: {}", f, e.getMessage());
            return null;
        }
    }

    // ── result.json ───────────────────────────────────────────────────────────

    public void writeResult(ArtifactKey runDir, CrossRefResult result) {
        try {
            writeBytes(resultFile(runDir), objectMapper.writeValueAsBytes(result));
        } catch (IOException e) {
            log.warn("CrossRefStore: failed to write result.json: {}", e.getMessage());
        }
    }

    public CrossRefResult readResult(ArtifactKey runDir) {
        ArtifactKey f = resultFile(runDir);
        if (!store.exists(f)) return null;
        try (InputStream in = store.read(f)) {
            return objectMapper.readValue(in, CrossRefResult.class);
        } catch (Exception e) {
            log.warn("CrossRefStore: cannot parse result.json at {}: {}", f, e.getMessage());
            return null;
        }
    }

    public String readResultRaw(ArtifactKey runDir) {
        ArtifactKey f = resultFile(runDir);
        if (!store.exists(f)) return null;
        try {
            return new String(readBytes(f), StandardCharsets.UTF_8);
        } catch (IOException e) {
            log.warn("CrossRefStore: cannot read result.json: {}", e.getMessage());
            return null;
        }
    }

    // ── report.md ─────────────────────────────────────────────────────────────

    public void writeReport(ArtifactKey runDir, String markdown) {
        try {
            writeBytes(reportFile(runDir), markdown.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            log.warn("CrossRefStore: failed to write report.md: {}", e.getMessage());
        }
    }

    public byte[] readReport(ArtifactKey runDir) {
        ArtifactKey f = reportFile(runDir);
        if (!store.exists(f)) return null;
        try {
            return readBytes(f);
        } catch (IOException e) {
            log.warn("CrossRefStore: cannot read report.md: {}", e.getMessage());
            return null;
        }
    }

    // ── History scan ──────────────────────────────────────────────────────────

    /**
     * Lists all cross-ref runs for a given jar, newest first.
     * Each entry is the meta.json content plus a "crossRefId" field.
     */
    public List<Map<String, Object>> listHistory(String jarId) {
        ArtifactKey dir = jarCrossRefDir(jarId);
        List<Map<String, Object>> entries = new ArrayList<>();
        try (var stream = store.list(dir, false)) {
            stream.filter(ArtifactEntry::directory)
                  .map(ArtifactEntry::key)
                  .forEach(runDir -> {
                      Map<String, Object> m = readMeta(runDir);
                      if (m != null) {
                          m.computeIfAbsent("crossRefId", k -> runDir.leaf());
                          entries.add(m);
                      }
                  });
        } catch (IOException e) {
            log.debug("CrossRefStore: cannot list cross-ref dirs for jar {}: {}", jarId, e.getMessage());
        }
        entries.sort((a, b) -> {
            long ta = toLong(a.get("timestamp"));
            long tb = toLong(b.get("timestamp"));
            return Long.compare(tb, ta);
        });
        return entries;
    }

    // ── Delete ────────────────────────────────────────────────────────────────

    public boolean delete(String jarId, String crossRefId) {
        try {
            return store.deletePrefix(runDir(jarId, crossRefId)) > 0;
        } catch (IOException e) {
            log.warn("CrossRefStore: failed to delete run {}/{}: {}", jarId, crossRefId, e.getMessage());
            return false;
        }
    }

    // ── Internal file path helpers ────────────────────────────────────────────

    public ArtifactKey metaFile(ArtifactKey runDir)   { return runDir.resolve("meta.json"); }
    public ArtifactKey resultFile(ArtifactKey runDir) { return runDir.resolve("result.json"); }
    public ArtifactKey reportFile(ArtifactKey runDir) { return runDir.resolve("report.md"); }

    // ── Utilities ─────────────────────────────────────────────────────────────

    private static String safe(String s) {
        return s == null ? "_" : s.replaceAll("[^a-zA-Z0-9._\\-]", "_");
    }

    private static long toLong(Object v) {
        if (v instanceof Number n) return n.longValue();
        if (v instanceof String s) {
            try { return Long.parseLong(s); } catch (NumberFormatException ignored) {}
        }
        return 0L;
    }
}
