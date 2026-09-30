package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
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
    private final ArtifactStore store;

    FragmentStore(JarDataPaths jarDataPaths, @Qualifier("jarArtifactStore") ArtifactStore store) {
        this.jarDataPaths = jarDataPaths;
        this.store = store;
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

    void writeIndex(ArtifactKey workDir, FragmentIndex index) {
        try {
            Map<String, Object> indexData = new LinkedHashMap<>();
            indexData.put("generatedAt", LocalDateTime.now().toString());
            indexData.put("totalFragments", index.getEntries().size());
            indexData.put("fragments", index.getEntries());
            writeFragment(workDir, "_index.json",
                    objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(indexData));
        } catch (IOException e) {
            log.debug("Failed to write index: {}", e.getMessage());
        }
    }

    /**
     * @param fileName untrusted only insofar as it is built from class and method names; the
     *                 callers sanitize first and {@link ArtifactKey} rejects any separator or
     *                 {@code ".."} outright, which is what the old {@code dir.resolve} did not.
     */
    /**
     * Writes one fragment through the length-aware {@link ArtifactStore#write(ArtifactKey, long)}.
     *
     * <p>The declared length is load-bearing, not an optimisation hint: it is what keeps every
     * small fragment (the per-endpoint {@code endpoint.json} / {@code nodes.json} / {@code
     * node_*.json}, the Claude work fragments) on the write-behind <b>buffered</b> path. With S3
     * write-behind enabled ({@code app.storage.s3.write-behind=true}), a write whose declared length
     * is under {@code WriteBehindStore.ROLLOVER_THRESHOLD} is buffered on the heap and uploaded on
     * the bounded pool, so {@code close()} returns without an S3 round trip and the analysis thread
     * moves on to the next artifact. Dropping the length (calling {@code write(key)}) would still
     * buffer here, but a large body would then also be buffered whole instead of rolling over into a
     * direct upload; passing the exact length is what lets the store pick the right path for both.
     *
     * <p>This is the single write site for every per-endpoint fragment, so a save with 317 endpoints
     * issues ~1,000 small PUTs on the pool rather than ~1,000 serialised round trips on the crawl
     * thread.
     */
    void writeFragment(ArtifactKey dir, String fileName, String content) throws IOException {
        byte[] body = content.getBytes(StandardCharsets.UTF_8);
        try (OutputStream out = store.write(dir.resolve(fileName), body.length)) {
            out.write(body);
        }
    }

    void writeFragmentSafe(ArtifactKey dir, String fileName, String content) {
        try {
            writeFragment(dir, fileName, content != null ? content : "unknown error");
        } catch (IOException e) {
            log.warn("Failed to write fragment {}: {}", fileName, e.getMessage());
        }
    }

    void writeMetaSafe(ArtifactKey dir, String jarName, int total, int done, String mode) {
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

    /** @deprecated local-filesystem view of {@link #getWorkKey}. */
    @Deprecated
    public Path getWorkDir(String jarName) {
        return jarDataPaths.claudeDir(jarName);
    }

    public ArtifactKey getWorkKey(String jarName) {
        return jarDataPaths.claudeDirKey(jarName);
    }

    /**
     * One listing, not a stat per fragment: {@code list} already carries each entry's size, so
     * the previous {@code Files.size} per file (an S3 HEAD each) is unnecessary.
     */
    public List<Map<String, Object>> listFragments(String jarName) {
        List<Map<String, Object>> fragments = new ArrayList<>();
        try (var stream = store.list(getWorkKey(jarName), false)) {
            stream.filter(e -> !e.directory())
                    .filter(e -> e.key().leaf().endsWith(".json") || e.key().leaf().endsWith(".txt"))
                    .sorted(Comparator.comparing(e -> e.key().leaf()))
                    .forEach(e -> {
                        Map<String, Object> info = new LinkedHashMap<>();
                        info.put("name", e.key().leaf());
                        info.put("size", e.size());
                        fragments.add(info);
                    });
        } catch (IOException e) {
            log.debug("Failed to list fragments for {}: {}", jarName, e.getMessage());
        }
        return fragments;
    }
}
