package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.model.CallNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Disk-backed subtree cache for cross-endpoint CallNode reuse.
 *
 * Design:
 *   - Each resolved subtree is serialized as one JSONL line in a temp file
 *   - In-memory index maps SubtreeCacheKey → byte offset (tiny: ~200 bytes/entry)
 *   - On cache hit, seeks to offset and deserializes just that one line
 *   - File is deleted on close()
 *
 * Cache key captures the 4 factors that make a subtree unique:
 *   1. nodeId (className#methodName#descriptor)
 *   2. narrowedImplFqn (specific impl after @Qualifier/heuristic narrowing, null = all impls)
 *   3. controllerJar (determines crossModule flag for every node below)
 *   4. remainingDepth (depth budget left — determines truncation point)
 */
class SubtreeCache implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(SubtreeCache.class);

    record SubtreeCacheKey(String nodeId, String narrowedImplFqn, String controllerJar, int remainingDepth) {}

    private final ObjectMapper objectMapper;
    private final Path cacheFile;
    private final RandomAccessFile raf;
    private final Map<SubtreeCacheKey, Long> index = new HashMap<>();

    private int hits = 0;
    private int misses = 0;

    SubtreeCache(ObjectMapper objectMapper) throws IOException {
        this.objectMapper = objectMapper;
        this.cacheFile = Files.createTempFile("subtree-cache-", ".jsonl");
        this.raf = new RandomAccessFile(cacheFile.toFile(), "rw");
        log.debug("Subtree cache file: {}", cacheFile);
    }

    /**
     * Store a resolved subtree. Appends one JSONL line, records byte offset.
     */
    void put(SubtreeCacheKey key, CallNode subtree) {
        try {
            byte[] json = objectMapper.writeValueAsBytes(subtree);
            long offset;
            synchronized (raf) {
                offset = raf.length();
                raf.seek(offset);
                raf.write(json);
                raf.write('\n');
            }
            index.put(key, offset);
        } catch (IOException e) {
            log.debug("Failed to cache subtree {}: {}", key.nodeId(), e.getMessage());
        }
    }

    /**
     * Look up a cached subtree. Returns a deep copy (independent from other endpoints).
     * Returns null on cache miss.
     */
    CallNode get(SubtreeCacheKey key) {
        Long offset = index.get(key);
        if (offset == null) {
            misses++;
            return null;
        }
        try {
            byte[] buf;
            synchronized (raf) {
                raf.seek(offset);
                String line = raf.readLine();
                if (line == null) {
                    misses++;
                    return null;
                }
                buf = line.getBytes(StandardCharsets.ISO_8859_1);
            }
            CallNode cached = objectMapper.readValue(buf, CallNode.class);
            hits++;
            return cached;
        } catch (IOException e) {
            log.debug("Failed to read cached subtree at offset {}: {}", offset, e.getMessage());
            misses++;
            return null;
        }
    }

    boolean containsKey(SubtreeCacheKey key) {
        return index.containsKey(key);
    }

    int size() { return index.size(); }
    int hits() { return hits; }
    int misses() { return misses; }

    double hitRate() {
        int total = hits + misses;
        return total == 0 ? 0.0 : (double) hits / total;
    }

    @Override
    public void close() {
        try {
            raf.close();
        } catch (IOException e) {
            log.debug("Failed to close cache RAF: {}", e.getMessage());
        }
        try {
            Files.deleteIfExists(cacheFile);
        } catch (IOException e) {
            log.debug("Failed to delete cache file: {}", e.getMessage());
        }
        log.info("Subtree cache closed: {} entries, {} hits, {} misses, {}% hit rate",
                index.size(), hits, misses, Math.round(hitRate() * 1000) / 10.0);
    }
}
