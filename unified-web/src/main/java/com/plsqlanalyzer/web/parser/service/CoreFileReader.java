package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Core file reads: merged call graph, source lookup, node-file collection.
 *
 * <p>All access goes through {@link ArtifactStore}. Locations are built with
 * {@link AnalysisPaths#dirKey}/{@link AnalysisPaths#resolveKey}, which translate the {@code '~'}
 * identity name into its nested {@code <DBTYPE>/<PROJECT>/<ENV>/<OBJECT>} key. The tilde name is
 * never used as a literal segment — doing that once made every source view 404.
 */
class CoreFileReader {

    /** Extensions tried when the requested source file name has no exact match. */
    private static final String[] SOURCE_EXTS = {".sql", ".prc", ".fnc", ".pkb", ".pks", ".trg"};

    private final StaticAnalysisReader r;

    CoreFileReader(StaticAnalysisReader r) {
        this.r = r;
    }

    JsonNode getCallGraph(String name) throws IOException {
        ObjectMapper mapper = r.mapper;
        ArtifactKey file = r.resolveKey(name, "api/call_graph.json");
        ArtifactStore store = r.store();
        if (store.exists(file)) {
            try (InputStream in = r.openRead(file)) {
                return mapper.readTree(in);
            }
        }
        throw new IOException("call_graph.json not found: " + file);
    }

    String getSource(String name, String fileName) throws IOException {
        // fileName arrives from a REST path variable and is used as a single key segment. The old
        // Path.resolve() form would have followed a "../.." out of the analysis folder, so the
        // guard is explicit here rather than implied by a startsWith(base) check.
        if (!isValidSourceName(fileName)) {
            throw new IOException("Invalid source file name: " + fileName);
        }
        ArtifactStore store = r.store();
        ArtifactKey sourcesDir = AnalysisPaths.dirKey(name).resolve("sources");

        ArtifactKey found = firstExisting(store, sourcesDir, fileName);
        if (found == null) {
            throw new IOException("Source file not found: " + sourcesDir.resolve(fileName));
        }
        return readText(r.openRead(found));
    }

    /**
     * The source text for {@code fileName}, resolved against a sources/ listing the caller already
     * paid for ({@link #sourceKeyIndex(String)}), or {@code null} when it is absent.
     *
     * <p>This is the fan-out path: a view that scans many nodes resolves every node's {@code
     * sourceFile} out of one listing instead of issuing the per-source exact→.sql→.prc HEAD probe
     * loop. Zero storage ops per source, versus up to a handful of HEADs each.
     */
    String getSourceFromIndex(Map<String, ArtifactKey> index, String fileName) throws IOException {
        if (!isValidSourceName(fileName) || index == null) return null;
        ArtifactKey found = resolveFromIndex(index, fileName);
        if (found == null) return null;
        return readText(r.openRead(found));
    }

    /**
     * One listing of the analysis's {@code sources/} directory, keyed by leaf file name — the
     * in-memory index {@link #getSourceFromIndex} resolves against. Built once per view computation
     * (the caller decides its lifetime); there is deliberately no cross-request cache.
     */
    Map<String, ArtifactKey> sourceKeyIndex(String name) throws IOException {
        ArtifactKey sourcesDir = AnalysisPaths.dirKey(name).resolve("sources");
        Map<String, ArtifactKey> byLeaf = new HashMap<>();
        for (ArtifactKey k : r.store().statPrefix(sourcesDir).keySet()) {
            byLeaf.putIfAbsent(k.leaf(), k);
        }
        return byLeaf;
    }

    private static String readText(InputStream in) throws IOException {
        try (InputStream stream = in) {
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static boolean isValidSourceName(String fileName) {
        return fileName != null && !fileName.isBlank() && !fileName.contains("..")
                && !fileName.contains("/") && !fileName.contains("\\");
    }

    /** The exact name if present, else the first extension variant that is. */
    private static ArtifactKey firstExisting(ArtifactStore store, ArtifactKey dir, String fileName) {
        ArtifactKey exact = dir.resolve(fileName);
        if (store.exists(exact)) return exact;
        String base = baseName(fileName);
        for (String ext : SOURCE_EXTS) {
            // The exact key already ends in this candidate when the caller passed a named file
            // (e.g. "X.sql"): probing "X.sql" a second time is a guaranteed-miss round trip.
            if (fileName.equals(base + ext)) continue;
            ArtifactKey alt = dir.resolve(base + ext);
            if (store.exists(alt)) return alt;
        }
        return null;
    }

    /** Resolves a requested source name against a prebuilt {@link #sourceKeyIndex} listing. */
    static ArtifactKey resolveFromIndex(Map<String, ArtifactKey> index, String fileName) {
        ArtifactKey exact = index.get(fileName);
        if (exact != null) return exact;
        String base = baseName(fileName);
        for (String ext : SOURCE_EXTS) {
            ArtifactKey alt = index.get(base + ext);
            if (alt != null) return alt;
        }
        return null;
    }

    private static String baseName(String fileName) {
        return fileName.contains(".") ? fileName.substring(0, fileName.lastIndexOf('.')) : fileName;
    }

    List<ArtifactKey> collectNodeFiles(String name) throws IOException {
        ArtifactStore store = r.store();
        ArtifactKey analysisDir = AnalysisPaths.dirKey(name);
        return new ArrayList<>(nodeJsonKeys(store, analysisDir));
    }

    /** {@code <dir>/api/nodes/*.json}. */
    private static List<ArtifactKey> nodeJsonKeys(ArtifactStore store, ArtifactKey dir) {
        try (var s = store.list(dir.resolve("api").resolve("nodes"), false)) {
            return s.filter(java.util.function.Predicate.not(ArtifactEntry::directory))
                    .filter(e -> e.key().leaf().endsWith(".json"))
                    .map(ArtifactEntry::key)
                    .toList();
        } catch (IOException e) {
            return List.of();
        }
    }
}
