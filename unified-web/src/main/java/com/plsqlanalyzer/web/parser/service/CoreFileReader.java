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
        if (fileName == null || fileName.isBlank() || fileName.contains("..")
                || fileName.contains("/") || fileName.contains("\\")) {
            throw new IOException("Invalid source file name: " + fileName);
        }
        ArtifactStore store = r.store();
        ArtifactKey analysisDir = AnalysisPaths.dirKey(name);
        ArtifactKey sourcesDir = analysisDir.resolve("sources");

        String base = fileName.contains(".") ? fileName.substring(0, fileName.lastIndexOf('.')) : fileName;

        ArtifactKey found = firstExisting(store, sourcesDir, fileName, base);
        if (found == null) {
            throw new IOException("Source file not found: " + sourcesDir.resolve(fileName));
        }
        try (InputStream in = r.openRead(found)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /** The exact name if present, else the first extension variant that is. */
    private static ArtifactKey firstExisting(ArtifactStore store, ArtifactKey dir,
                                             String fileName, String base) {
        ArtifactKey exact = dir.resolve(fileName);
        if (store.exists(exact)) return exact;
        for (String ext : SOURCE_EXTS) {
            ArtifactKey alt = dir.resolve(base + ext);
            if (store.exists(alt)) return alt;
        }
        return null;
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
