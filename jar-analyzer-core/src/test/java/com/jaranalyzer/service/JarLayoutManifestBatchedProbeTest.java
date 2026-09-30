package com.jaranalyzer.service;

import com.analyzer.storage.ArtifactEntry;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Fix A — the jar manifest is built from ONE listing.
 *
 * <p>{@code writeManifest} used to answer every presence/size question with a HEAD ({@code exists}
 * + {@code size}) per artifact — ~14 S3 round trips for a document whose entire purpose is to save
 * the reader round trips — and then take a second listing for the endpoints aggregate. It now takes
 * one {@code statPrefix} of the jar root and feeds both the per-artifact entries and the prefix
 * aggregate from it.
 *
 * <p>These tests pin the two properties that makes that safe: the mtime a listing already carries is
 * used verbatim (so the manifest bytes are unchanged), and when a caller supplies it the store is
 * never asked for one. A store that throws on ANY call is the sharpest form of "never asked".
 */
class JarLayoutManifestBatchedProbeTest {

    private static final ArtifactKey ANALYSIS = ArtifactKey.of("jar", "web", "analysis.json");
    private static final ArtifactKey HEADER = ArtifactKey.of("jar", "web", "_header.json");
    private static final ArtifactKey ENDPOINTS = ArtifactKey.of("jar", "web", "endpoints");
    private static final ArtifactKey EP_ONE = ArtifactKey.of("jar", "web", "endpoints", "001_a", "endpoint.json");
    private static final ArtifactKey EP_ONE_NODES = ArtifactKey.of("jar", "web", "endpoints", "001_a", "nodes.json");
    private static final ArtifactKey EP_TWO = ArtifactKey.of("jar", "web", "endpoints", "002_b", "endpoint.json");

    /** An ArtifactStore that must not be touched — any call is a test failure. */
    private static ArtifactStore untouchable() {
        return (ArtifactStore) Proxy.newProxyInstance(
                ArtifactStore.class.getClassLoader(), new Class<?>[]{ArtifactStore.class},
                (proxy, method, args) -> {
                    throw new AssertionError("store." + method.getName()
                            + "() must not be called: the manifest is built from one listing");
                });
    }

    private static Map<ArtifactKey, ArtifactEntry> listing() {
        Map<ArtifactKey, ArtifactEntry> tree = new LinkedHashMap<>();
        tree.put(ANALYSIS, new ArtifactEntry(ANALYSIS, 100, 1_000, false));
        tree.put(HEADER, new ArtifactEntry(HEADER, 20, 900, false));
        tree.put(EP_ONE, new ArtifactEntry(EP_ONE, 10, 800, false));
        tree.put(EP_ONE_NODES, new ArtifactEntry(EP_ONE_NODES, 30, 800, false));
        tree.put(EP_TWO, new ArtifactEntry(EP_TWO, 10, 800, false));
        return tree;
    }

    @Test
    @DisplayName("a supplied listing supplies sizes, mtimes and the prefix aggregate — no store call")
    void listingFeedsArtifactsAndPrefixAggregate() throws Exception {
        JarLayoutManifest m = new JarLayoutManifest("web", 5L);
        Map<ArtifactKey, ArtifactEntry> tree = listing();
        m.recordArtifact(ANALYSIS, tree.get(ANALYSIS).size(), 7L, tree.get(ANALYSIS).lastModified());
        m.recordPrefix(tree, ENDPOINTS);

        Map<String, Object> doc = m.toDocument(untouchable());

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> artifacts = (List<Map<String, Object>>) doc.get("artifacts");
        Map<String, Object> analysisRow = artifacts.stream()
                .filter(a -> ANALYSIS.toString().equals(a.get("key")))
                .findFirst().orElseThrow();
        assertEquals(100L, analysisRow.get("length"));
        assertEquals(7L, analysisRow.get("rows"));
        assertEquals(1_000L, analysisRow.get("lastModified"),
                "the mtime the listing carried must be used verbatim");

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> collections = (List<Map<String, Object>>) doc.get("collections");
        Map<String, Object> epAggregate = collections.stream()
                .filter(c -> (ENDPOINTS + "/").equals(c.get("prefix")))
                .findFirst().orElseThrow();
        assertEquals(3L, epAggregate.get("count"), "every non-directory key under endpoints/ counts");
        assertEquals(50L, epAggregate.get("totalBytes"));
    }

    @Test
    @DisplayName("the prefix aggregate ignores keys outside the prefix")
    void prefixAggregateIgnoresSiblings() throws Exception {
        JarLayoutManifest m = new JarLayoutManifest("web", 5L);
        Map<ArtifactKey, ArtifactEntry> tree = listing();
        tree.put(ArtifactKey.of("jar", "other", "endpoints", "x", "endpoint.json"),
                new ArtifactEntry(ArtifactKey.of("jar", "other", "endpoints", "x", "endpoint.json"),
                        999, 1, false));

        m.recordPrefix(tree, ENDPOINTS);
        Map<String, Object> doc = m.toDocument(untouchable());

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> collections = (List<Map<String, Object>>) doc.get("collections");
        Map<String, Object> epAggregate = collections.stream()
                .filter(c -> (ENDPOINTS + "/").equals(c.get("prefix")))
                .findFirst().orElseThrow();
        assertEquals(3L, epAggregate.get("count"), "a sibling analysis's keys must not leak in");
        assertEquals(50L, epAggregate.get("totalBytes"));
    }

    @Test
    @DisplayName("with no supplied mtime the store is consulted once — the documented fallback")
    void fallsBackToStoreWhenNoMtimeSupplied() throws Exception {
        long[] lastModifiedCalls = {0};
        ArtifactStore store = (ArtifactStore) Proxy.newProxyInstance(
                ArtifactStore.class.getClassLoader(), new Class<?>[]{ArtifactStore.class},
                (proxy, method, args) -> {
                    if ("lastModified".equals(method.getName())) {
                        lastModifiedCalls[0]++;
                        return 4_242L;
                    }
                    throw new AssertionError("only lastModified() may be called, got " + method.getName());
                });

        JarLayoutManifest m = new JarLayoutManifest("web", 5L);
        m.recordArtifact(ANALYSIS, 100L, null);
        Map<String, Object> doc = m.toDocument(store);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> artifacts = (List<Map<String, Object>>) doc.get("artifacts");
        assertEquals(4_242L, artifacts.get(0).get("lastModified"));
        assertEquals(1, lastModifiedCalls[0], "exactly one HEAD, only because none was supplied");
    }
}
