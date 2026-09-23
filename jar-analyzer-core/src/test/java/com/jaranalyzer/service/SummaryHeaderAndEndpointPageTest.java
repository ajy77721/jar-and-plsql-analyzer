package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PLAN-08 §4.3 step 8. Two things are being fixed at once and they fail in opposite ways.
 *
 * <p><b>The header.</b> Drawing the JAR title bar cost the whole summary — 429 782 B measured on
 * {@code web-1.0.0-SNAPSHOT}, 130 420 B through {@code /summary/headers} — for eleven scalars. The
 * property that has to hold is that the header response's size does not depend on the analysis at
 * all: if an array can leak into it, the fix silently stops working the next time someone adds a
 * field to the summary writer, and nobody notices because it still renders.
 *
 * <p><b>The positional-coupling hazard</b> (PLAN-03 §1.5 #4, "the highest-severity latent bug in
 * the UI"). The Summary tab joins each lazily-fetched per-endpoint slice onto the endpoint list by
 * array index. That is right only while two independent passes enumerate endpoints in identical
 * order — and when it is wrong, one endpoint's external calls are rendered under another and
 * nothing errors. The test the plan asks for (§8.2) is to <b>shuffle the endpoint order between the
 * two responses and assert the join is unchanged</b>, which is exactly what an index-based join
 * cannot survive and an identity-based one does not notice.
 */
@DisplayName("Summary header (Q21) and paged endpoint list (Q21b)")
class SummaryHeaderAndEndpointPageTest {

    @TempDir
    Path tempDir;

    private ObjectMapper mapper;
    private TestableProvider provider;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper();
        provider = new TestableProvider(tempDir.resolve("dummy.json"), mapper, tempDir);
    }

    // ── Q21: the header ──────────────────────────────────────────────────────

    @Test
    @DisplayName("the header carries every top-level scalar and no collection")
    void headerIsScalarsOnly() throws Exception {
        JsonNode h = mapper.readTree(header(summary(40)));

        assertEquals("svc.jar", h.get("jarName").asText());
        assertEquals(70489772L, h.get("jarSize").asLong());
        assertEquals(119, h.get("totalClasses").asInt());
        assertEquals(40, h.get("totalEndpoints").asInt());
        assertEquals("STATIC", h.get("analysisMode").asText());
        assertEquals("com.enterprise", h.get("basePackage").asText());
        assertEquals(3, h.get("scheduledCount").asInt());

        assertNull(h.get("endpoints"), "the endpoint list is the thing this endpoint exists to omit");
        assertNull(h.get("classIndex"));
        assertNull(h.get("entityCollMap"));
        h.fields().forEachRemaining(e -> assertTrue(e.getValue().isValueNode(),
                "a non-scalar leaked into the header: " + e.getKey()));
    }

    @Test
    @DisplayName("the header's size does not grow with the analysis — the property, not the number")
    void headerSizeIsIndependentOfTheAnalysis() throws Exception {
        int small = header(summary(1)).length();
        int large = header(summary(4000)).length();

        // Not equal: `totalEndpoints` is itself one of the scalars, so "1" costs three characters
        // less than "4000". Everything whose size is proportional to the analysis is gone, which is
        // the property — a 4000× larger analysis must not produce a measurably larger header.
        assertTrue(large - small <= 8,
                "the header grew with the analysis: " + small + " → " + large);
        assertTrue(large < 500, "expected a few hundred bytes, got " + large);
    }

    @Test
    @DisplayName("a cursor whose endpoint no longer exists is reported, never answered as empty")
    void aStaleCursorIsReported() throws Exception {
        var gone = endpoints(summary(10), "GET /api/v1/deleted com.gone.Ctrl#handle", 5, null);

        assertFalse(gone.mark().cursorResolved(),
                "the caller must be able to answer 409 rather than 'end of list'");
        assertEquals("[]", gone.json());
        assertEquals(10, gone.mark().matched(), "the total is still the truth about the document");

        // The normal case is not mistaken for it.
        var ok = endpoints(summary(10), null, 5, null);
        assertTrue(ok.mark().cursorResolved());
        assertTrue(endpoints(summary(10), ok.mark().lastId(), 5, null).mark().cursorResolved());
    }

    @Test
    @DisplayName("a cursor minted under one search still resolves under another")
    void theCursorIsIndependentOfTheFilter() throws Exception {
        String doc = summary(40);
        // Page 1 of an unfiltered walk, then the same cursor with a search applied. The anchor is
        // found by identity, so the page is the filtered remainder — not an empty result.
        var page1 = endpoints(doc, null, 5, null);
        var filtered = endpoints(doc, page1.mark().lastId(), 5, "Ctrl3");

        assertTrue(filtered.mark().cursorResolved());
        assertEquals(4, filtered.mark().matched(), "the total is over the whole document");
        JsonNode items = mapper.readTree(filtered.json());
        assertEquals(3, items.size(), "endpoint 3 is before the cursor; 13, 23, 33 follow it");
        assertEquals("/api/v1/r13", items.get(0).get("fullPath").asText());
    }

    // ── Q21b: the paged endpoint list ────────────────────────────────────────

    @Test
    @DisplayName("no limit returns every endpoint, in analysis order, minus the heavy fields")
    void unpagedReturnsTheWholeArray() throws Exception {
        var out = endpoints(summary(40), null, -1, null);
        JsonNode items = mapper.readTree(out.json());

        assertEquals(40, items.size());
        assertEquals(40, out.mark().matched());
        assertFalse(out.mark().hasMore());
        assertEquals("/api/v1/r0", items.get(0).get("fullPath").asText(), "analysis order");
        assertEquals("/api/v1/r39", items.get(39).get("fullPath").asText());
        // Heavy fields are dropped exactly as headers mode drops them.
        assertNull(items.get(0).get("externalCalls"));
        assertNull(items.get(0).get("beans"));
        assertNotNull(items.get(0).get("aggregatedCollections"), "light fields survive");
    }

    @Test
    @DisplayName("a cursor chain walks the whole list once, with no gap and no repeat")
    void cursorChainCoversEveryEndpointExactlyOnce() throws Exception {
        String doc = summary(40);
        List<String> seen = new ArrayList<>();
        String after = null;
        int guard = 0;
        while (guard++ < 20) {
            var out = endpoints(doc, after, 7, null);
            for (JsonNode item : mapper.readTree(out.json())) {
                seen.add(item.get("fullPath").asText());
            }
            assertEquals(40, out.mark().matched(), "the total is exact on every page");
            if (!out.mark().hasMore()) break;
            after = out.mark().lastId();
            assertNotNull(after, "hasMore without a cursor would be a dead end");
        }
        assertEquals(40, seen.size(), "every endpoint, exactly once");
        assertEquals(40, Set_of(seen).size(), "no repeats");
        assertEquals("/api/v1/r0", seen.get(0));
        assertEquals("/api/v1/r39", seen.get(39));
    }

    @Test
    @DisplayName("q filters server-side, and the total counts matches rather than rows returned")
    void searchMovesToTheServer() throws Exception {
        // r7 and r17..r19 contain "7" or start with "r1"; use the controller name instead so the
        // expectation is unambiguous.
        var out = endpoints(summary(40), null, 5, "Ctrl3");
        JsonNode items = mapper.readTree(out.json());

        assertEquals(4, out.mark().matched(), "Ctrl3 declares endpoints 3, 13, 23, 33");
        assertEquals(4, items.size());
        for (JsonNode item : items) {
            assertEquals("Ctrl3", item.get("controllerSimpleName").asText());
        }
        assertFalse(out.mark().hasMore());
    }

    @Test
    @DisplayName("q is case-insensitive and searches the path as well as the controller")
    void searchCoversTheColumnsTheBrowserSearched() throws Exception {
        assertEquals(1, endpoints(summary(40), null, -1, "/API/V1/R7").mark().matched());
        assertEquals(40, endpoints(summary(40), null, -1, "get").mark().matched());
        assertEquals(0, endpoints(summary(40), null, -1, "no-such-thing").mark().matched());
    }

    // ── The positional-coupling hazard ───────────────────────────────────────

    @Test
    @DisplayName("the endpoint identity is the natural key, never the ordinal")
    void identityIsANaturalKey() throws Exception {
        JsonNode items = mapper.readTree(endpoints(summary(3), null, -1, null).json());

        assertEquals("GET /api/v1/r0 com.enterprise.web.Ctrl0#handle0",
                items.get(0).get("endpointId").asText());
        // Nothing in the identity is derived from position, so it survives a reorder.
        for (JsonNode item : items) {
            assertFalse(item.get("endpointId").asText().contains("endpointIdx"));
        }
    }

    @Test
    @DisplayName("UI-025: endpointIndex carries the ordinal /endpoints/by-index/{idx}/call-tree needs")
    void endpointIndexMatchesThePositionInTheArray() throws Exception {
        // UI-025 (§55): the endpoint list used to hold analysis.endpoints[] whole and read each
        // card's ordinal straight off it (`_origIdx = i`). Paged, a card's row never sees the
        // array; the ordinal has to travel with the row instead, tracked over every endpoint the
        // scan visits — matched or not, before or after the cursor — not just the ones written.
        JsonNode items = mapper.readTree(endpoints(summary(10), null, -1, null).json());
        for (int i = 0; i < 10; i++) {
            assertEquals(i, items.get(i).get("endpointIndex").asInt(),
                    "row " + i + " must carry its true position in analysis.endpoints[]");
        }
    }

    @Test
    @DisplayName("UI-025: endpointIndex is still correct mid-scan, across a cursor and a filter")
    void endpointIndexSurvivesPagingAndFiltering() throws Exception {
        String doc = summary(40);
        // Page from the middle of the list.
        var firstPage = endpoints(doc, null, 10, null);
        var secondPage = endpoints(doc, firstPage.mark().lastId(), 10, null);
        JsonNode items = mapper.readTree(secondPage.json());
        assertEquals(10, items.get(0).get("endpointIndex").asInt(), "page 2 starts at ordinal 10");
        assertEquals(19, items.get(9).get("endpointIndex").asInt());

        // A filtered request must not renumber what survives — the ordinal is the position in the
        // whole array, not in the filtered result.
        JsonNode filtered = mapper.readTree(endpoints(doc, null, -1, "Ctrl3").json());
        // Ctrl3 owns endpoints 3, 13, 23, 33 (every 10th, per the fixture's controller-per-index).
        assertEquals(3, filtered.get(0).get("endpointIndex").asInt());
        assertEquals(33, filtered.get(3).get("endpointIndex").asInt());
    }

    @Test
    @DisplayName("PLAN-08 §8.2: shuffling the endpoint order does not change the slice join")
    void shufflingTheEndpointOrderDoesNotChangeTheJoin() throws Exception {
        // The list is fetched once; the slice is fetched later, from an analysis whose endpoints
        // have been reordered (a Claude correction rewriting the document is exactly this).
        String listDoc = summary(12);
        String sliceDoc = summaryShuffled(12, 20260902L);

        Map<String, String> list = idToPath(endpoints(listDoc, null, -1, null).json());
        Map<String, String> slice = sliceIdToOwner(sliceDoc);

        assertEquals(12, list.size());
        assertEquals(12, slice.size());
        // The join by identity: every slice item lands on the endpoint whose path matches the one
        // its own payload was generated from.
        for (var e : slice.entrySet()) {
            assertEquals(e.getValue(), list.get(e.getKey()),
                    "slice for " + e.getKey() + " joined to the wrong endpoint");
        }

        // And the demonstration that the old join was the bug: with the order shuffled, at least
        // one endpointIdx now points at a different endpoint than its identity does.
        List<String> byIndex = new ArrayList<>(idToPath(endpoints(sliceDoc, null, -1, null).json())
                .values());
        List<String> original = new ArrayList<>(list.values());
        assertNotEquals(original, byIndex,
                "the fixture must actually reorder, or this test proves nothing");
    }

    @Test
    @DisplayName("the slice still carries endpointIdx, so an unconverted caller is unaffected")
    void sliceKeepsItsLegacyIndex() throws Exception {
        JsonNode slice = mapper.readTree(slice(summary(5)));
        assertEquals(5, slice.size());
        for (int i = 0; i < 5; i++) {
            assertEquals(i, slice.get(i).get("endpointIdx").asInt());
            assertTrue(slice.get(i).has("endpointId"));
            assertTrue(slice.get(i).has("fullPath"));
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static java.util.Set<String> Set_of(List<String> l) {
        return new java.util.LinkedHashSet<>(l);
    }

    private String header(String doc) throws Exception {
        try (var parser = mapper.getFactory().createParser(doc);
             var baos = new ByteArrayOutputStream();
             var gen = mapper.getFactory().createGenerator(baos)) {
            provider.streamSummaryHeader(parser, gen);
            gen.flush();
            return baos.toString();
        }
    }

    private record Paged(String json, AbstractAnalysisDataProvider.EndpointPageMark mark) {}

    private Paged endpoints(String doc, String after, int limit, String q) throws Exception {
        try (var parser = mapper.getFactory().createParser(doc);
             var baos = new ByteArrayOutputStream();
             var gen = mapper.getFactory().createGenerator(baos)) {
            var mark = provider.streamSummaryEndpointPage(parser, gen, after, limit, q);
            gen.flush();
            return new Paged(baos.toString(), mark);
        }
    }

    private String slice(String doc) throws Exception {
        try (var parser = mapper.getFactory().createParser(doc);
             var baos = new ByteArrayOutputStream();
             var gen = mapper.getFactory().createGenerator(baos)) {
            provider.streamSummarySlice(parser, gen, "slice", java.util.Set.of("externalCalls"));
            gen.flush();
            return baos.toString();
        }
    }

    /** endpointId → fullPath, as the client would key the endpoint list. */
    private Map<String, String> idToPath(String itemsJson) throws Exception {
        Map<String, String> out = new LinkedHashMap<>();
        for (JsonNode item : mapper.readTree(itemsJson)) {
            out.put(item.get("endpointId").asText(), item.get("fullPath").asText());
        }
        return out;
    }

    /**
     * endpointId → the path baked into the slice's own payload.
     *
     * <p>Each fixture endpoint's {@code externalCalls} names the path it belongs to, so a join that
     * puts a slice on the wrong endpoint is detectable from the data alone rather than from a
     * count.
     */
    private Map<String, String> sliceIdToOwner(String doc) throws Exception {
        Map<String, String> out = new LinkedHashMap<>();
        for (JsonNode item : mapper.readTree(slice(doc))) {
            out.put(item.get("endpointId").asText(),
                    item.get("externalCalls").get(0).get("methodName").asText());
        }
        return out;
    }

    private String summary(int endpoints) throws Exception {
        return mapper.writeValueAsString(doc(order(endpoints)));
    }

    private String summaryShuffled(int endpoints, long seed) throws Exception {
        List<Integer> order = order(endpoints);
        Collections.shuffle(order, new java.util.Random(seed));
        return mapper.writeValueAsString(doc(order));
    }

    private static List<Integer> order(int n) {
        List<Integer> l = new ArrayList<>();
        for (int i = 0; i < n; i++) l.add(i);
        return l;
    }

    private Map<String, Object> doc(List<Integer> order) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("jarName", "svc.jar");
        m.put("jarSize", 70489772L);
        m.put("analyzedAt", "2026-07-19T19:45:20.171790");
        m.put("totalClasses", 119);
        m.put("totalEndpoints", order.size());
        m.put("analysisMode", "STATIC");
        m.put("basePackage", "com.enterprise");
        m.put("scheduledCount", 3);
        m.put("classIndex", List.of(Map.of("fullyQualifiedName", "com.enterprise.web.Ctrl0",
                                           "simpleName", "Ctrl0")));
        m.put("entityCollMap", Map.of("com.enterprise.E", "ENTITIES"));
        List<Map<String, Object>> eps = new ArrayList<>();
        for (int i : order) eps.add(endpoint(i));
        m.put("endpoints", eps);
        return m;
    }

    private Map<String, Object> endpoint(int i) {
        Map<String, Object> ep = new LinkedHashMap<>();
        ep.put("httpMethod", "GET");
        ep.put("fullPath", "/api/v1/r" + i);
        ep.put("controllerClass", "com.enterprise.web.Ctrl" + (i % 10));
        ep.put("controllerSimpleName", "Ctrl" + (i % 10));
        ep.put("methodName", "handle" + i);
        ep.put("aggregatedCollections", List.of("COLL_" + i));
        ep.put("totalDbOperations", i);
        // Heavy: dropped by headers mode, and the payload the slice endpoint serves. methodName
        // names the owning path so a mis-join is visible in the value, not just in a count.
        ep.put("externalCalls", List.of(Map.of("className", "com.ext.Client",
                                               "methodName", "/api/v1/r" + i)));
        ep.put("beans", List.of(Map.of("className", "com.enterprise.svc.Svc" + i)));
        return ep;
    }

    private static class TestableProvider extends AbstractAnalysisDataProvider {
        TestableProvider(Path filePath, ObjectMapper objectMapper, Path baseDir) {
            this(new JarDataPaths(baseDir.resolve("jar").toString()), filePath, objectMapper);
        }

        private TestableProvider(JarDataPaths paths, Path filePath, ObjectMapper objectMapper) {
            super(paths.keyForPath(filePath), objectMapper, paths, "test-jar");
        }

        @Override public String getProviderType() { return "TEST"; }
        @Override public com.analyzer.storage.ArtifactKey getSummaryCacheKey() {
            return com.analyzer.storage.ArtifactKey.of("jar", "test-jar", "summary.json");
        }
        @Override public boolean shouldComputeVerification() { return false; }
    }
}
