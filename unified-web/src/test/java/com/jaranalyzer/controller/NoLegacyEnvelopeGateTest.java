package com.jaranalyzer.controller;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MID-021 (§102) — two of {@code AnalyzerController}'s `wantsEnvelope()`/`legacy` branches were
 * confirmed genuinely dead by grepping every real client caller, not assumed:
 *
 * <ul>
 *   <li>{@code getCollectionCallPaths} ({@code /{id}/collections/call-paths}) — its only caller,
 *       {@code JA.api.getCollectionCallPaths}, always sends the
 *       {@code application/vnd.analyzer.page+json} Accept header, so {@code wantsEnvelope()} was
 *       never false in practice.</li>
 *   <li>{@code getSummaryEndpoints} ({@code /{id}/summary/endpoints}) — its only caller,
 *       {@code JA.api.endpointListPages} via {@code PA.pageSource}, always sends {@code limit}
 *       (unconditionally, in {@code fetchPage}), so {@code req.isLegacy()} was never true in
 *       practice; the bare-call API wrapper this endpoint was named for
 *       ({@code JA.api.getSummaryEndpoints}) has no callers left at all.</li>
 * </ul>
 *
 * <p>The other four `wantsEnvelope()`/`legacy` call sites in this codebase are deliberately NOT
 * covered here — `/class-tree`'s branch is a structurally different response shape (not a
 * legacy/paged pair at all), and `/bundled-jars` was checked (§103) and left alone as a real,
 * low-risk defensive fallback, not dead code. `/analyses` (§104) is deliberately kept, not deleted
 * — see {@link #analysesEndpointStillHasItsRecordedExemptionReason()} — because four real client
 * scripts still call it bare and its actual list size (2 analyses, this repo) is nowhere near the
 * threshold that would justify the conversion cost.
 */
class NoLegacyEnvelopeGateTest {

    private static String source() throws IOException {
        Path p = repoRoot().resolve(
                "unified-web/src/main/java/com/jaranalyzer/controller/AnalyzerController.java");
        return Files.readString(p, StandardCharsets.UTF_8);
    }

    /** Walks up from the working directory to the reactor root, so the test runs from any module. */
    private static Path repoRoot() {
        Path p = Path.of("").toAbsolutePath();
        while (p != null && !Files.exists(p.resolve("analyzer-storage"))) {
            p = p.getParent();
        }
        if (p == null) throw new IllegalStateException("Could not locate the repository root");
        return p;
    }

    private static String methodBody(String src, String signatureMarker, String nextMethodMarker) {
        int start = src.indexOf(signatureMarker);
        assertTrue(start >= 0, "could not find method starting with: " + signatureMarker);
        int end = src.indexOf(nextMethodMarker, start);
        assertTrue(end > start, "could not find the next method marker: " + nextMethodMarker);
        return src.substring(start, end);
    }

    @Test
    void collectionCallPathsNoLongerHasAnUnpagedBranch() throws IOException {
        String body = methodBody(source(),
                "public void getCollectionCallPaths(",
                "// ==================== Lazy-Loading Endpoints ====================");
        assertFalse(body.contains("params.wantsEnvelope()"),
                "getCollectionCallPaths must no longer branch on wantsEnvelope() — its only real caller always requests the envelope");
        assertFalse(body.contains("facets.respond(rows.toArray()"),
                "the unpaged rows.toArray() path must be gone, not just unreachable");
        assertTrue(body.contains("facets.respondStreaming("),
                "the paged path must still be there — this is a simplification, not a deletion of the feature");
    }

    @Test
    void summaryEndpointsNoLongerHasALegacyBranch() throws IOException {
        String body = methodBody(source(),
                "public void getSummaryEndpoints(",
                "@GetMapping(\"/{id}/summary/reports\")");
        assertFalse(body.contains("boolean legacy"),
                "getSummaryEndpoints must no longer compute a legacy flag — its only real caller always pages");
        assertFalse(body.contains("streamSummaryEndpointPage(parser, gen, null, -1"),
                "the bare-array streaming call (afterId=null, limit=-1) must be gone, not just unreachable");
    }

    /**
     * {@code ParserAnalysisController./analyses} is a deliberate exemption, not an oversight — this
     * doesn't assert the branch is gone (it must stay, four real callers depend on it), it asserts
     * the *decision* is still recorded next to the code, so a future edit that strips the comment
     * without re-deciding gets caught rather than silently drifting back to "unexplained legacy".
     */
    @Test
    void analysesEndpointStillHasItsRecordedExemptionReason() throws IOException {
        Path p = repoRoot().resolve(
                "unified-web/src/main/java/com/plsqlanalyzer/web/parser/controller/ParserAnalysisController.java");
        String src = Files.readString(p, StandardCharsets.UTF_8);
        int idx = src.indexOf("public ResponseEntity<?> listAnalyses(");
        assertTrue(idx >= 0, "could not find listAnalyses in ParserAnalysisController");

        String before = src.substring(Math.max(0, idx - 2000), idx);
        assertTrue(before.contains("permanently exempt"),
                "listAnalyses's own doc comment must still record why its unpaged branch stays");

        int end = src.indexOf("@GetMapping", idx + 1);
        String bodyAndFollowing = src.substring(idx, end > idx ? end : src.length());
        assertTrue(bodyAndFollowing.contains("!params.wantsEnvelope()"),
                "the exemption reason is only meaningful if the branch it explains is still there");
    }
}
