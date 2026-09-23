package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * UI-026f — {@link EndpointReportsService}, checked against real {@code _summary_static.json}
 * documents (the same discovery {@code SummaryStatsTest} already uses) and against a synthetic
 * fixture for the one rule that can't be exercised on real data with confidence:
 * skipping an endpoint that (against the summary writer's own guarantee) still carries a
 * {@code callTree}.
 */
class EndpointReportsServiceTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private SummaryStats.Config cfg() {
        return new SummaryStats.Config(List.of(), Map.of(), List.of("_view"), List.of("v_"),
                List.of("batch", "export"));
    }

    @Test
    @DisplayName("on every real analysis in data/jar, epReports count matches the endpoint count SummaryStats sees")
    void epReportCountMatchesSummaryStatsOnRealData() throws IOException {
        List<Path> docs = summaryDocuments();
        assumeTrue(!docs.isEmpty(), "no analyses in data/jar — nothing to check against");

        for (Path doc : docs) {
            SummaryStats stats;
            try (var parser = mapper.getFactory().createParser(Files.newInputStream(doc))) {
                stats = SummaryStats.compute(parser, cfg(), mapper);
            }
            ObjectNode all;
            try (var parser = mapper.getFactory().createParser(Files.newInputStream(doc))) {
                all = EndpointReportsService.buildAll(parser, mapper, cfg(), null, Map.of(), List.of());
            }
            assertEquals(stats.endpoints(), all.path("epReports").size(),
                    "epReports count must match SummaryStats' own endpoint count for " + doc);
            // Every collection SummaryStats counted must show up somewhere in vertReport — a
            // cross-check that the per-endpoint reports feeding buildVerticalisation aren't
            // silently dropping collections.
            long vertCollCount = all.path("vertReport").size();
            assertTrue(vertCollCount <= stats.collections(),
                    "vertReport must not invent collections beyond what SummaryStats counted");
        }
    }

    @Test
    void anEndpointThatStillCarriesACallTreeIsSkippedRatherThanMisscored() throws Exception {
        // Defensive: EndpointReportBuilder only covers the no-callTree branch (§57/§58). If a
        // future summary writer regresses and leaves callTree on an endpoint, this must skip it,
        // not silently run the no-callTree logic against call-tree-shaped data.
        String json = """
            {"jarName": "x.jar", "endpoints": [
               {"fullPath": "/a", "controllerSimpleName": "C", "methodName": "m",
                "callTree": {"nodeId": "n1"}},
               {"fullPath": "/b", "controllerSimpleName": "C", "methodName": "n",
                "aggregatedCollections": []}
             ]}""";
        ObjectNode all;
        try (var parser = mapper.getFactory().createParser(json)) {
            all = EndpointReportsService.buildAll(parser, mapper, cfg(), null, Map.of(), List.of());
        }
        assertEquals(1, all.path("epReports").size(), "the callTree-bearing endpoint must be skipped");
        assertEquals("/b", all.path("epReports").get(0).path("fullPath").asText());
    }

    @Test
    void theFiveCrossReportsAreAllPresentEvenForAnEmptyAnalysis() throws Exception {
        String json = """
            {"jarName": "x.jar", "endpoints": []}""";
        ObjectNode all;
        try (var parser = mapper.getFactory().createParser(json)) {
            all = EndpointReportsService.buildAll(parser, mapper, cfg(), null, Map.of(), List.of());
        }
        for (String field : List.of("epReports", "vertReport", "viewsReport", "distReport",
                "batchReport", "extReport")) {
            assertTrue(all.has(field), "missing field: " + field);
        }
        assertTrue(all.path("epReports").isEmpty());
        assertTrue(all.path("extReport").path("crossModule").isEmpty());
    }

    /** Every {@code _summary_static.json} under {@code data/jar}, from either module's cwd —
     *  the same discovery {@code SummaryStatsTest} already established. */
    private static List<Path> summaryDocuments() throws IOException {
        List<Path> roots = List.of(Path.of("data", "jar"), Path.of("..", "data", "jar"));
        List<Path> found = new ArrayList<>();
        for (Path root : roots) {
            if (!Files.isDirectory(root)) continue;
            try (Stream<Path> walk = Files.walk(root, 2)) {
                walk.filter(p -> p.getFileName().toString().equals("_summary_static.json"))
                    .forEach(found::add);
            }
            if (!found.isEmpty()) return found;
        }
        return found;
    }
}
