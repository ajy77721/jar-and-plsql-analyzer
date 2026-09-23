package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * PLAN-08 §6.1 #7 — the ten Summary headline aggregates move to the server.
 *
 * <p><b>What is actually at risk here.</b> Not performance: the computation is one streaming pass
 * either way. The risk is that a number changes. Ten chips sit at the top of the Summary tab and a
 * reader has no way to tell "46 collections" from "45 collections" without redoing the analysis by
 * hand, so a transcription slip is invisible in review, invisible in the UI, and permanent. Every
 * test below therefore pins a <em>rule</em> of the JavaScript rather than a number of the fixture,
 * and the rules chosen are the ones that look like accidents and would be "cleaned up" by someone
 * reimplementing instead of transcribing:
 *
 * <ul>
 *   <li>an empty {@code modules} array means {@code ["main"]}, not zero modules</li>
 *   <li>{@code modules} counts munged project <em>labels</em>, so two JARs can be one module</li>
 *   <li>{@code "Other"} is excluded from Domains but its collection still counts in Collections</li>
 *   <li>a domain already on the endpoint wins over prefix detection</li>
 *   <li>batch keywords match a lower-cased haystack case-sensitively</li>
 *   <li>{@code scheduledCount} wins over {@code scheduledJobs.length}, and 0 means "fall back"</li>
 * </ul>
 *
 * <p>The last test is the different one: it re-derives all ten numbers from the fully materialised
 * document with a deliberately naive implementation and asserts the streaming pass agrees. That is
 * the check that catches a parser bug — a {@code skipChildren()} that consumes one token too many
 * leaves the counts plausible and the shape intact.
 */
@DisplayName("Summary headline aggregates (PLAN-08 §6.1 #7)")
class SummaryStatsTest {

    private final ObjectMapper mapper = new ObjectMapper();

    /** The production config's rules, reduced to the parts the ten chips depend on. */
    private static final SummaryStats.Config CFG = new SummaryStats.Config(
            List.of(new String[]{"CLM_", "Claims"}, new String[]{"UWPL_", "Underwriting"},
                    new String[]{"AC_", "Accounting"}),
            Map.of("SB_UWPL_", "Underwriting"),
            List.of("_V_", "_VI_"), List.of("SB_"),
            List.of("batch", "job", "schedul"));

    private SummaryStats compute(String json) throws IOException {
        try (var parser = mapper.getFactory().createParser(json)) {
            return SummaryStats.compute(parser, CFG, mapper);
        }
    }

    // ── the chips, one rule at a time ────────────────────────────────────────

    @Test
    @DisplayName("Endpoints counts the array, Collections and Views the distinct names across it")
    void collectionsAndViewsAreDistinctAcrossEndpoints() throws IOException {
        SummaryStats s = compute("""
            {"jarName":"svc-1.0.0.jar","endpoints":[
              {"aggregatedCollections":["CLM_CLAIM","CLM_CLAIM_V_OPEN"],
               "aggregatedCollectionDomains":{"CLM_CLAIM":"Claims","CLM_CLAIM_V_OPEN":"Claims"}},
              {"aggregatedCollections":["CLM_CLAIM","SB_UWPL_POLICY"],
               "aggregatedCollectionDomains":{"CLM_CLAIM":"Claims","SB_UWPL_POLICY":"Underwriting"}}
            ]}""");

        assertEquals(2, s.endpoints());
        assertEquals(3, s.collections(), "CLM_CLAIM appears on both endpoints and counts once");
        // _V_ by `contains`, SB_ by `starts_with` — both view-detection arms, and neither is a domain rule.
        assertEquals(2, s.views());
        assertEquals(2, s.domains());
    }

    @Test
    @DisplayName("a collection whose domain is Other still counts as a collection")
    void otherDomainCollectionsAreNotDropped() throws IOException {
        SummaryStats s = compute("""
            {"jarName":"svc.jar","endpoints":[
              {"aggregatedCollections":["ZZ_UNKNOWN","CLM_CLAIM"]}
            ]}""");

        // `_addColl` drops an Other-domain collection only when no domain was forced, and this
        // branch always forces one — `_detectDomain` returns the truthy string "Other".
        assertEquals(2, s.collections());
        assertEquals(1, s.domains(), "Other is excluded from the Domains chip, but not from Collections");
    }

    @Test
    @DisplayName("a domain carried by the analysis wins over prefix detection")
    void carriedDomainWinsOverDetection() throws IOException {
        SummaryStats detected = compute("""
            {"endpoints":[{"aggregatedCollections":["CLM_CLAIM"]}]}""");
        SummaryStats carried = compute("""
            {"endpoints":[{"aggregatedCollections":["CLM_CLAIM"],
                           "aggregatedCollectionDomains":{"CLM_CLAIM":"Reinsurance"}}]}""");

        assertEquals(1, detected.domains());
        assertEquals(1, carried.domains());
        // Both are 1, so the count alone cannot tell them apart — assert on the rule directly.
        assertEquals("Claims", SummaryStats.detectDomain("CLM_CLAIM", CFG));
    }

    @Test
    @DisplayName("an empty or absent modules array means ['main'], not zero modules")
    void absentModulesMeansMain() throws IOException {
        SummaryStats s = compute("""
            {"jarName":"web-1.0.0-SNAPSHOT.jar","endpoints":[
              {"modules":[]},
              {},
              {"modules":["main","service-1.0.0-SNAPSHOT.jar"]}
            ]}""");

        // "main" resolves to the uploaded JAR's label, so it is the same module as the first two.
        assertEquals(2, s.modules());
    }

    @Test
    @DisplayName("Modules counts munged labels, so two JAR names can be one module")
    void modulesCountsLabelsNotJarNames() throws IOException {
        SummaryStats s = compute("""
            {"jarName":"svc.jar","endpoints":[
              {"modules":["service-1.0.0-SNAPSHOT.jar","service-2.4.jar"]}
            ]}""");

        assertEquals(1, s.modules(), "both munge to 'Service' — _jarToProject strips the version");
        assertEquals("Service", SummaryStats.jarToProject("service-1.0.0-SNAPSHOT.jar", "svc.jar"));
        assertEquals("Web", SummaryStats.jarToProject("main", "web-1.0.0-SNAPSHOT.jar"));
        assertEquals("Primary", SummaryStats.jarToProject("main", null),
                "no uploaded name: the browser's literal fallback");
        assertEquals("Jar Analyzer Core",
                SummaryStats.jarToProject("jar-analyzer-core-1.0.0-SNAPSHOT.jar", null));
    }

    @Test
    @DisplayName("Txn Req counts endpoints writing across more than one domain")
    void txnNeedsTwoWrittenDomains() throws IOException {
        SummaryStats s = compute("""
            {"endpoints":[
              {"aggregatedCollections":["CLM_A","AC_B"],
               "aggregatedCollectionDomains":{"CLM_A":"Claims","AC_B":"Accounting"},
               "aggregatedOperations":{"CLM_A":"WRITE","AC_B":"UPDATE"}},
              {"aggregatedCollections":["CLM_A","AC_B"],
               "aggregatedCollectionDomains":{"CLM_A":"Claims","AC_B":"Accounting"},
               "aggregatedOperations":{"CLM_A":"WRITE","AC_B":"READ"}},
              {"aggregatedCollections":["CLM_A","AC_B"],
               "aggregatedCollectionDomains":{"CLM_A":"Claims","AC_B":"Accounting"},
               "derivedOperationType":"DELETE"}
            ]}""");

        // 1st: two written domains. 2nd: one written domain, a read does not count.
        // 3rd: no per-collection operation, so `derivedOperationType` applies to both — two domains.
        assertEquals(2, s.txn());
    }

    @Test
    @DisplayName("an operation defaults to READ, which is never a write")
    void missingOperationDefaultsToRead() throws IOException {
        SummaryStats s = compute("""
            {"endpoints":[{"aggregatedCollections":["CLM_A","AC_B"],
                           "aggregatedCollectionDomains":{"CLM_A":"Claims","AC_B":"Accounting"}}]}""");
        assertEquals(0, s.txn());
    }

    @Test
    @DisplayName("Batch matches keywords against a lower-cased path and handler name")
    void batchKeywordsMatchLowerCased() throws IOException {
        SummaryStats s = compute("""
            {"endpoints":[
              {"fullPath":"/api/BATCH/run","controllerSimpleName":"C","methodName":"m"},
              {"fullPath":"/api/x","controllerSimpleName":"JobController","methodName":"m"},
              {"fullPath":"/api/x","controllerSimpleName":"C","methodName":"scheduleNightly"},
              {"fullPath":"/api/orders","controllerSimpleName":"OrderController","methodName":"list"}
            ]}""");

        // 'batch' matches only because the haystack is lower-cased; 'schedul' is the truncated
        // keyword the config actually ships, and it has to match "scheduleNightly".
        assertEquals(3, s.batch());
    }

    @Test
    @DisplayName("Cross-Module counts endpoints with any out-of-module call, not the calls")
    void crossModuleCountsEndpointsAndLocSums() throws IOException {
        SummaryStats s = compute("""
            {"endpoints":[
              {"externalScopeCalls":17,"totalLoc":100},
              {"externalScopeCalls":1,"totalLoc":20},
              {"externalScopeCalls":0,"totalLoc":3},
              {"totalLoc":7}
            ]}""");

        assertEquals(2, s.crossModule());
        assertEquals(130, s.loc());
    }

    @Test
    @DisplayName("scheduledCount wins; 0 falls back to scheduledJobs.length, whatever the field order")
    void scheduledFallsBackToTheArray() throws IOException {
        assertEquals(3, compute("""
            {"scheduledCount":3,"scheduledJobs":[{"a":1}],"endpoints":[]}""").scheduled());
        assertEquals(2, compute("""
            {"scheduledCount":0,"scheduledJobs":[{"a":1},{"b":[1,2]}],"endpoints":[]}""").scheduled());
        assertEquals(2, compute("""
            {"scheduledJobs":[{"a":1},{"b":2}],"endpoints":[],"scheduledCount":0}""").scheduled(),
                "the array may arrive before the scalar");
        assertEquals(0, compute("{\"endpoints\":[]}").scheduled());
    }

    @Test
    @DisplayName("the endpoint list may arrive before jarName, and Modules must not change")
    void jarNameMayFollowTheEndpoints() throws IOException {
        SummaryStats before = compute("""
            {"jarName":"web-1.0.0-SNAPSHOT.jar","endpoints":[{"modules":["main"]}]}""");
        SummaryStats after = compute("""
            {"endpoints":[{"modules":["main"]}],"jarName":"web-1.0.0-SNAPSHOT.jar"}""");

        assertEquals(before, after);
        assertEquals(1, after.modules());
    }

    @Test
    @DisplayName("nested objects in skipped fields do not derail the pass")
    void unrelatedFieldsAreSkippedWholesale() throws IOException {
        SummaryStats s = compute("""
            {"classIndex":[{"annotations":[{"attributes":{"endpoints":["decoy"]}}]}],
             "entityCollMap":{"endpoints":"decoy"},
             "endpoints":[{"aggregatedCollections":["CLM_A"]}],
             "repoCollMap":{"a":{"b":[1,2,3]}}}""");

        assertEquals(1, s.endpoints(), "a field named 'endpoints' nested inside another is not the list");
        assertEquals(1, s.collections());
    }

    @Test
    @DisplayName("chip keys are the ones the topbar reads")
    void chipKeysMatchTheUi() {
        assertEquals(List.of("endpoints", "collections", "views", "modules", "domains",
                        "crossmodule", "loc", "txn", "batch", "scheduled"),
                List.copyOf(new SummaryStats(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).asChipMap().keySet()));
    }

    // ── the streaming pass against a naive one, on the real measured analyses ─

    @Test
    @DisplayName("on every real analysis in data/jar, the streamed pass equals a naive one")
    void streamedPassAgreesWithANaivePassOnRealData() throws IOException {
        List<Path> docs = summaryDocuments();
        assumeTrue(!docs.isEmpty(), "no analyses in data/jar — nothing to compare against");

        for (Path doc : docs) {
            SummaryStats streamed;
            try (var parser = mapper.getFactory().createParser(Files.newInputStream(doc))) {
                streamed = SummaryStats.compute(parser, CFG, mapper);
            }
            assertEquals(naive(mapper.readTree(doc.toFile())), streamed,
                    "streamed and naive disagree on " + doc);
        }
    }

    /**
     * The same ten numbers, from the whole document in memory.
     *
     * <p>Independent of the streaming pass on purpose. It may materialise the document because it
     * is a test over a 430 KB fixture, not a request path — the production code may not, which is
     * precisely the difference this test exists to make safe.
     */
    private static SummaryStats naive(JsonNode doc) {
        Set<String> colls = new HashSet<>(), views = new HashSet<>(),
                    domains = new HashSet<>(), rawModules = new HashSet<>();
        long endpoints = 0, cross = 0, loc = 0, txn = 0, batch = 0;
        for (JsonNode ep : doc.path("endpoints")) {
            endpoints++;
            loc += ep.path("totalLoc").asLong(0);
            if (ep.path("externalScopeCalls").asLong(0) > 0) cross++;
            JsonNode mods = ep.path("modules");
            if (mods.isArray() && !mods.isEmpty()) {
                for (JsonNode m : mods) rawModules.add(m.asText(""));
            } else {
                rawModules.add("main");
            }
            Set<String> writeDomains = new HashSet<>();
            for (JsonNode c : ep.path("aggregatedCollections")) {
                String name = c.asText("");
                if (name.isEmpty()) continue;
                colls.add(name);
                if (SummaryStats.isView(name, CFG)) views.add(name);
                String domain = ep.path("aggregatedCollectionDomains").path(name).asText("");
                if (domain.isEmpty()) domain = SummaryStats.detectDomain(name, CFG);
                if (!domain.isEmpty() && !"Other".equals(domain)) domains.add(domain);
                String op = ep.path("aggregatedOperations").path(name).asText("");
                if (op.isEmpty()) op = ep.path("derivedOperationType").asText("");
                if (op.isEmpty()) op = "READ";
                if (Set.of("WRITE", "UPDATE", "DELETE").contains(op)) writeDomains.add(domain);
            }
            if (writeDomains.size() > 1) txn++;
            String hay = (ep.path("fullPath").asText("") + ' '
                    + ep.path("controllerSimpleName").asText("") + '.'
                    + ep.path("methodName").asText("")).toLowerCase(Locale.ROOT);
            for (String kw : CFG.batchKeywords()) {
                if (hay.contains(kw)) { batch++; break; }
            }
        }
        Set<String> labels = new HashSet<>();
        String jarName = doc.path("jarName").isMissingNode() ? null : doc.path("jarName").asText("");
        for (String raw : rawModules) labels.add(SummaryStats.jarToProject(raw, jarName));

        long scheduled = doc.path("scheduledCount").asLong(0);
        if (scheduled == 0) scheduled = doc.path("scheduledJobs").size();
        return new SummaryStats(endpoints, colls.size(), views.size(), labels.size(),
                domains.size(), cross, loc, txn, batch, scheduled);
    }

    /** Every {@code _summary_static.json} under {@code data/jar}, from either module's cwd. */
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
