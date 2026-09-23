package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * UI-026e — {@link EndpointCrossReportBuilder}, checked against
 * {@code jar/js/summary-analyzer.js}'s five whole-{@code epReports} builders.
 */
class EndpointCrossReportBuilderTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private JsonNode report(String json) throws Exception {
        return mapper.readTree(json);
    }

    // ==================== _buildDistributedTransactions ====================

    @Test
    void crossDomainWritesRequireATransaction() throws Exception {
        JsonNode r = report("""
            {"endpointName": "C.m", "procName": "P", "allProcNames": ["P"], "httpMethod": "POST",
             "fullPath": "/x", "typeOfEndpoint": "REST", "primaryDomain": "Orders",
             "totalCollections": 2, "totalLoc": 10, "performanceImplication": "Low Impact",
             "collections": {
               "ORD": {"domain": "Orders", "operations": ["WRITE"]},
               "BILL": {"domain": "Billing", "operations": ["UPDATE"]}
             }}""");
        ArrayNode out = EndpointCrossReportBuilder.buildDistributedTransactions(mapper, List.of(r));
        assertEquals(1, out.size());
        String txn = out.get(0).path("transactionRequirement").asText();
        assertTrue(txn.startsWith("REQUIRED - cross-domain writes detected"), txn);
        assertTrue(txn.contains("Orders") && txn.contains("Billing"), txn);
    }

    @Test
    void aSingleWriteDomainNeedsNoCrossDomainTransaction() throws Exception {
        JsonNode r = report("""
            {"endpointName": "C.m", "collections": {
               "ORD": {"domain": "Orders", "operations": ["WRITE"]},
               "ORD2": {"domain": "Orders", "operations": ["READ"]}
             }}""");
        ArrayNode out = EndpointCrossReportBuilder.buildDistributedTransactions(mapper, List.of(r));
        assertEquals("SINGLE-DOMAIN", out.get(0).path("transactionRequirement").asText());
    }

    @Test
    void noWritesAtAllNeedsNoTransaction() throws Exception {
        JsonNode r = report("""
            {"endpointName": "C.m", "collections": {"ORD": {"domain": "Orders", "operations": ["READ"]}}}""");
        ArrayNode out = EndpointCrossReportBuilder.buildDistributedTransactions(mapper, List.of(r));
        assertEquals("NOT REQUIRED", out.get(0).path("transactionRequirement").asText());
    }

    // ==================== _buildBatchAnalysis ====================

    @Test
    void onlyEndpointsMatchingABatchKeywordAreIncluded() throws Exception {
        JsonNode batch = report("""
            {"endpointName": "C.runBatchJob", "fullPath": "/x", "collections": {}}""");
        JsonNode normal = report("""
            {"endpointName": "C.getOrder", "fullPath": "/y", "collections": {}}""");
        ArrayNode out = EndpointCrossReportBuilder.buildBatchAnalysis(
                mapper, List.of(batch, normal), List.of("batch"));
        assertEquals(1, out.size());
        assertEquals("C.runBatchJob", out.get(0).path("batchName").asText());
    }

    // ==================== _buildVerticalisation ====================

    @Test
    void aCollectionTouchedByTwoEndpointsAccumulatesBoth() throws Exception {
        JsonNode r1 = report("""
            {"endpointName": "A.x", "procName": "PA", "primaryDomain": "Orders",
             "collBreadcrumbs": {},
             "collections": {"ORD": {"type": "COLLECTION", "domain": "Orders", "operations": ["READ"]}}}""");
        JsonNode r2 = report("""
            {"endpointName": "B.y", "procName": "PB", "primaryDomain": "Orders",
             "collBreadcrumbs": {},
             "collections": {"ORD": {"type": "COLLECTION", "domain": "Orders", "operations": ["WRITE"]}}}""");
        ArrayNode out = EndpointCrossReportBuilder.buildVerticalisation(mapper, List.of(r1, r2));
        assertEquals(1, out.size());
        JsonNode ord = out.get(0);
        assertEquals(2, ord.path("usageCount").asInt());
        assertEquals(2, ord.path("endpoints").size());
        assertTrue(ord.path("readOps").size() >= 1);
        assertTrue(ord.path("writeOps").size() >= 1);
    }

    @Test
    void verificationUpgradesFromNotInDbToVerifiedButNeverDowngrades() throws Exception {
        JsonNode r1 = report("""
            {"endpointName": "A.x", "collBreadcrumbs": {},
             "collections": {"ORD": {"type": "COLLECTION", "domain": "Orders", "operations": [],
                                      "verification": "NOT_IN_DB"}}}""");
        JsonNode r2 = report("""
            {"endpointName": "B.y", "collBreadcrumbs": {},
             "collections": {"ORD": {"type": "COLLECTION", "domain": "Orders", "operations": [],
                                      "verification": "VERIFIED"}}}""");
        ArrayNode out = EndpointCrossReportBuilder.buildVerticalisation(mapper, List.of(r1, r2));
        assertEquals("VERIFIED", out.get(0).path("verification").asText());

        // Reversed order: VERIFIED first, then NOT_IN_DB — must NOT downgrade.
        ArrayNode out2 = EndpointCrossReportBuilder.buildVerticalisation(mapper, List.of(r2, r1));
        assertEquals("VERIFIED", out2.get(0).path("verification").asText());
    }

    @Test
    void sortOrderIsDomainThenViewsLastThenName() throws Exception {
        JsonNode r = report("""
            {"endpointName": "A.x", "collBreadcrumbs": {}, "collections": {
               "Z_VIEW": {"type": "VIEW", "domain": "Alpha", "operations": []},
               "A_COLL": {"type": "COLLECTION", "domain": "Alpha", "operations": []},
               "B_COLL": {"type": "COLLECTION", "domain": "Beta", "operations": []}
             }}""");
        ArrayNode out = EndpointCrossReportBuilder.buildVerticalisation(mapper, List.of(r));
        assertEquals(List.of("A_COLL", "Z_VIEW", "B_COLL"),
                List.of(out.get(0).path("name").asText(), out.get(1).path("name").asText(),
                        out.get(2).path("name").asText()));
    }

    // ==================== _buildViewsAnalysis / _calcComplexity ====================

    @Test
    void withNoComplexityRulesUsageCountBucketsIntoLowMediumHigh() throws Exception {
        ArrayNode vert = mapper.createArrayNode();
        ObjectNode view = mapper.createObjectNode();
        view.put("name", "V1");
        view.put("type", "VIEW");
        view.put("domain", "Orders");
        view.putArray("operations").add("READ");
        view.putArray("endpoints").add("A.x");
        view.put("usageCount", 6);
        view.putArray("detectedVia");
        view.putArray("readOps");
        view.putArray("writeOps");
        view.putArray("domainSet");
        vert.add(view);

        ArrayNode out = EndpointCrossReportBuilder.buildViewsAnalysis(mapper, vert, null);
        assertEquals(1, out.size());
        assertEquals("High", out.get(0).path("complexity").asText()); // usageCount 6 > 5
        assertEquals("V1", out.get(0).path("viewName").asText());
        assertEquals("", out.get(0).path("possibleAlternative").asText()); // usageCount > 1
    }

    @Test
    void possibleAlternativeIsSuggestedForASingleUseView() throws Exception {
        ArrayNode vert = mapper.createArrayNode();
        ObjectNode view = mapper.createObjectNode();
        view.put("name", "V1");
        view.put("type", "VIEW");
        view.put("domain", "Orders");
        view.putArray("operations");
        view.putArray("endpoints");
        view.put("usageCount", 1);
        view.putArray("detectedVia");
        view.putArray("readOps");
        view.putArray("writeOps");
        view.putArray("domainSet");
        vert.add(view);

        ArrayNode out = EndpointCrossReportBuilder.buildViewsAnalysis(mapper, vert, null);
        assertEquals("Evaluate: direct query on base collection may suffice",
                out.get(0).path("possibleAlternative").asText());
    }

    @Test
    void withConfiguredRulesTheWeightedScoreDrivesTheLabel() throws Exception {
        ArrayNode vert = mapper.createArrayNode();
        ObjectNode view = mapper.createObjectNode();
        view.put("name", "V1");
        view.put("type", "VIEW");
        view.put("domain", "Orders");
        view.putArray("operations");
        view.putArray("endpoints").add("A").add("B"); // 2 endpoints
        view.put("usageCount", 0);
        view.putArray("detectedVia");
        view.putArray("readOps");
        view.putArray("writeOps");
        view.putArray("domainSet");
        vert.add(view);

        var rules = new EndpointCrossReportBuilder.ComplexityRules(
                List.of(new EndpointCrossReportBuilder.ComplexityRules.Factor("endpoints", 1.0)),
                4, 10);
        ArrayNode out = EndpointCrossReportBuilder.buildViewsAnalysis(mapper, vert, rules);
        // score = 2 endpoints * 1.0 weight = 2.0, <= lowMax(4) => Low
        assertEquals("Low", out.get(0).path("complexity").asText());
    }

    // ==================== _buildExternalDeps ====================

    @Test
    void externalCallsAreGroupedByJarAndSortedByCountDescending() throws Exception {
        JsonNode r = report("""
            {"endpointName": "A.x", "externalCalls": [
               {"sourceJar": "payments.jar", "simpleClassName": "PayClient", "methodName": "charge"},
               {"sourceJar": "billing.jar", "simpleClassName": "BillClient", "methodName": "invoice"},
               {"sourceJar": "billing.jar", "simpleClassName": "BillClient", "methodName": "refund"}
             ]}""");
        SummaryStats.Config cfg = new SummaryStats.Config(List.of(), Map.of(), List.of(), List.of(), List.of());
        ObjectNode out = EndpointCrossReportBuilder.buildExternalDeps(
                mapper, List.of(r), cfg, new LinkedHashMap<>(), null);
        ArrayNode crossModule = (ArrayNode) out.get("crossModule");
        assertEquals(2, crossModule.size());
        assertEquals("billing.jar", crossModule.get(0).path("jar").asText()); // count 2, sorted first
        assertEquals(2, crossModule.get(0).path("count").asInt());
        assertEquals("payments.jar", crossModule.get(1).path("jar").asText());
    }

    @Test
    void anEndpointWithNoExternalCallsContributesNothing() throws Exception {
        JsonNode r = report("""
            {"endpointName": "A.x", "externalCalls": []}""");
        SummaryStats.Config cfg = new SummaryStats.Config(List.of(), Map.of(), List.of(), List.of(), List.of());
        ObjectNode out = EndpointCrossReportBuilder.buildExternalDeps(
                mapper, List.of(r), cfg, new LinkedHashMap<>(), null);
        assertTrue(out.path("crossModule").isEmpty());
    }
}
