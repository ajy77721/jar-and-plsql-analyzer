package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * UI-026a — {@link EndpointReportBuilder}, checked against
 * {@code jar/js/summary-analyzer.js:_analyzeEndpoint}'s no-call-tree branch, and against a real
 * endpoint from {@code data/jar/unified-web-1.0.0-SNAPSHOT/_summary_static.json} (not just a
 * synthetic fixture) so the field shape is proven against what the summary writer actually
 * produces, not an assumed one.
 */
class EndpointReportBuilderTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private SummaryStats.Config cfg(String... prefixDomainPairs) {
        var pairs = new java.util.ArrayList<String[]>();
        for (int i = 0; i < prefixDomainPairs.length; i += 2) {
            pairs.add(new String[]{prefixDomainPairs[i], prefixDomainPairs[i + 1]});
        }
        pairs.sort((a, b) -> b[0].length() - a[0].length());
        return new SummaryStats.Config(pairs, Map.of(), List.of("_view"), List.of("v_"), List.of("batch", "export"));
    }

    private ObjectNode ep(String json) throws Exception {
        return (ObjectNode) mapper.readTree(json);
    }

    @Test
    void realEndpointFromTheSummaryDocumentProducesTheExpectedClassification() throws Exception {
        // The exact shape of a real endpoint in data/jar/unified-web-1.0.0-SNAPSHOT/_summary_static.json
        // that has aggregatedCollections — checked by reading the file directly, not invented.
        JsonNode ep = mapper.readTree("""
            {
              "fullPath": "/api/war/wars", "controllerSimpleName": "WarAnalyzerController",
              "methodName": "uploadAndAnalyze", "httpMethod": "POST",
              "aggregatedCollections": ["all_source", "all_views", "all_procedures",
                "information_schema.ROUTINES", "sys.objects", "DBMS_METADATA.SET_TRANSFORM_PARAM"],
              "aggregatedCollectionDomains": {"all_source": "Generic", "all_views": "Generic",
                "all_procedures": "Generic", "information_schema.ROUTINES": "Other",
                "sys.objects": "Other", "DBMS_METADATA.SET_TRANSFORM_PARAM": "Other"},
              "aggregatedOperations": {"all_source": "READ", "all_views": "READ",
                "all_procedures": "READ", "information_schema.ROUTINES": "READ",
                "sys.objects": "READ", "DBMS_METADATA.SET_TRANSFORM_PARAM": "CALL"},
              "derivedOperationType": "CALL", "totalMethods": 4927, "totalLoc": 100484,
              "totalDbOperations": 19, "procNames": [],
              "beans": [{"className": "com.waranalyzer.controller.WarAnalyzerController",
                "simpleClassName": "WarAnalyzerController", "stereotype": "REST_CONTROLLER",
                "sourceJar": ""}]
            }""");

        ObjectNode report = EndpointReportBuilder.build(mapper, ep, cfg(), Map.of(), List.of());

        assertEquals("WarAnalyzerController.uploadAndAnalyze", report.path("endpointName").asText());
        assertEquals(6, report.path("totalCollections").asInt());
        assertEquals(4927, report.path("totalMethods").asLong());
        assertEquals(19, report.path("totalDbOperations").asLong());

        // DBMS_METADATA... is the only CALL op — the only WRITE_OPS member present.
        var write = report.path("writeCollections");
        assertEquals(1, write.size());
        assertEquals("DBMS_METADATA.SET_TRANSFORM_PARAM", write.get(0).asText());

        // The other five are READ.
        var read = report.path("readCollections");
        assertEquals(5, read.size());

        // Domains came from the map, not detectDomain — "Generic" isn't in cfg()'s prefix list at all.
        assertEquals("Generic", report.path("collections").path("all_source").path("domain").asText());
    }

    @Test
    void unmappedOperationClassifiesIntoNeitherWriteNorRead() throws Exception {
        // A collection whose op is absent from aggregatedOperations gets defaulted to READ inside
        // _addColl, but the write/read split reads the RAW map value — so it lands in neither list.
        // This is the exact subtlety the first draft of this port got wrong.
        ObjectNode e = ep("""
            {"aggregatedCollections": ["orphan"], "aggregatedCollectionDomains": {},
             "aggregatedOperations": {}, "controllerSimpleName": "C", "methodName": "m"}""");
        ObjectNode report = EndpointReportBuilder.build(mapper, e, cfg(), Map.of(), List.of());

        assertEquals(1, report.path("totalCollections").asInt(), "the collection itself is still recorded");
        assertTrue(report.path("writeCollections").isEmpty(), "no raw op means not classified as a write");
        assertTrue(report.path("readCollections").isEmpty(), "no raw op means not classified as a read either");
    }

    @Test
    void domainFallsBackToPrefixDetectionWhenTheMapHasNoEntry() throws Exception {
        ObjectNode e = ep("""
            {"aggregatedCollections": ["WIP_ORDERS"], "aggregatedCollectionDomains": {},
             "aggregatedOperations": {"WIP_ORDERS": "READ"},
             "controllerSimpleName": "C", "methodName": "m"}""");
        ObjectNode report = EndpointReportBuilder.build(mapper, e, cfg("WIP_", "Sales"), Map.of(), List.of());
        assertEquals("Sales", report.path("collections").path("WIP_ORDERS").path("domain").asText());
    }

    @Test
    void viewDetectionMatchesTheConfiguredContainsAndStartsWithRules() throws Exception {
        ObjectNode e = ep("""
            {"aggregatedCollections": ["orders_view", "v_customers", "orders"],
             "aggregatedCollectionDomains": {}, "aggregatedOperations": {},
             "controllerSimpleName": "C", "methodName": "m"}""");
        ObjectNode report = EndpointReportBuilder.build(mapper, e, cfg(), Map.of(), List.of());
        var views = report.path("viewsUsed");
        var seen = new java.util.HashSet<String>();
        views.forEach(v -> seen.add(v.asText()));
        assertTrue(seen.contains("orders_view"), "matches the 'contains' rule (_view)");
        assertTrue(seen.contains("v_customers"), "matches the 'starts_with' rule (v_)");
        assertFalse(seen.contains("orders"));
        assertEquals(2, report.path("totalViews").asInt(), "both orders_view and v_customers match");
    }

    @Test
    void classificationAndScoringUseTheirOwnConfig() throws Exception {
        ObjectNode e = ep("""
            {"fullPath": "/api/batch/export", "methodName": "run",
             "controllerSimpleName": "C", "totalMethods": 25, "totalDbOperations": 3,
             "aggregatedCollections": [], "aggregatedCollectionDomains": {}, "aggregatedOperations": {}}""");
        var perfThresholds = List.<Object>of(
                Map.of("maxDbOps", 1, "label", "Low"),
                Map.of("maxDbOps", 5, "label", "Medium"));
        ObjectNode report = EndpointReportBuilder.build(mapper, e, cfg(), Map.of("S", 5, "M", 20, "L", 50),
                perfThresholds);
        assertEquals("BATCH", report.path("typeOfEndpoint").asText(), "'batch' keyword in the path");
        assertEquals("L", report.path("sizeCategory").asText(), "25 methods: > M(20), <= L(50)");
        assertEquals("Medium", report.path("performanceImplication").asText(), "3 ops: > Low(1), <= Medium(5)");
    }

    @Test
    void procNamesAreExtractedWithFirstWriteWinsSources() throws Exception {
        ObjectNode e = ep("""
            {"controllerSimpleName": "C", "methodName": "m",
             "aggregatedCollections": [], "aggregatedCollectionDomains": {}, "aggregatedOperations": {},
             "procNames": [
               {"procName": "PKG.PROC_A", "simpleClassName": "S1", "methodName": "call1", "className": "com.x.S1"},
               {"procName": "PKG.PROC_A", "simpleClassName": "S2", "methodName": "call2", "className": "com.x.S2"},
               {"procName": "PKG.PROC_B", "simpleClassName": "S3", "methodName": "call3", "className": "com.x.S3"}
             ]}""");
        ObjectNode report = EndpointReportBuilder.build(mapper, e, cfg(), Map.of(), List.of());
        assertEquals("PKG.PROC_A", report.path("procName").asText(), "first proc name is the primary");
        assertEquals(3, report.path("allProcNames").size(), "every occurrence is kept, including duplicates");
        assertEquals("S1", report.path("procNameSources").path("PKG.PROC_A").path("simpleClassName").asText(),
                "first source wins for a repeated proc name");
    }

    @Test
    void collDomainGroupsGroupByEachCollectionsOwnResolvedDomain() throws Exception {
        ObjectNode e = ep("""
            {"controllerSimpleName": "C", "methodName": "m",
             "aggregatedCollections": ["orders", "customers", "misc"],
             "aggregatedCollectionDomains": {"orders": "Sales", "customers": "Sales"},
             "aggregatedOperations": {}}""");
        ObjectNode report = EndpointReportBuilder.build(mapper, e, cfg(), Map.of(), List.of());
        var salesGroup = report.path("collDomainGroups").path("Sales");
        assertEquals(2, salesGroup.size());
        assertEquals("Other", report.path("collections").path("misc").path("domain").asText(),
                "no domain-map entry and no matching prefix falls back to Other");
    }

    @Test
    void serviceClassesFiltersBeansByStereotype() throws Exception {
        ObjectNode e = ep("""
            {"controllerSimpleName": "C", "methodName": "m",
             "aggregatedCollections": [], "aggregatedCollectionDomains": {}, "aggregatedOperations": {},
             "beans": [
               {"className": "com.x.OrderService", "simpleClassName": "OrderService", "stereotype": "SERVICE"},
               {"className": "com.x.OrderRepo", "simpleClassName": "OrderRepo", "stereotype": "REPOSITORY"}
             ]}""");
        ObjectNode report = EndpointReportBuilder.build(mapper, e, cfg(), Map.of(), List.of());
        var svc = report.path("serviceClasses");
        assertEquals(1, svc.size());
        assertEquals("OrderService", svc.get(0).asText());
    }

    @Test
    void collBreadcrumbsBuildOneSyntheticPathPerCollectionFromItsSourceMethods() throws Exception {
        ObjectNode e = ep("""
            {"controllerSimpleName": "OrderController", "controllerClass": "com.x.OrderController",
             "methodName": "placeOrder",
             "aggregatedCollections": ["orders"], "aggregatedCollectionDomains": {},
             "aggregatedOperations": {"orders": "WRITE"}}""");
        ObjectNode report = EndpointReportBuilder.build(mapper, e, cfg(), Map.of(), List.of());
        var bc = report.path("collBreadcrumbs").path("orders");
        assertEquals(1, bc.size(), "one path, wrapped in an outer array matching the JS's [segs] shape");
        var segs = bc.get(0);
        assertEquals("OrderController.placeOrder()", segs.get(0).path("label").asText());
        assertEquals(0, segs.get(0).path("level").asInt());
        // The source method comes from _addColl's `source` param, which in the no-call-tree branch
        // is always controllerSimpleName — so the second segment is the same controller again,
        // exactly as the JS produces (sourceMethodMap keyed by `source`, here the same value).
        assertEquals(2, segs.size());
    }

    @Test
    void collBreadcrumbsAreAbsentForACollectionWithNoRecordedSource() throws Exception {
        // _addColl is called with `source` populated in every no-call-tree-branch call site, but
        // the JS's own fallback (`cInfo.sources.size`) exists for when sourceMethodMap is empty —
        // pinned here so a future change to how sources are recorded doesn't silently drop this.
        ObjectNode e = ep("""
            {"controllerSimpleName": "", "methodName": "",
             "aggregatedCollections": ["orphan"], "aggregatedCollectionDomains": {},
             "aggregatedOperations": {}}""");
        ObjectNode report = EndpointReportBuilder.build(mapper, e, cfg(), Map.of(), List.of());
        assertTrue(report.path("collBreadcrumbs").path("orphan").isMissingNode(),
                "no controller and no source recorded means no breadcrumb segments at all");
    }

    @Test
    void emptyEndpointProducesAnEmptyButWellFormedReport() throws Exception {
        ObjectNode e = ep("""
            {"controllerSimpleName": "C", "methodName": "m",
             "aggregatedCollections": [], "aggregatedCollectionDomains": {}, "aggregatedOperations": {}}""");
        ObjectNode report = EndpointReportBuilder.build(mapper, e, cfg(), Map.of(), List.of());
        assertEquals("ALL", report.path("httpMethod").asText(), "missing httpMethod defaults to ALL");
        assertEquals("/", report.path("fullPath").asText(), "missing fullPath defaults to /");
        assertEquals("", report.path("procName").asText());
        assertEquals(0, report.path("totalCollections").asInt());
        assertTrue(report.path("beans").isArray());
        assertTrue(report.path("beans").isEmpty());
    }

    @Test
    void primaryDomainAndModulesFallBackToFilenameMungingWithNoMapEntry() throws Exception {
        // No jarModuleMap entry for "orders-service.jar" and no uploadedJarName for "main" —
        // both must fall back to the plain filename munge, matching _jarToProject/_jarToDomain's
        // own no-map-entry branch.
        ObjectNode e = ep("""
            {"controllerSimpleName": "C", "methodName": "m",
             "aggregatedCollections": [], "aggregatedCollectionDomains": {}, "aggregatedOperations": {},
             "modules": ["orders-service.jar"]}""");
        ObjectNode report = EndpointReportBuilder.build(mapper, e, cfg(), Map.of(), List.of(),
                new java.util.LinkedHashMap<>(), null);
        assertEquals("Orders Service", report.path("primaryDomain").asText());
        assertEquals(1, report.path("modules").size());
        assertEquals("Orders Service", report.path("modules").get(0).asText());
        assertEquals(List.of("Orders Service"),
                java.util.stream.StreamSupport.stream(report.path("domains").spliterator(), false)
                        .map(JsonNode::asText).toList());
    }

    @Test
    void jarModuleMapEntryIsPreferredOverFilenameMunging() throws Exception {
        // A pre-seeded jarModuleMap entry (as an earlier endpoint's externalCalls would have set)
        // must win over the filename munge, and is used verbatim (not re-munged) — matching
        // `if (this._jarModuleMap[jarName]) return this._jarModuleMap[jarName];`.
        ObjectNode e = ep("""
            {"controllerSimpleName": "C", "methodName": "m",
             "aggregatedCollections": [], "aggregatedCollectionDomains": {}, "aggregatedOperations": {},
             "modules": ["payments.jar"]}""");
        Map<String, String> map = new java.util.LinkedHashMap<>();
        map.put("payments.jar", "Payments-Core");
        ObjectNode report = EndpointReportBuilder.build(mapper, e, cfg(), Map.of(), List.of(), map, null);
        assertEquals("Payments-Core", report.path("modules").get(0).asText());
    }

    @Test
    void endpointsOwnExternalCallsSeedTheMapBeforeItsOwnDomainIsComputed() throws Exception {
        // The JS updates _jarModuleMap from THIS endpoint's own externalCalls before reading it
        // for THIS SAME endpoint's domain list — self-referential within one _analyzeEndpoint
        // call, not just order-dependent across endpoints. `modules: ["billing.jar"]` has no map
        // entry yet when this endpoint starts, but its own externalCalls supply one.
        ObjectNode e = ep("""
            {"controllerSimpleName": "C", "methodName": "m",
             "aggregatedCollections": [], "aggregatedCollectionDomains": {}, "aggregatedOperations": {},
             "modules": ["billing.jar"],
             "externalCalls": [{"sourceJar": "billing.jar", "module": "Billing-Ops"}]}""");
        ObjectNode report = EndpointReportBuilder.build(mapper, e, cfg(), Map.of(), List.of(),
                new java.util.LinkedHashMap<>(), null);
        assertEquals("Billing-Ops", report.path("modules").get(0).asText());
        // jarToDomain also consults the map, so it must see the same seeded entry, not the
        // filename-munge fallback ("Billing") it would fall back to without the seed.
        assertEquals("Billing Ops", report.path("primaryDomain").asText());
        assertEquals(List.of("Billing Ops"),
                java.util.stream.StreamSupport.stream(report.path("domains").spliterator(), false)
                        .map(JsonNode::asText).toList());
    }

    @Test
    void crossDomainCountExcludesThePrimaryDomainAndOther() throws Exception {
        // primaryDomain comes from `modules` ("Core", no map entry, no ignore segments, "main"
        // fallback); collDomainGroups comes from the collections' own resolved domains ("Orders",
        // "Billing", "Other"). crossDomainCount must count "Orders" and "Billing" but not "Other"
        // and not a group that happens to equal primaryDomain.
        ObjectNode e = ep("""
            {"controllerSimpleName": "C", "methodName": "m",
             "aggregatedCollections": ["ORD_X", "BILL_Y", "MYSTERY_Z"],
             "aggregatedCollectionDomains": {"ORD_X": "Orders", "BILL_Y": "Billing"},
             "aggregatedOperations": {}}""");
        ObjectNode report = EndpointReportBuilder.build(mapper, e, cfg(), Map.of(), List.of());
        assertEquals("Core", report.path("primaryDomain").asText());
        assertEquals(2, report.path("crossDomainCount").asLong());
    }
}
