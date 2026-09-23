package com.jaranalyzer.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.plsqlanalyzer.web.page.FacetPageResponder;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * UI-003 (§134) — {@code CLASS_TREE}'s new {@code sourceJar} column filter, and the sentinel that
 * makes filtering for main-app classes (empty {@code sourceJar}) actually work.
 *
 * <p>Caught mid-implementation, not assumed: {@code FacetPageResponder.columnFilters} splits a
 * {@code cf} entry's value on {@code |} and then drops any piece that is itself empty, so a naive
 * {@code cf=1:} (asking for {@code sourceJar == ""}) ends up with an empty allowed-set and the
 * whole filter clause is silently skipped — every class comes back, not just the main app's. This
 * proves the sentinel-based fix actually narrows the result, not just that the endpoint runs.
 */
class JarFacetSpecsMainJarFilterTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final FacetPageResponder RESPONDER = new FacetPageResponder(MAPPER, 10000);

    private static JsonNode cls(String simpleName, String pkg, String sourceJar) throws Exception {
        String jarField = sourceJar == null ? "" : ",\"sourceJar\":\"" + sourceJar + "\"";
        return MAPPER.readTree("{\"simpleName\":\"" + simpleName + "\",\"packageName\":\"" + pkg
                + "\",\"fullyQualifiedName\":\"" + pkg + "." + simpleName + "\"" + jarField + "}");
    }

    @Test
    void filteringByTheMainJarSentinelReturnsOnlyMainAppClasses() throws Exception {
        ArrayNode classes = MAPPER.createArrayNode();
        classes.add(cls("OrderService", "com.acme.service", null));
        classes.add(cls("PaymentService", "com.acme.service", null));
        classes.add(cls("StringUtils", "org.apache.commons.lang3", "commons-lang3-3.12.jar"));

        var response = RESPONDER.respond(classes, null, JarFacetSpecs.CLASS_TREE,
                new FacetPageResponder.Params(50, null, null, null, null,
                        List.of("1:" + JarFacetSpecs.MAIN_JAR_SENTINEL), null),
                null);

        JsonNode body = response.getBody();
        ArrayNode items = (ArrayNode) body.get("items");
        assertEquals(2, items.size(), "only the two main-app classes must survive the filter");
    }

    /**
     * The literal defect this whole sentinel exists to avoid: proves {@code cf=1:} (the empty
     * value a naive implementation would send for "main app") does NOT narrow anything — it must
     * return every class, confirming why the sentinel is required rather than the empty string.
     */
    @Test
    void filteringByAnEmptyValueMatchesEverythingNotJustMainApp() throws Exception {
        ArrayNode classes = MAPPER.createArrayNode();
        classes.add(cls("OrderService", "com.acme.service", null));
        classes.add(cls("StringUtils", "org.apache.commons.lang3", "commons-lang3-3.12.jar"));

        var response = RESPONDER.respond(classes, null, JarFacetSpecs.CLASS_TREE,
                new FacetPageResponder.Params(50, null, null, null, null, List.of("1:"), null),
                null);

        ArrayNode items = (ArrayNode) response.getBody().get("items");
        assertEquals(2, items.size(), "an empty cf value is dropped by columnFilters, not applied");
    }

    @Test
    void filteringByPackageNameNarrowsToThatPackageAlone() throws Exception {
        ArrayNode classes = MAPPER.createArrayNode();
        classes.add(cls("OrderService", "com.acme.service", null));
        classes.add(cls("PaymentService", "com.acme.service", null));
        classes.add(cls("OrderController", "com.acme.web", null));

        var response = RESPONDER.respond(classes, null, JarFacetSpecs.CLASS_TREE,
                new FacetPageResponder.Params(50, null, null, null, null,
                        List.of("0:com.acme.service"), null),
                null);

        ArrayNode items = (ArrayNode) response.getBody().get("items");
        assertEquals(2, items.size(), "only com.acme.service's own two classes must survive");
    }
}
