package com.jaranalyzer.controller;

import com.analyzer.storage.page.Page;
import com.analyzer.storage.page.PageRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.plsqlanalyzer.web.page.JsonFacetPager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The Code Structure tab's two projections and orderings — step 13 of PLAN-08 §4.3.
 *
 * <p>Two classes of failure are worth a test here. The first is the projection quietly dropping
 * something the tree renders, which would show as an empty badge rather than an error. The second
 * is a page boundary losing or repeating a row, which only appears on page 2 of large data — so
 * these walk the whole cursor chain and compare it against the same rows sorted in one go.
 */
class JarFacetSpecsTest {

    private static final ObjectMapper M = new ObjectMapper();

    /* ── bundled JARs ────────────────────────────────────────────────────────────────────── */

    private static ObjectNode bundledJar(String name, long size, int resources,
                                         boolean manifest) {
        ObjectNode o = M.createObjectNode();
        o.put("name", name);
        o.put("size", size);
        if (manifest) o.put("manifest", "Manifest-Version: 1.0\n" + "x".repeat(2000));
        ObjectNode res = o.putObject("resources");
        for (int i = 0; i < resources; i++) res.put("file" + i + ".xml", "y".repeat(3000));
        return o;
    }

    @Test
    @DisplayName("bundledJarRow keeps the header fields and the resource names, drops the bodies")
    void bundledJarProjection() {
        ObjectNode row = JarFacetSpecs.bundledJarRow(bundledJar("a.jar", 1234, 3, true));

        assertEquals("a.jar", row.path("name").asText());
        assertEquals(1234, row.path("size").asLong());
        assertTrue(row.path("hasManifest").asBoolean());
        assertEquals(3, row.path("resourceCount").asInt());
        assertEquals(List.of("file0.xml", "file1.xml", "file2.xml"),
                names(row.path("resourceNames")));

        // The point of the whole exercise: the 2.84 MB of bodies is not in the page.
        assertFalse(row.has("manifest"));
        assertFalse(row.has("resources"));
    }

    @Test
    @DisplayName("the projection is dramatically smaller than the row it replaces")
    void projectionIsSmaller() {
        ObjectNode full = bundledJar("a.jar", 1234, 10, true);
        int before = full.toString().length();
        int after = JarFacetSpecs.bundledJarRow(full).toString().length();
        assertTrue(after * 20 < before,
                "expected at least a 20x reduction, got " + before + " -> " + after);
    }

    @Test
    @DisplayName("hasManifest is false for an absent, null or empty manifest")
    void manifestAbsence() {
        assertFalse(JarFacetSpecs.bundledJarRow(bundledJar("a.jar", 1, 0, false))
                .path("hasManifest").asBoolean());
        ObjectNode nulled = bundledJar("b.jar", 1, 0, false);
        nulled.putNull("manifest");
        assertFalse(JarFacetSpecs.bundledJarRow(nulled).path("hasManifest").asBoolean());
        ObjectNode blank = bundledJar("c.jar", 1, 0, false);
        blank.put("manifest", "");
        assertFalse(JarFacetSpecs.bundledJarRow(blank).path("hasManifest").asBoolean());
    }

    @Test
    @DisplayName("the bundled-JAR cursor chain returns every JAR exactly once, in name order")
    void bundledJarChain() {
        List<JsonNode> rows = new ArrayList<>();
        for (int i = 0; i < 47; i++) {
            rows.add(JarFacetSpecs.bundledJarRow(
                    bundledJar(String.format("lib-%02d.jar", i), i * 100L, i % 4, i % 2 == 0)));
        }
        assertChainCoversEverything(rows, JarFacetSpecs.BUNDLED_JARS, 7,
                r -> r.path("name").asText());
    }

    /* ── class tree ──────────────────────────────────────────────────────────────────────── */

    private static ObjectNode classRow(String sourceJar, String pkg, String simple) {
        ObjectNode o = M.createObjectNode();
        o.put("fullyQualifiedName", pkg + "." + simple);
        o.put("packageName", pkg);
        o.put("simpleName", simple);
        o.put("stereotype", "OTHER");
        o.put("fieldCount", 2);
        o.put("methodCount", 5);
        o.put("repositoryEntityType", "");
        if (sourceJar != null) o.put("sourceJar", sourceJar);
        o.putArray("fields").addObject().put("name", "f").put("type", "int");
        ArrayNode methods = o.putArray("methodSummaries");
        for (int i = 0; i < 5; i++) methods.addObject().put("name", "m" + i)
                .put("returnType", "void").put("padding", "z".repeat(500));
        o.putArray("annotations").addObject().put("name", "Service");
        return o;
    }

    @Test
    @DisplayName("classRow drops methodSummaries and keeps everything else, including new fields")
    void classProjection() {
        ObjectNode full = classRow("dep.jar", "com.a", "Foo");
        full.put("someFieldAddedLater", 7);
        ObjectNode row = JarFacetSpecs.classRow(full);

        assertFalse(row.has("methodSummaries"),
                "methodSummaries is 72% of the response and has no reader");
        // Copy-and-remove, not an allow-list: a field added to the streamed tree keeps flowing.
        assertEquals(7, row.path("someFieldAddedLater").asInt());
        assertTrue(row.has("fields"), "the class body renders from fields on expand");
        assertEquals(5, row.path("methodCount").asInt(),
                "the member badge needs the count even though the summaries are gone");
        assertEquals("com.a.Foo", row.path("fullyQualifiedName").asText());
        assertTrue(row.path("annotations").isArray());
    }

    @Test
    @DisplayName("PACKAGE order puts main-application classes before dependency-JAR ones")
    void mainClassesFirst() {
        List<JsonNode> rows = List.of(
                JarFacetSpecs.classRow(classRow("zzz.jar", "com.z", "Z")),
                JarFacetSpecs.classRow(classRow(null, "com.a", "A")),
                JarFacetSpecs.classRow(classRow("aaa.jar", "com.b", "B")));
        Page<JsonNode> page = page(rows, "PACKAGE", 10, null, null);
        assertEquals(List.of("com.a.A", "com.b.B", "com.z.Z"),
                page.items().stream().map(r -> r.path("fullyQualifiedName").asText()).toList());
    }

    @Test
    @DisplayName("PACKAGE order keeps a parent package's classes before a child package's")
    void parentPackageBeforeChild() {
        List<JsonNode> rows = List.of(
                JarFacetSpecs.classRow(classRow(null, "com.a.b", "Aaa")),
                JarFacetSpecs.classRow(classRow(null, "com.a", "Zzz")));
        Page<JsonNode> page = page(rows, "PACKAGE", 10, null, null);
        // The tree renders package com.a before com.a.b; the key's separator has to sort that way
        // or a page boundary would fall in the middle of a package group.
        assertEquals(List.of("com.a.Zzz", "com.a.b.Aaa"),
                page.items().stream().map(r -> r.path("fullyQualifiedName").asText()).toList());
    }

    @Test
    @DisplayName("the class-tree cursor chain returns every class exactly once, in every sort")
    void classChain() {
        List<JsonNode> rows = new ArrayList<>();
        String[] jars = {null, "a.jar", "b.jar"};
        for (int i = 0; i < 60; i++) {
            rows.add(JarFacetSpecs.classRow(classRow(jars[i % 3],
                    "com.pkg" + (i % 5), String.format("Cls%02d", i))));
        }
        assertChainCoversEverything(rows, JarFacetSpecs.CLASS_TREE, 9,
                r -> r.path("fullyQualifiedName").asText());
    }

    @Test
    @DisplayName("server-side search matches FQN, simple name and annotation name")
    void classSearch() {
        List<JsonNode> rows = List.of(
                JarFacetSpecs.classRow(classRow(null, "com.a", "OrderController")),
                JarFacetSpecs.classRow(classRow(null, "com.b", "Widget")));

        assertEquals(1, page(rows, "PACKAGE", 10, "ordercontroller", null).page().total());
        assertEquals(1, page(rows, "PACKAGE", 10, "com.b", null).page().total());
        // Both rows carry @Service, so an annotation search matches both.
        assertEquals(2, page(rows, "PACKAGE", 10, "service", null).page().total());
        assertEquals(0, page(rows, "PACKAGE", 10, "no-such-thing", null).page().total());
    }

    @Test
    @DisplayName("an unknown sort is rejected rather than silently answered in another order")
    void unknownSortRejected() {
        List<JsonNode> rows = List.of(JarFacetSpecs.classRow(classRow(null, "com.a", "A")));
        assertThrows(IllegalArgumentException.class,
                () -> page(rows, "BY_WHATEVER", 10, null, null));
    }

    /* ── helpers ─────────────────────────────────────────────────────────────────────────── */

    private static Page<JsonNode> page(List<JsonNode> rows, String sort, int limit, String q,
                                       String after) {
        var spec = JarFacetSpecs.CLASS_TREE;
        PageRequest req = PageRequest.of(limit, after, sort, q, spec.sorts().keySet(),
                spec.defaultSort(), spec.defaultLimit());
        return JsonFacetPager.page(rows.iterator(), req, spec.sorts(), spec.id(),
                spec.searchText(), null);
    }

    /**
     * Walks every page of every declared sort and asserts the union is the whole set, with no
     * duplicate — the failure mode that only shows up at a boundary.
     */
    private static void assertChainCoversEverything(
            List<JsonNode> rows, com.plsqlanalyzer.web.page.FacetSpec spec, int limit,
            java.util.function.Function<JsonNode, String> id) {
        for (String sort : spec.sorts().keySet()) {
            Set<String> seen = new LinkedHashSet<>();
            int count = 0;
            String cursor = null;
            for (int guard = 0; guard < 100; guard++) {
                PageRequest req = PageRequest.of(limit, cursor, sort, null, spec.sorts().keySet(),
                        spec.defaultSort(), spec.defaultLimit());
                Page<JsonNode> p = JsonFacetPager.page(rows.iterator(), req, spec.sorts(),
                        spec.id(), spec.searchText(), null);
                assertEquals(rows.size(), p.page().total(),
                        sort + ": total must be the whole filtered set on every page");
                for (JsonNode r : p.items()) {
                    assertTrue(seen.add(id.apply(r)), sort + ": row returned twice: " + id.apply(r));
                    count++;
                }
                if (!p.page().hasMore()) {
                    assertNull(p.page().nextCursor(), sort + ": last page must carry no cursor");
                    break;
                }
                cursor = p.page().nextCursor();
                assertNotNull(cursor, sort + ": hasMore with no cursor");
            }
            assertEquals(rows.size(), count, sort + ": the chain lost or repeated rows");
        }
    }

    private static List<String> names(JsonNode array) {
        List<String> out = new ArrayList<>();
        array.forEach(n -> out.add(n.asText()));
        return out;
    }
}
