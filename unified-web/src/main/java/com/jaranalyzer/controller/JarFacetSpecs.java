package com.jaranalyzer.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.plsqlanalyzer.web.page.FacetSort;
import com.plsqlanalyzer.web.page.FacetSpec;

import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The paging contract of the Code Structure tab's two lazy reads, and the projections that make
 * them worth paging.
 *
 * <p><b>The projections are the point, not the page size.</b> Both of these documents are large
 * because of what lives <i>inside</i> each row, not because of how many rows there are:
 *
 * <ul>
 *   <li>{@code bundled-jars.json} is 3.03 MB for 94 rows — 2.84 MB of it manifest text and resource
 *       file bodies, which the tree does not render until a leaf is clicked. Paging 94 rows at 100
 *       per page would have saved nothing at all; dropping the bodies and fetching one on click
 *       saves 99%.</li>
 *   <li>The class tree is 656 KB for 119 classes, 475 KB of which is {@code methodSummaries} — and
 *       <b>nothing reads it</b>. The tree's own renderer looks for {@code cls.methods}, which this
 *       response has never contained, so every byte of it is transferred and discarded. The other
 *       reader of method summaries ({@code jar/js/summary.js}) works from the summary document, not
 *       from here.</li>
 * </ul>
 *
 * <p>Both projections apply only on the paged path. A caller that sends no paging parameter still
 * gets today's response byte for byte, bodies and all, which is what lets this land without a
 * coordinated client deploy.
 *
 * <p>Row identity is a natural key in both cases — the JAR file name, the fully-qualified class
 * name — and never an array index. An index would be a cursor tiebreaker that moves when a
 * re-analysis reorders the array, which is the failure cursor paging exists to avoid.
 */
public final class JarFacetSpecs {

    private JarFacetSpecs() {
    }

    private static String lower(String s) {
        return s == null ? "" : s.toLowerCase(Locale.ROOT);
    }

    /* ── Q41 / L2 — bundled JARs ─────────────────────────────────────────────────────────── */

    /**
     * Header fields plus the resource <i>names</i>; manifest and resource bodies are dropped.
     *
     * <p>The names stay because the tree renders a child leaf per resource before anything is
     * clicked, and they are cheap: 7.3 KB of the projection's 15.9 KB, against 2.84 MB for the
     * bodies they name.
     */
    static ObjectNode bundledJarRow(JsonNode row) {
        ObjectNode out = ((ObjectNode) row).objectNode();
        out.put("name", row.path("name").asText(""));
        out.put("size", row.path("size").asLong(0L));
        JsonNode resources = row.path("resources");
        out.put("hasManifest", !row.path("manifest").isMissingNode()
                && !row.path("manifest").isNull()
                && !row.path("manifest").asText("").isEmpty());
        var names = out.putArray("resourceNames");
        int count = 0;
        if (resources.isObject()) {
            for (Iterator<String> it = resources.fieldNames(); it.hasNext(); ) {
                names.add(it.next());
                count++;
            }
        }
        out.put("resourceCount", count);
        return out;
    }

    /**
     * Default order is {@code NAME_ASC} because that is the order the tree already shows — the
     * section is rendered in document order today and {@code bundled-jars.json} is written sorted
     * by name. A different default here would silently reshuffle the list on the day the client
     * starts paging.
     */
    static final FacetSpec BUNDLED_JARS = new FacetSpec(
            "bundled JARs",
            FacetSort.byName(
                    FacetSort.text("NAME_ASC", "name", false),
                    FacetSort.text("NAME_DESC", "name", true),
                    FacetSort.number("SIZE_ASC", "size", false),
                    FacetSort.number("SIZE_DESC", "size", true),
                    FacetSort.number("RESOURCES_ASC", "resourceCount", false),
                    FacetSort.number("RESOURCES_DESC", "resourceCount", true)),
            "NAME_ASC",
            100,
            row -> row.path("name").asText("").toUpperCase(Locale.ROOT),
            row -> lower(row.path("name").asText("")),
            null,
            null,
            Map.of());

    /* ── Q40 / L1 — class tree ───────────────────────────────────────────────────────────── */

    /** Plain ASCII, deliberately — an embedded NUL risks being stripped or rejected somewhere
     * in the request path between the browser and this filter; a JAR file name cannot contain
     * this exact string, which is the only uniqueness the sourceJar column filter needs (see
     * CLASS_TREE's own filterColumns comment, below, for why it exists at all). */
    static final String MAIN_JAR_SENTINEL = "__MAIN_JAR__";

    /**
     * Everything the tree header renders, minus {@code methodSummaries}.
     *
     * <p>Copy-and-remove rather than copy-the-keeps: the streamed legacy response has grown fields
     * over time ({@code repositoryEntityType}, {@code fieldCount}) and an allow-list here would
     * quietly stop forwarding the next one. {@code fields} is kept because the class body renders
     * from it the instant a class is expanded, with no second request.
     */
    static ObjectNode classRow(JsonNode row) {
        ObjectNode out = ((ObjectNode) row).deepCopy();
        out.remove("methodSummaries");
        return out;
    }

    /**
     * Default order is {@code PACKAGE} — source JAR, then package, then simple name — which is
     * exactly how the tree groups and sorts today (main classes first, then one group per
     * dependency JAR, packages sorted, classes sorted within a package). Serving the page in that
     * order is what allows a page boundary to fall between packages rather than through one.
     *
     * <p>{@code sourceJar} is absent on main-application classes and present on dependency ones,
     * and the tree renders main classes first — so the key maps absent to {@code ""}, which sorts
     * before every JAR name and reproduces that grouping without a second sort.
     */
    static final FacetSpec CLASS_TREE = new FacetSpec(
            "classes",
            FacetSort.byName(
                    FacetSort.derived("PACKAGE", JarFacetSpecs::packageKey, false),
                    FacetSort.text("NAME_ASC", "simpleName", false),
                    FacetSort.text("NAME_DESC", "simpleName", true),
                    FacetSort.text("STEREOTYPE_ASC", "stereotype", false),
                    FacetSort.number("MEMBERS_DESC", "methodCount", true)),
            "PACKAGE",
            200,
            row -> row.path("fullyQualifiedName").asText(
                    row.path("simpleName").asText("")).toUpperCase(Locale.ROOT),
            // The search the tree's own filter box performs, moved server-side: FQN, simple name
            // and annotation names. Method names are deliberately not included — they are not in
            // the projection, and a search box that matched on data the row no longer carries
            // would return rows whose match the user cannot see.
            JarFacetSpecs::classSearchText,
            null,
            null,
            // UI-003 (§134) — lets `codeTree.js` fetch exactly one package's own classes
            // (`cf=0:<packageName>|1:<sourceJar>`) instead of every class, once the tree's default
            // view stopped needing the whole array resident to render its (now server-computed)
            // package list. Exact match, case-insensitive — the same semantics every other
            // converted grid's column filters already have.
            Map.of("0", FacetSpec.scalarColumn("packageName", ""),
                   // §134's own calibration caught this: FacetPageResponder.columnFilters drops
                   // any cf value that is itself the empty string (the split-then-filter-blank
                   // step silently removes it), so a client asking for main-app classes via
                   // cf=1: would match nothing AND filter nothing (the whole clause is skipped
                   // when its allowed-set ends up empty) — every sourceJar would come back, not
                   // just the main app's. MAIN_JAR_SENTINEL is a value that can survive that split
                   // un-dropped and never collides with a real JAR file name.
                   "1", FacetSpec.scalarColumn("sourceJar", MAIN_JAR_SENTINEL)));

    private static String packageKey(JsonNode row) {
        return (row.path("sourceJar").asText("") + ' '
                + row.path("packageName").asText("") + ' '
                + row.path("simpleName").asText("")).toUpperCase(Locale.ROOT);
    }

    /* ── Q43 / L4 — collection call paths ────────────────────────────────────────────────── */

    /**
     * Default order is {@code SCAN} — the row's position in the single pass over
     * {@code endpoints[]}, which is the order the browser's {@code Promise.all} produced once its
     * responses were reduced, so page 1 is the table the user saw before.
     *
     * <p>{@code seq} is the sort key and <b>not</b> the identity: a re-analysis renumbers it. The
     * identity is the endpoint's natural key plus the target method — the same pair the client's
     * de-duplication used, so the cursor's tiebreaker is the row's actual uniqueness constraint
     * rather than a second, weaker guess at it.
     */
    static final FacetSpec COLLECTION_PATHS = new FacetSpec(
            "call paths",
            FacetSort.byName(
                    FacetSort.number("SCAN", "seq", false),
                    FacetSort.derived("ENDPOINT_ASC", JarFacetSpecs::pathEndpointKey, false),
                    FacetSort.derived("ENDPOINT_DESC", JarFacetSpecs::pathEndpointKey, true)),
            "SCAN",
            50,
            JarFacetSpecs::collectionPathId,
            JarFacetSpecs::collectionPathSearchText,
            null,
            null,
            Map.of());

    private static String collectionPathId(JsonNode row) {
        JsonNode path = row.path("path");
        String target = "";
        if (path.isArray() && path.size() > 0) {
            JsonNode last = path.get(path.size() - 1);
            target = last.path("className").asText("") + '.' + last.path("methodName").asText("");
        }
        return row.path("endpointId").asText("") + '|' + target;
    }

    /** Endpoint label, then the scan position, so the order is total within one endpoint. */
    private static String pathEndpointKey(JsonNode row) {
        return row.path("endpointName").asText("").toUpperCase(Locale.ROOT) + ' '
                + FacetSort.encodeLong(row.path("seq").asLong(0L));
    }

    /**
     * The popup has no search box today, so nothing has to be reproduced — but a table with a
     * server page and no server search is the regression PLAN-08 §4.1 names, and adding the box
     * later without this would silently search one page. The text is what the row displays: the
     * endpoint, its path, and every breadcrumb segment.
     */
    private static String collectionPathSearchText(JsonNode row) {
        StringBuilder sb = new StringBuilder(96);
        sb.append(row.path("endpointName").asText("")).append(' ')
          .append(row.path("httpMethod").asText("")).append(' ')
          .append(row.path("fullPath").asText(""));
        JsonNode path = row.path("path");
        if (path.isArray()) {
            for (JsonNode seg : path) {
                sb.append(' ').append(seg.path("label").asText(""));
            }
        }
        return lower(sb.toString());
    }

    /* ── Database calls — the JAR -> DB procedure/function facet ─────────────────────────── */

    /**
     * The DB-call list. Default order is {@code PACKAGE} — schema, then procedure, then the calling
     * class — which groups a package's procedures together the way a reader scans them, and puts
     * unqualified calls (schema absent) first.
     *
     * <p>Row identity is the call site's natural key (kind + object + class + method + raw text),
     * never the array index: a re-analysis renumbers the array, so an ordinal would move under a
     * paging reader.
     */
    static final FacetSpec DB_CALLS = new FacetSpec(
            "database calls",
            FacetSort.byName(
                    FacetSort.derived("PACKAGE", JarFacetSpecs::dbCallPackageKey, false),
                    FacetSort.text("PROCEDURE_ASC", "dbObject", false),
                    FacetSort.text("PROCEDURE_DESC", "dbObject", true),
                    FacetSort.text("CLASS_ASC", "className", false),
                    FacetSort.text("CLASS_DESC", "className", true),
                    FacetSort.text("KIND_ASC", "kind", false),
                    FacetSort.number("PARAMS_DESC", "paramCount", true)),
            "PACKAGE",
            100,
            JarFacetSpecs::dbCallId,
            JarFacetSpecs::dbCallSearchText,
            row -> List.of(lower(row.path("kind").asText(""))),
            null,
            Map.of("0", FacetSpec.scalarColumn("kind", "")));

    /** Schema, then procedure, then class — the grouping the table shows by default. */
    private static String dbCallPackageKey(JsonNode row) {
        return (row.path("schema").asText("") + ' '
                + row.path("dbObject").asText("") + ' '
                + row.path("className").asText("") + ' '
                + row.path("methodName").asText("")).toUpperCase(Locale.ROOT);
    }

    private static String dbCallId(JsonNode row) {
        return row.path("kind").asText("") + "::" + row.path("dbObject").asText("") + "::"
                + row.path("className").asText("") + "::" + row.path("methodName").asText("") + "::"
                + row.path("rawCall").asText("");
    }

    private static String dbCallSearchText(JsonNode row) {
        return lower(row.path("dbObject").asText("") + ' '
                + row.path("kind").asText("") + ' '
                + row.path("className").asText("") + ' '
                + row.path("methodName").asText("") + ' '
                + row.path("rawCall").asText(""));
    }

    /* ── Global Search — cross-facet normalized rows ─────────────────────────────────────── */

    /**
     * Mirrors {@code ParserFacetSpecs.SEARCH} exactly: the row shape is already normalized
     * ({@code type}/{@code name}/{@code schema}/{@code matchedField}/{@code _idx}) by the
     * controller's {@code buildSearchRows}, so this spec's own {@code searchText} is {@code null}
     * — re-filtering an already-filtered row set is not this spec's job, only paging/sorting/{@code
     * cf} is. The {@code cf} column contract (0=type, 1=schema checkbox; 2=name, 3=matchedField
     * text/contains/regex) is identical to PL/SQL's, on purpose, so {@code jar/js/search.js} can be
     * a line-for-line port of {@code plsql/js/search.js}.
     */
    /** Made public (was package-private) so WarAnalyzerController — a different package, same
     *  facet-paging contract — can reuse this spec verbatim for /global-search rather than
     *  duplicating it. Widening only, no existing behavior changed. */
    public static final FacetSpec SEARCH = new FacetSpec(
            "search results",
            FacetSort.byName(
                    FacetSort.text("TYPE_ASC", "type", false), FacetSort.text("TYPE_DESC", "type", true),
                    FacetSort.text("NAME_ASC", "name", false), FacetSort.text("NAME_DESC", "name", true),
                    FacetSort.text("SCHEMA_ASC", "schema", false), FacetSort.text("SCHEMA_DESC", "schema", true)),
            "TYPE_ASC",
            50,
            row -> row.path("type").asText("") + "::" + row.path("name").asText("") + "::"
                    + row.path("schema").asText("") + "::" + row.path("_idx").asText(""),
            null,
            null, null,
            Map.of("0", FacetSpec.scalarColumn("type", ""), "1", FacetSpec.scalarColumn("schema", "")),
            Map.of("2", row -> row.path("name").asText(""), "3", row -> row.path("matchedField").asText("")));

    private static String classSearchText(JsonNode row) {
        StringBuilder sb = new StringBuilder(96);
        sb.append(row.path("fullyQualifiedName").asText("")).append(' ')
          .append(row.path("simpleName").asText("")).append(' ')
          .append(row.path("packageName").asText(""));
        JsonNode annotations = row.path("annotations");
        if (annotations.isArray()) {
            for (JsonNode a : annotations) sb.append(' ').append(a.path("name").asText(""));
        }
        return lower(sb.toString());
    }
}
