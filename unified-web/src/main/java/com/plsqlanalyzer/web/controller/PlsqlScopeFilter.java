package com.plsqlanalyzer.web.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * UI-028 — server-side port of `plsql/js/tableOps.js` (and the identical logic in `joins.js`,
 * `cursors.js`) `_applyScopeData`'s row transform: not just "does this row match the current
 * scope" (a {@code Predicate<JsonNode>}, which {@link com.plsqlanalyzer.web.page.FacetSpec}-based
 * facets already support), but "keep this row, with its {@code accessDetails} trimmed to only the
 * scoped entries, and {@code accessCount} recomputed to match".
 *
 * <p>Applied <b>before</b> paging, not as a {@code FacetSpec} predicate: the shared paging
 * machinery has no row-rewrite hook, and adding one there would change behaviour for every
 * already-converted, already-tested facet to support a shape only this one screen family needs.
 * This runs once per request, over the same in-memory collection the plain paged endpoint already
 * iterates — no new cost the server was not already paying, since {@code AnalysisResult} is
 * resident in memory regardless.
 *
 * <h2>The two modes, exactly as the client defines them</h2>
 *
 * <ul>
 *   <li>{@code directProcId} — "Direct" scope. A row's access detail matches if its {@code
 *       procedureId}/{@code procedureName} equals the given id outright, or — the client's own
 *       fallback for package-qualified vs. bare ids — ends with {@code .<package>.<name>} (or
 *       equals the bare {@code <name>} when the given id has no package qualifier).</li>
 *   <li>{@code nodeIds} — "Full Flow" scope: the client already walked the selected procedure's
 *       call tree (a payload proportionate to one procedure, not the whole corpus — not the defect
 *       this row exists to fix) into a set of node ids. A row's access detail matches if its id
 *       equals one of these, or — the client's own fuzzy fallback for id-form mismatches —
 *       contains, or is contained by, one of them.</li>
 * </ul>
 *
 * <p>Both fallbacks are reproduced exactly as the client has them, not "improved": the acceptance
 * criterion for this port is that a converted screen renders the same rows, the same reasoning
 * {@code ParserScopeResolver}'s own javadoc states for the other app's scope conversion.
 *
 * <p><b>MID-021 (§107): made {@code public}</b> so `plsql/js/tableOps.js`'s own parser-backend
 * fallback (still on the unpaged {@code /api/parser/analyses/{name}/tables} array, per that file's
 * own header comment) can start converting to a paged endpoint using this same "all"/"scoped"
 * semantics — the row shapes it filters (`accessDetails[]`, `predicates[]`) are generic PL/SQL
 * facet rows, not specific to which backend produced them.
 */
public final class PlsqlScopeFilter {

    private PlsqlScopeFilter() {
    }

    /** @return a new array of rows matching {@code directProcId}, each with {@code accessDetails}
     *          trimmed to only that procedure's entries and {@code accessCount} recomputed;
     *          rows with no matching entry are dropped entirely. */
    public static ArrayNode direct(ObjectMapper mapper, JsonNode rows, String directProcId) {
        return trim(mapper, rows, directPredicate(directProcId));
    }

    /** @return a new array of rows with an access detail matching any id in {@code nodeIds}
     *          (exact, or fuzzy substring-containment either direction), same trimming as
     *          {@link #direct}. */
    public static ArrayNode subtree(ObjectMapper mapper, JsonNode rows, Set<String> nodeIds) {
        if (nodeIds == null || nodeIds.isEmpty()) return mapper.createArrayNode();
        return trim(mapper, rows, subtreePredicate(nodeIds));
    }

    /**
     * The same two scope modes, over {@code predicates.js}'s own row shape (§53) — {@code
     * predicates[]}, not {@code accessDetails[]}, and {@code predicateCount}/{@code
     * uniqueColumns}/{@code operations} recomputed from what survives, not just a count.
     *
     * <p>Made {@code public}: `com.plsqlanalyzer.web.parser.controller`'s own predicates facet
     * reuses these directly, same reasoning as {@link #directTables}/{@link #subtreeTables}'s own
     * widening — the row shape these trim is {@code PlsqlPredicatesFacet.build}'s generic output,
     * not specific to which backend produced the tables it was built from.
     */
    public static ArrayNode directPredicates(ObjectMapper mapper, JsonNode rows, String directProcId) {
        return trimPredicates(mapper, rows, directPredicate(directProcId));
    }

    public static ArrayNode subtreePredicates(ObjectMapper mapper, JsonNode rows, Set<String> nodeIds) {
        if (nodeIds == null || nodeIds.isEmpty()) return mapper.createArrayNode();
        return trimPredicates(mapper, rows, subtreePredicate(nodeIds));
    }

    /**
     * MID-021 (§110) — the same two scope modes, over the parser backend's own {@code tables[]}
     * artifact shape ({@code usedBy[]}, not {@code accessDetails[]} — see {@code ParserFacetSpecs
     * .TABLES}'s own doc comment). {@code document} is the whole {@code {tables:[...], ...}} object
     * the reader returns, not a bare array, so the array lives under {@code arrayField} and the rest
     * of the object (`totalTables`, `operationSummary`) is carried through unchanged.
     */
    public static JsonNode directTables(ObjectMapper mapper, JsonNode document, String arrayField,
                                        String directProcId) {
        return trimTables(mapper, document, arrayField, directPredicate(directProcId));
    }

    public static JsonNode subtreeTables(ObjectMapper mapper, JsonNode document, String arrayField,
                                         Set<String> nodeIds) {
        if (nodeIds == null || nodeIds.isEmpty()) {
            ObjectNode copy = ((ObjectNode) document).deepCopy();
            copy.set(arrayField, mapper.createArrayNode());
            return copy;
        }
        return trimTables(mapper, document, arrayField, subtreePredicate(nodeIds));
    }

    /**
     * Trims each table row's {@code usedBy} to only the entries whose {@code nodeId} matches;
     * drops a table with no matching entry at all, same as {@link #trim} does for
     * {@code accessDetails}. There is no {@code accessCount} field to recompute here — the table
     * facet computes it from {@code usedBy} on read (see {@code ParserFacetSpecs::accessLines}), so
     * trimming the array is the whole fix.
     */
    private static JsonNode trimTables(ObjectMapper mapper, JsonNode document, String arrayField,
                                       java.util.function.Predicate<String> matches) {
        ArrayNode out = mapper.createArrayNode();
        for (JsonNode row : document.path(arrayField)) {
            ArrayNode kept = mapper.createArrayNode();
            for (JsonNode u : row.path("usedBy")) {
                String rawId = u.path("nodeId").asText("");
                if (!rawId.isEmpty() && matches.test(rawId.toUpperCase(Locale.ROOT))) kept.add(u);
            }
            if (kept.isEmpty()) continue;
            ObjectNode copy = mapper.createObjectNode();
            row.fields().forEachRemaining(e -> {
                if (!"usedBy".equals(e.getKey())) copy.set(e.getKey(), e.getValue());
            });
            copy.set("usedBy", kept);
            out.add(copy);
        }
        ObjectNode docCopy = ((ObjectNode) document).deepCopy();
        docCopy.set(arrayField, out);
        return docCopy;
    }

    private static java.util.function.Predicate<String> directPredicate(String directProcId) {
        String upper = directProcId.toUpperCase(Locale.ROOT);
        int dot = upper.lastIndexOf('.');
        String last = dot >= 0 ? upper.substring(dot + 1) : upper;
        String pkg = null;
        if (dot >= 0) {
            String withoutLast = upper.substring(0, dot);
            int dot2 = withoutLast.lastIndexOf('.');
            pkg = dot2 >= 0 ? withoutLast.substring(dot2 + 1) : withoutLast;
        }
        final String finalLast = last;
        final String finalPkg = pkg;
        return pid -> {
            if (pid.equals(upper)) return true;
            if (finalPkg != null) return pid.endsWith("." + finalPkg + "." + finalLast);
            return pid.equals(finalLast);
        };
    }

    private static java.util.function.Predicate<String> subtreePredicate(Set<String> nodeIds) {
        Set<String> upperIds = new java.util.HashSet<>();
        for (String id : nodeIds) upperIds.add(id.toUpperCase(Locale.ROOT));
        return pid -> {
            if (upperIds.contains(pid)) return true;
            for (String nid : upperIds) {
                if (pid.contains(nid) || nid.contains(pid)) return true;
            }
            return false;
        };
    }

    private static ArrayNode trim(ObjectMapper mapper, JsonNode rows,
                                  java.util.function.Predicate<String> matches) {
        ArrayNode out = mapper.createArrayNode();
        for (JsonNode row : rows) {
            List<JsonNode> kept = new ArrayList<>();
            for (JsonNode d : row.path("accessDetails")) {
                String rawId = d.path("procedureId").asText("");
                if (rawId.isEmpty()) rawId = d.path("procedureName").asText("");
                if (rawId.isEmpty()) continue;
                if (matches.test(rawId.toUpperCase(Locale.ROOT))) kept.add(d);
            }
            if (kept.isEmpty()) continue;
            ObjectNode copy = mapper.createObjectNode();
            row.fields().forEachRemaining(e -> {
                if (!"accessDetails".equals(e.getKey()) && !"accessCount".equals(e.getKey())) {
                    copy.set(e.getKey(), e.getValue());
                }
            });
            ArrayNode details = copy.putArray("accessDetails");
            for (JsonNode d : kept) details.add(d);
            copy.put("accessCount", kept.size());
            // The badge fields the client's own scoped rendering already reads (t._fullAccessCount
            // / t._fullAccessDetails) — carried through so the UI can still say "N of full".
            copy.set("_fullAccessDetails", row.path("accessDetails"));
            copy.put("_fullAccessCount", row.path("accessDetails").size());
            out.add(copy);
        }
        return out;
    }

    /** {@link #trim}, over {@code predicates[]} — see {@link PlsqlPredicatesFacet}'s row shape. */
    private static ArrayNode trimPredicates(ObjectMapper mapper, JsonNode rows,
                                            java.util.function.Predicate<String> matches) {
        ArrayNode out = mapper.createArrayNode();
        for (JsonNode row : rows) {
            List<JsonNode> kept = new ArrayList<>();
            Set<String> upperColumns = new java.util.LinkedHashSet<>();
            Set<String> operations = new java.util.LinkedHashSet<>();
            for (JsonNode p : row.path("predicates")) {
                String rawId = p.path("procedureId").asText("");
                if (rawId.isEmpty()) rawId = p.path("procedureName").asText("");
                if (rawId.isEmpty() || !matches.test(rawId.toUpperCase(Locale.ROOT))) continue;
                kept.add(p);
                String col = p.path("columnName").asText("");
                if (!col.isEmpty()) upperColumns.add(col.toUpperCase(Locale.ROOT));
                String op = p.path("operation").asText("");
                if (!op.isEmpty()) operations.add(op);
            }
            if (kept.isEmpty()) continue;
            ObjectNode copy = mapper.createObjectNode();
            row.fields().forEachRemaining(e -> {
                String k = e.getKey();
                if (!"predicates".equals(k) && !"predicateCount".equals(k)
                        && !"uniqueColumns".equals(k) && !"operations".equals(k)) {
                    copy.set(k, e.getValue());
                }
            });
            ArrayNode predicates = copy.putArray("predicates");
            for (JsonNode p : kept) predicates.add(p);
            copy.put("predicateCount", kept.size());
            copy.put("uniqueColumns", upperColumns.size());
            ArrayNode ops = copy.putArray("operations");
            for (String op : operations) ops.add(op);
            out.add(copy);
        }
        return out;
    }
}
