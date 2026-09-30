package com.plsqlanalyzer.web.page;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

/**
 * Everything an endpoint must state before its table can be paged server-side.
 *
 * <p>The list is short but none of it is optional, because each entry corresponds to something the
 * browser used to do over the whole array and can no longer do over one page:
 *
 * <ul>
 *   <li>{@link #sorts} — the column headers, as a closed set (never a column name off the wire)</li>
 *   <li>{@link #searchText} — the search box, which has to move with paging or it silently
 *       searches the current page</li>
 *   <li>{@link #typeValues} — the filter pills, whose value list came from the whole array</li>
 *   <li>{@link #procedureIds} — the Direct/Subtree/SubFlow scope selector, which filtered rows by
 *       which procedures touch them</li>
 *   <li>{@link #id} — a stable row identity for the cursor tiebreaker. Not an ordinal: re-running
 *       an analysis renumbers its nodes, so an ordinal would move under a paging reader.</li>
 * </ul>
 *
 * @param label        the row noun, for messages ("sequences")
 * @param sorts        the closed sort set, keyed by wire name
 * @param defaultSort  the natural order, used when the caller does not choose. Must match what the
 *                     table showed before conversion, or page 1 changes under the user.
 * @param defaultLimit rows per page when the caller pages without saying how many
 * @param id           stable row identity
 * @param searchText   the row's searchable text
 * @param typeValues   the row's values for the pill filter (a row matches if any is selected)
 * @param procedureIds the ids of procedures this row belongs to, for scope filtering
 * @param filterColumns the per-column checkbox popovers, keyed by the column index the table's
 *                      HTML uses. Their value lists were collected from the whole array, so they
 *                      have to be served alongside the counts or the popover would offer only the
 *                      values on the current page — a filter that visibly does the wrong thing.
 * @param textColumns   the per-column free-text popovers (contains/regex), keyed by the same
 *                      column-index namespace as {@code filterColumns} — a column is one or the
 *                      other, never both. Unlike {@code filterColumns} this extracts one string per
 *                      row rather than an enumerable value list: there is nothing to enumerate for
 *                      a free-text identifier column, so no value-collection pass over the whole
 *                      array is needed for these, only the match itself at request time.
 */
public record FacetSpec(String label,
                        Map<String, FacetSort> sorts,
                        String defaultSort,
                        int defaultLimit,
                        Function<JsonNode, String> id,
                        Function<JsonNode, String> searchText,
                        Function<JsonNode, List<String>> typeValues,
                        Function<JsonNode, List<String>> procedureIds,
                        Map<String, Function<JsonNode, List<String>>> filterColumns,
                        Map<String, Function<JsonNode, String>> textColumns) {

    public FacetSpec {
        if (!sorts.containsKey(defaultSort)) {
            throw new IllegalStateException(
                    "defaultSort " + defaultSort + " is not one of " + sorts.keySet());
        }
        filterColumns = filterColumns == null ? Map.of() : Map.copyOf(filterColumns);
        textColumns = textColumns == null ? Map.of() : Map.copyOf(textColumns);
        for (String c : textColumns.keySet()) {
            if (filterColumns.containsKey(c)) {
                throw new IllegalStateException(
                        "Column '" + c + "' declared in both filterColumns and textColumns");
            }
        }
    }

    /**
     * Convenience constructor for the many call sites with no {@code textColumns} — every facet
     * declared before this capability existed, kept working unchanged.
     */
    public FacetSpec(String label, Map<String, FacetSort> sorts, String defaultSort, int defaultLimit,
                     Function<JsonNode, String> id, Function<JsonNode, String> searchText,
                     Function<JsonNode, List<String>> typeValues,
                     Function<JsonNode, List<String>> procedureIds,
                     Map<String, Function<JsonNode, List<String>>> filterColumns) {
        this(label, sorts, defaultSort, defaultLimit, id, searchText, typeValues, procedureIds,
                filterColumns, null);
    }

    /** A column whose value is one scalar field. */
    public static Function<JsonNode, List<String>> scalarColumn(String field, String emptyAs) {
        return row -> {
            String v = row.path(field).asText("");
            if (v.isEmpty()) return emptyAs == null ? List.of() : List.of(emptyAs);
            return List.of(v);
        };
    }

    /** A column whose value is a string array, e.g. {@code operations}. */
    public static Function<JsonNode, List<String>> arrayColumn(String field) {
        return row -> stringArray(row, field);
    }

    /** Reads {@code accessDetails[].procedureId} (falling back to the name), the shape every
     *  usage-style facet writes. Upper-cased, because the client's scope ids are. */
    public static List<String> accessDetailProcedureIds(JsonNode row) {
        JsonNode details = row.path("accessDetails");
        if (!details.isArray()) return List.of();
        List<String> out = new ArrayList<>(details.size());
        for (JsonNode d : details) {
            String pid = d.path("procedureId").asText("");
            if (pid.isEmpty()) pid = d.path("procedureName").asText("");
            if (!pid.isEmpty()) out.add(pid.toUpperCase(Locale.ROOT));
        }
        return out;
    }

    /**
     * The {@code lineNumber} of a usage row's first access detail, or {@code 0} when there is none.
     * A usage-style facet whose reader emits one row per <em>instance</em> (not per name) needs the
     * instance's source line in its row identity — two Oracle implicit FOR-loop cursors share the
     * synthetic name {@code "(FOR R)"} on the same node, so {@code name::node} alone is not unique
     * and a page boundary could drop one. See {@code ParserFacetSpecs.CURSORS}.
     */
    public static int accessDetailFirstLine(JsonNode row) {
        JsonNode details = row.path("accessDetails");
        if (!details.isArray() || details.isEmpty()) return 0;
        return details.get(0).path("lineNumber").asInt(0);
    }

    /** The elements of a string array field, e.g. {@code operations} or {@code joinTypes}. */
    public static List<String> stringArray(JsonNode row, String field) {
        JsonNode arr = row.path(field);
        if (!arr.isArray()) return List.of();
        List<String> out = new ArrayList<>(arr.size());
        for (JsonNode v : arr) {
            String s = v.asText("");
            if (!s.isEmpty()) out.add(s);
        }
        return out;
    }
}
