package com.jaranalyzer.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.plsqlanalyzer.web.page.FacetSort;
import com.plsqlanalyzer.web.page.FacetSpec;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Paging contracts for the two log-listing endpoints that had none — MID-017 (2 of 3).
 *
 * <h2>Why these two, and not {@code /catalog}</h2>
 *
 * <p>Checked before building anything: {@code JA.api.listAllCorrectionLogs} and
 * {@code JA.api.listRunLogs} have <b>zero callers</b> anywhere in the static tree. The screen that
 * browses corrections (`summary-corrections.js`) calls {@code getCorrectionLogs(jarId,
 * endpointName)} instead — a different, per-endpoint route, inherently bounded by one endpoint's
 * corrections rather than the whole jar's. Both list-all forms are the same "declared but never
 * wired" shape this registry has found repeatedly, just still worth a correct page envelope rather
 * than none, the same reasoning MID-005 applied to {@code /resolver}.
 *
 * <p>{@code /catalog} is deliberately not given this treatment. Its {@code collections}/{@code
 * views} arrays are consumed by {@code summary-tables.js} as a lookup <b>set</b> — every collection
 * this jar accesses is checked against the full catalog for verification — not rendered as a page
 * of rows. Paging that list would silently mark collections past the cut as "not in DB", which is
 * the wrong kind of wrong: quietly incorrect rather than absent.
 */
final class LogListFacets {

    private LogListFacets() {
    }

    private static String name(JsonNode row) {
        return row.path("name").asText("");
    }

    /** {@code name}, {@code label}, {@code type}, {@code endpoint}, {@code size}. */
    static final FacetSpec CORRECTION_LOGS = new FacetSpec(
            "correctionLogs",
            FacetSort.byName(
                    FacetSort.text("NAME_ASC", "name", false),
                    FacetSort.text("NAME_DESC", "name", true),
                    FacetSort.number("SIZE_ASC", "size", false),
                    FacetSort.number("SIZE_DESC", "size", true)),
            "NAME_ASC",
            50,
            row -> name(row).toUpperCase(Locale.ROOT),
            row -> (name(row) + ' ' + row.path("endpoint").asText("")).toLowerCase(Locale.ROOT),
            null,
            null,
            Map.of("type", FacetSpec.scalarColumn("type", null)));

    /** {@code name}, {@code size}, {@code modified}. Already newest-first from the reader. */
    static final FacetSpec RUN_LOGS = new FacetSpec(
            "runLogs",
            FacetSort.byName(
                    FacetSort.text("NAME_ASC", "name", false),
                    FacetSort.text("NAME_DESC", "name", true),
                    FacetSort.number("SIZE_ASC", "size", false),
                    FacetSort.number("SIZE_DESC", "size", true)),
            "NAME_DESC",
            50,
            row -> name(row).toUpperCase(Locale.ROOT),
            row -> name(row).toLowerCase(Locale.ROOT),
            null,
            null,
            null);

    static List<JsonNode> rows(com.fasterxml.jackson.databind.ObjectMapper mapper,
                               List<Map<String, Object>> maps) {
        List<JsonNode> out = new java.util.ArrayList<>(maps.size());
        for (Map<String, Object> m : maps) out.add(mapper.valueToTree(m));
        return out;
    }
}
