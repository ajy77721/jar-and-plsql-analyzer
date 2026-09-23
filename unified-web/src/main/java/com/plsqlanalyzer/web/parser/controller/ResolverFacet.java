package com.plsqlanalyzer.web.parser.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.plsqlanalyzer.web.page.FacetSort;
import com.plsqlanalyzer.web.page.FacetSpec;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The paging contract for {@code /resolver/{type}} — MID-005.
 *
 * <h2>Why this is the light pattern, not the repository one</h2>
 *
 * <p>{@code /tables/page} and its siblings go through {@code AnalysisRepository.document(...)},
 * which needs an {@code AnalysisQuery.Dataset} entry wired through both the artifact-scan and
 * Mongo repositories. That machinery earns its cost for data multiple screens read. Checked before
 * building anything here: <b>no client script calls {@code /resolver/} at all</b> — the one hit for
 * "resolver" in the static tree is a documentation string in {@code help.js} describing an
 * unrelated on-disk cache directory. Building the dual-repository path for an endpoint with zero
 * consumers would be inventing a sort order, a search field and a filter column set that only a
 * real screen could tell you were right.
 *
 * <p>So this follows {@code /analyses}'s existing shortcut instead: read the small JSON directly
 * and hand a {@code List<JsonNode>} to {@code FacetPageResponder.respondStreaming}, bypassing the
 * repository abstraction entirely. Measured today: the largest resolver file is 32 KB over 85
 * rows — comparable to the 8.2 KB {@code /analyses} history this pattern was written for, and
 * nowhere near where the heavier path would start paying for itself.
 *
 * <h2>Four shapes, one row form</h2>
 *
 * <p>{@code q1}/{@code ambiguous} are arrays keyed by {@code objectName}; {@code q3} is an array of
 * a different shape (empty in every fixture measured, so its real fields are unconfirmed — the
 * generic projection below is deliberately permissive rather than guessed at); {@code q4} is a
 * data-keyed map ({@code objectName -> [dependents]}), the same shape {@code jar-dep-map.json} was
 * (§29) — normalised into rows here rather than served as an object, for the same reason: an
 * object name is data, not a schema key a client should have to enumerate.
 */
final class ResolverFacet {

    private ResolverFacet() {
    }

    /**
     * {@code q4_direct_dependencies.json}'s shape: {@code {"SCHEMA.OBJECT": [...]}}. One row per
     * key, the array kept intact as {@code dependencies} rather than split further — nothing here
     * needs to query into it, and the row itself is already small.
     */
    static List<JsonNode> rowsFromMap(com.fasterxml.jackson.databind.ObjectMapper mapper,
                                      JsonNode map) {
        List<JsonNode> rows = new ArrayList<>();
        if (!map.isObject()) return rows;
        Iterator<Map.Entry<String, JsonNode>> it = map.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            var row = mapper.createObjectNode();
            row.put("objectName", e.getKey());
            row.set("dependencies", e.getValue());
            rows.add(row);
        }
        return rows;
    }

    /** Rows already in array form ({@code q1}, {@code ambiguous}, {@code q3}) — used as-is. */
    static List<JsonNode> rowsFromArray(JsonNode array) {
        List<JsonNode> rows = new ArrayList<>();
        if (array.isArray()) array.forEach(rows::add);
        return rows;
    }

    private static String name(JsonNode row) {
        String n = row.path("objectName").asText(null);
        return n != null ? n : row.path("name").asText("");
    }

    static final FacetSpec SPEC = new FacetSpec(
            "resolver",
            FacetSort.byName(
                    FacetSort.text("NAME_ASC", "objectName", false),
                    FacetSort.text("NAME_DESC", "objectName", true)),
            "NAME_ASC",
            50,
            row -> name(row).toUpperCase(Locale.ROOT),
            row -> (name(row) + ' ' + row.path("schema").asText("")).toLowerCase(Locale.ROOT),
            null,
            null,
            null);
}
