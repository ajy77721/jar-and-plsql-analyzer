package com.plsqlanalyzer.web.parser.controller;

import com.analyzer.storage.AnalysisRepository;
import com.analyzer.storage.repo.AnalysisQuery;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.plsqlanalyzer.web.parser.service.AnalysisViewStore;

/**
 * The raw, unfiltered facet arrays {@code ParserAnalysisController#buildSearchRows} normalizes and
 * filters for {@code /global-search} and {@code /global-search/summary}.
 *
 * <p><b>This is the input to the search, not its output.</b> The endpoint's response depends on
 * {@code q} (and the paging/sort parameters), so caching a response would be wrong for every query
 * but one. What does not depend on {@code q} is the set of source rows the search scans — the
 * tables wrapper, the procedures array, and the five node-scan facets — which is exactly what this
 * object holds. {@link AnalysisViewStore#writeSearch} stores it at save time; the controller reads
 * it back and applies the same per-facet {@code searchText} filter it always applied, so the served
 * bytes are identical to reading the 7 artifacts this stands in for.
 *
 * <p><b>The reads are the controller's own.</b> Each facet goes through
 * {@link AnalysisRepository#document} with the dataset and array field the controller used, and the
 * predicates array is built by the same adapter + facet builder the controller called, so this is an
 * extraction of that code, not a second implementation of it.
 */
public final class ParserSearchFacets {

    private ParserSearchFacets() {}

    /** Facet key holding the {@code tables} array of the tables index. */
    public static final String TABLES = "tables";
    public static final String PROCEDURES = "procedures";
    public static final String SEQUENCES = "sequences";
    public static final String CURSORS = "cursors";
    public static final String JOINS = "joins";
    public static final String DYNAMIC_SQL = "dynamicSql";
    public static final String PREDICATES = "predicates";

    /**
     * Builds the facet map. A facet whose artifact is missing/unreadable is omitted rather than
     * failing the build — the search would have skipped it too.
     *
     * @return {@code {facet -> raw rows[]}} for every readable facet
     */
    public static ObjectNode build(AnalysisRepository repository, ObjectMapper mapper, String name) {
        ObjectNode out = mapper.createObjectNode();
        put(out, TABLES, readArray(repository, name, AnalysisQuery.Dataset.PLSQL_TABLES, "tables"));
        put(out, PROCEDURES, readArray(repository, name, AnalysisQuery.Dataset.PLSQL_PROCEDURES, null));
        put(out, SEQUENCES, readArray(repository, name, AnalysisQuery.Dataset.PLSQL_SEQUENCES, null));
        put(out, CURSORS, readArray(repository, name, AnalysisQuery.Dataset.PLSQL_CURSORS, null));
        put(out, JOINS, readArray(repository, name, AnalysisQuery.Dataset.PLSQL_JOINS, null));
        put(out, DYNAMIC_SQL, readArray(repository, name, AnalysisQuery.Dataset.PLSQL_DYNAMIC_SQL, null));
        put(out, PREDICATES, predicateRows(repository, mapper, name));
        return out;
    }

    /** One facet's raw row array, or {@code null} if the dataset can't be read. */
    public static ArrayNode readArray(AnalysisRepository repository, String name,
                                      AnalysisQuery.Dataset dataset, String arrayField) {
        try {
            JsonNode document = repository.document(AnalysisQuery.of(dataset, name));
            if (document == null || document.isNull() || document.isMissingNode()) return null;
            if (document.isArray()) return (ArrayNode) document;
            if (arrayField != null && document.path(arrayField).isArray()) {
                return (ArrayNode) document.get(arrayField);
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Predicate rows for search, built exactly as the controller's {@code getPredicates} builds them
     * for its own grid (raw {@code usedBy[]} tables adapted via {@code ParserPredicatesAdapter}, then
     * {@code PlsqlPredicatesFacet.build}).
     */
    private static ArrayNode predicateRows(AnalysisRepository repository, ObjectMapper mapper, String name) {
        try {
            JsonNode rawTables = repository.document(AnalysisQuery.of(AnalysisQuery.Dataset.PLSQL_TABLES, name));
            JsonNode adapted = ParserPredicatesAdapter.adaptTables(mapper, rawTables);
            JsonNode document = com.plsqlanalyzer.web.controller.PlsqlPredicatesFacet.build(mapper, adapted);
            return (document != null && document.isArray()) ? (ArrayNode) document : null;
        } catch (Exception e) {
            return null;
        }
    }

    /** Stores only real arrays; an absent facet is simply left out of the map. */
    private static void put(ObjectNode out, String key, JsonNode rows) {
        if (rows != null && rows.isArray()) out.set(key, rows);
    }
}
