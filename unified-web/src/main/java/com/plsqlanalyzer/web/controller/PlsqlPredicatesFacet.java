package com.plsqlanalyzer.web.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

/**
 * UI-028 — server-side port of `plsql/js/predicates.js`'s `_buildPredicateData`.
 *
 * <p>Unlike `tableOps.js`/`joins.js`/`cursors.js` (a direct pass-through of one summary collection,
 * §51/§52), `predicates.js`'s rows are a client-side aggregation: it reads `getTableOperations()`
 * and extracts every {@code accessDetails[].whereFilters[]} into a new row shape keyed by table,
 * not by the summary's own rows. That aggregation has to move server-side too, or the paged
 * endpoint would still require the whole tables array in the browser to build from — the same
 * defect one layer removed. This class is that port, checked against the JS source field for
 * field, not reimplemented from the file's own doc comment.
 *
 * <p><b>MID-021 (predicates.js's own paged-conversion project)</b>: made {@code public}, same
 * reasoning as {@code PlsqlScopeFilter}'s own §107 widening — this class operates purely on the
 * generic {@code accessDetails[].whereFilters[]} shape, not anything specific to the live-crawl
 * backend, so `com.plsqlanalyzer.web.parser.controller`'s own predicates facet reuses it verbatim
 * once the parser backend's raw {@code usedBy[]} rows are adapted into that same shape (see
 * {@code ParserPredicatesAdapter}).
 */
public final class PlsqlPredicatesFacet {

    private PlsqlPredicatesFacet() {
    }

    /**
     * @param tables an array of rows already shaped {@code accessDetails[].whereFilters[]} — the
     *               live-crawl backend's own {@code TableOperationSummary} array, or the parser
     *               backend's raw {@code usedBy[]} rows once adapted to the same shape.
     * @return one row per table with at least one {@code whereFilter}, sorted by
     *         {@code predicateCount} descending (the client's own default, reproduced here since
     *         this row's own default sort key needs a real order to fall back on before a caller
     *         asks for one).
     */
    public static ArrayNode build(ObjectMapper mapper, JsonNode tables) {
        ArrayNode out = mapper.createArrayNode();
        for (JsonNode table : tables) {
            ArrayNode predicates = mapper.createArrayNode();
            Set<String> upperColumns = new LinkedHashSet<>();
            Set<String> operations = new LinkedHashSet<>();
            for (JsonNode d : table.path("accessDetails")) {
                for (JsonNode f : d.path("whereFilters")) {
                    ObjectNode p = mapper.createObjectNode();
                    String column = f.path("columnName").asText("");
                    p.put("columnName", column);
                    p.put("operator", f.path("operator").asText(""));
                    p.put("value", f.path("value").asText(""));
                    int line = f.path("lineNumber").asInt(0);
                    p.put("lineNumber", line != 0 ? line : d.path("lineNumber").asInt(0));
                    p.put("procedureId", d.path("procedureId").asText(""));
                    p.put("procedureName", d.path("procedureName").asText(""));
                    p.put("sourceFile", d.path("sourceFile").asText(""));
                    String operation = d.path("operation").asText("");
                    p.put("operation", operation);
                    predicates.add(p);
                    if (!column.isEmpty()) upperColumns.add(column.toUpperCase(Locale.ROOT));
                    if (!operation.isEmpty()) operations.add(operation);
                }
            }
            if (predicates.isEmpty()) continue;

            ObjectNode row = mapper.createObjectNode();
            row.put("tableName", table.path("tableName").asText(""));
            row.put("schemaName", table.path("schemaName").asText(""));
            row.put("tableType", table.path("tableType").asText("TABLE"));
            row.put("predicateCount", predicates.size());
            row.put("uniqueColumns", upperColumns.size());
            row.set("predicates", predicates);
            ArrayNode ops = row.putArray("operations");
            for (String op : operations) ops.add(op);
            out.add(row);
        }
        return out;
    }
}
