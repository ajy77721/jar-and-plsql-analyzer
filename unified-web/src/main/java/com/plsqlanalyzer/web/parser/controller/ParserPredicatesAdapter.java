package com.plsqlanalyzer.web.parser.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * MID-021 (predicates.js's own paged-conversion project) — server-side twin of {@code
 * PA.api._adaptParserTableRow} (`plsql/js/api.js`), built to feed {@code
 * com.plsqlanalyzer.web.controller.PlsqlPredicatesFacet#build}, which expects rows already shaped
 * {@code accessDetails[].whereFilters[]} — the live-crawl backend's own shape, not the parser's raw
 * {@code usedBy[]} artifact (see {@code ParserFacetSpecs.TABLES}'s own doc comment for why the two
 * backends' table rows differ at all).
 *
 * <p>Field-for-field port of the client adapter, not a reimplementation from memory: {@code
 * usedBy[].predicates} (flat, {@code {columnName, operator, value, lineNumber}}, per §138's fix to
 * {@code ChunkedFlowWriter.writeTableIndex}) is grouped by {@code lineNumber} and attached as each
 * matching flattened {@code accessDetails} entry's own {@code whereFilters} — exactly what {@code
 * PlsqlPredicatesFacet.build} already reads.
 */
final class ParserPredicatesAdapter {

    private ParserPredicatesAdapter() {
    }

    /**
     * @param rawTablesDocument the {@code {tables:[...], totalTables, ...}} document {@code
     *                          AnalysisQuery.Dataset.PLSQL_TABLES} resolves to — the same raw
     *                          artifact {@code ParserFacetSpecs.TABLES} pages.
     * @return an array of tables, each shaped {@code {tableName, schemaName, tableType,
     *         accessDetails:[{operation, procedureName, procedureId, lineNumber, whereFilters}]}}.
     */
    static ArrayNode adaptTables(ObjectMapper mapper, JsonNode rawTablesDocument) {
        ArrayNode out = mapper.createArrayNode();
        for (JsonNode t : rawTablesDocument.path("tables")) {
            out.add(adaptRow(mapper, t));
        }
        return out;
    }

    private static ObjectNode adaptRow(ObjectMapper mapper, JsonNode t) {
        ArrayNode accessDetails = mapper.createArrayNode();
        for (JsonNode u : t.path("usedBy")) {
            Map<Integer, List<JsonNode>> predicatesByLine = new LinkedHashMap<>();
            for (JsonNode p : u.path("predicates")) {
                int line = p.path("lineNumber").asInt(0);
                predicatesByLine.computeIfAbsent(line, k -> new ArrayList<>()).add(p);
            }
            // `operations` is stored grouped by operation name ({"SELECT":[12,88],"INSERT":[40]}),
            // so walking it operation-by-operation would emit this routine's accesses clustered by
            // operation (12, 88, 40) rather than in the order they actually execute (12, 40, 88).
            // Flatten every (operation, line) occurrence first and sort by the access's own line
            // across ALL operations, so the emitted sequence is true source order regardless of
            // operation identity. Stable sort, so occurrences sharing a line keep the operation
            // order the artifact recorded.
            JsonNode ops = u.path("operations");
            List<Map.Entry<String, Integer>> occurrences = new ArrayList<>();
            Iterator<Map.Entry<String, JsonNode>> opFields = ops.fields();
            while (opFields.hasNext()) {
                Map.Entry<String, JsonNode> opEntry = opFields.next();
                String op = opEntry.getKey();
                for (JsonNode lineNode : opEntry.getValue()) {
                    occurrences.add(new AbstractMap.SimpleEntry<>(op, lineNode.asInt(0)));
                }
            }
            occurrences.sort(Comparator.comparingInt(Map.Entry::getValue));
            for (Map.Entry<String, Integer> occurrence : occurrences) {
                int line = occurrence.getValue();
                ObjectNode ad = mapper.createObjectNode();
                ad.put("operation", occurrence.getKey());
                ad.put("procedureName", u.path("nodeName").asText(""));
                ad.put("procedureId", u.path("nodeId").asText(""));
                ad.put("lineNumber", line);
                ArrayNode wf = ad.putArray("whereFilters");
                List<JsonNode> matches = predicatesByLine.get(line);
                if (matches != null) matches.forEach(wf::add);
                accessDetails.add(ad);
            }
        }
        ObjectNode row = mapper.createObjectNode();
        row.put("tableName", t.path("name").asText(""));
        row.put("schemaName", t.path("schema").asText(""));
        row.put("tableType", t.path("objectType").asText("TABLE"));
        row.set("accessDetails", accessDetails);
        return row;
    }
}
