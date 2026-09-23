package com.plsqlanalyzer.web.parser.service;

import com.analyzer.storage.ArtifactKey;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

/**
 * One table's column-level statement detail and in-procedure DDL — the B3 data.
 *
 * <h2>Why this is a new facet and not an extension of {@code table-columns}</h2>
 * {@code PLSQL_TABLE_COLUMNS} sounds like the same thing and is not. It answers "which PL/SQL
 * variables are declared {@code %TYPE} against this table", scanned out of the node documents,
 * with rows of {@code varName / refField / dataType / refType}. This answers "which columns do the
 * statements touch, and for a write, where did the value come from", folded per column by the
 * crawler. Different source artifact, different row shape, different question. Folding the two
 * into one endpoint would mean a row union where half the fields are null depending on which kind
 * it is, and it could not be done without changing the existing response — which must stay
 * byte-identical. So: a new dataset, and the old routes are untouched.
 *
 * <h2>Where the data is</h2>
 * {@code tables.json}, not {@code api/tables/index.json}. The API index does not carry
 * {@code columns} or {@code ddl} at all — checked on disk rather than assumed — so reading the
 * index would have produced an endpoint that always answered "no detail" while looking correct.
 *
 * <h2>Totals</h2>
 * {@code totalColumns} and {@code totalDdl} are counted over the full arrays before any cap is
 * applied, so a capped {@code ddl} still states how many there really are. The column array is not
 * capped here: it is the paged array, and {@code FacetPageResponder} is what bounds it.
 */
class TableColumnDetailReader {

    /**
     * DDL entries returned inline. This array is not the paged one, so it needs its own bound;
     * {@code totalDdl} always states the true count, capped or not.
     */
    static final int MAX_DDL = 200;

    private final StaticAnalysisReader r;

    TableColumnDetailReader(StaticAnalysisReader r) {
        this.r = r;
    }

    /**
     * @param table {@code TABLE}, {@code SCHEMA.TABLE} or the raw {@code tables.json} key; matched
     *              case-insensitively, because the crawler stores the schema as the source wrote
     *              it and the table name upper-cased.
     * @return the detail document, or a missing node when the analysis has no {@code tables.json}
     *         and {@code null} when it has one but not this table — the two are different answers
     *         and the controller turns the second into a 404.
     */
    JsonNode getTableColumnDetail(String name, String table) throws IOException {
        ObjectMapper mapper = r.mapper;
        String target = upper(table);
        String cacheKey = r.cacheKey(name, "tableColumnDetail::" + target);
        JsonNode cached = r.synthesizedCache.get(cacheKey);
        if (cached != null) return cached;

        ArtifactKey key = r.resolveKey(name, "tables.json");
        JsonNode all;
        try (InputStream in = r.openRead(key)) {
            all = mapper.readTree(in);
        }
        if (all == null || !all.isObject()) return mapper.missingNode();

        JsonNode entry = find(all, target);
        ObjectNode out = mapper.createObjectNode();
        out.put("table", table);
        if (entry == null) {
            // A table the analysis does not know. Answered as an explicit empty rather than as an
            // absent facet, so the caller can tell "no such table" from "no detail for it".
            out.put("schema", "");
            out.put("objectType", "");
            out.put("found", false);
            out.put("totalColumns", 0);
            out.put("totalDdl", 0);
            out.putArray("ddl");
            out.putArray("columns");
            r.synthesizedCache.put(cacheKey, out);
            return out;
        }

        out.put("schema", entry.path("schema").asText(""));
        out.put("tableName", entry.path("tableName").asText(""));
        out.put("objectType", entry.path("objectType").asText(""));
        out.put("found", true);

        JsonNode ddl = entry.path("ddl");
        int totalDdl = ddl.isArray() ? ddl.size() : 0;
        out.put("totalDdl", totalDdl);
        ArrayNode ddlOut = out.putArray("ddl");
        if (ddl.isArray()) {
            for (int i = 0; i < ddl.size() && i < MAX_DDL; i++) ddlOut.add(ddl.get(i));
        }
        out.put("ddlTruncated", totalDdl > MAX_DDL);

        JsonNode columns = entry.path("columns");
        int totalColumns = columns.isArray() ? columns.size() : 0;
        out.put("totalColumns", totalColumns);
        ArrayNode rows = out.putArray("columns");
        if (columns.isArray()) {
            int seq = 0;
            for (JsonNode c : columns) {
                ObjectNode row = rows.addObject();
                copy(row, c, "columnName");
                copyArray(mapper, row, c, "usages");
                copyArray(mapper, row, c, "valueSources");
                copyArray(mapper, row, c, "referencedBy");
                copyArray(mapper, row, c, "intoVariables");
                copyArray(mapper, row, c, "lines");
                row.put("sampleExpression", c.path("sampleExpression").asText(""));
                // Derived, so the client does not have to compute the same thing five times:
                // whether this column is ever written, which is the question the value-provenance
                // columns only make sense next to.
                row.put("written", hasAny(c.path("usages"), "SET", "INSERT", "MERGE_SET"));
                row.put("firstLine", c.path("lines").isArray() && c.path("lines").size() > 0
                        ? c.path("lines").get(0).asInt(0) : 0);
                row.put("seq", seq++);
            }
        }
        r.synthesizedCache.put(cacheKey, out);
        return out;
    }

    /**
     * Matches on the map key, on {@code SCHEMA.TABLE} and on the bare table name, in that order.
     *
     * <p>The bare-name pass is last and only accepted when it is unambiguous. Two schemas can hold
     * a table of the same name, and silently answering with whichever one iterated first would put
     * one schema's columns under the other's heading.
     */
    private JsonNode find(JsonNode all, String target) {
        JsonNode byKey = null, byQualified = null, byBare = null;
        int bareMatches = 0;
        Iterator<Map.Entry<String, JsonNode>> it = all.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            JsonNode v = e.getValue();
            if (!v.isObject()) continue;
            if (upper(e.getKey()).equals(target)) byKey = v;
            String qualified = upper(v.path("schema").asText("") + "." + v.path("tableName").asText(""));
            if (qualified.equals(target)) byQualified = v;
            if (upper(v.path("tableName").asText("")).equals(target)) {
                bareMatches++;
                byBare = v;
            }
        }
        if (byKey != null) return byKey;
        if (byQualified != null) return byQualified;
        return bareMatches == 1 ? byBare : null;
    }

    private static boolean hasAny(JsonNode arr, String... wanted) {
        if (!arr.isArray()) return false;
        for (JsonNode n : arr) {
            for (String w : wanted) if (w.equalsIgnoreCase(n.asText(""))) return true;
        }
        return false;
    }

    private static void copy(ObjectNode row, JsonNode src, String field) {
        row.put(field, src.path(field).asText(""));
    }

    private static void copyArray(ObjectMapper mapper, ObjectNode row, JsonNode src, String field) {
        ArrayNode out = row.putArray(field);
        JsonNode v = src.path(field);
        if (v.isArray()) v.forEach(out::add);
    }

    private static String upper(String s) {
        return s == null ? "" : s.toUpperCase(Locale.ROOT);
    }
}
