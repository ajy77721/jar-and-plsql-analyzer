package com.plsqlanalyzer.web.parser.service;

import com.analyzer.storage.ArtifactKey;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.plsqlanalyzer.web.parser.service.StaticAnalysisReader.textOrNull;

/**
 * The two table-scoped node scans behind the Table Detail modal — PLAN-08 Q59 and Q59b.
 *
 * <p><b>Why this exists at all.</b> Opening the modal used to run two {@code Promise.all} fan-outs
 * in the browser, each fetching <em>every</em> node-detail document in the current call tree — 64
 * documents totalling 3.6 MB on the measured {@code ERP_USP_RUN_CYCLE} analysis, twice, so ~7 MB
 * per modal open, to end up with a few dozen rows. The scan itself is cheap; shipping its input to
 * the client was the cost. It runs here instead, once, cached, and the client asks for a page.
 *
 * <p><b>The row semantics are the browser's, reproduced rather than improved.</b>
 * {@code parser/js/tableDetail.js} is the definition of what a "join on this table" and a "column
 * reference" mean to a user, including two things that look like bugs and are not:
 *
 * <ul>
 *   <li>joins are de-duplicated per <em>relationship and procedure</em> ({@code direction|joinType|
 *       otherTable|procedureId}) and <b>not</b> per line, so a query repeated five times in one
 *       procedure counts once — consistent with how the Accesses tab counts;</li>
 *   <li>both directions are collected: this table joining out, and another table joining in. The
 *       incoming row reports the <em>other</em> table in {@code joinedTable}, because that is the
 *       column the table shows.</li>
 * </ul>
 *
 * <p>Each row carries a {@code seq}, its position in the scan. That is the paging default order,
 * so page 1 is the same rows in the same order the unpaged scan produces, and the cursor has a
 * total order that cannot repeat or skip a row at a page boundary. It is <b>not</b> used as the
 * row identity — re-running an analysis renumbers everything — the de-duplication key is.
 */
class TableDetailFacetReader {

    /** {@code TABLE.COLUMN} / {@code ALIAS.COLUMN} inside a join condition. */
    private static final Pattern QUALIFIED_COLUMN = Pattern.compile(
            "\\b([A-Za-z_][A-Za-z0-9_$#]*)\\s*\\.\\s*([A-Za-z_][A-Za-z0-9_$#]*)\\b");

    private final StaticAnalysisReader r;

    TableDetailFacetReader(StaticAnalysisReader r) {
        this.r = r;
    }

    private JsonNode readTree(ArtifactKey key) throws IOException {
        try (InputStream in = r.openRead(key)) {
            return r.mapper.readTree(in);
        }
    }

    /** Q59 — every join touching {@code table}, in either direction, across the analysis. */
    JsonNode getTableJoins(String name, String table) throws IOException {
        ObjectMapper mapper = r.mapper;
        String target = upper(table);
        String cacheKey = r.cacheKey(name, "tableJoins::" + target);
        JsonNode cached = r.synthesizedCache.get(cacheKey);
        if (cached != null) return cached;

        ArrayNode out = mapper.createArrayNode();
        Set<String> seen = new LinkedHashSet<>();
        for (ArtifactKey nodeFile : r.collectNodeFiles(name)) {
            JsonNode node = readTree(nodeFile);
            String procId = orEmpty(textOrNull(node, "nodeId"));
            String procName = orEmpty(textOrNull(node, "name"));
            String sourceFile = orEmpty(textOrNull(node, "sourceFile"));
            JsonNode tables = node.get("tables");
            if (tables == null || !tables.isArray()) continue;

            for (JsonNode tbl : tables) {
                String baseName = orEmpty(textOrNull(tbl, "name"));
                String baseUpper = upper(baseName);
                JsonNode joins = tbl.get("joins");
                if (joins == null || !joins.isArray()) continue;

                for (JsonNode join : joins) {
                    if (!join.isObject()) continue;
                    String joinType = orEmpty(textOrNull(join, "joinType"));
                    String joined = orEmpty(textOrNull(join, "joinedTable"));

                    if (baseUpper.equals(target)) {
                        String key = "OUT|" + joinType + "|" + upper(joined) + "|" + procId;
                        if (seen.add(key)) {
                            ObjectNode row = row(join, "OUT", procId, procName, sourceFile,
                                    out.size());
                            out.add(row);
                        }
                    }
                    if (upper(joined).equals(target)) {
                        String key = "IN|" + joinType + "|" + baseUpper + "|" + procId;
                        if (seen.add(key)) {
                            ObjectNode row = row(join, "IN", procId, procName, sourceFile,
                                    out.size());
                            // The row shows the OTHER side, which for an incoming join is the
                            // table that owns the FROM clause.
                            row.put("joinedTable", baseName);
                            row.put("joinedTableAlias", orEmpty(textOrNull(tbl, "alias")));
                            out.add(row);
                        }
                    }
                }
            }
        }
        r.synthesizedCache.put(cacheKey, out);
        return out;
    }

    private ObjectNode row(JsonNode join, String direction, String procId, String procName,
                           String sourceFile, int seq) {
        ObjectNode row = ((ObjectNode) join).deepCopy();
        row.put("direction", direction);
        row.put("procedureId", procId);
        row.put("procedureName", procName);
        row.put("sourceFile", sourceFile);
        // Normalised so the client never has to test for absence on the columns it renders.
        if (!row.hasNonNull("joinType")) row.put("joinType", "JOIN");
        if (!row.hasNonNull("joinedTable")) row.put("joinedTable", "");
        if (!row.hasNonNull("joinedTableAlias")) row.put("joinedTableAlias", "");
        if (!row.hasNonNull("condition")) row.put("condition", "");
        if (!row.has("line")) row.put("line", 0);
        row.put("seq", seq);
        return row;
    }

    /**
     * Q59b — every reference to a column of {@code table}: {@code %TYPE} / {@code %ROWTYPE}
     * variables, parameters typed off the table, and columns named in a join condition.
     *
     * <p>One deliberate difference from the browser version: the join-condition scan runs over
     * every node in the analysis, not only the one currently open. The browser's restriction was
     * an artefact of which document it happened to have in hand, and it made the same table report
     * different columns depending on which procedure you had selected.
     */
    JsonNode getTableColumnRefs(String name, String table) throws IOException {
        ObjectMapper mapper = r.mapper;
        String target = upper(table);
        String cacheKey = r.cacheKey(name, "tableColumns::" + target);
        JsonNode cached = r.synthesizedCache.get(cacheKey);
        if (cached != null) return cached;

        ArrayNode out = mapper.createArrayNode();
        Set<String> seen = new LinkedHashSet<>();
        for (ArtifactKey nodeFile : r.collectNodeFiles(name)) {
            JsonNode node = readTree(nodeFile);
            String procId = orEmpty(textOrNull(node, "nodeId"));
            String procName = orEmpty(textOrNull(node, "name"));
            String sourceFile = orEmpty(textOrNull(node, "sourceFile"));

            JsonNode vars = node.path("variables");
            for (String field : List.of("typeRef", "rowtypeRef", "typeRefs", "rowtypeRefs")) {
                JsonNode arr = vars.path(field);
                if (!arr.isArray()) continue;
                for (JsonNode v : arr) {
                    if (!target.equals(upper(orEmpty(textOrNull(v, "refTable"))))) continue;
                    String varName = orEmpty(textOrNull(v, "name"));
                    String refField = orEmpty(textOrNull(v, "refField"));
                    int line = v.path("line").asInt(0);
                    if (!seen.add(procId + "::" + varName + "::" + refField + "::" + line)) continue;
                    out.add(columnRow(mapper, varName, refField,
                            orEmpty(textOrNull(v, "dataType")), line, procName, procId, sourceFile,
                            "TYPE_REF", out.size()));
                }
            }

            JsonNode params = node.path("parameters");
            if (params.isArray()) {
                for (JsonNode p : params) {
                    String dataType = orEmpty(textOrNull(p, "dataType"));
                    if (!upper(dataType).contains(target + ".")) continue;
                    String refField = lastSegment(dataType.replaceAll("(?i)%TYPE$", ""));
                    String varName = orEmpty(textOrNull(p, "name"));
                    if (!seen.add(procId + "::param::" + varName + "::" + refField)) continue;
                    out.add(columnRow(mapper, varName, refField, dataType, p.path("line").asInt(0),
                            procName, procId, sourceFile, "PARAMETER", out.size()));
                }
            }

            JsonNode tables = node.get("tables");
            if (tables == null || !tables.isArray()) continue;
            for (JsonNode tbl : tables) {
                if (!target.equals(upper(orEmpty(textOrNull(tbl, "name"))))) continue;
                JsonNode joins = tbl.get("joins");
                if (joins == null || !joins.isArray()) continue;
                for (JsonNode join : joins) {
                    String condition = orEmpty(textOrNull(join, "condition"));
                    if (condition.isEmpty()) continue;
                    int line = join.path("line").asInt(0);
                    for (String col : columnsOfCondition(condition, target)) {
                        if (!seen.add("join::" + procId + "::" + col + "::" + line)) continue;
                        out.add(columnRow(mapper, "-", col, "", line, procName, procId, sourceFile,
                                "JOIN_CONDITION", out.size()));
                    }
                }
            }
        }
        r.synthesizedCache.put(cacheKey, out);
        return out;
    }

    private static ObjectNode columnRow(ObjectMapper mapper, String varName, String refField,
                                        String dataType, int line, String procName, String procId,
                                        String sourceFile, String refType, int seq) {
        ObjectNode row = mapper.createObjectNode();
        row.put("varName", varName);
        row.put("refField", refField);
        row.put("dataType", dataType);
        row.put("line", line);
        row.put("procedureName", procName);
        row.put("procedureId", procId);
        row.put("sourceFile", sourceFile);
        row.put("refType", refType);
        row.put("seq", seq);
        return row;
    }

    /** The columns of {@code target} named in a join condition, upper-cased, in order. */
    static List<String> columnsOfCondition(String condition, String target) {
        Set<String> cols = new LinkedHashSet<>();
        Matcher m = QUALIFIED_COLUMN.matcher(condition);
        while (m.find()) {
            if (target.equals(m.group(1).toUpperCase(Locale.ROOT))) {
                cols.add(m.group(2).toUpperCase(Locale.ROOT));
            }
        }
        return List.copyOf(cols);
    }

    private static String lastSegment(String s) {
        int dot = s.lastIndexOf('.');
        return dot >= 0 ? s.substring(dot + 1) : s;
    }

    private static String upper(String s) {
        return s == null ? "" : s.toUpperCase(Locale.ROOT);
    }

    private static String orEmpty(String s) {
        return s == null ? "" : s;
    }
}
