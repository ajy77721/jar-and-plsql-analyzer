package com.plsqlanalyzer.web.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.plsqlanalyzer.web.page.FacetSort;
import com.plsqlanalyzer.web.page.FacetSpec;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * MID-106 — paging contracts for {@link AnalyzerController}, the live-Oracle-crawl PL/SQL app.
 *
 * <h2>Why this is a separate file from {@code ParserFacetSpecs}</h2>
 *
 * <p>That one describes the artifact-store app's facets, over documents read from {@code
 * ArtifactStore} by key. This controller has no such document: it holds one in-memory {@code
 * AnalysisResult} (the live crawl's result), and its collections are already Java objects —
 * {@code List<Map<String,Object>>} for procedures, {@code Collection<TableOperationSummary>} etc.
 * — never serialized to a store between the crawl and the response. Converting each to a {@code
 * JsonNode} array at the controller boundary (via {@code ObjectMapper.valueToTree}) is the
 * cheapest way to reuse the already-tested {@link FacetPageResponder}/{@link FacetSpec} machinery
 * rather than building a second, POJO-native pager for one controller.
 *
 * <h2>Added one endpoint at a time, not all five at once</h2>
 *
 * <p>{@code /procedures} is first because {@link com.plsqlanalyzer.web.controller.detail
 * .CallGraphQueryService#listProcedures} already returns {@code List<Map<String,Object>>} — no
 * POJO-to-map conversion needed, only list-to-JsonNode. The remaining four ({@code /tables},
 * {@code /joins}, {@code /sequences}, {@code /cursors}) return typed POJO collections
 * ({@code TableOperationSummary}, {@code JoinOperationSummary}, ...) and are deliberately left for
 * their own increments — seeing one converted and tested end to end before committing to the same
 * shape four more times, the same reasoning this registry has applied elsewhere to avoid guessing
 * a pattern across files that turn out to differ.
 */
final class PlsqlFacetSpecs {

    private PlsqlFacetSpecs() {
    }

    private static String lower(String s) {
        return s == null ? "" : s.toLowerCase(Locale.ROOT);
    }

    /**
     * Procedures — {@code GraphMapHelper.nodeToMap}'s shape: {@code id}, {@code name}, {@code
     * schemaName}, {@code packageName}, {@code sourceFile}, {@code startLine}/{@code endLine},
     * {@code unitType}, {@code reachable}.
     *
     * <p>No real analysis corpus exists in this repository to measure a natural default order or
     * page size against (the same gap recorded for MID-026/MID-027/MID-106 itself) — {@code
     * NAME_ASC} and 100 are reasoned defaults, matching {@code ParserFacetSpecs.PROCEDURES}'s own
     * page size, not a measurement. Revisit if a real crawl ever populates {@code data/plsql}.
     */
    static final FacetSpec PROCEDURES = new FacetSpec(
            "procedures",
            FacetSort.byName(
                    FacetSort.text("NAME_ASC", "name", false),
                    FacetSort.text("NAME_DESC", "name", true),
                    FacetSort.text("SCHEMA_ASC", "schemaName", false),
                    FacetSort.text("SCHEMA_DESC", "schemaName", true),
                    FacetSort.text("PACKAGE_ASC", "packageName", false),
                    FacetSort.text("PACKAGE_DESC", "packageName", true)),
            "NAME_ASC",
            100,
            // The crawler's own node id is unique by construction — the same identity CallGraph
            // itself keys nodes under.
            row -> row.path("id").asText(""),
            row -> lower(row.path("id").asText("") + " " + row.path("name").asText("")
                    + " " + row.path("schemaName").asText("") + " " + row.path("packageName").asText("")),
            row -> List.of(row.path("unitType").asText("")),
            // A procedure row belongs to itself — same reasoning as ParserFacetSpecs.PROCEDURES
            // (UI-029, §46): scope filtering was never wired to this endpoint before, so there is
            // no existing caller for this to regress.
            row -> List.of(row.path("id").asText("")),
            Map.of("0", FacetSpec.scalarColumn("schemaName", ""),
                   "1", FacetSpec.scalarColumn("unitType", "")));

    /**
     * Tables — {@code TableOperationSummary}'s Jackson shape: {@code tableName}, {@code
     * schemaName}, {@code tableType}, {@code accessCount}, {@code operations} (a {@code
     * Set<SqlOperationType>}, serialised as strings), {@code accessDetails[]} ({@code
     * procedureId}/{@code procedureName}/{@code operation}/{@code lineNumber}/{@code sourceFile}),
     * {@code triggers[]}, {@code external}, {@code metadataUnresolved}.
     *
     * <p>{@code accessCount} is a real stored field here, unlike {@code ParserFacetSpecs.TABLES}
     * where it has to be computed from {@code usedBy[].operations} — this summary already carries
     * it, one less thing to get wrong porting the sort.
     */
    static final FacetSpec TABLES = new FacetSpec(
            "tables",
            FacetSort.byName(
                    FacetSort.text("SCHEMA_ASC", "schemaName", false),
                    FacetSort.text("SCHEMA_DESC", "schemaName", true),
                    FacetSort.text("NAME_ASC", "tableName", false),
                    FacetSort.text("NAME_DESC", "tableName", true),
                    FacetSort.text("TYPE_ASC", "tableType", false),
                    FacetSort.text("TYPE_DESC", "tableType", true),
                    FacetSort.number("USAGES_ASC", "accessCount", false),
                    FacetSort.number("USAGES_DESC", "accessCount", true)),
            "USAGES_DESC",
            50,
            row -> row.path("schemaName").asText("").toUpperCase(Locale.ROOT) + "."
                    + row.path("tableName").asText("").toUpperCase(Locale.ROOT),
            row -> {
                StringBuilder b = new StringBuilder();
                b.append(row.path("tableName").asText("")).append(' ')
                 .append(row.path("tableType").asText("")).append(' ');
                for (JsonNode d : row.path("accessDetails")) {
                    b.append(d.path("procedureName").asText("")).append(' ');
                }
                return lower(b.toString());
            },
            row -> FacetSpec.stringArray(row, "operations"),
            FacetSpec::accessDetailProcedureIds,
            Map.of("0", FacetSpec.scalarColumn("schemaName", ""),
                   "1", FacetSpec.scalarColumn("tableType", "TABLE"),
                   "2", FacetSpec.arrayColumn("operations"),
                   // UI-028: plsql/js/tableOps.js's "External only" / "Enabled triggers only"
                   // checkboxes (cf=3:Y / cf=4:Y) — the same hidden-preset shape UI-029 (§46)
                   // used for exHandlers.js's exception-handler filter.
                   "3", row -> List.of(row.path("external").asBoolean(false) ? "Y" : "N"),
                   "4", row -> {
                       for (JsonNode t : row.path("triggers")) {
                           if ("ENABLED".equals(t.path("status").asText(""))) return List.of("Y");
                       }
                       return List.of("N");
                   }),
            // "9" — free-text filter on the table name; see ParserFacetSpecs.TABLES's identical "9"
            // for why this key, kept in sync across the two backends `plsql/js/tableOps.js` can
            // point at.
            Map.of("9", row -> row.path("tableName").asText("")));

    /**
     * Joins — {@code JoinOperationSummary}'s Jackson shape: {@code leftTable}, {@code rightTable},
     * {@code joinTypes[]}, {@code accessCount}, {@code accessDetails[]} ({@code
     * procedureId}/{@code procedureName}/{@code joinType}/{@code onPredicate}/{@code
     * lineNumber}/{@code sourceFile}). Deliberately the same shape as {@code
     * ParserFacetSpecs.JOINS} field-for-field — this summary and that artifact were written from
     * the same join-pair model, unlike tables/procedures where the two apps' field names diverge.
     */
    static final FacetSpec JOINS = new FacetSpec(
            "joins",
            FacetSort.byName(
                    FacetSort.text("LEFT_ASC", "leftTable", false),
                    FacetSort.text("LEFT_DESC", "leftTable", true),
                    FacetSort.text("RIGHT_ASC", "rightTable", false),
                    FacetSort.text("RIGHT_DESC", "rightTable", true),
                    FacetSort.number("USAGES_ASC", "accessCount", false),
                    FacetSort.number("USAGES_DESC", "accessCount", true)),
            "USAGES_DESC",
            50,
            row -> row.path("leftTable").asText("").toUpperCase(Locale.ROOT) + "::"
                    + row.path("rightTable").asText("").toUpperCase(Locale.ROOT),
            row -> {
                StringBuilder b = new StringBuilder();
                b.append(row.path("leftTable").asText("")).append(' ')
                 .append(row.path("rightTable").asText(""));
                for (JsonNode d : row.path("accessDetails")) {
                    b.append(' ').append(d.path("onPredicate").asText(""));
                }
                return lower(b.toString());
            },
            row -> FacetSpec.stringArray(row, "joinTypes"),
            FacetSpec::accessDetailProcedureIds,
            Map.of("2", FacetSpec.arrayColumn("joinTypes")),
            Map.of("0", row -> row.path("leftTable").asText(""),
                   "1", row -> row.path("rightTable").asText("")));

    /**
     * Sequences — {@code SequenceOperationSummary}'s Jackson shape: {@code sequenceName}, {@code
     * schemaName}, {@code operations[]} (NEXTVAL/CURRVAL), {@code accessCount}, {@code
     * accessDetails[]}.
     */
    static final FacetSpec SEQUENCES = new FacetSpec(
            "sequences",
            FacetSort.byName(
                    FacetSort.text("SCHEMA_ASC", "schemaName", false),
                    FacetSort.text("SCHEMA_DESC", "schemaName", true),
                    FacetSort.text("NAME_ASC", "sequenceName", false),
                    FacetSort.text("NAME_DESC", "sequenceName", true),
                    FacetSort.number("USAGES_ASC", "accessCount", false),
                    FacetSort.number("USAGES_DESC", "accessCount", true)),
            "USAGES_DESC",
            50,
            row -> row.path("schemaName").asText("").toUpperCase(Locale.ROOT) + "."
                    + row.path("sequenceName").asText("").toUpperCase(Locale.ROOT),
            row -> lower(row.path("sequenceName").asText("") + " " + row.path("schemaName").asText("")),
            row -> FacetSpec.stringArray(row, "operations"),
            FacetSpec::accessDetailProcedureIds,
            Map.of("0", FacetSpec.scalarColumn("schemaName", ""),
                   "1", FacetSpec.arrayColumn("operations")));

    /**
     * Cursors — {@code CursorOperationSummary}'s Jackson shape: {@code cursorName}, {@code
     * cursorType}, {@code queryText}, {@code operations[]} (DECLARE/OPEN/FETCH/CLOSE/FOR_LOOP),
     * {@code accessCount}, {@code external}, {@code accessDetails[]}.
     *
     * <p>No schema field on this summary (cursors are PL/SQL-local, not database objects), unlike
     * the other three — the id and search key are built without one accordingly, rather than
     * reading a field that is never present.
     */
    static final FacetSpec CURSORS = new FacetSpec(
            "cursors",
            FacetSort.byName(
                    FacetSort.text("NAME_ASC", "cursorName", false),
                    FacetSort.text("NAME_DESC", "cursorName", true),
                    FacetSort.text("TYPE_ASC", "cursorType", false),
                    FacetSort.text("TYPE_DESC", "cursorType", true),
                    FacetSort.number("USAGES_ASC", "accessCount", false),
                    FacetSort.number("USAGES_DESC", "accessCount", true)),
            "USAGES_DESC",
            50,
            row -> row.path("cursorName").asText("").toUpperCase(Locale.ROOT),
            row -> lower(row.path("cursorName").asText("") + " " + row.path("cursorType").asText("")
                    + " " + row.path("queryText").asText("")),
            row -> FacetSpec.stringArray(row, "operations"),
            FacetSpec::accessDetailProcedureIds,
            Map.of("0", FacetSpec.scalarColumn("cursorType", ""),
                   "1", FacetSpec.arrayColumn("operations")));

    /** The procedures behind a {@code predicates.js} row's {@code predicates[]} — that field, not
     *  {@code accessDetails}, is where {@link PlsqlPredicatesFacet#build} put them. */
    private static java.util.List<String> predicateProcedureIds(JsonNode row) {
        java.util.List<String> out = new java.util.ArrayList<>();
        for (JsonNode p : row.path("predicates")) {
            String pid = p.path("procedureId").asText("");
            if (pid.isEmpty()) pid = p.path("procedureName").asText("");
            if (!pid.isEmpty()) out.add(pid.toUpperCase(Locale.ROOT));
        }
        return out;
    }

    /**
     * Predicates — {@link PlsqlPredicatesFacet#build}'s row shape: {@code tableName}, {@code
     * schemaName}, {@code tableType}, {@code predicateCount}, {@code uniqueColumns}, {@code
     * predicates[]}, {@code operations[]}. Not a pass-through of a summary collection — see
     * {@link PlsqlPredicatesFacet}'s own javadoc for why this one needed its aggregation ported
     * server-side too, not just its transport.
     */
    static final FacetSpec PREDICATES = new FacetSpec(
            "predicates",
            FacetSort.byName(
                    FacetSort.text("SCHEMA_ASC", "schemaName", false),
                    FacetSort.text("SCHEMA_DESC", "schemaName", true),
                    FacetSort.text("NAME_ASC", "tableName", false),
                    FacetSort.text("NAME_DESC", "tableName", true),
                    FacetSort.number("COLUMNS_ASC", "uniqueColumns", false),
                    FacetSort.number("COLUMNS_DESC", "uniqueColumns", true),
                    FacetSort.number("PREDICATES_ASC", "predicateCount", false),
                    FacetSort.number("PREDICATES_DESC", "predicateCount", true)),
            "PREDICATES_DESC",
            50,
            row -> row.path("schemaName").asText("").toUpperCase(Locale.ROOT) + "."
                    + row.path("tableName").asText("").toUpperCase(Locale.ROOT),
            row -> {
                StringBuilder b = new StringBuilder();
                b.append(row.path("tableName").asText("")).append(' ')
                 .append(row.path("schemaName").asText("")).append(' ');
                for (JsonNode p : row.path("predicates")) {
                    b.append(p.path("columnName").asText("")).append(' ')
                     .append(p.path("value").asText("")).append(' ');
                }
                return lower(b.toString());
            },
            row -> FacetSpec.stringArray(row, "operations"),
            PlsqlFacetSpecs::predicateProcedureIds,
            Map.of("1", FacetSpec.scalarColumn("schemaName", "")));
}
