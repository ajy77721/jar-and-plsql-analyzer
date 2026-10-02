package com.plsqlanalyzer.web.parser.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.plsqlanalyzer.web.page.FacetSort;
import com.plsqlanalyzer.web.page.FacetSpec;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The paging contract of each converted parser facet, in one place.
 *
 * <p>Kept together rather than spread across the controller methods because the four declarations
 * have to agree with each other and with the tables they serve: the sort names appear verbatim in
 * the JS call sites, and {@code defaultSort} must be the order the table already showed, or page 1
 * silently changes for the user on the day the endpoint is converted.
 *
 * <p>Each {@code id} reproduces the key the reader already merged rows under, so it is unique by
 * construction. That matters more than it looks: a non-unique cursor tiebreaker loses or repeats a
 * row at a page boundary, and only for data where the duplicate exists.
 */
final class ParserFacetSpecs {

    private ParserFacetSpecs() {
    }

    /** How many distinct procedures touch a row — the "Procedures" column. */
    private static long distinctProcedures(JsonNode row) {
        Set<String> seen = new HashSet<>();
        for (String pid : FacetSpec.accessDetailProcedureIds(row)) seen.add(pid);
        return seen.size();
    }

    private static String lower(String s) {
        return s == null ? "" : s.toLowerCase(Locale.ROOT);
    }

    /** Access lines on a table row: {@code usedBy[].operations{op: [line, ...]}}. */
    private static long accessLines(JsonNode row) {
        long total = 0;
        for (JsonNode u : row.path("usedBy")) {
            for (JsonNode lines : u.path("operations")) {
                if (lines.isArray()) total += lines.size();
            }
        }
        return total;
    }

    /** The node ids using a table. The tables index says {@code usedBy}, not {@code accessDetails}. */
    private static List<String> tableProcedureIds(JsonNode row) {
        List<String> out = new java.util.ArrayList<>();
        for (JsonNode u : row.path("usedBy")) {
            String id = u.path("nodeId").asText("");
            if (!id.isEmpty()) out.add(id);
        }
        return out;
    }

    private static long distinctTableProcedures(JsonNode row) {
        return new HashSet<>(tableProcedureIds(row)).size();
    }

    /**
     * Table operations.
     *
     * <h2>Why this one reads different field names</h2>
     *
     * <p>The other specs run over rows the readers already merged into the shape the browser uses.
     * This one runs over {@code api/tables/index.json} as the crawler wrote it, where the fields are
     * {@code name}/{@code schema}/{@code allOperations}/{@code usedBy} rather than
     * {@code tableName}/{@code tableSchema}/{@code operations}/{@code accessDetails}. Mapping is left
     * to the call site, so this stays a description of the artifact rather than of the screen.
     *
     * <p>{@code accessCount} is computed rather than read: it is not a field on the row, it is the
     * size of the access-line structure. Sorting by a field that does not exist would put every row
     * in insertion order and look like a stable sort.
     *
     * <p>The default is Usages descending because the grid sets {@code sortCol 4, desc} immediately
     * after init. Any other default changes page 1 for the user on conversion day.
     */
    static final FacetSpec TABLES = new FacetSpec(
            "tables",
            FacetSort.byName(
                    FacetSort.text("SCHEMA_ASC", "schema", false),
                    FacetSort.text("SCHEMA_DESC", "schema", true),
                    FacetSort.text("NAME_ASC", "name", false),
                    FacetSort.text("NAME_DESC", "name", true),
                    FacetSort.text("TYPE_ASC", "objectType", false),
                    FacetSort.text("TYPE_DESC", "objectType", true),
                    FacetSort.computed("USAGES_ASC", ParserFacetSpecs::accessLines, false),
                    FacetSort.computed("USAGES_DESC", ParserFacetSpecs::accessLines, true),
                    FacetSort.computed("PROCS_ASC", ParserFacetSpecs::distinctTableProcedures, false),
                    FacetSort.computed("PROCS_DESC", ParserFacetSpecs::distinctTableProcedures, true)),
            "USAGES_DESC",
            50,
            // The crawler merges tables by schema.name, so that pair is the row identity.
            row -> row.path("schema").asText("").toUpperCase(Locale.ROOT) + "."
                    + row.path("name").asText("").toUpperCase(Locale.ROOT),
            // Mirrors the grid's searchFn: table name, object type, and the names of the procedures
            // touching it — that last one is why searching "ERP_USP_..." finds the tables it uses.
            row -> {
                StringBuilder b = new StringBuilder();
                b.append(row.path("name").asText("")).append(' ')
                 .append(row.path("objectType").asText("")).append(' ');
                for (JsonNode u : row.path("usedBy")) {
                    b.append(u.path("nodeName").asText("")).append(' ');
                }
                return lower(b.toString());
            },
            row -> FacetSpec.stringArray(row, "allOperations"),
            ParserFacetSpecs::tableProcedureIds,
            Map.of("0", FacetSpec.scalarColumn("schema", ""),
                   "1", FacetSpec.scalarColumn("name", null),
                   "2", FacetSpec.scalarColumn("objectType", "TABLE"),
                   "3", FacetSpec.arrayColumn("allOperations")),
            // "9" — free-text filter on the table name, for callers that want contains/regex over
            // it rather than the exact-match checkbox "1" already gives (kept for backward compat).
            // Picked well clear of 0-4/9 used elsewhere on this spec and PROCEDURES's own "9".
            Map.of("9", row -> row.path("name").asText("")));

    /**
     * Procedures — the navigation list.
     *
     * <h2>Why this facet exists</h2>
     *
     * <p>The node list was served inside {@code /index}, where it is <b>53.1%</b> of the response
     * measured across every analysis on disk, and {@code /index} is fetched on every analysis load.
     * At the 8,000-procedure target that is ~14 MB of node list on a request whose other fields
     * total 446 bytes.
     *
     * <p>The left panel reads 8 of the 21 fields on a node, so the rows are also projected: 90,901
     * bytes of full nodes become 19,846 projected, 4.6× smaller before paging is applied at all.
     *
     * <p>Sorted by depth then name, which is the order the panel already imposed client-side —
     * changing it here would reorder the sidebar on the day the endpoint is switched on.
     */
    static final FacetSpec PROCEDURES = new FacetSpec(
            "procedures",
            FacetSort.byName(
                    FacetSort.text("NAME_ASC", "name", false),
                    FacetSort.text("NAME_DESC", "name", true),
                    FacetSort.text("SCHEMA_ASC", "schema", false),
                    FacetSort.text("SCHEMA_DESC", "schema", true),
                    FacetSort.number("DEPTH_ASC", "depth", false),
                    FacetSort.number("DEPTH_DESC", "depth", true),
                    FacetSort.number("LOC_ASC", "linesOfCode", false),
                    FacetSort.number("LOC_DESC", "linesOfCode", true)),
            "DEPTH_ASC",
            100,
            // nodeId is the crawler's own identity for a node and is unique by construction.
            row -> row.path("nodeId").asText(""),
            // Mirrors the panel's own filter key: id, name and schema, lower-cased.
            row -> lower(row.path("nodeId").asText("") + " " + row.path("name").asText("")
                    + " " + row.path("schema").asText("")),
            row -> List.of(row.path("objectType").asText("")),
            // UI-029: a procedure row belongs to exactly one procedure — itself. Was `List.of()`, a
            // stub that made every scope predicate reject every row; scope/procId were never wired
            // to this endpoint before, so nothing depended on the stub's (wrong) behaviour.
            row -> List.of(row.path("nodeId").asText("")),
            Map.of("0", FacetSpec.scalarColumn("schema", ""),
                   "1", FacetSpec.scalarColumn("objectType", ""),
                   // UI-013/exHandlers.js: a hidden preset filter (`cf=9:Y`), not a user-facing
                   // popover column — lets that screen ask this same endpoint for "procedures with
                   // exception handlers" server-side instead of aggregating PA.nodeIndex.all() in
                   // the browser. "9" is unused by any existing caller of this facet.
                   "9", row -> List.of(row.path("counts").path("exceptionHandlers").asInt(0) > 0
                           ? "Y" : "N")));

    /**
     * Sequences. Merged by {@code schema.name} by the reader, so that is the row identity.
     *
     * <p>The table's own default is Usages descending (it sets that immediately after init), so
     * that is the default here too — not the reader's insertion order, which is what the *legacy*
     * unpaged response still returns untouched.
     */
    static final FacetSpec SEQUENCES = new FacetSpec(
            "sequences",
            FacetSort.byName(
                    FacetSort.text("NAME_ASC", "sequenceName", false),
                    FacetSort.text("NAME_DESC", "sequenceName", true),
                    FacetSort.text("SCHEMA_ASC", "sequenceSchema", false),
                    FacetSort.text("SCHEMA_DESC", "sequenceSchema", true),
                    FacetSort.number("USAGES_ASC", "accessCount", false),
                    FacetSort.number("USAGES_DESC", "accessCount", true),
                    FacetSort.computed("PROCS_ASC", ParserFacetSpecs::distinctProcedures, false),
                    FacetSort.computed("PROCS_DESC", ParserFacetSpecs::distinctProcedures, true)),
            "USAGES_DESC",
            50,
            row -> row.path("sequenceSchema").asText("").toUpperCase(Locale.ROOT) + "."
                    + row.path("sequenceName").asText("").toUpperCase(Locale.ROOT),
            // Mirrors the table's searchFn: name and schema, nothing else.
            row -> lower(row.path("sequenceName").asText("") + " "
                    + row.path("sequenceSchema").asText("")),
            row -> FacetSpec.stringArray(row, "operations"),
            FacetSpec::accessDetailProcedureIds,
            // Column indices are the table's own `data-sort-col` values, so the JS call site and
            // this declaration cannot drift apart silently.
            Map.of("0", FacetSpec.scalarColumn("sequenceName", null),
                   "1", FacetSpec.scalarColumn("sequenceSchema", "-"),
                   "2", FacetSpec.arrayColumn("operations")));

    /**
     * Cursors. The reader emits one row per cursor INSTANCE, keyed by
     * {@code name::nodeId#line}; {@code nodeId} is not a field on the row, so the identity is
     * rebuilt from the first access detail — that same node and the declaration's own line. The
     * line must be part of the identity: Oracle names every implicit FOR-loop cursor
     * {@code "(FOR R)"}, so {@code name::node} alone is not unique per row and a page boundary
     * could drop one of two same-named cursors on one node.
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
            row -> {
                List<String> pids = FacetSpec.accessDetailProcedureIds(row);
                return row.path("cursorName").asText("").toUpperCase(Locale.ROOT) + "::"
                        + (pids.isEmpty() ? "" : pids.get(0))
                        + "#" + FacetSpec.accessDetailFirstLine(row);
            },
            row -> lower(row.path("cursorName").asText("") + " " + row.path("queryText").asText("")),
            row -> List.of(row.path("cursorType").asText("EXPLICIT")),
            FacetSpec::accessDetailProcedureIds,
            Map.of("0", FacetSpec.scalarColumn("cursorName", null),
                   "1", FacetSpec.scalarColumn("cursorType", "EXPLICIT"),
                   "2", FacetSpec.arrayColumn("operations")));

    /** Joins. Merged by the ordered table pair, so that pair is the identity. */
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
            // The table also searches the ON predicates, which live in the access details.
            row -> {
                StringBuilder sb = new StringBuilder();
                sb.append(row.path("leftTable").asText("")).append(' ')
                  .append(row.path("rightTable").asText(""));
                JsonNode details = row.path("accessDetails");
                if (details.isArray()) {
                    for (JsonNode d : details) {
                        sb.append(' ').append(d.path("onPredicate").asText(""));
                    }
                }
                return lower(sb.toString());
            },
            row -> FacetSpec.stringArray(row, "joinTypes"),
            FacetSpec::accessDetailProcedureIds,
            Map.of("2", FacetSpec.arrayColumn("joinTypes")),
            // "0"/"1" are free-text (table names are not an enumerable set); "2" (join types)
            // stays checkbox — it genuinely is a small fixed set, and its pill UI never sent cf=2
            // anyway.
            Map.of("0", row -> row.path("leftTable").asText(""),
                   "1", row -> row.path("rightTable").asText("")));

    /**
     * Dynamic SQL. One row per execution, not merged, so there is no natural key — the identity is
     * the node, line and type, which together address one statement.
     */
    static final FacetSpec DYNAMIC_SQL = new FacetSpec(
            "dynamic SQL",
            FacetSort.byName(
                    FacetSort.text("PROCEDURE_ASC", "procedureName", false),
                    FacetSort.text("PROCEDURE_DESC", "procedureName", true),
                    FacetSort.text("TYPE_ASC", "type", false),
                    FacetSort.text("TYPE_DESC", "type", true),
                    FacetSort.number("LINE_ASC", "lineNumber", false),
                    FacetSort.number("LINE_DESC", "lineNumber", true)),
            "PROCEDURE_ASC",
            25,
            // The expression hash is part of the identity because two executions can share a
            // node, a line and a type (a generated line, a macro); without it one of them would
            // disappear at a page boundary.
            row -> row.path("procedureId").asText("").toUpperCase(Locale.ROOT) + "::"
                    + row.path("lineNumber").asLong(0) + "::" + row.path("type").asText("") + "::"
                    + Integer.toHexString(row.path("sqlExpression").asText("").hashCode()),
            row -> lower(row.path("procedureName").asText("") + " "
                    + row.path("sqlExpression").asText("") + " " + row.path("type").asText("")),
            row -> List.of(row.path("type").asText("DYNAMIC")),
            // Flat rows: the procedure is on the row itself, not in an accessDetails list.
            row -> {
                String pid = row.path("procedureId").asText("");
                if (pid.isEmpty()) pid = row.path("procedureName").asText("");
                return pid.isEmpty() ? List.of() : List.of(pid.toUpperCase(Locale.ROOT));
            },
            // This table has no column-filter popovers today, so it declares none.
            Map.of());

    private static List<String> predicateProcedureIds(JsonNode row) {
        List<String> out = new java.util.ArrayList<>();
        for (JsonNode p : row.path("predicates")) {
            String pid = p.path("procedureId").asText("");
            if (pid.isEmpty()) pid = p.path("procedureName").asText("");
            if (!pid.isEmpty()) out.add(pid.toUpperCase(Locale.ROOT));
        }
        return out;
    }

    /**
     * MID-021 (predicates.js's own paged-conversion project) — {@link
     * com.plsqlanalyzer.web.controller.PlsqlPredicatesFacet#build}'s row shape, exactly as {@code
     * PlsqlFacetSpecs.PREDICATES} (the live-crawl sibling) declares it: {@code tableName}, {@code
     * schemaName}, {@code tableType}, {@code predicateCount}, {@code uniqueColumns}, {@code
     * predicates[]}, {@code operations[]}. Not a pass-through of a summary collection — the
     * aggregation runs once per request in {@code ParserAnalysisController.getPredicates} (raw
     * {@code usedBy[]} tables adapted to {@code accessDetails[]} via {@code
     * ParserPredicatesAdapter}, then built exactly as the live-crawl backend does), so this spec
     * only ever pages already-aggregated rows, never the raw tables document.
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
            ParserFacetSpecs::predicateProcedureIds,
            Map.of("1", FacetSpec.scalarColumn("schemaName", "")));

    /**
     * UI-045/§256 — the cross-facet global search results grid.
     *
     * <p>This spec never pages a facet's own document; it pages an already-built, already-filtered
     * {@code ArrayNode} of normalized rows ({@code {type, name, schema, matchedField, _idx, raw}})
     * that {@code ParserAnalysisController#buildSearchRows} assembles by re-using each of the 7 real
     * facets' own {@code searchFn}/id logic from {@code TABLES}/{@code PROCEDURES}/{@code
     * SEQUENCES}/{@code CURSORS}/{@code JOINS}/{@code DYNAMIC_SQL}/{@code PREDICATES} above — this
     * class does not reimplement matching. Routing that already-normalized array back through {@code
     * FacetPageResponder}/{@code JsonFacetPager} gets cursor pagination, sorting and the {@code cf}
     * column-filter mechanism for free, with the EXACT SAME cursor convention every other facet
     * uses, rather than a bespoke offset scheme.
     *
     * <p>{@code searchText} is deliberately {@code null}: the {@code q} match already happened once,
     * per-facet, while the rows were being normalized, so passing {@code q} through to {@code
     * facets.respond} a second time here would either be a redundant no-op (fine) or, if a caller
     * ever reused this spec directly with a raw {@code q}, would throw per {@code JsonFacetPager}'s
     * own "endpoint has no server-side search" guard — which is the correct, loud failure, not a
     * silent double-filter.
     *
     * <p>{@code cf} column indices: {@code "0"} = {@code type}, {@code "1"} = {@code schema}. These
     * two numbers are the entire contract the new frontend (`plsql/js/search.js`) must match
     * exactly — see that file's own header comment, which restates them, for the §252-class bug
     * class ("a client column index that doesn't match what the backend actually expects") this
     * pairing exists to keep from happening again.
     */
    static final FacetSpec SEARCH = new FacetSpec(
            "search results",
            FacetSort.byName(
                    FacetSort.text("TYPE_ASC", "type", false),
                    FacetSort.text("TYPE_DESC", "type", true),
                    FacetSort.text("NAME_ASC", "name", false),
                    FacetSort.text("NAME_DESC", "name", true),
                    FacetSort.text("SCHEMA_ASC", "schema", false),
                    FacetSort.text("SCHEMA_DESC", "schema", true)),
            "TYPE_ASC",
            50,
            // _idx is stamped onto every normalized row by buildSearchRows purely to keep this id
            // unique — two different facets can otherwise normalize to the same type/name/schema
            // (e.g. a table and a predicate row both named ERP_CUSTOMER.ACCOUNTS).
            row -> row.path("type").asText("") + "::" + row.path("name").asText("") + "::"
                    + row.path("schema").asText("") + "::" + row.path("_idx").asText(""),
            null,
            null,
            null,
            Map.of("0", FacetSpec.scalarColumn("type", ""),
                   "1", FacetSpec.scalarColumn("schema", "")),
            // "2"=name, "3"=matchedField — text-mode (contains/regex), see search.js's own header
            // comment: these two numbers are that file's contract with this spec, same as "0"/"1"
            // above.
            Map.of("2", row -> row.path("name").asText(""),
                   "3", row -> row.path("matchedField").asText("")));

    /* ═══════════════════════ estate facets ═══════════════════════
     * Views/MVs, triggers and package globals. Same paging contract as the seven above; the row
     * shapes are the ones {@code EstateFacetReader} emits (see that class's doc).
     */

    /** Which nodes read a view — {@code usedBy[].nodeId}. */
    private static List<String> viewProcedureIds(JsonNode row) {
        List<String> out = new java.util.ArrayList<>();
        for (JsonNode u : row.path("usedBy")) {
            String id = u.path("nodeId").asText("");
            if (!id.isEmpty()) out.add(id.toUpperCase(Locale.ROOT));
        }
        return out;
    }

    /** Views and materialized views. Merged by {@code schema.name}, so that is the identity. */
    static final FacetSpec VIEWS = new FacetSpec(
            "views",
            FacetSort.byName(
                    FacetSort.text("NAME_ASC", "name", false),
                    FacetSort.text("NAME_DESC", "name", true),
                    FacetSort.text("SCHEMA_ASC", "schema", false),
                    FacetSort.text("SCHEMA_DESC", "schema", true),
                    FacetSort.number("COLUMNS_ASC", "columnCount", false),
                    FacetSort.number("COLUMNS_DESC", "columnCount", true),
                    FacetSort.number("BASETABLES_ASC", "baseTableCount", false),
                    FacetSort.number("BASETABLES_DESC", "baseTableCount", true),
                    FacetSort.number("USEDBY_ASC", "usedByCount", false),
                    FacetSort.number("USEDBY_DESC", "usedByCount", true),
                    // Enrichment counts (EstateFacetReader.viewRow): joins inside the definition and
                    // indexes on the view itself. The detail panels surface the full lists; the sorts
                    // let a caller order the list by them.
                    FacetSort.number("JOINS_ASC", "joinCount", false),
                    FacetSort.number("JOINS_DESC", "joinCount", true),
                    FacetSort.number("INDEXES_ASC", "indexCount", false),
                    FacetSort.number("INDEXES_DESC", "indexCount", true)),
            "NAME_ASC",
            50,
            row -> row.path("schema").asText("").toUpperCase(Locale.ROOT) + "."
                    + row.path("name").asText("").toUpperCase(Locale.ROOT),
            row -> lower(row.path("name").asText("") + " " + row.path("schema").asText("") + " "
                    + row.path("definition").asText("")),
            row -> List.of(row.path("objectType").asText("VIEW")),
            ParserFacetSpecs::viewProcedureIds,
            Map.of("1", FacetSpec.scalarColumn("schema", "")));

    /** Triggers. Merged by {@code schema.name}, so that is the identity. */
    static final FacetSpec TRIGGERS = new FacetSpec(
            "triggers",
            FacetSort.byName(
                    FacetSort.text("NAME_ASC", "name", false),
                    FacetSort.text("NAME_DESC", "name", true),
                    FacetSort.text("TABLE_ASC", "table", false),
                    FacetSort.text("TABLE_DESC", "table", true),
                    FacetSort.text("TIMING_ASC", "timing", false),
                    FacetSort.text("TIMING_DESC", "timing", true),
                    FacetSort.text("EVENT_ASC", "event", false),
                    FacetSort.text("EVENT_DESC", "event", true)),
            "NAME_ASC",
            50,
            row -> row.path("schema").asText("").toUpperCase(Locale.ROOT) + "."
                    + row.path("name").asText("").toUpperCase(Locale.ROOT),
            row -> lower(row.path("name").asText("") + " " + row.path("table").asText("") + " "
                    + row.path("triggerType").asText("") + " " + row.path("status").asText("") + " "
                    + row.path("timing").asText("") + " " + row.path("body").asText("")),
            row -> List.of(row.path("event").asText("")),
            row -> List.of(),
            Map.of("1", FacetSpec.scalarColumn("timing", "")));

    /** Package global variables, one row per package. The package name is the identity. */
    static final FacetSpec GLOBALS = new FacetSpec(
            "global variables",
            FacetSort.byName(
                    FacetSort.text("PACKAGE_ASC", "package", false),
                    FacetSort.text("PACKAGE_DESC", "package", true),
                    FacetSort.number("VARIABLES_ASC", "variableCount", false),
                    FacetSort.number("VARIABLES_DESC", "variableCount", true),
                    FacetSort.number("USAGES_ASC", "usageCount", false),
                    FacetSort.number("USAGES_DESC", "usageCount", true)),
            "PACKAGE_ASC",
            50,
            row -> row.path("package").asText("").toUpperCase(Locale.ROOT),
            row -> {
                StringBuilder b = new StringBuilder(row.path("package").asText(""));
                for (JsonNode v : row.path("variables")) b.append(' ').append(v.path("name").asText(""));
                return lower(b.toString());
            },
            row -> List.of(),
            row -> List.of(),
            Map.of());

    /**
     * Scheduled jobs — the roots of many estates (Oracle DBMS_SCHEDULER/DBMS_JOB, SQL Server
     * Agent, pg_cron, MySQL EVENT). Merged by {@code schema.name}, so that pair is the identity.
     * The scope predicate follows the job's {@code targetNodeId} — a job is "in scope" for the
     * procedure it schedules.
     */
    static final FacetSpec JOBS = new FacetSpec(
            "scheduled jobs",
            FacetSort.byName(
                    FacetSort.text("NAME_ASC", "name", false),
                    FacetSort.text("NAME_DESC", "name", true),
                    FacetSort.text("SCHEMA_ASC", "schema", false),
                    FacetSort.text("SCHEMA_DESC", "schema", true),
                    FacetSort.text("TYPE_ASC", "type", false),
                    FacetSort.text("TYPE_DESC", "type", true),
                    FacetSort.text("SCHEDULE_ASC", "schedule", false),
                    FacetSort.text("SCHEDULE_DESC", "schedule", true),
                    FacetSort.text("TARGET_ASC", "target", false),
                    FacetSort.text("TARGET_DESC", "target", true),
                    FacetSort.text("ENABLED_ASC", "enabled", false),
                    FacetSort.text("ENABLED_DESC", "enabled", true)),
            "NAME_ASC",
            50,
            row -> row.path("schema").asText("").toUpperCase(Locale.ROOT) + "."
                    + row.path("name").asText("").toUpperCase(Locale.ROOT),
            row -> lower(row.path("name").asText("") + " " + row.path("schema").asText("") + " "
                    + row.path("schedule").asText("") + " " + row.path("target").asText("") + " "
                    + row.path("type").asText("")),
            row -> List.of(row.path("type").asText("JOB")),
            row -> {
                String targetNodeId = row.path("targetNodeId").asText("");
                return targetNodeId.isEmpty() ? List.of() : List.of(targetNodeId);
            },
            Map.of("1", FacetSpec.scalarColumn("schema", ""),
                   "2", FacetSpec.scalarColumn("type", "JOB")));
}
