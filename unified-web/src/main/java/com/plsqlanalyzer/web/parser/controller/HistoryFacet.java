package com.plsqlanalyzer.web.parser.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.plsqlanalyzer.web.page.FacetSort;
import com.plsqlanalyzer.web.page.FacetSpec;
import com.plsqlanalyzer.web.parser.model.AnalysisInfo;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * The paging and faceting contract of the analysis history — Q31 / I11.
 *
 * <p>This one list feeds two screens: the History cards and the Performance Summary table. Both
 * recompute the same things in the browser, over the whole history, on every keystroke — the
 * datasource and project dropdown values, eight running totals for the stat cards, and the dialect
 * of every row derived by scanning its {@code jdbcUrl} for a substring. That scanner exists twice,
 * in {@code history.js:101-109} and {@code perfSummary.js:13-22}, and <b>the two copies disagree</b>:
 * one falls back to an em dash for an unrecognised URL, the other to {@code ORACLE}. PLAN-08 §4.3
 * #13 asks for both to be deleted in favour of one server-side value, which is what {@code dbType}
 * below is.
 *
 * <p><b>{@code errorCount}, not {@code errors[]}.</b> Both screens read the error list only for its
 * length — a count for a column, and a boolean for a status badge. Sending the messages so the
 * browser can call {@code .length} on them is 9% of this response at the measured size, and grows
 * with how badly an analysis went rather than with anything the list view shows.
 *
 * <p><b>Honest note on size.</b> At the size measured here — 16 analyses, 8.2 KB — paging this list
 * saves nothing: one page holds all of it. What it fixes is that the response had no bound at all,
 * so it grew linearly with every analysis ever run, and that the search and the sort had nowhere to
 * go but the browser. The projection is the part that pays off today.
 */
final class HistoryFacet {

    private HistoryFacet() {
    }

    /* ── the row ─────────────────────────────────────────────────────────────────────────── */

    /**
     * One history row as the two screens actually render it.
     *
     * <p>{@code errors} is replaced by {@code errorCount}; everything else the legacy response
     * carries is kept, because both screens read nearly all of it and dropping a field to save
     * bytes only to have a column render blank is not a saving.
     */
    static ObjectNode row(ObjectMapper mapper, AnalysisInfo a) {
        ObjectNode n = mapper.createObjectNode();
        n.put("name", a.getName());
        n.put("entryPoint", a.getEntryPoint());
        n.put("entrySchema", a.getEntrySchema());
        n.put("totalNodes", a.getTotalNodes());
        n.put("totalTables", a.getTotalTables());
        n.put("totalEdges", a.getTotalEdges());
        n.put("totalLinesOfCode", a.getTotalLinesOfCode());
        n.put("maxDepth", a.getMaxDepth());
        n.put("crawlTimeMs", a.getCrawlTimeMs());
        n.put("totalDurationMs", a.getTotalDurationMs());
        n.put("parseMs", a.getParseMs());
        n.put("discoverMs", a.getDiscoverMs());
        n.put("saveMs", a.getSaveMs());
        n.put("dbCallCount", a.getDbCallCount());
        n.put("timestamp", a.getTimestamp());
        n.put("project", a.getProject());
        n.put("environment", a.getEnvironment());
        n.put("connectionName", a.getConnectionName());
        // No jdbcUrl. Its only two readers on this list were the duplicated dialect scanners that
        // `dbType` replaces — nothing renders the URL itself here, so shipping it was shipping an
        // input to a computation that has moved.
        n.put("dbType", dbType(a));
        n.put("errorCount", a.getErrors() == null ? 0 : a.getErrors().size());
        return n;
    }

    /**
     * The datasource dialect, derived once, here.
     *
     * <p>Reproduces what {@code perfSummary.js} did, including PostgreSQL — which the server's own
     * existing {@code AnalysisService.dialectOf} omits, so two of the sixteen analyses in the
     * measured data would have been reported as {@code UNKNOWN} had this reused it. The clients
     * keep their own fallback for a row that predates this field; they no longer scan a URL.
     */
    static String dbType(AnalysisInfo a) {
        String u = a.getJdbcUrl() == null ? "" : a.getJdbcUrl().toLowerCase(Locale.ROOT);
        if (u.contains("sqlserver")) return "SQLSERVER";
        if (u.contains("oracle")) return "ORACLE";
        if (u.contains("mysql")) return "MYSQL";
        if (u.contains("db2")) return "DB2";
        if (u.contains("postgresql")) return "POSTGRES";
        return "";
    }

    /* ── the sorts ───────────────────────────────────────────────────────────────────────── */

    /**
     * Default {@code TIME_DESC}: {@code listAnalyses()} already returns newest first, and page 1
     * must not change under the user on the day this endpoint is paged.
     *
     * <p>The names are the Performance Summary table's own sort columns so the JS call site and
     * this declaration cannot drift. {@code duration} falls back from {@code totalDurationMs} to
     * {@code crawlTimeMs} exactly as the client comparator does, or a row analysed before the
     * former existed would sort as zero.
     */
    static final FacetSpec SPEC = new FacetSpec(
            "analyses",
            FacetSort.byName(
                    FacetSort.number("TIME_DESC", "timestamp", true),
                    FacetSort.number("TIME_ASC", "timestamp", false),
                    FacetSort.text("NAME_ASC", "name", false),
                    FacetSort.text("NAME_DESC", "name", true),
                    FacetSort.text("ENTRY_ASC", "entryPoint", false),
                    FacetSort.text("ENTRY_DESC", "entryPoint", true),
                    FacetSort.text("DATASOURCE_ASC", "dbType", false),
                    FacetSort.text("DATASOURCE_DESC", "dbType", true),
                    FacetSort.computed("DURATION_ASC", HistoryFacet::durationMs, false),
                    FacetSort.computed("DURATION_DESC", HistoryFacet::durationMs, true),
                    FacetSort.number("DBCALLS_ASC", "dbCallCount", false),
                    FacetSort.number("DBCALLS_DESC", "dbCallCount", true),
                    FacetSort.number("PROCS_ASC", "totalNodes", false),
                    FacetSort.number("PROCS_DESC", "totalNodes", true),
                    FacetSort.number("TABLES_ASC", "totalTables", false),
                    FacetSort.number("TABLES_DESC", "totalTables", true),
                    FacetSort.number("LOC_ASC", "totalLinesOfCode", false),
                    FacetSort.number("LOC_DESC", "totalLinesOfCode", true),
                    FacetSort.number("ERRORS_ASC", "errorCount", false),
                    FacetSort.number("ERRORS_DESC", "errorCount", true)),
            "TIME_DESC",
            25,
            // The analysis name is the natural key and is unique by construction — it is the folder
            // the analysis lives in.
            row -> row.path("name").asText("").toUpperCase(Locale.ROOT),
            HistoryFacet::searchText,
            null,
            null,
            Map.of("dbType", FacetSpec.scalarColumn("dbType", null),
                   "project", FacetSpec.scalarColumn("project", null),
                   "environment", FacetSpec.scalarColumn("environment", null),
                   "connectionName", FacetSpec.scalarColumn("connectionName", null)));

    private static long durationMs(JsonNode row) {
        long total = row.path("totalDurationMs").asLong(0L);
        return total > 0 ? total : row.path("crawlTimeMs").asLong(0L);
    }

    /**
     * The union of the two screens' search fields, so one {@code q} serves both.
     *
     * <p>History searched entry point, schema, name, connection and project; the Performance
     * Summary added the formatted timestamp. The formatted form is locale-dependent and cannot be
     * reproduced here, so it is dropped — searching for a rendered date string is the one
     * capability this conversion loses, and it is named rather than left to be discovered.
     */
    private static String searchText(JsonNode row) {
        return (row.path("entryPoint").asText("") + ' '
                + row.path("entrySchema").asText("") + ' '
                + row.path("name").asText("") + ' '
                + row.path("connectionName").asText("") + ' '
                + row.path("project").asText("")).toLowerCase(Locale.ROOT);
    }

    /* ── the summary ─────────────────────────────────────────────────────────────────────── */

    /**
     * Everything the two toolbars and the eight stat cards used to compute in the browser over the
     * whole history, on every keystroke.
     *
     * <p>{@code maxDurationMs} is in here because the Performance Summary's time bar is a
     * percentage of the longest run <i>in the filtered set</i> — a value a single page cannot know,
     * and the reason a naive conversion of that table would draw every bar wrong.
     *
     * <p><b>The two lists are scoped differently, on purpose.</b> The aggregates describe the
     * <i>filtered</i> set, because that is what the stat cards claim to show. The dropdown values
     * describe <i>every</i> analysis, because a dropdown that only offers the values surviving its
     * own filter cannot be used to change the filter — pick "Db2" and Oracle vanishes from the
     * list. The client-side original had the same split for the same reason.
     *
     * @param all     every row, for the dropdown value lists
     * @param matched the rows surviving the request's filter, for the aggregates
     */
    static ObjectNode summary(ObjectMapper mapper, List<ObjectNode> all, List<ObjectNode> matched) {
        long totalTime = 0, totalDb = 0, totalProcs = 0, totalTables = 0, totalLoc = 0;
        long maxDuration = 0;
        int withErrors = 0;
        Map<String, Integer> dsCounts = new TreeMap<>();
        // project -> its datasource. A project has exactly one, which is what makes the two
        // dropdowns cascade; preserved here rather than rebuilt client-side.
        Map<String, String> projectToDs = new LinkedHashMap<>();

        for (ObjectNode r : all) {
            String ds = r.path("dbType").asText("");
            if (!ds.isEmpty()) dsCounts.merge(ds, 1, Integer::sum);
            String project = r.path("project").asText("");
            if (!project.isEmpty()) projectToDs.putIfAbsent(project, ds);
        }

        for (ObjectNode r : matched) {
            long ms = durationMs(r);
            totalTime += ms;
            if (ms > maxDuration) maxDuration = ms;
            totalDb += r.path("dbCallCount").asLong(0);
            totalProcs += r.path("totalNodes").asLong(0);
            totalTables += r.path("totalTables").asLong(0);
            totalLoc += r.path("totalLinesOfCode").asLong(0);
            if (r.path("errorCount").asInt(0) > 0) withErrors++;
        }

        ObjectNode out = mapper.createObjectNode();
        out.put("total", matched.size());
        out.put("unfilteredTotal", all.size());
        out.put("withErrors", withErrors);
        out.put("success", matched.size() - withErrors);
        out.put("totalDurationMs", totalTime);
        out.put("maxDurationMs", maxDuration);
        out.put("dbCallCount", totalDb);
        out.put("totalNodes", totalProcs);
        out.put("totalTables", totalTables);
        out.put("totalLinesOfCode", totalLoc);
        ObjectNode counts = out.putObject("dsCounts");
        dsCounts.forEach(counts::put);
        ObjectNode projects = out.putObject("projectToDs");
        projectToDs.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> projects.put(e.getKey(), e.getValue()));
        ArrayNode sorts = out.putArray("sorts");
        SPEC.sorts().keySet().forEach(sorts::add);
        return out;
    }
}
