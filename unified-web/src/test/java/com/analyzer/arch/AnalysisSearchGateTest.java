package com.analyzer.arch;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cross-analysis search must stay wired, and must stay honest (§B13).
 *
 * <p>Behaviour is covered by {@code StatementSearchTest} (16 tests over the query language and
 * matcher) and was verified end-to-end against the live endpoint over the real 5,015-row MySQL
 * index. What is left is structural: a missing script tag or an unloaded stylesheet turns the whole
 * feature into a {@code ReferenceError} on click — the §B8 shape — and the two honesty properties
 * are invisible until someone is misled by them.
 */
class AnalysisSearchGateTest {

    private static final Path STATIC = Path.of("src/main/resources/static");

    @Test
    @DisplayName("the search module and its trigger are wired into the page")
    void wiredIntoThePage() throws IOException {
        String html = Files.readString(STATIC.resolve("parser/index.html"));
        assertTrue(html.contains("js/analysisSearch.js"), "analysisSearch.js is not loaded");
        assertTrue(html.contains("id=\"asModal\""), "the results modal markup is missing");
        assertTrue(html.contains("id=\"asQuery\"") && html.contains("id=\"asHeader\"")
                        && html.contains("id=\"asBody\""),
                "the module writes into asQuery/asHeader/asBody; all three must exist");
        assertTrue(html.indexOf("js/api.js") < html.indexOf("js/analysisSearch.js"),
                "analysisSearch.js reads PA.api._analysisName and must load after api.js");
    }

    @Test
    @DisplayName("the result header cannot report a page count as a total")
    void totalIsTheServerTotal() throws IOException {
        String js = Files.readString(STATIC.resolve("parser/js/analysisSearch.js"));
        assertTrue(js.contains("d.total"),
                "the header must show the server's total over every match");
        assertTrue(js.contains("d.truncated"),
                "and must say when it is showing only part of it");
        assertTrue(js.contains("SearchQuery.trim"),
                "the query must be trimmed like every other search in this app");
        assertTrue(js.contains("_seq"),
                "this is the most stale-prone panel here; it needs a request-token guard");
    }

    @Test
    @DisplayName("an unindexed analysis is reported, not answered with zero")
    void unindexedIsNotZero() throws IOException {
        String js = Files.readString(STATIC.resolve("parser/js/analysisSearch.js"));
        assertTrue(js.contains("d.indexed === false"),
                "an analysis with no statement index must say so — answering 'none' to a question "
                        + "that cannot be evaluated is a confident wrong answer");

        String ctl = Files.readString(Path.of(
                "src/main/java/com/plsqlanalyzer/web/parser/controller/PlsqlSearchController.java"));
        assertTrue(ctl.contains("\"indexed\", false"), "the API must distinguish the two cases");
        assertTrue(ctl.contains("badRequest"),
                "a mistyped qualifier must 400 with the valid ones, never return an empty page");
    }

    @Test
    @DisplayName("the statement index is produced at crawl time")
    void producerExists() throws IOException {
        String w = Files.readString(Path.of(
                "../plsql-parser/src/main/java/com/plsql/parser/flow/ChunkedFlowWriter.java"));
        assertTrue(w.contains("writeStatementIndex"),
                "nothing writes api/statements/index.json, so search has no data to read");
        assertTrue(w.contains("columnsKnown"),
                "rows must record whether columns were captured; an empty column list and an "
                        + "uncaptured one are different answers");
    }
}
