package com.analyzer.arch;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The left panel must stay bounded while filtering, and must say what it is hiding (§B11).
 *
 * <h2>The defect</h2>
 *
 * The panel rendered 50 items at a time behind a "Show more" button, which worked — until the user
 * typed in the filter. Filtering called {@code _expandAllLeftItems}, which injected <b>every</b>
 * remaining item into the DOM and then set {@code display:none} on the non-matches. The one action a
 * user takes when a list is too long was also the one action that materialised all of it, so the
 * batching protected every case except the one it existed for.
 *
 * <p>Two disclosure failures went with it. The header read {@code Procedures (114)} whether or not a
 * filter was active, so a list narrowed to three matches was indistinguishable from the whole set.
 * And on the PL/SQL page the flow-tree filter had no {@code else} branch at all — it only ever
 * showed matching nodes and never hid the rest, so filtering the tree changed nothing while
 * appearing to work.
 *
 * <h2>Why a static gate</h2>
 *
 * Both failures are invisible until the data is large enough, and both look like success: an
 * unbounded list renders correctly, just slowly, and a filter that hides nothing still returns. A
 * behavioural test would need a browser and a large fixture to show either.
 *
 * <p>Behaviour was verified separately by driving the shipped {@code leftPanel.js} over the real
 * 114-procedure, 4-schema analysis on this machine: grouping produced 4 groups capped at 50 each
 * (62 items, one "Show more"), the padded query {@code "  erp_fn  "} matched 14 without
 * materialising the other 100, the count read {@code 14 of 114}, and a no-match query rendered zero
 * items rather than all of them. This gate stops that regressing.
 */
class LeftPanelScaleGateTest {

    private static final Path STATIC = Path.of("src/main/resources/static");

    @Test
    @DisplayName("filtering narrows the data, and never expands the whole list first")
    void filterDoesNotMaterialiseEverything() throws IOException {
        String src = Files.readString(STATIC.resolve("parser/js/leftPanel.js"));

        // Matched as "PA._expandAllLeftItems", the call/definition form, rather than the bare
        // name: the bare name also appears in this file's own header comment explaining the defect,
        // and a gate that trips on its own documentation is a gate someone deletes.
        assertFalse(src.contains("PA._expandAllLeftItems"),
                "the filter expands every item into the DOM before hiding the non-matches, which "
                        + "defeats the batching precisely when it is needed");
        assertTrue(src.contains("PA._lpApplyFilter"),
                "filtering must narrow the backing array, not walk the DOM");
        assertTrue(src.contains("PA._LP_BATCH = 50"),
                "the panel must stay batched at the size it was measured against");
    }

    @Test
    @DisplayName("both panels disclose how much the filter is hiding")
    void filteredCountsAreDisclosed() throws IOException {
        String parser = Files.readString(STATIC.resolve("parser/js/leftPanel.js"));
        assertTrue(parser.contains("' of '"),
                "parser/js/leftPanel.js must report 'N of M' when a filter is active; a bare "
                        + "smaller number reads as the total");
        assertTrue(parser.contains("lpProcCount") && parser.contains("lpTableCount"),
                "both lists need a count element to write that into");

        String plsql = Files.readString(STATIC.resolve("plsql/js/app.js"));
        assertTrue(plsql.contains("lpFilterCount"),
                "the PL/SQL flow tree must report what its filter is hiding");
        assertTrue(Files.readString(STATIC.resolve("plsql/index.html")).contains("id=\"lpFilterCount\""),
                "plsql/index.html has no lpFilterCount element, so the count is written nowhere — "
                        + "the shape of the §B8 defect");
    }

    @Test
    @DisplayName("the PL/SQL tree filter hides non-matching nodes")
    void treeFilterActuallyHides() throws IOException {
        String plsql = Files.readString(STATIC.resolve("plsql/js/app.js"));
        int at = plsql.indexOf("PA.filterLeft");
        assertTrue(at >= 0, "PA.filterLeft not found");
        String fn = plsql.substring(at, Math.min(plsql.length(), at + 3000));

        assertTrue(fn.contains("node.style.display = 'none'"),
                "the tree filter has no branch that hides a non-matching node, so filtering the "
                        + "flow tree changes nothing visible while appearing to work");
        // Deepest-first matters: a parent asks whether anything inside it survived, so its children
        // must already have been decided. In document order every ancestor stays visible — the same
        // bug, one level up.
        assertTrue(fn.contains(".reverse()"),
                "tree nodes must be walked deepest-first, or a parent's 'keeps a child' test reads "
                        + "children that have not been hidden yet");
    }
}
