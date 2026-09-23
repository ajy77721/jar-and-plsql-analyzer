package com.analyzer.arch;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Access Details panel must stay a bounded, shared table (§B9).
 *
 * <p>It used to render as a flat unbounded {@code <div>} list — one node per access, all built and
 * inserted at once. Measured against the analysis on this machine, one table produces <b>1,925</b>
 * access rows, and there was no cap, no filter and no sort. Opening it locked the tab, and once open
 * the list could not be narrowed.
 *
 * <p>Two properties are pinned here, and both are structural because neither is reachable by a
 * behavioural test: an unbounded list looks identical to a bounded one until the data is large
 * enough, and a missing script tag only shows as a {@code ReferenceError} at click time — the §B8
 * failure shape exactly.
 *
 * <ol>
 *   <li><b>Both pages load the shared module, before the code that calls it.</b> {@code parser/} and
 *       {@code plsql/} each had their own copy of this renderer and had already drifted; the popup
 *       modal needs the same panel. A third copy would guarantee further divergence.</li>
 *   <li><b>Neither caller loops over {@code accessDetails} to build markup any more.</b> That is the
 *       shape of the original defect, and it is what would come back if someone re-inlined it.</li>
 * </ol>
 *
 * <p>Behavioural verification was done separately by running the shipped {@code tableFramework.js}
 * and {@code access-details.js} over real analysis data: 184 rows with 3 operations paged to 50,
 * a padded query {@code "  USP_RUNMRPCYCLE  "} matched 13, the {@code SELECT} pill narrowed to 15,
 * and sorting ordered by line — identical results against <i>both</i> pages' diverged copies of the
 * framework. This gate exists to stop that state regressing, not to re-prove it.
 */
class AccessDetailsPanelGateTest {

    private static final Path STATIC = Path.of("src/main/resources/static");

    /** The pages that render the panel. */
    private static final List<String> PAGES = List.of("parser/index.html", "plsql/index.html");

    @Test
    @DisplayName("both pages load the shared panel before the code that calls it")
    void sharedModuleIsLoadedBeforeItsCallers() throws IOException {
        for (String page : PAGES) {
            String html = Files.readString(STATIC.resolve(page));
            int panel = html.indexOf("shared/access-details.js");
            int caller = html.indexOf("js/tableOps.js");
            // Matched on the filename, not on a directory. The path moved when the parser and
            // plsql forks were merged into shared/, and a gate keyed to the old location fails for
            // a change that is entirely correct — which teaches people to edit gates rather than
            // read them. What this gate is actually about is ORDER, so it should not care where
            // the file lives.
            int framework = html.indexOf("tableFramework.js");

            assertTrue(panel >= 0, page + " does not load shared/access-details.js, so every "
                    + "expanded table row throws ReferenceError: AccessDetails is not defined");
            assertTrue(caller >= 0, page + " unexpectedly does not load tableOps.js");
            assertTrue(panel < caller, page + " loads access-details.js after tableOps.js");
            assertTrue(framework >= 0 && framework < panel,
                    page + " must load tableFramework.js before access-details.js, which drives PA.tf");
        }
    }

    @Test
    @DisplayName("neither caller rebuilds the access list inline")
    void callersDelegateRatherThanLoop() throws IOException {
        for (String js : List.of("parser/js/tableOps.js", "plsql/js/tableOps.js")) {
            String src = Files.readString(STATIC.resolve(js));
            assertTrue(src.contains("AccessDetails.render") && src.contains("AccessDetails.mount"),
                    js + " no longer delegates to the shared panel");

            // The dead branches went with the rewrite and must not come back: nothing populates
            // whereFilters on this path, so it rendered for nobody and implied filter detail the
            // analysis does not actually have.
            assertFalse(src.contains("whereFilters"),
                    js + " reintroduced the whereFilters branch, which no data ever populates");

            // The original defect: the caller building the "Access Details (N)" list itself. The
            // title is now emitted only by the shared panel, so its presence here means the inline
            // list came back.
            //
            // Checked by that exact marker rather than by "does this file contain to-detail-item":
            // both callers still render a "Tables via Triggers" list with that class, which is a
            // different and genuinely small list (triggers per table), and flagging it would be a
            // false positive of the kind that gets a gate switched off.
            assertFalse(src.contains("Access Details ("),
                    js + " builds the Access Details list inline again — it must stay paged through "
                            + "the shared panel, which reaches 1,925 rows on real data");
        }
    }

    @Test
    @DisplayName("the panel caps rows and searches with a trimmed query")
    void panelIsBoundedAndTrims() throws IOException {
        String src = Files.readString(STATIC.resolve("shared/access-details.js"));
        assertTrue(src.contains("PAGE_SIZE = 50"),
                "the panel must stay paged at the same 50 as PA.tf's other call sites");
        assertTrue(src.contains("PA.tf.init"), "paging must come from PA.tf, not be reinvented");
        assertTrue(src.contains("SearchQuery.normUpper"),
                "the panel's own search box must trim, like every other search in the app");
        // Only PA.tf's common API may be used: the two pages' copies of the framework have diverged
        // (parser has server-paging, plsql has column filters), so anything outside the shared
        // surface works on one page and silently fails on the other.
        for (String forbidden : List.of("PA.tf.initColFilters", "PA.tf.isServerPaged",
                                        "PA.tf.setServerSort", "PA.tf.registerColumnFilters")) {
            assertFalse(src.contains(forbidden), "shared/access-details.js uses " + forbidden
                    + ", which exists in only one page's copy of tableFramework.js");
        }
    }
}
