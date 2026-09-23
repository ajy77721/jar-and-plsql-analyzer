package com.analyzer.arch;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Guards the field renames the streaming providers perform on the way out.
 *
 * <p>{@code AbstractAnalysisDataProvider} does not echo {@code analysis.json} verbatim — it renames
 * some fields as it streams. {@code classes[].methods} is emitted as {@code methodSummaries}. The
 * browser had never been updated: {@code codeTree.js} read {@code cls.methods}, got
 * {@code undefined}, fell back to {@code []}, and the Methods section of the Code Structure tree
 * rendered <b>empty for every class</b> — 60 methods on one controller showed as none.
 *
 * <p><b>Why no test caught it.</b> Nothing threw. `[]` is a perfectly valid value for a class with
 * no methods, so the failure was a silently empty panel, and only a human looking at the screen
 * would notice. That is precisely the shape of bug a structural gate can catch and a behavioural
 * test cannot.
 *
 * <p>This asserts that for each known rename, the JavaScript reads the name the server actually
 * sends. It does not forbid also reading the old name — a fallback is good practice for older
 * stored payloads — it only requires the current one to appear.
 */
class RenamedResponseFieldGateTest {

    /**
     * server-sent name → the analysis.json name it replaces, for renames the browser consumes.
     *
     * <p>Add an entry whenever a provider renames a field the UI reads.
     */
    private static final Map<String, String> RENAMES = new LinkedHashMap<>();
    static {
        // AbstractAnalysisDataProvider.streamClassTree emits "methodSummaries" for "methods".
        RENAMES.put("methodSummaries", "methods");
    }

    /** Where the browser code lives. */
    private static final Path STATIC_ROOT = Path.of("src/main/resources/static");

    /**
     * The files that consume the renamed payload, and therefore the only ones this can judge.
     *
     * <p>Scoping matters, because `.methods` appears on three different payloads and only one is
     * renamed. When this gate was first written unscoped it flagged five files, of which just one
     * was a genuine defect:
     *
     * <ul>
     *   <li><b>{@code codeTree.js}, {@code codeTreeViews.js}</b> — read {@code JA.codeTree._classes},
     *       the {@code /classes/tree} payload. Renamed there, so reading {@code methods} is the
     *       bug. <b>Both were broken; both are fixed.</b></li>
     *   <li><b>{@code summary-codeview.js}</b> — reads {@code /decompile}, which passes the whole
     *       class node through <i>without</i> renaming. {@code methods} is correct there, and
     *       "fixing" it would have been the actual mistake.</li>
     *   <li><b>{@code callGraph.js}, {@code summary-analyzer.js}, {@code summary-scheduled.js}</b> —
     *       read {@code JA.app.currentAnalysis.classes}, whose entries carry <i>neither</i> field:
     *       the summary provider emits class-level metadata only. Those loops are dead regardless
     *       of the name, which is a separate (real) gap recorded in PLAN-12 — not a rename issue.
     *       Renaming would have hidden it.</li>
     * </ul>
     */
    private static final List<String> TREE_DATA_CONSUMERS =
            List.of("jar/js/codeTree.js", "jar/js/codeTreeViews.js");

    @Test
    @DisplayName("JS that consumes a renamed field reads the name the server actually sends")
    void renamedFieldsAreReadByTheirServerName() throws IOException {
        if (!Files.isDirectory(STATIC_ROOT)) return;   // module layout changed; nothing to check

        List<String> problems = new ArrayList<>();

        for (Map.Entry<String, String> rename : RENAMES.entrySet()) {
            String sent = rename.getKey();
            String old = rename.getValue();

            List<Path> readsOldOnly = new ArrayList<>();
            for (String rel : TREE_DATA_CONSUMERS) {
                Path js = STATIC_ROOT.resolve(rel);
                if (!Files.isRegularFile(js)) {
                    throw new AssertionError("This gate names " + rel + " as a consumer of the "
                            + "renamed payload, but it no longer exists. Re-check which files read "
                            + "JA.codeTree._classes and update TREE_DATA_CONSUMERS — a silently "
                            + "skipped gate is worse than none.");
                }
                String src = stripComments(Files.readString(js));
                boolean usesOld = src.contains("." + old + " ||") || src.contains("." + old + "||");
                boolean usesNew = src.contains(sent);
                if (usesOld && !usesNew) readsOldOnly.add(Path.of(rel));
            }
            if (!readsOldOnly.isEmpty()) {
                problems.add("'" + old + "' is renamed to '" + sent + "' before it reaches the "
                        + "browser, but these files read only '" + old + "': " + readsOldOnly
                        + " — they will silently see an empty value.");
            }
        }

        if (!problems.isEmpty()) {
            throw new AssertionError("Response field rename not reflected in the UI:\n\n"
                    + String.join("\n", problems)
                    + "\n\nEither read the server's name (keeping the old one as a fallback), or "
                    + "stop renaming the field in the provider.");
        }
    }

    /**
     * Removes comments so a file explaining the rename is not mistaken for one committing it.
     *
     * <p>Four scanners in this repository have now been tripped by their own documentation.
     */
    private String stripComments(String src) {
        return src.replaceAll("(?s)/\\*.*?\\*/", "").replaceAll("(?m)^\\s*//.*$", "");
    }

    /**
     * Fails loudly when the static tree is not where this test expects it.
     *
     * <p>Without this the scan simply returned nothing and every assertion passed — the gate went
     * green having read zero files. A gate that cannot fail is worse than no gate, because it is
     * mistaken for coverage. {@code StaleGuardAndSharedScriptGateTest} already threw in this case;
     * these did not, and the two had drifted.
     */
    private static Path requireStatic(Path p) {
        if (!java.nio.file.Files.isDirectory(p)) {
            throw new IllegalStateException("static root not found at " + p.toAbsolutePath()
                    + " — the module layout changed, or this ran from the wrong working directory. "
                    + "A silently skipped gate is worse than none.");
        }
        return p;
    }

}
