package com.analyzer.arch;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The call trace must stay readable at real depth (§B12).
 *
 * <h2>Measured, not assumed</h2>
 *
 * The largest trace served by this machine is {@code ERP$ERP_USP_RUN_CYCLE}: <b>1,250 nodes at depth
 * 71</b>. Indentation was {@code depth * 20}px with {@code .ct-indent { flex-shrink: 0 }}, so the
 * deepest rows carried <b>1,420px</b> of leading whitespace. In a panel a few hundred pixels wide
 * the procedure name sat entirely outside the viewport — the bottom of a deep trace was not cramped,
 * it was unreadable.
 *
 * <p>Capping the indent is safe because <i>every</i> row already carries an {@code L<depth>} badge.
 * The cap loses the visual nesting past 12 levels; it does not lose the depth.
 *
 * <h2>The second defect: "Expand All" expanded one level</h2>
 *
 * It took a single {@code querySelectorAll} snapshot and expanded what was in it. Expanding a node
 * inserts <i>its</i> children, which carry their own unexpanded containers — and those were not in
 * the snapshot. On a deep trace one click expanded one more level and the button looked broken. It
 * now repeats until a pass finds nothing left.
 *
 * <p>Both properties are structural here because neither is reachable without a browser: 1,420px of
 * indentation renders perfectly, just off-screen, and a partial expand returns normally.
 */
class TraceReadabilityGateTest {

    private static final Path STATIC = Path.of("src/main/resources/static");

    /**
     * Every renderer that indents by depth.
     *
     * <p>This list had two entries and there were three. {@code jar/js/callGraph.js} indented by
     * {@code op.depth * 20} with no cap and no depth label, and it is loaded live by
     * {@code jar/index.html} — so the gate reported the rule enforced while the jar page still had
     * the exact defect. A list of files IS the blind spot in a gate like this, which is why the
     * companion test below derives its own list from the source instead of trusting this one.
     */
    private static final List<String> RENDERERS =
            List.of("parser/js/callGraph.js", "plsql/js/callGraph.js", "jar/js/callGraph.js");

    @Test
    @DisplayName("no asset anywhere indents by an uncapped depth")
    void noUncappedDepthIndentAnywhere() throws IOException {
        // Derived, not listed. The hardcoded RENDERERS list is what let the jar page keep the bug
        // through a green gate; this scans every asset so a fourth renderer cannot hide the same
        // way. Matches `<anything>depth * 20` that is not already wrapped in Math.min.
        Path root = STATIC;
        List<String> bad = new ArrayList<>();
        try (Stream<Path> w = Files.walk(root)) {
            for (Path f : w.filter(Files::isRegularFile)
                           .filter(p -> p.toString().endsWith(".js"))
                           .filter(p -> !p.toString().contains("xlsx"))
                           .sorted().toList()) {
                String src = Files.readString(f);
                Matcher m = UNCAPPED_INDENT.matcher(src);
                while (m.find()) {
                    int line = (int) src.substring(0, m.start()).chars().filter(c -> c == '\n').count() + 1;
                    bad.add(root.relativize(f) + ":" + line + "  " + m.group().trim());
                }
            }
        }
        assertTrue(bad.isEmpty(),
                "these indent by depth with no cap; at the depth-71 trace measured here that is "
                        + "1,420px of whitespace and the row content leaves the viewport:\n  "
                        + String.join("\n  ", bad));
    }

    /** `depth * 20` not already inside a Math.min(...) cap. */
    private static final Pattern UNCAPPED_INDENT =
            Pattern.compile("(?<!Math\\.min\\()\\b\\w*[Dd]epth\\s*\\*\\s*20\\b");

    @Test
    @DisplayName("indentation is capped on every trace renderer")
    void indentIsCapped() throws IOException {
        for (String js : RENDERERS) {
            String src = Files.readString(STATIC.resolve(js));
            assertFalse(src.contains("const indent = depth * 20;"),
                    js + " indents by depth without limit; at the depth-71 trace measured here that "
                            + "is 1,420px of whitespace and the row label leaves the viewport");
            // Matched by shape, not by one exact spelling. The three renderers legitimately differ
            // — `depth` vs `op.depth`, `PA.callTrace._MAX_INDENT_LEVELS` vs
            // `JA.callGraph.MAX_INDENT_LEVELS` — and an assertion pinned to one of them reported
            // the jar renderer broken after it had been fixed.
            assertTrue(src.matches("(?s).*Math\\.min\\([^)]*[Dd]epth[^)]*\\)\\s*\\*\\s*20.*"),
                    js + " must cap the indent");
        }
        // A named constant, not a literal buried mid-expression.
        assertTrue(Files.readString(STATIC.resolve("parser/js/callGraph.js"))
                        .contains("_MAX_INDENT_LEVELS: 12"),
                "the cap must be a named constant, not a literal buried in the renderer");
        assertTrue(Files.readString(STATIC.resolve("jar/js/callGraph.js"))
                        .contains("MAX_INDENT_LEVELS: 12"),
                "the jar renderer needs the same named cap");
    }

    @Test
    @DisplayName("depth is still disclosed on every row, which is what makes the cap safe")
    void depthRemainsVisible() throws IOException {
        for (String js : RENDERERS) {
            String src = Files.readString(STATIC.resolve(js));
            assertTrue(src.contains("ct-depth-badge"),
                    js + " has no per-row depth badge; with the indent capped, nothing would tell "
                            + "the reader how deep a row actually is");
            // Exactly one emitter. A second was added during this work and duplicated a badge that
            // already existed on all 1,250 rows — caught by counting rendered badges against nodes
            // (2,118 vs 1,250), and removed. The count is pinned so it cannot come back.
            assertEquals(1, src.split("ct-depth-badge", -1).length - 1,
                    js + " emits the depth badge more than once per row");
        }
    }

    @Test
    @DisplayName("Expand All expands every level, and warns on a large trace")
    void expandAllIsCompleteAndBounded() throws IOException {
        String src = Files.readString(STATIC.resolve("parser/js/callGraph.js"));
        assertTrue(src.contains("for (let pass = 0"),
                "Expand All must repeat until nothing is left collapsed; a single querySelectorAll "
                        + "snapshot misses the containers that expanding creates");
        assertTrue(src.contains("_EXPAND_ALL_WARN"),
                "fully expanding the 1,250-node trace measured here builds every row at once and "
                        + "must ask first");
    }
}
