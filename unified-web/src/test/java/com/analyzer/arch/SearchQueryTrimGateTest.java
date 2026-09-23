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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Every search box must trim what the user typed before matching.
 *
 * <p>The defect is silent by construction. A query pasted from a spreadsheet or a ticket carries a
 * trailing space; the comparison then fails; the list renders empty; and the screen offers no reason
 * why. The user concludes the data is missing. Nothing throws, nothing logs, and no behavioural test
 * catches it because a test never types a stray space — which is exactly why this is a static gate
 * rather than a UI test.
 *
 * <p>The rule this enforces is one-sided, and the asymmetry is the whole point:
 *
 * <ul>
 *   <li>the <b>needle</b> — the value read off an input, or a query parameter threaded down from
 *       one — is trimmed and case-folded, via {@code SearchQuery} in {@code shared/ui-busy.js};</li>
 *   <li>the <b>haystack</b> — {@code textContent}, {@code dataset} attributes, model field values —
 *       is only case-folded. Trimming it would be wrong: interior spacing is part of the content
 *       being searched, and {@code "a  b".trim()} does not remove it anyway.</li>
 * </ul>
 *
 * <p>So the scanner deliberately looks only at case-folding applied <i>directly to an input's
 * value</i>. A broader pattern would flag the hundreds of legitimate haystack sites and the gate
 * would be turned off within a week.
 *
 * <h2>Why this scanner is trusted</h2>
 *
 * <p>Scanners written in this repository have produced confident nonsense before — one reported 142
 * uncalled endpoints of which every sampled case was wrong. So this one is calibrated by
 * {@link #scannerCatchesAKnownViolation()}, which feeds it a synthetic file containing the exact
 * defect and fails if the scanner stays quiet. A gate that cannot fail proves nothing.
 */
class SearchQueryTrimGateTest {

    private static final Path STATIC = Path.of("src/main/resources/static");

    /**
     * Case-folding applied straight to a DOM input's value.
     *
     * <p>Matches {@code el.value.toLowerCase()} and {@code (el.value || '').toUpperCase()} — the two
     * spellings actually used here — and nothing that reads from the DOM's text.
     *
     * <p>The receiver must look like an element, and that restriction is load-bearing rather than
     * cosmetic. {@code .value} is also an ordinary field name on this project's own models —
     * {@code ca.attributes.value} on an annotation, {@code pr.value} on a predicate — and both of
     * those are haystacks that must not be trimmed. An earlier draft of this pattern flagged exactly
     * those two and nothing else, which is how the list below was derived: from the false positives,
     * not from guesswork.
     */
    private static final Pattern UNTRIMMED_NEEDLE = Pattern.compile(
            "(?:getElementById\\s*\\([^)]*\\)|querySelector(?:All)?\\s*\\([^)]*\\)"
                    + "|\\b[A-Za-z_$][\\w$]*(?:[Ii]nput|[Ii]np|[Ee]l|[Ff]ield|[Bb]ox|[Ss]earch|[Ff]ilter)\\b)"
                    + "\\s*\\??\\.value\\s*(\\|\\|\\s*'')?\\s*\\)?\\s*\\.to(Lower|Upper)Case\\s*\\(\\s*\\)"
                    + "(?!\\s*\\.trim\\s*\\(\\s*\\))");

    /**
     * Third-party bundles and code that is not wired into any page.
     *
     * <p>{@code xlsx.full.min.js} is a vendored library — editing it would be lost on the next
     * upgrade. {@code cross-ref.js} is loaded by no page (its script tag is commented out), so a
     * finding there would be noise about dead code.
     */
    private static final List<String> EXEMPT = List.of("xlsx.full.min.js", "cross-ref.js");

    @Test
    @DisplayName("no search reads an input's value without trimming it")
    void everyNeedleIsTrimmed() throws IOException {
        List<String> violations = scan(STATIC);
        if (!violations.isEmpty()) {
            fail("These read an input's value and case-fold it without trimming, so a query with a\n"
                    + "leading or trailing space silently matches nothing. Use SearchQuery.norm(...)\n"
                    + "or SearchQuery.normUpper(...) from shared/ui-busy.js:\n  "
                    + String.join("\n  ", violations));
        }
    }

    @Test
    @DisplayName("the normaliser lives where every page can reach it")
    void normaliserIsGloballyAvailable() throws IOException {
        String uiBusy = Files.readString(STATIC.resolve("shared/ui-busy.js"));
        for (String fn : List.of("norm:", "normUpper:", "trim:", "hadPadding:", "markPadding:")) {
            assertTrue(uiBusy.contains(fn), "SearchQuery." + fn + " missing from shared/ui-busy.js");
        }
        // ui-busy.js being loaded by every page is asserted by StaleGuardAndSharedScriptGateTest's
        // shared-global registry; duplicating that check here would let the two drift apart.
    }

    @Test
    @DisplayName("padded input is made visible, not just silently trimmed")
    void paddingIsSurfacedToTheUser() throws IOException {
        // Trimming fixes the match but leaves the user none the wiser about their clipboard. The
        // delegated listener is what turns a silent correction into something they can see.
        String uiBusy = Files.readString(STATIC.resolve("shared/ui-busy.js"));
        assertTrue(uiBusy.contains("search-had-padding"),
                "no marker class — trimming would be silent");
        assertTrue(uiBusy.contains("addEventListener('input'"),
                "the marker must be applied by a delegated listener, so search boxes added later "
                        + "are covered without anyone remembering to wire them up");
    }

    @Test
    @DisplayName("the scanner catches a known violation")
    void scannerCatchesAKnownViolation() throws IOException {
        Path tmp = Files.createTempDirectory("trim-gate-calibration");
        try {
            Path js = tmp.resolve("bad.js");
            Files.writeString(js, """
                    function filterRows(rows) {
                        // the exact defect: needle read from the input, folded, never trimmed
                        const q = document.getElementById('f').value.toLowerCase();
                        return rows.filter(r => r.textContent.toLowerCase().includes(q));
                    }
                    """);
            List<String> found = scan(tmp);
            assertTrue(found.size() == 1,
                    "the scanner must flag the untrimmed needle and must NOT flag the haystack "
                            + "textContent on the next line; got " + found);

            // And the inverse: the corrected form must come back clean, or the gate would be
            // unsatisfiable and would simply be deleted.
            Files.writeString(js, """
                    function filterRows(rows) {
                        const q = SearchQuery.norm(document.getElementById('f').value);
                        return rows.filter(r => r.textContent.toLowerCase().includes(q));
                    }
                    """);
            assertTrue(scan(tmp).isEmpty(), "the fixed form must pass: " + scan(tmp));
        } finally {
            try (Stream<Path> w = Files.walk(tmp)) {
                w.sorted((a, b) -> b.getNameCount() - a.getNameCount()).forEach(p -> p.toFile().delete());
            }
        }
    }

    private List<String> scan(Path root) throws IOException {
        List<String> out = new ArrayList<>();
        // Only the calibration test passes a temp dir; the real scan must never be silently empty.
        if (!Files.isDirectory(root)) {
            if (root.equals(STATIC)) requireStatic(root);
            return out;
        }
        List<Path> files;
        try (Stream<Path> w = Files.walk(root)) {
            files = w.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".js"))
                    .filter(p -> EXEMPT.stream().noneMatch(e -> p.getFileName().toString().equals(e)))
                    .sorted()
                    .toList();
        }
        for (Path f : files) {
            String src = Files.readString(f);
            Matcher m = UNTRIMMED_NEEDLE.matcher(src);
            while (m.find()) {
                int line = (int) src.substring(0, m.start()).chars().filter(c -> c == '\n').count() + 1;
                out.add(root.relativize(f) + ":" + line + "  " + lineAt(src, m.start()));
            }
        }
        return out;
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

    private String lineAt(String src, int idx) {
        int s = src.lastIndexOf('\n', idx) + 1;
        int e = src.indexOf('\n', idx);
        String t = src.substring(s, e < 0 ? src.length() : e).trim();
        return t.length() > 120 ? t.substring(0, 120) + " …" : t;
    }
}
