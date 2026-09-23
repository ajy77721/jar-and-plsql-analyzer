package com.analyzer.arch;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Two static gates for the failure modes behind tasks §B4 (disabled states) and §B5 (stale-response
 * guards). Both defects are invisible at runtime — a double-submitted POST looks like a working
 * button, and a guard that rejects every response looks like a panel that is merely slow — so
 * neither is reachable by a behavioural test. They are asserted structurally instead.
 *
 * <h2>1. A shared global must be loaded by every page that uses it</h2>
 *
 * <p>{@code shared/ui-busy.js} defines {@code window.UIBusy}, which §B4 now calls from inline
 * {@code onclick} attributes across three different UIs. If a page loads a file that references
 * {@code UIBusy} but does not load {@code ui-busy.js} itself, every one of those buttons throws
 * {@code ReferenceError} on click and the action silently does nothing. That is precisely the shape
 * of §B8 ("the guard tested the factory, not its product") and of the {@code PollConfig} defect
 * already guarded by {@link PollingConfigGateTest} — this generalises that one-pair check to a
 * registry.
 *
 * <p><b>Validated against known truth before being trusted.</b> Three scanners in this repository
 * have produced wrong answers from exactly this kind of resolution, and one of them was resolving
 * an absolute {@code <script src="/shared/x.js">} against the page's own directory instead of the
 * static root — which made all 45 WAR files look unloaded. So {@link #scriptResolutionIsCorrect()}
 * asserts the resolver on cases whose answer is known independently, and it runs whether or not
 * the dependency check finds anything.
 *
 * <h2>2. A stale-response counter must be initialised</h2>
 *
 * <p>The guard pattern is {@code const token = ++this._fooSeq; ... if (token !== this._fooSeq)
 * return;}. If {@code _fooSeq} is never initialised, {@code ++undefined} is {@code NaN}, and since
 * {@code NaN !== NaN} the guard rejects <i>every</i> response: the panel never updates at all. The
 * bug is worse than the race it was written to fix, and it presents as "the feature does nothing",
 * which nobody attributes to the guard. This was hit for real while writing §B5.
 *
 * <p>{@code UIBusy.seq()} exists so new code cannot make this mistake, but hand-rolled counters
 * still exist and are legitimate; this only requires that they be given a numeric starting value.
 */
class StaleGuardAndSharedScriptGateTest {

    /** A script tag's {@code src}, so a filename in a comment is not mistaken for a load. */
    private static final Pattern SCRIPT_SRC =
            Pattern.compile("<script[^>]*\\ssrc\\s*=\\s*[\"']([^\"']+)[\"']");

    /** An inline {@code <script>} block — the conversion screen calls the API from one. */
    private static final Pattern INLINE_SCRIPT =
            Pattern.compile("(?s)<script(?![^>]*\\ssrc\\s*=)[^>]*>(.*?)</script>");

    /**
     * Global symbol → the shared script that defines it.
     *
     * <p>Add an entry whenever a file under {@code shared/} publishes a new {@code window.X}.
     */
    private static final Map<String, String> PROVIDED_GLOBALS = new LinkedHashMap<>();
    static {
        PROVIDED_GLOBALS.put("UIBusy", "shared/ui-busy.js");
        PROVIDED_GLOBALS.put("PollConfig", "shared/poll-config.js");
        // SearchQuery lives in ui-busy.js too, and its absence here is how index.html shipped a
        // User Manual that threw on every filter keystroke: the gate scanned that page, found no
        // registered global in use, and passed. A registry that is not exhaustive is a gate that
        // reports success for the cases nobody remembered to add.
        PROVIDED_GLOBALS.put("SearchQuery", "shared/ui-busy.js");
        // Esc is the single escaping utility every other escaper delegates to. Registering it here
        // is what makes "loaded before anything that escapes" a build failure rather than a
        // convention — and the previous convention had already produced four divergent copies.
        PROVIDED_GLOBALS.put("Esc", "shared/escape.js");
    }

    // ------------------------------------------------------------------ gate 1

    @Test
    @DisplayName("every page that uses a shared global also loads the script defining it, first")
    void pagesLoadTheScriptsDefiningTheGlobalsTheyUse() throws IOException {
        Path staticRoot = staticRoot();
        List<String> problems = new ArrayList<>();

        for (Path page : htmlPages(staticRoot)) {
            List<Path> loaded = loadedScripts(staticRoot, page);
            String pageName = rel(staticRoot, page);
            String html = Files.readString(page, StandardCharsets.UTF_8);

            for (Map.Entry<String, String> e : PROVIDED_GLOBALS.entrySet()) {
                String symbol = e.getKey();
                Path provider = staticRoot.resolve(e.getValue()).normalize();

                // Where, in this page's load order, does the provider appear?
                int providerAt = loaded.indexOf(provider);

                // The first loaded script that uses the symbol. Inline blocks count as "last",
                // because they run after the tags above them.
                int firstUseAt = -1;
                String firstUser = null;
                for (int i = 0; i < loaded.size(); i++) {
                    Path js = loaded.get(i);
                    if (js.equals(provider) || !Files.isRegularFile(js)) continue;
                    if (usesSymbol(readStripped(js), symbol)) { firstUseAt = i; firstUser = rel(staticRoot, js); break; }
                }
                if (firstUseAt < 0) {
                    for (Matcher m = INLINE_SCRIPT.matcher(html); m.find(); ) {
                        if (usesSymbol(stripComments(m.group(1)), symbol)) {
                            firstUseAt = loaded.size();
                            firstUser = pageName + " (inline <script>)";
                            break;
                        }
                    }
                }
                if (firstUseAt < 0) continue;   // page never uses it — nothing to require

                if (providerAt < 0) {
                    problems.add(pageName + " loads " + firstUser + ", which references " + symbol
                            + ", but never loads " + e.getValue() + " — every use throws "
                            + "ReferenceError and the action silently does nothing.");
                } else if (providerAt > firstUseAt) {
                    problems.add(pageName + " loads " + e.getValue() + " AFTER " + firstUser
                            + ", which references " + symbol + ". The provider must come first.");
                }
            }
        }

        if (!problems.isEmpty()) {
            fail("A page uses a shared global without loading its script:\n  "
                    + String.join("\n  ", problems));
        }
    }

    /**
     * Proves the src resolver on cases whose answer is known independently of the resolver.
     *
     * <p>Without this, gate 1 could pass by resolving everything to a nonexistent path and finding
     * no usages anywhere — a green test that checks nothing. Three of this repository's scanners
     * were wrong in exactly that direction.
     */
    @Test
    @DisplayName("the <script src> resolver handles relative, absolute and versioned paths")
    void scriptResolutionIsCorrect() throws IOException {
        Path staticRoot = staticRoot();
        Path jarPage = staticRoot.resolve("jar/index.html");
        assertTrue(Files.isRegularFile(jarPage), "jar/index.html is the fixture for this check");

        // '../shared/x.js' from jar/index.html  ->  static/shared/x.js  (NOT static/jar/shared/…)
        assertEquals(staticRoot.resolve("shared/ui-busy.js").normalize(),
                resolveSrc(staticRoot, jarPage, "../shared/ui-busy.js?v=1"),
                "a relative src must resolve against the page's directory, and ?v= must be dropped");

        // '/shared/x.js' is absolute: it resolves against the STATIC ROOT, not the page directory.
        // Getting this wrong is what made all 45 WAR files look unloaded in the §B6 scanner.
        assertEquals(staticRoot.resolve("shared/theme-toggle.js").normalize(),
                resolveSrc(staticRoot, jarPage, "/shared/theme-toggle.js"),
                "an absolute src must resolve against the static root");

        // 'js/x.js' is relative to the page directory.
        assertEquals(staticRoot.resolve("jar/js/utils.js").normalize(),
                resolveSrc(staticRoot, jarPage, "js/utils.js"),
                "a bare relative src must resolve against the page's directory");

        // A CDN src is not a local file and must be dropped, not turned into a bogus local path.
        assertEquals(null, resolveSrc(staticRoot, jarPage, "https://cdn.example.com/x.js"),
                "an external src is not a local file");

        // End-to-end: the resolved list must contain real files, or gate 1 is vacuous.
        List<Path> loaded = loadedScripts(staticRoot, jarPage);
        long real = loaded.stream().filter(Files::isRegularFile).count();
        assertTrue(real >= 20,
                "jar/index.html should resolve to at least 20 existing scripts, got " + real
                        + " — the resolver is broken and gate 1 would pass vacuously");
        assertTrue(loaded.contains(staticRoot.resolve("jar/js/utils.js").normalize()),
                "jar/js/utils.js is loaded by jar/index.html; the resolver missed it");
    }

    // ------------------------------------------------------------------ gate 2

    /** {@code ++<owner>._fooSeq} — the increment half of the stale-response guard. */
    private static final Pattern SEQ_INCREMENT =
            Pattern.compile("\\+\\+\\s*(?:this|[A-Za-z_$][\\w$.]*)\\.(\\w*[Ss]eq)\\b");

    @Test
    @DisplayName("a ++_seq stale-response counter is initialised — ++undefined is NaN, which never matches")
    void staleResponseCountersAreInitialised() throws IOException {
        Path staticRoot = staticRoot();
        List<String> problems = new ArrayList<>();

        for (Path js : jsFiles(staticRoot)) {
            String src = readStripped(js);
            Set<String> counters = new LinkedHashSet<>();
            for (Matcher m = SEQ_INCREMENT.matcher(src); m.find(); ) counters.add(m.group(1));

            for (String c : counters) {
                // An initialiser is any of: an object-literal property `_fooSeq: 0`, an assignment
                // `x._fooSeq = 0`, or a defensive read `typeof x._fooSeq === 'number' ? … : 0`.
                boolean initialised =
                        Pattern.compile("\\b" + Pattern.quote(c) + "\\s*:\\s*-?\\d").matcher(src).find()
                     || Pattern.compile("\\." + Pattern.quote(c) + "\\s*=\\s*-?\\d").matcher(src).find()
                     || Pattern.compile("\\b" + Pattern.quote(c) + "\\b[^\\n]*\\|\\|\\s*0").matcher(src).find();
                if (!initialised) {
                    problems.add(rel(staticRoot, js) + "  ++…" + c + " with no numeric initialiser"
                            + " — `++undefined` is NaN and `NaN !== NaN`, so the guard rejects"
                            + " EVERY response and the panel never updates. Declare `" + c
                            + ": 0`, or use UIBusy.seq(owner, '" + c + "') which cannot get this"
                            + " wrong.");
                }
            }
        }

        if (!problems.isEmpty()) {
            fail("Uninitialised stale-response counter (§B5):\n  " + String.join("\n  ", problems));
        }
    }

    /**
     * The gate above is only meaningful if it can actually tell the two cases apart, so prove it
     * on synthetic sources rather than trusting that a clean tree means a working check.
     */
    @Test
    @DisplayName("the counter check accepts an initialised counter and rejects an uninitialised one")
    void counterCheckDistinguishesTheTwoCases() {
        assertTrue(hasUninitialisedCounter("var o = { load: function () { var t = ++o._reqSeq; } };"),
                "an incremented counter with no initialiser must be flagged");
        assertTrue(!hasUninitialisedCounter("var o = { _reqSeq: 0, load: function () { var t = ++o._reqSeq; } };"),
                "an object-literal initialiser must be accepted");
        assertTrue(!hasUninitialisedCounter("o._reqSeq = 0;\nvar t = ++o._reqSeq;"),
                "an assignment initialiser must be accepted");
        assertTrue(!hasUninitialisedCounter("var t = ++this._reqSeq;\nthis._reqSeq = this._reqSeq || 0;"),
                "a `|| 0` fallback must be accepted");
        assertTrue(!hasUninitialisedCounter("var isCurrent = UIBusy.seq(this, '_reqSeq');"),
                "UIBusy.seq is not an increment and must not be flagged");
    }

    private static boolean hasUninitialisedCounter(String src) {
        Set<String> counters = new LinkedHashSet<>();
        for (Matcher m = SEQ_INCREMENT.matcher(src); m.find(); ) counters.add(m.group(1));
        for (String c : counters) {
            boolean initialised =
                    Pattern.compile("\\b" + Pattern.quote(c) + "\\s*:\\s*-?\\d").matcher(src).find()
                 || Pattern.compile("\\." + Pattern.quote(c) + "\\s*=\\s*-?\\d").matcher(src).find()
                 || Pattern.compile("\\b" + Pattern.quote(c) + "\\b[^\\n]*\\|\\|\\s*0").matcher(src).find();
            if (!initialised) return true;
        }
        return false;
    }

    // ------------------------------------------------------------------ helpers

    /**
     * Resolves one {@code src} to a file under the static root, or null if it is not a local file.
     *
     * <p>An absolute src resolves against the static root; anything else against the page's own
     * directory. The {@code ?v=} cache-buster is dropped.
     */
    private static Path resolveSrc(Path staticRoot, Path page, String src) {
        if (src.startsWith("http://") || src.startsWith("https://") || src.startsWith("//")) return null;
        String clean = src;
        int q = clean.indexOf('?');
        if (q >= 0) clean = clean.substring(0, q);
        int h = clean.indexOf('#');
        if (h >= 0) clean = clean.substring(0, h);
        if (clean.isEmpty()) return null;
        Path base = clean.startsWith("/") ? staticRoot : page.getParent();
        String relPart = clean.startsWith("/") ? clean.substring(1) : clean;
        return base.resolve(relPart).normalize();
    }

    /** The scripts a page loads, in load order. May contain paths that do not exist. */
    private static List<Path> loadedScripts(Path staticRoot, Path page) throws IOException {
        String html = Files.readString(page, StandardCharsets.UTF_8);
        // Commented-out tags are not loads: jar/index.html keeps cross-ref.js in an HTML comment.
        html = html.replaceAll("(?s)<!--.*?-->", "");
        List<Path> out = new ArrayList<>();
        for (Matcher m = SCRIPT_SRC.matcher(html); m.find(); ) {
            Path p = resolveSrc(staticRoot, page, m.group(1));
            if (p != null) out.add(p);
        }
        return out;
    }

    /**
     * True when the source references {@code Symbol.} or {@code window.Symbol} as code.
     *
     * <p>Comments are stripped by the caller: four scanners in this repository have been tripped by
     * a file's own explanatory comment, including one that read "if a tab is ever built for it" and
     * hid a genuinely dead endpoint behind its own documentation.
     */
    private static boolean usesSymbol(String strippedSrc, String symbol) {
        return Pattern.compile("(?<![\\w$.])" + Pattern.quote(symbol) + "\\s*\\.")
                .matcher(strippedSrc).find();
    }

    private static String readStripped(Path js) throws IOException {
        return stripComments(Files.readString(js, StandardCharsets.UTF_8));
    }

    private static String stripComments(String src) {
        return src.replaceAll("(?s)/\\*.*?\\*/", "").replaceAll("(?m)//.*$", "");
    }

    private static List<Path> htmlPages(Path staticRoot) throws IOException {
        try (Stream<Path> s = Files.walk(staticRoot)) {
            return s.filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".html"))
                    .filter(p -> !p.toString().contains("_migration-backup"))
                    .sorted().toList();
        }
    }

    private static List<Path> jsFiles(Path staticRoot) throws IOException {
        try (Stream<Path> s = Files.walk(staticRoot)) {
            return s.filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".js"))
                    .filter(p -> !p.toString().contains("_migration-backup"))
                    .filter(p -> !p.getFileName().toString().contains(".min."))
                    .sorted().toList();
        }
    }

    private static String rel(Path staticRoot, Path p) {
        return staticRoot.relativize(p).toString().replace('\\', '/');
    }

    private static Path staticRoot() {
        Path root = repoRoot().resolve("unified-web/src/main/resources/static");
        if (!Files.isDirectory(root)) {
            throw new IllegalStateException("static root not found at " + root
                    + " — the module layout changed; a silently skipped gate is worse than none");
        }
        return root;
    }

    /** Walks up from the working directory, so the test runs from any module. */
    private static Path repoRoot() {
        Path p = Path.of("").toAbsolutePath();
        while (p != null && !Files.exists(p.resolve("analyzer-storage"))) p = p.getParent();
        if (p == null) throw new IllegalStateException("Could not locate the repository root");
        return p;
    }
}
