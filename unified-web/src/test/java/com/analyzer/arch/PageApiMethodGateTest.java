package com.analyzer.arch;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A shared script must not call an API method the page's own {@code api.js} does not define.
 *
 * <h2>The defect this catches</h2>
 *
 * <p>{@code jar/js/codeTree.js} is loaded by both {@code jar/index.html} and {@code war/index.html},
 * but the two pages ship different API objects: {@code jar/js/api.js} defines
 * {@code classTreePages} and {@code war/js/api.js} does not. The call was guarded — on
 * {@code PA.pageSource}, a different object — so the guard passed on the WAR page and the call
 * threw {@code TypeError}. The Code Structure tab was simply dead there.
 *
 * <p>Nothing fails at build time and nothing logs on the working page, so the only way this is
 * found is by someone opening that tab on that page. Two pages sharing a script while shipping
 * different APIs is a standing invitation to repeat it.
 *
 * <h2>What is checked, and what deliberately is not</h2>
 *
 * <p>Only calls of the form {@code JA.api.someMethod(} inside scripts a page actually loads, against
 * the {@code api.js} that page loads. Dynamic access ({@code JA.api[name]}) is not resolvable
 * statically and is left alone rather than guessed at — a gate that reports a call it cannot
 * verify teaches people to ignore it.
 *
 * <p>A call is accepted when the method is defined <em>or</em> the call site checks for it, so the
 * correct fix — guarding the method itself — satisfies the gate.
 */
class PageApiMethodGateTest {

    private static final Pattern SCRIPT_SRC =
            Pattern.compile("<script[^>]*\\ssrc\\s*=\\s*[\"']([^\"']+)[\"']");

    /** A call through the page API object: {@code JA.api.foo(} / {@code PA.api.foo(}. */
    private static final Pattern API_CALL =
            Pattern.compile("\\b(?:JA|PA)\\.api\\.([A-Za-z_$][\\w$]*)\\s*\\(");

    /** A wholesale replacement of the API object: {@code JA.api = {}. */
    private static final Pattern WHOLESALE_ASSIGN =
            Pattern.compile("(?m)^\\s*(?:window\\.)?(?:JA|PA)\\.api\\s*=\\s*\\{");

    /**
     * The api.js merges the previously-defined API back in, rather than discarding it.
     *
     * <p>Recognised by a capture of the prior object plus a copy loop over its keys. Deliberately
     * narrow: it must not match a file that merely mentions the earlier API, only one that
     * demonstrably carries its methods forward.
     */
    private static final Pattern MERGES_PREVIOUS = Pattern.compile(
            "(?s)_jarApi\\s*=\\s*JA\\.api.*?Object\\.keys\\(_jarApi\\)");

    /** A definition inside an api.js: {@code foo(} , {@code foo:} or {@code foo =}. */
    private static Pattern definition(String method) {
        return Pattern.compile("(?m)(^|[\\s,{])" + Pattern.quote(method) + "\\s*[:(=]");
    }

    private static Path staticRoot() {
        Path p = Path.of("src/main/resources/static");
        if (!Files.isDirectory(p)) {
            throw new IllegalStateException("static root not found at " + p.toAbsolutePath()
                    + " — a silently skipped gate is worse than none");
        }
        return p;
    }

    @Test
    @DisplayName("no page calls an API method its own api.js does not define")
    void everyApiCallIsDefinedOnThatPage() throws IOException {
        Path root = staticRoot();
        List<String> problems = new ArrayList<>();
        int pagesChecked = 0;

        for (Path page : pages(root)) {
            List<Path> scripts = loadedScripts(root, page);
            List<Path> apis = scripts.stream()
                    .filter(s -> s.getFileName().toString().equals("api.js"))
                    .toList();
            if (apis.isEmpty()) continue;               // page has no API object to violate
            pagesChecked++;

            // LAST WRITE WINS — concatenating every api.js was wrong and hid the very defect this
            // gate exists for. `war/index.html` loads `/jar/js/api.js` and then `/war/js/api.js`,
            // and both do a wholesale `JA.api = { … }`. The second REPLACES the first, so methods
            // defined only in the first are gone at runtime. Reading both together makes the page
            // look like it has an API it does not have.
            Path effective = apis.get(apis.size() - 1);
            for (int i = apis.size() - 1; i >= 0; i--) {
                String candidate = Files.readString(apis.get(i), StandardCharsets.UTF_8);
                if (WHOLESALE_ASSIGN.matcher(candidate).find()) { effective = apis.get(i); break; }
            }
            String effectiveSrc = Files.readString(effective, StandardCharsets.UTF_8);

            // A file that replaces the API may also merge the previous one back in. When it does,
            // methods from the earlier api.js survive at runtime, so the union is the honest view.
            // Without this the gate would report methods that are in fact callable — and a gate
            // that cries wolf is one people stop reading.
            String apiSrc = MERGES_PREVIOUS.matcher(effectiveSrc).find()
                    ? apis.stream().map(a -> {
                          try { return Files.readString(a, StandardCharsets.UTF_8); }
                          catch (IOException io) { throw new java.io.UncheckedIOException(io); }
                      }).reduce("", String::concat)
                    : effectiveSrc;

            for (Path s : scripts) {
                if (s.getFileName().toString().equals("api.js")) continue;
                String src = Files.readString(s, StandardCharsets.UTF_8);
                Matcher m = API_CALL.matcher(src);
                Set<String> reported = new LinkedHashSet<>();
                while (m.find()) {
                    String method = m.group(1);
                    if (!reported.add(method)) continue;
                    if (definition(method).matcher(apiSrc).find()) continue;
                    // A call site that checks for the method before calling it is correct even
                    // when the method is absent — that is precisely the intended fix.
                    if (src.contains("typeof JA.api." + method)
                            || src.contains("typeof PA.api." + method)) continue;
                    problems.add(root.relativize(page) + " loads " + root.relativize(s)
                            + " which calls JA.api." + method + "(), not defined by "
                            + root.relativize(effective) + " (the api.js that wins on that page)");
                }
            }
        }
        assertTrue(pagesChecked > 0, "no page with an api.js was examined — the gate did nothing");
        if (!problems.isEmpty()) {
            fail("These call an API method the page does not ship, so the feature is dead on that\n"
               + "page with a TypeError and no build failure. Define it, or guard with\n"
               + "`typeof JA.api.<method> === 'function'`:\n  " + String.join("\n  ", problems));
        }
    }

    /** Every HTML page under the static root. */
    private static List<Path> pages(Path root) throws IOException {
        try (var w = Files.walk(root)) {
            return w.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".html"))
                    .sorted().toList();
        }
    }

    /**
     * The scripts a page actually loads, resolved to real files.
     *
     * <p>Only {@code <script src=…>} counts: a filename inside an HTML comment is not a load, and
     * treating it as one would resurrect features that were deliberately disabled.
     */
    private static List<Path> loadedScripts(Path root, Path page) throws IOException {
        String html = Files.readString(page, StandardCharsets.UTF_8);
        // Strip comments first, so a commented-out tag is not mistaken for a live one.
        html = html.replaceAll("(?s)<!--.*?-->", "");
        List<Path> out = new ArrayList<>();
        Matcher m = SCRIPT_SRC.matcher(html);
        while (m.find()) {
            String src = m.group(1).replaceAll("\\?.*$", "");
            Path resolved = src.startsWith("/")
                    ? root.resolve(src.substring(1))
                    : page.getParent().resolve(src);
            resolved = resolved.normalize();
            if (Files.isRegularFile(resolved)) out.add(resolved);
        }
        return out;
    }

    @Test
    @DisplayName("the scanner catches a known violation")
    void scannerCatchesAKnownViolation() {
        // Calibration without touching the tree: the two patterns are the whole gate, so proving
        // they discriminate is proving the gate can fail.
        String api = "const JA = { api: { getClassTree(id) {}, other: function(){} } };";
        assertTrue(definition("getClassTree").matcher(api).find(), "should see a method definition");
        assertTrue(definition("other").matcher(api).find(), "should see a property definition");
        assertTrue(!definition("classTreePages").matcher(api).find(),
                "must NOT claim an undefined method is defined — this is the WAR-page case");
        Matcher m = API_CALL.matcher("const s = JA.api.classTreePages(jarId, v, null);");
        assertTrue(m.find() && m.group(1).equals("classTreePages"), "should extract the call");
    }
}
