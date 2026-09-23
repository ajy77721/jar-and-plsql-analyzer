package com.analyzer.arch;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * UI-027 — fails the build when a new {@code innerHTML = ... .map(...)} render appears outside
 * {@code shared/} without being in {@link #ALLOWED} with the reason its source array is bounded.
 *
 * <h2>Why this pattern, not the row's own literal text</h2>
 *
 * <p>UI-027's own DoD says "grep {@code .map(...).join('')} over fetched arrays". Tried that
 * literally first: it matches **16 files**, and every one sampled (`endpointList.js`'s own
 * ≤5-parameter join, `cross-ref.js`'s saved-bookmark list, per-row annotation/badge joins in
 * `codeTree.js`/`codeTreeViews.js`) was a false positive — {@code .map(...).join(...)} is one of
 * the most common idioms in this codebase for joining a *few* strings, and the text alone says
 * nothing about which array it came from or whether it was already narrowed. A gate built on that
 * pattern would be pure noise, worse than no gate (see the registry, §104).
 *
 * <p>{@code innerHTML = ...map(...)} — a direct assignment of a mapped-and-joined array straight
 * into the DOM — is a real signal: it identifies "here is a screen rendering one row per element of
 * some array" specifically, not any string-joining idiom. Measured against the real codebase before
 * writing this gate: only **4 files** (excluding `shared/`), each checked directly and found to
 * render a list that is bounded by design (how many analyses/bookmarks/prompts/DB projects exist,
 * not how many procedures or endpoints an analysis has) — see {@link #ALLOWED}'s own comments.
 */
class EagerRenderGateTest {

    private static final Pattern PATTERN =
            Pattern.compile("\\binnerHTML\\s*=[^;]*\\.map\\s*\\(");

    /** Key is the path relative to {@code unified-web/src/main/resources/static/}. */
    private static final Map<String, Integer> ALLOWED = new LinkedHashMap<>();
    static {
        // The JAR sidebar's own analysis list — bounded by how many JARs have ever been analyzed,
        // the same "small, bounded-by-run-history" shape as MID-021's /analyses exemption (§104).
        ALLOWED.put("jar/js/sidebar.js", 1);
        // Two occurrences: a user's own saved cross-reference bookmarks (inherently a handful,
        // never corpus-scale), plus a search-result render already explicitly capped at 200 rows
        // (`filtered.slice(0,200).map(...)`) — a real, already-applied window, not a violation.
        ALLOWED.put("jar/js/cross-ref.js", 2);
        // Curated prompt config (a handful of named prompt templates), not per-analysis data.
        ALLOWED.put("parser/js/promptManager.js", 1);
        // Configured DB connection projects — operator config, not analysis output.
        ALLOWED.put("plsql/js/config.js", 1);
    }

    @Test
    @DisplayName("no new innerHTML = array.map(...) render exists outside shared/ without a recorded reason")
    void noUnapprovedEagerRender() throws IOException {
        Path staticRoot = staticRoot();
        Map<String, Integer> counts = new TreeMap<>();
        Map<String, List<String>> where = new TreeMap<>();

        try (Stream<Path> files = Files.walk(staticRoot)) {
            for (Path f : files.filter(p -> p.toString().endsWith(".js")).toList()) {
                String rel = staticRoot.relativize(f).toString().replace('\\', '/');
                if (rel.startsWith("shared/") || rel.startsWith("tests/")) continue;
                // Third-party vendor code, not ours to gate.
                if (rel.endsWith("xlsx.full.min.js")) continue;

                int n = 0;
                List<String> hits = new ArrayList<>();
                List<String> lines = stripComments(Files.readString(f, StandardCharsets.UTF_8));
                for (int i = 0; i < lines.size(); i++) {
                    Matcher m = PATTERN.matcher(lines.get(i));
                    while (m.find()) {
                        n++;
                        hits.add(rel + ":" + (i + 1));
                    }
                }
                if (n > 0) {
                    counts.put(rel, n);
                    where.put(rel, hits);
                }
            }
        }

        List<String> problems = new ArrayList<>();
        for (Map.Entry<String, Integer> e : counts.entrySet()) {
            Integer budget = ALLOWED.get(e.getKey());
            if (budget == null) {
                problems.add("NOT ALLOWED: " + e.getKey() + " renders innerHTML from array.map(...) ("
                        + e.getValue() + " occurrences). Confirm the source array is bounded by design"
                        + " (not a full analysis fetch) and add it to ALLOWED with the reason, or page it.\n    "
                        + String.join("\n    ", where.get(e.getKey())));
            } else if (e.getValue() > budget) {
                problems.add("BUDGET EXCEEDED: " + e.getKey() + " has " + e.getValue()
                        + " occurrences, allowed " + budget + ".\n    "
                        + String.join("\n    ", where.get(e.getKey())));
            }
        }
        for (Map.Entry<String, Integer> e : ALLOWED.entrySet()) {
            int actual = counts.getOrDefault(e.getKey(), 0);
            if (actual < e.getValue()) {
                problems.add("RATCHET: " + e.getKey() + " is down to " + actual + " occurrences (budget "
                        + e.getValue() + "). Lower the budget in ALLOWED"
                        + (actual == 0 ? ", or delete the entry — it is fully converted." : "."));
            }
        }

        if (!problems.isEmpty()) {
            fail("Eager innerHTML = array.map(...) renders outside shared/:\n\n" + String.join("\n\n", problems));
        }
    }

    private static Path staticRoot() {
        Path p = Path.of("").toAbsolutePath();
        while (p != null) {
            Path candidate = p.resolve("unified-web/src/main/resources/static");
            if (Files.isDirectory(candidate)) return candidate;
            p = p.getParent();
        }
        throw new IllegalStateException("Could not locate unified-web/src/main/resources/static");
    }

    private static List<String> stripComments(String src) {
        String noBlock = src.replaceAll("(?s)/\\*.*?\\*/", "");
        List<String> out = new ArrayList<>();
        for (String line : noBlock.split("\n", -1)) {
            StringBuilder kept = new StringBuilder();
            boolean inString = false;
            char quote = 0;
            for (int i = 0; i < line.length(); i++) {
                char c = line.charAt(i);
                if ((c == '"' || c == '\'' || c == '`') && (i == 0 || line.charAt(i - 1) != '\\')) {
                    if (!inString) { inString = true; quote = c; }
                    else if (quote == c) inString = false;
                }
                if (!inString && c == '/' && i + 1 < line.length() && line.charAt(i + 1) == '/') break;
                kept.append(c);
            }
            out.add(kept.toString());
        }
        return out;
    }
}
