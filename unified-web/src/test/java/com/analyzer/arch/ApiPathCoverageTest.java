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
 * Every {@code /api/...} path in the frontend must resolve to a mapped controller route.
 *
 * <p>This exists because of a defect that shipped and survived: {@code perfSummary.js} fetched
 * {@code /api/analyses/{name}/claude/result}, but no controller maps {@code /api/analyses} — the
 * real prefix is {@code /api/parser/analyses}. The call had <em>always</em> returned 404, and
 * because the failure branch rendered "No verification data", a wiring bug was indistinguishable
 * from an analysis that genuinely had no verification. The Claude Verification grid was empty for
 * every user, for the entire life of the feature, with nothing in any log.
 *
 * <p>A runtime test cannot catch this class of bug cheaply — it needs every screen exercised with
 * data present. Comparing the two sides statically does, in under a second.
 *
 * <p>The check is deliberately loose in one direction: it verifies the path could be served by
 * <em>some</em> mapping, not that the full path and HTTP verb match a specific handler. Prefix
 * agreement is what was actually wrong, and a stricter check would need to model
 * {@code @PathVariable} patterns and inherited class-level prefixes, which buys precision at the
 * cost of false alarms.
 */
class ApiPathCoverageTest {

    /** Literal {@code /api/...} paths in JS/HTML, in quotes, template literals or fetch calls. */
    private static final Pattern API_PATH = Pattern.compile("['\"`](/api/[a-zA-Z0-9/_.$-]*)");

    /**
     * {@code @RequestMapping("...")} / {@code @GetMapping("...")} and friends.
     *
     * <p>Accepts the bare form as well as {@code value = "..."} and {@code path = "..."}. Missing
     * the {@code path =} variant made this test report {@code /api/war/progress} as unmapped when
     * {@code ProgressController.streamWarProgress} declares it exactly that way — a false positive,
     * which in a gate is worse than no gate at all: it trains people to ignore the failure.
     */
    private static final Pattern MAPPING = Pattern.compile(
            "@(?:Request|Get|Post|Put|Delete|Patch)Mapping\\s*\\(\\s*"
            + "(?:(?:value|path)\\s*=\\s*)?\\{?\\s*\"([^\"]*)\"");

    /**
     * Paths that intentionally have no controller behind them.
     *
     * <p>Keep this list short and justified. An entry here is a claim that a 404 is expected and
     * handled, which is exactly the assumption that hid the original bug.
     */
    /**
     * "/api/" is not a call — it is the PREFIX GUARD in page-envelope.js's isPublicPath
     * ({@code path.indexOf('/api/') !== 0}), added with the auth-expiry fix (2668594). The
     * API_PATH regex cannot tell a guard from a fetch target, and no controller maps "/api/"
     * exactly, so it lands here rather than the gate staying red for a string that never goes
     * over the wire. (First seen when the full reactor ran for the task-51 push — the failure
     * reproduces on the clean tree, so it pre-dates that push's diff.)
     */
    private static final Set<String> KNOWN_ABSENT = Set.of("/api/");

    @Test
    @DisplayName("every /api path used by the frontend has a matching controller mapping")
    void frontendApiPathsAreMapped() throws IOException {
        Path root = repoRoot();

        Set<String> mappedPrefixes = collectMappedPrefixes(root);
        if (mappedPrefixes.isEmpty()) {
            fail("Found no controller mappings at all — the scan is broken, not the code");
        }

        Map<String, List<String>> unmatched = new TreeMap<>();
        try (Stream<Path> files = Files.walk(root.resolve("unified-web/src/main/resources/static"))) {
            for (Path f : files.filter(Files::isRegularFile)
                              .filter(p -> {
                                  String n = p.toString();
                                  return n.endsWith(".js") || n.endsWith(".html");
                              })
                              .filter(p -> !p.toString().contains("_migration-backup"))
                              .toList()) {
                // Strip comments first: a path mentioned in prose is not a call. Without this the
                // test flags its own explanatory comments, which is the fastest way to get a gate
                // switched off.
                List<String> lines = stripComments(Files.readString(f, StandardCharsets.UTF_8));
                for (int i = 0; i < lines.size(); i++) {
                    Matcher m = API_PATH.matcher(lines.get(i));
                    while (m.find()) {
                        String path = m.group(1);
                        if (KNOWN_ABSENT.contains(path)) continue;
                        if (!isServable(path, mappedPrefixes)) {
                            unmatched.computeIfAbsent(path, k -> new ArrayList<>())
                                    .add(root.relativize(f) + ":" + (i + 1));
                        }
                    }
                }
            }
        }

        if (!unmatched.isEmpty()) {
            StringBuilder sb = new StringBuilder(
                    "Frontend calls /api paths that no controller maps. Each of these returns 404 "
                    + "at runtime, and if the caller treats a failed fetch as \"no data\" the screen "
                    + "is silently broken:\n");
            unmatched.forEach((path, sites) -> {
                sb.append("\n  ").append(path).append('\n');
                sites.forEach(s -> sb.append("      ").append(s).append('\n'));
            });
            sb.append("\nFix the path (route it through the shared api.js layer, which is the only ")
              .append("file that should know URL prefixes), or add it to KNOWN_ABSENT with a reason.");
            fail(sb.toString());
        }
    }

    /**
     * True when some mapping could serve this path.
     *
     * <p>A frontend path such as {@code /api/parser/analyses/x/tables} is servable if a mapping
     * exists whose literal prefix it starts with. Comparison is done on path segments so
     * {@code /api/analysis} does not appear to cover {@code /api/analyses}, which is precisely the
     * confusion that caused the original defect.
     */
    private static boolean isServable(String path, Set<String> mappedPrefixes) {
        for (String prefix : mappedPrefixes) {
            if (path.equals(prefix)) return true;
            if (path.startsWith(prefix + "/")) return true;
        }
        return false;
    }

    /**
     * Every class-level and method-level mapping, plus the concatenation of the two.
     *
     * <p>A method mapping is relative to its class mapping, so both are gathered per file and
     * combined; taking method mappings alone would miss {@code /analyses/{name}} sitting under a
     * class-level {@code /api/parser}.
     */
    private static Set<String> collectMappedPrefixes(Path root) throws IOException {
        Set<String> prefixes = new TreeSet<>();
        try (Stream<Path> files = Files.walk(root)) {
            for (Path f : files.filter(p -> p.toString().endsWith(".java"))
                               .filter(p -> p.toString().contains("/src/main/java/"))
                               .filter(p -> !p.toString().contains("/target/"))
                               .filter(p -> !p.toString().contains("_migration-backup"))
                               .toList()) {
                String src = Files.readString(f, StandardCharsets.UTF_8);
                if (!src.contains("Mapping")) continue;

                List<String> classLevel = new ArrayList<>();
                List<String> methodLevel = new ArrayList<>();
                Matcher m = MAPPING.matcher(src);
                while (m.find()) {
                    String v = m.group(1);
                    if (v.startsWith("/api")) classLevel.add(normalise(v));
                    else if (!v.isBlank()) methodLevel.add(normalise(v));
                }
                prefixes.addAll(classLevel);
                for (String cl : classLevel) {
                    for (String ml : methodLevel) {
                        prefixes.add(normalise(cl + ml));
                    }
                }
            }
        }
        // Collapse path variables to a wildcard-free literal prefix: everything up to the first
        // "{" is the part a frontend path must agree with.
        Set<String> literal = new TreeSet<>();
        for (String p : prefixes) {
            int brace = p.indexOf('{');
            String cut = brace < 0 ? p : p.substring(0, brace);
            while (cut.endsWith("/")) cut = cut.substring(0, cut.length() - 1);
            if (cut.startsWith("/api")) literal.add(cut);
        }
        return literal;
    }

    private static String normalise(String p) {
        String s = p.startsWith("/") ? p : "/" + p;
        return s.length() > 1 && s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
    }

    /**
     * Blanks out {@code //} line comments and block comments, leaving line numbering intact so a
     * reported location still points at the right line.
     */
    private static List<String> stripComments(String src) {
        String noBlock = src.replaceAll("(?s)/\\*.*?\\*/", "");
        List<String> out = new ArrayList<>();
        for (String line : noBlock.split("\n", -1)) {
            StringBuilder kept = new StringBuilder();
            boolean inString = false;
            char quote = 0;
            for (int i = 0; i < line.length(); i++) {
                char c = line.charAt(i);
                boolean escaped = i > 0 && line.charAt(i - 1) == '\\';
                if (!escaped && (c == '"' || c == '\'' || c == '`')) {
                    if (!inString) { inString = true; quote = c; }
                    else if (c == quote) { inString = false; }
                }
                if (!inString && c == '/' && i + 1 < line.length() && line.charAt(i + 1) == '/') break;
                kept.append(c);
            }
            out.add(kept.toString());
        }
        return out;
    }

    private static Path repoRoot() {
        Path p = Path.of("").toAbsolutePath();
        while (p != null && !Files.exists(p.resolve("analyzer-storage"))) {
            p = p.getParent();
        }
        if (p == null) throw new IllegalStateException("Could not locate the repository root");
        return p;
    }
}
