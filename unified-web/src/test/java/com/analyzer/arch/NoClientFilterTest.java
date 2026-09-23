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
 * UI-013 — fails the build when a client-side filter/sort/search chain (`sortKeys`/`searchFn`/
 * `extraFilter`, the local counterpart to server paging) appears outside {@code shared/} without
 * being in {@link #ALLOWED} with the reason it must stay local.
 *
 * <p>The registry's own history is why this is a gate rather than a one-off count: UI-013's row
 * named "5 files blocked on UI-026" and "5 files needing new server-side aggregation endpoints" as
 * of §61, and by §100 — after UI-026/UI-030/most of UI-024 shipped — every one of those ten turned
 * out to already be accounted for, while a fresh grep found 24 occurrences total, not the 13 the
 * old estimate implied. A description of "which files are fine" goes stale as soon as the next
 * conversion lands; this test is the version of that fact which fails loudly instead.
 *
 * <p>Every entry below was verified by reading the actual file, not assumed from its presence on
 * an old list — see {@link #ALLOWED}'s own comments for the specific reason each one stays local.
 */
class NoClientFilterTest {

    private static final Pattern PATTERN = Pattern.compile("\\b(?:sortKeys|searchFn|extraFilter)\\s*:");

    /**
     * Key is the path relative to {@code unified-web/src/main/resources/static/}. Value is the
     * exact current occurrence count — a budget, not a target (see {@link DirectFileIoGateTest}'s
     * own doc for why a budget that can silently rise is worse than no gate at all).
     */
    private static final Map<String, Integer> ALLOWED = new LinkedHashMap<>();
    static {
        // ── JAR side (UI-026): epReports and its five cross-reports are precomputed server-side
        //    (EndpointReportBuilder/EndpointCrossReportBuilder/EndpointReportsService) and fetched
        //    whole. Measured against this repo's only real analysis
        //    (data/jar/unified-web-1.0.0-SNAPSHOT_20260905_110310/_summary_static.json): 321
        //    endpoints — comfortably under MID-002's own 1,000-row instant-client-filter threshold,
        //    so local sortKeys/searchFn/extraFilter over that fetched array is the same
        //    threshold-justified choice MID-002 made for the left panel, not a violation.
        ALLOWED.put("jar/js/summary-helpers.js", 1);
        ALLOWED.put("jar/js/summary-tables.js", 7);
        ALLOWED.put("jar/js/summary-dynamic.js", 2);
        ALLOWED.put("jar/js/summary-aggregation.js", 2);
        ALLOWED.put("jar/js/summary-vertical.js", 2);

        // ── Parser side, already-small server aggregates (UI-024 §93/§98/§99, or already done in
        //    an earlier session per each file's own comment): the fetched array is a handful of
        //    statement types / risk-scored rows / table-verification rows, not a corpus scan.
        ALLOWED.put("parser/js/complexity.js", 3);
        ALLOWED.put("parser/js/statements.js", 3);
        ALLOWED.put("parser/js/joinSummary.js", 4);
        ALLOWED.put("parser/js/claudeEnrichment.js", 1);
        // refs.js (§90 UI-024 bug fix): one procedure's own calls/callers, not a corpus scan.
        ALLOWED.put("parser/js/refs.js", 3);

        // ── Parser side, permanently exempt by design (UI-013's original 3-file list, each with
        //    its own header comment measuring the real payload): paging measurably makes these
        //    *worse* (payload dominated by accessDetails/table body text; row count barely moves
        //    bytes), or the data is structurally bounded per entity already.
        ALLOWED.put("parser/js/joins.js", 3);
        ALLOWED.put("parser/js/sequences.js", 3);
        ALLOWED.put("parser/js/tableDetail.js", 11);

        // ── PL/SQL side, dual-backend fallback (UI-028): sortKeys/searchFn/extraFilter apply only
        //    on the array path (a parser-backed analysis, PA.x._src === null); serverSorts applies
        //    only on the src path. Both are declared together deliberately — see each file's own
        //    "_initTable" comment.
        ALLOWED.put("plsql/js/tableOps.js", 3);
        ALLOWED.put("plsql/js/predicates.js", 3);
        ALLOWED.put("plsql/js/cursors.js", 3);
        ALLOWED.put("plsql/js/joins.js", 3);

        // ── PL/SQL side, UI-026h/UI-030 conversions: procReports and the four cross-reports are
        //    precomputed server-side (ProcedureReportBuilder/ProcedureCrossReportBuilder/
        //    ProcedureReportsService) and fetched whole, the same shape as the JAR side above.
        ALLOWED.put("plsql/js/summary-tables.js", 6);
        ALLOWED.put("plsql/js/summary-progress.js", 1);
        ALLOWED.put("plsql/js/summary-helpers.js", 1);
        ALLOWED.put("plsql/js/summary-corrections.js", 1);
        ALLOWED.put("plsql/js/summary-claude.js", 2);
        // app.js's own PA.refs table: one procedure's own calls/callers (PA.refs.callsItems /
        // .callersItems), the same bounded-per-procedure shape as parser/js/refs.js above.
        ALLOWED.put("plsql/js/app.js", 2);
    }

    @Test
    @DisplayName("no client-side filter/sort chain exists outside shared/ without a recorded reason")
    void noUnapprovedClientFilterChain() throws IOException {
        Path staticRoot = staticRoot();
        Map<String, Integer> counts = new TreeMap<>();
        Map<String, List<String>> where = new TreeMap<>();

        try (Stream<Path> files = Files.walk(staticRoot)) {
            for (Path f : files.filter(p -> p.toString().endsWith(".js")).toList()) {
                String rel = staticRoot.relativize(f).toString().replace('\\', '/');
                if (rel.startsWith("shared/") || rel.startsWith("tests/")) continue;

                int n = 0;
                List<String> hits = new ArrayList<>();
                List<String> lines = stripComments(Files.readString(f, StandardCharsets.UTF_8));
                for (int i = 0; i < lines.size(); i++) {
                    Matcher m = PATTERN.matcher(lines.get(i));
                    while (m.find()) {
                        n++;
                        hits.add(rel + ":" + (i + 1) + "  " + m.group());
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
                problems.add("NOT ALLOWED: " + e.getKey() + " has a client-side filter/sort chain ("
                        + e.getValue() + " occurrences). Convert it to server paging, or add it to "
                        + "ALLOWED with the reason it must stay local.\n    "
                        + String.join("\n    ", where.get(e.getKey())));
            } else if (e.getValue() > budget) {
                problems.add("BUDGET EXCEEDED: " + e.getKey() + " has " + e.getValue()
                        + " occurrences, allowed " + budget + ". New local filtering must be server-paged.\n    "
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
            fail("Client-side filter/sort chains outside shared/:\n\n" + String.join("\n\n", problems));
        }
    }

    /** Walks up from the working directory to find the static resources root. */
    private static Path staticRoot() {
        Path p = Path.of("").toAbsolutePath();
        while (p != null) {
            Path candidate = p.resolve("unified-web/src/main/resources/static");
            if (Files.isDirectory(candidate)) return candidate;
            Path direct = p.resolve("src/main/resources/static");
            if (Files.isDirectory(direct) && p.getFileName() != null && p.getFileName().toString().equals("unified-web")) {
                return direct;
            }
            p = p.getParent();
        }
        throw new IllegalStateException("Could not locate unified-web/src/main/resources/static");
    }

    /** Drops comments so a pattern mentioned in prose (like this file's own) is not counted. */
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
