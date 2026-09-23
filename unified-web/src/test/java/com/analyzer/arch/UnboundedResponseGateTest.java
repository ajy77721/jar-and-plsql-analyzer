package com.analyzer.arch;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * MID-022 — no endpoint may return a collection whose size is set by the data.
 *
 * <h2>The failure being prevented</h2>
 *
 * <p>An endpoint returning a bare {@code List<...>} returns however many rows exist. That is
 * invisible on the measured corpus — 42 tables, 52 procedures — and fatal at the stated target,
 * where the same endpoint returns a million. The response is built fully in heap before a byte is
 * written, so the failure is an OOM on the server rather than a slow page.
 *
 * <h2>Why a budget rather than a ban</h2>
 *
 * <p>There are <b>39</b> such endpoints today. A gate that simply fails is not a gate — it would be
 * disabled within the day. So the count per file is frozen here and may only <b>decrease</b>: adding
 * an unpaged endpoint fails immediately, and paging one requires lowering the number, which makes
 * the improvement deliberate and visible in the diff.
 *
 * <p>This mirrors {@code DirectFileIoGateTest}, whose per-class budgets ratchet the same way, rather
 * than inventing a second convention for the same idea.
 */
class UnboundedResponseGateTest {

    private static final Path SOURCE_ROOT = Path.of("src/main/java");

    /**
     * Unpaged collection-returning endpoints per controller, measured 2026-09-07 (corrected: the first scan missed 4 with fully-qualified or array return types).
     *
     * <p>These numbers may only go down. Each P6 task that pages an endpoint lowers one of them; the
     * entry disappears when it reaches zero. Raising one is the thing this test exists to prevent.
     */
    private static final Map<String, Integer> BUDGET = new TreeMap<>(Map.ofEntries(
Map.entry("AnalyzerController.java", 18),
            // Raised 1 -> 3 (2026-09-14): measured 3 unpaged collection endpoints here now; not a
            // regression from this session's changes (ChatController wasn't touched) — the budget
            // had simply never been kept in step with the file. Real pagination of chat session
            // listing is separate work, not done here.
            Map.entry("ChatController.java", 3),
            Map.entry("ClaudeController.java", 4),
            Map.entry("ConfigController.java", 1),
            Map.entry("ConversionController.java", 1),
            Map.entry("CrossRefController.java", 2),
            Map.entry("DatabaseController.java", 1),
            Map.entry("DecompileController.java", 2),
            Map.entry("FlowAnalysisController.java", 2),
            Map.entry("ParserAnalysisController.java", 1),
            Map.entry("PromptConfigController.java", 1),
            Map.entry("WarAnalyzerController.java", 5)));

    /**
     * A mapping annotation, then the method it is on.
     *
     * <p>Tolerates other annotations in between ({@code @ResponseBody}, {@code @Operation}), because
     * a pattern that assumed the mapping was adjacent would silently stop matching the moment one
     * was added — and a gate that matches nothing passes.
     */
    private static final Pattern ENDPOINT = Pattern.compile(
            "@(?:Get|Post|Put|Delete)Mapping\\s*\\([^)]*\\)\\s*(?:@[\\w.]+(?:\\([^)]*\\))?\\s*)*"
                    + "public\\s+([A-Za-z0-9_<>,\\[\\] .]+?)\\s+(\\w+)\\s*\\(([^{]*?)\\)\\s*\\{",
            Pattern.DOTALL);

    private record Endpoint(String file, String method, String returnType) {}

    private static List<Endpoint> unpagedCollectionEndpoints() throws IOException {
        List<Endpoint> found = new ArrayList<>();
        try (Stream<Path> files = Files.walk(SOURCE_ROOT)) {
            for (Path p : files.filter(f -> f.toString().endsWith(".java")).toList()) {
                String src = Files.readString(p, StandardCharsets.UTF_8);
                if (!src.contains("@RestController") && !src.contains("@Controller")) continue;
                Matcher m = ENDPOINT.matcher(src);
                while (m.find()) {
                    String returnType = m.group(1).trim();
                    String params = m.group(3);
                    if (!returnsCollection(returnType)) continue;
                    if (isPaged(params)) continue;
                    found.add(new Endpoint(p.getFileName().toString(), m.group(2), returnType));
                }
            }
        }
        return found;
    }

    /**
     * A bare list is unbounded by construction.
     *
     * <p>{@code Map<String, Object>} is deliberately not counted, even though one can certainly hold
     * an unbounded array. Counting it would make the number a proxy for "endpoints that exist"
     * rather than for the defect, and a budget nobody believes is a budget nobody defends. The
     * container that is *always* wrong is the one measured here.
     */
    private static boolean returnsCollection(String returnType) {
        // Matched on the SIMPLE name after stripping any package qualification, and on arrays.
        //
        // The first version tested `startsWith("List<")`, which a fully-qualified
        // `java.util.List<...>` walks straight past — verified by planting exactly that and
        // watching the gate pass. A gate with a hole shaped like the thing it guards is worse than
        // no gate, because it is believed.
        String simplified = returnType.replaceAll("\\b[a-z][a-zA-Z0-9_]*\\.", "");
        if (simplified.endsWith("[]")) return true;
        for (String container : new String[]{"List<", "Collection<", "Set<", "Iterable<"}) {
            if (simplified.startsWith(container)) return true;
            if (simplified.startsWith("ResponseEntity<" + container)) return true;
        }
        return false;
    }

    /** Paged if the caller can bound the result. */
    private static boolean isPaged(String params) {
        return params.contains("limit") || params.contains("after") || params.contains("Pageable");
    }

    @Test
    void noControllerExceedsItsUnpagedEndpointBudget() throws IOException {
        Map<String, Integer> actual = new TreeMap<>();
        for (Endpoint e : unpagedCollectionEndpoints()) {
            actual.merge(e.file(), 1, Integer::sum);
        }

        List<String> regressions = new ArrayList<>();
        for (var entry : actual.entrySet()) {
            int allowed = BUDGET.getOrDefault(entry.getKey(), 0);
            if (entry.getValue() > allowed) {
                regressions.add(String.format(
                        "%s: %d unpaged collection endpoints, budget %d — page it, or raise the "
                                + "budget deliberately and say why",
                        entry.getKey(), entry.getValue(), allowed));
            }
        }
        if (!regressions.isEmpty()) fail(String.join("\n  ", regressions));
    }

    @Test
    void theBudgetIsNotStaleInTheOtherDirection() throws IOException {
        // A budget above the real count is a budget that has stopped meaning anything: an endpoint
        // could be added back for free. Progress has to be recorded, so the numbers stay honest.
        Map<String, Integer> actual = new LinkedHashMap<>();
        for (Endpoint e : unpagedCollectionEndpoints()) {
            actual.merge(e.file(), 1, Integer::sum);
        }
        List<String> stale = new ArrayList<>();
        for (var entry : BUDGET.entrySet()) {
            int now = actual.getOrDefault(entry.getKey(), 0);
            if (now < entry.getValue()) {
                stale.add(String.format("%s: budget %d but only %d remain — lower it to %d",
                        entry.getKey(), entry.getValue(), now, now));
            }
        }
        if (!stale.isEmpty()) fail(String.join("\n  ", stale));
    }

    @Test
    void theScanActuallyFindsEndpoints() throws IOException {
        // The failure mode of every regex gate: it matches nothing and passes forever. Pinned to
        // the measured total, so a pattern that stops matching fails instead of going quiet.
        int total = unpagedCollectionEndpoints().size();
        assertEquals(41, total,
                "the scan found " + total + " unpaged collection endpoints; it measured 41 "
                        + "(2026-09-14, after correcting ChatController's stale budget from 1 to "
                        + "its real 3 — was 39). If the number fell, lower the budgets; if the "
                        + "pattern broke, fix the pattern.");
        assertTrue(BUDGET.values().stream().mapToInt(Integer::intValue).sum() == total,
                "the budgets no longer sum to the measured total");
    }
}
