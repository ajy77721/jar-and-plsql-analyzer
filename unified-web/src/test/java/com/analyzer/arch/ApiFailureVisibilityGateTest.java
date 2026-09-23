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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A failed request must never render as an empty result (§API-ERROR-CONTRACT).
 *
 * <p>The browser API layers answered {@code return []} on any non-OK response, so a 500 and "this
 * table has no writers" arrived as the same value. Screens rendered their empty state and the user
 * believed a statement about their code that was really a failed request. On a tool whose whole
 * purpose is reporting what code touches, that is the worst defect available — and the sharpest
 * example was a green <b>"No parse errors"</b> toast shown when no analysis was loaded at all.
 *
 * <p>The fix is a sentinel: {@code PA.FAILED} is a frozen empty array, so every render path keeps
 * working, while {@code PA.failed(x)} identifies it and {@code PA.apiFail(res)} raises a banner.
 * This gate keeps the bare {@code return []} from coming back.
 */
class ApiFailureVisibilityGateTest {

    private static final Path STATIC = Path.of("src/main/resources/static");

    /** The API layers that were rewritten. */
    private static final List<String> API_FILES = List.of(
            "jar/js/api.js", "parser/js/api.js", "plsql/js/api.js", "war/js/api.js");

    /** `if (!res.ok) return [];` and friends — a failure silently shaped like an empty result. */
    private static final Pattern SILENT_EMPTY =
            Pattern.compile("if\\s*\\(\\s*!res\\.ok\\s*\\)\\s*(\\{\\s*)?return\\s*(\\[\\s*\\]|\\{\\s*\\}|null)\\s*;");

    @Test
    @DisplayName("no API call turns a failed response into a bare empty result")
    void failuresAreNotSilentlyEmpty() throws IOException {
        List<String> bad = new ArrayList<>();
        for (String rel : API_FILES) {
            Path f = STATIC.resolve(rel);
            if (!Files.isDirectory(STATIC)) {
                throw new IllegalStateException("static root missing at " + STATIC.toAbsolutePath()
                        + " — a silently skipped gate is worse than none");
            }
            String src = Files.readString(f);
            Matcher m = SILENT_EMPTY.matcher(src);
            while (m.find()) {
                int line = (int) src.substring(0, m.start()).chars().filter(c -> c == '\n').count() + 1;
                bad.add(rel + ":" + line + "  " + m.group());
            }
        }
        if (!bad.isEmpty()) {
            fail("These turn a failed HTTP response into an empty result the screen renders as fact.\n"
                    + "Use PA.apiFail(res) / JA.apiFail(res), which returns the FAILED sentinel and\n"
                    + "raises a banner:\n  " + String.join("\n  ", bad));
        }
    }

    @Test
    @DisplayName("the sentinel is defined where every page can reach it")
    void sentinelIsGloballyAvailable() throws IOException {
        String env = Files.readString(STATIC.resolve("shared/page-envelope.js"));
        for (String sym : List.of("FAILED", "apiFail", "failed")) {
            assertTrue(env.contains(sym), "page-envelope.js is missing " + sym);
        }
        assertTrue(env.contains("Object.freeze([])"),
                "FAILED must be a frozen empty array: an array so render paths keep working, frozen "
                        + "so nothing can mutate it and destroy its identity");
        assertTrue(env.contains("res.url"),
                "apiFail must read the URL off the Response. An earlier version took it as an "
                        + "argument, and 76 of 80 call sites had no such variable in scope — that "
                        + "would have thrown ReferenceError on nearly every request in the app");
        // Both namespaces: jar/war use JA, the rest use PA.
        assertTrue(env.contains("JAsafe.apiFail") || env.contains("JA.apiFail"),
                "the jar and war pages use JA and would not find a PA-only helper");
    }

    @Test
    @DisplayName("the server does not answer 200-empty when there is no analysis")
    void serverDistinguishesMissingFromEmpty() throws IOException {
        String misc = Files.readString(Path.of(
                "src/main/java/com/plsqlanalyzer/web/controller/detail/MiscQueryService.java"));
        assertTrue(misc.contains("notFound().build()"),
                "getParseErrors returned 200 with an empty list when no analysis was loaded, and the "
                        + "UI showed a green 'No parse errors' toast — an error state confirming a "
                        + "fact about the user's code");
        assertFalse(misc.contains("log.error(\"Failed to get references: {}\", e.getMessage());\n"
                        + "            return ResponseEntity.ok(Collections.emptyList());"),
                "a failed live-database query must not render as 'referenced by nothing'");
    }
}
