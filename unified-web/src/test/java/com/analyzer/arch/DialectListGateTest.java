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
 * No screen may hardcode the list of supported dialects.
 *
 * <p>Three files built a datasource dropdown from a literal array. Two of them read
 * {@code ['ORACLE','SQLSERVER','MYSQL','DB2']} — written before PostgreSQL support existed — and
 * one read all five. The server reported all five datasources correctly, so a PostgreSQL analysis
 * appeared in the history table and was <b>not offered in the filter</b>: present, but
 * unfilterable.
 *
 * <p>{@code history.js} carries the comment <i>"a filter that silently omits its own options is
 * worse than no filter"</i> fifteen lines above the literal that did exactly that. The intent was
 * right; a hardcoded list defeated it, and nothing failed.
 *
 * <p>All three now derive from {@code PA._DS_LABEL}, the single place a dialect is named, so a
 * sixth dialect reaches every screen at once. This gate keeps it that way.
 */
class DialectListGateTest {

    private static final Path STATIC = Path.of("src/main/resources/static");

    /** A literal array of dialect names — the shape that went stale. */
    private static final Pattern HARDCODED_DIALECTS = Pattern.compile(
            "\\[\\s*'(?:ORACLE|SQLSERVER|MYSQL|DB2|POSTGRES)'\\s*(?:,\\s*'(?:ORACLE|SQLSERVER|MYSQL|DB2|POSTGRES)'\\s*)+\\]");

    @Test
    @DisplayName("no asset builds a dialect list from a literal array")
    void dialectsAreNotHardcoded() throws IOException {
        if (!Files.isDirectory(STATIC)) {
            throw new IllegalStateException("static root missing at " + STATIC.toAbsolutePath()
                    + " — a silently skipped gate is worse than none");
        }
        List<String> hits = new ArrayList<>();
        try (Stream<Path> w = Files.walk(STATIC)) {
            for (Path f : w.filter(Files::isRegularFile)
                           .filter(p -> p.toString().endsWith(".js"))
                           .filter(p -> !p.toString().contains("xlsx"))
                           .sorted().toList()) {
                String src = Files.readString(f);
                Matcher m = HARDCODED_DIALECTS.matcher(src);
                while (m.find()) {
                    // Skip comments. The note explaining this very defect quotes the stale literal
                    // verbatim, and a gate that fails on its own documentation gets deleted — this
                    // is the third time that trap has come up in this repo.
                    if (inLineComment(src, m.start())) continue;
                    int line = (int) src.substring(0, m.start()).chars().filter(c -> c == '\n').count() + 1;
                    hits.add(STATIC.relativize(f) + ":" + line + "  " + m.group());
                }
            }
        }
        if (!hits.isEmpty()) {
            fail("These hardcode the dialect list. Two such lists went stale when PostgreSQL was\n"
                    + "added, leaving Postgres analyses visible in the table but missing from the\n"
                    + "filter. Iterate Object.keys(PA._DS_LABEL) instead:\n  "
                    + String.join("\n  ", hits));
        }
    }

    /** True when {@code at} sits after a {@code //} on its own line. */
    private static boolean inLineComment(String src, int at) {
        int lineStart = src.lastIndexOf('\n', at) + 1;
        int slashes = src.lastIndexOf("//", at);
        return slashes >= lineStart;
    }

    @Test
    @DisplayName("the one place dialects are named lists all five")
    void labelMapIsComplete() throws IOException {
        String src = Files.readString(STATIC.resolve("parser/js/history.js"));
        int at = src.indexOf("PA._DS_LABEL");
        assertTrue(at >= 0, "PA._DS_LABEL is gone — the screens derive their options from it");
        String decl = src.substring(at, Math.min(src.length(), at + 300));
        for (String d : List.of("ORACLE", "SQLSERVER", "MYSQL", "DB2", "POSTGRES")) {
            assertTrue(decl.contains(d),
                    d + " missing from PA._DS_LABEL, so every screen deriving from it loses that "
                            + "dialect's filter option");
        }
    }
}
