package com.plsql.parser.arch;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Statement-text capture must not be re-forked into the dialect visitors.
 *
 * <h2>Why this gate exists</h2>
 *
 * <p>{@code originalText} was copied byte-for-byte between the PostgreSQL and DB2 visitors. Both
 * copies carried the same two defects — a capture that ran past the read into the surrounding loop
 * body, and one that closed a parenthesis it never opened — and each had to be diagnosed and fixed
 * twice, on separate days, from separate measurements. The second time was only found because the
 * numbers were re-measured per dialect; nothing pointed from one copy to the other.
 *
 * <p>The logic now lives in {@code com.plsql.dialect.StatementTextCapture}. Copying is easier than
 * depending, so this makes the copy fail the build instead of being noticed in review, or not.
 *
 * <p>Calibrated by {@link #scannerCatchesAKnownViolation()} — a gate that cannot fail proves
 * nothing, and scanners in this repository have reported confident nonsense before.
 */
class NoForkedStatementCaptureGateTest {

    /** Visitor sources, relative to this module. */
    private static final List<Path> DIALECT_SRC = List.of(
            Path.of("../postgres-parser/src/main/java"),
            Path.of("../db2-parser/src/main/java"),
            Path.of("../mysql-parser/src/main/java"),
            Path.of("../sqlserver-parser/src/main/java"));

    /** Method bodies that belong to the shared class and nowhere else. */
    private static final List<String> FORK_MARKERS = List.of(
            "private static int parenDepth(",
            "isBlockKeywordEndingCapture(");

    @Test
    @DisplayName("no dialect visitor re-implements the shared capture helpers")
    void captureIsNotForked() throws IOException {
        List<String> violations = scan(DIALECT_SRC);
        if (!violations.isEmpty()) {
            fail("These re-implement logic that lives in com.plsql.dialect.StatementTextCapture.\n"
               + "It was already fixed twice as two copies; depend on the shared class instead:\n  "
               + String.join("\n  ", violations));
        }
    }

    @Test
    @DisplayName("the shared class is actually where the logic lives")
    void sharedClassExists() throws IOException {
        Path p = Path.of("../dialect-common/src/main/java/com/plsql/dialect/StatementTextCapture.java");
        assertTrue(Files.isRegularFile(p), "StatementTextCapture is missing at " + p.toAbsolutePath());
        String s = Files.readString(p);
        assertTrue(s.contains("parenDepth"), "the shared class lost parenDepth");
        // If the module tree moves, this gate must fail loudly rather than scan nothing and pass.
        assertTrue(DIALECT_SRC.stream().anyMatch(Files::isDirectory),
                "no dialect source tree found — the gate would have scanned nothing");
    }

    @Test
    @DisplayName("the scanner catches a known violation")
    void scannerCatchesAKnownViolation() throws IOException {
        Path tmp = Files.createTempDirectory("fork-gate");
        try {
            Files.writeString(tmp.resolve("Bad.java"),
                    "class Bad { private static int parenDepth(Object cs, int a, int b){ return 0; } }");
            assertTrue(scan(List.of(tmp)).size() == 1, "the scanner must flag a re-forked helper");
            Files.writeString(tmp.resolve("Bad.java"),
                    "class Bad { String t(){ return com.plsql.dialect.StatementTextCapture.capture(null, null); } }");
            assertTrue(scan(List.of(tmp)).isEmpty(), "the delegating form must pass");
        } finally {
            try (Stream<Path> w = Files.walk(tmp)) {
                w.sorted((a, b) -> b.getNameCount() - a.getNameCount()).forEach(p -> p.toFile().delete());
            }
        }
    }

    private List<String> scan(List<Path> roots) throws IOException {
        List<String> out = new ArrayList<>();
        for (Path root : roots) {
            if (!Files.isDirectory(root)) continue;
            try (Stream<Path> w = Files.walk(root)) {
                for (Path f : w.filter(Files::isRegularFile)
                               .filter(p -> p.getFileName().toString().endsWith(".java")).toList()) {
                    String src = Files.readString(f);
                    for (String marker : FORK_MARKERS) {
                        if (src.contains(marker)) out.add(f + "  (" + marker + ")");
                    }
                }
            }
        }
        return out;
    }
}
