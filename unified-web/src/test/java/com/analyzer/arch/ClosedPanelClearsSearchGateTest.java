package com.analyzer.arch;

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
 * A panel that hides itself must clear its search boxes.
 *
 * <h2>The defect</h2>
 * Closing a modal drops the table's filter state but leaves the DOM untouched, so reopening it
 * shows an unfiltered list <em>with the previous query still in the search box</em>. The box and
 * the rows then disagree and the screen gives no clue which to believe — a reader either trusts a
 * complete list as filtered, or a filtered-looking box as explaining the rows. The user found this
 * by using the app; no test would have.
 *
 * <h2>Why a static gate</h2>
 * The fix is one call at each close site, and the failure is invisible — nothing throws and every
 * screen still renders. The next modal added will hide itself the same way and reintroduce it, so
 * the rule is enforced against the source rather than left to reviewers to remember.
 *
 * <p>Calibrated by {@link #scannerCatchesAKnownViolation()}: a gate that cannot fail proves nothing,
 * and this repository has produced confidently wrong scanners before.
 */
class ClosedPanelClearsSearchGateTest {

    private static final Path STATIC = Path.of("src/main/resources/static");

    /**
     * The chat panels, exempt for the inverse defect: their input holds a half-written
     * <em>message</em>, and clearing it on close would destroy the user's own words.
     * {@code SearchQuery.resetWithin} also skips them by name, so exemption and helper agree.
     */
    private static final List<String> EXEMPT = List.of("chatbox.js", "xlsx.full.min.js");

    /**
     * A {@code display='none'} whose target is a whole panel, rather than a bar, badge, row or
     * tree node.
     *
     * <p>This restriction is the difference between a gate and a nuisance. The first version asked
     * only "does this file hide anything, and does it mention a search box anywhere" — and flagged
     * seven files that hide a count badge, a toolbar or a set of collapsed tree children while
     * having an unrelated filter box elsewhere in the same file. None was a defect. A gate with a
     * 100% false-positive rate teaches people to delete gates, so the receiver must look like a
     * panel for the finding to mean anything.
     */
    private static final java.util.regex.Pattern HIDES_A_PANEL = java.util.regex.Pattern.compile(
            "(?i)\\b[\\w.$()\\[\\]'\"-]*(modal|overlay|panel|dialog|popup)[\\w.$()\\[\\]'\"-]*"
                    + "\\s*\\.style\\.display\\s*=\\s*'none'");

    @Test
    @DisplayName("every panel that hides itself clears its search boxes")
    void closingClearsSearch() throws IOException {
        requireStatic(STATIC);
        List<String> violations = new ArrayList<>();
        List<Path> files;
        try (Stream<Path> w = Files.walk(STATIC)) {
            files = w.filter(Files::isRegularFile)
                     .filter(p -> p.getFileName().toString().endsWith(".js"))
                     .filter(p -> EXEMPT.stream().noneMatch(e -> p.getFileName().toString().equals(e)))
                     .sorted().toList();
        }
        for (Path f : files) {
            String src = Files.readString(f);
            boolean hides = HIDES_A_PANEL.matcher(src).find();
            boolean hasSearchBox = src.contains("type=\"search\"") || src.contains("type='search'")
                                || src.contains("-search") || src.contains("ad-cf");
            if (hides && hasSearchBox && !src.contains("SearchQuery.resetWithin")) {
                violations.add(STATIC.relativize(f).toString());
            }
        }
        if (!violations.isEmpty()) {
            fail("These hide a panel that contains a search box without clearing it, so it reopens\n"
               + "showing a query it is no longer applying. Call SearchQuery.resetWithin(<panel>)\n"
               + "before hiding it (shared/ui-busy.js):\n  " + String.join("\n  ", violations));
        }
    }

    @Test
    @DisplayName("the helper exists and protects message inputs")
    void helperExistsAndProtectsChat() throws IOException {
        String s = Files.readString(STATIC.resolve("shared/ui-busy.js"));
        assertTrue(s.contains("resetWithin:"), "SearchQuery.resetWithin missing");
        // Clearing a half-written chat message would be a worse bug than the one being fixed, so
        // the skip list is part of the contract rather than an implementation detail.
        assertTrue(s.contains("chat|message|prompt"),
                "resetWithin must skip message-style inputs by name");
    }

    @Test
    @DisplayName("the scanner catches a known violation")
    void scannerCatchesAKnownViolation() throws IOException {
        Path tmp = Files.createTempDirectory("close-clear-gate");
        try {
            Path js = tmp.resolve("bad.js");
            Files.writeString(js, """
                    function render() {
                        el.innerHTML = '<input type="search" class="x-search">';
                    }
                    function close() {
                        document.getElementById('myModal').style.display = 'none';
                    }
                    """);
            assertTrue(scanCount(tmp) == 1, "the scanner must flag a close with no reset");

            Files.writeString(js, """
                    function render() {
                        el.innerHTML = '<input type="search" class="x-search">';
                    }
                    function close() {
                        SearchQuery.resetWithin(document.getElementById('myModal'));
                        document.getElementById('myModal').style.display = 'none';
                    }
                    """);
            assertTrue(scanCount(tmp) == 0, "the corrected form must pass, or the gate is unsatisfiable");
        } finally {
            try (Stream<Path> w = Files.walk(tmp)) {
                w.sorted((a, b) -> b.getNameCount() - a.getNameCount()).forEach(p -> p.toFile().delete());
            }
        }
    }

    private int scanCount(Path root) throws IOException {
        int n = 0;
        try (Stream<Path> w = Files.walk(root)) {
            for (Path f : w.filter(Files::isRegularFile).toList()) {
                String src = Files.readString(f);
                boolean hides = HIDES_A_PANEL.matcher(src).find();
                boolean box = src.contains("type=\"search\"") || src.contains("-search");
                if (hides && box && !src.contains("SearchQuery.resetWithin")) n++;
            }
        }
        return n;
    }

    private static void requireStatic(Path p) {
        if (!Files.isDirectory(p)) {
            throw new IllegalStateException("static root not found at " + p.toAbsolutePath()
                    + " — a silently skipped gate is worse than none.");
        }
    }
}
