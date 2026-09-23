package com.jaranalyzer.controller;

import com.analyzer.storage.log.LineBufferTail;
import com.analyzer.storage.log.LogLine;
import com.analyzer.storage.log.LogTail;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The conversion detail screen polled {@code GET /jobs/{id}} every 1.5 s and got the whole job
 * back: the progress log, up to 2 000 live output lines, and the immutable generated instruction —
 * 261 KB measured on a realistic job, re-sent whether or not anything had changed.
 *
 * <p>Two properties have to hold for the replacement to be safe, and neither is about bytes.
 *
 * <p><b>The screen must render the same thing.</b> The live pane coloured each line by re-running
 * five regexes in the browser. Those regexes moved to the server as a {@code level} field, and if
 * the classification differs by one case the user sees a differently coloured log and nothing
 * fails. The precedence between the five tests is load-bearing — "Phase 3 failed" was
 * <em>phase</em>, not <em>err</em> — so it is pinned here rather than left to be rediscovered.
 *
 * <p><b>The old endpoint must not change.</b> {@code /jobs/{id}} is what makes this conversion
 * revertible on its own: reverting the client leaves a server that still answers the old shape. A
 * well-meaning later cleanup that strips {@code liveLines} from it to "finish the job" would break
 * exactly that, silently, so the frozen fields are asserted.
 */
class ConversionJobPollTest {

    // ── The five regexes, moved from the browser ─────────────────────────────

    @Test
    @DisplayName("the level classifier reproduces the browser's five regexes, in the same order")
    void classifierMatchesTheBrowser() {
        // /phase \d|---/ wins outright — this is the precedence trap.
        assertEquals("PHASE", ConversionController.classifyLine("--- Phase 4 — Coverage ---"));
        assertEquals("PHASE", ConversionController.classifyLine("Phase 3 failed to complete"),
                "the browser coloured this 'phase', not 'err'; keeping that is the point");
        assertEquals("PHASE", ConversionController.classifyLine("---"));

        assertEquals("DONE", ConversionController.classifyLine("✓ Instruction ready"));
        assertEquals("DONE", ConversionController.classifyLine("✅ Completed in 42s"));
        assertEquals("DONE", ConversionController.classifyLine("mapping done for OrderService"));
        assertEquals("DONE", ConversionController.classifyLine("all tests passed"));

        assertEquals("WARN", ConversionController.classifyLine("⚠ Claude exited with code 1"));
        assertEquals("WARN", ConversionController.classifyLine("caution: unresolved dynamic SQL"));

        assertEquals("ERROR", ConversionController.classifyLine("❌ Claude error: no output"));
        assertEquals("ERROR", ConversionController.classifyLine("Cannot start CLI: failure"));

        assertEquals("INFO", ConversionController.classifyLine("info: 12 files scanned"));

        assertEquals("", ConversionController.classifyLine("  ↳ 42:    public void run() {"));
        assertEquals("", ConversionController.classifyLine(""));
        assertEquals("", ConversionController.classifyLine(null));
    }

    @Test
    @DisplayName("case does not change the classification, as in the browser's lowercased test")
    void classifierIsCaseInsensitive() {
        assertEquals("ERROR", ConversionController.classifyLine("FAILED to compile"));
        assertEquals("ERROR", ConversionController.classifyLine("failed to compile"));
        assertEquals("WARN", ConversionController.classifyLine("WARNING: deprecated"));
    }

    // ── The frozen legacy response ───────────────────────────────────────────

    @Test
    @DisplayName("/jobs/{id} still carries log, liveLines and instructionMd — the revert path")
    void legacyJobResponseIsUnchanged() {
        ConversionController.JobState s = state();
        s.addLog("⏳ Building Claude instruction file…");
        s.appendLiveLine("--- Phase 1 ---");
        s.instructionMd = "# instruction";

        Map<String, Object> full = s.toMap(true);
        assertEquals(List.of("⏳ Building Claude instruction file…"), full.get("log"));
        assertEquals(List.of("--- Phase 1 ---"), full.get("liveLines"));
        assertEquals("# instruction", full.get("instructionMd"));
        for (String f : List.of("jobId", "action", "procedure", "module", "projectPath",
                                "status", "phase", "startedAt", "elapsedMs")) {
            assertTrue(full.containsKey(f), "legacy field missing: " + f);
        }

        // The list view has always omitted liveLines; that is unchanged too.
        assertFalse(s.toMap(false).containsKey("liveLines"));
    }

    // ── The rolling buffer's sequence numbers ────────────────────────────────

    @Test
    @DisplayName("the live cursor counts lines ever appended, so eviction cannot renumber it")
    void evictionDoesNotRenumberTheCursor() {
        ConversionController.JobState s = state();
        for (int i = 1; i <= 2100; i++) s.appendLiveLine("line " + i);

        assertEquals(2000, s.liveLines.size(), "the rolling cap still holds");
        assertEquals(2100, s.liveAppended.get(), "but the watermark counts everything appended");

        // A client that had seen line 2098 gets exactly two lines, numbered 2099 and 2100 — not
        // "the last two entries of a 2000-element list", which would be a different pair every
        // time the buffer evicted.
        LogTail t = LineBufferTail.of(new ArrayList<>(s.liveLines), s.liveAppended.get(),
                2098L, 200, null, null, ConversionController::classifyLine);
        List<LogLine> items = t.page().items();
        assertEquals(2, items.size());
        assertEquals(2099, items.get(0).seq());
        assertEquals("line 2099", items.get(0).message());
        assertEquals(2100, t.watermark());
        assertEquals(101, t.fromSeq(), "the oldest line still retained");
        assertFalse(t.reset());
    }

    @Test
    @DisplayName("a restored job's live buffer is numbered from 1 — nothing else could be pointed at")
    void restoredJobHasAConsistentWatermark() {
        ConversionController.JobState s = ConversionController.JobState.fromMap(Map.of(
                "jobId", "7", "action", "fix", "status", "interrupted", "phase", "Interrupted",
                "liveLines", List.of("a", "b", "c"), "log", List.of("x")));

        assertEquals(3, s.liveAppended.get());
        LogTail t = LineBufferTail.of(new ArrayList<>(s.liveLines), s.liveAppended.get(),
                null, 200, null, null, ConversionController::classifyLine);
        assertEquals(1, t.fromSeq());
        assertEquals(3, t.watermark());
        assertEquals(3, t.page().items().size());
    }

    private static ConversionController.JobState state() {
        return new ConversionController.JobState("1", "validate",
                new ConversionController.ConversionRequest("P", "m", null, null, null, "/p", null,
                        false));
    }
}
