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
 * Guards the two halves of the {@code PollConfig} repair described in PLAN-08 §5.2.
 *
 * <p>The configuration mechanism existed and did nothing. Three separate defects: {@code load()}
 * was never awaited, so timers started during script execution captured the hard-coded defaults;
 * nothing under {@code parser/**} referenced the file at all, so six intervals were literals and
 * every one of them polled faster than the configured value; and {@code parser/index.html} loaded
 * {@code shared/log-fab.js} without loading {@code shared/poll-config.js}, so the FAB's Auto
 * checkbox threw {@code ReferenceError} on every click.
 *
 * <p>Both failures are invisible at runtime — a poll that runs at 2 s instead of 30 s looks like a
 * working feature, and the {@code ReferenceError} is one line in a console nobody has open. So
 * they are asserted statically instead, which is also the only form of this check that stays true
 * after the next commit.
 */
class PollingConfigGateTest {

    /**
     * A literal millisecond delay handed to {@code setInterval}.
     *
     * <p>Only {@code setInterval} — a {@code setTimeout} is a one-shot, usually an animation or a
     * DOM settle, and has nothing to do with polling. Three digits or more, so a 20 ms debounce is
     * not swept up with a 30 000 ms poll.
     */
    private static final Pattern LITERAL_INTERVAL = Pattern.compile(",\\s*(\\d{3,})\\s*$");

    /** A script tag's {@code src}, so a filename mentioned in a comment is not a load. */
    private static final Pattern SCRIPT_SRC =
            Pattern.compile("<script[^>]*\\ssrc\\s*=\\s*[\"']([^\"']+)[\"']");

    /** Marks a timer that drives only local state (an elapsed counter, a DOM-ready wait). */
    private static final String EXEMPT = "poll-exempt";

    /**
     * The directories PLAN-08 §5.2 names, plus the static root itself.
     *
     * <p>{@code "."} is not decoration. The root holds {@code index.html}, the home page, and it was
     * outside this list — so the two worst pollers in the app sat in a file this gate never opened
     * while it stayed green. One of them polled {@code /api/queue} every 3 s forever, measured at
     * <b>21 requests in 70 seconds</b> on an idle page with an empty queue, and it could not have
     * read {@code PollConfig} in any case because {@code poll-config.js} loaded ~350 lines later.
     *
     * <p>The root is walked non-recursively (see {@link #assetFiles}); the named directories are
     * walked in full, as before.
     */
    private static final List<String> POLLING_DIRS =
            List.of(".", "jar", "parser", "plsql", "conversion", "shared");

    @Test
    @DisplayName("no literal-ms setInterval in polling code — intervals come from PollConfig")
    void pollingIntervalsAreConfigured() throws IOException {
        Path staticRoot = repoRoot().resolve("unified-web/src/main/resources/static");
        List<String> problems = new ArrayList<>();

        for (Path f : assetFiles(staticRoot, POLLING_DIRS)) {
            if (f.getFileName().toString().equals("poll-config.js")) continue;
            // Each call is isolated by matching parentheses rather than by one regex over the file.
            // The common shape puts the callback on one line and the delay a dozen lines later, so
            // a per-line scan misses every one of them, and a newline-spanning regex backtracks
            // catastrophically on a 4 000-line asset.
            String src = Files.readString(f, StandardCharsets.UTF_8);
            for (int at = src.indexOf("setInterval"); at >= 0; at = src.indexOf("setInterval", at + 1)) {
                // Skip occurrences inside a line comment. Documentation explaining a poller that
                // was REMOVED often quotes the old call verbatim, and a gate that trips on the note
                // describing its own fix is a gate someone deletes. Only `//` is handled: a
                // setInterval inside a block comment is dead code that should be deleted, and
                // treating it as invisible would be the wrong lesson.
                if (inLineComment(src, at)) continue;
                int open = src.indexOf('(', at);
                if (open < 0) break;
                int close = matchingParen(src, open);
                if (close < 0) continue;
                String call = src.substring(open + 1, close);
                // The exemption may sit anywhere inside the call — usually on the opening line.
                if (call.contains(EXEMPT)) continue;
                Matcher m = LITERAL_INTERVAL.matcher(call.stripTrailing());
                if (!m.find()) continue;
                int line = (int) src.substring(0, at).chars().filter(c -> c == '\n').count() + 1;
                problems.add(staticRoot.relativize(f) + ":" + line + "  setInterval(…, "
                        + m.group(1) + ") — use PollConfig.every('<key>', fn), or mark the call "
                        + "'// " + EXEMPT + ": <reason>' if it drives no network request");
            }
        }

        if (!problems.isEmpty()) {
            fail("Hard-coded poll intervals (PLAN-08 §5.2 defect 2):\n  "
                    + String.join("\n  ", problems));
        }
    }

    @Test
    @DisplayName("every page that loads log-fab.js also loads poll-config.js, first")
    void pagesLoadingLogFabAlsoLoadPollConfig() throws IOException {
        Path staticRoot = repoRoot().resolve("unified-web/src/main/resources/static");
        List<String> problems = new ArrayList<>();

        try (Stream<Path> files = Files.walk(staticRoot)) {
            for (Path f : files.filter(p -> p.toString().endsWith(".html"))
                               .filter(p -> !p.toString().contains("_migration-backup"))
                               .sorted().toList()) {
                String html = Files.readString(f, StandardCharsets.UTF_8);
                int fab = -1, cfg = -1;
                Matcher m = SCRIPT_SRC.matcher(html);
                while (m.find()) {
                    if (fab < 0 && m.group(1).endsWith("log-fab.js")) fab = m.start();
                    if (cfg < 0 && m.group(1).endsWith("poll-config.js")) cfg = m.start();
                }
                if (fab < 0) continue;
                String page = staticRoot.relativize(f).toString();
                if (cfg < 0) {
                    problems.add(page + " loads log-fab.js but not shared/poll-config.js — "
                            + "log-fab.js reads PollConfig.logRefreshMs and will throw "
                            + "ReferenceError");
                } else if (cfg > fab) {
                    problems.add(page + " loads poll-config.js after log-fab.js — it must come "
                            + "first, as plsql/index.html does");
                }
            }
        }

        if (!problems.isEmpty()) {
            fail("Missing PollConfig on a page that needs it (PLAN-08 §5.2 defect 3):\n  "
                    + String.join("\n  ", problems));
        }
    }

    /** Index of the {@code )} closing the {@code (} at {@code open}, or -1 if unbalanced. */
    private static int matchingParen(String src, int open) {
        int depth = 0;
        for (int i = open; i < src.length(); i++) {
            char c = src.charAt(i);
            if (c == '(') depth++;
            else if (c == ')' && --depth == 0) return i;
        }
        return -1;
    }

    /**
     * Recursive {@code setTimeout} pollers must be configured too.
     *
     * <p>The other test only inspects {@code setInterval}, and a self-rescheduling
     * {@code setTimeout} is the same thing with a different spelling — it was invisible to this
     * gate. Three real pollers used it, including two that hit {@code /api/queue} every 3 s and one
     * that never stopped when the queue was empty.
     *
     * <p>Recognised by shape: a {@code setTimeout} whose body re-enters the function it is inside.
     * Rather than parse JavaScript, this looks for a {@code setTimeout} with a literal delay that
     * appears inside a function whose own name is called within that timeout — the pattern every
     * one of these uses. Narrow on purpose: a false positive here costs more than a missed case,
     * because a gate that cries wolf gets switched off.
     */
    @Test
    @DisplayName("recursive setTimeout pollers use PollConfig, like setInterval must")
    void recursiveTimeoutPollersAreConfigured() throws IOException {
        Path staticRoot = repoRoot().resolve("unified-web/src/main/resources/static");
        List<String> problems = new ArrayList<>();

        for (Path f : assetFiles(staticRoot, POLLING_DIRS)) {
            if (f.getFileName().toString().equals("poll-config.js")) continue;
            String src = Files.readString(f, StandardCharsets.UTF_8);
            for (int at = src.indexOf("setTimeout"); at >= 0; at = src.indexOf("setTimeout", at + 1)) {
                if (inLineComment(src, at)) continue;
                int open = src.indexOf('(', at);
                if (open < 0) break;
                int close = matchingParen(src, open);
                if (close < 0) continue;
                String call = src.substring(open + 1, close);
                // The marker may sit inside the call or in the comment immediately above it. A
                // one-line reason fits inside; anything worth explaining properly does not, and
                // forcing it inside produces a worse comment than the rule was trying to encourage.
                if (call.contains(EXEMPT) || precededByExemption(src, at)) continue;

                Matcher m = LITERAL_INTERVAL.matcher(call.stripTrailing());
                if (!m.find()) continue;
                // Only a delay long enough to be a poll. Short timeouts are animation and
                // debounce, which are not network polling and must not be dragged in here.
                if (Integer.parseInt(m.group(1)) < 1000) continue;
                // Self-rescheduling: the timeout body mentions a fetch or re-enters a render.
                if (!call.contains("fetch") && !call.contains("_render") && !call.contains("poll")
                        && !call.contains("Poll") && !call.contains("refresh")) continue;

                int line = (int) src.substring(0, at).chars().filter(c -> c == '\n').count() + 1;
                problems.add(staticRoot.relativize(f) + ":" + line + "  setTimeout(…, "
                        + m.group(1) + ") re-polls — use PollConfig, or mark it '// " + EXEMPT + "'");
            }
        }

        if (!problems.isEmpty()) {
            fail("Self-rescheduling setTimeout pollers with hard-coded intervals — the spelling the\n"
                    + "setInterval check cannot see:\n  " + String.join("\n  ", problems));
        }
    }

    /** True when a {@code poll-exempt} marker appears in the comment block just above {@code at}. */
    private static boolean precededByExemption(String src, int at) {
        int lineStart = src.lastIndexOf('\n', at) + 1;
        // Walk back over contiguous comment lines only, so an exemption three statements above
        // cannot silently cover this one.
        int cursor = lineStart;
        for (int i = 0; i < 6 && cursor > 0; i++) {
            int prevEnd = cursor - 1;
            int prevStart = src.lastIndexOf('\n', prevEnd - 1) + 1;
            if (prevStart < 0 || prevStart >= prevEnd) break;
            String prev = src.substring(prevStart, prevEnd).trim();
            if (!prev.startsWith("//")) break;
            if (prev.contains(EXEMPT)) return true;
            cursor = prevStart;
        }
        return false;
    }

    /** True when {@code at} sits after a {@code //} on its own line. */
    private static boolean inLineComment(String src, int at) {
        int lineStart = src.lastIndexOf('\n', at) + 1;
        int slashes = src.lastIndexOf("//", at);
        return slashes >= lineStart;
    }

    private static List<Path> assetFiles(Path staticRoot, List<String> dirs) throws IOException {
        List<Path> out = new ArrayList<>();
        for (String d : dirs) {
            Path dir = staticRoot.resolve(d);
            if (!Files.isDirectory(dir)) continue;
            // The root is depth 1: walking it recursively would re-scan every named directory and
            // report each finding twice.
            int depth = ".".equals(d) ? 1 : Integer.MAX_VALUE;
            try (Stream<Path> files = Files.walk(dir, depth)) {
                out.addAll(files.filter(Files::isRegularFile)
                                .filter(p -> p.toString().endsWith(".js")
                                          || p.toString().endsWith(".html"))
                                .filter(p -> !p.toString().contains("_migration-backup"))
                                .sorted().toList());
            }
        }
        return out;
    }

    /** Walks up from the working directory, so the test runs from any module. */
    private static Path repoRoot() {
        Path p = Path.of("").toAbsolutePath();
        while (p != null && !Files.exists(p.resolve("analyzer-storage"))) {
            p = p.getParent();
        }
        if (p == null) throw new IllegalStateException("Could not locate the repository root");
        return p;
    }
}
