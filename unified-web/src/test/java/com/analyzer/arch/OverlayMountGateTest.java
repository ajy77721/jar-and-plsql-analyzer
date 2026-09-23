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
 * Opening the same overlay twice must not stack two of them.
 *
 * <p>Overlays were appended with {@code document.body.insertAdjacentHTML('beforeend', html)}, and
 * their markup declares a <b>fixed id</b>. Three clicks produced three overlays sharing one id —
 * and the close handler,
 * {@code document.getElementById('sum-trace-overlay').remove()}, removes only the <b>first</b>
 * match. So closing removed the one underneath while the visible one stayed, and the screen looked
 * stuck. Reported from live use: "click 3 time … on same screen open 3 modle one top of top".
 *
 * <p>{@code PA.mountOverlay} / {@code JA.mountOverlay} remove every existing instance of that id
 * before appending, which makes re-opening idempotent. 25 call sites were converted.
 */
class OverlayMountGateTest {

    private static final Path STATIC = Path.of("src/main/resources/static");

    /** Appending an overlay straight to the body, bypassing the helper. */
    private static final Pattern RAW_BODY_APPEND =
            Pattern.compile("document\\.body\\.insertAdjacentHTML\\(\\s*'beforeend'");

    @Test
    @DisplayName("no overlay is appended to the body without the idempotent helper")
    void everyOverlayGoesThroughTheHelper() throws IOException {
        if (!Files.isDirectory(STATIC)) {
            throw new IllegalStateException("static root missing at " + STATIC.toAbsolutePath()
                    + " — a silently skipped gate is worse than none");
        }
        List<String> raw = new ArrayList<>();
        try (Stream<Path> w = Files.walk(STATIC)) {
            for (Path f : w.filter(Files::isRegularFile)
                           .filter(p -> p.toString().endsWith(".js"))
                           .filter(p -> !p.toString().contains("xlsx"))
                           // the helper itself is the one legitimate caller
                           .filter(p -> !p.getFileName().toString().equals("page-envelope.js"))
                           .sorted().toList()) {
                String src = Files.readString(f);
                Matcher m = RAW_BODY_APPEND.matcher(src);
                while (m.find()) {
                    int line = (int) src.substring(0, m.start()).chars().filter(c -> c == '\n').count() + 1;
                    raw.add(STATIC.relativize(f) + ":" + line);
                }
            }
        }
        if (!raw.isEmpty()) {
            fail("These append an overlay straight to document.body. If the markup carries a fixed "
                    + "id, clicking twice stacks two overlays and the close handler only removes "
                    + "the first. Use PA.mountOverlay(html) / JA.mountOverlay(html):\n  "
                    + String.join("\n  ", raw));
        }
    }

    @Test
    @DisplayName("the helper removes every prior instance, not just the first")
    void helperClearsThePile() throws IOException {
        String env = Files.readString(STATIC.resolve("shared/page-envelope.js"));
        assertTrue(env.contains("mountOverlay"), "page-envelope.js defines no mountOverlay");
        assertTrue(env.contains("querySelectorAll"),
                "the helper must clear EVERY instance of the id — getElementById would remove one "
                        + "and leave any pile that had already accumulated");
        assertTrue(env.contains("PA = window.PA") && env.contains("JA = window.JA"),
                "both namespaces need it: jar and war use JA, the other pages use PA");
    }
}
