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
 * Fails the build when production code outside a true CLI {@code main} can terminate the JVM.
 *
 * <p>The mandate this encodes: the application must NEVER terminate itself. A failed analysis,
 * request or session must fail the <em>request</em> or the <em>session</em> — the queue marks the
 * job FAILED, a session ends, an exception surfaces to the caller — never the whole process. The
 * server path (unified-web) runs every user's jobs in one JVM; a stray {@code System.exit} on any
 * of its threads (the queue worker, a pool, a Claude-CLI child-process handler) kills every other
 * user's in-flight work along with it.
 *
 * <p>A one-off audit answers this for one commit and is wrong by the next. So the inventory lives
 * here as a gate, mirroring {@link DirectFileIoGateTest}: {@link ALLOWED} is the complete list of
 * classes permitted to name a JVM-terminating call, each a genuine process entry point, with its
 * count. Removing an exit means deleting or lowering its line; adding one anywhere else fails with
 * the exact file and line. A class that improves must ratchet its budget down, or the budget
 * silently becomes headroom for a regression.
 *
 * <p>A second test bans the JVM flags that turn an out-of-memory condition into a process exit
 * ({@code -XX:+ExitOnOutOfMemoryError}, {@code -XX:OnOutOfMemoryError=...}) from the Dockerfile and
 * the start scripts, because those bypass every Java-level guard below.
 */
class NoJvmSelfTerminationGateTest {

    /** Optional package qualifier, so {@code java.lang.System.exit} is caught as well as {@code System.exit}. */
    private static final String Q = "(?:[a-zA-Z_][\\w.]*\\.)?";

    private static final Map<String, Pattern> PATTERNS = new LinkedHashMap<>();
    static {
        PATTERNS.put("System.exit",   Pattern.compile("\\b" + Q + "System\\s*\\.\\s*exit\\s*\\("));
        PATTERNS.put("System.halt",   Pattern.compile("\\b" + Q + "System\\s*\\.\\s*halt\\s*\\("));
        PATTERNS.put("Runtime.exit",  Pattern.compile("\\b" + Q + "Runtime\\s*\\.\\s*getRuntime\\s*\\(\\s*\\)\\s*\\.\\s*(?:exit|halt)\\s*\\("));
        PATTERNS.put("Runtime.halt",  Pattern.compile("\\b" + Q + "Runtime\\s*\\.\\s*halt\\s*\\("));
        PATTERNS.put("Signal.handle", Pattern.compile("\\b" + Q + "(?:sun\\.misc\\.)?Signal\\s*\\.\\s*handle\\s*\\("));
        PATTERNS.put("ProcessHandle.parent",
                Pattern.compile("\\b" + Q + "ProcessHandle\\s*\\.\\s*current\\s*\\(\\s*\\)\\s*\\.\\s*parent\\s*\\("));
    }

    /**
     * Classes permitted to terminate the JVM, with the count of terminating calls and the reason.
     *
     * <p>Key is {@code <module>/<SimpleName>}. Every entry MUST define a {@code public static void
     * main(String[])} — i.e. it is a real process entry point. If a class here ever stops being one,
     * this test should fail rather than let the allowance linger.
     */
    private static final Map<String, Integer> ALLOWED = new LinkedHashMap<>();
    static {
        // True process entry points: a CLI tool exits by design. These are never on the
        // unified-web request/analysis/session path.
        ALLOWED.put("plsql-parser/PlSqlParserMain", 1);
        ALLOWED.put("analysis-mongo-ingest/IngestMain", 1);
        ALLOWED.put("flow-poc/PocRunner", 1);
    }

    @Test
    @DisplayName("no production class outside a CLI main can terminate the JVM")
    void noUnapprovedJvmSelfTermination() throws IOException {
        Path root = repoRoot();
        Map<String, Integer> counts = new TreeMap<>();
        Map<String, List<String>> where = new TreeMap<>();
        Map<String, Boolean> declaresMain = new HashMap<>();

        try (Stream<Path> files = Files.walk(root)) {
            for (Path f : files.filter(p -> p.toString().endsWith(".java"))
                               .filter(NoJvmSelfTerminationGateTest::isProductionSource).toList()) {
                String module = root.relativize(f).getName(0).toString();
                String id = module + "/" + f.getFileName().toString().replace(".java", "");

                List<String> lines = stripComments(Files.readString(f, StandardCharsets.UTF_8));
                declaresMain.put(id, lines.stream().anyMatch(l -> l.matches(
                        ".*\\bpublic\\s+static\\s+void\\s+main\\s*\\(\\s*String\\s*\\[\\s*\\]\\s+\\w+\\s*\\).*")));

                int n = 0;
                List<String> hits = new ArrayList<>();
                for (int i = 0; i < lines.size(); i++) {
                    for (Map.Entry<String, Pattern> e : PATTERNS.entrySet()) {
                        Matcher m = e.getValue().matcher(lines.get(i));
                        while (m.find()) {
                            n++;
                            hits.add(root.relativize(f) + ":" + (i + 1) + "  " + e.getKey());
                        }
                    }
                }
                if (n > 0) {
                    counts.put(id, n);
                    where.computeIfAbsent(id, k -> new ArrayList<>()).addAll(hits);
                }
            }
        }

        List<String> problems = new ArrayList<>();
        for (Map.Entry<String, Integer> e : counts.entrySet()) {
            Integer budget = ALLOWED.get(e.getKey());
            if (budget == null) {
                problems.add("NOT ALLOWED: " + e.getKey() + " can terminate the JVM (" + e.getValue()
                        + " call(s)). Fail the request/session/job instead — never the process. If "
                        + "this really is a CLI entry point, give it a main() and add it to ALLOWED "
                        + "with the reason.\n    " + String.join("\n    ", where.get(e.getKey())));
            } else if (e.getValue() > budget) {
                problems.add("BUDGET EXCEEDED: " + e.getKey() + " has " + e.getValue()
                        + " JVM-terminating call(s), allowed " + budget + ".\n    "
                        + String.join("\n    ", where.get(e.getKey())));
            }
        }
        // A class that improved must ratchet, or its budget becomes headroom for the next regression.
        for (Map.Entry<String, Integer> e : ALLOWED.entrySet()) {
            int actual = counts.getOrDefault(e.getKey(), 0);
            if (actual < e.getValue()) {
                problems.add("RATCHET: " + e.getKey() + " is down to " + actual + " JVM-terminating "
                        + "call(s) (budget " + e.getValue() + "). Lower the budget in ALLOWED"
                        + (actual == 0 ? ", or delete the entry — it no longer terminates the JVM." : "."));
            }
            if (!Boolean.TRUE.equals(declaresMain.get(e.getKey()))) {
                problems.add("NOT A CLI MAIN: " + e.getKey() + " is allowlisted to terminate the JVM "
                        + "but does not declare a public static void main(String[]). A CLI exemption "
                        + "is only valid for a real process entry point.");
            }
        }

        if (!problems.isEmpty()) {
            fail("JVM self-termination outside a CLI main:\n\n" + String.join("\n\n", problems));
        }
    }

    @Test
    @DisplayName("no JVM/script flag turns out-of-memory into a process exit")
    void noOutOfMemoryDrivenExitFlags() throws IOException {
        Path root = repoRoot();
        // Flags that make the JVM (or a shell) kill the process on OOM, bypassing every Java guard.
        List<Pattern> flags = List.of(
                Pattern.compile("-XX:\\+?ExitOnOutOfMemoryError"),
                Pattern.compile("-XX:\\+?CrashOnOutOfMemoryError"),
                Pattern.compile("-XX:OnOutOfMemoryError="));
        List<String> hits = new ArrayList<>();

        try (Stream<Path> files = Files.walk(root)) {
            for (Path f : files.filter(NoJvmSelfTerminationGateTest::isConfigOrScript).toList()) {
                List<String> lines;
                try {
                    lines = Files.readAllLines(f, StandardCharsets.UTF_8);
                } catch (IOException notText) {
                    continue;
                }
                for (int i = 0; i < lines.size(); i++) {
                    for (Pattern p : flags) {
                        if (p.matcher(lines.get(i)).find()) {
                            hits.add(root.relativize(f) + ":" + (i + 1) + "  " + lines.get(i).trim());
                        }
                    }
                }
            }
        }
        if (!hits.isEmpty()) {
            fail("Out-of-memory conditions must fail the request/job, not the JVM. Remove these flags "
                    + "(HeapDumpOnOutOfMemoryError is fine and stays):\n    " + String.join("\n    ", hits));
        }
    }

    private static boolean isProductionSource(Path p) {
        String s = p.toString();
        return s.contains("/src/main/java/")
                && !s.contains("/target/")
                && !s.contains("_migration-backup")
                && !s.contains("data_old_");
    }

    /** Build/run configuration and scripts — where a JVM-launching flag would live. */
    private static boolean isConfigOrScript(Path p) {
        String s = p.toString().replace('\\', '/');
        if (s.contains("/target/") || s.contains("/.git/") || s.contains("/node_modules/")) return false;
        String name = p.getFileName().toString();
        if (name.equals("Dockerfile") || name.equals(".drone.yml") || name.equals(".drone.yaml")) return true;
        return s.endsWith(".sh") || s.endsWith(".bat") || s.endsWith(".cmd") || s.endsWith(".ps1")
                || s.endsWith(".yml") || s.endsWith(".yaml");
    }

    /** Walks up from the working directory to the reactor root, so the test runs from any module. */
    private static Path repoRoot() {
        Path p = Path.of("").toAbsolutePath();
        while (p != null && !Files.exists(p.resolve("analyzer-storage"))) {
            p = p.getParent();
        }
        if (p == null) throw new IllegalStateException("Could not locate the repository root");
        return p;
    }

    /** Drops comments so a pattern mentioned in prose is not counted as a call. */
    private static List<String> stripComments(String src) {
        String noBlock = src.replaceAll("(?s)/\\*.*?\\*/", "");
        List<String> out = new ArrayList<>();
        for (String line : noBlock.split("\n", -1)) {
            StringBuilder kept = new StringBuilder();
            boolean inString = false;
            for (int i = 0; i < line.length(); i++) {
                char c = line.charAt(i);
                if (c == '"' && (i == 0 || line.charAt(i - 1) != '\\')) inString = !inString;
                if (!inString && c == '/' && i + 1 < line.length() && line.charAt(i + 1) == '/') break;
                kept.append(c);
            }
            out.add(kept.toString());
        }
        return out;
    }
}
