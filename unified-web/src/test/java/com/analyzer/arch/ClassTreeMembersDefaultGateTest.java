package com.analyzer.arch;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * MID-018 — the class tree omits member arrays unless asked, on every fork.
 *
 * <h2>The measurement</h2>
 *
 * <p>Member arrays are <b>93.8%</b> of this response: 8.2 MB of `methodSummaries` plus 3.6 MB of
 * `fields` out of 11.8 MB for 1,623 classes. The tree paints class rows only —
 * `_renderClassBody` renders whatever members are present and `_loadClassMembers` fetches them per
 * class on expand — so the arrays were downloaded and ignored.
 *
 * <h2>Why the default is the whole point</h2>
 *
 * <p>`members` existed and defaulted to <b>true</b>, so the expensive answer was the one you got by
 * forgetting. Two callers forgot:
 *
 * <ul>
 *   <li>`jar/js/api.js:classTreePages` — the <b>paged</b> path the Code Structure tab actually
 *       uses. The `members=false` fix had been applied only to the unpaged fallback beside it.</li>
 *   <li>the entire WAR fork, whose endpoint had no `members` parameter at all.</li>
 * </ul>
 *
 * <p>A parameter that has to be remembered at every call site will be forgotten at one of them. The
 * default is what makes the saving real rather than available.
 */
class ClassTreeMembersDefaultGateTest {

    private static final Path SOURCE_ROOT = Path.of("src/main/java");

    /** `members` declarations on a class-tree endpoint, with the default they declare. */
    private record MembersParam(String file, String declaredDefault) {}

    private static final Pattern MEMBERS_PARAM = Pattern.compile(
            "@RequestParam\\(\\s*value\\s*=\\s*\"members\"[^)]*defaultValue\\s*=\\s*\"(\\w+)\"[^)]*\\)",
            Pattern.DOTALL);

    private static List<MembersParam> membersParams() throws IOException {
        List<MembersParam> found = new ArrayList<>();
        try (Stream<Path> files = Files.walk(SOURCE_ROOT)) {
            for (Path p : files.filter(f -> f.toString().endsWith("Controller.java")).toList()) {
                String src = Files.readString(p);
                if (!src.contains("/classes/tree")) continue;
                Matcher m = MEMBERS_PARAM.matcher(src);
                while (m.find()) {
                    found.add(new MembersParam(p.getFileName().toString(), m.group(1)));
                }
            }
        }
        return found;
    }

    @Test
    void everyClassTreeEndpointDefaultsToOmittingMembers() throws IOException {
        List<MembersParam> params = membersParams();
        assertTrue(params.size() >= 2,
                "expected a members parameter on both the JAR and WAR class-tree endpoints, found "
                        + params.size() + " — an endpoint with no parameter cannot omit members "
                        + "at all, which is how the WAR fork kept sending the full tree");

        List<String> wrong = params.stream()
                .filter(p -> !"false".equals(p.declaredDefault()))
                .map(p -> p.file() + " defaults members to " + p.declaredDefault())
                .toList();
        assertEquals(List.of(), wrong,
                "these endpoints make the 11.8 MB answer the one you get by forgetting");
    }

    @Test
    void everyClassTreeEndpointAcceptsTheParameter() throws IOException {
        // A fork without the parameter is not merely defaulted wrong — it cannot be asked.
        List<String> without = new ArrayList<>();
        try (Stream<Path> files = Files.walk(SOURCE_ROOT)) {
            for (Path p : files.filter(f -> f.toString().endsWith("Controller.java")).toList()) {
                String src = Files.readString(p);
                if (!src.contains("/classes/tree")) continue;
                if (!src.contains("\"members\"")) without.add(p.getFileName().toString());
            }
        }
        assertEquals(List.of(), without,
                "these class-tree endpoints have no members parameter, so they always send the "
                        + "full tree regardless of what a caller asks for");
    }

    @Test
    void theStreamingProviderCanActuallyOmitThem() throws IOException {
        // The parameter is only worth having if it reaches something that honours it. A default of
        // false plus a provider that ignores it would look fixed and change nothing.
        Path api = Path.of("../jar-analyzer-core/src/main/java/com/jaranalyzer/service/AnalysisDataProvider.java");
        if (!Files.isRegularFile(api)) return;
        String src = Files.readString(api);
        assertTrue(src.contains("boolean includeMembers"),
                "the provider has no member-omitting overload, so the parameter cannot be honoured");
    }
}
