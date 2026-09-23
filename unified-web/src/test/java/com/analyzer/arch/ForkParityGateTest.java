package com.analyzer.arch;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The JAR and WAR controllers must not drift apart on the endpoints they share.
 *
 * <h2>Why this gate exists</h2>
 *
 * <p>The two forks serve the same routes over the same analysis shape, and fixes have repeatedly
 * landed on one and not the other:
 *
 * <ul>
 *   <li><b>`members`</b> — the JAR class tree gained the parameter that removes 93.8% of the
 *       response. The WAR endpoint had no such parameter <em>at all</em> and always sent the full
 *       tree (MID-018).</li>
 *   <li><b>`/collections`</b> — both loaded the entire analysis to build a 3.8 KB aggregate. Fixing
 *       one would have left the other reading 49.4 MB per request (MID-013 / MID-014).</li>
 * </ul>
 *
 * <p>The pattern is not carelessness, it is duplication: two hand-written copies of the same logic
 * cannot be fixed once. So this checks the shared behaviour rather than the text — the forks are
 * allowed to differ, they are not allowed to differ <em>here</em>.
 */
class ForkParityGateTest {

    private static final Path JAR =
            Path.of("src/main/java/com/jaranalyzer/controller/AnalyzerController.java");
    private static final Path WAR =
            Path.of("src/main/java/com/waranalyzer/controller/WarAnalyzerController.java");

    private static String read(Path p) throws IOException {
        return Files.readString(p);
    }

    @Test
    void bothForksDelegateTheCollectionReportToOneAggregator() throws IOException {
        // Not "both are streamed" — both must use the SAME code, or the next fix lands once again.
        for (Path p : List.of(JAR, WAR)) {
            String src = read(p);
            assertTrue(src.contains("CollectionReportAggregator.aggregate("),
                    p.getFileName() + " builds the collection report itself rather than delegating; "
                            + "a second copy is how the forks came to differ in the first place");
            assertTrue(!src.contains("ep.computeAggregates()"),
                    p.getFileName() + " still walks endpoint objects, so it is still loading the "
                            + "whole analysis to produce a few kilobytes");
        }
    }

    @Test
    void neitherForkLoadsTheAnalysisForTheCollectionReport() throws IOException {
        // The defect was the load, not the response size.
        for (Path p : List.of(JAR, WAR)) {
            String src = read(p);
            int at = src.indexOf("/{id}/collections\")");
            assertTrue(at > 0, p.getFileName() + " has no /collections endpoint");
            // The method body, up to the next mapping annotation.
            int next = src.indexOf("@GetMapping", at + 10);
            int end = next < 0 ? Math.min(src.length(), at + 3000) : next;
            String body = src.substring(at, end);
            assertTrue(!body.contains("ersistenceService.load("),
                    p.getFileName() + " still loads the whole analysis for the collection report");
        }
    }

    @Test
    void bothClassTreeEndpointsOmitMembersByDefault() throws IOException {
        // MID-018's divergence, pinned so it cannot recur on either side.
        List<String> wrong = new ArrayList<>();
        for (Path p : List.of(JAR, WAR)) {
            String src = read(p);
            if (!src.contains("/classes/tree")) continue;
            if (!src.contains("\"members\"")) {
                wrong.add(p.getFileName() + " has no members parameter");
            } else if (!src.contains("value = \"members\", required = false, defaultValue = \"false\"")) {
                wrong.add(p.getFileName() + " does not default members to false");
            }
        }
        assertEquals(List.of(), wrong, "the forks disagree on the class-tree members default");
    }
}
