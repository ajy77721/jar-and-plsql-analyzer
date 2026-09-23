package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.map.IngestContext;
import com.codeanalyzer.ingest.mongo.map.jar.AnalysisJsonMapper;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import org.bson.Document;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Counts what the mapper emits, with no database involved.
 *
 * <p>Separating the two halves matters: "33,407 rows are in MongoDB but the file has 33,409" does
 * not say whether the mapper never produced them or the writer lost them, and those have different
 * fixes. This runs the real mapper against the real file and counts, so the question is answered
 * rather than argued about.
 */
class AnalysisJsonMapperCountTest {

    @Test
    @DisplayName("the mapper emits one document per method, with distinct ids")
    void mapperEmitsEveryMethod() throws Exception {
        Path dir = newestJarAnalysis();
        Assumptions.assumeTrue(dir != null, "no JAR analysis on disk");

        List<Document> methods = new ArrayList<>();
        IngestContext ctx = new IngestContext("run~v1", "JAR");
        try (var in = Files.newInputStream(dir.resolve("analysis.json"))) {
            new AnalysisJsonMapper().map(ctx, in, (coll, doc) -> {
                if (Collections.JAVA_METHOD.equals(coll)) methods.add(doc);
            });
        }

        Set<Object> ids = new HashSet<>();
        Set<String> keys = new HashSet<>();
        for (Document d : methods) { ids.add(d.get("_id")); keys.add(d.getString("entityKey")); }

        // If emitted == source but ids collide, the writer collapsed them and the id scheme is at
        // fault. If emitted < source, the mapper is dropping them. The two counts separate those.
        assertEquals(methods.size(), keys.size(),
                "two emitted methods share an entityKey: " + (methods.size() - keys.size()) + " collisions");
        assertEquals(methods.size(), ids.size(),
                "two emitted methods share an _id: " + (methods.size() - ids.size())
                        + " content-hash collisions — different methods would overwrite each other");
        // Counted from the file under test rather than hard-coded. A literal here plus a fuzzy
        // file selector is what manufactured a phantom "two methods lost" bug: the number came
        // from one analysis and the ingest from another.
        int expected = 0;
        com.fasterxml.jackson.databind.JsonNode root =
                new com.fasterxml.jackson.databind.ObjectMapper().readTree(dir.resolve("analysis.json").toFile());
        for (com.fasterxml.jackson.databind.JsonNode c : root.path("classes")) {
            expected += c.path("methods").size();
        }
        assertEquals(expected, methods.size(), "the mapper did not emit every method in the file");
    }

    /**
     * The most recent JAR analysis, chosen by the mtime of {@code analysis.json} itself.
     *
     * <p>Sorting by the <em>directory</em> mtime picked the wrong one: a directory is touched by
     * anything that writes inside it, so an older analysis whose folder was touched later won. The
     * test then ingested one file and asserted counts taken from another, which looked exactly
     * like the mapper silently dropping two methods and cost a long hunt to disprove.
     */
    static Path newestJarAnalysis() throws Exception {
        Path root = Path.of("../data/jar");
        if (!Files.isDirectory(root)) return null;
        try (Stream<Path> s = Files.list(root)) {
            return s.filter(Files::isDirectory)
                    .filter(p -> Files.isRegularFile(p.resolve("analysis.json")))
                    .max(Comparator.comparingLong(
                            p -> p.resolve("analysis.json").toFile().lastModified()))
                    .orElse(null);
        }
    }
}
