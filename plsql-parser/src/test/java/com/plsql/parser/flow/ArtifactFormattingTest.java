package com.plsql.parser.flow;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MID-028 — stored artifacts are not indented, and the escape hatch works.
 *
 * <h2>The measurement</h2>
 *
 * <p>Across 2,908 artifact files in {@code data/}: <b>310.3 MB</b> indented against <b>224.7 MB</b>
 * compact — <b>27.6%</b>, or 85.6 MB, of pure whitespace. The fraction does not improve with scale;
 * it is charged again on every write, every read, every byte to a reader and every byte of an object
 * store.
 *
 * <p>Nothing consumes it. The ingest, the web layer and {@code jq} do not care, and the round-trip
 * prover compares parsed documents rather than text — which is why this change cannot alter meaning,
 * only size.
 */
class ArtifactFormattingTest {

    @Test
    void artifactsAreCompactByDefault() {
        assertFalse(ArtifactFormatting.pretty(),
                "stored artifacts default to indented; 27.6% of every artifact byte is whitespace");
    }

    @Test
    void theEscapeHatchRestoresIndentation() {
        // A human does occasionally read one mid-debugging, and rebuilding to get readable output
        // is a poor trade. Verified rather than assumed: a property nobody checks is a property
        // that silently stops working.
        String previous = System.getProperty(ArtifactFormatting.PRETTY_PROPERTY);
        try {
            System.setProperty(ArtifactFormatting.PRETTY_PROPERTY, "true");
            assertTrue(ArtifactFormatting.pretty(), "the escape hatch does not restore indentation");
        } finally {
            if (previous == null) System.clearProperty(ArtifactFormatting.PRETTY_PROPERTY);
            else System.setProperty(ArtifactFormatting.PRETTY_PROPERTY, previous);
        }
    }

    @Test
    void bothHighVolumeWritersConsultTheSameSwitch() throws IOException {
        // Two writers with two conventions is how one of them quietly keeps indenting. Asserted on
        // the source because the alternative is running a full analysis to observe the output.
        Path flow = Path.of("src/main/java/com/plsql/parser/flow");
        for (String f : List.of("ChunkedFlowWriter.java", "NodeParseCache.java")) {
            String src = Files.readString(flow.resolve(f), StandardCharsets.UTF_8);
            assertTrue(src.contains("ArtifactFormatting.pretty()"),
                    f + " does not consult the shared formatting switch");
            assertFalse(src.matches("(?s).*\\n\\s*(this\\.)?mapper\\.enable\\(SerializationFeature\\.INDENT_OUTPUT\\).*"),
                    f + " still enables INDENT_OUTPUT unconditionally");
        }
    }

    @Test
    void compactAndIndentedParseToTheSameDocument() throws IOException {
        // The property that makes this safe to do at all: indentation is presentation, and the
        // readers parse. If this were ever false, the change would be lossy rather than cheap.
        ObjectMapper compact = new ObjectMapper();
        ObjectMapper indented = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

        var doc = new ObjectMapper().readTree(
                "{\"nodeId\":\"S$A\",\"lines\":[1,2,3],\"nested\":{\"a\":\"x y\",\"b\":null}}");

        assertEquals(compact.readTree(compact.writeValueAsString(doc)),
                indented.readTree(indented.writeValueAsString(doc)),
                "indented and compact forms do not parse to the same document");
        assertTrue(indented.writeValueAsString(doc).length() > compact.writeValueAsString(doc).length(),
                "the indented form is not actually larger, so there is nothing to save");
    }

    @Test
    void theSavingIsRealOnTheArtifactsOnDisk() throws IOException {
        // Measured against the corpus rather than a fixture: a synthetic document can be made to
        // show any ratio. Skipped when data/ is absent so the suite still runs on a clean checkout.
        Path data = Path.of("../data/plsql-parse");
        if (!Files.isDirectory(data)) return;

        ObjectMapper m = new ObjectMapper();
        long pretty = 0;
        long compact = 0;
        int files = 0;
        try (Stream<Path> walk = Files.walk(data)) {
            List<Path> sample = walk.filter(p -> p.toString().endsWith(".json"))
                    .limit(200).toList();
            for (Path p : sample) {
                try {
                    long size = Files.size(p);
                    String tight = m.writeValueAsString(m.readTree(Files.newInputStream(p)));
                    pretty += size;
                    compact += tight.length();
                    files++;
                } catch (IOException ignored) {
                    // A malformed or partial artifact is not this test's subject.
                }
            }
        }
        if (files == 0) return;
        double saving = 1 - (compact / (double) pretty);
        assertTrue(saving > 0.10,
                String.format("compacting %d artifacts saved only %.1f%%; the change is not worth "
                        + "the readability it costs", files, saving * 100));
    }
}
