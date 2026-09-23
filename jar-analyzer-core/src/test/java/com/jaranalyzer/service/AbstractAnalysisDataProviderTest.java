package com.jaranalyzer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("AbstractAnalysisDataProvider — streamClassTree safety")
class AbstractAnalysisDataProviderTest {

    @TempDir
    Path tempDir;

    private TestableProvider provider;
    private ObjectMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper();
        provider = new TestableProvider(tempDir.resolve("dummy.json"), mapper, tempDir);
    }

    // ── streamClassTree: safe cast guard ──────────────────────────────────

    @Test
    @DisplayName("streamClassTree handles normal object entries without throwing")
    void streamClassTree_handles_normal_class_object() throws Exception {
        String json = "{\"classes\": [{\"fullyQualifiedName\": \"com.example.Foo\", \"methods\": []}]}";
        String output = runStreamClassTree(json);
        assertTrue(output.contains("com.example.Foo"),
                "Normal class entry should be written to output");
    }

    @Test
    @DisplayName("streamClassTree skips non-object entry without ClassCastException")
    void streamClassTree_skips_non_object_entry_gracefully() throws Exception {
        // A classes array containing a primitive integer — malformed but should not crash
        String json = "{\"classes\": [42, {\"fullyQualifiedName\": \"com.example.Bar\", \"methods\": []}]}";
        String output = assertDoesNotThrow(() -> runStreamClassTree(json),
                "Non-object entry must not cause ClassCastException");
        // The valid object after the bad entry should still be emitted
        assertTrue(output.contains("com.example.Bar"),
                "Valid entry after bad entry should still appear in output");
    }

    @Test
    @DisplayName("streamClassTree skips string entry in classes array")
    void streamClassTree_skips_string_entry_gracefully() throws Exception {
        String json = "{\"classes\": [\"unexpected\", {\"fullyQualifiedName\": \"com.Good\", \"methods\": []}]}";
        String output = assertDoesNotThrow(() -> runStreamClassTree(json));
        assertTrue(output.contains("com.Good"));
    }

    @Test
    @DisplayName("streamClassTree returns empty array when classes is absent")
    void streamClassTree_empty_when_no_classes_field() throws Exception {
        String json = "{\"otherField\": \"value\"}";
        String output = runStreamClassTree(json);
        assertEquals("[]", output.trim());
    }

    @Test
    @DisplayName("streamClassTree returns empty array for empty classes array")
    void streamClassTree_empty_array() throws Exception {
        String json = "{\"classes\": []}";
        String output = runStreamClassTree(json);
        assertEquals("[]", output.trim());
    }

    @Test
    @DisplayName("streamClassTree processes multiple valid classes")
    void streamClassTree_multiple_classes() throws Exception {
        String json = "{\"classes\": ["
                + "{\"fullyQualifiedName\": \"com.A\", \"methods\": []},"
                + "{\"fullyQualifiedName\": \"com.B\", \"methods\": []}"
                + "]}";
        String output = runStreamClassTree(json);
        assertTrue(output.contains("com.A"));
        assertTrue(output.contains("com.B"));
    }

    // ── Fix A: invocations removed after hasOutboundRestCall detection ────

    @Test
    @DisplayName("streamClassTreeRemovesInvocationsAfterDetection: invocations absent, hasOutboundRestCall true")
    void streamClassTreeRemovesInvocationsAfterDetection() throws Exception {
        String json = "{\"classes\": [{"
                + "\"fullyQualifiedName\": \"com.example.MyService\","
                + "\"methods\": [{"
                + "  \"name\": \"callRemote\","
                + "  \"invocations\": [{"
                + "    \"ownerClass\": \"org.springframework.web.client.RestTemplate\","
                + "    \"methodName\": \"getForObject\""
                + "  }]"
                + "}]"
                + "}]}";
        String output = runStreamClassTree(json);

        // hasOutboundRestCall must be detected
        assertTrue(output.contains("\"hasOutboundRestCall\":true"),
                "Method with RestTemplate invocation should have hasOutboundRestCall=true; output: " + output);

        // invocations must NOT appear in the output (stripped after detection)
        assertFalse(output.contains("\"invocations\""),
                "invocations array must be removed from output after detection; output: " + output);
        assertFalse(output.contains("ownerClass"),
                "ownerClass (inside invocations) must not appear in output; output: " + output);
    }

    // ── Fix B: non-object node in classes array skipped gracefully ────────

    @Test
    @DisplayName("streamClassTreeSkipsNonObjectNode: string/number at class level does not throw")
    void streamClassTreeSkipsNonObjectNode() throws Exception {
        // Mix of bad entries (string, number) and one valid class object
        String json = "{\"classes\": ["
                + "\"unexpected-string\","
                + "99,"
                + "{\"fullyQualifiedName\": \"com.example.Good\", \"methods\": []}"
                + "]}";
        String output = assertDoesNotThrow(() -> runStreamClassTree(json),
                "Non-object entries must not cause exceptions");
        assertTrue(output.contains("com.example.Good"),
                "Valid class after bad entries must still be emitted; output: " + output);
        assertFalse(output.contains("unexpected-string"),
                "String entry must not appear in output; output: " + output);
    }

    // ── Fix C: HTTP_CLIENT_PREFIXES detects Feign, WebClient, HttpClient ─

    @Test
    @DisplayName("httpClientPrefixesDetectedCorrectly: Feign, WebClient, HttpClient all set hasOutboundRestCall")
    void httpClientPrefixesDetectedCorrectly() throws Exception {
        String[] restOwners = {
                "org.springframework.web.reactive.function.client.WebClient",
                "feign.Client",
                "feign.SomeSpecificClient",
                "org.apache.http.client.HttpClient",
                "java.net.http.HttpClient",
                "org.springframework.cloud.openfeign.FeignClient"
        };

        for (String owner : restOwners) {
            String json = "{\"classes\": [{"
                    + "\"fullyQualifiedName\": \"com.example.Caller\","
                    + "\"methods\": [{"
                    + "  \"name\": \"doCall\","
                    + "  \"invocations\": [{\"ownerClass\": \"" + owner + "\", \"methodName\": \"execute\"}]"
                    + "}]"
                    + "}]}";
            String output = runStreamClassTree(json);
            assertTrue(output.contains("\"hasOutboundRestCall\":true"),
                    "Owner class '" + owner + "' should trigger hasOutboundRestCall=true; output: " + output);
        }
    }

    // ── helpers ───────────────────────────────────────────────────────────

    private String runStreamClassTree(String analysisJson) throws Exception {
        try (var parser = mapper.getFactory().createParser(analysisJson);
             var baos = new ByteArrayOutputStream();
             var gen = mapper.getFactory().createGenerator(baos)) {
            gen.writeStartArray();
            provider.streamClassTree(parser, gen);
            gen.writeEndArray();
            gen.flush();
            // strip the outer array wrapper added by test harness
            String raw = baos.toString();
            return raw.substring(1, raw.length() - 1).trim();
        }
    }

    // Minimal concrete subclass — just enough to instantiate the abstract class.
    private static class TestableProvider extends AbstractAnalysisDataProvider {
        TestableProvider(Path filePath, ObjectMapper objectMapper, Path baseDir) {
            this(new JarDataPaths(baseDir.resolve("jar").toString()), filePath, objectMapper);
        }

        private TestableProvider(JarDataPaths paths, Path filePath, ObjectMapper objectMapper) {
            super(paths.keyForPath(filePath), objectMapper, paths, "test-jar");
        }

        @Override public String getProviderType() { return "TEST"; }
        @Override public com.analyzer.storage.ArtifactKey getSummaryCacheKey() {
            return com.analyzer.storage.ArtifactKey.of("jar", "test-jar", "summary.json");
        }
        @Override public boolean shouldComputeVerification() { return false; }
    }
}
