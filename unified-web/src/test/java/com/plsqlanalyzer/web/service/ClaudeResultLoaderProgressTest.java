package com.plsqlanalyzer.web.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.LocalArtifactStore;
import com.plsqlanalyzer.web.parser.service.AnalysisPaths;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@code getProgress} must count only real CHUNK artifacts.
 *
 * <p>A leading {@code '_'} marks an internal sidecar ({@code _index.json}, {@code _checkpoint.json},
 * {@code _result.json} and their {@code _error.txt} companions). The error count came from an
 * unfiltered {@code endsWith("_error.txt")} scan, so an internal failure was reported to the UI as
 * a failed chunk — inflating errorChunks above the total chunk count.
 */
class ClaudeResultLoaderProgressTest {

    private static final String NAME = "DB2~Db2Demo~local~ERP_USP_RUN_CYCLE";

    @TempDir
    Path root;

    private ArtifactStore store;
    private ClaudeResultLoader loader;

    @BeforeEach
    void setUp() {
        store = new LocalArtifactStore(root);
        loader = new ClaudeResultLoader(new ObjectMapper(), store, null, null);
    }

    private ArtifactKey claudeDir() {
        // Stale fixture found live: ClaudeResultLoader/ClaudeVerificationRunner/ClaudeChunkReader
        // were deliberately renamed from "claude" to "claude-legacy" (see ClaudeVerificationRunner's
        // own doc comment — the old name collided with ClaudeVerificationService's unrelated
        // "claude" subfolder and could clobber its fragments). This test predated that rename and
        // was writing fixtures to a folder getProgress() never reads, so every assertion silently
        // saw an empty store instead of the fixtures below.
        return AnalysisPaths.claudeFileKey(NAME, "claude-legacy");
    }

    private void write(String leaf, String body) throws IOException {
        try (OutputStream out = store.write(claudeDir().resolve(leaf))) {
            out.write(body.getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test
    @DisplayName("internal '_'-prefixed error sidecars are not counted as chunk errors")
    void internalErrorSidecarsAreNotChunkErrors() throws IOException {
        write("_index.json", "{\"chunks\":[{\"id\":\"chunk_1\"},{\"id\":\"chunk_2\"}]}");
        write("chunk_1_error.txt", "boom");          // a real chunk failure
        write("_result_error.txt", "internal boom"); // internal sidecar — must be ignored
        write("_index_error.txt", "internal boom");  // internal sidecar — must be ignored

        Map<String, Object> progress = loader.getProgress(NAME);
        assertEquals(1, progress.get("errorChunks"),
                "only chunk_1_error.txt is a chunk error; the two '_' sidecars are internal");
        assertEquals(2, progress.get("totalChunks"));
    }

    @Test
    @DisplayName("the completed-count fallback also ignores '_'-prefixed outputs")
    void internalOutputsAreNotCompletedChunks() throws IOException {
        write("_index.json", "{\"chunks\":[{\"id\":\"chunk_1\"},{\"id\":\"chunk_2\"}]}");
        write("chunk_1_output.json", "{}");
        write("_result_output.json", "{}"); // internal — must not count as a completed chunk

        Map<String, Object> progress = loader.getProgress(NAME);
        assertEquals(1, progress.get("completedChunks"));
        assertEquals(50, progress.get("percentComplete"));
        assertEquals(0, progress.get("errorChunks"));
    }

    @Test
    @DisplayName("real chunk errors are still counted")
    void realChunkErrorsAreCounted() throws IOException {
        write("_index.json", "{\"chunks\":[{\"id\":\"chunk_1\"},{\"id\":\"chunk_2\"}]}");
        write("chunk_1_error.txt", "boom");
        write("chunk_2_error.txt", "boom");

        assertEquals(2, loader.getProgress(NAME).get("errorChunks"));
    }
}
