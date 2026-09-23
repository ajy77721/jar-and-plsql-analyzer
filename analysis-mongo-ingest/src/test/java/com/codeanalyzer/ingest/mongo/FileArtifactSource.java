package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.MongoIngestService.ArtifactSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Reads artifacts from an analysis directory on disk.
 *
 * <p>Test-scope only. Production ingest reads through {@code ArtifactStore}, which is why this
 * module adds no {@code DirectFileIoGateTest} allowlist entries — the gate scans main sources.
 */
record FileArtifactSource(Path root) implements ArtifactSource {

    @Override
    public List<String> list(String directory) throws IOException {
        Path d = root.resolve(directory);
        if (!Files.isDirectory(d)) return List.of();
        try (var s = Files.list(d)) {
            return s.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".json"))
                    .map(p -> directory + "/" + p.getFileName())
                    .sorted()
                    .toList();
        }
    }

    @Override
    public InputStream open(String artifactPath) throws IOException {
        Path p = root.resolve(artifactPath);
        return Files.isRegularFile(p) ? Files.newInputStream(p) : null;
    }
}
