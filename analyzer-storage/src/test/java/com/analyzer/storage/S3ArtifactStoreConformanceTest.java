package com.analyzer.storage;

import com.analyzer.storage.s3.FakeS3Server;
import com.analyzer.storage.s3.S3ArtifactStore;
import org.junit.jupiter.api.AfterEach;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.UncheckedIOException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;

/**
 * The {@link ArtifactStoreConformanceTest} contract, run against {@link S3ArtifactStore}.
 *
 * <p>This is the assertion the interface's own docs ask for: "callers must never assume which is
 * active". Every behaviour the local backend promises is re-run here over real HTTP against
 * {@link FakeS3Server}, unchanged — so a semantic divergence between the two backends fails the
 * build instead of surfacing as a production bug in S3 mode only.
 *
 * <p>{@code root} from the base class is ignored: S3 has no filesystem root. The bucket is fresh
 * per test, which gives the same isolation the {@code @TempDir} gives locally.
 */
class S3ArtifactStoreConformanceTest extends ArtifactStoreConformanceTest {

    private FakeS3Server server;
    private S3Client client;

    /**
     * Created lazily rather than in a {@code @BeforeEach}: the base class's own {@code @BeforeEach}
     * runs first and is what calls this, so a subclass setup method would be too late.
     */
    @Override
    protected ArtifactStore newStore(Path root) {
        try {
            server = new FakeS3Server("conformance-bucket");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        // Small pages, so every listing in the contract exercises the pagination path.
        server.maxKeys(3);
        client = S3Client.builder()
                .httpClientBuilder(ApacheHttpClient.builder())
                .endpointOverride(URI.create(server.endpoint()))
                .forcePathStyle(true)
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test-access-key", "test-secret-key")))
                .build();
        return new S3ArtifactStore(client, "conformance-bucket", "data");
    }

    @AfterEach
    void stopServer() {
        if (client != null) client.close();
        if (server != null) server.close();
    }
}
