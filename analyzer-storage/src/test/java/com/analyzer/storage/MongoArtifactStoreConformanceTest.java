package com.analyzer.storage;

import com.analyzer.storage.mongo.MongoArtifactStore;
import com.analyzer.storage.mongo.MongoTestSupport;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.junit.jupiter.api.AfterEach;

import java.nio.file.Path;

/**
 * The {@link ArtifactStoreConformanceTest} contract, run against {@link MongoArtifactStore}.
 *
 * <p><b>This class is the C1 proof.</b> {@code ArtifactStore}'s 14 methods are frozen across 74
 * dependent classes, and the interface's own docs say "callers must never assume which is active".
 * Every behaviour the local backend promises is re-run here against a real MongoDB, with the suite
 * <b>unmodified</b> — so a semantic divergence between the three backends fails the build instead
 * of surfacing as a production bug in {@code mongodb} mode only. Not one assertion was relaxed to
 * make this pass; where Mongo genuinely cannot match local, it is a documented performance
 * difference rather than a behavioural one, and those are listed on {@link MongoArtifactStore}.
 *
 * <p>{@code root} from the base class is ignored: MongoDB has no filesystem root. A fresh
 * collection per test gives the same isolation {@code @TempDir} gives locally.
 *
 * <h2>The threshold is 1 KiB on purpose</h2>
 *
 * <p>Production default is 1 MiB, at which every artifact the suite writes would be inline and the
 * GridFS half of the backend — rollover, chunk-aware seek, payload collection on overwrite, the
 * streaming {@code copy} fallback — would never execute. 1 KiB puts the suite's small artifacts on
 * the inline path and its 4 MiB one on the GridFS path, so <b>both</b> storage paths are held to the
 * whole contract by a single run. This is the same trick {@code S3ArtifactStoreConformanceTest}
 * plays with {@code server.maxKeys(3)} to force pagination.
 */
class MongoArtifactStoreConformanceTest extends ArtifactStoreConformanceTest {

    /** Small enough that the suite's 4 MiB artifact rolls over; large enough that most do not. */
    private static final int THRESHOLD = 1024;

    private MongoClient client;
    private MongoDatabase db;
    private String collection;

    /**
     * Created lazily rather than in a {@code @BeforeEach}: the base class's own {@code @BeforeEach}
     * runs first and is what calls this, so a subclass setup method would be too late.
     */
    @Override
    protected ArtifactStore newStore(Path root) {
        client = MongoTestSupport.clientOrSkip();
        db = client.getDatabase(MongoTestSupport.DATABASE);
        collection = MongoTestSupport.freshCollection("conformance");
        MongoArtifactStore store =
                new MongoArtifactStore(db, "data", THRESHOLD, collection, 100_000L);
        store.ensureIndexes();
        return store;
    }

    @AfterEach
    void dropAndDisconnect() {
        if (db != null && collection != null) MongoTestSupport.drop(db, collection);
        if (client != null) client.close();
    }
}
