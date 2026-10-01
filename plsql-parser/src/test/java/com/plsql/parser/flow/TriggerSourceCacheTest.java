package com.plsql.parser.flow;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.InMemoryArtifactStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The trigger-source cache chain: the TriggerDiscoverer's batched pull writes trigger bodies
 * through the SAME persistent cache as object sources, so a re-crawl of an unchanged trigger reads
 * the cached body instead of hitting ALL_SOURCE again (the old per-trigger N+1 that bypassed it).
 */
@DisplayName("Trigger source cache")
class TriggerSourceCacheTest {

    @Test
    @DisplayName("stored trigger body round-trips; a different owner is a miss")
    void roundTrip() {
        InMemoryArtifactStore store = new InMemoryArtifactStore();
        SourceDownloader d = new SourceDownloader(null);
        d.setSourceCache(store, ArtifactKey.of("plsql-parse", ".source-cache"), "ORACLE|url");

        assertNull(d.getCachedTriggerSource("HR", "TRG_AUDIT"), "miss before store");
        d.storeTriggerSource("HR", "TRG_AUDIT", "CREATE OR REPLACE TRIGGER TRG_AUDIT ...");
        assertEquals("CREATE OR REPLACE TRIGGER TRG_AUDIT ...",
                d.getCachedTriggerSource("HR", "TRG_AUDIT"));
        assertNull(d.getCachedTriggerSource("FIN", "TRG_AUDIT"),
                "a same-named trigger in another owner must not collide");
    }

    @Test
    @DisplayName("cache disabled (no store) is a safe no-op")
    void disabledIsNoOp() {
        SourceDownloader d = new SourceDownloader(null);
        assertFalse(d.hasSourceCache());
        assertNull(d.getCachedTriggerSource("HR", "TRG"));
        assertDoesNotThrow(() -> d.storeTriggerSource("HR", "TRG", "body"));
    }
}
