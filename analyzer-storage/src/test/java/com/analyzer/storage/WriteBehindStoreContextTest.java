package com.analyzer.storage;

import com.analyzer.storage.sso.AwsSsoLoginService;
import com.analyzer.storage.sso.CapturedCredentials;
import com.analyzer.storage.sso.CurrentHttpSession;
import com.analyzer.storage.sso.SsoCredentialsResolver;
import com.analyzer.storage.sso.SsoJobContext;
import com.analyzer.storage.sso.SsoSessionRegistry;
import com.analyzer.storage.sso.SsoSessionState;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The write-behind upload pool must sign each buffered entry with the identity its <em>writer</em>
 * held, not with whatever the pool thread happens to resolve.
 *
 * <p>This is the live production failure: a job wrote artifacts through the routing store on the
 * queue thread (where {@code SsoJobContext} made the submitter's credentials resolvable), the
 * write-behind buffered them, and the flush on an {@code s3-write-behind} pool thread signed with
 * the default AWS chain — which on a pod with no ambient credentials fails every upload. The store
 * below records the credentials its {@code write()} is invoked under, exactly as the real S3 client
 * resolves them; the tests assert the flush thread sees the writer's identity.
 */
class WriteBehindStoreContextTest {

    private static final AwsCredentials ALICE = AwsBasicCredentials.create("AKIAALICE", "alice-secret");
    private static final AwsCredentials BOB = AwsBasicCredentials.create("AKIABOB", "bob-secret");

    /** A resolver with no request-bound session: only an installed job context can answer. */
    private static SsoCredentialsResolver resolverWithNoSession() {
        CurrentHttpSession session = mock(CurrentHttpSession.class);
        return new SsoCredentialsResolver(mock(AwsSsoLoginService.class),
                mock(SsoSessionRegistry.class), session);
    }

    /**
     * Stands in for the S3 store's client: records the credentials in force when a write reaches it,
     * which is precisely the triple the S3 client would sign the upload with.
     */
    private static final class RecordingStore implements ArtifactStore {
        private final SsoCredentialsResolver resolver;
        final Map<ArtifactKey, AwsCredentials> seen = new ConcurrentHashMap<>();

        RecordingStore(SsoCredentialsResolver resolver) {
            this.resolver = resolver;
        }

        @Override public OutputStream write(ArtifactKey key) throws IOException {
            return write(key, -1);
        }

        @Override public OutputStream write(ArtifactKey key, long contentLength) {
            seen.put(key, resolver.resolveCredentials());
            return OutputStream.nullOutputStream();
        }

        @Override public InputStream read(ArtifactKey key) { throw new UnsupportedOperationException(); }
        @Override public InputStream read(ArtifactKey key, long o, long l) { throw new UnsupportedOperationException(); }
        @Override public boolean exists(ArtifactKey key) { throw new UnsupportedOperationException(); }
        @Override public long size(ArtifactKey key) { throw new UnsupportedOperationException(); }
        @Override public long lastModified(ArtifactKey key) { throw new UnsupportedOperationException(); }
        @Override public boolean delete(ArtifactKey key) { throw new UnsupportedOperationException(); }
        @Override public int deletePrefix(ArtifactKey prefix) { throw new UnsupportedOperationException(); }
        @Override public Stream<ArtifactEntry> list(ArtifactKey prefix, boolean recursive) { throw new UnsupportedOperationException(); }
        @Override public void copy(ArtifactKey from, ArtifactKey to) { throw new UnsupportedOperationException(); }
        @Override public void move(ArtifactKey from, ArtifactKey to) { throw new UnsupportedOperationException(); }
        @Override public LocalHandle materialize(ArtifactKey key) { throw new UnsupportedOperationException(); }
    }

    private static void writeBuffered(WriteBehindStore store, ArtifactKey key) throws IOException {
        try (OutputStream out = store.write(key)) {
            out.write("{}".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test
    void flushUploadsUnderTheJobSubmittersCredentials() throws IOException {
        SsoCredentialsResolver resolver = resolverWithNoSession();
        RecordingStore delegate = new RecordingStore(resolver);
        WriteBehindStore store = new WriteBehindStore(delegate, 2, resolver);
        ArtifactKey key = ArtifactKey.of("jar", "web-1.0.0-SNAPSHOT", "node_1.json");

        // The write happens on a thread that has the job context installed — the pool thread does not.
        try (SsoJobContext ignored = SsoJobContext.install(null, new CapturedCredentials("sess-1", ALICE))) {
            writeBuffered(store, key);
        }

        store.awaitPending();
        assertEquals(ALICE, delegate.seen.get(key),
                "the flush thread must sign with the writer's captured credentials, not the default chain");
    }

    @Test
    void aRequestThreadCaptureUsesTheSessionsCredentials() throws IOException {
        AwsCredentials alice = AwsBasicCredentials.create("AKIAALICE", "alice-secret");
        SsoSessionState state = mock(SsoSessionState.class);
        when(state.loggedIn()).thenReturn(true);
        when(state.iamCredentials()).thenReturn(alice);
        CurrentHttpSession session = mock(CurrentHttpSession.class);
        when(session.currentSessionId()).thenReturn("sess-1");
        SsoSessionRegistry registry = mock(SsoSessionRegistry.class);
        when(registry.get("sess-1")).thenReturn(state);
        SsoCredentialsResolver resolver =
                new SsoCredentialsResolver(mock(AwsSsoLoginService.class), registry, session);

        RecordingStore delegate = new RecordingStore(resolver);
        WriteBehindStore store = new WriteBehindStore(delegate, 2, resolver);
        ArtifactKey key = ArtifactKey.of("parser", "sources", "X.sql");

        // No job context: the capture must fall back to the request thread's session credentials.
        writeBuffered(store, key);

        store.awaitPending();
        assertEquals(alice, delegate.seen.get(key));
    }

    @Test
    void eachEntryCarriesItsOwnWritersCredentials() throws Exception {
        SsoCredentialsResolver resolver = resolverWithNoSession();
        RecordingStore delegate = new RecordingStore(resolver);
        WriteBehindStore store = new WriteBehindStore(delegate, 4, resolver);
        ArtifactKey aliceKey = ArtifactKey.of("jar", "alice.json");
        ArtifactKey bobKey = ArtifactKey.of("jar", "bob.json");

        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread alice = new Thread(() -> {
            try (SsoJobContext ignored = SsoJobContext.install(null, new CapturedCredentials("sess-a", ALICE))) {
                writeBuffered(store, aliceKey);
            } catch (Throwable t) {
                failure.compareAndSet(null, t);
            }
        });
        Thread bob = new Thread(() -> {
            try (SsoJobContext ignored = SsoJobContext.install(null, new CapturedCredentials("sess-b", BOB))) {
                writeBuffered(store, bobKey);
            } catch (Throwable t) {
                failure.compareAndSet(null, t);
            }
        });
        alice.start();
        bob.start();
        alice.join();
        bob.join();
        if (failure.get() != null) throw new AssertionError(failure.get());

        store.awaitPending();
        assertEquals(ALICE, delegate.seen.get(aliceKey), "entries from different users must not cross identities");
        assertEquals(BOB, delegate.seen.get(bobKey));
    }

    @Test
    void withoutAnyContextTheFlushStillUploads() throws IOException {
        // No job context and no resolver: capture is a no-op, and the flush must still work exactly
        // as before (the legacy CLI / no-SSO path).
        InMemoryArtifactStore delegate = new InMemoryArtifactStore();
        WriteBehindStore store = new WriteBehindStore(delegate, 2, null);
        ArtifactKey key = ArtifactKey.of("jar", "plain.json");

        writeBuffered(store, key);
        store.awaitPending();

        assertTrue(delegate.exists(key));
    }
}
