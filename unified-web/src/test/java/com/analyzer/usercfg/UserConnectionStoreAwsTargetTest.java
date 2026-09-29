package com.analyzer.usercfg;

import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.InMemoryArtifactStore;
import com.analyzer.storage.sso.CurrentHttpSession;
import com.analyzer.storage.sso.SsoAuditEvent;
import com.analyzer.storage.sso.SsoIdentity;
import com.analyzer.storage.sso.SsoSessionRegistry;
import com.analyzer.storage.sso.SsoStoreSwapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The per-identity AWS target that makes auto-reconnect survive a redeploy: it is written on a
 * successful CONNECT (via the audit event) and read by identity at login. This covers the round trip,
 * the identity keying, and that a non-CONNECT event is ignored — all against an in-memory store
 * standing in for the deployment's default bucket.
 */
class UserConnectionStoreAwsTargetTest {

    private final ArtifactStore shared = new InMemoryArtifactStore();
    private final SsoStoreSwapper storeSwapper = mock(SsoStoreSwapper.class);

    private final UserConnectionStore store = new UserConnectionStore(
            new InMemoryArtifactStore(), mock(SsoSessionRegistry.class),
            mock(CurrentHttpSession.class), storeSwapper, "");

    UserConnectionStoreAwsTargetTest() {
        when(storeSwapper.sharedStoreFor(anyString(), anyString())).thenReturn(shared);
    }

    @Test
    @DisplayName("a saved target is found again by identity, not by session")
    void roundTripsByIdentity() {
        store.writeAwsTarget("ajay@x.com", "session-1", "my-bucket", "ap-south-1");

        UserConnectionStore.AwsTarget found = store.readAwsTarget("ajay@x.com", "session-1");
        assertEquals("my-bucket", found.bucket());
        assertEquals("ap-south-1", found.region());

        // A different identity (or no save) sees nothing: the key is the identity.
        assertNull(store.readAwsTarget("someone-else@x.com", "session-1"));
    }

    @Test
    @DisplayName("a CONNECT audit event persists the verified target under the connecting identity")
    void connectEventPersistsTarget() {
        SsoIdentity identity = new SsoIdentity("Ajay", "Ajay@X.com", "123", null, "role", null);
        store.onSsoAuditEvent(new SsoAuditEvent(SsoAuditEvent.CONNECT, "session-9",
                identity, "bucket-9", "us-east-1", null, 0L));

        // identityKey lower-cases the email.
        UserConnectionStore.AwsTarget found = store.readAwsTarget("ajay@x.com", "session-9");
        assertEquals("bucket-9", found.bucket());
        assertEquals("us-east-1", found.region());
    }

    @Test
    @DisplayName("a non-CONNECT event stores nothing")
    void otherEventsIgnored() {
        SsoIdentity identity = new SsoIdentity("Ajay", "Ajay@X.com", "123", null, "role", null);
        store.onSsoAuditEvent(new SsoAuditEvent(SsoAuditEvent.LOGIN, "session-9",
                identity, null, null, null, 0L));

        assertNull(store.readAwsTarget("ajay@x.com", "session-9"));
    }

    @Test
    @DisplayName("reads degrade to null when no shared store is available (local/Mongo mode)")
    void noSharedStoreDegradesSilently() {
        when(storeSwapper.sharedStoreFor(anyString(), anyString())).thenReturn(null);
        store.writeAwsTarget("ajay@x.com", "session-1", "b", "r");
        assertNull(store.readAwsTarget("ajay@x.com", "session-1"));
    }
}
