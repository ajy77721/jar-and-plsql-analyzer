package com.analyzer.auth;

import com.analyzer.storage.sso.AwsIamIdentityResolver;
import com.analyzer.storage.sso.AwsSsoLoginService;
import com.analyzer.storage.sso.SsoIdentity;
import com.analyzer.storage.sso.SsoSessionRegistry;
import com.analyzer.storage.sso.SsoSessionState;
import com.analyzer.storage.sso.SsoStoreSwapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Task #39 — the iam-connect response carries an expires-in note.
 *
 * <p>Temporary STS credentials (a {@code sessionToken} was supplied) report the derived ~1h
 * {@code expiresAt}/{@code expiresInSeconds} window; long-lived IAM user keys report both as
 * {@code null}. {@code sts:GetCallerIdentity} returns no real expiry, so this is the documented
 * derived window.
 */
@DisplayName("iam-connect: expiry note for temporary vs long-lived credentials")
class AwsSsoControllerIamExpiryTest {

    private AwsSsoController controller(SsoStoreSwapper swapper) {
        SsoSessionRegistry registry = mock(SsoSessionRegistry.class);
        when(registry.getOrCreate(anyString())).thenReturn(mock(SsoSessionState.class));
        AwsIamIdentityResolver iam = mock(AwsIamIdentityResolver.class);
        when(iam.resolve(any())).thenReturn(
                new SsoIdentity("Ada", null, "123456789012", null, "AdminRole", null));
        return new AwsSsoController(mock(AwsSsoLoginService.class), swapper, registry, iam,
                mock(LoginWarmUpTrigger.class));
    }

    private Map<String, String> body(boolean withSessionToken) {
        Map<String, String> b = new LinkedHashMap<>();
        b.put("accessKeyId", "AKIAEXAMPLEKEY");
        b.put("secretKey", "secret");
        if (withSessionToken) b.put("sessionToken", "FwoGZXIvYXdz...");
        b.put("bucket", "my-bucket");
        b.put("region", "us-east-1");
        b.put("dryRun", "true");
        return b;
    }

    @Test
    @DisplayName("a sessionToken → expiresAt / expiresInSeconds (~1h) are present")
    void temporaryCredentialsReportExpiry() {
        SsoStoreSwapper swapper = mock(SsoStoreSwapper.class);
        when(swapper.testCapabilities(anyString(), any(), any()))
                .thenReturn(new SsoStoreSwapper.Capabilities(true, true, true, true, null));

        Map<String, Object> out = controller(swapper)
                .iamConnect(body(true), new MockHttpServletRequest());

        assertEquals(Boolean.TRUE, out.get("ok"));
        assertEquals(3600L, out.get("expiresInSeconds"));
        Object expiresAt = out.get("expiresAt");
        assertNotNull(expiresAt, "temporary credentials must carry an absolute expiry");
        assertTrue((Long) expiresAt > System.currentTimeMillis(),
                "the derived expiry must be in the future");
    }

    @Test
    @DisplayName("no sessionToken (long-lived IAM keys) → expiresAt / expiresInSeconds are null")
    void longLivedKeysHaveNoExpiry() {
        SsoStoreSwapper swapper = mock(SsoStoreSwapper.class);
        when(swapper.testCapabilities(anyString(), any(), any()))
                .thenReturn(new SsoStoreSwapper.Capabilities(true, true, true, true, null));

        Map<String, Object> out = controller(swapper)
                .iamConnect(body(false), new MockHttpServletRequest());

        assertEquals(Boolean.TRUE, out.get("ok"));
        assertTrue(out.containsKey("expiresAt"));
        assertTrue(out.containsKey("expiresInSeconds"));
        assertNull(out.get("expiresAt"), "long-lived IAM user keys do not expire");
        assertNull(out.get("expiresInSeconds"));
    }
}
