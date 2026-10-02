package com.analyzer.chat;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.RequestScopedArtifactStore;
import com.analyzer.storage.sso.CapturedCredentials;
import com.analyzer.storage.sso.SsoCredentialsResolver;
import com.plsqlanalyzer.web.config.ConfigDirService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

/**
 * Task #32 — chat persistence must route through the user's context.
 *
 * <p>Every chat write is wrapped in the CAPTURE → INSTALL idiom
 * ({@code captureScope()} + {@link com.analyzer.storage.sso.CapturedSsoContext}) so it runs under
 * the caller's routing identity and can never degrade to the shared default store. These tests
 * assert the wrapper is actually applied at the write sites, not merely present.
 */
@DisplayName("Chat writes run under the caller's captured routing scope")
class ChatControllerContextTest {

    private static final AwsBasicCredentials SUBMITTER =
            AwsBasicCredentials.create("AKIAEXAMPLEKEY", "secret");

    /** A routing store whose captured scope flips {@code installed} on for the write's duration. */
    private static RequestScopedArtifactStore routingStore(AtomicBoolean installed) {
        RequestScopedArtifactStore store = mock(RequestScopedArtifactStore.class);
        RequestScopedArtifactStore.Scope scope = () -> {
            installed.set(true);
            return () -> installed.set(false);
        };
        when(store.captureScope()).thenReturn(scope);
        return store;
    }

    @Test
    @DisplayName("writeFile: captures scope, installs it, and writes under it")
    void writeFileRunsUnderCapturedScope() throws Exception {
        AtomicBoolean installed = new AtomicBoolean(false);
        RequestScopedArtifactStore store = routingStore(installed);
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        when(store.write(any(ArtifactKey.class), anyLong())).thenAnswer(inv -> {
            assertTrue(installed.get(),
                    "the chat file write must run while the caller's routing scope is installed");
            return (OutputStream) sink;
        });

        SsoCredentialsResolver resolver = mock(SsoCredentialsResolver.class);
        when(resolver.captureCurrent()).thenReturn(new CapturedCredentials("sess-1", SUBMITTER));

        ChatController controller = new ChatController(
                mock(ChatService.class), store, mock(ConfigDirService.class), resolver);

        ResponseEntity<?> response = controller.writeFile(Map.of(
                "path", "claude-chatbot/scripts/x.py",
                "content", "print('hello')\n"));

        assertEquals(200, response.getStatusCode().value());
        assertTrue(sink.size() > 0, "the content must have been written");
        verify(store, times(1)).captureScope();
        verify(store, times(1)).write(any(ArtifactKey.class), anyLong());
        assertFalse(installed.get(), "the scope must be uninstalled after the write");
    }

    @Test
    @DisplayName("createSession: the persistence call runs under the captured scope")
    void createSessionRunsUnderCapturedScope() {
        AtomicBoolean installed = new AtomicBoolean(false);
        RequestScopedArtifactStore store = routingStore(installed);
        ChatService chatService = mock(ChatService.class);
        ChatSession session = new ChatSession("s", "GLOBAL", "classic", Map.of());
        when(chatService.createSession(any(), any(), any(), any())).thenAnswer(inv -> {
            assertTrue(installed.get(),
                    "chatService.createSession must run while the caller's routing scope is installed");
            return session;
        });

        SsoCredentialsResolver resolver = mock(SsoCredentialsResolver.class);
        when(resolver.captureCurrent()).thenReturn(new CapturedCredentials("sess-1", SUBMITTER));

        ChatController controller = new ChatController(
                chatService, store, mock(ConfigDirService.class), resolver);

        ResponseEntity<?> response = controller.createSession(Map.of("name", "my chat"));

        assertEquals(200, response.getStatusCode().value());
        verify(store, times(1)).captureScope();
        verify(chatService, times(1)).createSession(any(), any(), any(), any());
    }

    @Test
    @DisplayName("no request-bound session: captureScope is NONE and the legacy path is untouched")
    void noSessionUsesNoneScope() {
        // A plain (non-routing) store: captureScope cannot be called, and the write must still work.
        com.analyzer.storage.InMemoryArtifactStore store =
                new com.analyzer.storage.InMemoryArtifactStore();
        SsoCredentialsResolver resolver = mock(SsoCredentialsResolver.class);
        when(resolver.captureCurrent()).thenReturn(null);

        ChatController controller = new ChatController(
                mock(ChatService.class), store, mock(ConfigDirService.class), resolver);

        ResponseEntity<?> response = controller.writeFile(Map.of(
                "path", "claude-chatbot/scripts/y.py",
                "content", "print('legacy')\n"));

        assertEquals(200, response.getStatusCode().value());
        assertEquals(1, store.objectCount(), "the legacy (no-scope) write must still land");
    }
}
