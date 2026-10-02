package com.analyzer.queue;

import com.analyzer.auth.ConnectionGuard;
import com.analyzer.storage.sso.CapturedCredentials;
import com.analyzer.storage.sso.CapturedStores;
import com.analyzer.storage.sso.SsoCredentialsResolver;
import com.analyzer.storage.sso.SsoStoreSwapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Task #31 — "Verify P4 (credentials for ALL queue-job types)".
 *
 * <p>The acceptance criterion is a test proving <b>every</b> {@link QueueJob.Type}'s captured
 * context carries credentials (or that the type needs none — documented per type). The fix made
 * {@code AnalysisQueueService.needsCredentials(type)} answer {@code true} for every constant, so
 * this test iterates the whole enum and asserts the captured snapshot is attached. It also checks
 * the converse: with no SSO session (resolver returns null) no credentials are fabricated, so the
 * documented legacy/default path is unchanged.
 *
 * <p>Why "every type" matters: a follow-up job is re-submitted from the queue worker with its
 * parent's snapshots verbatim, bypassing {@code needsCredentials}. Under the old split a
 * {@code JAR_UPLOAD}/{@code WAR_UPLOAD} parent captured none, so its {@code CLAUDE_ENRICH} child ran
 * without the submitter's role. Universal capture closes that.
 */
@DisplayName("Queue credentials: every QueueJob.Type captures the submitter's context")
class AnalysisQueueServiceCredentialsTest {

    private static final AwsBasicCredentials SUBMITTER =
            AwsBasicCredentials.create("AKIAEXAMPLEKEY", "secret");

    private AnalysisQueueService service(SsoCredentialsResolver resolver) {
        return new AnalysisQueueService(
                mock(JarAnalysisExecutor.class),
                mock(JarTargetedAnalysisExecutor.class),
                mock(WarAnalysisExecutor.class),
                mock(PlsqlAnalysisExecutor.class),
                mock(FastAnalysisExecutor.class),
                mock(ParserAnalysisExecutor.class),
                mock(ParserValidationExecutor.class),
                mock(ClaudeJobExecutor.class),
                mock(JarExportBundleExecutor.class),
                mock(PurposeSummaryExecutor.class),
                mock(SsoStoreSwapper.class),
                resolver,
                mock(ConnectionGuard.class));
    }

    @Test
    @DisplayName("every QueueJob.Type captures credentials when the submitter has an SSO session")
    void everyTypeCapturesCredentials() {
        SsoCredentialsResolver resolver = mock(SsoCredentialsResolver.class);
        CapturedCredentials captured = new CapturedCredentials("sess-alice", SUBMITTER);
        when(resolver.captureCurrent()).thenReturn(captured);
        AnalysisQueueService service = service(resolver);

        QueueJob.Type[] types = QueueJob.Type.values();
        assertTrue(types.length > 0, "sanity: the enum has constants");
        for (QueueJob.Type type : types) {
            QueueJob job = service.submit(type, "capture-" + type, Map.of());
            assertNotNull(job.capturedCredentials,
                    type + " must carry the submitter's captured credentials (task #31)");
            assertSame(captured, job.capturedCredentials,
                    type + " must carry the exact captured snapshot, not a copy");
            assertEquals("sess-alice", job.capturedCredentials.sessionId());
        }
    }

    @Test
    @DisplayName("no session => no fabricated credentials (legacy/default path unchanged)")
    void noSessionCapturesNothing() {
        SsoCredentialsResolver resolver = mock(SsoCredentialsResolver.class);
        when(resolver.captureCurrent()).thenReturn(null);
        AnalysisQueueService service = service(resolver);

        for (QueueJob.Type type : QueueJob.Type.values()) {
            QueueJob job = service.submit(type, "no-session-" + type, Map.of());
            assertNull(job.capturedCredentials,
                    type + ": captureCurrent() returned null, so the job must stay on the legacy path");
        }
    }

    @Test
    @DisplayName("captured stores are attached independently of credentials")
    void storesStillCaptured() {
        SsoStoreSwapper swapper = mock(SsoStoreSwapper.class);
        CapturedStores stores = new CapturedStores("sess-bob", null, null, "bucket", "us-east-1", null);
        when(swapper.captureCurrent()).thenReturn(stores);
        SsoCredentialsResolver resolver = mock(SsoCredentialsResolver.class);
        when(resolver.captureCurrent()).thenReturn(new CapturedCredentials("sess-bob", SUBMITTER));

        AnalysisQueueService service = new AnalysisQueueService(
                mock(JarAnalysisExecutor.class), mock(JarTargetedAnalysisExecutor.class),
                mock(WarAnalysisExecutor.class), mock(PlsqlAnalysisExecutor.class),
                mock(FastAnalysisExecutor.class), mock(ParserAnalysisExecutor.class),
                mock(ParserValidationExecutor.class), mock(ClaudeJobExecutor.class),
                mock(JarExportBundleExecutor.class), mock(PurposeSummaryExecutor.class),
                swapper, resolver, mock(ConnectionGuard.class));

        QueueJob job = service.submit(QueueJob.Type.JAR_UPLOAD, "stores", Map.of());
        assertSame(stores, job.capturedStores);
        assertNotNull(job.capturedCredentials,
                "JAR_UPLOAD now also captures credentials so its CLAUDE_ENRICH follow-up inherits them");
    }
}
