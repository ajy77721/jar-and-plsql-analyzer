package com.analyzer.auth;

import com.analyzer.storage.sso.CapturedCredentials;
import com.analyzer.storage.sso.CapturedStores;
import com.analyzer.storage.sso.CurrentHttpSession;
import com.analyzer.storage.sso.SsoCredentialsResolver;
import com.analyzer.storage.sso.SsoStoreSwapper;
import com.analyzer.usercfg.UserConnectionStore;
import com.plsqlanalyzer.web.parser.service.SidecarWarmUpService;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * On a completed login, hands the first logged-in user's own captured credentials to the sidecar
 * warm-up sweep, so the precompute finally has a principal that can reach the store.
 *
 * <h2>The problem this closes</h2>
 *
 * <p>A pod has no ambient AWS credentials, so the boot/periodic sweep's every S3 call signs with the
 * default chain and fails (one INFO per sweep, no sidecars). The first user to log in <em>does</em>
 * have credentials — the short-lived role credentials minted during the device flow — and the
 * credential resolver has already cached them under that session. This component snapshots those
 * credentials (plus whatever stores the session has connected) on the login thread and starts an
 * async sweep bound to them, so the very analyses the first view-click would have computed are
 * precomputed while nobody is waiting.
 *
 * <h2>Why the capture is re-bound, not taken on the request thread</h2>
 *
 * <p>{@link SsoCredentialsResolver#captureCurrent()} and {@link SsoStoreSwapper#captureCurrent()}
 * discover the caller from request-bound state ({@code CurrentHttpSession}); a background sweep has
 * no request. This hook therefore runs on its own worker and re-binds the login's session id there
 * via {@link CurrentHttpSession#propagate(String)}, so the snapshot is taken without keeping the
 * poll response waiting. That is deliberate: the auto-reconnect probe (a saved-config read plus a
 * three-operation S3 capability probe) used to run on the poll thread and was the bulk of the
 * "approval to usable app" latency. The snapshot is installed by {@link SidecarWarmUpService} around
 * the sweep via {@code SsoJobContext}, the same mechanism a queue job uses.
 *
 * <h2>Never in the way, never noisy</h2>
 *
 * <p>The trigger returns as soon as the work is queued; the login response is never blocked, and
 * the user is admitted as soon as the identity is resolved. Any failure to capture or to start is
 * swallowed to a debug line — a warm-up must never be able to fail a login. No credential, token or
 * full session id is ever logged (the sweep logs a masked suffix).
 */
@Component
public class LoginWarmUpTrigger {

    private static final Logger log = LoggerFactory.getLogger(LoginWarmUpTrigger.class);

    private final SsoStoreSwapper storeSwapper;
    private final SsoCredentialsResolver credentialsResolver;
    private final SidecarWarmUpService warmUp;
    private final UserConnectionStore userConnectionStore;
    private final CurrentHttpSession currentSession;

    /** Runs the reconnect + capture off the login thread. */
    private final Executor executor;

    /** Non-null when this instance owns {@link #executor} and must stop it on shutdown. */
    private final ExecutorService ownedExecutor;

    @Autowired
    public LoginWarmUpTrigger(SsoStoreSwapper storeSwapper,
                             SsoCredentialsResolver credentialsResolver,
                             SidecarWarmUpService warmUp,
                             UserConnectionStore userConnectionStore,
                             CurrentHttpSession currentSession) {
        this(storeSwapper, credentialsResolver, warmUp, userConnectionStore, currentSession,
                Executors.newSingleThreadExecutor(r -> {
                    Thread t = new Thread(r, "aws-login-warmup");
                    t.setDaemon(true);
                    return t;
                }));
    }

    /**
     * Test seam: run the warm-up on the supplied executor (a same-thread {@code Runnable::run} keeps
     * the assertions deterministic). The production constructor owns its single daemon worker.
     */
    LoginWarmUpTrigger(SsoStoreSwapper storeSwapper,
                       SsoCredentialsResolver credentialsResolver,
                       SidecarWarmUpService warmUp,
                       UserConnectionStore userConnectionStore,
                       CurrentHttpSession currentSession,
                       Executor executor) {
        this.storeSwapper = storeSwapper;
        this.credentialsResolver = credentialsResolver;
        this.warmUp = warmUp;
        this.userConnectionStore = userConnectionStore;
        this.currentSession = currentSession;
        this.executor = executor;
        this.ownedExecutor = executor instanceof ExecutorService service ? service : null;
    }

    @PreDestroy
    void shutdown() {
        if (ownedExecutor != null) ownedExecutor.shutdownNow();
    }

    /**
     * On a completed login, queue the reconnect + warm sweep and return immediately. The poll
     * response that called this is never made to wait on the saved-config read or the S3 capability
     * probe; the session is already authenticated by the time this runs.
     *
     * <p>Best-effort: a rejection or a failure is swallowed to a debug line, because a warm-up must
     * never be able to fail a login.
     *
     * @param sessionId the session whose login completed
     */
    public void onLoginComplete(String sessionId) {
        if (sessionId == null || sessionId.isBlank()) return;
        try {
            executor.execute(() -> runWarmUp(sessionId));
        } catch (RuntimeException e) {
            log.debug("[aws-sso] login warm-up could not be queued ({})",
                    e.getClass().getSimpleName());
        }
    }

    /**
     * The off-thread body: reconnect this session to the AWS target it saved before (so a redeploy
     * no longer silently drops the user's connection), then snapshot the session and start the warm
     * sweep. Reconnect runs <em>before</em> the capture on purpose, so the warm sweep is handed the
     * reconnected per-user stores rather than running without a target.
     *
     * <p>The worker has no servlet request, so the login's session id is re-bound around the whole
     * body — that is what lets the captures and the saved-target lookup resolve the caller.
     */
    private void runWarmUp(String sessionId) {
        try (AutoCloseable ignored = currentSession.propagate(sessionId)) {
            autoReconnect(sessionId);
            CapturedStores stores = storeSwapper.captureCurrent();
            CapturedCredentials credentials = credentialsResolver.captureCurrent();
            warmUp.triggerLoginSweep(stores, credentials, sessionId);
        } catch (Exception e) {
            // Class name only: a capture can never carry credential material into a message.
            log.debug("[aws-sso] login warm-up could not be triggered ({})",
                    e.getClass().getSimpleName());
        }
    }

    /**
     * Re-runs the connect path for this session against the target stored under the user's
     * <b>identity</b> (never the session id), so a redeploy — which wipes the in-memory binding but
     * lets SSO re-login — no longer leaves the user silently disconnected. The connect's capability
     * probe still gates the build, and a missing target or a failed probe is skipped silently here
     * (the probe failure is logged by the swapper at WARN). Runs on the warm-up worker, where the
     * session has been re-bound (see {@link #runWarmUp}) so the saved-target lookup and the connect
     * resolve the freshly-minted credentials.
     */
    private void autoReconnect(String sessionId) {
        try {
            UserConnectionStore.AwsTarget target = userConnectionStore.findAwsTargetForCurrentUser();
            if (target == null || target.bucket() == null || target.bucket().isBlank()) {
                log.debug("[auto-reconnect] no saved AWS target for this login; manual connect required");
                return;
            }
            storeSwapper.autoReconnect(sessionId, target.bucket(), target.region());
        } catch (RuntimeException e) {
            // A lookup or reconnect failure must never fail a login; the user can connect manually.
            log.debug("[auto-reconnect] skipped ({})", e.getClass().getSimpleName());
        }
    }
}
