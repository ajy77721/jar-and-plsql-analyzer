package com.analyzer.auth;

import com.analyzer.storage.sso.CapturedCredentials;
import com.analyzer.storage.sso.CapturedStores;
import com.analyzer.storage.sso.SsoCredentialsResolver;
import com.analyzer.storage.sso.SsoStoreSwapper;
import com.plsqlanalyzer.web.parser.service.SidecarWarmUpService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

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
 * <h2>Why capture here, on the request thread</h2>
 *
 * <p>{@link SsoCredentialsResolver#captureCurrent()} and {@link SsoStoreSwapper#captureCurrent()}
 * discover the caller from request-bound state ({@code CurrentHttpSession}); a background sweep has
 * no request, so the snapshot must be taken while the login request is still on the stack — which is
 * exactly where {@link AwsSsoController#poll} calls this, after the login is marked complete. The
 * snapshot is installed by {@link SidecarWarmUpService} around the sweep via {@code SsoJobContext},
 * the same mechanism a queue job uses.
 *
 * <h2>Never in the way, never noisy</h2>
 *
 * <p>The trigger returns as soon as the sweep is queued; the login response is never blocked. Any
 * failure to capture or to start is swallowed to a debug line — a warm-up must never be able to fail
 * a login. No credential, token or full session id is ever logged (the sweep logs a masked suffix).
 */
@Component
public class LoginWarmUpTrigger {

    private static final Logger log = LoggerFactory.getLogger(LoginWarmUpTrigger.class);

    private final SsoStoreSwapper storeSwapper;
    private final SsoCredentialsResolver credentialsResolver;
    private final SidecarWarmUpService warmUp;

    public LoginWarmUpTrigger(SsoStoreSwapper storeSwapper,
                             SsoCredentialsResolver credentialsResolver,
                             SidecarWarmUpService warmUp) {
        this.storeSwapper = storeSwapper;
        this.credentialsResolver = credentialsResolver;
        this.warmUp = warmUp;
    }

    /**
     * Snapshots the just-logged-in session and starts a warm sweep under it. Best-effort: never
     * throws, never blocks.
     *
     * @param sessionId the session whose login completed; used only for the (masked) log line
     */
    public void onLoginComplete(String sessionId) {
        try {
            // Both captures read the request-bound session, so this must stay on the login thread.
            CapturedStores stores = storeSwapper.captureCurrent();
            CapturedCredentials credentials = credentialsResolver.captureCurrent();
            warmUp.triggerLoginSweep(stores, credentials, sessionId);
        } catch (RuntimeException e) {
            // Class name only: a capture can never carry credential material into a message.
            log.debug("[aws-sso] login warm-up could not be triggered ({})",
                    e.getClass().getSimpleName());
        }
    }
}
