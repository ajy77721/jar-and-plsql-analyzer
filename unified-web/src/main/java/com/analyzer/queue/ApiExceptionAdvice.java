package com.analyzer.queue;

import com.analyzer.storage.sso.NotAuthenticatedException;
import com.jaranalyzer.service.StorageListingException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Maps an unauthenticated SSO session to HTTP 401 on every endpoint.
 *
 * <h2>Defense in depth, behind the interceptor</h2>
 *
 * <p>{@code ApiKeyAuthInterceptor} already refuses a data request whose session has not completed
 * the AWS SSO device flow, so most callers never reach a handler without a login. This advice covers
 * the case where the exception is raised deeper in the stack instead — {@link
 * com.analyzer.storage.sso.SessionRoutingArtifactStore} throws {@link NotAuthenticatedException} when
 * a request that <em>does</em> carry an SSO session reaches storage without a connected, logged-in
 * user. Without this mapping that unchecked exception would surface as a 500.
 *
 * <h2>Fixed body</h2>
 *
 * <p>The message is the exception's own constant, so the UI can key on the exact string
 * {@code "Authenticate via AWS SSO first"} and prompt for login. Kept deliberately narrow: only
 * {@link NotAuthenticatedException} is translated, so a genuine storage fault (an S3 outage, a
 * missing object) keeps its existing 5xx handling.
 *
 * <p>The specific reason travels in a separate {@code reason} field when the throw site supplied
 * one (e.g. the routing store's "logged in but no AWS bucket connected" — a login will NOT fix
 * that, only connecting a bucket will), so the UI can distinguish the two without breaking the
 * keyed-on constant above.
 */
@RestControllerAdvice
public class ApiExceptionAdvice {

    @ExceptionHandler(NotAuthenticatedException.class)
    public ResponseEntity<Map<String, Object>> onNotAuthenticated(NotAuthenticatedException e) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("ok", false);
        body.put("error", NotAuthenticatedException.MESSAGE);
        String reason = e.getMessage();
        if (reason != null && !reason.isBlank() && !NotAuthenticatedException.MESSAGE.equals(reason)) {
            body.put("reason", reason);
        }
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(body);
    }

    /**
     * Maps a listing that could not read the artifact store to HTTP 503 with the same
     * {@code {ok:false,error}} shape the UI already understands, instead of a 200 {@code []} the
     * screen would render as "no analyses". 503 (not 500) because the store being unreachable is a
     * transient backend condition, not a defect in the request.
     */
    @ExceptionHandler(StorageListingException.class)
    public ResponseEntity<Map<String, Object>> onStorageListing(StorageListingException e) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("ok", false);
        body.put("error", e.getMessage());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(body);
    }
}
