package com.analyzer.queue;

import com.analyzer.storage.sso.NotAuthenticatedException;
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
 */
@RestControllerAdvice
public class ApiExceptionAdvice {

    @ExceptionHandler(NotAuthenticatedException.class)
    public ResponseEntity<Map<String, Object>> onNotAuthenticated(NotAuthenticatedException e) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("ok", false);
        body.put("error", NotAuthenticatedException.MESSAGE);
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(body);
    }
}
