package com.analyzer.storage.sso;

/**
 * Thrown by {@link SessionRoutingArtifactStore} when a request that belongs to a browser session
 * reaches S3 storage without that user having authenticated via AWS SSO.
 *
 * <h2>Why a dedicated type</h2>
 *
 * <p>In a per-user deployment an S3 read or write arriving on a request whose session is known but
 * unauthenticated is an authentication failure, not a storage failure — the fix is "log in", not
 * "check the bucket". A distinct unchecked type lets the web layer map exactly this case to HTTP 401
 * without having to pattern-match on message text, while every other storage failure (an S3 outage,
 * a missing object) keeps its existing handling.
 *
 * <h2>Scope of the throw</h2>
 *
 * <p>It is raised only when a request <em>does</em> carry a session and that session has no
 * completed SSO login. A request with no session at all — a background job, a CLI, the pre-SSO local
 * workflow — does <b>not</b> raise this: it routes to the default store and the default AWS
 * credentials, which is existing behaviour that must keep working. See
 * {@link SessionRoutingArtifactStore} for the exact rule.
 *
 * <p>The fixed message is part of the contract: the web layer's 401 body uses it verbatim so the UI
 * can prompt the user to authenticate.
 */
public class NotAuthenticatedException extends RuntimeException {

    /** The user-facing message; the web layer surfaces it on its 401 response. */
    public static final String MESSAGE = "Authenticate via AWS SSO first";

    public NotAuthenticatedException() {
        super(MESSAGE);
    }

    public NotAuthenticatedException(String message) {
        super(message);
    }
}
