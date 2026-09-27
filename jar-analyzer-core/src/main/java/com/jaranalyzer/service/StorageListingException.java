package com.jaranalyzer.service;

/**
 * Raised when an analysis listing cannot reach the artifact store at all, so the caller can report
 * a failure instead of an empty list.
 *
 * <h2>Why this type exists</h2>
 *
 * <p>The listing path used to catch every exception from the store and return an empty list. That
 * made "the store could not be read" indistinguishable from "there is nothing stored" — an operator
 * looking at a live staging screen saw {@code []} for analyses that were plainly in the bucket, and
 * had to go read the server log to discover the store call had actually thrown. A screen that says
 * "nothing here" when the truth is "we could not ask" is worse than an error: it invites the user to
 * upload work that already exists.
 *
 * <p>{@code PersistenceService.listJarsStrict()} wraps the underlying {@code IOException} or SDK
 * failure in this unchecked type, which {@code ApiExceptionAdvice} maps to a non-2xx body. The
 * lenient {@code listJars()} keeps its old empty-on-error behaviour for internal callers.
 *
 * <p>The message carries the underlying failure's {@code toString()} — an AWS SDK exception's
 * message names the failed provider chain but no credential material.
 */
public class StorageListingException extends RuntimeException {

    public StorageListingException(String message, Throwable cause) {
        super(message, cause);
    }
}
