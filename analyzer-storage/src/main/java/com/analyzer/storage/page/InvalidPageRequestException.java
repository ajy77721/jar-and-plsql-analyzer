package com.analyzer.storage.page;

/**
 * A paging request the server refuses to answer — an over-cap limit, or an unknown sort.
 *
 * <h2>Why a distinct type</h2>
 *
 * <p>These are the caller's mistakes and deserve a 400 naming the rule they broke. A bare
 * {@link IllegalArgumentException} cannot be mapped to one safely, because the same type is thrown
 * all over the JVM for genuine internal faults, and translating those into 400s would report server
 * bugs as client errors — hiding them from every alert that watches 5xx.
 *
 * <p>It <b>extends</b> {@code IllegalArgumentException} rather than replacing it so that the
 * existing {@code catch (IllegalArgumentException)} in the responder keeps working unchanged. The
 * type is narrowed for the code that cares and identical for the code that does not.
 */
public class InvalidPageRequestException extends IllegalArgumentException {

    public InvalidPageRequestException(String message) {
        super(message);
    }
}
