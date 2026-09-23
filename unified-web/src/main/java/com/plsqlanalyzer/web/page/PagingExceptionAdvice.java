package com.plsqlanalyzer.web.page;

import com.analyzer.storage.page.Cursor;
import com.analyzer.storage.page.InvalidPageRequestException;
import com.analyzer.storage.page.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * MID-024 — a refused paging request answers 400 with the rule it broke, on every endpoint.
 *
 * <h2>What this replaces</h2>
 *
 * <p>Without it these exceptions reached Spring's default handler and came back as <b>500</b>. Two
 * costs: the caller is told the server is broken when the request was, and a genuine server fault
 * becomes indistinguishable from a mistyped limit in any dashboard counting 5xx.
 *
 * <p>The alternative — clamping an over-cap limit and returning fewer rows — is worse than either.
 * A clamped page is indistinguishable from the end of the data, so the caller stops early and
 * believes it has everything. A 400 naming the cap is the only answer that cannot be misread.
 *
 * <h2>Scope</h2>
 *
 * <p>Deliberately narrow. It maps two specific types, not {@link IllegalArgumentException} at large:
 * that one is thrown across the JVM for real internal faults, and translating those to 400 would
 * report server bugs as client errors and hide them from every alert watching 5xx.
 */
@RestControllerAdvice
public class PagingExceptionAdvice {

    /** An over-cap limit, or a sort the endpoint does not declare. */
    @ExceptionHandler(InvalidPageRequestException.class)
    public ResponseEntity<Map<String, Object>> onInvalidRequest(InvalidPageRequestException e) {
        return ResponseEntity.badRequest().body(body("INVALID_PAGE_REQUEST", e.getMessage()));
    }

    /**
     * A cursor that cannot be trusted: wrong sort, wrong result set, or an envelope this build does
     * not understand. Answered with an error rather than a page, because the alternative is a page
     * that looks continuous with the previous one and is not.
     */
    @ExceptionHandler(Cursor.InvalidCursorException.class)
    public ResponseEntity<Map<String, Object>> onInvalidCursor(Cursor.InvalidCursorException e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(body("INVALID_CURSOR", e.getMessage()));
    }

    private static Map<String, Object> body(String reason, String message) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("reason", reason);
        out.put("message", message);
        // Echoed so a client can render the rule without parsing prose out of the message.
        out.put("maxLimit", PageRequest.MAX_LIMIT);
        return out;
    }
}
