package com.analyzer.storage.page;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

/**
 * An opaque resume position: the sort key of the last returned row, plus a stable tiebreaker.
 *
 * <p>Cursor paging rather than offset paging, for reasons specific to this data. The PL/SQL crawl is
 * not deterministic — re-running an analysis renumbers its nodes — so an offset means something
 * different before and after a re-analysis. The append-only logs also grow underneath a reader, and
 * with offsets that silently duplicates or skips rows. A cursor is a row comparison against the same
 * columns the index is built on, so it stays correct in both cases.
 *
 * <p>Deliberately <b>not</b> a database cursor: no server-side state, no timeout, no session
 * affinity. It is valid until the sort discriminator or the envelope version changes.
 *
 * <p>Not signed and not encrypted. The contents are sort keys the caller can already see, so
 * signing would add key management against no threat. The base64 is a discouragement, not security.
 *
 * @param sort       the sort this cursor was minted under. A request with a different sort is
 *                   rejected rather than silently answered from the wrong ordering.
 * @param keys       the sort key tuple of the last row returned
 * @param tiebreaker the row's stable primary key — <b>never an ordinal</b>, because ordinals are
 *                   run-scoped (see the non-determinism note above)
 * @param runId      the analysis run this cursor was minted against, or {@code null} for streams
 *                   that have no run (the append-only logs). When present it is checked, because a
 *                   cursor from a superseded run answered against the current one silently
 *                   duplicates or drops rows at the seam — a re-analysis between page 1 and page 2
 *                   is enough
 * @param version    envelope version, so a cursor minted by an older deploy is rejected cleanly
 *                   instead of being misread
 */
public record Cursor(String sort, List<String> keys, String tiebreaker, String runId, int version) {

    /** Current envelope version. Bump when the meaning of {@link #keys} changes for any sort. */
    public static final int CURRENT_VERSION = 1;

    public Cursor {
        Objects.requireNonNull(sort, "sort");
        Objects.requireNonNull(tiebreaker, "tiebreaker");
        keys = List.copyOf(Objects.requireNonNull(keys, "keys"));
    }

    /** A cursor with no run scope — for streams that genuinely have none. */
    public static Cursor of(String sort, List<String> keys, String tiebreaker) {
        return new Cursor(sort, keys, tiebreaker, null, CURRENT_VERSION);
    }

    /** A cursor bound to the run it was minted against. Preferred wherever a run exists. */
    public static Cursor of(String sort, List<String> keys, String tiebreaker, String runId) {
        return new Cursor(sort, keys, tiebreaker, runId, CURRENT_VERSION);
    }

    /**
     * Rejects a cursor that does not belong to {@code expectedRunId}.
     *
     * <p>Called by the reader once it knows which run it is about to answer from. Both mismatch and
     * absence are failures: a cursor minted before this field existed cannot be shown to belong to
     * the current run, and "cannot be shown to be safe" is not the same as safe.
     *
     * @throws InvalidCursorException naming both runs, so the caller can see what changed underneath
     */
    public void requireRun(String expectedRunId) {
        if (expectedRunId == null) return;
        if (runId == null) {
            throw new InvalidCursorException(
                    "Cursor carries no run id and cannot be shown to belong to run '"
                    + expectedRunId + "'; restart from the first page");
        }
        if (!runId.equals(expectedRunId)) {
            throw new InvalidCursorException(
                    "Cursor was minted against run '" + runId + "' but the current run is '"
                    + expectedRunId + "' — the analysis was re-run while you were paging. "
                    + "Restart from the first page; continuing would mix rows from two runs.");
        }
    }

    /** A cursor for an append-only stream, where the sequence number is the whole key. */
    public static Cursor ofSeq(String sort, long seq) {
        return new Cursor(sort, List.of(Long.toString(seq)), Long.toString(seq), null,
                CURRENT_VERSION);
    }

    /** The sequence number for a {@link #ofSeq} cursor. */
    public long seq() {
        return Long.parseLong(tiebreaker);
    }

    /**
     * Base64url of a compact JSON object. Hand-rolled rather than Jackson so that this class has no
     * dependency beyond the JDK — it is used by the storage library, the repository and the web
     * layer, and a shared type that drags in a mapper is harder to place.
     */
    public String encode() {
        StringBuilder json = new StringBuilder(64);
        json.append("{\"s\":\"").append(escape(sort)).append("\",\"k\":[");
        for (int i = 0; i < keys.size(); i++) {
            if (i > 0) json.append(',');
            json.append('"').append(escape(keys.get(i))).append('"');
        }
        json.append("],\"i\":\"").append(escape(tiebreaker)).append('"');
        // Omitted rather than written as null when there is no run: a log stream has none, and an
        // absent field decodes back to null on any build, including one that predates this field.
        if (runId != null) json.append(",\"r\":\"").append(escape(runId)).append('"');
        json.append(",\"v\":").append(version).append('}');
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(json.toString().getBytes(StandardCharsets.UTF_8));
    }

    /**
     * @throws InvalidCursorException if the cursor is malformed, was minted for a different sort, or
     *         carries an envelope version this build does not understand. Every failure is explicit:
     *         a cursor that cannot be trusted must produce an error, never a wrong page.
     */
    public static Cursor decode(String encoded, String expectedSort) {
        String json;
        try {
            json = new String(Base64.getUrlDecoder().decode(encoded), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new InvalidCursorException("Cursor is not valid base64url");
        }
        String sort = extractString(json, "\"s\":\"");
        String tiebreaker = extractString(json, "\"i\":\"");
        int version = extractInt(json, "\"v\":");
        if (sort == null || tiebreaker == null) {
            throw new InvalidCursorException("Cursor is missing required fields");
        }
        if (version != CURRENT_VERSION) {
            throw new InvalidCursorException(
                    "Cursor was minted by a different version (" + version + " != "
                    + CURRENT_VERSION + "); restart from the first page");
        }
        if (expectedSort != null && !expectedSort.equals(sort)) {
            throw new InvalidCursorException(
                    "Cursor was minted for sort '" + sort + "' but the request asks for '"
                    + expectedSort + "'; a cursor is only meaningful within one ordering");
        }
        return new Cursor(sort, extractKeys(json), tiebreaker, extractString(json, "\"r\":\""),
                version);
    }

    /** Finds {@code "<field>"} then skips the colon and any whitespace before the value. */
    private static int valueStart(String json, String field) {
        int i = json.indexOf("\"" + field + "\"");
        if (i < 0) return -1;
        int p = i + field.length() + 2;
        while (p < json.length() && Character.isWhitespace(json.charAt(p))) p++;
        if (p >= json.length() || json.charAt(p) != ':') return -1;
        p++;
        while (p < json.length() && Character.isWhitespace(json.charAt(p))) p++;
        return p;
    }

    private static String extractString(String json, String marker) {
        // Tolerate whitespace around the colon. The parser previously matched the literal
        // `"s":"` and a cursor written as `"s": "SIDEBAR"` was silently unreadable — and for the
        // version field that surfaced as "minted by a different version", which is the wrong
        // diagnosis for a perfectly well-formed cursor.
        String field = marker.substring(1, marker.indexOf('"', 1));
        int vs = valueStart(json, field);
        if (vs < 0 || vs >= json.length() || json.charAt(vs) != '"') return null;
        int from = vs + 1;
        StringBuilder out = new StringBuilder();
        for (int p = from; p < json.length(); p++) {
            char c = json.charAt(p);
            if (c == '\\' && p + 1 < json.length()) { out.append(json.charAt(++p)); continue; }
            if (c == '"') return out.toString();
            out.append(c);
        }
        return null;
    }

    private static int extractInt(String json, String marker) {
        String field = marker.substring(1, marker.indexOf('"', 1));
        int p = valueStart(json, field);
        if (p < 0) return -1;
        StringBuilder n = new StringBuilder();
        while (p < json.length() && (Character.isDigit(json.charAt(p)) || json.charAt(p) == '-')) {
            n.append(json.charAt(p++));
        }
        try {
            return Integer.parseInt(n.toString());
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private static List<String> extractKeys(String json) {
        int p = valueStart(json, "k");
        if (p < 0 || p >= json.length() || json.charAt(p) != '[') return List.of();
        p++;
        List<String> out = new ArrayList<>();
        StringBuilder cur = null;
        while (p < json.length()) {
            char c = json.charAt(p);
            if (c == ']' && cur == null) break;
            if (c == '"') {
                if (cur == null) {
                    cur = new StringBuilder();
                } else {
                    out.add(cur.toString());
                    cur = null;
                }
            } else if (cur != null) {
                if (c == '\\' && p + 1 < json.length()) { cur.append(json.charAt(++p)); }
                else { cur.append(c); }
            }
            p++;
        }
        return out;
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /** Thrown for a cursor that is malformed, stale, or from another sort. Maps to HTTP 400. */
    public static class InvalidCursorException extends RuntimeException {
        public InvalidCursorException(String message) {
            super(message);
        }
    }
}
