package com.analyzer.storage.page;

import java.util.Objects;
import java.util.Set;

/**
 * A validated paging request: how many rows, from where, in which order.
 *
 * <p>Built from raw query parameters in one place so that every endpoint enforces the same rules —
 * a per-controller interpretation of {@code limit} is how one endpoint ends up able to return half a
 * million rows.
 *
 * <p><b>Absent parameters mean legacy behaviour.</b> {@link #isLegacy()} is true when the caller
 * asked for no paging at all, and the endpoint then returns a bare array exactly as it does today.
 * That is what allows conversion one endpoint at a time, with old scripts and new screens hitting
 * the same URL on the same deploy.
 */
public final class PageRequest {

    /** Hard ceiling regardless of what a caller asks for. */
    public static final int MAX_LIMIT = 1000;

    private final int limit;
    private final Cursor after;
    private final String sort;
    private final String query;
    private final boolean legacy;

    private PageRequest(int limit, Cursor after, String sort, String query, boolean legacy) {
        this.limit = limit;
        this.after = after;
        this.sort = sort;
        this.query = query;
        this.legacy = legacy;
    }

    /**
     * @param limitParam  raw {@code limit}, may be null
     * @param afterParam  raw {@code after} cursor, may be null
     * @param sortParam   raw {@code sort}, may be null
     * @param queryParam  raw {@code q} search text, may be null. Search must move to the server with
     *                    pagination: once only one page is in memory, client-side search would
     *                    silently search that page alone.
     * @param allowedSorts the endpoint's closed set of sort names — never a free-form column, or the
     *                     sort name becomes an injection surface and an unindexed-scan surface
     * @param defaultSort  used when the caller does not choose
     * @param defaultLimit used when the caller pages but does not say how much
     * @throws IllegalArgumentException on an unknown sort or an out-of-range limit
     * @throws Cursor.InvalidCursorException on a cursor that is malformed, stale, or from another sort
     */
    public static PageRequest of(Integer limitParam, String afterParam, String sortParam,
                                 String queryParam, Set<String> allowedSorts,
                                 String defaultSort, int defaultLimit) {
        Objects.requireNonNull(allowedSorts, "allowedSorts");
        boolean paged = limitParam != null || (afterParam != null && !afterParam.isBlank())
                || (sortParam != null && !sortParam.isBlank());

        String sort = (sortParam == null || sortParam.isBlank()) ? defaultSort : sortParam;
        if (!allowedSorts.contains(sort)) {
            throw new InvalidPageRequestException(
                    "Unknown sort '" + sort + "'. Allowed: " + allowedSorts);
        }

        int limit = limitParam == null ? defaultLimit : limitParam;
        if (paged && (limit < 1 || limit > MAX_LIMIT)) {
            throw new InvalidPageRequestException(
                    "limit must be between 1 and " + MAX_LIMIT + ", got " + limit);
        }

        Cursor after = (afterParam == null || afterParam.isBlank())
                ? null
                : Cursor.decode(afterParam, sort);

        return new PageRequest(limit, after, sort, blankToNull(queryParam), !paged);
    }

    /**
     * For an append-only stream, where {@code sinceSeq} reads better than an opaque cursor.
     *
     * <p>An over-cap limit is <b>rejected</b>, not clamped. Clamping returns fewer rows than were
     * asked for while reporting success, and the caller has no way to tell a clamped page from a
     * complete one — it looks like the stream ended. Silently returning less than was requested is
     * the failure mode this cap exists to make visible, so it fails the same way
     * {@link #of} does rather than inventing a quieter convention for the same rule.
     */
    public static PageRequest sinceSeq(Long sinceSeq, Integer limitParam, String sort,
                                       int defaultLimit) {
        if (limitParam != null && (limitParam < 1 || limitParam > MAX_LIMIT)) {
            throw new InvalidPageRequestException(
                    "limit must be between 1 and " + MAX_LIMIT + ", got " + limitParam);
        }
        int limit = limitParam == null ? defaultLimit : limitParam;
        Cursor after = sinceSeq == null ? null : Cursor.ofSeq(sort, sinceSeq);
        return new PageRequest(limit, after, sort, null, sinceSeq == null && limitParam == null);
    }

    private static String blankToNull(String s) {
        return (s == null || s.isBlank()) ? null : s;
    }

    public int limit() { return limit; }
    public Cursor after() { return after; }
    public String sort() { return sort; }
    public String query() { return query; }
    public boolean hasQuery() { return query != null; }

    /** True when the caller sent no paging parameters and expects today's bare array. */
    public boolean isLegacy() { return legacy; }

    /** One more than {@link #limit()}, the usual trick for detecting a further page in one read. */
    public int fetchSize() { return Math.min(limit + 1, MAX_LIMIT + 1); }
}
