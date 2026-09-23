package com.analyzer.storage.page;

/**
 * The paging half of a {@link Page} response.
 *
 * @param nextCursor opaque resume token; {@code null} iff {@code hasMore} is false
 * @param hasMore    whether another page exists
 * @param total      total matching rows, or {@code -1} when counting is not cheap
 * @param totalIsExact false when {@code total} is {@code -1}; lets the UI render "Page 3" with a
 *                     next arrow instead of a fabricated "Page 3 of 17"
 * @param limit      the limit actually applied, which may be lower than requested
 * @param sort       the sort actually applied — the client echoes this back with the cursor
 */
public record PageInfo(String nextCursor, boolean hasMore, long total, boolean totalIsExact,
                       int limit, String sort) {

    public PageInfo {
        if (hasMore && (nextCursor == null || nextCursor.isBlank())) {
            throw new IllegalArgumentException("hasMore=true requires a nextCursor");
        }
        if (!hasMore && nextCursor != null) {
            throw new IllegalArgumentException("hasMore=false must not carry a nextCursor");
        }
    }

    /** Total is unknown; the UI must not render a page count. */
    public static PageInfo unknownTotal(String nextCursor, boolean hasMore, int limit, String sort) {
        return new PageInfo(nextCursor, hasMore, -1, false, limit, sort);
    }
}
