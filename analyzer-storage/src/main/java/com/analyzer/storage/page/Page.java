package com.analyzer.storage.page;

import java.util.List;
import java.util.function.Function;

/**
 * One page of results plus the information needed to ask for the next.
 *
 * <p>The serialised shape is fixed and small:
 *
 * <pre>
 * { "items": [ ... ],
 *   "page": { "nextCursor": "...", "hasMore": true, "total": 1743,
 *             "totalIsExact": true, "limit": 25, "sort": "SIDEBAR" } }
 * </pre>
 *
 * <p>There is deliberately no {@code page}, {@code offset} or {@code pageCount}. Offering them
 * would invite the client to compute its own cursor, which must never happen — the cursor's
 * correctness depends on it being produced by whoever produced the ordering.
 *
 * <p>{@code total} is {@code -1} with {@code totalIsExact=false} whenever counting is not cheap.
 * That is surfaced honestly rather than hidden behind an estimate, because a UI that renders
 * "Page 3 of 17" from a guess is worse than one that renders "Page 3" and a next arrow.
 */
public record Page<T>(List<T> items, PageInfo page) {

    public Page {
        items = List.copyOf(items);
    }

    /** A page that is known to be the last one, with an exact total. */
    public static <T> Page<T> last(List<T> items, String sort, int limit) {
        return new Page<>(items, new PageInfo(null, false, items.size(), true, limit, sort));
    }

    /** A page with more to come. {@code nextCursor} must be non-null. */
    public static <T> Page<T> more(List<T> items, Cursor nextCursor, String sort, int limit,
                                   long total, boolean totalIsExact) {
        if (nextCursor == null) {
            throw new IllegalArgumentException("hasMore=true requires a nextCursor");
        }
        return new Page<>(items, new PageInfo(nextCursor.encode(), true, total, totalIsExact,
                limit, sort));
    }

    /**
     * Wraps a full result set as a single page.
     *
     * <p>Used by the artifact-scan repository, which reads a whole JSON document anyway, and by any
     * endpoint whose result is provably bounded. It keeps the contract identical for callers so a
     * screen never has to know which implementation answered.
     */
    public static <T> Page<T> single(List<T> items, String sort) {
        return new Page<>(items, new PageInfo(null, false, items.size(), true, items.size(), sort));
    }

    public static <T> Page<T> empty(String sort, int limit) {
        return new Page<>(List.of(), new PageInfo(null, false, 0, true, limit, sort));
    }

    /** Maps the items, keeping the paging information untouched. */
    public <R> Page<R> map(Function<? super T, ? extends R> f) {
        return new Page<>(items.stream().<R>map(f::apply).toList(), page);
    }

    public boolean hasMore() {
        return page.hasMore();
    }
}
