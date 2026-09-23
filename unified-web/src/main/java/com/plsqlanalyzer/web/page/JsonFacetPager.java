package com.plsqlanalyzer.web.page;

import com.analyzer.storage.page.Cursor;
import com.analyzer.storage.page.Page;
import com.analyzer.storage.page.PageInfo;
import com.analyzer.storage.page.PageRequest;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Serves one page of a JSON facet, filtered and sorted server-side, without ever holding more than
 * one page of rows.
 *
 * <p><b>Why this exists.</b> Every table in the parser UI used to fetch its whole facet and slice
 * it in the browser. Moving the slice to the server only helps if the search box and the column
 * headers move with it: with one page in memory a client-side search silently searches that page.
 * So this class answers all three — filter, order, page — from the same request.
 *
 * <p><b>Memory.</b> The input is an {@link Iterator}, and selection uses a bounded heap of
 * {@code limit + 1} rows. Nothing proportional to the result set is retained, so a facet with
 * 200 000 rows costs the same as one with 20. The count of matching rows is still exact, because
 * counting is a scalar increment; there is no reason to report {@code totalIsExact=false} here and
 * make the UI drop its page count.
 *
 * <p><b>Cursor resume.</b> A row is on a later page than the cursor when its {@code (key, id)}
 * tuple sorts strictly after the cursor's, compared with the very same comparator that produced
 * the order (see {@link FacetSort}). Rows equal to the cursor are excluded, which is what makes a
 * page boundary neither duplicate nor drop a row.
 */
public final class JsonFacetPager {

    private JsonFacetPager() {
    }

    /**
     * @param rows      the facet rows, consumed once
     * @param req       the validated request; {@code req.sort()} must name a member of {@code sorts}
     * @param sorts     the endpoint's closed sort set
     * @param id        a row's stable identity, used as the final tiebreaker and the cursor's
     *                  {@code i}. Must be unique per row, or a page boundary can lose a row.
     * @param searchText a row's searchable text, already lower-cased by the caller's convention;
     *                  {@code null} means the endpoint has no server-side search, in which case a
     *                  {@code q} on the request is an error rather than a no-op
     * @param extra     an additional predicate (scope, pills), or {@code null}
     */
    public static Page<JsonNode> page(Iterator<JsonNode> rows, PageRequest req,
                                      Map<String, FacetSort> sorts,
                                      Function<JsonNode, String> id,
                                      Function<JsonNode, String> searchText,
                                      Predicate<JsonNode> extra) {
        return page(rows, req, sorts, id, searchText, extra, null);
    }

    /**
     * @param resultSetId identifies the set of rows being paged, stamped into the cursor and
     *                    checked on the next request. Null keeps the old unscoped behaviour for
     *                    callers that have no such identity.
     */
    public static Page<JsonNode> page(Iterator<JsonNode> rows, PageRequest req,
                                      Map<String, FacetSort> sorts,
                                      Function<JsonNode, String> id,
                                      Function<JsonNode, String> searchText,
                                      Predicate<JsonNode> extra,
                                      String resultSetId) {
        // Checked before a single row is read. A cursor from a different result set cannot be
        // answered correctly, and answering it partly and failing later would already have
        // returned rows the caller will treat as continuous with the previous page.
        if (req.after() != null) req.after().requireRun(resultSetId);
        FacetSort sort = sorts.get(req.sort());
        if (sort == null) {
            throw new IllegalArgumentException(
                    "Unknown sort '" + req.sort() + "'. Allowed: " + sorts.keySet());
        }
        if (req.hasQuery() && searchText == null) {
            throw new IllegalArgumentException(
                    "This endpoint does not support server-side search; refusing to ignore 'q' "
                    + "silently, which would return unfiltered rows that look filtered");
        }

        String needle = req.hasQuery() ? req.query().toLowerCase(java.util.Locale.ROOT) : null;
        Comparator<JsonNode> order = sort.comparator(id);
        int limit = req.limit();

        // Max-heap on the sort order: the head is the row furthest along, so it is the one to
        // discard once we hold one more than a page.
        PriorityQueue<JsonNode> best = new PriorityQueue<>(Math.min(limit + 2, 1024),
                order.reversed());
        long matched = 0;

        while (rows.hasNext()) {
            JsonNode row = rows.next();
            if (row == null || !row.isObject()) continue;
            if (extra != null && !extra.test(row)) continue;
            if (needle != null) {
                String text = searchText.apply(row);
                if (text == null || !text.toLowerCase(java.util.Locale.ROOT).contains(needle)) {
                    continue;
                }
            }
            // `total` is the size of the filtered result set, not of what is left after the
            // cursor — the UI shows it as "N items", which must not shrink as the user pages.
            matched++;
            if (req.after() != null && !isAfter(row, req.after(), sort, id)) continue;

            best.add(row);
            if (best.size() > limit + 1) best.poll();
        }

        List<JsonNode> items = new ArrayList<>(best);
        items.sort(order);

        boolean hasMore = items.size() > limit;
        if (hasMore) items = items.subList(0, limit);
        if (items.isEmpty()) {
            return new Page<>(List.of(), new PageInfo(null, false, matched, true, limit,
                    sort.name()));
        }
        if (!hasMore) {
            return new Page<>(List.copyOf(items),
                    new PageInfo(null, false, matched, true, limit, sort.name()));
        }
        JsonNode last = items.get(items.size() - 1);
        Cursor next = Cursor.of(sort.name(), List.of(sort.key().apply(last)), id.apply(last),
                resultSetId);
        return Page.more(List.copyOf(items), next, sort.name(), limit, matched, true);
    }

    /**
     * Whether {@code row} sorts strictly after {@code cursor}.
     *
     * <p>Compared on the {@code (key, id)} tuple with the sort's own direction, which is the same
     * comparison that produced the ordering. Deriving the resume test from a second, independent
     * piece of logic is how paged lists come to skip rows only on page 2 of large data.
     */
    private static boolean isAfter(JsonNode row, Cursor cursor, FacetSort sort,
                                   Function<JsonNode, String> id) {
        String cursorKey = cursor.keys().isEmpty() ? "" : cursor.keys().get(0);
        int c = sort.key().apply(row).compareTo(cursorKey);
        if (sort.descending()) c = -c;
        if (c != 0) return c > 0;
        // Equal keys: the stable id breaks the tie, always ascending (see FacetSort#comparator).
        return id.apply(row).compareTo(cursor.tiebreaker()) > 0;
    }
}
