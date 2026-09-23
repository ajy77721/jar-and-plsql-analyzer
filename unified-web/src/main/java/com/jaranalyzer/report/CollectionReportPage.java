package com.jaranalyzer.report;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.analyzer.storage.page.Cursor;
import com.analyzer.storage.page.InvalidPageRequestException;
import com.analyzer.storage.page.PageRequest;

import java.util.List;

/**
 * Serialises the collection report, paged when asked and a bare array when not.
 *
 * <p>Bare array by default because this endpoint is documented and has no in-application caller —
 * an external script reading it must keep working. A caller that sends {@code limit} gets an
 * envelope, so the response is bounded by contract rather than by the aggregate happening to be
 * small.
 */
public final class CollectionReportPage {

    /** Cap, so an unbounded response is impossible even without a limit. */
    public static final int MAX_ROWS = 1000;

    private CollectionReportPage() {
    }

    public static JsonNode respond(ObjectMapper mapper,
                                   List<CollectionReportAggregator.CollectionRow> rows,
                                   Integer limit, String after, String accept) {
        if (limit == null && (after == null || after.isBlank())) {
            ArrayNode arr = mapper.createArrayNode();
            // Capped even on the legacy path: "the aggregate is small today" is a property of the
            // data, not a guarantee of the endpoint.
            for (int i = 0; i < Math.min(rows.size(), MAX_ROWS); i++) arr.add(row(mapper, rows.get(i)));
            return arr;
        }

        int size = limit == null ? 50 : limit;
        if (size < 1 || size > PageRequest.MAX_LIMIT) {
            throw new InvalidPageRequestException(
                    "limit must be between 1 and " + PageRequest.MAX_LIMIT + ", got " + size);
        }

        // The cursor's tiebreaker is the collection name: it is the aggregate's key and therefore
        // unique by construction, which is what a page boundary needs.
        int from = 0;
        if (after != null && !after.isBlank()) {
            Cursor cursor = Cursor.decode(after, "ENDPOINTS_DESC");
            String lastName = cursor.tiebreaker();
            for (int i = 0; i < rows.size(); i++) {
                if (lastName.equals(rows.get(i).collection())) { from = i + 1; break; }
            }
        }

        int to = Math.min(rows.size(), from + size);
        ArrayNode items = mapper.createArrayNode();
        for (int i = from; i < to; i++) items.add(row(mapper, rows.get(i)));

        boolean hasMore = to < rows.size();
        ObjectNode page = mapper.createObjectNode();
        page.put("nextCursor", hasMore
                ? Cursor.of("ENDPOINTS_DESC", List.of(String.valueOf(rows.get(to - 1).endpointCount())),
                        rows.get(to - 1).collection()).encode()
                : null);
        page.put("hasMore", hasMore);
        page.put("total", rows.size());
        page.put("totalIsExact", true);
        page.put("limit", size);
        page.put("sort", "ENDPOINTS_DESC");

        ObjectNode out = mapper.createObjectNode();
        out.set("items", items);
        out.set("page", page);
        return out;
    }

    private static ObjectNode row(ObjectMapper mapper,
                                  CollectionReportAggregator.CollectionRow r) {
        ObjectNode o = mapper.createObjectNode();
        o.put("collection", r.collection());
        o.put("domain", r.domain());
        ArrayNode ops = o.putArray("operations");
        r.operations().forEach(ops::add);
        ArrayNode eps = o.putArray("endpoints");
        for (var e : r.endpoints()) {
            ObjectNode en = eps.addObject();
            e.forEach(en::put);
        }
        o.put("endpointCount", r.endpointCount());
        return o;
    }
}
