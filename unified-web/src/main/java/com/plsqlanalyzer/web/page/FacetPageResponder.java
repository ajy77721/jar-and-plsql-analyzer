package com.plsqlanalyzer.web.page;

import com.analyzer.storage.page.Cursor;
import com.analyzer.storage.page.Page;
import com.analyzer.storage.page.PageRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Turns a facet document into either <b>today's exact response</b> or a page envelope, decided by
 * the request alone.
 *
 * <p>That decision is the whole reason conversion can proceed one endpoint at a time. A caller
 * that sends no paging parameter and does not ask for the envelope media type gets the bare array
 * byte-for-byte as before — including its original ordering, because the legacy path does not go
 * near the pager. So an old screen and a converted screen can hit the same URL on the same
 * deploy, and reverting one controller touches nothing else.
 *
 * <p>The legacy path does gain one thing: a cap. Without it an unconverted screen can re-introduce
 * a multi-megabyte response after all this work, and nothing would notice. Over the cap the
 * response is truncated and carries {@code X-Analyzer-Truncated: true} — visible, rather than an
 * error the screen cannot handle or a silent 500 later.
 */
@Component
public class FacetPageResponder {

    /** Opting into the envelope without sending a paging parameter. */
    public static final String PAGE_MEDIA_TYPE = "application/vnd.analyzer.page+json";

    /** Set when the legacy full-array path hit the cap. */
    public static final String TRUNCATED_HEADER = "X-Analyzer-Truncated";

    private final ObjectMapper mapper;
    private final int maxUnpagedItems;

    public FacetPageResponder(ObjectMapper mapper,
                              @Value("${app.api.max-unpaged-items:10000}") int maxUnpagedItems) {
        this.mapper = mapper;
        this.maxUnpagedItems = maxUnpagedItems;
    }

    /**
     * The raw query parameters of a paged request, straight off the controller signature.
     *
     * @param cf column filters, one entry per column, each {@code <colIdx>:<v1>|<v2>|…}
     */
    public record Params(Integer limit, String after, String sort, String q, String types,
                         List<String> cf, String accept) {

        /**
         * Whether this request asked for the envelope at all. Public because endpoints outside
         * this package build their legacy response themselves — a streamed one cannot be produced
         * by {@link #respond}, so they need the same decision, made the same way, rather than a
         * second interpretation of "did the caller page".
         */
        public boolean wantsEnvelope() {
            return limit != null
                    || (after != null && !after.isBlank())
                    || (sort != null && !sort.isBlank())
                    || (q != null && !q.isBlank())
                    || (cf != null && !cf.isEmpty())
                    || (accept != null && accept.contains(PAGE_MEDIA_TYPE));
        }
    }

    /**
     * @param document the facet as it is today: an array, or an object with an array under
     *                 {@code arrayField}
     * @param scope    an extra row predicate (the Direct/Subtree/SubFlow selector), or null
     * @throws IllegalArgumentException on an unknown sort or an out-of-range limit → 400
     * @throws Cursor.InvalidCursorException on a stale or foreign cursor → 400
     */
    public ResponseEntity<JsonNode> respond(JsonNode document, String arrayField, FacetSpec spec,
                                            Params params, Predicate<JsonNode> scope) {
        ArrayNode rows = rowsOf(document, arrayField);
        if (rows == null) {
            // No data at all is its own answer. An empty array would be indistinguishable from
            // "the filter matched nothing", which is a different thing for the UI to say.
            return ResponseEntity.noContent().build();
        }

        if (!params.wantsEnvelope()) {
            return legacy(document, rows);
        }

        PageRequest req = PageRequest.of(params.limit(), params.after(), params.sort(),
                params.q(), spec.sorts().keySet(), spec.defaultSort(), spec.defaultLimit());

        Predicate<JsonNode> filter = combine(scope, typeFilter(spec, params.types()));
        filter = combine(filter, columnFilters(spec, params.cf()));
        Page<JsonNode> page = JsonFacetPager.page(rows.iterator(), req, spec.sorts(),
                spec.id(), spec.searchText(), filter, resultSetId(rows, spec));
        return ResponseEntity.ok(envelope(page));
    }

    /**
     * An identity for the set of rows being paged, stamped into every cursor it mints.
     *
     * <p>MID-019 asks for a run id. These readers are file-backed and have no run, so the identity
     * is taken from the rows themselves: how many there are, and the first and last row's id under
     * the facet's own id function.
     *
     * <p>That is a better fit than a run id here, and strictly so: a re-analysis producing
     * byte-identical output is genuinely safe to page across — measured, 188 of 189 artifact files
     * are unchanged between runs — and a run id would reject it anyway. A content identity rejects
     * exactly the case that is unsafe.
     *
     * <p>Cheap by construction: the array is already materialised and only two elements are read.
     */
    private String resultSetId(ArrayNode rows, FacetSpec spec) {
        if (rows.isEmpty()) return "empty";
        String first = spec.id().apply(rows.get(0));
        String last = spec.id().apply(rows.get(rows.size() - 1));
        return Integer.toHexString(java.util.Objects.hash(rows.size(), first, last));
    }

    /**
     * The row predicate a request implies — search, pills and column filters, ANDed.
     *
     * <p>Exposed so that a caller computing its own aggregates over the filtered set uses the very
     * same predicate the page used. A second, hand-rolled interpretation of "does this row match"
     * is how a header that reads "8 analyses" comes to sit above a table showing nine.
     *
     * @param extra an additional predicate to AND in (scope, pills), or null
     * @throws IllegalArgumentException on an undeclared filter column, or on {@code q} for an
     *         endpoint with no server-side search
     */
    public Predicate<JsonNode> filterFor(FacetSpec spec, Params params, Predicate<JsonNode> extra) {
        if (params.q() != null && !params.q().isBlank() && spec.searchText() == null) {
            throw new IllegalArgumentException(
                    "This endpoint does not support server-side search; refusing to ignore 'q'");
        }
        Predicate<JsonNode> filter = combine(extra, typeFilter(spec, params.types()));
        filter = combine(filter, columnFilters(spec, params.cf()));
        if (params.q() != null && !params.q().isBlank()) {
            String needle = params.q().toLowerCase(Locale.ROOT);
            Function<JsonNode, String> text = spec.searchText();
            filter = combine(filter, row -> {
                String t = text.apply(row);
                return t != null && t.toLowerCase(Locale.ROOT).contains(needle);
            });
        }
        return filter == null ? row -> true : filter;
    }

    /**
     * The same page envelope, but from an {@link java.util.Iterator} of rows instead of a parsed
     * document — for facets too large to hold in memory even once.
     *
     * <p>Used where the legacy response is streamed rather than built (the class tree, the bundled
     * JAR list). The caller has already decided that the request wants an envelope and has already
     * answered the legacy and no-data cases, because for a streamed source those decisions are made
     * before a parser is opened. What is left is identical to {@link #respond}: validate, filter,
     * page, serialise.
     *
     * @param rows consumed once; the caller owns closing the underlying parser
     */
    public ResponseEntity<JsonNode> respondStreaming(Iterator<JsonNode> rows, FacetSpec spec,
                                                     Params params, Predicate<JsonNode> scope) {
        PageRequest req = PageRequest.of(params.limit(), params.after(), params.sort(),
                params.q(), spec.sorts().keySet(), spec.defaultSort(), spec.defaultLimit());
        Predicate<JsonNode> filter = combine(scope, typeFilter(spec, params.types()));
        filter = combine(filter, columnFilters(spec, params.cf()));
        Page<JsonNode> page = JsonFacetPager.page(rows, req, spec.sorts(), spec.id(),
                spec.searchText(), filter);
        return ResponseEntity.ok(envelope(page));
    }

    /**
     * Counts for the header line, which used to be read off the full array: the unscoped total,
     * the total inside the current scope, and the pill values that exist at all.
     *
     * <p>Separate from {@link #respond} because they answer different questions and change at
     * different rates — folding them into the page envelope would have added fields the contract
     * deliberately does not have, and would recompute them on every page turn.
     */
    public ResponseEntity<JsonNode> summary(JsonNode document, String arrayField, FacetSpec spec,
                                            Predicate<JsonNode> scope) {
        ArrayNode rows = rowsOf(document, arrayField);
        if (rows == null) return ResponseEntity.noContent().build();

        long total = 0;
        long scoped = 0;
        Map<String, Integer> typeCounts = new TreeMap<>();
        Map<String, Set<String>> columnValues = new LinkedHashMap<>();
        spec.filterColumns().keySet().forEach(c -> columnValues.put(c, new TreeSet<>()));

        for (JsonNode row : rows) {
            if (!row.isObject()) continue;
            total++;
            boolean inScope = scope == null || scope.test(row);
            if (!inScope) continue;
            scoped++;
            if (spec.typeValues() != null) {
                for (String t : spec.typeValues().apply(row)) typeCounts.merge(t, 1, Integer::sum);
            }
            // Collected inside the scope, because that is the set the popover offered before —
            // the client built it from the scoped array it had been handed.
            spec.filterColumns().forEach((col, fn) -> columnValues.get(col).addAll(fn.apply(row)));
        }
        ObjectNode out = mapper.createObjectNode();
        out.put("total", total);
        out.put("scopedTotal", scoped);
        ObjectNode counts = out.putObject("typeCounts");
        typeCounts.forEach(counts::put);
        ArrayNode types = out.putArray("types");
        typeCounts.keySet().forEach(types::add);
        ObjectNode cols = out.putObject("columnValues");
        columnValues.forEach((col, values) -> {
            ArrayNode arr = cols.putArray(col);
            values.forEach(arr::add);
        });
        ArrayNode sorts = out.putArray("sorts");
        spec.sorts().keySet().forEach(sorts::add);
        return ResponseEntity.ok(out);
    }

    /* ------------------------------------------------------------------ */

    private ArrayNode rowsOf(JsonNode document, String arrayField) {
        if (document == null || document.isNull() || document.isMissingNode()) return null;
        if (document.isArray()) return (ArrayNode) document;
        if (arrayField != null && document.path(arrayField).isArray()) {
            return (ArrayNode) document.get(arrayField);
        }
        return null;
    }

    /** Today's response, unchanged, except that it can no longer be unbounded. */
    private ResponseEntity<JsonNode> legacy(JsonNode document, ArrayNode rows) {
        if (rows.size() <= maxUnpagedItems) {
            return ResponseEntity.ok(document);
        }
        ArrayNode capped = mapper.createArrayNode();
        for (int i = 0; i < maxUnpagedItems; i++) capped.add(rows.get(i));
        JsonNode body = capped;
        if (!document.isArray()) {
            // Rebuilt field by field rather than deep-copied: a deep copy would duplicate the
            // very array we are truncating to save memory.
            ObjectNode copy = mapper.createObjectNode();
            Iterator<String> names = document.fieldNames();
            while (names.hasNext()) {
                String n = names.next();
                copy.set(n, document.get(n) == rows ? capped : document.get(n));
            }
            body = copy;
        }
        return ResponseEntity.ok()
                .header(TRUNCATED_HEADER, "true")
                .body(body);
    }

    private ObjectNode envelope(Page<JsonNode> page) {
        ObjectNode out = mapper.createObjectNode();
        ArrayNode items = out.putArray("items");
        page.items().forEach(items::add);
        ObjectNode info = out.putObject("page");
        // Serialised by hand so the wire shape is stated in one visible place rather than being
        // whatever field-naming configuration happens to be in effect.
        if (page.page().nextCursor() == null) info.putNull("nextCursor");
        else info.put("nextCursor", page.page().nextCursor());
        info.put("hasMore", page.page().hasMore());
        info.put("total", page.page().total());
        info.put("totalIsExact", page.page().totalIsExact());
        info.put("limit", page.page().limit());
        info.put("sort", page.page().sort());
        return out;
    }

    /** A pill filter: a row matches when any of its type values was selected. */
    private Predicate<JsonNode> typeFilter(FacetSpec spec, String types) {
        if (types == null || types.isBlank() || spec.typeValues() == null) return null;
        List<String> wanted = new ArrayList<>();
        for (String t : types.split(",")) {
            String v = t.trim();
            if (!v.isEmpty()) wanted.add(v);
        }
        if (wanted.isEmpty()) return null;
        Map<String, Boolean> set = new LinkedHashMap<>();
        wanted.forEach(w -> set.put(w.toUpperCase(Locale.ROOT), Boolean.TRUE));
        return row -> {
            for (String v : spec.typeValues().apply(row)) {
                if (set.containsKey(v.toUpperCase(Locale.ROOT))) return true;
            }
            return false;
        };
    }

    /** A text-mode filter value longer than this is refused rather than compiled — a cap on the
     *  regex-complexity/catastrophic-backtracking surface a free-form query string opens up. */
    private static final int MAX_TEXT_FILTER_LENGTH = 200;

    /**
     * The per-column popovers, ANDed together — the same shape the client-side version had, so a
     * converted table filters identically.
     *
     * <p>Two token shapes share the {@code cf} parameter, disambiguated by the column's own
     * declaration rather than by sniffing the token, so a value that happens to start with
     * {@code "contains:"} or {@code "regex:"} cannot be misread as a mode keyword:
     *
     * <ul>
     *   <li>Checkbox columns ({@link FacetSpec#filterColumns}): {@code <col>:<value>|<value>} — a
     *       row matches if any of its values for that column is one of the given ones (ORed within
     *       the token, unchanged from before this capability existed).</li>
     *   <li>Text columns ({@link FacetSpec#textColumns}): {@code <col>:contains:<value>} or
     *       {@code <col>:regex:<pattern>}, each with an optional trailing {@code :ci} for
     *       case-insensitive matching (case-sensitive is the default without it).</li>
     * </ul>
     *
     * @throws IllegalArgumentException on a column the endpoint has not declared, an invalid or
     *         oversized regex, or a malformed token — a 400, never a silent no-op or a 500.
     */
    private Predicate<JsonNode> columnFilters(FacetSpec spec, List<String> cf) {
        if (cf == null || cf.isEmpty()) return null;
        Predicate<JsonNode> all = null;
        for (String entry : cf) {
            if (entry == null || entry.isBlank()) continue;
            int colon = entry.indexOf(':');
            if (colon <= 0) {
                throw new IllegalArgumentException(
                        "Column filter must be '<column>:<value>|<value>', got '" + entry + "'");
            }
            String column = entry.substring(0, colon);
            String rest = entry.substring(colon + 1);

            if (spec.textColumns().containsKey(column)) {
                all = combine(all, textColumnFilter(spec, column, rest));
                continue;
            }
            Function<JsonNode, List<String>> values = spec.filterColumns().get(column);
            if (values == null) {
                throw new IllegalArgumentException("Column '" + column + "' is not filterable. "
                        + "Filterable: " + spec.filterColumns().keySet() + "; text: "
                        + spec.textColumns().keySet());
            }
            Set<String> allowed = new LinkedHashSet<>();
            for (String v : rest.split("\\|")) {
                if (!v.isEmpty()) allowed.add(v.toUpperCase(Locale.ROOT));
            }
            if (allowed.isEmpty()) continue;
            all = combine(all, row -> {
                for (String v : values.apply(row)) {
                    if (allowed.contains(v.toUpperCase(Locale.ROOT))) return true;
                }
                return false;
            });
        }
        return all;
    }

    /**
     * One {@code <col>:contains:<value>[:ci]} or {@code <col>:regex:<pattern>[:ci]} token, applied
     * against {@link FacetSpec#textColumns}'s extractor for that column.
     */
    private Predicate<JsonNode> textColumnFilter(FacetSpec spec, String column, String rest) {
        Function<JsonNode, String> text = spec.textColumns().get(column);
        boolean ci = rest.endsWith(":ci");
        if (ci) rest = rest.substring(0, rest.length() - 3);

        String mode;
        String value;
        if (rest.startsWith("contains:")) {
            mode = "contains";
            value = rest.substring("contains:".length());
        } else if (rest.startsWith("regex:")) {
            mode = "regex";
            value = rest.substring("regex:".length());
        } else {
            throw new IllegalArgumentException("Text column filter for '" + column
                    + "' must be 'contains:<value>' or 'regex:<pattern>', optionally ':ci', got '"
                    + rest + "'");
        }
        if (value.isEmpty()) return row -> true;
        if (value.length() > MAX_TEXT_FILTER_LENGTH) {
            throw new IllegalArgumentException("Text filter for '" + column + "' is too long "
                    + "(" + value.length() + " > " + MAX_TEXT_FILTER_LENGTH + " characters)");
        }

        if (mode.equals("regex")) {
            java.util.regex.Pattern pattern;
            try {
                int flags = ci ? java.util.regex.Pattern.CASE_INSENSITIVE : 0;
                pattern = java.util.regex.Pattern.compile(value, flags);
            } catch (java.util.regex.PatternSyntaxException e) {
                throw new IllegalArgumentException("Invalid regex for column '" + column + "': "
                        + e.getMessage());
            }
            java.util.regex.Pattern finalPattern = pattern;
            return row -> {
                String t = text.apply(row);
                return t != null && finalPattern.matcher(t).find();
            };
        }

        // contains
        String needle = ci ? value.toLowerCase(Locale.ROOT) : value;
        return row -> {
            String t = text.apply(row);
            if (t == null) return false;
            return ci ? t.toLowerCase(Locale.ROOT).contains(needle) : t.contains(needle);
        };
    }

    private static Predicate<JsonNode> combine(Predicate<JsonNode> a, Predicate<JsonNode> b) {
        if (a == null) return b;
        if (b == null) return a;
        return a.and(b);
    }

    /** Maps the two validation failures to 400 with a reason, never to an empty page. */
    public static ResponseEntity<JsonNode> badRequest(ObjectMapper mapper, String message) {
        ObjectNode body = mapper.createObjectNode();
        body.put("error", "BAD_PAGE_REQUEST");
        body.put("message", message);
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(body);
    }
}
