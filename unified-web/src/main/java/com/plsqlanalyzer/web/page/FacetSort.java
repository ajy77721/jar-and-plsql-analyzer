package com.plsqlanalyzer.web.page;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * One member of an endpoint's <b>closed</b> set of sort orders.
 *
 * <p>Closed on purpose. A free-form column name on the wire is two problems at once: it is an
 * injection surface, and it invites an ordering nothing supports, which becomes a scan. Every
 * converted endpoint declares its orders up front and answers anything else with 400 — never a
 * silently different ordering.
 *
 * <p><b>One definition, not two.</b> The sort key is a single text function and the row order is
 * <em>derived</em> from it. This matters because cursor paging resumes with "strictly after this
 * key", and if the comparison that produced the order and the comparison that resumes it could
 * disagree, rows would repeat or vanish at a page boundary — a defect that only appears on page 2
 * of large data. Numbers are therefore encoded into a fixed-width form whose lexicographic order
 * <em>is</em> their numeric order, rather than being compared one way and resumed another.
 *
 * @param name       the wire name, e.g. {@code NAME_ASC}
 * @param key        the sort key of a row, as text; also the cursor's {@code k} tuple
 * @param descending whether the key runs backwards
 */
public record FacetSort(String name, Function<JsonNode, String> key, boolean descending) {

    public FacetSort {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(key, "key");
    }

    /** Text order on one field, upper-cased to match what the UI displays and sorts on. */
    public static FacetSort text(String name, String field, boolean descending) {
        return new FacetSort(name, n -> n.path(field).asText("").toUpperCase(Locale.ROOT),
                descending);
    }

    /** Text order on a value the caller derives (a nested field, a computed label). */
    public static FacetSort derived(String name, Function<JsonNode, String> key,
                                    boolean descending) {
        return new FacetSort(name, key, descending);
    }

    /** Numeric order on one field. */
    public static FacetSort number(String name, String field, boolean descending) {
        return new FacetSort(name, n -> encodeLong(n.path(field).asLong(0L)), descending);
    }

    /** Numeric order on a value the caller computes. */
    public static FacetSort computed(String name, java.util.function.ToLongFunction<JsonNode> value,
                                     boolean descending) {
        return new FacetSort(name, n -> encodeLong(value.applyAsLong(n)), descending);
    }

    /**
     * Fixed-width big-endian hex of the sign-flipped value, so lexicographic order over the text
     * equals signed numeric order — including for negatives, where zero-padded decimal does not.
     */
    public static String encodeLong(long v) {
        return String.format("%016x", v ^ Long.MIN_VALUE);
    }

    /**
     * The total order actually applied: this key, then the stable row id as tiebreaker.
     *
     * <p>The tiebreaker always ascends, whichever way the key runs, so that the resume comparison
     * has one unambiguous answer. It is a stable id and <b>never an ordinal</b> — ordinals here are
     * run-scoped, because re-running an analysis renumbers its nodes.
     */
    public Comparator<JsonNode> comparator(Function<JsonNode, String> id) {
        Comparator<JsonNode> byKey = Comparator.comparing(key);
        if (descending) byKey = byKey.reversed();
        return byKey.thenComparing(id);
    }

    /** Indexes a declared set by wire name; iteration order is declaration order. */
    public static Map<String, FacetSort> byName(FacetSort... sorts) {
        Map<String, FacetSort> out = new LinkedHashMap<>();
        for (FacetSort s : sorts) {
            if (out.put(s.name(), s) != null) {
                throw new IllegalStateException("Duplicate sort name " + s.name());
            }
        }
        return out;
    }
}
