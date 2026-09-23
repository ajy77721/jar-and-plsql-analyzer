package com.plsqlanalyzer.web.parser.search;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The cross-analysis search query language.
 *
 * <p>Deliberately small. The motivating question is a real one a user asked —
 * <i>"is there any UPDATE on this table for this field in this proc"</i> — and it becomes:
 *
 * <pre>
 *   op:UPDATE table:ERP_INVENTORY col:QTY_ALLOCATED proc:USP_ALLOCATE
 * </pre>
 *
 * <h2>Rules</h2>
 *
 * <ul>
 *   <li>{@code qualifier:value} constrains one field; bare words match any field.</li>
 *   <li>Different qualifiers are <b>AND</b>ed — each is a further constraint.</li>
 *   <li>The <b>same</b> qualifier repeated is <b>OR</b>ed: {@code op:UPDATE op:DELETE} means
 *       "either", because that is what a person means when they name two operations. AND-ing them
 *       would always return nothing, which is a language that punishes the obvious reading.</li>
 *   <li>{@code !} negates a term.</li>
 *   <li>{@code "…"} quotes a phrase, so {@code sql:"order by"} is one term.</li>
 *   <li>A trailing {@code *} is a prefix match. That is the <b>only</b> wildcard, and there is no
 *       regex: accepting a user-supplied pattern and running it over 5,000 {@code sqlText} blobs
 *       from a URL parameter is a denial-of-service surface bought for no real gain.</li>
 * </ul>
 *
 * <h2>Unknown qualifiers fail loudly</h2>
 *
 * {@code tabel:ORDERS} is a typo, and treating it as a bare word would return a confident, empty
 * result — the user would conclude there are no accesses to that table. {@link #errors()} carries
 * the problem so the caller can answer with the valid qualifiers instead of a blank page. This is
 * the same rule the rest of this branch follows: never present an absence as a fact when the real
 * answer is "the question did not parse".
 */
public final class SearchQuery {

    /** A single parsed term. */
    public record Term(String field, String value, boolean negated, boolean prefix) {
        /** True when this term matches any field rather than a named one. */
        public boolean isBare() { return field == null; }
    }

    /** Qualifier → the row field it constrains. Order is the order shown in an error message. */
    public static final Map<String, String> QUALIFIERS = new LinkedHashMap<>();
    static {
        QUALIFIERS.put("table", "tableName");
        QUALIFIERS.put("proc", "nodeName");
        QUALIFIERS.put("col", "columns");
        QUALIFIERS.put("op", "operation");
        QUALIFIERS.put("schema", "schema");
        QUALIFIERS.put("file", "sourceFile");
        QUALIFIERS.put("sql", "sqlText");
        QUALIFIERS.put("line", "line");
    }

    /** Aliases people reach for. Accepting them costs nothing and avoids an unhelpful error. */
    private static final Map<String, String> ALIASES = Map.of(
            "t", "table", "p", "proc", "c", "col", "o", "op",
            "column", "col", "procedure", "proc", "operation", "op", "tablename", "table");

    private final List<Term> terms;
    private final List<String> errors;
    private final String raw;

    private SearchQuery(String raw, List<Term> terms, List<String> errors) {
        this.raw = raw;
        this.terms = terms;
        this.errors = errors;
    }

    public List<Term> terms()   { return terms; }
    public List<String> errors(){ return errors; }
    public String raw()         { return raw; }
    public boolean isEmpty()    { return terms.isEmpty(); }

    /**
     * Parses a query string. Never throws: a malformed query yields {@link #errors()}, because the
     * caller needs to tell the user what was wrong rather than return a 500.
     */
    public static SearchQuery parse(String q) {
        List<Term> out = new ArrayList<>();
        List<String> errs = new ArrayList<>();
        if (q == null || q.isBlank()) return new SearchQuery(q == null ? "" : q, out, errs);

        for (String tok : tokenize(q)) {
            boolean negated = tok.startsWith("!");
            if (negated) tok = tok.substring(1);
            if (tok.isEmpty()) continue;

            String field = null;
            String value = tok;
            int colon = tok.indexOf(':');
            if (colon > 0) {
                String qual = tok.substring(0, colon).toLowerCase(Locale.ROOT);
                String rest = tok.substring(colon + 1);
                String canonical = ALIASES.getOrDefault(qual, qual);
                if (QUALIFIERS.containsKey(canonical)) {
                    field = QUALIFIERS.get(canonical);
                    value = rest;
                } else if (looksLikeQualifier(qual)) {
                    // A typo here would otherwise be searched as literal text and return nothing,
                    // which reads as "no such data" rather than "no such qualifier".
                    errs.add("Unknown qualifier '" + qual + "'. Valid: "
                             + String.join(", ", QUALIFIERS.keySet()));
                    continue;
                }
                // Otherwise the colon is part of the text (a schema-qualified name, say) — keep the
                // whole token as a bare term.
            }
            if (value.isEmpty()) {
                errs.add("Qualifier '" + tok.substring(0, Math.max(colon, 0)) + ":' has no value");
                continue;
            }
            boolean prefix = value.endsWith("*");
            if (prefix) value = value.substring(0, value.length() - 1);
            if (value.isEmpty()) { errs.add("'*' needs something before it"); continue; }
            out.add(new Term(field, value.toUpperCase(Locale.ROOT), negated, prefix));
        }
        return new SearchQuery(q, out, errs);
    }

    /**
     * Whether an unrecognised {@code word:} prefix should be reported rather than searched.
     *
     * <p>Only bare identifiers qualify. {@code ANALYZERDB.ERP_ORDER:5} and a URL should be treated
     * as text, not diagnosed as a bad qualifier — the point is to catch {@code tabel:}, not to
     * reject anything containing a colon.
     */
    private static boolean looksLikeQualifier(String s) {
        if (s.isEmpty() || s.length() > 12) return false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (!Character.isLetter(c)) return false;
        }
        return true;
    }

    /** Splits on whitespace, honouring double quotes so a phrase stays one term. */
    private static List<String> tokenize(String q) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuote = false;
        for (int i = 0; i < q.length(); i++) {
            char c = q.charAt(i);
            if (c == '"') { inQuote = !inQuote; continue; }
            if (!inQuote && Character.isWhitespace(c)) {
                if (cur.length() > 0) { out.add(cur.toString()); cur.setLength(0); }
            } else {
                cur.append(c);
            }
        }
        if (cur.length() > 0) out.add(cur.toString());
        return out;
    }

    /**
     * The query as the engine understood it, for echoing back to the user.
     *
     * <p>Shown as removable chips in the UI. A search that silently reinterprets what someone typed
     * and then reports "no results" gives them no way to tell a real absence from a misunderstood
     * query.
     */
    public List<String> echo() {
        List<String> out = new ArrayList<>();
        for (Term t : terms) {
            out.add((t.negated() ? "NOT " : "")
                  + (t.isBare() ? "any" : labelFor(t.field()))
                  + " " + (t.prefix() ? t.value() + "*" : t.value()));
        }
        return out;
    }

    private static String labelFor(String field) {
        for (var e : QUALIFIERS.entrySet()) if (e.getValue().equals(field)) return e.getKey();
        return field;
    }

    /** The distinct fields constrained, used to describe what was searched. */
    public Set<String> fields() {
        Set<String> f = new LinkedHashSet<>();
        for (Term t : terms) if (!t.isBare()) f.add(t.field());
        return f;
    }
}
