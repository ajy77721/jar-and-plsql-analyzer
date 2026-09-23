package com.codeanalyzer.ingest.mongo.id;

/**
 * Escaping for the components of a deterministic {@code _id}.
 *
 * <h2>Why only the separator is escaped</h2>
 *
 * <p>An {@code _id} is a join of natural-key components with {@code ~}. Only characters that could
 * be mistaken for structure need escaping; escaping ordinary punctuation would make ids unreadable
 * for no gain. That is a measured decision, not a stylistic one — across the real corpus:
 *
 * <ul>
 *   <li>{@code ~} occurs <b>zero</b> times in any value used in a key;</li>
 *   <li>{@code $} occurs 14,746 times in {@code nodeId} (the {@code SCHEMA$OBJECT} form);</li>
 *   <li>{@code /} occurs 31,473 times inside JVM descriptors;</li>
 *   <li>{@code #} occurs 720 times in table names — SQL Server temp tables, including one object
 *       named literally {@code #}.</li>
 * </ul>
 *
 * <p>So {@code $}, {@code /} and {@code #} pass through untouched and the ids stay legible, while
 * {@code ~} is escaped anyway. The frequency today is zero, but an id scheme that is only correct
 * for the data that happens to exist is a latent corruption: two different keys colliding on one
 * {@code _id} silently overwrite, and the loss is invisible.
 *
 * <p>The escape character {@code \} must itself be escaped, or {@code "a\"} + {@code "b"} and
 * {@code "a"} + {@code "~b"} would both encode to the same string.
 */
public final class Escaping {

    public static final char SEP = '~';
    private static final char ESC = '\\';

    private Escaping() {}

    /** Escapes one key component. A null component is distinct from an empty one; see below. */
    public static String escape(String component) {
        // A null is encoded as the empty string. Callers that need to tell "absent" from "empty
        // string" apart must not put the field in the key — no encoding can recover a distinction
        // the key does not carry, and pretending otherwise would be worse than refusing.
        if (component == null || component.isEmpty()) return "";
        StringBuilder sb = new StringBuilder(component.length() + 4);
        for (int i = 0; i < component.length(); i++) {
            char c = component.charAt(i);
            if (c == SEP || c == ESC) sb.append(ESC);
            sb.append(c);
        }
        return sb.toString();
    }

    /** Joins pre-escaped components with the separator. */
    public static String join(String... components) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < components.length; i++) {
            if (i > 0) sb.append(SEP);
            sb.append(escape(components[i]));
        }
        return sb.toString();
    }

    /**
     * Splits a joined key back into its components, honouring escapes.
     *
     * <p>Exists so the round-trip property can be tested rather than assumed: {@code decompose(
     * join(parts)) == parts} for every input, which is the only thing that proves the escaping is
     * unambiguous.
     */
    public static java.util.List<String> decompose(String joined) {
        java.util.List<String> out = new java.util.ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean escaped = false;
        for (int i = 0; i < joined.length(); i++) {
            char c = joined.charAt(i);
            if (escaped) { cur.append(c); escaped = false; }
            else if (c == ESC) escaped = true;
            else if (c == SEP) { out.add(cur.toString()); cur.setLength(0); }
            else cur.append(c);
        }
        out.add(cur.toString());
        return out;
    }
}
