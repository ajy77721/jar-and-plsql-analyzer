package com.codeanalyzer.ingest.mongo.id;

import org.bson.Document;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A stable hash of an entity's <em>content</em>, ignoring anything that varies between runs.
 *
 * <h2>What this buys — measured, not hoped for</h2>
 *
 * <p>Every run currently writes a complete new copy of everything. The archived runs on disk are a
 * natural experiment: re-running an unchanged schema produces byte-identical output for three of
 * five dialects. So most of what a re-run writes is content that already exists.
 *
 * <p>Content-addressing turns that into storage: an entity whose hash is unchanged is written once
 * and joined to each run by a membership row. At ten runs that is roughly 95 GB of index instead
 * of 654 GB — the single largest lever in this design.
 *
 * <h2>Which fields are excluded, and why the direction of error matters</h2>
 *
 * <p>A hash over the whole document would change on every run, because the document carries the
 * run's own identifiers. Those describe <em>this analysis</em>, not the entity, so they are left
 * out. Getting the exclusion wrong fails silently in both directions: include too much and nothing
 * ever dedups, exclude too much and two genuinely different entities collide so one overwrites the
 * other. The second is far worse — it is data loss wearing the appearance of a saving.
 *
 * <h2>Canonical ordering</h2>
 *
 * <p>Keys are sorted, so field insertion order cannot change the hash — otherwise dedup would
 * depend on the accident of how a mapper happened to build the document. Arrays keep their given
 * order, because order is meaningful here: INTO targets correspond to select items by position.
 */
public final class ContentHasher {

    /**
     * Field and record separators.
     *
     * <p>Non-printing control characters, chosen because they cannot occur inside any value in
     * this corpus. A printable delimiter could appear in a SQL string literal or a Java
     * descriptor, and two different documents could then serialise to the same byte sequence —
     * a hash collision manufactured by the encoding rather than by the data.
     */
    private static final char FS = '\u001F';
    private static final char RS = '\u001E';

    /**
     * Fields excluded from the hash: each is a property of the run rather than of the entity.
     * {@code _id} is derived from the hash, so including it would be circular.
     */
    public static final Set<String> RUN_SCOPED = Set.of(
            "_id", "key", "_key", "runId", "runRef", "parentRef", "nodeRef", "objectRef",
            "entityKey", "contentHash", "ingestedAt", "version");

    private ContentHasher() {}

    /**
     * Hex SHA-256 of a <b>source</b> JSON node — the form to use when arrays are split out.
     *
     * <p>Hashing the flattened document instead is a correctness bug, and a quiet one. The splitter
     * removes every array before the document is written, so two methods identical in their
     * scalars but different in their bodies — different {@code parameters},
     * {@code localVariables}, {@code stringLiterals} — produce the same hash, share an {@code _id},
     * and one overwrites the other. Measured: it merged 2 of 33,409 methods and lost 12 invocation
     * references with them. Nothing failed; the collection was simply two rows short.
     *
     * <p>So content identity is taken from the whole source element, arrays included, before any
     * of it is moved elsewhere. Where the data physically lands is a storage decision; what the
     * entity <em>is</em> must not depend on it.
     */
    public static String hashJson(com.fasterxml.jackson.databind.JsonNode node) {
        StringBuilder sb = new StringBuilder();
        canonicaliseJson(node, sb);
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(sb.toString().getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static void canonicaliseJson(com.fasterxml.jackson.databind.JsonNode n, StringBuilder sb) {
        if (n == null || n.isNull()) { sb.append("\\0"); return; }
        if (n.isObject()) {
            List<String> keys = new ArrayList<>();
            n.fieldNames().forEachRemaining(keys::add);
            keys.removeIf(RUN_SCOPED::contains);
            java.util.Collections.sort(keys);
            sb.append('{');
            for (String k : keys) {
                sb.append(k).append(FS);
                canonicaliseJson(n.get(k), sb);
                sb.append(RS);
            }
            sb.append('}');
            return;
        }
        if (n.isArray()) {
            sb.append('[');
            n.forEach(e -> { canonicaliseJson(e, sb); sb.append(RS); });
            sb.append(']');
            return;
        }
        sb.append(n.getNodeType()).append(':').append(n.asText());
    }

    /** Combines an entity's identity with its content hash into one content id. */
    public static String hashScoped(String entityKey, String contentHash) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest((entityKey + FS + contentHash).getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    /** Hex SHA-256 of the entity's content, run-scoped fields removed. */
    public static String hash(Document doc) {
        StringBuilder sb = new StringBuilder();
        canonicalise(doc, sb);
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(sb.toString().getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    @SuppressWarnings("unchecked")
    private static void canonicalise(Object value, StringBuilder sb) {
        if (value == null) {
            sb.append("\\0");
        } else if (value instanceof Document d) {
            List<String> keys = new ArrayList<>(d.keySet());
            keys.removeIf(RUN_SCOPED::contains);
            java.util.Collections.sort(keys);
            sb.append('{');
            for (String k : keys) {
                sb.append(k).append(FS);
                canonicalise(d.get(k), sb);
                sb.append(RS);
            }
            sb.append('}');
        } else if (value instanceof List<?> l) {
            sb.append('[');
            for (Object o : l) {
                canonicalise(o, sb);
                sb.append(RS);
            }
            sb.append(']');
        } else if (value instanceof Map<?, ?> m) {
            canonicalise(new Document((Map<String, Object>) m), sb);
        } else {
            // The type is part of the identity: the string "1" and the number 1 differ in content.
            sb.append(value.getClass().getSimpleName()).append(':').append(value);
        }
    }
}
