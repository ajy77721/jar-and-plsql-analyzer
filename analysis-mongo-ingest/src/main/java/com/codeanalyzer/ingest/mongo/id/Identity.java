package com.codeanalyzer.ingest.mongo.id;

import org.bson.Document;

/**
 * The three identities every stored entity carries, and the reason there are three.
 *
 * <p>One identifier cannot answer three different questions, and conflating them is what made the
 * first version of this module a per-run snapshot store rather than an entity store:
 *
 * <dl>
 *   <dt>{@code _id} — <b>content identity</b></dt>
 *   <dd>Derived from {@link ContentHasher}. Two runs that produce identical content produce the
 *       same {@code _id}, so the second run writes nothing new. This is the dedup lever: measured
 *       on the archived runs, three of five dialects re-run byte-identically.</dd>
 *
 *   <dt>{@code entityKey} — <b>continuity across runs</b></dt>
 *   <dd>Run-independent: {@code ORACLE~ANALYZER~ERP_PRODUCT} is the same table in every run and
 *       every version. This is what makes "how did this table change between runs?" an indexed
 *       lookup rather than a scan of two whole runs diffed in application code. Without it, runs
 *       are islands.</dd>
 *
 *   <dt>{@code key} — <b>occurrence within one run</b></dt>
 *   <dd>Run-scoped and unique: {@code runId ~ nodeId}. The same {@code nodeId} recurs across runs,
 *       and the same node recurs many times <em>within</em> a run through recursion — one measured
 *       node appears 2,534 times as a call-edge endpoint. So "which node is this" and "which
 *       appearance of it is this" are genuinely different questions and need different keys.</dd>
 * </dl>
 *
 * <p>References between documents use {@code entityKey} or {@code key} — never {@code _id}. That
 * is deliberate: a content hash changes when the content changes, so a reference to it would break
 * on every edit. A reference must point at <em>the thing</em>, not at one version of it.
 */
public record Identity(String id, String entityKey, String key) {

    /**
     * Builds all three from a document plus its keys.
     *
     * @param entityKey run-independent identity of the thing
     * @param key       run-scoped identity of this occurrence
     * @param content   the document whose content determines {@code _id}
     */
    public static Identity of(String entityKey, String key, Document content) {
        // Scoped by the entity key, exactly as ofSource is. Leaving this unscoped was a real bug:
        // `parentRef` is excluded from the content hash (it is a reference, not content), so two
        // DAG edges from different parents to the same child at the same ordinal hashed
        // identically and one overwrote the other — 2,289 of 7,281 edges vanished with no error.
        //
        // The rule is the same either way: content says which VERSION, the key says which THING.
        // An id built from only one of them cannot be right.
        return new Identity(
                ContentHasher.hashScoped(entityKey, ContentHasher.hash(content)), entityKey, key);
    }

    /**
     * Identity whose content hash comes from the <b>source</b> element rather than the stored one.
     *
     * <p>Required wherever the splitter has moved arrays into other collections: the stored
     * document no longer contains everything that distinguishes this entity from another, so
     * hashing it would merge entities that differ only in their split-out children.
     */
    public static Identity ofSource(String entityKey, String key,
                                    com.fasterxml.jackson.databind.JsonNode source) {
        // The entity key participates in the hash, and it has to.
        //
        // Hashing the source element alone merges things that are genuinely different: a method's
        // JSON does not name its class, so `toString()` in one class and `toString()` in another
        // are byte-identical and collapse into a single document. Measured, that lost 1,849 of
        // 33,409 methods — and it looked like successful deduplication.
        //
        // The key supplies what the element omits: WHICH thing this is. The content supplies WHICH
        // VERSION of it. Both are needed, and only together do they mean "this entity, unchanged",
        // which is the condition deduplication is allowed to act on. This is why the entity key
        // must be run-independent — if the run were part of it, nothing would ever dedup.
        return new Identity(
                ContentHasher.hashScoped(entityKey, ContentHasher.hashJson(source)), entityKey, key);
    }

    /**
     * Stamps an entity document with its content identity.
     *
     * <p>Deliberately does <b>not</b> write the run-scoped {@code key}. An entity document is
     * shared by every run whose content matches, so a single run's key has no place on it — that
     * was a real error in the first version: writing it produced duplicate-key failures the moment
     * two occurrences shared content, which is precisely the case the design exists to exploit.
     * Occurrence lives in {@code am_run_member}; the entity holds only what is true of the entity.
     */
    public Document stampOnto(Document doc) {
        doc.put("_id", id);
        doc.put("entityKey", entityKey);
        doc.put("contentHash", id);
        return doc;
    }
}
