package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.id.ContentHasher;
import com.codeanalyzer.ingest.mongo.id.Identity;
import com.codeanalyzer.ingest.mongo.model.RunMember;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DB-003/004/005 — the three identities, and the deduplication they enable.
 *
 * <p>These properties are the whole basis of the storage saving (measured: ~95 GB of index at ten
 * runs instead of ~654 GB). Each one fails silently if broken — nothing errors, storage simply
 * stays ten times larger, or two different entities quietly become one. That is exactly the kind
 * of defect that needs a test rather than a review.
 */
class IdentityAndDedupTest {

    private Document node(String name, int lines) {
        return new Document("nodeId", "ANALYZER$" + name)
                .append("name", name)
                .append("linesOfCode", lines)
                .append("objectType", "PROCEDURE");
    }

    @Test
    @DisplayName("identical content in two runs yields the same _id — this is the dedup lever")
    void identicalContentDedups() {
        Document a = node("ERP_RUN", 120);
        Document b = node("ERP_RUN", 120);
        a.put("runId", "ORACLE~P~L~E~v1");     // run-scoped: must not affect the hash
        b.put("runId", "ORACLE~P~L~E~v2");
        assertEquals(ContentHasher.hash(a), ContentHasher.hash(b),
                "the same procedure in two runs must hash identically, or nothing ever dedups");
    }

    @Test
    @DisplayName("changed content yields a different _id")
    void changedContentDiffers() {
        assertNotEquals(ContentHasher.hash(node("ERP_RUN", 120)),
                        ContentHasher.hash(node("ERP_RUN", 121)),
                "a real change must produce a new content id, or an edit would be invisible");
    }

    @Test
    @DisplayName("field insertion order does not change the hash")
    void keyOrderIsCanonical() {
        Document a = new Document("x", 1).append("y", 2);
        Document b = new Document("y", 2).append("x", 1);
        assertEquals(ContentHasher.hash(a), ContentHasher.hash(b),
                "dedup must not depend on the accident of how a mapper built the document");
    }

    @Test
    @DisplayName("array order DOES change the hash — order is meaningful here")
    void arrayOrderIsSignificant() {
        // INTO targets correspond to select items by position, so treating arrays as unordered
        // would merge two genuinely different statements into one.
        Document a = new Document("into", List.of("v_a", "v_b"));
        Document b = new Document("into", List.of("v_b", "v_a"));
        assertNotEquals(ContentHasher.hash(a), ContentHasher.hash(b));
    }

    @Test
    @DisplayName("a string and a number with the same text are different content")
    void typeIsPartOfIdentity() {
        assertNotEquals(ContentHasher.hash(new Document("v", "1")),
                        ContentHasher.hash(new Document("v", 1)));
    }

    @Test
    @DisplayName("an entity in three runs is one entity document and three membership rows")
    void membershipModel() {
        Document content = node("ERP_RUN", 120);
        Identity id = Identity.of("ORACLE~ANALYZER~ERP_RUN", "ORACLE~P~L~E~v1~ERP_RUN", content);

        var members = List.of(
                RunMember.of("ORACLE~P~L~E~v1", id, "am_proc_node", null),
                RunMember.of("ORACLE~P~L~E~v2", id, "am_proc_node", null),
                RunMember.of("ORACLE~P~L~E~v3", id, "am_proc_node", null));

        assertEquals(3, members.stream().map(m -> m.getString("_id")).distinct().count(),
                "each run needs its own membership row");
        assertEquals(1, members.stream().map(m -> m.getString("contentHash")).distinct().count(),
                "all three point at ONE entity document — that is the saving");
    }

    @Test
    @DisplayName("references use entityKey, not the content hash")
    void refsSurviveContentChange() {
        // A content hash changes when content changes. If children referenced it, every edit would
        // orphan them. The entityKey is stable across versions, which is why refs point at it.
        Identity v1 = Identity.of("ORACLE~ANALYZER~ERP_RUN", "run1~ERP_RUN", node("ERP_RUN", 120));
        Identity v2 = Identity.of("ORACLE~ANALYZER~ERP_RUN", "run2~ERP_RUN", node("ERP_RUN", 999));
        assertNotEquals(v1.id(), v2.id(), "content changed, so the content id must change");
        assertEquals(v1.entityKey(), v2.entityKey(), "but it is still the same procedure");
    }
}
