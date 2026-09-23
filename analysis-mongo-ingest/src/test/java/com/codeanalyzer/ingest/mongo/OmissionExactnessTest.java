package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.verify.RoundTripProver;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * VER-005 — the round-trip prover's "omitted by design" exemption is exact.
 *
 * <h2>What this is proving, and why it is not the same as VER-004</h2>
 *
 * <p>{@link RoundTripProver#compare} treats {@code null} and empty containers as omitted by
 * design: a source field with one of those values is allowed to be absent from the stored
 * document, because that is what every mapper in this module does on purpose (Rule 1,
 * {@code RELATIONAL-MODEL-PLAN.md} §17.1). VER-004 proves the round trip holds on <b>real
 * documents</b>, which only ever exercises the exemption on whatever happens to be null or empty
 * in the fixture. It cannot prove the exemption's <em>boundary</em> — that a field just outside it
 * is not swept in by the same code path.
 *
 * <p>This is that proof, direct on the pure function rather than through a database: every value
 * on the documented omitted side is accepted absent, and every value one step outside it — a
 * non-empty container, a zero, an empty string, a {@code false} — is not.
 *
 * <h2>Why {@code false} is tested as a <b>negative</b> case</h2>
 *
 * <p>{@code RELATIONAL-MODEL-PLAN.md} §17.1 documents Rule 1 as "omit null/false/empty keys on
 * write" and measures the byte savings of doing so. Reading the actual write path —
 * {@code Splitter.flatten} and {@code JsonDocs.toValue} — neither omits a boolean {@code false}:
 * {@code n.isBoolean()} returns {@code n.booleanValue()}, a non-null {@code Boolean}, which is
 * written like any other scalar. So today a {@code false} field that goes missing from storage is
 * a real loss, not a documented omission, and the prover is correct to flag it as one.
 *
 * <p>If Rule 1's boolean clause is implemented later, it changes the wire format for every boolean
 * field across every collection — present-false becomes absent — and every reader that does
 * {@code doc.getBoolean(key)} or {@code doc.containsKey(key)} needs to treat "absent" and "false"
 * as the same fact. That is a decision with a blast radius, not a one-line fix, so it is not made
 * here. This test pins <b>today's</b> behaviour and is the place a future implementer updates it,
 * rather than a place that quietly already assumed the change.
 */
class OmissionExactnessTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private static JsonNode node(String json) throws Exception {
        return JSON.readTree(json);
    }

    @Test
    void aNullFieldMayBeAbsent() throws Exception {
        JsonNode source = node("""
                {"a": "kept", "b": null}
                """);
        Document stored = new Document("a", "kept");   // b omitted, as Rule 1 asks
        assertEquals(List.of(), RoundTripProver.compare(source, stored),
                "a null source field that is absent from storage was reported as lost");
    }

    @Test
    void anEmptyObjectMayBeAbsent() throws Exception {
        JsonNode source = node("""
                {"a": "kept", "b": {}}
                """);
        Document stored = new Document("a", "kept");
        assertEquals(List.of(), RoundTripProver.compare(source, stored),
                "an empty object that is absent from storage was reported as lost");
    }

    @Test
    void anEmptyArrayMayBeAbsent() throws Exception {
        JsonNode source = node("""
                {"a": "kept", "b": []}
                """);
        Document stored = new Document("a", "kept");
        assertEquals(List.of(), RoundTripProver.compare(source, stored),
                "an empty array that is absent from storage was reported as lost");
    }

    @Test
    void aNonEmptyObjectDroppedEntirelyIsCaught() throws Exception {
        // The boundary one step past "empty object". If this passed, the exemption would be
        // "any object", not "empty objects" — the gap VER-005 exists to close.
        JsonNode source = node("""
                {"a": "kept", "b": {"x": 1}}
                """);
        Document stored = new Document("a", "kept");
        List<RoundTripProver.Loss> losses = RoundTripProver.compare(source, stored);
        assertEquals(1, losses.size(), "a non-empty object silently dropped was not caught");
        assertEquals("b", losses.get(0).path());
    }

    @Test
    void aNonEmptyArrayDroppedEntirelyIsCaught() throws Exception {
        JsonNode source = node("""
                {"a": "kept", "b": [1, 2, 3]}
                """);
        Document stored = new Document("a", "kept");
        List<RoundTripProver.Loss> losses = RoundTripProver.compare(source, stored);
        assertEquals(1, losses.size(), "a non-empty array silently dropped was not caught");
        assertEquals("b", losses.get(0).path());
    }

    @Test
    void anEmptyStringIsNotTreatedAsOmittedByDesign() throws Exception {
        // "" is not null and not an empty container — one of the two things this rule actually
        // exempts. A prover that also let empty strings vanish would be exempting more than Rule 1
        // documents, in the direction that hides real loss.
        JsonNode source = node("""
                {"a": ""}
                """);
        Document stored = new Document();   // dropped
        List<RoundTripProver.Loss> losses = RoundTripProver.compare(source, stored);
        assertEquals(1, losses.size(), "an empty string silently dropped was not caught");
        assertEquals("a", losses.get(0).path());
    }

    @Test
    void aZeroIsNotTreatedAsOmittedByDesign() throws Exception {
        // The scalar analogue of the same risk: 0 is falsy in a way that invites the same "empty
        // means omit" shortcut a maintainer might reach for.
        JsonNode source = node("""
                {"a": 0}
                """);
        Document stored = new Document();
        List<RoundTripProver.Loss> losses = RoundTripProver.compare(source, stored);
        assertEquals(1, losses.size(), "a zero silently dropped was not caught");
        assertEquals("a", losses.get(0).path());
    }

    @Test
    void aFalseFieldDroppedIsCaught_becauseTheWritePathDoesNotOmitFalseToday() throws Exception {
        // See the class javadoc. Rule 1's boolean clause is documented and not implemented, so a
        // false field going missing is a real loss under today's write path, and must be reported
        // as one. If this test starts failing because the write path changed to omit false, that
        // change needs the wire-format decision described above — this test should not be
        // "fixed" by loosening the assertion without that decision being made first.
        JsonNode source = node("""
                {"a": "kept", "b": false}
                """);
        Document stored = new Document("a", "kept");
        List<RoundTripProver.Loss> losses = RoundTripProver.compare(source, stored);
        assertEquals(1, losses.size(),
                "a false field dropped from storage was not reported as a loss, but the write "
                        + "path does not omit false today — either the write path changed and "
                        + "this test needs the wire-format decision from its own javadoc, or the "
                        + "prover started exempting a value it should not");
        assertEquals("b", losses.get(0).path());
    }

    @Test
    void aTrueFieldMustMatchExactly() throws Exception {
        JsonNode source = node("""
                {"a": true}
                """);
        assertEquals(List.of(), RoundTripProver.compare(source, new Document("a", true)));
        assertEquals(1, RoundTripProver.compare(source, new Document("a", false)).size(),
                "true stored as false was not caught");
        assertEquals(1, RoundTripProver.compare(source, new Document()).size(),
                "a true field dropped from storage was not caught");
    }

    @Test
    void theWritePathToday_doesNotActuallyOmitFalse() throws Exception {
        // Corroborates the javadoc's claim about Splitter/JsonDocs with the real classes, so this
        // test breaks the day someone implements Rule 1's boolean clause — which is exactly when
        // the two tests above need to be revisited, and a broken test is a better prompt for that
        // than a comment nobody re-reads.
        com.codeanalyzer.ingest.mongo.map.IngestContext ctx =
                new com.codeanalyzer.ingest.mongo.map.IngestContext("run~1", "ORACLE");
        java.util.List<Document> emitted = new java.util.ArrayList<>();
        JsonNode source = node("""
                {"flag": false, "name": "x"}
                """);
        Document out = com.codeanalyzer.ingest.mongo.map.Splitter.flatten(
                ctx, (coll, doc) -> emitted.add(doc), "am_probe", "run~1~x", source);
        assertTrue(out.containsKey("flag"),
                "Splitter.flatten now omits false — the OmissionExactnessTest javadoc and "
                        + "aFalseFieldDroppedIsCaught are stale and need the wire-format decision "
                        + "they describe, not a quiet update");
        assertEquals(false, out.get("flag"));
    }
}
