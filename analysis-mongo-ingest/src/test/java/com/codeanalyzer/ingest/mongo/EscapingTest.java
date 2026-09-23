package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.id.Escaping;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.random.RandomGenerator;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The escaping must be unambiguous, because the failure mode is silent.
 *
 * <p>If two different key tuples can encode to the same string, their documents share an
 * {@code _id} and one upsert overwrites the other. Nothing errors; a row is simply gone. So the
 * property tested here is the only one that matters: <b>decompose(join(parts)) == parts</b>, for
 * every input including the awkward ones.
 */
class EscapingTest {

    @Test
    @DisplayName("round-trips the punctuation that actually occurs in this corpus")
    void realWorldPunctuation() {
        // Measured frequencies: $ 14,746 in nodeId, / 31,473 in descriptors, # 720 in table names.
        List<List<String>> cases = List.of(
                List.of("ANALYZER$ERP_USP_RUN_CYCLE", "SELECT", "61"),
                List.of("(Lcom/plsql/Parser;I)V", "method"),
                List.of("#STAGE6", "#"),
                List.of("ERP.PRODUCT", "COL.NAME"),
                List.of("", "empty-first"),
                List.of("trailing", ""));
        for (List<String> parts : cases) {
            String joined = Escaping.join(parts.toArray(new String[0]));
            assertEquals(parts, Escaping.decompose(joined), "round-trip failed for " + parts);
        }
    }

    @Test
    @DisplayName("a value containing the separator cannot collide with a component boundary")
    void separatorInsideValue() {
        // ~ occurs zero times in the corpus today. Relying on that would make the scheme correct
        // only for the data that happens to exist, and the collision it permits is undetectable.
        String a = Escaping.join("a~b", "c");
        String b = Escaping.join("a", "b~c");
        assertNotEquals(a, b, "two distinct keys encoded identically — silent overwrite");
        assertEquals(List.of("a~b", "c"), Escaping.decompose(a));
        assertEquals(List.of("a", "b~c"), Escaping.decompose(b));
    }

    @Test
    @DisplayName("the escape character itself is escaped")
    void backslashIsEscaped() {
        assertNotEquals(Escaping.join("a\\", "b"), Escaping.join("a", "~b"));
        assertEquals(List.of("a\\", "b"), Escaping.decompose(Escaping.join("a\\", "b")));
    }

    @Test
    @DisplayName("randomised: no input breaks the round trip")
    void propertyRoundTrip() {
        RandomGenerator rnd = RandomGenerator.getDefault();
        String alphabet = "ab~\\$#/.$ ";
        for (int i = 0; i < 5000; i++) {
            int n = 1 + rnd.nextInt(4);
            String[] parts = new String[n];
            for (int p = 0; p < n; p++) {
                int len = rnd.nextInt(6);
                StringBuilder sb = new StringBuilder();
                for (int c = 0; c < len; c++) sb.append(alphabet.charAt(rnd.nextInt(alphabet.length())));
                parts[p] = sb.toString();
            }
            assertEquals(List.of(parts), Escaping.decompose(Escaping.join(parts)));
        }
    }

    @Test
    @DisplayName("a nested id keeps the parent readable and still cannot collide")
    void nestedIdsAreReadableAndUnambiguous() {
        var run = com.codeanalyzer.ingest.mongo.id.IdCodec
                .run("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE", 1);
        var obj = com.codeanalyzer.ingest.mongo.id.IdCodec
                .dbObject(run.id(), "ANALYZER", "ERP_SALES_ORDER");

        assertFalse(obj.id().contains("\\~"),
                "the parent's separators were escaped again: " + obj.id());
        assertTrue(obj.id().startsWith(run.id()),
                "a child id must begin with its parent's id, so the chain is readable: " + obj.id());
        // 7, not 6: the run id is dialect~project~env~entryPoint~vN — the version is its own
        // component. Getting this wrong in the assertion is exactly why arity is worth pinning:
        // the safety argument for unescaped nesting rests on it being fixed and known.
        assertEquals(7, Escaping.decompose(obj.id()).size(),
                "arity is fixed per collection — 5 run components plus schema and name");

        // Components containing the separator are still escaped, so nesting has not reintroduced
        // the ambiguity the escaping exists to prevent.
        var odd = com.codeanalyzer.ingest.mongo.id.IdCodec.dbObject(run.id(), "A~B", "C");
        var odd2 = com.codeanalyzer.ingest.mongo.id.IdCodec.dbObject(run.id(), "A", "B~C");
        assertNotEquals(odd.id(), odd2.id(), "two distinct keys encoded identically");
    }
}
