package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.map.IngestContext;
import com.codeanalyzer.ingest.mongo.map.plsql.NodeDetailMapper;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A cursor table with no schema in its node gets no reference, and keeps its name.
 *
 * <h2>Why this is a separate class from {@link CursorResolutionTest}</h2>
 *
 * <p>{@code CursorResolutionTest} asserts the same property over the real corpus, and <b>it cannot
 * fail</b>. Calibration proved it: changing the writer to guess a schema —
 * {@code schemas.getOrDefault(name, "ANALYZER")}, the exact mis-attribution ING-014 forbids — left
 * all six of its tests passing. Every one of the 1,103 cursor tables in that corpus resolves, so
 * the branch that guesses is never taken and a test that iterates stored rows can never observe it.
 *
 * <p>That is the tenth guard in this project to pass its first calibration while guarding nothing,
 * and the pattern is always the same: <i>testing that a rule is correct is not testing that
 * anything consults it</i>. Here the rule is only consulted on input the corpus does not contain.
 *
 * <p>So this class supplies the input instead. {@code DocSink} is a functional interface for
 * exactly this reason, and no MongoDB is needed — which also means this runs in milliseconds and
 * cannot be skipped on a machine with no server.
 */
class CursorSchemaAttributionTest {

    /** A node with two cursor tables: one declared in {@code directTables}, one not. */
    private static final String NODE = """
            {
              "nodeId": "ERP_PKG.DO_WORK",
              "directTables": [
                {"schema": "ANALYZER", "name": "KNOWN_TABLE", "objectType": "TABLE"}
              ],
              "cursors": [
                {"name": "c_known", "line": 10, "tables": ["KNOWN_TABLE"]},
                {"name": "c_unknown", "line": 20, "tables": ["MYSTERY_TABLE"]}
              ]
            }
            """;

    private static List<Document> mapAndCollect(String json, String collection) throws Exception {
        List<Document> out = new ArrayList<>();
        IngestContext ctx = new IngestContext(
                "ORACLE~ORACLEDEMO~LOCAL~ERP_USP_RUN_CYCLE~v1", "ORACLE");
        new NodeDetailMapper().map(ctx,
                new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)),
                (coll, doc) -> {
                    if (collection.equals(coll)) out.add(doc);
                });
        return out;
    }

    @Test
    void aTableTheNodeDeclaresGetsAReference() throws Exception {
        List<Document> rows = mapAndCollect(NODE, Collections.CURSOR_OBJECT);
        Document known = rows.stream()
                .filter(d -> "KNOWN_TABLE".equals(d.getString("tableName")))
                .findFirst().orElse(null);
        assertNotNull(known, "the resolvable cursor table produced no row at all: " + rows);
        assertEquals("ANALYZER", known.getString("schemaName"));
        assertNotNull(known.getString("objectRef"),
                "a table declared in the same node got no object reference");
        assertTrue(known.getString("objectRef").contains("KNOWN_TABLE"));
    }

    @Test
    void aTableTheNodeDoesNotDeclareIsPreservedWithoutAReference() throws Exception {
        List<Document> rows = mapAndCollect(NODE, Collections.CURSOR_OBJECT);
        Document mystery = rows.stream()
                .filter(d -> "MYSTERY_TABLE".equals(d.getString("tableName")))
                .findFirst().orElse(null);

        // Preserved: the name is source data and must survive whether or not it resolves.
        assertNotNull(mystery,
                "the unresolvable cursor table was dropped instead of preserved — ING-014 asks "
                        + "for the opposite");
        // Not mis-attributed: no schema was invented for it, and so no reference either.
        assertEquals(null, mystery.getString("objectRef"),
                "a schema was guessed for a table the node never declared, which attaches the "
                        + "cursor to an object that may not be the one it reads");
        assertFalse(mystery.containsKey("schemaName"),
                "the row carries a schema that came from nowhere in the source");
    }

    @Test
    void bothCursorsStillProducedTheirOwnRows() throws Exception {
        // Guards against the fix that would make the test above pass trivially: dropping the
        // unresolvable row, or collapsing the two cursors into one.
        assertEquals(2, mapAndCollect(NODE, Collections.CURSOR_OBJECT).size(),
                "expected one row per declared cursor table");
        assertEquals(2, mapAndCollect(NODE, Collections.CURSOR_DECL).size(),
                "expected one row per cursor");
    }

    @Test
    void theCursorRowNoLongerCarriesItsTablesInline() throws Exception {
        for (Document c : mapAndCollect(NODE, Collections.CURSOR_DECL)) {
            assertFalse(c.containsKey("tables"),
                    "the cursor document still holds its tables array, so the same fact is "
                            + "stored in two shapes");
        }
    }
}
