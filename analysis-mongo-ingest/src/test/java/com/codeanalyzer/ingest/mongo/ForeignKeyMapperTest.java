package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.map.IngestContext;
import com.codeanalyzer.ingest.mongo.map.plsql.NodeDetailMapper;
import com.codeanalyzer.ingest.mongo.schema.Catalog;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Declared FOREIGN KEY constraints on {@code api/nodes/*.json} must reach
 * {@link Collections#FOREIGN_KEY} — one row per constraint, with BOTH column lists intact.
 *
 * <p>The column lists are the part worth testing. A foreign key stripped of them degrades into
 * "these two tables are related", which the analyzer already infers from DML; the pairing is the
 * only thing the constraint adds. {@code Splitter.flatten} would have taken both arrays out into
 * child collections, so a row that still carries them is a real assertion, not a tautology.
 *
 * <p>Mirrors {@code PackageVarRefMapperTest}: the real {@link NodeDetailMapper} against a
 * synthetic node document with an in-memory sink — no mocks, no MongoDB instance.
 */
class ForeignKeyMapperTest {

    private static final String NODE_JSON = """
            {
              "nodeId": "alter_orders",
              "schema": "SALES",
              "objectName": "alter_orders",
              "objectType": "ALTER_TABLE",
              "foreignKeys": [
                {
                  "constraintName": "fk_customer",
                  "schema": "SALES",
                  "tableName": "orders",
                  "columns": ["customer_id"],
                  "referencedSchema": "SALES",
                  "referencedTable": "customers",
                  "referencedColumns": ["id"],
                  "line": 12
                },
                {
                  "constraintName": null,
                  "schema": null,
                  "tableName": "order_lines",
                  "columns": ["order_id", "line_no"],
                  "referencedSchema": null,
                  "referencedTable": "orders",
                  "referencedColumns": ["id", "line_no"],
                  "line": 20
                }
              ]
            }
            """;

    private static List<Document> mapAndCollect(String nodeJson) throws Exception {
        IngestContext ctx = new IngestContext("MYSQL~MYSQLDEMO~LOCAL~TESTRUN~v1", "MYSQL");
        List<Document> rows = new ArrayList<>();
        new NodeDetailMapper().map(ctx,
                new ByteArrayInputStream(nodeJson.getBytes(StandardCharsets.UTF_8)),
                (coll, doc) -> {
                    if (Collections.FOREIGN_KEY.equals(coll)) rows.add(doc);
                });
        return rows;
    }

    @Test
    void declaredForeignKeysReachTheirOwnCollection() throws Exception {
        List<Document> rows = mapAndCollect(NODE_JSON);

        assertEquals(2, rows.size(), "expected one am_foreign_key row per constraint, got: " + rows);

        Document named = rows.get(0);
        assertEquals("fk_customer", named.getString("constraintName"));
        assertEquals("SALES", named.getString("schemaName"));
        assertEquals("orders", named.getString("tableName"));
        assertEquals("SALES", named.getString("referencedSchema"));
        assertEquals("customers", named.getString("referencedTable"));
        assertEquals(12, named.getInteger("line"));
        assertNotNull(named.getString("nodeRef"), "row must point back at the node that declared it");
        assertEquals("MYSQL~MYSQLDEMO~LOCAL~TESTRUN~v1", named.getString("runId"));
        assertNotNull(named.getString("key"), "every am_* row needs its own key");
    }

    /**
     * The pairing survives ingestion. Splitting these out would leave the row saying only that
     * two tables are related — which is what the analyzer already guesses from DML.
     */
    @Test
    void bothColumnListsSurviveOnTheRowItself() throws Exception {
        List<Document> rows = mapAndCollect(NODE_JSON);

        assertEquals(List.of("customer_id"), rows.get(0).get("columns"));
        assertEquals(List.of("id"), rows.get(0).get("referencedColumns"));

        Document composite = rows.get(1);
        assertEquals(List.of("order_id", "line_no"), composite.get("columns"),
                "a composite key must keep every column, in order");
        assertEquals(List.of("id", "line_no"), composite.get("referencedColumns"));
    }

    /** An unnamed constraint is still a constraint; it must not be dropped for lacking a name. */
    @Test
    void unnamedConstraintIsStillEmitted() throws Exception {
        Document unnamed = mapAndCollect(NODE_JSON).get(1);
        assertNull(unnamed.getString("constraintName"));
        assertEquals("order_lines", unnamed.getString("tableName"));
        assertEquals("orders", unnamed.getString("referencedTable"));
    }

    @Test
    void noRowsEmittedWhenNodeDeclaresNoForeignKeys() throws Exception {
        String plain = """
                {"nodeId": "NO_FKS", "schema": "SALES", "objectName": "NO_FKS"}
                """;
        assertTrue(mapAndCollect(plain).isEmpty(), "a node with no foreign keys must emit none");
    }

    /**
     * {@code foreignKeys} must be stripped out of {@code n} before {@code Splitter.flatten} sees
     * it, or the generic splitter also writes an unintended {@code am_proc_node__foreign_keys}
     * shadow collection alongside the hand-built {@link Collections#FOREIGN_KEY} rows — the same
     * failure mode the class javadoc on {@code emitForeignKeys} describes. This asserts the
     * negative directly: nothing is ever emitted to the collection name
     * {@code Splitter.childCollection} would derive for that field.
     */
    @Test
    void noShadowCollectionFromTheGenericSplitter() throws Exception {
        String shadowCollection = com.codeanalyzer.ingest.mongo.map.Splitter
                .childCollection(Collections.PROC_NODE, "foreignKeys");
        assertEquals("am_proc_node__foreign_keys", shadowCollection,
                "sanity check on the exact name the bug would have created");

        IngestContext ctx = new IngestContext("MYSQL~MYSQLDEMO~LOCAL~TESTRUN~v1", "MYSQL");
        List<String> shadowRows = new ArrayList<>();
        new NodeDetailMapper().map(ctx,
                new ByteArrayInputStream(NODE_JSON.getBytes(StandardCharsets.UTF_8)),
                (coll, doc) -> {
                    if (shadowCollection.equals(coll)) shadowRows.add(coll);
                });

        assertTrue(shadowRows.isEmpty(),
                "foreignKeys must not also be split into " + shadowCollection);
    }

    /**
     * Declared in the catalogue, not created implicitly on first insert. An undeclared collection
     * gets no validator and — the part that bites — no index on {@code nodeRef}, which is the one
     * field every read of a node child filters by.
     */
    @Test
    void collectionIsDeclaredInTheCatalogWithBothDirectionalIndexes() {
        var spec = Catalog.all().stream()
                .filter(s -> Collections.FOREIGN_KEY.equals(s.name()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("am_foreign_key missing from Catalog.all()"));

        var indexNames = spec.indexes().stream().map(i -> i.name()).toList();
        assertTrue(indexNames.contains("ix_fk_node"), "nodeRef must be indexed: " + indexNames);
        assertTrue(indexNames.contains("ix_fk_child"),
                "'what does this table depend on' must be one seek: " + indexNames);
        assertTrue(indexNames.contains("ix_fk_parent"),
                "'what depends on this table' must be one seek too: " + indexNames);
    }
}
