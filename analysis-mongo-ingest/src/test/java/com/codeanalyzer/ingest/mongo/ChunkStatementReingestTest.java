package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.map.IngestContext;
import com.codeanalyzer.ingest.mongo.map.plsql.ChunkStatementMapper;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.codeanalyzer.ingest.mongo.write.BulkWriter;
import com.codeanalyzer.ingest.mongo.write.WriteStats;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * §267 regression — re-ingesting an analysis whose statement content has changed must replace the
 * existing {@code am_chunk_statement} row, not attempt a second insert under the same {@code key}.
 *
 * <h2>The bug this pins</h2>
 *
 * <p>{@code ChunkStatementMapper} used to call {@code Identity.stampOnto}, which sets {@code _id}
 * to a <b>content</b> hash, on a document that also carries {@code key} — and {@code key} has a
 * unique index ({@code uk_key}). {@code BulkWriter.upsert()} matches on {@code _id} whenever one
 * is present, so a re-run with any changed statement computed a new {@code _id}, missed the
 * existing row, and tried to INSERT a duplicate {@code key}. MongoDB answered {@code E11000}, the
 * bulk write threw, and the entire ingest run was marked {@code FAILED} — leaving the previous
 * (empty) run current and every entity-backed endpoint serving {@code []}. Observed live on
 * SQLSERVER {@code USP_RUNMRPCYCLE}, first failing key
 * {@code SQLSERVER~ERP$USP_EDGEOPS~chunkstmt~23~15}.
 *
 * <p>This is the same defect {@code CallGraphMapper} documents for {@code am_call_edge_unique};
 * the fix is the same: keep the business {@code key}, record the content hash in
 * {@code contentHash}, and do not set {@code _id}.
 *
 * <p>The fixture is SQL Server shaped on purpose — a {@code ##} global temp table and a
 * {@code $}-bearing node id — because the live failure was dialect-isolated and the first
 * hypothesis was the naming. It is not the naming, and this keeps that demonstrated.
 */
class ChunkStatementReingestTest {

    private static final String NODE_ID = "ERP$USP_EDGEOPS";

    /** Two statements sharing line 23, which is why ordinal is in the key at all. */
    private static String chunkJson(String secondStatementText) {
        return """
                {"nodeId":"%s","statements":[
                  {"type":"CREATE","line":23,"text":"CREATE TABLE ##g103 (id INT)"},
                  {"type":"SET","line":23,"text":"%s"}
                ]}
                """.formatted(NODE_ID, secondStatementText);
    }

    private static List<Document> map(String json) throws IOException {
        IngestContext ctx = new IngestContext("SQLSERVER~SqlServerDemo~local~USP_RUNMRPCYCLE~v9",
                "SQLSERVER");
        List<Document> out = new ArrayList<>();
        new ChunkStatementMapper().map(ctx,
                new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)),
                (collection, doc) -> {
                    if (Collections.CHUNK_STATEMENT.equals(collection)) out.add(doc);
                });
        return out;
    }

    @Test
    @DisplayName("a chunk statement carries a business key and no content-hash _id")
    void keyedNotContentAddressed() throws IOException {
        List<Document> docs = map(chunkJson("SET @x = 1"));
        assertEquals(2, docs.size(), "both statements on line 23 must survive — that is what ordinal is for");
        for (Document d : docs) {
            assertNotNull(d.getString("key"), "the key is what the split-out `__tables` children "
                    + "reference as parentRef — it cannot be dropped");
            assertNotNull(d.getString("contentHash"), "the content hash is still recorded, just not as _id");
            assertNotNull(d.getString("entityKey"), "continuity across runs still needs the entity key");
            assertNull(d.get("_id"),
                    "_id must not be the content hash: BulkWriter matches on _id when present, "
                            + "which makes a changed statement insert a duplicate `key` instead of "
                            + "replacing it, and the whole ingest run then fails with E11000");
        }
        assertEquals("SQLSERVER~ERP$USP_EDGEOPS~chunkstmt~23~0", docs.get(0).getString("key"));
        assertEquals("SQLSERVER~ERP$USP_EDGEOPS~chunkstmt~23~1", docs.get(1).getString("key"));
    }

    @Test
    @DisplayName("changed statement content replaces the stored row rather than throwing E11000")
    void changedContentReplacesUnderTheSameKey() throws IOException {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod on " + MongoAvailable.URI);
        try (MongoClient client = MongoClients.create(MongoAvailable.URI)) {
            MongoDatabase db = client.getDatabase("am_ingest_chunkstmt_reingest_test");
            db.drop();
            try {
                WriteStats stats = new WriteStats();
                try (BulkWriter w = new BulkWriter(db, stats)) {
                    for (Document d : map(chunkJson("SET @x = 1"))) {
                        w.upsert(Collections.CHUNK_STATEMENT, d);
                    }
                }
                assertEquals(2, db.getCollection(Collections.CHUNK_STATEMENT).countDocuments());

                // The re-analysis: same source position, different content — the everyday case
                // that used to abort the run.
                assertDoesNotThrow(() -> {
                    try (BulkWriter w = new BulkWriter(db, new WriteStats())) {
                        for (Document d : map(chunkJson("SET @x = 2"))) {
                            w.upsert(Collections.CHUNK_STATEMENT, d);
                        }
                    }
                }, "re-ingest of changed content must not fail on uk_key — this threw E11000 before §267");

                assertEquals(2, db.getCollection(Collections.CHUNK_STATEMENT).countDocuments(),
                        "the row must have been replaced, not duplicated");
                Document after = db.getCollection(Collections.CHUNK_STATEMENT)
                        .find(Filters.eq("key", "SQLSERVER~ERP$USP_EDGEOPS~chunkstmt~23~1")).first();
                assertNotNull(after, "the row vanished");
                assertEquals("SET @x = 2", after.getString("text"),
                        "the stored row must hold the re-ingest's content, not the stale one");
            } finally {
                db.drop();
            }
        }
    }
}
