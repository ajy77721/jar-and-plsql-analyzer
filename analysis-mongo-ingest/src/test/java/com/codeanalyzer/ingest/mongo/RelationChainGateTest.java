package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.model.RunMember;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * VER-007 — every {@code am_*} collection reaches its run in one hop, and the hop is indexed.
 *
 * <h2>What "one hop" means, and the one exception</h2>
 *
 * <p>{@link Collections#RUN} documents <b>are</b> the run: {@code _id} is the run identity, not a
 * reference to it, so it is the zero-hop base case and is exempt from the {@code runId} check
 * below. Every other collection this module writes carries {@code runId} directly — never a chain
 * of {@code parentRef}s that must be walked to discover which run a document belongs to. That
 * property is what lets "delete everything from run X" and "which run wrote this row" both be a
 * single indexed query, regardless of how deep a collection's split ancestry goes.
 *
 * <h2>Why the index is checked separately from the field</h2>
 *
 * <p>A {@code runId} field with no index is a query away from being a collection scan on every
 * "documents from this run" lookup — correct today, at a few hundred thousand documents, and a
 * silent regression once the corpus reaches the stated target. Checked against
 * {@code listIndexes()} directly, not by asserting a plan has no {@code COLLSCAN}: the second form
 * would need one query per collection and would only prove the index exists for whichever query
 * shape was tried.
 */
class RelationChainGateTest {

    private static final Path PLSQL =
            Path.of("../data/plsql-parse/ORACLE/ORACLEDEMO/LOCAL/ERP_USP_RUN_CYCLE");

    private static MongoClient client;
    private static MongoDatabase db;

    @BeforeAll
    static void setUp() throws Exception {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod");
        Assumptions.assumeTrue(Files.isDirectory(PLSQL), "ORACLE analysis not on disk");
        Path jar = MongoIngestM5JarTest.newestJarAnalysis();
        Assumptions.assumeTrue(jar != null, "no JAR analysis on disk");

        client = MongoClients.create(MongoAvailable.URI);
        db = client.getDatabase("am_relation_chain_test");
        db.drop();
        MongoIngestService svc = new MongoIngestService(db);
        svc.installSchema();
        svc.ingest("ORACLE", "ORACLEDEMO", "LOCAL", "ERP_USP_RUN_CYCLE",
                new FileArtifactSource(PLSQL));
        svc.ingestJar("unified-web", new FileArtifactSource(jar));
    }

    @AfterAll
    static void tearDown() {
        if (db != null) db.drop();
        if (client != null) client.close();
    }

    /**
     * Collections that legitimately do not carry a plain {@code runId}, with the reason — the same
     * discipline {@code OrphanRefChecker.NULLABLE} uses for nullable references, so an exception
     * here is a documented judgement and not silence.
     */
    private static final Set<String> NO_PLAIN_RUN_ID = Set.of(
            // Content-addressed and deliberately shared across runs — 73% of captured SQL is
            // duplicated (IdCodec.sqlText's own comment). A runId field would force a choice
            // between one row per (text, run) pair, which defeats the dedup this collection
            // exists for, or a meaningless first-writer value. Reachable in one hop from the
            // am_statement row that references it, which does carry runId — just not from here.
            "am_sql_text",
            // Schema-level bookkeeping — an id-sequence counter and a schema-version marker, not
            // per-run analysis data. Exempt for the same reason am_run itself is: there is no run
            // to be one hop from.
            "am_counter", "am_meta");

    /** {@code am_run_member}'s field naming the run it joins, since it is not called {@code runId}
     * here — the collection's whole purpose is the join, so the field is {@code runRef}. */
    private static final Map<String, String> RUN_FIELD = Map.of(RunMember.COLLECTION, "runRef");

    private String runFieldOf(String collection) {
        return RUN_FIELD.getOrDefault(collection, "runId");
    }

    /** Every {@code am_*} collection except the run collection and the documented exceptions. */
    private List<String> nonRunCollections() {
        List<String> out = new ArrayList<>();
        for (String name : db.listCollectionNames()) {
            if (name.startsWith("am_") && !name.equals(Collections.RUN)
                    && !NO_PLAIN_RUN_ID.contains(name)) {
                out.add(name);
            }
        }
        return out;
    }

    @Test
    void everyDocumentCarriesARunId() {
        List<String> offenders = new ArrayList<>();
        for (String coll : nonRunCollections()) {
            String field = runFieldOf(coll);
            long total = db.getCollection(coll).countDocuments();
            long withField = db.getCollection(coll).countDocuments(
                    com.mongodb.client.model.Filters.exists(field));
            if (total > 0 && withField != total) {
                offenders.add(coll + ": " + (total - withField) + " of " + total
                        + " rows have no " + field);
            }
        }
        assertTrue(offenders.isEmpty(),
                "these collections have a row with no run-reaching field — its run cannot be "
                        + "found in one hop, or at all: " + offenders);
    }

    @Test
    void everyRunIdResolvesToARealRun() {
        Set<String> realRunIds = new HashSet<>();
        for (Document d : db.getCollection(Collections.RUN)
                .find().projection(new Document("_id", 1))) {
            realRunIds.add(d.getString("_id"));
        }
        assertFalse(realRunIds.isEmpty(), "no runs exist; this gate would pass on nothing");

        List<String> offenders = new ArrayList<>();
        for (String coll : nonRunCollections()) {
            for (Object rid : db.getCollection(coll).distinct(runFieldOf(coll), String.class)) {
                if (rid != null && !realRunIds.contains(rid)) {
                    offenders.add(coll + " -> " + rid);
                }
            }
        }
        assertEquals(List.of(), offenders,
                "these collections carry a runId that names no am_run document");
    }

    @Test
    void everyRunIdIsIndexed() {
        List<String> offenders = new ArrayList<>();
        for (String coll : nonRunCollections()) {
            String field = runFieldOf(coll);
            boolean indexed = false;
            for (Document idx : db.getCollection(coll).listIndexes()) {
                Document key = idx.get("key", Document.class);
                // The field a compound index leads with is the one it actually accelerates for an
                // equality lookup, so the run field must be first, not merely present in it.
                if (key != null && key.keySet().iterator().hasNext()
                        && key.keySet().iterator().next().equals(field)) {
                    indexed = true;
                    break;
                }
            }
            if (!indexed) offenders.add(coll + " (expects an index leading with " + field + ")");
        }
        assertTrue(offenders.isEmpty(),
                "these collections have no index leading with their run-reaching field, so "
                        + "finding their rows for one run is a collection scan: " + offenders);
    }

    @Test
    void aCollectionMissingTheIndexIsCaught() {
        // Calibration: create a collection with runId present but unindexed, and confirm the
        // exact assertion above would report it.
        String scratch = "am_relation_chain_scratch";
        db.getCollection(scratch).drop();
        db.getCollection(scratch).insertOne(new Document("runId", "JAR~x~y~z~v1"));
        try {
            boolean indexed = false;
            for (Document idx : db.getCollection(scratch).listIndexes()) {
                Document key = idx.get("key", Document.class);
                if (key != null && key.keySet().iterator().hasNext()
                        && key.keySet().iterator().next().equals("runId")) {
                    indexed = true;
                }
            }
            assertFalse(indexed, "a freshly inserted, unindexed field was reported as indexed");
        } finally {
            db.getCollection(scratch).drop();
        }
    }
}
