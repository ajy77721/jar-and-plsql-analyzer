package com.codeanalyzer.ingest.mongo;

import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.codeanalyzer.ingest.mongo.verify.OrphanRefChecker;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ING-112 — the endpoint-to-pool join is stored with both sides resolved.
 *
 * <h2>What was wrong</h2>
 *
 * <p>An endpoint references the three shared pools by <b>position</b>:
 * {@code externalCallsRefs: [0, 3, 7]}. Splitter did what it does to any array of scalars and
 * stored one row per element holding {@code {value: 7}} — the integer, and no reference to the pool
 * document at all. Three consequences, none of which fail anything:
 *
 * <ul>
 *   <li>"Which endpoints use this bean" had no index to use and no reference to match. The caller
 *       had to rebuild {@code am_endpoint_pool~<jar>~beans~7} by string concatenation, which means
 *       the key format becomes every caller's problem.</li>
 *   <li>{@code OrphanRefChecker} discovers fields ending in {@code Ref}. There was none here, so
 *       60,483 references were <b>unchecked</b> — indistinguishable from valid.</li>
 *   <li>A position is only meaningful against the array it indexes. Nothing tied the integer to a
 *       particular pool of a particular run.</li>
 * </ul>
 *
 * <p>This is the {@code am_txn_statement} shape again: the data is present, every integrity check
 * passes, and the query a person would write returns nothing.
 *
 * <h2>Why the rows moved rather than multiplied</h2>
 *
 * <p>The positional rows are removed from the endpoint before Splitter sees it, so the count is
 * unchanged: 47,461 + 9,264 + 3,758 = <b>60,483</b> before as three collections of integers, and
 * 60,483 after as one collection of resolved links. Storing both shapes would mean the same fact in
 * two places, which is how a query gets written against the wrong one.
 */
class PoolLinkTest {

    private static final Path PLSQL =
            Path.of("../data/plsql-parse/ORACLE/ORACLEDEMO/LOCAL/ERP_USP_RUN_CYCLE");

    private static MongoClient client;
    private static MongoDatabase db;
    private static JsonNode analysis;

    @BeforeAll
    static void setUp() throws Exception {
        Assumptions.assumeTrue(MongoAvailable.present(), "no local mongod");
        Path jar = MongoIngestM5JarTest.newestJarAnalysis();
        Assumptions.assumeTrue(jar != null, "no JAR analysis on disk");

        client = MongoClients.create(MongoAvailable.URI);
        db = client.getDatabase("am_pool_link_test");
        db.drop();
        MongoIngestService svc = new MongoIngestService(db);
        svc.installSchema();
        svc.ingestJar("unified-web", new FileArtifactSource(jar));
        analysis = new ObjectMapper().readTree(
                Files.newInputStream(jar.resolve("analysis.json")));
    }

    @AfterAll
    static void tearDown() {
        if (db != null) db.drop();
        if (client != null) client.close();
    }

    @Test
    void everyPositionInTheSourceBecameALink() {
        // Counted from the file rather than hard-coded, so the test cannot drift from the data.
        int expected = 0;
        for (JsonNode e : analysis.path("endpoints")) {
            for (String f : List.of("externalCallsRefs", "beansRefs", "dynamicFlowsRefs")) {
                if (e.path(f).isArray()) expected += e.path(f).size();
            }
        }
        assertTrue(expected > 0, "the fixture has no pooled references at all");
        assertEquals(expected,
                db.getCollection(Collections.ENDPOINT_POOL_LINK).countDocuments(),
                "the link collection does not hold one row per source position");
    }

    @Test
    void thePositionalRowsAreGoneNotDuplicated() {
        // The same fact in two shapes is the defect this fix would otherwise introduce.
        Set<String> names = new HashSet<>();
        db.listCollectionNames().into(names);
        for (String stale : List.of("am_endpoint__external_calls_refs",
                "am_endpoint__beans_refs", "am_endpoint__dynamic_flows_refs")) {
            assertFalse(names.contains(stale),
                    stale + " still exists — the positional integers are being stored alongside "
                            + "the resolved links");
        }
        Document endpoint = db.getCollection(Collections.ENDPOINT).find().first();
        assertNotNull(endpoint);
        for (String f : List.of("externalCallsRefs", "beansRefs", "dynamicFlowsRefs")) {
            assertFalse(endpoint.containsKey(f),
                    "the endpoint document still carries " + f + " inline");
        }
    }

    @Test
    void bothSidesOfEveryLinkResolve() {
        // The property the bare integers could not have. Asserted over all 60,483 rows rather than
        // a sample: a positional reference that lands on the wrong pool is exactly the case a
        // sample of the first few hundred would miss.
        Set<String> poolKeys = new HashSet<>();
        for (Document d : db.getCollection(Collections.ENDPOINT_POOL)
                .find().projection(new Document("entityKey", 1))) {
            poolKeys.add(d.getString("entityKey"));
        }
        Set<String> endpointKeys = new HashSet<>();
        for (Document d : db.getCollection(Collections.ENDPOINT)
                .find().projection(new Document("entityKey", 1))) {
            endpointKeys.add(d.getString("entityKey"));
        }
        assertFalse(poolKeys.isEmpty());

        int danglingPool = 0;
        int danglingEndpoint = 0;
        String example = null;
        for (Document l : db.getCollection(Collections.ENDPOINT_POOL_LINK).find()) {
            if (!poolKeys.contains(l.getString("poolRef"))) {
                danglingPool++;
                if (example == null) example = l.getString("poolRef");
            }
            if (!endpointKeys.contains(l.getString("endpointRef"))) danglingEndpoint++;
        }
        assertEquals(0, danglingPool, "links point at pool rows that do not exist, e.g. " + example);
        assertEquals(0, danglingEndpoint, "links point at endpoints that do not exist");
    }

    @Test
    void theCheckerNowSeesTheseReferences() {
        // The second consequence: 60,483 references used to be invisible to the orphan checker
        // because no field ended in "Ref". Discovery must now find both sides, or the links are
        // still unchecked and this fix bought only ergonomics.
        var fields = new OrphanRefChecker(db).discoverRefFields()
                .getOrDefault(Collections.ENDPOINT_POOL_LINK, Set.of());
        assertTrue(fields.containsAll(Set.of("endpointRef", "poolRef")),
                "the checker does not discover both sides of the link: " + fields);

        var unresolvable = new OrphanRefChecker(db).check().stream()
                .filter(o -> Collections.ENDPOINT_POOL_LINK.equals(o.collection()))
                .toList();
        assertEquals(List.of(), unresolvable,
                "the checker reports the link collection's references as unresolvable — poolRef "
                        + "needs a declared target");
    }

    @Test
    void theReverseQuestionIsAnswerableFromAnIndex() {
        // "Which endpoints use this pool entry" is the question the positional integers could not
        // answer without the caller knowing the key format. It must now be one indexed lookup.
        Document link = db.getCollection(Collections.ENDPOINT_POOL_LINK).find().first();
        assertNotNull(link);
        String poolRef = link.getString("poolRef");

        long users = db.getCollection(Collections.ENDPOINT_POOL_LINK)
                .countDocuments(Filters.eq("poolRef", poolRef));
        assertTrue(users >= 1, "no endpoint resolves to a pool entry that a link names");

        Document plan = db.getCollection(Collections.ENDPOINT_POOL_LINK)
                .find(Filters.eq("poolRef", poolRef)).explain();
        assertFalse(plan.toJson().contains("COLLSCAN"),
                "the reverse lookup is a collection scan; ix_epl_pool is missing");
    }

    @Test
    void aLinkNamesWhichPoolItIndexes() {
        // A position is meaningless without the array it indexes. `pool` is stored so a row is
        // interpretable on its own, and it must agree with the key it points at.
        for (Document l : db.getCollection(Collections.ENDPOINT_POOL_LINK).find().limit(500)) {
            String pool = l.getString("pool");
            assertNotNull(pool, "a link does not say which pool it indexes");
            assertTrue(l.getString("poolRef").contains("~" + pool + "~"),
                    "link says pool=" + pool + " but points at " + l.getString("poolRef"));
        }
    }
}
