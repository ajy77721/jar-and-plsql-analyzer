package com.analyzer.storage.mongo;

import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The client-wide operation timeout is set, is a default rather than an override, and actually
 * bounds a query nobody budgeted.
 *
 * <h2>Why this test exists</h2>
 *
 * <p>Driver 5.2 brought CSOT, and {@link MongoConnection} sets one client-wide timeout. Two things
 * about that are decided by <b>the order of chained builder calls</b>, which reads as decoration:
 *
 * <ul>
 *   <li>{@code .timeout()} before {@code applyConnectionString} — an operator's own
 *       {@code timeoutMS} in the URI wins, and ours is only a default.</li>
 *   <li>{@code .timeout()} after — ours silently overrules theirs, and an operator debugging an
 *       unresponsive server has no setting they can reach.</li>
 * </ul>
 *
 * <p>Both compile, both connect, and the difference is invisible until someone needs it. Anyone
 * reformatting that builder can reverse it without knowing there was a decision there.
 *
 * <h2>The trap this also pins</h2>
 *
 * <p>Under CSOT the server <b>ignores</b> {@code maxTimeMS}. Measured here rather than asserted
 * from the release notes, because the whole conversion of {@code QueryBudget} away from
 * {@code maxTime} rests on it: if it were false, the 18 converted call sites were churn, and if it
 * is true, leaving them would have widened every request budget from 15 s to 300 s with nothing
 * failing.
 */
class MongoConnectionTimeoutTest {

    private static final String DB = "am_timeout_test";
    private static final String COLL = "scan";
    /** Big enough that an unindexed scan cannot finish inside a 1 ms budget. */
    private static final int DOCS = 200_000;

    private static MongoClient seedClient;

    @BeforeAll
    static void seed() {
        seedClient = MongoTestSupport.clientOrSkip();
        MongoCollection<Document> col = seedClient.getDatabase(DB).getCollection(COLL);
        if (col.countDocuments() >= DOCS) return;
        col.drop();
        List<Document> batch = new ArrayList<>();
        for (int i = 0; i < DOCS; i++) {
            batch.add(new Document("s", "value-that-is-long-enough-to-scan-" + i));
            if (batch.size() == 10_000) {
                col.insertMany(batch);
                batch.clear();
            }
        }
    }

    @AfterAll
    static void cleanUp() {
        // Twenty-three test classes once accumulated 2.1 GB in this mongod and the OS began
        // killing runs. This one seeds 200,000 documents, so it cleans up after itself.
        if (seedClient != null) {
            seedClient.getDatabase(DB).drop();
            seedClient.close();
        }
    }

    private static MongoConnection open(String uri) {
        return MongoConnection.open(uri, DB);
    }

    @Test
    void theClientWideTimeoutIsSetToTheBatchBudget() {
        try (MongoConnection c = open(MongoTestSupport.uri())) {
            assertEquals(QueryBudget.BATCH_MS, c.database().getTimeout(TimeUnit.MILLISECONDS),
                    "no client-wide operation timeout — an unbudgeted query can hang again");
        }
    }

    @Test
    void anOperatorsOwnTimeoutInTheUriWins() {
        // The builder-order decision, asserted rather than left in a comment.
        String uri = MongoTestSupport.uri() + "/?timeoutMS=7000";
        try (MongoConnection c = open(uri)) {
            assertEquals(7000L, c.database().getTimeout(TimeUnit.MILLISECONDS),
                    "the application overrode the operator's timeoutMS; move .timeout() before "
                            + "applyConnectionString in MongoConnection");
        }
    }

    @Test
    void theRequestBudgetTightensTheBatchFloor() {
        try (MongoConnection c = open(MongoTestSupport.uri())) {
            MongoCollection<Document> col = c.database().getCollection(COLL);
            assertEquals(QueryBudget.REQUEST_MS,
                    QueryBudget.request(col).getTimeout(TimeUnit.MILLISECONDS),
                    "QueryBudget.request did not tighten the collection to the request budget");
            assertTrue(QueryBudget.REQUEST_MS < QueryBudget.BATCH_MS,
                    "the request budget is no longer tighter than the batch floor, so wrapping a "
                            + "collection with it achieves nothing");
        }
    }

    @Test
    void everyUnbudgetedCollectionInheritsTheFloor() {
        // The actual win from the upgrade, and the reason QueryTimeoutGateTest's original
        // justification is gone: a collection nobody wrapped still carries a finite budget. The
        // 50 sites the gate still lists are all of this shape.
        try (MongoConnection c = open(MongoTestSupport.uri())) {
            assertEquals(QueryBudget.BATCH_MS,
                    c.database().getCollection(COLL).getTimeout(TimeUnit.MILLISECONDS),
                    "an unwrapped collection has no timeout, so unbudgeted queries can hang");
        }
    }

    @Test
    void anUnbudgetedQueryIsActuallyRejectedNotJustConfigured() {
        // The setting above is necessary and not sufficient: it has to reach the server. So one
        // behavioural proof, on a query that wraps nothing.
        //
        // Margins matter more than tightness here. timeoutMS=1 was the obvious choice and is
        // wrong: CSOT bounds *server selection* too, so open() could not connect and the test
        // failed in setUp with "not reachable" — a pass/fail that says nothing about queries.
        // 1500 ms is far above the ~20 ms connect and far below this aggregation, which rescans
        // the whole collection once per facet.
        try (MongoConnection c = open(MongoTestSupport.uri() + "/?timeoutMS=1500")) {
            MongoCollection<Document> col = c.database().getCollection(COLL);
            List<Document> facets = new ArrayList<>();
            for (int i = 0; i < 60; i++) {
                facets.add(new Document("f" + i, List.of(
                        new Document("$match", new Document("s",
                                new Document("$regex", "no-match-" + i + "-anywhere"))),
                        new Document("$count", "n"))));
            }
            assertThrows(MongoOperationTimeoutException.class,
                    () -> col.aggregate(List.of(new Document("$facet", new Document(facets
                            .stream().reduce(new Document(), (a, b) -> {
                                a.putAll(b);
                                return a;
                            }))))).first(),
                    "a query with no budget of its own was not bounded by the client timeout");
        }
    }

    @Test
    void theClientTimeoutAlsoBoundsConnecting() {
        // Found by writing the test above wrongly. Worth pinning: it means the floor protects
        // startup against an unreachable server as well as queries against a missing index, and
        // it is why no test here may use a timeout smaller than the connect cost.
        assertThrows(IllegalStateException.class,
                () -> open(MongoTestSupport.uri() + "/?timeoutMS=1"),
                "a 1 ms client timeout allowed a connection, so CSOT is not covering server "
                        + "selection and MongoConnection.open can still stall on a dead server");
    }

    @Test
    @SuppressWarnings("deprecation")
    void maxTimeIsIgnoredOnceAClientTimeoutIsSet() {
        // Why the 18 call sites had to change and why the gate now bans .maxTime outright. A 1 ms
        // budget on a 200,000-document unindexed scan is impossible to satisfy, so completing is
        // proof the server disregarded it.
        try (MongoConnection c = open(MongoTestSupport.uri())) {
            MongoCollection<Document> col = c.database().getCollection(COLL);
            Document ignoredBudget = col.find(Filters.regex("s", "zzz-no-match-anywhere"))
                    .maxTime(1, TimeUnit.MILLISECONDS).first();
            assertEquals(null, ignoredBudget, "the fixture query should match nothing");
        }

        // And the same impossible budget applied the supported way is honoured, so the difference
        // is the mechanism and not the fixture.
        try (MongoConnection c = open(MongoTestSupport.uri())) {
            MongoDatabase db = c.database();
            assertThrows(MongoOperationTimeoutException.class,
                    () -> db.getCollection(COLL).withTimeout(1, TimeUnit.MILLISECONDS)
                            .find(Filters.regex("s", "zzz-no-match-anywhere")).first(),
                    "withTimeout was ignored too — then there is no working budget mechanism at "
                            + "all and QueryBudget cannot do its job");
        }
    }

    @Test
    void theFixtureIsBigEnoughToOutrunTheBudget() {
        // Every timing-based assertion above is vacuous if the collection is small: a scan that
        // finishes in under a millisecond satisfies even a 1 ms budget, and the two tests that
        // expect a rejection would fail for the right reason only by luck.
        assertNotNull(seedClient);
        assertTrue(seedClient.getDatabase(DB).getCollection(COLL).countDocuments() >= DOCS,
                "the scan fixture is smaller than " + DOCS + " documents");
    }
}
