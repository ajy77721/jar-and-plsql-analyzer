package com.analyzer.storage.mongo;

import com.mongodb.client.MongoCollection;

import java.util.concurrent.TimeUnit;

/**
 * A time budget on every query, so a missing index fails loudly instead of hanging.
 *
 * <h2>What goes wrong without one</h2>
 *
 * <p>A query with no usable index does not error — it does a collection scan. On the measured
 * corpus that is a few thousand documents and nobody notices; at the target volume it is a million,
 * and the request simply never returns. The connection is held, the thread is held, and the symptom
 * that reaches anyone is "the page is slow" with nothing naming the query responsible.
 *
 * <p>A timeout converts that into an error the server raises, naming the operation. It is a
 * diagnosis rather than a cure: the fix is the index, and this is what makes the missing index
 * findable on the day it starts mattering rather than after a support call.
 *
 * <h2>Two budgets, because two things are being protected</h2>
 *
 * <p>A request has a person waiting and must fail fast. A batch job has nobody waiting and a real
 * reason to take minutes — capping it at request latency would break correct work.
 *
 * <p>{@link #BATCH_MS} is now applied <b>once</b>, client-wide, in
 * {@link MongoConnection#open(String, String)}. That is the floor: every operation issued by this
 * application is bounded, including ones nobody remembered to budget.
 * {@link #request(MongoCollection)} tightens it where a person is waiting.
 *
 * <h2>Why this is not {@code maxTime} any more</h2>
 *
 * <p>Driver 5.2 introduced CSOT (client-side operation timeout), and under it {@code maxTimeMS} is
 * <b>silently ignored</b>. Measured on driver 5.2.1 against a 200,000-document unindexed scan:
 *
 * <pre>
 *   maxTime=1ms, no client timeout        REJECTED MongoExecutionTimeoutException
 *   maxTime=1ms, client timeout=300s      COMPLETED in 59 ms   &lt;-- budget discarded
 *   withTimeout=1ms, client timeout=300s  REJECTED MongoOperationTimeoutException
 *   no budget, client timeout=1ms         REJECTED MongoOperationTimeoutException
 * </pre>
 *
 * <p>The second row is the whole reason this class changed shape. Setting the client timeout while
 * leaving the old {@code .maxTime(REQUEST_MS, …)} calls in place would have turned every 15-second
 * request budget into 300 seconds, with nothing failing and nothing logged — a guard that reads as
 * present and enforces nothing. The budget is therefore applied to the <i>collection</i>, via
 * {@code withTimeout}, which CSOT does honour.
 *
 * <p>The previous version of this class offered six methods wrapping {@code FindIterable},
 * {@code AggregateIterable} and {@code DistinctIterable}. All six had zero callers — every site
 * inlined {@code .maxTime(QueryBudget.REQUEST_MS, MILLISECONDS)} instead, and {@code BATCH_MS} had
 * no caller at all. They are gone rather than ported: an unused overload for a type nobody passes
 * is how the last one came to describe a design that was never built.
 */
public final class QueryBudget {

    /**
     * Serving a request. Above this the caller has given up anyway, so continuing only holds a
     * connection and a thread.
     */
    public static final long REQUEST_MS = 15_000;

    /**
     * The client-wide floor, applied in {@link MongoConnection}. Long, because ingest, verification
     * and retention legitimately walk whole collections — the reachability traversal and the
     * round-trip prover both do — but finite, because "walks a whole collection" and "never
     * returns" look identical from outside.
     *
     * <p>Note this is an <i>operation</i> timeout, not a per-round-trip one: it covers cursor
     * iteration to exhaustion, which is exactly the case that used to hang.
     */
    public static final long BATCH_MS = 300_000;

    private QueryBudget() {
    }

    /**
     * Tightens a collection to the request budget. Use for anything on a request path; leave
     * batch work on the client-wide {@link #BATCH_MS} floor.
     */
    public static <T> MongoCollection<T> request(MongoCollection<T> collection) {
        return collection.withTimeout(REQUEST_MS, TimeUnit.MILLISECONDS);
    }
}
