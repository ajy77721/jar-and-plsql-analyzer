package com.analyzer.storage.mongo;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One MongoDB connection pool, shared by {@link MongoArtifactStore} and
 * {@link MongoAnalysisRepository}.
 *
 * <p>It exists because the two consumers are selected by <b>two independent properties</b>.
 * {@code app.storage.mode=mongodb} chooses the store and {@code app.storage.repository=mongo}
 * chooses the repository, deliberately so PLAN-09's rollout can move them one at a time — Increment
 * F is {@code mode=s3} with {@code repository=mongo}, which needs a Mongo connection and no Mongo
 * store at all. Letting each build its own {@link MongoClient} would mean two connection pools in
 * the configuration where both are on, and a driver client is a pool of sockets and monitor
 * threads, not a handle.
 *
 * <h2>Unconfigured is a state, not an error</h2>
 *
 * <p>In {@code local} and {@code s3} mode with the scan repository — the default, and every
 * deployment today — there is nothing to connect to and no property set. So this is always a bean
 * and is {@link #configured()}{@code == false} in that case, rather than the bean being absent.
 * A missing bean turns "Mongo is not in use" into a wiring failure at the point of injection, which
 * is a confusing error for the normal configuration.
 *
 * <h2>The connection is proven at boot</h2>
 *
 * <p>{@link #open} pings before returning. The driver connects in the background, so without this
 * an unreachable server lets the context start and fails on the first artifact write instead —
 * PLAN-09 §8 R1 is precisely a green startup hiding a storage failure, and "the app booted but
 * every analysis fails" is the worst form of it. PLAN-09 §5 states the requirement directly:
 * {@code repository=mongo} with an unreachable server must fail at startup, not on the first query.
 */
public final class MongoConnection implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MongoConnection.class);

    private static final MongoConnection UNCONFIGURED = new MongoConnection(null, null, null);

    private final MongoClient client;
    private final MongoDatabase database;
    private final String redactedUri;

    private MongoConnection(MongoClient client, MongoDatabase database, String redactedUri) {
        this.client = client;
        this.database = database;
        this.redactedUri = redactedUri;
    }

    /** The "Mongo is not in use" instance. */
    public static MongoConnection unconfigured() {
        return UNCONFIGURED;
    }

    /**
     * Opens and verifies a connection.
     *
     * <p>There is deliberately no username or password parameter, and no property for one, so no
     * secret can land in a config file this application writes or serves — the same discipline the
     * S3 backend gets from the AWS provider chain. Authentication belongs in the URI the operator
     * supplies from their own secret store, and {@link #redact} keeps it out of the logs.
     *
     * @throws IllegalStateException if the server cannot be reached, naming the property to fix
     */
    public static MongoConnection open(String uri, String databaseName) {
        if (uri == null || uri.isBlank()) {
            throw new IllegalArgumentException(
                    "app.storage.mongodb.uri must be set to use MongoDB storage or repository.");
        }
        if (databaseName == null || databaseName.isBlank()) {
            throw new IllegalArgumentException(
                    "app.storage.mongodb.database must be set to use MongoDB storage or repository.");
        }
        String redacted = redact(uri);
        // The client-wide operation timeout, and the only place it is set. Every operation this
        // application issues is bounded by it, including the ones nobody remembered to budget —
        // the gap QueryTimeoutGateTest could only count, never close.
        //
        // Set BEFORE applyConnectionString, which is what makes it a default rather than an
        // override. Measured on 5.2.1:
        //     URI has timeoutMS=7000, .timeout() after  -> 300000   (operator silently overruled)
        //     URI has timeoutMS=7000, .timeout() before ->   7000   (operator wins)
        //     URI has none,           .timeout() before -> 300000   (default applies)
        // The chained order reads as decoration and decides who wins, so it is asserted in
        // MongoConnectionTimeoutTest rather than left to whoever next tidies this builder.
        MongoClient c = MongoClients.create(MongoClientSettings.builder()
                .timeout(QueryBudget.BATCH_MS, java.util.concurrent.TimeUnit.MILLISECONDS)
                .applyConnectionString(new ConnectionString(uri))
                .applicationName("jar-plsql-analyzer")
                .build());
        try {
            c.getDatabase(databaseName).runCommand(new Document("ping", 1));
        } catch (RuntimeException e) {
            try {
                c.close();
            } catch (RuntimeException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw new IllegalStateException(
                    "MongoDB at " + redacted + " is not reachable: " + e.getMessage()
                    + ". Fix app.storage.mongodb.uri, or set app.storage.mode=local and "
                    + "app.storage.repository=artifact-scan.", e);
        }
        log.info("[mongo] connected to {}/{}", redacted, databaseName);
        return new MongoConnection(c, c.getDatabase(databaseName), redacted);
    }

    /** Wraps an already-open database, for tests and for an embedding application. */
    public static MongoConnection wrapping(MongoDatabase database) {
        return new MongoConnection(null, database, "(supplied)");
    }

    public boolean configured() {
        return database != null;
    }

    /** @throws IllegalStateException when {@link #configured()} is false */
    public MongoDatabase database() {
        if (database == null) {
            throw new IllegalStateException(
                    "MongoDB is not configured. Set app.storage.mongodb.uri and "
                    + "app.storage.mongodb.database.");
        }
        return database;
    }

    /** The connection string with any inline credentials removed. Safe to log. */
    public String redactedUri() {
        return redactedUri == null ? "(unconfigured)" : redactedUri;
    }

    /**
     * Strips {@code user:password@} from a connection string.
     *
     * <p>The documented deployment uses a credential-free URI, but a Mongo URI <em>can</em> carry a
     * password inline and there is no provider chain forcing the operator's hand — so this assumes
     * one might be present rather than trusting that it is not. C4 is about credentials never
     * reaching shared storage; a credential in an application log is the same disclosure by a
     * shorter route.
     */
    public static String redact(String uri) {
        if (uri == null) return "";
        int schemeEnd = uri.indexOf("://");
        if (schemeEnd < 0) return uri;
        // The host section ends at the first '/' or '?' after the scheme; credentials, if any, are
        // separated from it by the last '@' BEFORE that boundary. Bounding the search matters
        // because '@' is legal in a database name and in a query parameter value.
        int hostEnd = uri.length();
        for (int i = schemeEnd + 3; i < uri.length(); i++) {
            char ch = uri.charAt(i);
            if (ch == '/' || ch == '?') {
                hostEnd = i;
                break;
            }
        }
        int at = uri.lastIndexOf('@', hostEnd - 1);
        if (at <= schemeEnd + 2) return uri;
        return uri.substring(0, schemeEnd + 3) + "***@" + uri.substring(at + 1);
    }

    /**
     * Closes the pool if this instance owns one.
     *
     * <p>A no-op for {@link #unconfigured()} and for {@link #wrapping}, because neither created the
     * client and closing someone else's pool from a Spring shutdown hook would break the owner.
     */
    @Override
    public void close() {
        if (client != null) client.close();
    }
}
