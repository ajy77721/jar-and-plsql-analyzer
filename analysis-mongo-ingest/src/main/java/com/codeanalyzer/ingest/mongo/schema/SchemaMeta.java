package com.codeanalyzer.ingest.mongo.schema;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import org.bson.Document;

/**
 * The one document that says which version of this schema a database holds.
 *
 * <h2>The failure it prevents</h2>
 *
 * <p>Nothing stops an older build from being pointed at a database a newer one wrote. Without a
 * check, the old build installs its own validators over the new ones and ingests happily — writing
 * documents in an older shape alongside newer ones, into collections whose readers expect the
 * newer. The result is not a crash but a database in two shapes at once, and no record of when it
 * happened. Rolling back a deployment is enough to cause it.
 *
 * <p>So the rule is one-directional: a newer database refuses an older writer, and an older
 * database is upgraded by the newer writer. Refusing is safe and reversible — redeploy the newer
 * build. Writing is not.
 *
 * <h2>Why the version is stored rather than derived</h2>
 *
 * <p>It cannot be inferred from the collections present: an empty database and a fully populated
 * one at the current version look identical to a schema-shape check, and a mapper fix that changes
 * document <em>contents</em> changes no shape at all. Only a number someone increments deliberately
 * records "the documents in here were written by that code".
 */
public final class SchemaMeta {

    public static final String COLLECTION = "am_meta";
    public static final String ID = "schema";

    /**
     * The schema this build writes.
     *
     * <p>Increment when a change makes previously written documents wrong rather than merely
     * incomplete — a renamed field, a changed identity, a different split. Adding a collection or a
     * field does not need it: old documents remain valid, they are simply missing the new part.
     *
     * <p>Started at 1 rather than 0 so that "absent" and "version zero" cannot be confused, which
     * matters because an unversioned database predates this check entirely.
     */
    public static final int CURRENT_VERSION = 1;

    private SchemaMeta() {
    }

    /** Raised when the database was written by a newer build than this one. */
    public static final class SchemaTooNewException extends RuntimeException {
        public SchemaTooNewException(int found, int supported) {
            super("database schema version " + found + " was written by a newer build; this one "
                    + "supports " + supported + ". Refusing to write, because an older writer would "
                    + "install older validators and mix document shapes. Deploy the newer build.");
        }
    }

    /** The version recorded in the database, or 0 when it predates this check. */
    public static int storedVersion(MongoDatabase db) {
        Document d = db.getCollection(COLLECTION).find(Filters.eq("_id", ID)).first();
        return d == null ? 0 : d.getInteger("schemaVersion", 0);
    }

    /**
     * Refuses a database newer than this build, then records this build's version.
     *
     * <p>The check runs before any collection is created or modified. Doing it afterwards would
     * mean the validators of an unsupported build had already been installed by the time the
     * mismatch was noticed — the exact damage the check exists to prevent.
     */
    public static void checkAndStamp(MongoDatabase db) {
        int found = storedVersion(db);
        if (found > CURRENT_VERSION) {
            throw new SchemaTooNewException(found, CURRENT_VERSION);
        }
        db.getCollection(COLLECTION).replaceOne(
                Filters.eq("_id", ID),
                new Document("_id", ID)
                        .append("schemaVersion", CURRENT_VERSION)
                        .append("updatedAt", java.time.Instant.now().toString()),
                new ReplaceOptions().upsert(true));
    }
}
