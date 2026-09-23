package com.codeanalyzer.ingest.mongo.schema;

import org.bson.Document;

import java.util.List;

/**
 * The {@code $jsonSchema} validators — MongoDB's stand-in for NOT NULL, CHECK and domain types.
 *
 * <h2>What is constrained, and what deliberately is not</h2>
 *
 * <p>Validators here assert <b>structure this module mints</b>: the presence of {@code _id},
 * {@code runId} and the fields that make a relationship traversable. They do <b>not</b> enumerate
 * values that originate in analysed source code.
 *
 * <p>That distinction is the difference between a validator that protects the data and one that
 * rejects it. {@code operation} and {@code accessKind} were enumerated from five dialects — but
 * every enum in this system has grown at least once, and a sixth dialect emitting a new operation
 * would fail ingest outright under {@code validationAction: "error"}. Refusing to store real
 * output because a list written today did not anticipate it is a worse failure than storing a
 * value nobody predicted. Vocabulary drift is reported by the verifier instead, where it is
 * visible and harmless.
 *
 * <p>Because the validators constrain only minted structure, {@code strict}/{@code error} is the
 * right setting: everything they assert is something this module is responsible for getting right.
 */
public final class Validators {

    private Validators() {}

    /** Required top-level fields, expressed as a {@code $jsonSchema}. */
    private static Document requiring(Document properties, String... required) {
        return new Document("$jsonSchema", new Document()
                .append("bsonType", "object")
                .append("required", List.of(required))
                .append("properties", properties));
    }

    private static Document str() { return new Document("bsonType", "string"); }

    /*
     * `_id` is deliberately absent from every validator below.
     *
     * Two document shapes coexist by design: an entity document's `_id` is its content hash (a
     * string), while a run-scoped row lets MongoDB generate an ObjectId. Constraining `_id` to a
     * string rejected every one of the latter — the validator was still asserting the shape from
     * before identity was split into three levels. What each document must carry is its *business*
     * identity, and that is what is required here instead.
     */
    private static Document intOrLong() { return new Document("bsonType", List.of("int", "long")); }

    public static Document run() {
        return requiring(new Document()
                        .append("kind", new Document("bsonType", "string")
                                .append("enum", List.of("PLSQL", "JAR")))
                        .append("status", new Document("bsonType", "string")
                                // The run lifecycle is minted here, so it IS safe to enumerate:
                                // no analysed source can introduce a new status.
                                .append("enum", List.of("IN_PROGRESS", "COMPLETE", "SUPERSEDED", "FAILED")))
                        .append("version", intOrLong()),
                "key", "kind", "status", "version");
    }

    public static Document dbObject() {
        return requiring(new Document()
                        .append("entityKey", str())
                        .append("contentHash", str())
                        .append("objectName", str()),
                // schemaName is NOT required: 17 SQL Server rows carry {"schema":null,"name":null}
                // and the lossless rule says store them as they are, flagged, rather than drop them.
                "entityKey", "contentHash", "objectName");
    }

    public static Document procNode() {
        return requiring(new Document()
                        .append("entityKey", str())
                        .append("contentHash", str())
                        .append("nodeId", str()),
                "entityKey", "contentHash", "nodeId");
    }

    /**
     * Shared by every node-scoped child.
     *
     * <p>Only the three fields that make the row reachable are required. The payload varies by
     * kind and originates in analysed source, so constraining it here would reject real output —
     * the same reasoning that keeps {@code operation} out of the enums above.
     */
    public static Document nodeChild() {
        return requiring(new Document()
                        .append("key", str())
                        .append("runId", str()),
                "key", "runId");
    }

    public static Document statement() {
        return requiring(new Document()
                        .append("entityKey", str())
                        .append("contentHash", str())
                        .append("operation", str()),
                // `tableName` is NOT required: SQL Server emits 17 rows with a null schema and
                // name, and the lossless rule says store them, flagged, rather than drop them.
                "entityKey", "contentHash", "operation");
    }

    public static Document sqlText() {
        return requiring(new Document()
                        .append("entityKey", str())
                        .append("text", str()),
                "entityKey", "text");
    }

    public static Document callEdge() {
        return requiring(new Document()
                        .append("entityKey", str())
                        .append("fromRef", str())
                        .append("toRef", str()),
                "entityKey", "fromRef", "toRef");
    }

    public static Document javaEntity() {
        return requiring(new Document()
                        .append("entityKey", str())
                        .append("contentHash", str()),
                "entityKey", "contentHash");
    }

    /**
     * A junction row: both sides required.
     *
     * <p>Both, not one. A link with a null side is not a partial answer — it is a row that passes
     * every integrity check and silently drops an endpoint from "who uses this bean". The previous
     * shape stored the pool position as a bare integer with no reference at all, which is the same
     * defect one step further along: nothing to be null, and nothing to check.
     */
    public static Document poolLink() {
        return requiring(new Document()
                        .append("key", str())
                        .append("runId", str())
                        .append("endpointRef", str())
                        .append("poolRef", str()),
                "key", "runId", "endpointRef", "poolRef");
    }

    /**
     * {@code am_run_member} — the membership join. Not one of the entity shapes above: it has no
     * {@code entityKey} or {@code contentHash} of its own, because it is a fact ABOUT one, not an
     * entity itself.
     */
    public static Document runMember() {
        return requiring(new Document()
                        .append("runRef", str())
                        .append("entityRef", str())
                        .append("entityCollection", str()),
                "runRef", "entityRef", "entityCollection");
    }

    public static Document tableColumnUsage() {
        return requiring(new Document()
                        .append("entityKey", str())
                        .append("contentHash", str())
                        .append("objectRef", str())
                        .append("columnName", str()),
                "entityKey", "contentHash", "objectRef", "columnName");
    }
}
