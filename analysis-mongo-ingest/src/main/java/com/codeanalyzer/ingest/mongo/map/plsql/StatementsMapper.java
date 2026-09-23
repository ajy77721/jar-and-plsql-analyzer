package com.codeanalyzer.ingest.mongo.map.plsql;

import com.codeanalyzer.ingest.mongo.id.ContentHasher;
import com.codeanalyzer.ingest.mongo.id.IdCodec;
import com.codeanalyzer.ingest.mongo.id.Identity;
import com.codeanalyzer.ingest.mongo.id.Ids;
import com.codeanalyzer.ingest.mongo.map.ArtifactMapper;
import com.codeanalyzer.ingest.mongo.map.DocSink;
import com.codeanalyzer.ingest.mongo.map.IngestContext;
import com.codeanalyzer.ingest.mongo.map.Splitter;
import com.codeanalyzer.ingest.mongo.model.RunMember;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;

import java.io.IOException;
import java.io.InputStream;

/**
 * {@code api/statements/index.json} → {@code am_statement} (+ its split arrays and {@code am_sql_text}).
 *
 * <h2>Streamed, not read whole</h2>
 *
 * <p>This is the largest artifact — 20.7 MB across five dialects, 15,824 rows — and the target
 * workload is 1,000,000 statements. Reading the document into a tree first would put the entire
 * array in memory before the first row is written, which is the failure already measured upstream
 * in the analyser. So the file is parsed as a token stream and each row is emitted as it is read.
 *
 * <h2>Why the key has five parts</h2>
 *
 * <p>{@code (nodeId, operation, line)} looks like a natural key and is not one: one statement can
 * touch several tables at the same line, and it collides on 63 ORACLE / 81 POSTGRES / 64 DB2 rows.
 * Keyed on three parts, ingest would upsert those on top of each other and lose 208 statements
 * with no error and no count to reveal it. Schema and table are therefore part of the key.
 *
 * <h2>{@code sqlText} is stored once</h2>
 *
 * <p>Measured, captured SQL is 3.7 MB of which 1.0 MB is distinct — 73% duplication, because the
 * same statement is recorded against each table it touches. The text goes to a content-addressed
 * collection and the statement keeps a reference, so the duplication costs one hash per row
 * instead of a copy of the text.
 */
public final class StatementsMapper implements ArtifactMapper {

    public static final String ARTIFACT = "api/statements/index.json";

    private final ObjectMapper json = new ObjectMapper();

    @Override
    public String artifact() {
        return ARTIFACT;
    }

    @Override
    public void map(IngestContext ctx, InputStream in, DocSink sink) throws IOException {
        try (var parser = json.getFactory().createParser(in)) {
            while (parser.nextToken() != null) {
                if (parser.currentToken() == JsonToken.FIELD_NAME
                        && "statements".equals(parser.currentName())) {
                    parser.nextToken();                       // START_ARRAY
                    while (parser.nextToken() == JsonToken.START_OBJECT) {
                        emit(ctx, sink, json.readTree(parser));
                    }
                    return;                                   // nothing after the array matters
                }
            }
        }
    }

    private void emit(IngestContext ctx, DocSink sink, JsonNode row) {
        String nodeId = text(row, "nodeId");
        String schema = text(row, "schema");
        String table  = text(row, "tableName");
        String op     = text(row, "operation");
        int line      = row.path("line").asInt(0);

        // Prefer the node's real entity key; fall back to computing one so a statement whose node
        // was not in api/nodes/ is still stored. Losing the row would be a worse answer than
        // storing it with a reference the verifier can report as dangling.
        String nodeRef = nodeId == null ? null : ctx.nodeId(nodeId);
        if (nodeRef == null && nodeId != null) nodeRef = ctx.entityKey(nodeId);

        String entityKey = IdCodec.statement(
                nodeRef == null ? ctx.entityKey("?") : nodeRef, op, line, schema, table).key();

        Document d = Splitter.flatten(ctx, sink, Collections.STATEMENT, entityKey, row);
        d.put("runId", ctx.runId());
        if (nodeRef != null) d.put("nodeRef", nodeRef);
        if (op != null) d.put("operation", op);
        d.put("line", line);
        if (schema != null) d.put("schemaName", schema);
        if (table != null) d.put("tableName", table);
        if (schema == null || table == null) d.put("unresolved", true);

        String objectRef = ctx.objectId(schema, table);
        if (objectRef != null) d.put("objectRef", objectRef);

        // The SQL text moves to its own content-addressed row and leaves a reference behind.
        Object sql = d.remove("sqlText");
        if (sql instanceof String s && !s.isEmpty()) {
            String hash = ContentHasher.hash(new Document("t", s));
            Ids textId = IdCodec.sqlText(hash);
            Document textDoc = new Document("entityKey", textId.key())
                    .append("_id", hash)
                    .append("contentHash", hash)
                    .append("text", s)
                    .append("length", s.length());
            sink.emit(Collections.SQL_TEXT, textDoc);
            d.put("sqlTextRef", textId.key());
        }

        Identity ident = Identity.ofSource(entityKey, entityKey, row);
        ident.stampOnto(d);
        sink.emit(Collections.STATEMENT, d);
        sink.emit(RunMember.COLLECTION,
                RunMember.of(ctx.runId(), ident, Collections.STATEMENT, null));
    }

    private static String text(JsonNode row, String field) {
        JsonNode n = row.path(field);
        return n.isMissingNode() || n.isNull() ? null : n.asText();
    }
}
