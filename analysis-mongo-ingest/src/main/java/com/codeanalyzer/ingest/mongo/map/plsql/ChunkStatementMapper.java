package com.codeanalyzer.ingest.mongo.map.plsql;

import com.codeanalyzer.ingest.mongo.id.IdCodec;
import com.codeanalyzer.ingest.mongo.id.Identity;
import com.codeanalyzer.ingest.mongo.id.Ids;
import com.codeanalyzer.ingest.mongo.map.ArtifactMapper;
import com.codeanalyzer.ingest.mongo.map.DocSink;
import com.codeanalyzer.ingest.mongo.map.IngestContext;
import com.codeanalyzer.ingest.mongo.map.Splitter;
import com.codeanalyzer.ingest.mongo.model.RunMember;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;

import java.io.IOException;
import java.io.InputStream;

/**
 * {@code chunks/chunk_*.json} → {@code am_chunk_statement}: every statement, not just the ones that
 * touch a table.
 *
 * <h2>Why this is a second statement collection rather than more fields on the first</h2>
 *
 * <p>{@code api/statements/index.json} is a projection built for the Statements grid: one row per
 * (statement, table) pair, carrying the columns that grid shows. Measured on ORACLE it holds 2,436
 * rows against the chunks' <b>3,832</b>, and the difference is not detail — it is whole categories
 * of statement. {@code IF}, {@code FOR_LOOP}, {@code CASE}, {@code EXIT} and {@code ASSIGNMENT}
 * touch no table, so they are absent from the api index entirely.
 *
 * <p>Eight fields exist only here: {@code type}, {@code nestingDepth}, {@code lineEnd},
 * {@code dmlType}, {@code boundsExpression}, {@code indexVariable}, {@code savepointName} and
 * {@code tables}. {@code nestingDepth} in particular is what makes "which statements are inside
 * this loop" answerable, and nothing in the api index can reconstruct it.
 *
 * <p>Merging the two would mean either dropping the api index's per-table fan-out or inventing
 * null-filled rows for every control-flow statement. They answer different questions and are stored
 * separately, which is also what lets the round-trip prover check each against its own source.
 */
public final class ChunkStatementMapper implements ArtifactMapper {

    /** Driven by directory listing, like the node files — one chunk per procedure. */
    public static final String DIRECTORY = "chunks";

    private final ObjectMapper json = new ObjectMapper();

    @Override
    public String artifact() {
        return DIRECTORY;
    }

    @Override
    public void map(IngestContext ctx, InputStream in, DocSink sink) throws IOException {
        JsonNode chunk = json.readTree(in);
        String nodeId = chunk.path("nodeId").asText(null);
        if (nodeId == null) return;

        // The chunk's own node may not have been seen — a chunk exists per parsed unit, and the
        // node files are a separate artifact. Falling back to the computed key keeps the row rather
        // than dropping it; RefAssertion will report it if the node genuinely never arrives.
        String nodeRef = ctx.nodeId(nodeId);
        if (nodeRef == null) nodeRef = ctx.entityKey(nodeId);

        JsonNode statements = chunk.path("statements");
        if (!statements.isArray()) return;

        int ordinal = 0;
        for (JsonNode st : statements) {
            int line = st.path("line").asInt(0);
            // Ordinal is part of the key, not decoration: two statements can share a line — an
            // assignment inside a single-line IF is the common case — and keying on line alone
            // would upsert one over the other and lose it with no error.
            Ids id = IdCodec.nodeChild(Collections.CHUNK_STATEMENT, nodeRef, "chunkstmt", line, ordinal);

            Document d = Splitter.flatten(ctx, sink, Collections.CHUNK_STATEMENT, id.key(), st);
            d.put("runId", ctx.runId());
            d.put("nodeRef", nodeRef);
            d.put("key", id.key());
            d.put("ordinal", ordinal);
            String type = st.path("type").asText(null);
            if (type != null) d.put("statementType", type);

            Identity ident = Identity.ofSource(id.key(), id.key(), st);
            // BUGFIX (§266, same defect as CallGraphMapper's am_call_edge_unique): deliberately
            // NOT `ident.stampOnto(d)`. stampOnto sets `_id` to a *content* hash, and
            // BulkWriter.upsert() matches on `_id` whenever one is present — but this document
            // also carries `key`, which has a unique index (`uk_key`). Re-analysing the same
            // source with any statement changed therefore computes a NEW `_id`, misses the
            // existing row entirely, and tries to INSERT a second document under the SAME `key`.
            // MongoDB rejects it with E11000, the bulk write throws, and the whole ingest run is
            // marked FAILED — which then makes every entity-backed endpoint fall back to the
            // previous (possibly empty) run. Confirmed live on SQLSERVER: re-running
            // USP_RUNMRPCYCLE failed on `SQLSERVER~ERP$USP_EDGEOPS~chunkstmt~23~15` and four
            // siblings, all of which already existed from an earlier run with different content.
            //
            // `key` cannot simply be dropped instead: Splitter passes `id.key()` as the parentRef
            // of the split-out `am_chunk_statement__tables` children, so the parent must carry it
            // for that join to resolve. A chunk statement is therefore a current-state row keyed
            // by its natural (node, line, ordinal) identity — matching on `key` gives true
            // upsert-replace semantics. The content hash is still recorded in `contentHash` and
            // is still what am_run_member points at, so per-run history is unaffected.
            d.put("entityKey", ident.entityKey());
            d.put("contentHash", ident.id());
            sink.emit(Collections.CHUNK_STATEMENT, d);
            sink.emit(RunMember.COLLECTION,
                    RunMember.of(ctx.runId(), ident, Collections.CHUNK_STATEMENT, null));
            ordinal++;
        }
    }
}
