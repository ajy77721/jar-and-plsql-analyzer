package com.codeanalyzer.ingest.mongo.map.plsql;

import com.codeanalyzer.ingest.mongo.id.IdCodec;
import com.codeanalyzer.ingest.mongo.id.Ids;
import com.codeanalyzer.ingest.mongo.map.ArtifactMapper;
import com.codeanalyzer.ingest.mongo.map.DocSink;
import com.codeanalyzer.ingest.mongo.map.IngestContext;
import com.codeanalyzer.ingest.mongo.id.Identity;
import com.codeanalyzer.ingest.mongo.map.JsonDocs;
import com.codeanalyzer.ingest.mongo.map.Splitter;
import com.codeanalyzer.ingest.mongo.model.RunMember;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;

import java.io.IOException;
import java.io.InputStream;

/**
 * {@code api/tables/index.json} → {@code am_db_object}.
 *
 * <p>The catalogue entry for each table: its operations, the nodes that use it, and its database
 * columns, constraints and indexes where the analyser could resolve them. Measured at 42 objects
 * for the ORACLE analysis.
 *
 * <p><b>Correction.</b> This said {@code dbColumns}, {@code dbConstraints} and {@code dbIndexes}
 * "stay embedded", on a sound measurement: 1.2 columns per object on average, worst case around 40,
 * bounded, owned by exactly one object and always read with it — so a join per read would buy
 * nothing. That reasoning still holds, and it is <b>not what happens</b>. They are arrays, and the
 * no-arrays invariant splits every array, so they land in {@code am_db_object__db_columns},
 * {@code __db_constraints} and {@code __db_indexes}. Measured 160 / 108 / 47 rows on the ORACLE
 * analysis.
 *
 * <p>The invariant wins for a reason the measurement did not weigh: "bounded today" is a property
 * of the data, and a document that embeds an array is one schema change away from being unbounded.
 * But the comment was asserting the opposite of the behaviour, which is worse than either choice —
 * a reader trusting it would look for columns on the parent document and find none.
 *
 * <p>PLAN-04 names these {@code am_db_column} / {@code am_db_constraint} / {@code am_db_index}.
 * Reconciling the planned names with the {@code parent__field} convention is an open decision; see
 * §17 of the task registry.
 *
 * <p>This mapper also populates the context's object lookup, which is how the column-usage rows
 * that arrive later resolve their parent without a database round-trip.
 */
public final class TablesIndexMapper implements ArtifactMapper {

    public static final String ARTIFACT = "api/tables/index.json";

    private final ObjectMapper json = new ObjectMapper();

    @Override
    public String artifact() {
        return ARTIFACT;
    }

    @Override
    public void map(IngestContext ctx, InputStream in, DocSink sink) throws IOException {
        JsonNode root = json.readTree(in);
        JsonNode tables = root.path("tables");
        if (!tables.isArray()) return;

        for (JsonNode t : tables) {
            String schema = t.path("schema").isNull() ? null : t.path("schema").asText(null);
            String name   = t.path("name").isNull()   ? null : t.path("name").asText(null);

            // 17 SQL Server rows carry {"schema":null,"name":null}. The lossless rule says store
            // them; the id recipe still yields a distinct key per run because the remaining
            // components differ, and `unresolved` marks them so a reader is never misled into
            // treating a nameless row as a real object.
            Ids id = IdCodec.dbObject(ctx.runId(), schema, name);
            String entityKey = ctx.entityKey(schema, name);

            // Arrays become their own collections, keyed under the ENTITY so they dedup with it.
            Document d = Splitter.flatten(ctx, sink, Collections.DB_OBJECT, entityKey, t);
            d.put("runId", ctx.runId());
            d.put("schemaName", schema);
            d.put("objectName", name == null ? "" : name);
            if (schema == null || name == null) d.put("unresolved", true);

            Identity ident = Identity.ofSource(entityKey, id.key(), t);
            ident.stampOnto(d);
            sink.emit(Collections.DB_OBJECT, d);
            sink.emit(RunMember.COLLECTION,
                    RunMember.of(ctx.runId(), ident, Collections.DB_OBJECT, null));
            // Children reference the run-scoped key, which they can compute themselves.
            // Children point at the ENTITY, not at one run's occurrence of it: a content hash
            // changes whenever content changes, so a reference to it would break on every edit.
            ctx.putObjectId(schema, name, entityKey);
        }
    }
}
