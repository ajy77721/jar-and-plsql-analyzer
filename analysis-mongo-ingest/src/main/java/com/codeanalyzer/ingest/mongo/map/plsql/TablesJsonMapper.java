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
import java.util.Iterator;
import java.util.Map;

/**
 * {@code tables.json} → {@code am_table_column_usage}.
 *
 * <h2>Why this artifact exists as its own mapper</h2>
 *
 * <p>{@code tables.json} was very nearly discarded as "the map-keyed form of
 * {@code api/tables/index.json}". It is not. It carries a {@code columns[]} structure that appears
 * in <b>no other document</b> — per column: {@code usages}, {@code valueSources},
 * {@code referencedBy}, {@code intoVariables}, {@code sampleExpression}, {@code lines} and
 * {@code lineCount}. Measured: <b>132 rows across 34 of 42 tables</b>, matching the analyser's own
 * {@code columnCoverage.columnRows: 132}.
 *
 * <p>That is a per-column aggregate — exactly what a table-detail screen renders — and it was found
 * only by diffing leaf field paths rather than comparing container shapes. Had the original verdict
 * stood, it would have vanished at migration and the loss would have shown up as an empty panel
 * long after anyone connected it to the schema.
 *
 * <p>Rows are <b>referenced</b>, not embedded in {@code am_db_object}: the aggregate is queried on
 * its own (all usages of a column, across tables) and embedding would force the whole table
 * document to be read to answer that.
 */
public final class TablesJsonMapper implements ArtifactMapper {

    public static final String ARTIFACT = "tables.json";

    private final ObjectMapper json = new ObjectMapper();

    @Override
    public String artifact() {
        return ARTIFACT;
    }

    @Override
    public void map(IngestContext ctx, InputStream in, DocSink sink) throws IOException {
        JsonNode root = json.readTree(in);
        if (!root.isObject()) return;

        Iterator<Map.Entry<String, JsonNode>> it = root.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            JsonNode table = e.getValue();
            String schema = table.path("schema").asText(null);
            String name   = table.path("tableName").asText(null);

            // Resolve the parent from the lookup the previous mapper filled. No read-back.
            String objectRef = ctx.objectId(schema, name);
            if (objectRef == null) {
                // Fall back to computing it: the object may legitimately not be in
                // api/tables/index.json. Computing rather than skipping keeps the row — the
                // reference may dangle, and the verifier reports that, but the DATA is not lost.
                objectRef = IdCodec.dbObject(ctx.runId(), schema, name).id();
            }

            JsonNode columns = table.path("columns");
            if (!columns.isArray()) continue;

            for (JsonNode col : columns) {
                String columnName = col.path("columnName").asText(null);
                if (columnName == null) continue;

                Ids id = IdCodec.tableColumnUsage(objectRef, columnName);
                String colEntityKey = ctx.entityKey(schema, name, columnName);
                Document d = Splitter.flatten(ctx, sink,
                        Collections.TABLE_COLUMN_USAGE, colEntityKey, col);
                d.put("runId", ctx.runId());
                d.put("objectRef", objectRef);
                d.put("schemaName", schema);
                d.put("tableName", name);
                d.put("columnName", columnName);
                Identity ident = Identity.ofSource(colEntityKey, id.key(), col);
                d.put("objectRef", objectRef);
                ident.stampOnto(d);
                sink.emit(Collections.TABLE_COLUMN_USAGE, d);
                sink.emit(RunMember.COLLECTION,
                        RunMember.of(ctx.runId(), ident, Collections.TABLE_COLUMN_USAGE, null));
            }
        }
    }
}
