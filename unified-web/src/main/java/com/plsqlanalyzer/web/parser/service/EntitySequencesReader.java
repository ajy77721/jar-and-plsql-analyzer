package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.analyzer.storage.mongo.QueryBudget;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Mode 4 read-side, facet 4 (see {@code EntityTablesReader}/{@code EntityJoinsReader}/
 * {@code EntityCursorsReader}'s own doc comments for the pattern and discipline — measure real
 * coverage per facet, do not assume it looks like the last one): rebuilds
 * {@code SequenceFacetReader.getSequences}'s exact row shape — {@code [{sequenceName,
 * sequenceSchema, operations:[...], accessCount, accessDetails:[{operation, procedureId,
 * procedureName, lineNumber, sourceFile}]}]} — from {@code am_sequence_use} + {@code am_proc_node}
 * instead of scanning every node file.
 *
 * <h2>Cheaper than tables/joins, on par with cursors — measured, not assumed</h2>
 *
 * <p>One collection carries the real usage data: {@code am_sequence_use}, one row per (node,
 * sequence, operation, line) — already flat, no split children ({@code NodeDetailMapper.emitSequences}
 * emits it directly, never through {@code Splitter}, since it is not derived from a JSON array of
 * objects on the node but built row-by-row in Java). {@code am_sequence_obj} exists too (one per
 * distinct schema.name, deduplicated) but is not needed here: every field this facet's row shape
 * wants — {@code sequenceName}/{@code sequenceSchema} — is already denormalised onto {@code
 * am_sequence_use} itself, the same reasoning {@code am_cursor_decl} carried its own display fields
 * so {@code EntityCursorsReader} did not need a second collection either.
 *
 * <p>The one join needed is {@code am_sequence_use.nodeRef} → {@code am_proc_node} for the owning
 * procedure's display name/id and source file — the same single batched lookup shape as
 * {@code EntityCursorsReader}/{@code EntityJoinsReader}.
 *
 * <h2>One real difference from the artifact-scan reader, stated rather than silently reproduced</h2>
 *
 * <p>{@code SequenceFacetReader.getSequences} re-derives each use's real line number by scanning
 * the node's own source text for {@code SEQ.NEXTVAL}/{@code NEXT VALUE FOR SEQ}, falling back to
 * the stored line only when that scan finds nothing — because older analyses stored the
 * body-start line for every usage, not the real one. {@code am_sequence_use.line} is written
 * straight from the same {@code sequences[].operations} array the artifact-scan reader's fallback
 * path reads, i.e. it is that pre-scan value, never the source-rescanned one. On the real
 * {@code ERP_USP_RUN_CYCLE} corpus this made no observed difference (confirmed by the comparison
 * test below matching exactly), but it is a real, stated limitation: an analysis whose stored line
 * numbers are the older body-start values would show different {@code lineNumber}s between the two
 * paths. Not fixed here — reproducing source-line rescanning against Mongo-stored data would need
 * its own investigation of where source text lives in entity storage, out of scope for one facet.
 */
@Component
public class EntitySequencesReader {

    private final ObjectMapper mapper;

    public EntitySequencesReader(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * @param db            the entity-ingest database
     * @param analysisName  the parser's own {@code DIALECT~PROJECT~ENV~ENTRYPOINT} analysis name
     * @return the same bare-array shape the artifact-scan {@code SEQUENCES} facet's document
     *         already is, or {@code null} if no {@code am_run} matches (caller falls back to
     *         artifact-scan)
     */
    public JsonNode readSequences(MongoDatabase db, String analysisName) {
        String[] parts = analysisName.split("~", 4);
        if (parts.length != 4) return null;
        String dialect = parts[0], project = parts[1], env = parts[2], entry = parts[3];

        Document run = QueryBudget.request(db.getCollection("am_run")).find(Filters.and(
                        Filters.eq("dialect", dialect),
                        Filters.eq("isCurrent", true),
                        caseInsensitiveEq("project", project),
                        caseInsensitiveEq("environment", env),
                        caseInsensitiveEq("entryPoint", entry)))
                .first();
        if (run == null) return null;
        String runId = run.getString("key");

        record SeqUse(String nodeRef, String schema, String name, String operation, int line) {}
        List<SeqUse> uses = new ArrayList<>();
        try (MongoCursor<Document> cur = QueryBudget.request(db.getCollection("am_sequence_use"))
                .find(Filters.eq("runId", runId))
                .sort(Sorts.ascending("nodeRef", "line")).iterator()) {
            while (cur.hasNext()) {
                Document d = cur.next();
                uses.add(new SeqUse(d.getString("nodeRef"), d.getString("schemaName"),
                        d.getString("sequenceName"), d.getString("operation"),
                        d.get("line") == null ? 0 : ((Number) d.get("line")).intValue()));
            }
        }
        // BUGFIX (§267): `null`, NOT an empty result. An empty result is an ANSWER — the caller
        // takes it as "the entity store says this analysis has no sequence uses" and serves it. But a
        // current run with zero rows does not mean that; it means this run was never ingested, or
        // its ingest died part-way and still left an `am_run` row behind. Confirmed live: the
        // SQLSERVER USP_RUNMRPCYCLE v1 run was `isCurrent: true, status: COMPLETE` with 0
        // `am_proc_node` and 0 `am_subtree_table` rows (the v2 re-ingest that had the real data
        // aborted on an E11000 and so never became current), and every entity-backed endpoint
        // served `[]` for an analysis whose artifacts held 55 procedures and 68 tables.
        //
        // Returning `null` is the contract this method already documents: the caller falls back to
        // the artifact scan. When the analysis genuinely has no sequence uses the artifact scan returns
        // the same empty result anyway, so the only cost of being wrong in this direction is one
        // cheap read — whereas being wrong in the other direction silently blanks the UI.
        if (uses.isEmpty()) return null;

        // nodeRef -> {nodeId, name, sourceFile}, batched.
        Set<String> nodeRefs = new LinkedHashSet<>();
        for (SeqUse u : uses) if (u.nodeRef() != null) nodeRefs.add(u.nodeRef());
        record NodeInfo(String nodeId, String name, String sourceFile) {}
        Map<String, NodeInfo> nodes = new LinkedHashMap<>();
        try (MongoCursor<Document> cur = QueryBudget.request(db.getCollection("am_proc_node"))
                .find(Filters.and(Filters.eq("runId", runId), Filters.in("entityKey", nodeRefs)))
                .iterator()) {
            while (cur.hasNext()) {
                Document d = cur.next();
                nodes.put(d.getString("entityKey"),
                        new NodeInfo(d.getString("nodeId"), d.getString("name"), d.getString("sourceFile")));
            }
        }

        // Grouped by schema.name, the same key SequenceFacetReader.getSequences uses.
        Map<String, ObjectNode> rows = new LinkedHashMap<>();
        for (SeqUse u : uses) {
            NodeInfo node = nodes.get(u.nodeRef());
            String nodeId = node == null || node.nodeId() == null ? "" : node.nodeId();
            String nodeName = node == null || node.name() == null ? "" : node.name();
            String sourceFile = node == null || node.sourceFile() == null ? "" : node.sourceFile();
            String seqName = u.name() == null ? "" : u.name();
            String seqSchema = u.schema() == null ? "" : u.schema();
            String mapKey = (seqSchema.isEmpty() ? "" : seqSchema + ".") + seqName;

            ObjectNode row = rows.computeIfAbsent(mapKey, k -> {
                ObjectNode n = mapper.createObjectNode();
                n.put("sequenceName", seqName);
                n.put("sequenceSchema", seqSchema);
                n.set("operations", mapper.createArrayNode());
                n.put("accessCount", 0);
                n.set("accessDetails", mapper.createArrayNode());
                return n;
            });

            addOp((ArrayNode) row.get("operations"), u.operation() == null ? "" : u.operation());
            row.put("accessCount", row.get("accessCount").asInt() + 1);

            ObjectNode det = mapper.createObjectNode();
            det.put("operation", u.operation() == null ? "" : u.operation());
            det.put("procedureId", nodeId);
            det.put("procedureName", nodeName);
            det.put("lineNumber", u.line());
            det.put("sourceFile", sourceFile);
            ((ArrayNode) row.get("accessDetails")).add(det);
        }

        ArrayNode out = mapper.createArrayNode();
        rows.values().forEach(out::add);
        return out;
    }

    private static void addOp(ArrayNode operations, String op) {
        for (JsonNode existing : operations) if (existing.asText().equals(op)) return;
        operations.add(op);
    }

    private static Document caseInsensitiveEq(String field, String value) {
        return new Document(field, Pattern.compile("^" + Pattern.quote(value) + "$",
                Pattern.CASE_INSENSITIVE));
    }
}
