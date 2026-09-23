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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Mode 4 read-side, facet 5 (see {@code EntityTablesReader}/{@code EntityJoinsReader}/
 * {@code EntityCursorsReader}/{@code EntitySequencesReader}'s own doc comments for the pattern and
 * discipline): rebuilds {@code ParserFacetSpecs.PROCEDURES}'s exact row shape — {@code [{nodeId,
 * name, schema, objectType, depth, linesOfCode, counts:{exceptionHandlers}}]} — from
 * {@code am_proc_node} + {@code am_exception_handler} instead of scanning {@code api/index.json}.
 *
 * <h2>Escalated once (§154), resolved without a new mapper</h2>
 *
 * <p>{@code am_proc_node} (written by {@code NodeDetailMapper}, which ingests {@code
 * api/nodes/*.json}) carries every field this facet reads except {@code counts} — that object
 * lives only in {@code api/index.json}'s own embedded per-node summary, a different artifact, and
 * {@code analysis-mongo-ingest} has no mapper for it. §154 escalated rather than fake
 * {@code exceptionHandlers} as always-0, which would silently break UI-029's hidden {@code cf=9:Y}
 * filter under {@code ?source=entity}.
 *
 * <p>The only {@code counts.*} subfield this facet actually reads is {@code exceptionHandlers}
 * (confirmed directly against {@code ParserFacetSpecs.PROCEDURES} — no other {@code counts.*} field
 * is used), and it turns out to be free: {@code NodeDetailMapper.emitChildren} already splits each
 * node's {@code exceptionHandlers[]} array into its own {@code am_exception_handler} collection,
 * one row per handler, keyed back to the owning node by {@code nodeRef}. Counting those rows per
 * node reproduces {@code exceptionHandlers} exactly — confirmed against the real corpus before
 * writing this class: {@code am_exception_handler.countDocuments({nodeRef: <ERP_USP_RUN_CYCLE's
 * entityKey>})} returns 86, matching the number named in §154's own escalation. No new artifact
 * mapper needed, just a derived count.
 *
 * <h2>Cheapest join shape of the five facets so far</h2>
 *
 * <p>{@code am_proc_node} is itself the base row — no join into it is needed the way every prior
 * facet joined <em>into</em> {@code am_proc_node} for display fields. The one join is the reverse
 * direction: {@code am_exception_handler} grouped by {@code nodeRef}, one aggregation query,
 * batched over every node in the run.
 *
 * <h2>Real bug found in production use: {@code detailFile} was missing entirely</h2>
 *
 * <p>This class was verified (§154/§155) only against what {@code ParserFacetSpecs.PROCEDURES}
 * itself reads — the grid's own field usage. It was never checked against the *other* real
 * consumer of this same endpoint's data: {@code parser/js/app.js}'s {@code PA.showProcedure}, which
 * builds {@code PA.nodeIndex} from this exact response and then reads each node's
 * {@code detailFile} to fetch the full per-node detail (source, rich counts, variables) via
 * {@code GET /node/{file}.json} — an endpoint outside this facet entirely, always artifact-backed.
 * Without {@code detailFile}, {@code PA.showProcedure}'s {@code if (detailFile)} branch never ran:
 * clicking a procedure showed no source, no detail header counts, nothing — a real, live regression
 * under {@code ?source=entity}/the mode-wide toggle, found by actually using the app, not by any of
 * this session's own facet-comparison tests (which correctly checked only the grid's own fields).
 * Added {@code detailFile} (computed as {@code "nodes/" + nodeId + ".json"}, matching the real
 * convention confirmed against the artifact-scan path's own output for this corpus) plus
 * {@code sourceFile}/{@code lineStart}/{@code lineEnd} (already on {@code am_proc_node}, free to
 * include) so the existing merge logic in {@code PA.showProcedure} has something to fetch and merge
 * over. <b>Known remaining gap, stated not assumed away:</b> for a package analysis, the artifact
 * path's own {@code detailFile} can point into a proc subfolder (see {@code IndexFacetReader
 * .getProcedures}'s own package-analysis enrichment) — this simple top-level construction does not
 * reproduce that; not exercised by this repo's own standalone-entrypoint corpus, and not fixed here.
 */
@Component
public class EntityProceduresReader {

    private final ObjectMapper mapper;

    public EntityProceduresReader(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * @param db            the entity-ingest database
     * @param analysisName  the parser's own {@code DIALECT~PROJECT~ENV~ENTRYPOINT} analysis name
     * @return the same bare-array shape the artifact-scan {@code PROCEDURES} facet's document
     *         already is, or {@code null} if no {@code am_run} matches (caller falls back to
     *         artifact-scan)
     */
    public JsonNode readProcedures(MongoDatabase db, String analysisName) {
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

        record ProcNode(String entityKey, String nodeId, String name, String schema,
                        String objectType, int depth, int linesOfCode, String sourceFile,
                        int lineStart, int lineEnd) {}
        List<ProcNode> nodes = new java.util.ArrayList<>();
        try (MongoCursor<Document> cur = QueryBudget.request(db.getCollection("am_proc_node"))
                .find(Filters.eq("runId", runId))
                .sort(Sorts.ascending("depth", "name")).iterator()) {
            while (cur.hasNext()) {
                Document d = cur.next();
                nodes.add(new ProcNode(
                        d.getString("entityKey"),
                        d.getString("nodeId"),
                        d.getString("name"),
                        d.getString("schema"),
                        d.getString("objectType"),
                        d.get("depth") == null ? 0 : ((Number) d.get("depth")).intValue(),
                        d.get("linesOfCode") == null ? 0 : ((Number) d.get("linesOfCode")).intValue(),
                        d.getString("sourceFile"),
                        d.get("lineStart") == null ? 0 : ((Number) d.get("lineStart")).intValue(),
                        d.get("lineEnd") == null ? 0 : ((Number) d.get("lineEnd")).intValue()));
            }
        }
        // BUGFIX (§267): `null`, NOT an empty result. An empty result is an ANSWER — the caller
        // takes it as "the entity store says this analysis has no procedures" and serves it. But a
        // current run with zero rows does not mean that; it means this run was never ingested, or
        // its ingest died part-way and still left an `am_run` row behind. Confirmed live: the
        // SQLSERVER USP_RUNMRPCYCLE v1 run was `isCurrent: true, status: COMPLETE` with 0
        // `am_proc_node` and 0 `am_subtree_table` rows (the v2 re-ingest that had the real data
        // aborted on an E11000 and so never became current), and every entity-backed endpoint
        // served `[]` for an analysis whose artifacts held 55 procedures and 68 tables.
        //
        // Returning `null` is the contract this method already documents: the caller falls back to
        // the artifact scan. When the analysis genuinely has no procedures the artifact scan returns
        // the same empty result anyway, so the only cost of being wrong in this direction is one
        // cheap read — whereas being wrong in the other direction silently blanks the UI.
        if (nodes.isEmpty()) return null;

        // exceptionHandlers count per node, derived from am_exception_handler's own already-split
        // rows rather than a new artifact mapper — see this class's own doc comment.
        Map<String, Integer> handlerCounts = new LinkedHashMap<>();
        try (MongoCursor<Document> cur = QueryBudget.request(db.getCollection("am_exception_handler"))
                .aggregate(List.of(
                        new Document("$match", new Document("runId", runId)),
                        new Document("$group", new Document("_id", "$nodeRef")
                                .append("c", new Document("$sum", 1)))))
                .iterator()) {
            while (cur.hasNext()) {
                Document d = cur.next();
                handlerCounts.put(d.getString("_id"), d.getInteger("c", 0));
            }
        }

        ArrayNode out = mapper.createArrayNode();
        for (ProcNode n : nodes) {
            ObjectNode row = mapper.createObjectNode();
            row.put("nodeId", n.nodeId() == null ? "" : n.nodeId());
            row.put("name", n.name() == null ? "" : n.name());
            row.put("schema", n.schema() == null ? "" : n.schema());
            row.put("objectType", n.objectType() == null ? "" : n.objectType());
            row.put("depth", n.depth());
            row.put("linesOfCode", n.linesOfCode());
            if (n.sourceFile() != null) row.put("sourceFile", n.sourceFile());
            if (n.lineStart() > 0) row.put("lineStart", n.lineStart());
            if (n.lineEnd() > 0) row.put("lineEnd", n.lineEnd());
            // The real fix for the "clicking a procedure shows nothing" bug — see this class's own
            // doc comment. Standalone-entrypoint convention only; see the doc comment's own stated
            // package-analysis gap.
            if (n.nodeId() != null && !n.nodeId().isEmpty()) {
                row.put("detailFile", "nodes/" + n.nodeId() + ".json");
            }
            ObjectNode counts = row.putObject("counts");
            counts.put("exceptionHandlers", handlerCounts.getOrDefault(n.entityKey(), 0));
            out.add(row);
        }
        return out;
    }

    private static Document caseInsensitiveEq(String field, String value) {
        return new Document(field, Pattern.compile("^" + Pattern.quote(value) + "$",
                Pattern.CASE_INSENSITIVE));
    }
}
