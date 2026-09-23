package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.analyzer.storage.mongo.QueryBudget;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * Mode 4 read-side, "statements" — investigated and found <b>disqualified for real use, not
 * wired into the controller</b>. Kept as a record of the investigation, not a live code path.
 *
 * <h2>What this was meant to be</h2>
 *
 * <p>Measured before building: {@code ParserFacetSpecs} has exactly seven entries ({@code TABLES},
 * {@code PROCEDURES}, {@code SEQUENCES}, {@code CURSORS}, {@code JOINS}, {@code DYNAMIC_SQL},
 * {@code PREDICATES}) — there is no pageable {@code statements} facet at all. The only {@code
 * statements} endpoint in this API is {@code GET .../statements/summary} ({@link
 * StatementsSummaryService}), a per-node aggregation of statement-type counts, structurally
 * different from the other seven's paged-grid shape. This class was meant to be that aggregation's
 * entity-backed twin.
 *
 * <h2>Why it's disqualified — found only by testing against real data, not assumed correct</h2>
 *
 * <p>{@code StatementsSummaryService} reads {@code counts.statements} from {@code api/index.json}'s
 * own per-node summary — a per-<em>statement-occurrence</em> count across every statement type the
 * parser recognises (SELECT, LOOP, IF, ASSIGNMENT, CURSOR_FOR_LOOP, EXECUTE_IMMEDIATE, CASE,
 * SUBQUERY, RAISE, COMMIT, RETURN, and more — 27 types on the real corpus). {@code am_statement} is
 * a fundamentally different unit: one row per <em>table access</em> within a statement, populated
 * only for statements that touch a table at all. Grouping it by {@code (nodeRef, operation)} does
 * <b>not</b> reproduce {@code counts.statements} — confirmed by direct HTTP comparison against the
 * real, rebuilt, restarted app: only 6 of the real 27 statement types appear at all (every
 * non-table-touching type — LOOP, IF, ASSIGNMENT, CASE, RAISE, COMMIT, RETURN, etc. — is entirely
 * absent, because {@code am_statement} was never populated for those), and even the 6 shared types
 * disagree on count for a structural reason, not a bug: {@code am_statement} counts once per table
 * touched, {@code counts.statements} counts once per statement regardless of how many tables it
 * touches — SELECT measured 403 real occurrences vs. 2082 derived here (a single multi-table SELECT
 * inflates the derived count by however many tables it joins).
 *
 * <p>There is no cheap fix analogous to {@code procedures}' {@code exceptionHandlers} derivation
 * (§155) — that worked because an already-split child collection happened to carry the exact right
 * unit of count. Here the right unit (one row per statement, of every type, table-touching or not)
 * simply isn't ingested anywhere; reproducing it for real would need a new {@code api/index.json}
 * mapper, the same category of gap §154 first found for {@code counts.exceptionHandlers} before a
 * cheaper derivation was found for that one specific field. Left unbuilt rather than guessed at.
 *
 * <h2>Scope-predicate reuse — the one part of this investigation that IS reusable</h2>
 *
 * <p>{@link ParserScopeResolver#predicate} only ever reads {@code nodeId} off the {@code JsonNode}
 * it is given (confirmed by reading its own implementation) — it resolves the allowed id set from
 * the call tree via {@code AnalysisService}, not from the row source. So the exact same resolved
 * {@code Predicate<JsonNode>} the artifact-scan path already builds can be reused unchanged by any
 * future entity-backed reader needing scope: build one minimal {@code {nodeId: "..."}} node per
 * candidate procedure and test it, rather than reimplementing Direct/Subtree/SubFlow/Full a second
 * time. That pattern is demonstrated correctly below even though the aggregation itself is not used.
 */
@Component
public class EntityStatementsSummaryReader {

    private final ObjectMapper mapper;

    public EntityStatementsSummaryReader(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * @param db             the entity-ingest database
     * @param analysisName   the parser's own {@code DIALECT~PROJECT~ENV~ENTRYPOINT} analysis name
     * @param scopePredicate the same predicate {@code ParserScopeResolver.predicate} already
     *                       produces for this request, or {@code null} for no filtering
     * @return the same array shape {@code StatementsSummaryService.getSummary} already produces, or
     *         {@code null} if no {@code am_run} matches (caller falls back to artifact-scan)
     */
    public JsonNode getSummary(MongoDatabase db, String analysisName, Predicate<JsonNode> scopePredicate) {
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

        // nodeRef -> {nodeId, name, schema, sourceFile}, and which pass the scope predicate.
        record NodeInfo(String nodeId, String name, String schema, String sourceFile, boolean inScope) {}
        Map<String, NodeInfo> nodes = new LinkedHashMap<>();
        try (MongoCursor<Document> cur = QueryBudget.request(db.getCollection("am_proc_node"))
                .find(Filters.eq("runId", runId)).iterator()) {
            while (cur.hasNext()) {
                Document d = cur.next();
                String nodeId = d.getString("nodeId");
                boolean inScope = true;
                if (scopePredicate != null) {
                    ObjectNode probe = mapper.createObjectNode();
                    probe.put("nodeId", nodeId == null ? "" : nodeId);
                    inScope = scopePredicate.test(probe);
                }
                nodes.put(d.getString("entityKey"), new NodeInfo(
                        nodeId, d.getString("name"), d.getString("schema"), d.getString("sourceFile"), inScope));
            }
        }
        // BUGFIX (§267): `null`, NOT an empty result. An empty result is an ANSWER — the caller
        // takes it as "the entity store says this analysis has no statements" and serves it. But a
        // current run with zero rows does not mean that; it means this run was never ingested, or
        // its ingest died part-way and still left an `am_run` row behind. Confirmed live: the
        // SQLSERVER USP_RUNMRPCYCLE v1 run was `isCurrent: true, status: COMPLETE` with 0
        // `am_proc_node` and 0 `am_subtree_table` rows (the v2 re-ingest that had the real data
        // aborted on an E11000 and so never became current), and every entity-backed endpoint
        // served `[]` for an analysis whose artifacts held 55 procedures and 68 tables.
        //
        // Returning `null` is the contract this method already documents: the caller falls back to
        // the artifact scan. When the analysis genuinely has no statements the artifact scan returns
        // the same empty result anyway, so the only cost of being wrong in this direction is one
        // cheap read — whereas being wrong in the other direction silently blanks the UI.
        if (nodes.isEmpty()) return null;

        // (nodeRef, operation) -> count, aggregated server-side rather than pulling every row.
        record Key(String nodeRef, String operation) {}
        Map<Key, Long> counts = new LinkedHashMap<>();
        try (MongoCursor<Document> cur = QueryBudget.request(db.getCollection("am_statement"))
                .aggregate(List.of(
                        new Document("$match", new Document("runId", runId)),
                        new Document("$group", new Document("_id",
                                new Document("nodeRef", "$nodeRef").append("operation", "$operation"))
                                .append("c", new Document("$sum", 1)))))
                .iterator()) {
            while (cur.hasNext()) {
                Document d = cur.next();
                Document id = (Document) d.get("_id");
                String nodeRef = id.getString("nodeRef");
                String operation = id.getString("operation");
                if (nodeRef == null || operation == null) continue;
                counts.merge(new Key(nodeRef, operation), d.get("c", Number.class).longValue(), Long::sum);
            }
        }

        Map<String, ObjectNode> typeMap = new LinkedHashMap<>();
        Map<String, List<ObjectNode>> procsByType = new LinkedHashMap<>();

        for (var e : counts.entrySet()) {
            NodeInfo node = nodes.get(e.getKey().nodeRef());
            if (node == null || !node.inScope()) continue;
            long count = e.getValue();
            if (count <= 0) continue;
            String stype = e.getKey().operation().toUpperCase(Locale.ROOT);

            ObjectNode typeEntry = typeMap.get(stype);
            if (typeEntry == null) {
                typeEntry = mapper.createObjectNode();
                typeEntry.put("type", stype);
                typeEntry.put("totalCount", 0L);
                typeEntry.put("procCount", 0);
                typeMap.put(stype, typeEntry);
                procsByType.put(stype, new ArrayList<>());
            }
            typeEntry.put("totalCount", typeEntry.path("totalCount").asLong(0) + count);
            typeEntry.put("procCount", typeEntry.path("procCount").asInt(0) + 1);

            ObjectNode proc = mapper.createObjectNode();
            proc.put("nodeId", node.nodeId() == null ? "" : node.nodeId());
            String name = node.name() == null || node.name().isEmpty() ? node.nodeId() : node.name();
            proc.put("name", name == null ? "" : name);
            proc.put("schema", node.schema() == null ? "" : node.schema());
            proc.put("sourceFile", node.sourceFile() == null ? "" : node.sourceFile());
            proc.put("count", count);
            procsByType.get(stype).add(proc);
        }

        ArrayNode result = mapper.createArrayNode();
        for (var e : typeMap.entrySet()) {
            List<ObjectNode> procs = procsByType.get(e.getKey());
            procs.sort((a, b) -> Long.compare(b.path("count").asLong(0), a.path("count").asLong(0)));
            ArrayNode procsOut = e.getValue().putArray("procs");
            for (ObjectNode p : procs) procsOut.add(p);
            result.add(e.getValue());
        }
        return result;
    }

    private static Document caseInsensitiveEq(String field, String value) {
        return new Document(field, Pattern.compile("^" + Pattern.quote(value) + "$",
                Pattern.CASE_INSENSITIVE));
    }
}
