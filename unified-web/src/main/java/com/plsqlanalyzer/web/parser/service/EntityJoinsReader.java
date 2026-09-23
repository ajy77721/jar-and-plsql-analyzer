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

/**
 * Mode 4 read-side, facet 2 (see {@code EntityTablesReader}'s own doc comment for the pilot's
 * pattern and discipline): rebuilds {@code ParserFacetSpecs.JOINS}'s own row shape —
 * {@code [{leftTable, rightTable, joinTypes:[...], accessDetails:[{procedureId, procedureName,
 * onPredicate, lineNumber}]}]} — from {@code am_*} entity collections instead of scanning the
 * parser's own joins artifact.
 *
 * <h2>Cheaper than the tables pilot, measured rather than assumed</h2>
 *
 * <p>Two collections carry join data, not one: {@code am_statement__joins} (keyed by statement
 * identity) and {@code am_node_table_access__joins} (keyed by node-table-access identity — the same
 * parent collection {@link EntityTablesReader} already reads). Table-pair semantics (this facet's
 * own row identity, {@code leftTable::rightTable}) map directly onto
 * {@code am_node_table_access__joins}'s own {@code leftTable}/{@code joinedTable} fields, so this
 * reader uses that collection alone — {@code am_statement__joins} is a genuine duplicate view of
 * the same underlying data (confirmed by matching row counts, 981/981, against the real
 * {@code ERP_USP_RUN_CYCLE} corpus), not a second source this facet needs. That leaves a
 * two-collection join ({@code am_node_table_access__joins} → {@code am_node_table_access} →
 * {@code am_proc_node} for the procedure's display name), simpler than the tables pilot's four.
 *
 * <h2>One real surprise, found only by testing against real data</h2>
 *
 * <p>A join sub-document omits {@code leftTable} entirely when the left side of the join is the
 * row's own owning table — an implicit self-reference the crawler doesn't bother repeating. Blindly
 * skipping rows with a null {@code leftTable} (the obvious first implementation) silently dropped
 * two real table pairs, 64 real join usages — a raw Mongo aggregate found 33 distinct pairs, the
 * first version of this reader produced 31. Fixed by falling back to
 * {@code am_node_table_access}'s own table name when the join's own {@code leftTable} is absent.
 *
 * <p>Second surprise, found by direct HTTP comparison against the artifact-scan path: the same
 * logical table pair is recorded with left/right flipped across different SQL statements that join
 * the two tables in opposite directions, and the artifact-scan path already merges both directions
 * into one row (27 rows there; this reader produced 33 direction-sensitive ones before being fixed
 * to group on the unordered pair instead — same 981 total access details either way).
 */
@Component
public class EntityJoinsReader {

    private final ObjectMapper mapper;

    public EntityJoinsReader(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * @param db            the entity-ingest database (only queried when entity-ingest is enabled
     *                      and reachable — callers check that before reaching here)
     * @param analysisName  the parser's own {@code DIALECT~PROJECT~ENV~ENTRYPOINT} analysis name
     * @return the same bare-array shape the artifact-scan {@code JOINS} facet's document already
     *         is, or {@code null} if no {@code am_run} matches (caller falls back to the
     *         artifact-scan path — this is never the only path, by design)
     */
    public JsonNode readJoins(MongoDatabase db, String analysisName) {
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

        record JoinRow(String parentRef, String leftTable, String joinedTable, String joinType,
                       String condition, int line) {}
        List<JoinRow> joinRows = new ArrayList<>();
        try (MongoCursor<Document> cur = QueryBudget.request(db.getCollection("am_node_table_access__joins"))
                .find(Filters.eq("runId", runId)).sort(new Document("ordinal", 1)).iterator()) {
            while (cur.hasNext()) {
                Document d = cur.next();
                Object lineVal = d.get("line");
                joinRows.add(new JoinRow(d.getString("parentRef"), d.getString("leftTable"),
                        d.getString("joinedTable"), d.getString("joinType"), d.getString("condition"),
                        lineVal == null ? 0 : ((Number) lineVal).intValue()));
            }
        }
        // BUGFIX (§267): `null`, NOT an empty result. An empty result is an ANSWER — the caller
        // takes it as "the entity store says this analysis has no joins" and serves it. But a
        // current run with zero rows does not mean that; it means this run was never ingested, or
        // its ingest died part-way and still left an `am_run` row behind. Confirmed live: the
        // SQLSERVER USP_RUNMRPCYCLE v1 run was `isCurrent: true, status: COMPLETE` with 0
        // `am_proc_node` and 0 `am_subtree_table` rows (the v2 re-ingest that had the real data
        // aborted on an E11000 and so never became current), and every entity-backed endpoint
        // served `[]` for an analysis whose artifacts held 55 procedures and 68 tables.
        //
        // Returning `null` is the contract this method already documents: the caller falls back to
        // the artifact scan. When the analysis genuinely has no joins the artifact scan returns
        // the same empty result anyway, so the only cost of being wrong in this direction is one
        // cheap read — whereas being wrong in the other direction silently blanks the UI.
        if (joinRows.isEmpty()) return null;

        // parentRef (am_node_table_access's own `key`) -> nodeRef + owning table name, batched.
        // The owning table matters because a join sub-document omits `leftTable` entirely when the
        // left side of the join *is* the row's own owning table (confirmed against real data — a
        // self/implicit join written without a redundant explicit left side); the artifact-scan
        // facet treats that omission as "this row's own table", not as an absent/unknown table, so
        // this reader must reproduce that fallback or it silently drops real join usages (found by
        // testing against real data: 31 pairs instead of the true 33).
        Set<String> parentKeys = new LinkedHashSet<>();
        for (JoinRow r : joinRows) parentKeys.add(r.parentRef());
        record AccessOwner(String nodeRef, String tableName) {}
        Map<String, AccessOwner> ownerByAccessKey = new LinkedHashMap<>();
        try (MongoCursor<Document> cur = QueryBudget.request(db.getCollection("am_node_table_access"))
                .find(Filters.and(Filters.eq("runId", runId), Filters.in("key", parentKeys)))
                .iterator()) {
            while (cur.hasNext()) {
                Document d = cur.next();
                ownerByAccessKey.put(d.getString("key"),
                        new AccessOwner(d.getString("nodeRef"), d.getString("name")));
            }
        }

        // nodeRef -> {display name, sourceFile}, batched.
        Set<String> nodeRefs = new LinkedHashSet<>();
        for (AccessOwner o : ownerByAccessKey.values()) if (o.nodeRef() != null) nodeRefs.add(o.nodeRef());
        Map<String, String> nodeNames = new LinkedHashMap<>();
        Map<String, String> nodeSourceFiles = new LinkedHashMap<>();
        try (MongoCursor<Document> cur = QueryBudget.request(db.getCollection("am_proc_node"))
                .find(Filters.and(Filters.eq("runId", runId), Filters.in("entityKey", nodeRefs)))
                .iterator()) {
            while (cur.hasNext()) {
                Document d = cur.next();
                nodeNames.put(d.getString("entityKey"), d.getString("name"));
                nodeSourceFiles.put(d.getString("entityKey"), d.getString("sourceFile"));
            }
        }

        // Group by the unordered table pair — the facet's own row identity — same as
        // ParserFacetSpecs.JOINS's `leftTable::rightTable` key, uppercased.
        record PairKey(String left, String right) {}
        Map<PairKey, ObjectNode> rows = new LinkedHashMap<>();
        Map<PairKey, Set<String>> typesByPair = new LinkedHashMap<>();
        for (JoinRow r : joinRows) {
            AccessOwner owner = ownerByAccessKey.get(r.parentRef());
            // `leftTable` is absent on the join sub-document itself when the left side of the join
            // is the row's own owning table — fall back to it rather than drop the row.
            String left = r.leftTable() != null ? r.leftTable()
                    : (owner == null ? null : owner.tableName());
            String right = r.joinedTable();
            if (left == null || right == null) continue;
            // Grouped by the *unordered* table pair, uppercased — found only by comparing real
            // output against the artifact-scan path: the same logical pair (e.g. ERP_CUSTOMER /
            // ERP_INVOICE) is recorded with the left/right direction flipped across different SQL
            // statements that join the same two tables the other way round, and the artifact-scan
            // path merges both directions into one row (981 access details either way; 27 rows
            // there vs. 33 when this reader kept direction-sensitive keys). Canonicalised here by
            // sorting the pair; `leftTable`/`rightTable` on the assembled row keep whichever
            // direction was recorded first, same as a `Map`'s own first-insert-wins behaviour.
            String upperLeft = left.toUpperCase(Locale.ROOT);
            String upperRight = right.toUpperCase(Locale.ROOT);
            PairKey pk = upperLeft.compareTo(upperRight) <= 0
                    ? new PairKey(upperLeft, upperRight) : new PairKey(upperRight, upperLeft);
            ObjectNode row = rows.computeIfAbsent(pk, k -> {
                ObjectNode n = mapper.createObjectNode();
                n.put("leftTable", left);
                n.put("rightTable", right);
                n.set("accessDetails", mapper.createArrayNode());
                return n;
            });
            if (r.joinType() != null) {
                typesByPair.computeIfAbsent(pk, k -> new LinkedHashSet<>()).add(r.joinType());
            }
            String nodeRef = owner == null ? null : owner.nodeRef();
            ObjectNode detail = mapper.createObjectNode();
            detail.put("procedureId", nodeRef == null ? "" : stripDialectPrefix(nodeRef));
            detail.put("procedureName", nodeRef == null ? "" : nodeNames.getOrDefault(nodeRef, ""));
            detail.put("onPredicate", r.condition() == null ? "" : r.condition());
            detail.put("lineNumber", r.line());
            // joins.js's own _renderDetail reads d.joinType per occurrence (badge colour/label) and
            // d.sourceFile as its "show SQL" fallback when the node-detail fetch doesn't carry one —
            // both real gaps, found by auditing that consumer directly rather than assuming the
            // pair-level joinTypes[] (already emitted below) covered it.
            detail.put("joinType", r.joinType() == null ? "" : r.joinType());
            detail.put("sourceFile", nodeRef == null ? "" : nodeSourceFiles.getOrDefault(nodeRef, ""));
            ((ArrayNode) row.get("accessDetails")).add(detail);
        }
        for (var e : rows.entrySet()) {
            ArrayNode types = e.getValue().putArray("joinTypes");
            typesByPair.getOrDefault(e.getKey(), Set.of()).forEach(types::add);
        }

        ArrayNode out = mapper.createArrayNode();
        rows.values().forEach(out::add);
        return out;
    }

    private static Document caseInsensitiveEq(String field, String value) {
        return new Document(field, java.util.regex.Pattern.compile("^" + java.util.regex.Pattern.quote(value) + "$",
                java.util.regex.Pattern.CASE_INSENSITIVE));
    }

    /** {@code nodeRef} is {@code DIALECT~nodeId}; the artifact's own {@code nodeId} has no dialect prefix. */
    private static String stripDialectPrefix(String nodeRef) {
        int i = nodeRef.indexOf('~');
        return i < 0 ? nodeRef : nodeRef.substring(i + 1);
    }
}
