package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.client.MongoCollection;
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
import java.util.Map;
import java.util.Set;

/**
 * Mode 4 read-side pilot (one facet only): rebuilds {@code ParserFacetSpecs.TABLES}'s own row
 * shape — {@code {tables:[{name, schema, objectType, triggers, allOperations, usedBy:[{nodeId,
 * nodeName, operations:{op:[lines]}, predicates:[...]}]}]}} — from the {@code am_*} entity
 * collections instead of scanning {@code api/tables/index.json}.
 *
 * <h2>{@code triggers}: a real gap found auditing the other real consumer, not {@code ParserFacetSpecs}</h2>
 *
 * <p>{@code ParserFacetSpecs.TABLES} itself never reads {@code triggers} — but {@code
 * parser/js/tableOps.js} does, directly off the table row, for its own "Triggers" grid column and
 * detail-panel trigger section (the same class of gap {@code EntityProceduresReader}'s own
 * {@code detailFile} fix (§182) found: a reader verified only against its facet's declared grid
 * usage, not every real consumer). Joined via {@code am_node_table_access.objectRef} (already read
 * for {@code usedBy[]}) into {@code am_db_object__triggers.parentRef}, confirmed the same key
 * convention against the real corpus. Only the source-parsed trigger shape ({@code nodeId, name,
 * schema, event, timing}) is reproduced — DB-discovered enrichment ({@code status, tableOps,
 * definition}) is written straight onto the artifact by the crawler with no ingest mapper of its
 * own and isn't exercised by this corpus (0 DB-discovered trigger rows here); a real, stated,
 * not-fixed-here gap if a future analysis run merges DB-discovered triggers under entity-read mode.
 *
 * <h2>Why this is a join, not a read</h2>
 *
 * <p>There is no single {@code am_table} "catalogue" collection. The entity model is normalised
 * around {@code am_node_table_access} (one row per node-table pair — which, usefully, already maps
 * 1:1 onto one {@code usedBy[]} entry), with operations split further into six per-verb child
 * collections (Splitter's own naming rule), and predicates reachable only via {@code am_statement}
 * → {@code am_statement__predicates}, keyed by the statement's own identity, not the table's. This
 * class does the four-collection join in application code rather than a single aggregation
 * pipeline, since the corpus this pilot was built and verified against (52 nodes) does not yet
 * justify the complexity of a server-side pipeline over "fetch everything for this run, group in
 * memory" — revisit if a real corpus makes that assumption wrong.
 *
 * <p><b>Pilot scope, stated plainly:</b> reproduces every field {@code ParserFacetSpecs.TABLES}
 * reads (verified against the real, already-ingested {@code ERP_USP_RUN_CYCLE} analysis — same
 * table set, same operation lines, same predicate rows). {@code objectType} takes whichever value
 * was seen first across a table's own access rows, since {@code am_node_table_access} does not
 * carry a single canonical one; on real data this never actually varies per table, but it is a
 * known, named simplification, not a proven-universal equivalence.
 */
@Component
public class EntityTablesReader {

    private final ObjectMapper mapper;

    public EntityTablesReader(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    private static final String[] OP_CHILD_SUFFIXES =
            {"select", "insert", "update", "delete", "merge", "dynamic"};
    private static final String[] OP_NAMES =
            {"SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "DYNAMIC"};

    /**
     * @param db            the entity-ingest database (only queried when entity-ingest is enabled
     *                      and reachable — callers check that before reaching here)
     * @param analysisName  the parser's own {@code DIALECT~PROJECT~ENV~ENTRYPOINT} analysis name
     * @return the same {@code {tables:[...]}} document shape {@code TableFacetReader.getTables}
     *         returns, or {@code null} if no {@code am_run} matches (caller falls back to the
     *         artifact-scan path — this is never the only path, by design)
     */
    public JsonNode readTables(MongoDatabase db, String analysisName) {
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

        // One am_node_table_access row == one usedBy[] entry, keyed by its own `key` field so the
        // six operation-child collections and predicate lookups can be joined back onto it.
        record AccessRow(String key, String schema, String name, String objectType, String nodeRef,
                         String objectRef) {}
        List<AccessRow> rows = new ArrayList<>();
        try (MongoCursor<Document> cur = QueryBudget.request(db.getCollection("am_node_table_access"))
                .find(Filters.eq("runId", runId)).iterator()) {
            while (cur.hasNext()) {
                Document d = cur.next();
                rows.add(new AccessRow(d.getString("key"), d.getString("schema"),
                        d.getString("name"), d.getString("objectType"), d.getString("nodeRef"),
                        d.getString("objectRef")));
            }
        }
        // BUGFIX (§267): `null`, NOT an empty result. An empty result is an ANSWER — the caller
        // takes it as "the entity store says this analysis has no tables" and serves it. But a
        // current run with zero rows does not mean that; it means this run was never ingested, or
        // its ingest died part-way and still left an `am_run` row behind. Confirmed live: the
        // SQLSERVER USP_RUNMRPCYCLE v1 run was `isCurrent: true, status: COMPLETE` with 0
        // `am_proc_node` and 0 `am_subtree_table` rows (the v2 re-ingest that had the real data
        // aborted on an E11000 and so never became current), and every entity-backed endpoint
        // served `[]` for an analysis whose artifacts held 55 procedures and 68 tables.
        //
        // Returning `null` is the contract this method already documents: the caller falls back to
        // the artifact scan. When the analysis genuinely has no tables the artifact scan returns
        // the same empty result anyway, so the only cost of being wrong in this direction is one
        // cheap read — whereas being wrong in the other direction silently blanks the UI.
        if (rows.isEmpty()) return null;

        // nodeRef -> display name, batched rather than one lookup per row.
        Set<String> nodeRefs = new LinkedHashSet<>();
        for (AccessRow r : rows) nodeRefs.add(r.nodeRef());
        Map<String, String> nodeNames = new LinkedHashMap<>();
        try (MongoCursor<Document> cur = QueryBudget.request(db.getCollection("am_proc_node"))
                .find(Filters.and(Filters.eq("runId", runId), Filters.in("entityKey", nodeRefs)))
                .iterator()) {
            while (cur.hasNext()) {
                Document d = cur.next();
                nodeNames.put(d.getString("entityKey"), d.getString("name"));
            }
        }

        // Triggers: tableOps.js's own grid column ("Triggers" count) and its detail-panel trigger
        // section both read `t.triggers` directly off the table row (parser/js/tableOps.js) — a
        // real gap found by auditing that consumer, not declared anywhere in ParserFacetSpecs.TABLES
        // itself (which never reads `triggers`). There is no `am_table` catalogue row to hang this
        // off (see this class's own doc comment); `am_node_table_access`'s own `objectRef` field
        // (ORACLE~SCHEMA~NAME) is exactly the `am_db_object__triggers.parentRef` key, confirmed
        // against the real corpus, so the join goes access row -> objectRef -> triggers, batched.
        // Only the fields the source-parsed shape actually carries (nodeId/name/schema/event/timing)
        // are reproduced here — DB-discovered enrichment (status/tableOps/definition) is written by
        // the artifact-scan crawler directly onto the artifact and has no ingest mapper of its own;
        // not exercised by this corpus (0 DB-discovered rows here) and not fixed here.
        Set<String> objectRefs = new LinkedHashSet<>();
        for (AccessRow r : rows) if (r.objectRef() != null) objectRefs.add(r.objectRef());
        Map<String, ArrayNode> triggersByObjectRef = new LinkedHashMap<>();
        try (MongoCursor<Document> cur = QueryBudget.request(db.getCollection("am_db_object__triggers"))
                .find(Filters.and(Filters.eq("runId", runId), Filters.in("parentRef", objectRefs)))
                .sort(new Document("ordinal", 1)).iterator()) {
            while (cur.hasNext()) {
                Document d = cur.next();
                ObjectNode trig = mapper.createObjectNode();
                trig.put("nodeId", d.getString("nodeId") == null ? "" : d.getString("nodeId"));
                trig.put("name", d.getString("name") == null ? "" : d.getString("name"));
                trig.put("schema", d.getString("schema") == null ? "" : d.getString("schema"));
                trig.put("event", d.getString("event") == null ? "" : d.getString("event"));
                trig.put("timing", d.getString("timing") == null ? "" : d.getString("timing"));
                triggersByObjectRef.computeIfAbsent(d.getString("parentRef"), k -> mapper.createArrayNode())
                        .add(trig);
            }
        }

        // key -> operation -> ordered line numbers, from the six split children.
        Map<String, Map<String, List<Integer>>> opsByRowKey = new LinkedHashMap<>();
        for (int i = 0; i < OP_CHILD_SUFFIXES.length; i++) {
            MongoCollection<Document> child = QueryBudget.request(db.getCollection("am_node_table_access__" + OP_CHILD_SUFFIXES[i]));
            try (MongoCursor<Document> cur = child.find(Filters.eq("runId", runId))
                    .sort(new Document("ordinal", 1)).iterator()) {
                while (cur.hasNext()) {
                    Document d = cur.next();
                    String parentRef = d.getString("parentRef");
                    Object v = d.get("value");
                    if (v == null) continue;
                    opsByRowKey.computeIfAbsent(parentRef, k -> new LinkedHashMap<>())
                            .computeIfAbsent(OP_NAMES[i], k -> new ArrayList<>())
                            .add(((Number) v).intValue());
                }
            }
        }

        // Predicates: am_statement (schema+table+nodeRef identify which usedBy[] entry a
        // statement's predicates belong to) -> am_statement__predicates keyed by parentRef.
        record StmtKey(String nodeRef, String schema, String table) {}
        Map<String, StmtKey> statementOwner = new LinkedHashMap<>();
        try (MongoCursor<Document> cur = QueryBudget.request(db.getCollection("am_statement"))
                .find(Filters.eq("runId", runId)).iterator()) {
            while (cur.hasNext()) {
                Document d = cur.next();
                String schema = d.getString("schemaName");
                String table = d.getString("tableName");
                String nodeRef = d.getString("nodeRef");
                if (schema == null || table == null || nodeRef == null) continue;
                // am_statement's own business-key field is entityKey, not key — unlike
                // am_node_table_access/am_run/am_statement__predicates, which do use `key`. Real,
                // subtle schema-naming inconsistency, found only by testing against real data: the
                // earlier attempt (reading `key`, always null here) silently produced zero matches
                // rather than an error.
                statementOwner.put(d.getString("entityKey"), new StmtKey(nodeRef, schema, table));
            }
        }
        // (nodeRef, schema, table) -> predicate objects, flattened across every statement that
        // owns predicates for that node+table pair — matches the artifact's own flat usedBy[].
        // predicates[] shape (§138), each predicate carrying its own line number.
        Map<StmtKey, ArrayNode> predicatesByOwner = new LinkedHashMap<>();
        try (MongoCursor<Document> cur = QueryBudget.request(db.getCollection("am_statement__predicates"))
                .find(Filters.eq("runId", runId)).sort(new Document("ordinal", 1)).iterator()) {
            while (cur.hasNext()) {
                Document d = cur.next();
                StmtKey owner = statementOwner.get(d.getString("parentRef"));
                if (owner == null) continue;
                ObjectNode p = mapper.createObjectNode();
                p.put("columnName", d.getString("column"));
                p.put("operator", d.getString("operator"));
                p.put("value", String.valueOf(d.get("value")));
                Object line = d.get("line");
                p.put("lineNumber", line == null ? 0 : ((Number) line).intValue());
                predicatesByOwner.computeIfAbsent(owner, k -> mapper.createArrayNode()).add(p);
            }
        }

        // Assemble: group AccessRows by (schema, name) into table rows, each row's usedBy[]
        // built from its own member AccessRows.
        record TableKey(String schema, String name) {}
        Map<TableKey, ObjectNode> tables = new LinkedHashMap<>();
        Map<TableKey, Set<String>> allOps = new LinkedHashMap<>();
        for (AccessRow r : rows) {
            TableKey tk = new TableKey(r.schema(), r.name());
            ObjectNode tableNode = tables.computeIfAbsent(tk, k -> {
                ObjectNode t = mapper.createObjectNode();
                t.put("schema", r.schema());
                t.put("name", r.name());
                t.put("objectType", r.objectType() == null ? "TABLE" : r.objectType());
                ArrayNode triggers = triggersByObjectRef.get(r.objectRef());
                t.set("triggers", triggers == null ? mapper.createArrayNode() : triggers);
                t.set("usedBy", mapper.createArrayNode());
                return t;
            });
            ObjectNode usedByEntry = mapper.createObjectNode();
            usedByEntry.put("nodeId", stripDialectPrefix(r.nodeRef()));
            usedByEntry.put("nodeName", nodeNames.getOrDefault(r.nodeRef(), ""));
            ObjectNode opsNode = mapper.createObjectNode();
            Set<String> tableOps = allOps.computeIfAbsent(tk, k -> new LinkedHashSet<>());
            Map<String, List<Integer>> ops = opsByRowKey.get(r.key());
            if (ops != null) {
                ops.forEach((op, lines) -> {
                    ArrayNode arr = opsNode.putArray(op);
                    lines.forEach(arr::add);
                    tableOps.add(op);
                });
            }
            usedByEntry.set("operations", opsNode);
            ArrayNode preds = predicatesByOwner.get(new StmtKey(r.nodeRef(), r.schema(), r.name()));
            if (preds != null) usedByEntry.set("predicates", preds);
            ((ArrayNode) tableNode.get("usedBy")).add(usedByEntry);
        }
        for (var tableEntry : tables.entrySet()) {
            ArrayNode opsArr = tableEntry.getValue().putArray("allOperations");
            allOps.get(tableEntry.getKey()).forEach(opsArr::add);
        }

        ArrayNode tablesArray = mapper.createArrayNode();
        tables.values().forEach(tablesArray::add);
        ObjectNode result = mapper.createObjectNode();
        result.put("totalTables", tables.size());
        result.set("tables", tablesArray);
        return result;
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
