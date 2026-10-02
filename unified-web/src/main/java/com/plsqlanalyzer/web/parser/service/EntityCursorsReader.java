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

/**
 * Mode 4 read-side, facet 3 (see {@code EntityTablesReader}/{@code EntityJoinsReader}'s own doc
 * comments for the pilot's pattern and discipline — measure real coverage per facet, do not assume
 * it looks like the last one): rebuilds {@code NodeScanFacetReader.getCursors}'s exact row shape —
 * {@code [{cursorName, cursorType, queryText, operations:[...], accessCount, accessDetails:
 * [{operation, procedureId, procedureName, lineNumber, sourceFile, queryText}]}]} — from
 * {@code am_cursor_decl} + {@code am_proc_node} instead of scanning every node file.
 *
 * <h2>Cheaper than both tables and joins, measured rather than assumed</h2>
 *
 * <p>Two collections carry the real data: {@code am_cursor_decl}, one document per cursor
 * declaration (scoped to one node, one line), plus {@code am_cursor_decl__stmt_operations} — the
 * declaration's post-DECLARE lifecycle (OPEN/FETCH/CLOSE/DEALLOCATE). {@code Splitter} puts every
 * non-empty array field, with no exceptions, into its own child collection keyed by the owning
 * document's {@code key} as {@code parentRef}; {@code stmtOperations} is no different, so it is
 * never embedded on {@code am_cursor_decl} itself and must be joined in the same way
 * {@code am_cursor_object} already is. Confirmed against the real
 * {@code SQLSERVER~SQLSERVERDEMO~LOCAL~USP_RUNMRPCYCLE} corpus after the SQL Server T-SQL parser
 * was fixed to track a cursor's lifecycle past its own DECLARE: {@code am_cursor_decl__stmt_operations}
 * now holds real OPEN/FETCH/CLOSE/DEALLOCATE rows, joined back to their declaring cursor by
 * {@code parentRef == am_cursor_decl.key}.
 *
 * <p>The one join needed is {@code am_cursor_decl.nodeRef} → {@code am_proc_node} for the owning
 * procedure's display name and source file — a single batched lookup, no intermediate collection
 * (unlike {@code tables}' four-collection join or {@code joins}' two).
 *
 * <h2>The merge key, reproduced exactly</h2>
 *
 * <p>The artifact-scan reader groups by {@code cursorName::nodeId#line} — one facet row per cursor
 * declaration, keyed by the declaration's own source line. It deliberately is <em>not</em>
 * {@code cursorName::nodeId}: Oracle names every implicit FOR-loop cursor {@code "(FOR R)"}, so
 * keying without the line collapsed a node's many cursors into one row (64 rows for a corpus whose
 * {@code counts.cursors} sums to 476). Including the line gives each instance its own row and its
 * own accessDetails, which is what this reader reproduces.
 */
@Component
public class EntityCursorsReader {

    private final ObjectMapper mapper;

    public EntityCursorsReader(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * @param db            the entity-ingest database
     * @param analysisName  the parser's own {@code DIALECT~PROJECT~ENV~ENTRYPOINT} analysis name
     * @return the same bare-array shape the artifact-scan {@code CURSORS} facet's document already
     *         is, or {@code null} if no {@code am_run} matches (caller falls back to artifact-scan)
     */
    public JsonNode readCursors(MongoDatabase db, String analysisName) {
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

        record CursorDecl(String key, String nodeRef, String name, String query, boolean forLoop,
                          boolean refCursor, int line, List<Document> stmtOps) {}
        List<CursorDecl> decls = new ArrayList<>();
        try (MongoCursor<Document> cur = QueryBudget.request(db.getCollection("am_cursor_decl"))
                .find(Filters.eq("runId", runId)).sort(Sorts.ascending("nodeRef", "line")).iterator()) {
            while (cur.hasNext()) {
                Document d = cur.next();
                decls.add(new CursorDecl(d.getString("key"), d.getString("nodeRef"), d.getString("name"), d.getString("query"),
                        Boolean.TRUE.equals(d.getBoolean("forLoop")), Boolean.TRUE.equals(d.getBoolean("refCursor")),
                        d.get("line") == null ? 0 : ((Number) d.get("line")).intValue(),
                        new ArrayList<>()));
            }
        }
        // BUGFIX (§267): `null`, NOT an empty result. An empty result is an ANSWER — the caller
        // takes it as "the entity store says this analysis has no cursors" and serves it. But a
        // current run with zero rows does not mean that; it means this run was never ingested, or
        // its ingest died part-way and still left an `am_run` row behind. Confirmed live: the
        // SQLSERVER USP_RUNMRPCYCLE v1 run was `isCurrent: true, status: COMPLETE` with 0
        // `am_proc_node` and 0 `am_subtree_table` rows (the v2 re-ingest that had the real data
        // aborted on an E11000 and so never became current), and every entity-backed endpoint
        // served `[]` for an analysis whose artifacts held 55 procedures and 68 tables.
        //
        // Returning `null` is the contract this method already documents: the caller falls back to
        // the artifact scan. When the analysis genuinely has no cursors the artifact scan returns
        // the same empty result anyway, so the only cost of being wrong in this direction is one
        // cheap read — whereas being wrong in the other direction silently blanks the UI.
        if (decls.isEmpty()) return null;

        // stmtOperations (OPEN/FETCH/CLOSE/DEALLOCATE) never lands embedded on am_cursor_decl:
        // Splitter puts every non-empty array field — no exceptions — into its own child
        // collection, am_cursor_decl__stmt_operations, one row per operation, keyed by
        // parentRef == the owning cursor decl's own key. Read that collection and re-attach by key,
        // the same join Splitter's own child rows always need.
        Map<String, List<Document>> stmtOpsByDeclKey = new LinkedHashMap<>();
        Set<String> declKeys = new LinkedHashSet<>();
        for (CursorDecl d : decls) if (d.key() != null) declKeys.add(d.key());
        if (!declKeys.isEmpty()) {
            try (MongoCursor<Document> cur = QueryBudget.request(db.getCollection("am_cursor_decl__stmt_operations"))
                    .find(Filters.and(Filters.eq("runId", runId), Filters.in("parentRef", declKeys)))
                    .sort(Sorts.ascending("parentRef", "ordinal")).iterator()) {
                while (cur.hasNext()) {
                    Document d = cur.next();
                    stmtOpsByDeclKey.computeIfAbsent(d.getString("parentRef"), k -> new ArrayList<>()).add(d);
                }
            }
        }
        for (int i = 0; i < decls.size(); i++) {
            CursorDecl d = decls.get(i);
            List<Document> ops = stmtOpsByDeclKey.get(d.key());
            if (ops != null) decls.set(i, new CursorDecl(d.key(), d.nodeRef(), d.name(), d.query(),
                    d.forLoop(), d.refCursor(), d.line(), ops));
        }

        // nodeRef -> {nodeId, name, sourceFile}, batched.
        Set<String> nodeRefs = new LinkedHashSet<>();
        for (CursorDecl d : decls) if (d.nodeRef() != null) nodeRefs.add(d.nodeRef());
        record NodeInfo(String nodeId, String name, String sourceFile, boolean returnsResultSet,
                        int resultSets) {}
        Map<String, NodeInfo> nodes = new LinkedHashMap<>();
        try (MongoCursor<Document> cur = QueryBudget.request(db.getCollection("am_proc_node"))
                .find(Filters.and(Filters.eq("runId", runId), Filters.in("entityKey", nodeRefs)))
                .iterator()) {
            while (cur.hasNext()) {
                Document d = cur.next();
                // Task #26: the node's "returns data" marker is a scalar on the node detail, so
                // Splitter copies it onto am_proc_node — read it back for row parity with the
                // artifact-scan path.
                nodes.put(d.getString("entityKey"),
                        new NodeInfo(d.getString("nodeId"), d.getString("name"), d.getString("sourceFile"),
                                Boolean.TRUE.equals(d.getBoolean("returnsResultSet")),
                                d.get("resultSets") == null ? 0 : ((Number) d.get("resultSets")).intValue()));
            }
        }

        // Grouped by cursorName::nodeId#line, same per-instance key the artifact-scan reader uses.
        Map<String, ObjectNode> rows = new LinkedHashMap<>();
        for (CursorDecl d : decls) {
            NodeInfo node = nodes.get(d.nodeRef());
            String nodeId = node == null ? "" : (node.nodeId() == null ? "" : node.nodeId());
            String nodeName = node == null ? "" : (node.name() == null ? "" : node.name());
            String sourceFile = node == null ? "" : (node.sourceFile() == null ? "" : node.sourceFile());
            String cursorName = d.name() == null ? "UNNAMED" : d.name();
            // One row per cursor INSTANCE: name::node#line, exactly the key NodeScanFacetReader now
            // uses. Keying on name::node alone collapsed every synthetic Oracle FOR-loop cursor
            // ("(FOR R)") on a node into one row; the declaration's line keeps each distinct.
            String mapKey = cursorName + "::" + nodeId + "#" + d.line();

            ObjectNode row = rows.computeIfAbsent(mapKey, k -> {
                ObjectNode n = mapper.createObjectNode();
                n.put("cursorName", cursorName);
                n.put("cursorType", d.refCursor() ? "REF_CURSOR" : d.forLoop() ? "FOR_LOOP" : "EXPLICIT");
                n.put("queryText", d.query() == null ? "" : d.query());
                n.put("returnsData", node != null && node.returnsResultSet());
                n.put("resultSets", node == null ? 0 : node.resultSets());
                n.set("operations", mapper.createArrayNode());
                n.put("accessCount", 0);
                n.set("accessDetails", mapper.createArrayNode());
                return n;
            });

            String baseOperation = d.forLoop() ? "CURSOR_FOR_LOOP" : "DECLARE";
            addOp((ArrayNode) row.get("operations"), baseOperation);
            row.put("accessCount", row.get("accessCount").asInt() + 1);

            ObjectNode det = mapper.createObjectNode();
            det.put("operation", baseOperation);
            det.put("procedureId", nodeId);
            det.put("procedureName", nodeName);
            det.put("lineNumber", d.line());
            det.put("sourceFile", sourceFile);
            det.put("queryText", d.query() == null ? "" : d.query());
            ((ArrayNode) row.get("accessDetails")).add(det);

            // Mirrors NodeScanFacetReader.getCursors' own stmtOperations handling. Unverified
            // against real data (this corpus's own decls all have zero stmtOperations entries — see
            // this class's own doc comment), reproduced defensively rather than silently dropped.
            for (Document sop : d.stmtOps()) {
                String sopType = sop.getString("operation");
                if (sopType == null) continue;
                int sopLine = sop.get("line") == null ? 0 : ((Number) sop.get("line")).intValue();
                addOp((ArrayNode) row.get("operations"), sopType);
                row.put("accessCount", row.get("accessCount").asInt() + 1);
                ObjectNode sopDetail = mapper.createObjectNode();
                sopDetail.put("operation", sopType);
                sopDetail.put("procedureId", nodeId);
                sopDetail.put("procedureName", nodeName);
                sopDetail.put("lineNumber", sopLine);
                sopDetail.put("sourceFile", sourceFile);
                ((ArrayNode) row.get("accessDetails")).add(sopDetail);
            }
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
        return new Document(field, java.util.regex.Pattern.compile("^" + java.util.regex.Pattern.quote(value) + "$",
                java.util.regex.Pattern.CASE_INSENSITIVE));
    }
}
