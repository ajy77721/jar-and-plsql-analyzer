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
 * Mode 4 read-side, the final facet (see {@code EntityTablesReader}/{@code EntityJoinsReader}/
 * {@code EntityCursorsReader}/{@code EntitySequencesReader}/{@code EntityProceduresReader}/{@code
 * EntityPredicatesReader}'s own doc comments for the pattern and discipline — measure real coverage
 * per facet, do not assume it looks like the last one, and disqualify honestly (§157, {@code
 * statements}) if the unit being counted doesn't actually match): rebuilds {@code
 * SequenceFacetReader.getDynamicSql}'s exact row shape — {@code [{procedureId, procedureName,
 * schema, sourceFile, type, lineNumber, sqlExpression, usingVariables}]}, one row per execution, not
 * merged — from {@code am_dynamic_sql} + {@code am_dynamic_sql__using_variables} + {@code
 * am_proc_node} instead of scanning every node file's own {@code dynamicSql[]} array.
 *
 * <h2>Unit verified to match before building, per §157's own lesson</h2>
 *
 * <p>Confirmed both the artifact-scan endpoint and {@code am_dynamic_sql}'s own row count agree
 * exactly on the real corpus — 341 rows, one per dynamic-SQL execution, no merging or
 * re-aggregation in either direction (unlike {@code statements}, where the entity data counted a
 * structurally different unit and was disqualified rather than forced). {@code am_dynamic_sql} is
 * written directly by {@code NodeDetailMapper.emitChildren} — every node's own {@code dynamicSql[]}
 * array split one row per execution, the exact same source and unit the artifact-scan reader
 * iterates.
 *
 * <h2>Cheapest shape tied with cursors/sequences/predicates</h2>
 *
 * <p>One join needed: {@code am_dynamic_sql.nodeRef} → {@code am_proc_node} for the owning
 * procedure's schema/name/source file (the artifact-scan reader reads these off the same node
 * file the dynamic-SQL array lives inside). {@code usingVariables} is a real, already-split child
 * collection (confirmed non-empty — 62 of 341 executions carry at least one bound variable),
 * grouped back by {@code parentRef} the same way {@code EntityPredicatesReader} joins predicates to
 * their owning statement.
 */
@Component
public class EntityDynamicSqlReader {

    private final ObjectMapper mapper;

    public EntityDynamicSqlReader(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * @param db            the entity-ingest database
     * @param analysisName  the parser's own {@code DIALECT~PROJECT~ENV~ENTRYPOINT} analysis name
     * @return the same bare-array shape the artifact-scan {@code DYNAMIC_SQL} facet's document
     *         already is, or {@code null} if no {@code am_run} matches (caller falls back to
     *         artifact-scan)
     */
    public JsonNode readDynamicSql(MongoDatabase db, String analysisName) {
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

        record Dyn(String key, String nodeRef, String type, int line, String sqlExpression) {}
        List<Dyn> execs = new ArrayList<>();
        try (MongoCursor<Document> cur = QueryBudget.request(db.getCollection("am_dynamic_sql"))
                .find(Filters.eq("runId", runId)).sort(Sorts.ascending("nodeRef", "line")).iterator()) {
            while (cur.hasNext()) {
                Document d = cur.next();
                execs.add(new Dyn(d.getString("key"), d.getString("nodeRef"), d.getString("type"),
                        d.get("line") == null ? 0 : ((Number) d.get("line")).intValue(),
                        d.getString("sqlExpression")));
            }
        }
        // BUGFIX (§267): `null`, NOT an empty result. An empty result is an ANSWER — the caller
        // takes it as "the entity store says this analysis has no dynamic SQL" and serves it. But a
        // current run with zero rows does not mean that; it means this run was never ingested, or
        // its ingest died part-way and still left an `am_run` row behind. Confirmed live: the
        // SQLSERVER USP_RUNMRPCYCLE v1 run was `isCurrent: true, status: COMPLETE` with 0
        // `am_proc_node` and 0 `am_subtree_table` rows (the v2 re-ingest that had the real data
        // aborted on an E11000 and so never became current), and every entity-backed endpoint
        // served `[]` for an analysis whose artifacts held 55 procedures and 68 tables.
        //
        // Returning `null` is the contract this method already documents: the caller falls back to
        // the artifact scan. When the analysis genuinely has no dynamic SQL the artifact scan returns
        // the same empty result anyway, so the only cost of being wrong in this direction is one
        // cheap read — whereas being wrong in the other direction silently blanks the UI.
        if (execs.isEmpty()) return null;

        // usingVariables, batched by parentRef — same join shape EntityPredicatesReader uses for
        // predicates back to their owning statement.
        Map<String, List<String>> variablesByParent = new LinkedHashMap<>();
        Set<String> execKeys = new LinkedHashSet<>();
        for (Dyn d : execs) if (d.key() != null) execKeys.add(d.key());
        try (MongoCursor<Document> cur = QueryBudget.request(db.getCollection("am_dynamic_sql__using_variables"))
                .find(Filters.and(Filters.eq("runId", runId), Filters.in("parentRef", execKeys)))
                .sort(Sorts.ascending("ordinal")).iterator()) {
            while (cur.hasNext()) {
                Document d = cur.next();
                variablesByParent.computeIfAbsent(d.getString("parentRef"), k -> new ArrayList<>())
                        .add(d.getString("value"));
            }
        }

        // nodeRef -> {nodeId, name, schema, sourceFile}, batched.
        Set<String> nodeRefs = new LinkedHashSet<>();
        for (Dyn d : execs) if (d.nodeRef() != null) nodeRefs.add(d.nodeRef());
        record NodeInfo(String nodeId, String name, String schema, String sourceFile) {}
        Map<String, NodeInfo> nodes = new LinkedHashMap<>();
        try (MongoCursor<Document> cur = QueryBudget.request(db.getCollection("am_proc_node"))
                .find(Filters.and(Filters.eq("runId", runId), Filters.in("entityKey", nodeRefs)))
                .iterator()) {
            while (cur.hasNext()) {
                Document d = cur.next();
                nodes.put(d.getString("entityKey"), new NodeInfo(
                        d.getString("nodeId"), d.getString("name"), d.getString("schema"),
                        d.getString("sourceFile")));
            }
        }

        ArrayNode out = mapper.createArrayNode();
        for (Dyn d : execs) {
            NodeInfo node = nodes.get(d.nodeRef());
            ObjectNode row = mapper.createObjectNode();
            row.put("procedureId", node == null || node.nodeId() == null ? "" : node.nodeId());
            row.put("procedureName", node == null || node.name() == null ? "" : node.name());
            row.put("schema", node == null || node.schema() == null ? "" : node.schema());
            row.put("sourceFile", node == null || node.sourceFile() == null ? "" : node.sourceFile());
            row.put("type", d.type() == null ? "DYNAMIC" : d.type());
            row.put("lineNumber", d.line());
            row.put("sqlExpression", d.sqlExpression() == null ? "" : d.sqlExpression());
            List<String> vars = variablesByParent.get(d.key());
            if (vars != null && !vars.isEmpty()) {
                ArrayNode uv = row.putArray("usingVariables");
                vars.forEach(uv::add);
            }
            out.add(row);
        }
        return out;
    }

    private static Document caseInsensitiveEq(String field, String value) {
        return new Document(field, Pattern.compile("^" + Pattern.quote(value) + "$",
                Pattern.CASE_INSENSITIVE));
    }
}
