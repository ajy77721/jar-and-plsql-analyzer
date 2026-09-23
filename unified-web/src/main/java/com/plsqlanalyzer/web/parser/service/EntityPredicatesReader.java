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
import java.util.regex.Pattern;

/**
 * Mode 4 read-side, facet 6 (see {@code EntityTablesReader}/{@code EntityJoinsReader}/{@code
 * EntityCursorsReader}/{@code EntitySequencesReader}/{@code EntityProceduresReader}'s own doc
 * comments for the pattern and discipline): rebuilds {@code PlsqlPredicatesFacet.build}'s exact row
 * shape — {@code [{tableName, schemaName, tableType, predicateCount, uniqueColumns, predicates:
 * [{columnName, operator, value, lineNumber, procedureId, procedureName, sourceFile, operation}],
 * operations:[...]}]} — from {@code am_statement__predicates} + {@code am_statement} instead of
 * {@code ParserPredicatesAdapter} adapting the raw {@code usedBy[]} artifact.
 *
 * <h2>Not a direct artifact read — the artifact-scan sibling is an aggregation too</h2>
 *
 * <p>{@code PlsqlPredicatesFacet.build} itself groups whatever rows it is given by table; this
 * class reproduces the same grouping over entity data instead of over {@code
 * ParserPredicatesAdapter}'s adapted array. The predicate data was already real and populated
 * before this class existed — {@code am_statement__predicates} (912 rows on the real corpus, per
 * this session's own earlier §150 tables-facet verification) — so no new ingest coverage was
 * needed, only a reader that joins it back to its owning statement.
 *
 * <h2>The one join: predicates back to their owning statement</h2>
 *
 * <p>{@code am_statement__predicates.parentRef} points at {@code am_statement.entityKey}. Each
 * predicate needs its parent statement's {@code tableName}/{@code schemaName}/{@code objectType}
 * (to group by table), {@code operation}, {@code line} (fallback when the predicate's own {@code
 * line} is 0 — same fallback {@code PlsqlPredicatesFacet.build} itself already does), and {@code
 * nodeId}/{@code nodeName} (for {@code procedureId}/{@code procedureName}). One batched lookup by
 * {@code parentRef}, not a per-row query.
 */
@Component
public class EntityPredicatesReader {

    private final ObjectMapper mapper;

    public EntityPredicatesReader(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * @param db            the entity-ingest database
     * @param analysisName  the parser's own {@code DIALECT~PROJECT~ENV~ENTRYPOINT} analysis name
     * @return the same bare-array shape {@code PlsqlPredicatesFacet.build} already produces, or
     *         {@code null} if no {@code am_run} matches (caller falls back to artifact-scan)
     */
    public JsonNode readPredicates(MongoDatabase db, String analysisName) {
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

        record Pred(String parentRef, String column, String operator, String value, int line) {}
        List<Pred> preds = new ArrayList<>();
        try (MongoCursor<Document> cur = QueryBudget.request(db.getCollection("am_statement__predicates"))
                .find(Filters.eq("runId", runId)).iterator()) {
            while (cur.hasNext()) {
                Document d = cur.next();
                preds.add(new Pred(d.getString("parentRef"), d.getString("column"),
                        d.getString("operator"), d.getString("value"),
                        d.get("line") == null ? 0 : ((Number) d.get("line")).intValue()));
            }
        }
        // BUGFIX (§267): `null`, NOT an empty result. An empty result is an ANSWER — the caller
        // takes it as "the entity store says this analysis has no predicates" and serves it. But a
        // current run with zero rows does not mean that; it means this run was never ingested, or
        // its ingest died part-way and still left an `am_run` row behind. Confirmed live: the
        // SQLSERVER USP_RUNMRPCYCLE v1 run was `isCurrent: true, status: COMPLETE` with 0
        // `am_proc_node` and 0 `am_subtree_table` rows (the v2 re-ingest that had the real data
        // aborted on an E11000 and so never became current), and every entity-backed endpoint
        // served `[]` for an analysis whose artifacts held 55 procedures and 68 tables.
        //
        // Returning `null` is the contract this method already documents: the caller falls back to
        // the artifact scan. When the analysis genuinely has no predicates the artifact scan returns
        // the same empty result anyway, so the only cost of being wrong in this direction is one
        // cheap read — whereas being wrong in the other direction silently blanks the UI.
        if (preds.isEmpty()) return null;

        // parentRef -> owning statement, batched.
        Set<String> parentRefs = new LinkedHashSet<>();
        for (Pred p : preds) if (p.parentRef() != null) parentRefs.add(p.parentRef());
        record Stmt(String tableName, String schemaName, String objectType, String operation,
                    int line, String nodeId, String nodeName, String sourceFile) {}
        Map<String, Stmt> statements = new LinkedHashMap<>();
        try (MongoCursor<Document> cur = QueryBudget.request(db.getCollection("am_statement"))
                .find(Filters.and(Filters.eq("runId", runId), Filters.in("entityKey", parentRefs)))
                .iterator()) {
            while (cur.hasNext()) {
                Document d = cur.next();
                statements.put(d.getString("entityKey"), new Stmt(
                        d.getString("tableName"), d.getString("schemaName"), d.getString("objectType"),
                        d.getString("operation"),
                        d.get("line") == null ? 0 : ((Number) d.get("line")).intValue(),
                        d.getString("nodeId"), d.getString("nodeName"), d.getString("sourceFile")));
            }
        }

        // Grouped by schema.table, same key PlsqlPredicatesFacet.build's own caller groups by.
        record TableKey(String schema, String table, String objectType) {}
        Map<String, ObjectNode> rowByKey = new LinkedHashMap<>();
        Map<String, ArrayNode> predsByKey = new LinkedHashMap<>();
        Map<String, Set<String>> columnsByKey = new LinkedHashMap<>();
        Map<String, Set<String>> opsByKey = new LinkedHashMap<>();

        for (Pred p : preds) {
            Stmt s = statements.get(p.parentRef());
            if (s == null || s.tableName() == null) continue; // dangling ref or unresolved table
            String key = (s.schemaName() == null ? "" : s.schemaName()) + "." + s.tableName();

            ObjectNode row = rowByKey.computeIfAbsent(key, k -> {
                ObjectNode n = mapper.createObjectNode();
                n.put("tableName", s.tableName());
                n.put("schemaName", s.schemaName() == null ? "" : s.schemaName());
                n.put("tableType", s.objectType() == null ? "TABLE" : s.objectType());
                return n;
            });
            ArrayNode predicatesArr = predsByKey.computeIfAbsent(key, k -> mapper.createArrayNode());
            Set<String> upperColumns = columnsByKey.computeIfAbsent(key, k -> new LinkedHashSet<>());
            Set<String> operations = opsByKey.computeIfAbsent(key, k -> new LinkedHashSet<>());

            ObjectNode pr = mapper.createObjectNode();
            String column = p.column() == null ? "" : p.column();
            pr.put("columnName", column);
            pr.put("operator", p.operator() == null ? "" : p.operator());
            pr.put("value", p.value() == null ? "" : p.value());
            int line = p.line() != 0 ? p.line() : s.line();
            pr.put("lineNumber", line);
            pr.put("procedureId", s.nodeId() == null ? "" : s.nodeId());
            pr.put("procedureName", s.nodeName() == null ? "" : s.nodeName());
            pr.put("sourceFile", s.sourceFile() == null ? "" : s.sourceFile());
            String operation = s.operation() == null ? "" : s.operation();
            pr.put("operation", operation);
            predicatesArr.add(pr);
            if (!column.isEmpty()) upperColumns.add(column.toUpperCase(Locale.ROOT));
            if (!operation.isEmpty()) operations.add(operation);
        }

        ArrayNode out = mapper.createArrayNode();
        for (var e : rowByKey.entrySet()) {
            String key = e.getKey();
            ObjectNode row = e.getValue();
            ArrayNode predicatesArr = predsByKey.get(key);
            row.put("predicateCount", predicatesArr.size());
            row.put("uniqueColumns", columnsByKey.get(key).size());
            row.set("predicates", predicatesArr);
            ArrayNode ops = row.putArray("operations");
            for (String op : opsByKey.get(key)) ops.add(op);
            out.add(row);
        }
        return out;
    }

    private static Document caseInsensitiveEq(String field, String value) {
        return new Document(field, Pattern.compile("^" + Pattern.quote(value) + "$",
                Pattern.CASE_INSENSITIVE));
    }
}
