package com.codeanalyzer.ingest.mongo.map.plsql;

import com.codeanalyzer.ingest.mongo.id.IdCodec;
import com.codeanalyzer.ingest.mongo.id.Ids;
import com.codeanalyzer.ingest.mongo.map.DocSink;
import com.codeanalyzer.ingest.mongo.map.IngestContext;
import com.codeanalyzer.ingest.mongo.id.Identity;
import com.codeanalyzer.ingest.mongo.map.JsonDocs;
import com.codeanalyzer.ingest.mongo.map.Splitter;
import com.codeanalyzer.ingest.mongo.model.RunMember;
import com.codeanalyzer.ingest.mongo.schema.Collections;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import org.bson.Document;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * One {@code api/nodes/*.json} document → {@code am_proc_node} plus its child collections.
 *
 * <h2>Why the arrays are split out rather than embedded</h2>
 *
 * <p>Measured, a node document reaches 293 KB and 49.8% of its bytes are {@code subtreeTables}
 * alone (11,411 rows across the corpus). Embedded, the parent's size is set by how much code it
 * transitively reaches; split, it is set by its own attribute count — 643 bytes. That is the point
 * of the decomposition: the ceiling stops depending on the size of the program being analysed.
 *
 * <h2>{@code tables} and {@code directTables}</h2>
 *
 * <p>These are byte-identical wherever both appear (300 of 300 measured; 56 documents have
 * neither). Storage keeps one copy — but <b>both names must still resolve</b>, because a screen may
 * read either and the rule is that no field disappears. So the rows are written once and the node
 * records which source fields carried them; a reader asking for {@code directTables} gets the same
 * array. If they ever diverge, the {@code else} branch stores both, so the equality is an
 * optimisation that is checked rather than assumed.
 */
public final class NodeDetailMapper {

    private final ObjectMapper json = new ObjectMapper();

    /** Maps one node file. Called once per file, so it is not an {@code ArtifactMapper}. */
    public void map(IngestContext ctx, InputStream in, DocSink sink) throws IOException {
        JsonNode n = json.readTree(in);
        String nodeId = n.path("nodeId").asText(null);
        if (nodeId == null) return;

        Ids id = IdCodec.procNode(ctx.runId(), nodeId);
        // Derived from natural keys, so it is known before the content is. That is what lets ONE
        // key serve as the splitter's parent key AND every child's nodeRef. Using id.key() in one
        // place and the entity key in another orphaned all 476 cursors — two different strings,
        // and only a reachability test would ever have caught it.
        String entityKey = ctx.entityKey(nodeId);

        // ── Every hand-mapped array is captured and stripped BEFORE flatten ──────────────
        //
        // This is the whole of the rule, and it has to be the whole of it. `Splitter.flatten`
        // walks the node structurally and emits EVERY array it meets into a collection named
        // after the field — that is its job, and it is what makes an array added upstream
        // impossible to forget. But a field that ALSO has a hand-written emitter below is then
        // written twice: once correctly, once into a shadow collection nobody reads.
        //
        // Measured on three real nodes, before this strip existed (SQLSERVER USP_ALLOCATEDEMAND /
        // POSTGRES ERP_USP_COLLECT_DEMAND / DB2 ERP_USP_ALLOCATE_INVENTORY):
        //
        //   am_txn_statement 3/2/8      also am_proc_node__statements          3/2/8
        //   am_exception_handler 1/17/31     am_proc_node__exception_handlers  1/17/31
        //   am_variable_type_ref 74/14/233   am_proc_node__plain (+_type_refs) 74/14/233
        //   am_dynamic_sql 42/2/10           am_proc_node__dynamic_sql         42/3/10
        //   am_cursor_decl 10/1/30           am_proc_node__cursors             60/4/30
        //   am_node_table_access 74/12/30    am_proc_node__tables              386/56/301
        //                               AND  am_proc_node__direct_tables       386/56/301
        //   am_subtree_table 78/34/58        am_proc_node__subtree_tables      2405/781/4162
        //   am_sequence_use 3/2/-            am_proc_node__sequences           6/3/-
        //
        // `subtreeTables` is the one that shows the cost rather than just the untidiness: 4,162
        // shadow rows against 58 real ones on the DB2 node, and 335,441 against 10,174 across the
        // stored corpus. The shadow rows are also the wrong shape — flattened per element with no
        // `objectRef`, so nothing can resolve or query them, which is why no reader uses them.
        //
        // `foreignKeys` was fixed first and alone; the comment left on it noted
        // `transactionScope.statements` as the same latent risk. It was not latent — it, and
        // seven more, were already duplicating. Stripping is done here in one place so the
        // invariant is "a field with an emitter below never reaches flatten", which is checkable,
        // rather than one strip per bug found.
        //
        // What is deliberately NOT stripped: `calls` and `parameters` have no hand-written
        // emitter, so `am_proc_node__calls` / `__parameters` are their only home and are correct.
        // The child collections the hand-written emitters produce through their OWN flatten calls
        // — `am_node_table_access__joins` (read by EntityJoinsReader), `am_node_table_access__*`
        // per operation (EntityTablesReader), `am_cursor_decl__stmt_operations`,
        // `am_dynamic_sql__using_variables` — are unaffected: they are emitted below, from the
        // captured nodes, not from this top-level walk.
        JsonNode foreignKeysNode = take(n, "foreignKeys");
        JsonNode cursorsNode = take(n, "cursors");
        JsonNode dynamicSqlNode = take(n, "dynamicSql");
        JsonNode exceptionHandlersNode = take(n, "exceptionHandlers");
        JsonNode pkgVarRefsNode = take(n, "externalPackageVarRefDetails");
        JsonNode sequencesNode = take(n, "sequences");
        JsonNode subtreeTablesNode = take(n, "subtreeTables");
        JsonNode tables = take(n, "tables");
        JsonNode direct = take(n, "directTables");

        // These two are OBJECTS holding a summary plus the rows. Only the arrays inside are taken,
        // so the summary scalars (`total`, `typeRefCount`, the transaction counts) stay on the
        // node document where the screens already read them.
        JsonNode txnStatementsNode = takeNested(n, "transactionScope", "statements");

        // An OBJECT keyed by statement TYPE — `{"IF": 2, "CASE": 1, "MERGE": 1, …}`. Taken off
        // before flatten for the same reason the arrays are: flatten keeps a nested object inline,
        // and inline is precisely what the data-keyed-map gate refuses once the key set is wide.
        // It refused the DB2 entry point at 41 keys and aborted that run's whole entity-ingest.
        JsonNode statementSummaryNode = takeObject(n, "statementSummary");
        JsonNode varsNode = n.path("variables");
        Map<String, JsonNode> varBuckets = new HashMap<>();
        for (String kind : VAR_KINDS) {
            JsonNode bucket = take(varsNode, kind);
            if (bucket.isArray()) varBuckets.put(kind, bucket);
        }

        // Splitter takes every array out; what remains is scalars only.
        Document node = Splitter.flatten(ctx, sink, Collections.PROC_NODE, entityKey, n);
        node.put("runId", ctx.runId());
        node.put("nodeId", nodeId);

        boolean sameArrays = tables.equals(direct);
        node.put("directTablesEqualsTables", sameArrays);

        Identity ident = Identity.ofSource(entityKey, id.key(), n);
        ident.stampOnto(node);
        sink.emit(Collections.PROC_NODE, node);
        sink.emit(RunMember.COLLECTION,
                RunMember.of(ctx.runId(), ident, Collections.PROC_NODE, null));
        ctx.putNodeId(nodeId, entityKey);

        emitCursors(ctx, sink, entityKey, cursorsNode, schemaOf(direct.isArray() ? direct : tables));
        emitChildren(ctx, sink, entityKey, dynamicSqlNode, Collections.DYNAMIC_SQL, "dyn");
        emitChildren(ctx, sink, entityKey, exceptionHandlersNode, Collections.EXCEPTION_HANDLER, "exc");
        // Step 4 (cross-package global-variable references): one row per PKG.VAR reference
        // this node makes, carrying line + READ/WRITE/UNKNOWN access type. Source field is
        // externalPackageVarRefDetails (the structured sibling of the older bare
        // externalPackageVarRefs string list, which stays JSON-only and is not ingested).
        emitChildren(ctx, sink, entityKey, pkgVarRefsNode, Collections.PACKAGE_VAR_REF, "pkgvar");
        // These two are OBJECTS, not arrays — a summary with the rows nested inside. Passing them
        // to emitChildren left both collections permanently empty: `isArray()` was false, so it
        // returned immediately and no error was raised anywhere. The rows were not lost, because
        // Splitter still split the inner arrays out of the proc_node document, but they landed in
        // `am_proc_node__statements` / `__type_refs` / `__plain` — names derived from the source
        // field rather than the names PLAN-04 specifies and the middle layer queries.
        emitChildren(ctx, sink, entityKey, txnStatementsNode, Collections.TXN_STATEMENT, "txn");
        emitVariableRefs(ctx, sink, entityKey, varBuckets);
        emitForeignKeys(ctx, sink, entityKey, foreignKeysNode);

        emitObjectRows(ctx, sink, entityKey, tables, Collections.NODE_TABLE_ACCESS, sameArrays);
        if (!sameArrays && direct.isArray()) {
            // They diverged. Store the second set too, discriminated, rather than lose it.
            emitObjectRows(ctx, sink, entityKey, direct, Collections.NODE_TABLE_ACCESS, false);
        }
        emitObjectRows(ctx, sink, entityKey, subtreeTablesNode, Collections.SUBTREE_TABLE, false);

        emitSequences(ctx, sink, entityKey, sequencesNode);
        emitStatementSummary(ctx, sink, entityKey, statementSummaryNode);
    }

    /**
     * The node's statement mix, as rows — ING-118.
     *
     * <p>{@code statementSummary} is a count per statement TYPE. The type vocabulary is defined by
     * the grammars, not by the analysed code, so it does not grow with the corpus the way
     * {@code localVariables} did — but it is <b>not closed</b> either: PLAN-11 §3 records 11–28
     * tokens per dialect and the corpus has 43, because every grammar addition (CTE, MERGE, window
     * functions, temporal tables) widens it. At 41 keys on DB2's {@code ERP_USP_RUN_CYCLE} it
     * passed {@code Splitter.MAX_SCHEMA_KEYS} and the gate threw, which aborted the entity-ingest
     * of the whole run: 13 nodes written instead of 63, and the survivors were the leaf functions
     * with a handful of statement types each.
     *
     * <p>The fix is the one the gate's own message names — split it into rows — rather than an
     * exemption, because a threshold that has to be argued with per field stops being a rule, and
     * because the type is only queryable as a value. {@code count} is stored as a long so a
     * {@code $sum} over the collection does not depend on how Jackson typed the JSON number.
     *
     * <p>Iteration order is preserved in {@code ordinal}: the parser builds the map as a
     * {@code LinkedHashMap} in first-appearance order, and a relation would otherwise lose it.
     */
    private void emitStatementSummary(IngestContext ctx, DocSink sink, String nodeRef,
                                      JsonNode summary) {
        if (summary == null || !summary.isObject()) return;
        int ordinal = 0;
        var fields = summary.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> e = fields.next();
            String type = e.getKey();
            if (type == null || type.isBlank()) continue;
            Ids id = IdCodec.nested(Collections.NODE_STATEMENT_SUMMARY, nodeRef,
                    String.valueOf(ordinal), type);
            Document row = new Document("_id", id.key())
                    .append("key", id.key())
                    .append("runId", ctx.runId())
                    .append("nodeRef", nodeRef)
                    .append("statementType", type)
                    .append("count", e.getValue().isNumber() ? e.getValue().asLong() : 0L)
                    .append("ordinal", ordinal++);
            sink.emit(Collections.NODE_STATEMENT_SUMMARY, row);
        }
    }

    /** The four variable buckets, in the order they are emitted. */
    static final String[] VAR_KINDS = {"typeRefs", "rowtypeRefs", "collections", "plain"};

    /**
     * Reads an array field and removes it from its parent in one step.
     *
     * <p>Read-then-remove has to be atomic at the call site, because doing it in two statements is
     * exactly how {@code foreignKeys} ended up the only field that was stripped: the read is
     * obvious and the remove is easy to leave out. The removed subtree stays valid — Jackson's
     * {@code remove} unlinks the child, it does not clear it — so the returned node is still the
     * one the hand-written emitter walks.
     *
     * @return the array, or a missing node when the field is absent or not an array
     */
    private static JsonNode take(JsonNode parent, String field) {
        JsonNode v = parent.path(field);
        if (parent.isObject() && v.isArray()) ((ObjectNode) parent).remove(field);
        return v;
    }

    /**
     * {@link #take} for an OBJECT field — a map, which {@link #take} deliberately will not touch.
     *
     * <p>Separate from {@code take} rather than a widened version of it: {@code take} guards on
     * {@code isArray()} precisely so that the nested schema objects it walks past (`variables`,
     * `transactionScope`) stay on the node document. Only a field with a hand-written row emitter
     * below may be removed, and for a map that is a deliberate decision per field, not a default.
     *
     * @return the object, or a missing node when the field is absent or not an object
     */
    private static JsonNode takeObject(JsonNode parent, String field) {
        JsonNode v = parent.path(field);
        if (parent.isObject() && v.isObject()) ((ObjectNode) parent).remove(field);
        return v;
    }

    /** {@link #take} one level down, leaving the wrapper object's own scalars in place. */
    private static JsonNode takeNested(JsonNode parent, String objectField, String field) {
        JsonNode obj = parent.path(objectField);
        return obj.isObject() ? take(obj, field) : obj.path(field);
    }

    /**
     * Table name to schema, taken from this node's own table list.
     *
     * <p>A cursor's {@code tables} is an array of bare names — {@code ["ERP_CUSTOMER", …]} — while
     * {@code directTables} in the <b>same document</b> carries {@code {schema, name}}. So the
     * schema a cursor's table belongs to is resolvable in one pass, with no second artifact and no
     * post-ingest resolution step.
     *
     * <p>Measured on the ORACLE corpus: all <b>1,103</b> cursor-to-table references resolve this
     * way, and no name appears under two different schemas within one node — so the map cannot be
     * ambiguous where it is used.
     */
    private static Map<String, String> schemaOf(JsonNode tables) {
        if (!tables.isArray()) return Map.of();
        Map<String, String> out = new HashMap<>();
        for (JsonNode t : tables) {
            String name = t.path("name").asText(null);
            if (name != null) out.putIfAbsent(name, t.path("schema").asText(null));
        }
        return out;
    }

    /**
     * Cursors, with each declared table resolved to an {@code am_db_object} reference — ING-014.
     *
     * <p>The tables were previously left to {@code Splitter}, which stored each one as
     * {@code {value: "ERP_CUSTOMER"}} in {@code am_cursor_decl__tables}: the name, and nothing to
     * resolve it. "Which cursors read this table" therefore had no reference to match, exactly as
     * the endpoint pool links did not.
     *
     * <p>{@code objectRef} is <b>nullable by design</b>, and that is the row's actual requirement —
     * "unresolved bare names preserved, not mis-attributed". A name this node does not declare a
     * schema for keeps its name and gets no reference, rather than being attached to a guessed
     * schema. {@code objectRef} is already on {@code OrphanRefChecker}'s nullable allowlist with
     * that reason. On today's corpus nothing takes that path — which is why
     * {@code am_cursor_unresolved} would be an empty collection, and is not created.
     */
    private void emitCursors(IngestContext ctx, DocSink sink, String nodeRef, JsonNode arr,
                             Map<String, String> schemas) {
        if (!arr.isArray()) return;
        int ordinal = 0;
        for (JsonNode e : arr) {
            int line = e.path("line").asInt(0);
            Ids id = IdCodec.nodeChild(Collections.CURSOR_DECL, nodeRef, "cursor", line, ordinal++);

            // Taken off before flatten, for the reason DagAndEndpointMapper records: Splitter
            // emits child rows during flatten, so removing the field afterwards would leave the
            // unresolved rows already written alongside the resolved ones.
            JsonNode tables = e.path("tables");
            if (e.isObject() && tables.isArray()) {
                ((ObjectNode) e).remove("tables");
            }

            Document d = Splitter.flatten(ctx, sink, Collections.CURSOR_DECL, id.key(), e);
            d.put("runId", ctx.runId());
            d.put("nodeRef", nodeRef);
            d.put("key", id.key());
            sink.emit(Collections.CURSOR_DECL, d);

            if (!tables.isArray()) continue;
            int t = 0;
            for (JsonNode nameNode : tables) {
                String name = nameNode.asText(null);
                if (name == null || name.isEmpty()) continue;
                String schema = schemas.get(name);
                Document row = new Document()
                        .append("runId", ctx.runId())
                        .append("nodeRef", nodeRef)
                        .append("cursorRef", id.key())
                        .append("tableName", name)
                        .append("ordinal", t);
                if (schema != null) {
                    // The ENTITY key, not the run-scoped one. `am_db_object` is stored under
                    // ctx.entityKey(schema, name) so that the same table dedups across runs, and
                    // TablesIndexMapper states the rule for children explicitly: they point at the
                    // entity, not at one run's occurrence of it.
                    //
                    // Written with IdCodec.dbObject(runId, …) first, which produced a key that
                    // looks entirely plausible — same shape, same components, run prefix instead
                    // of dialect prefix — and left all 26 distinct tables dangling. Only
                    // OrphanRefChecker distinguished the two.
                    row.append("schemaName", schema)
                            .append("objectRef", ctx.entityKey(schema, name));
                }
                row.put("key", id.key() + "~tbl~" + t);
                sink.emit(Collections.CURSOR_OBJECT, row);
                t++;
            }
        }
    }

    private void emitChildren(IngestContext ctx, DocSink sink, String nodeRef,
                              JsonNode arr, String collection, String kind) {
        if (!arr.isArray()) return;
        int ordinal = 0;
        for (JsonNode e : arr) {
            int line = e.path("line").asInt(0);
            Ids id = IdCodec.nodeChild(collection, nodeRef, kind, line, ordinal++);
            Document d = Splitter.flatten(ctx, sink, collection, id.key(), e);
            d.put("runId", ctx.runId());
            d.put("nodeRef", nodeRef);
            d.put("key", id.key());
            sink.emit(collection, d);
        }
    }

    /**
     * The four variable buckets, as rows in one collection discriminated by {@code varKind}.
     *
     * <p>{@code variables} is a summary object holding {@code typeRefs}, {@code rowtypeRefs},
     * {@code collections} and {@code plain}. They are separate arrays in the artifact because they
     * are counted separately, but they are the same entity — a declared variable — and splitting
     * them into four collections would make "every variable in this procedure" a four-way union.
     *
     * <p>The kind is carried as a field so that question stays one query and
     * "every variable that references a table column" stays one filter.
     */
    private void emitVariableRefs(IngestContext ctx, DocSink sink, String nodeRef,
                                  Map<String, JsonNode> buckets) {
        for (String kind : VAR_KINDS) {
            JsonNode arr = buckets.get(kind);
            if (arr == null || !arr.isArray()) continue;
            int ordinal = 0;
            for (JsonNode e : arr) {
                int line = e.path("line").asInt(0);
                Ids id = IdCodec.nodeChild(Collections.VARIABLE_TYPE_REF, nodeRef,
                        "var:" + kind, line, ordinal++);
                Document d = Splitter.flatten(ctx, sink, Collections.VARIABLE_TYPE_REF, id.key(), e);
                d.put("runId", ctx.runId());
                d.put("nodeRef", nodeRef);
                d.put("varKind", kind);
                d.put("key", id.key());
                sink.emit(Collections.VARIABLE_TYPE_REF, d);
            }
        }
    }

    /**
     * Declared FOREIGN KEY constraints — one row per constraint.
     *
     * <p>Built by hand rather than through {@code Splitter.flatten} on purpose. Flatten would
     * take {@code columns} and {@code referencedColumns} out into their own child collections,
     * and those two arrays <em>are</em> the constraint: {@code orders(customer_id) ->
     * customers(id)} split into three documents is no longer a statement about a column pairing.
     * They are also bounded — a composite key is a handful of columns, not a subtree — so the
     * reason the other arrays are split (document size scaling with program size) does not apply.
     *
     * <p>Both sides resolve to {@code am_db_object} where the catalogue knows them. That is what
     * makes the reverse question — "which tables reference this one" — an indexed lookup rather
     * than a string match over names. Unresolved stays null rather than guessed, the same rule
     * {@code emitCursors} and {@code emitObjectRows} follow.
     */
    private void emitForeignKeys(IngestContext ctx, DocSink sink, String nodeRef, JsonNode arr) {
        if (!arr.isArray()) return;
        int ordinal = 0;
        for (JsonNode e : arr) {
            int line = e.path("line").asInt(0);
            Ids id = IdCodec.nodeChild(Collections.FOREIGN_KEY, nodeRef, "fk", line, ordinal++);

            String schema = e.path("schema").asText(null);
            String table = e.path("tableName").asText(null);
            String refSchema = e.path("referencedSchema").asText(null);
            String refTable = e.path("referencedTable").asText(null);

            Document d = new Document()
                    .append("runId", ctx.runId())
                    .append("nodeRef", nodeRef)
                    .append("constraintName", e.path("constraintName").asText(null))
                    .append("schemaName", schema)
                    .append("tableName", table)
                    .append("columns", stringList(e.path("columns")))
                    .append("referencedSchema", refSchema)
                    .append("referencedTable", refTable)
                    .append("referencedColumns", stringList(e.path("referencedColumns")))
                    .append("line", line);

            String objectRef = ctx.objectId(schema, table);
            if (objectRef != null) d.append("objectRef", objectRef);
            String referencedObjectRef = ctx.objectId(refSchema, refTable);
            if (referencedObjectRef != null) d.append("referencedObjectRef", referencedObjectRef);

            d.put("key", id.key());
            sink.emit(Collections.FOREIGN_KEY, d);
        }
    }

    private static java.util.List<String> stringList(JsonNode arr) {
        java.util.List<String> out = new java.util.ArrayList<>();
        if (!arr.isArray()) return out;
        for (JsonNode e : arr) {
            String v = e.asText(null);
            if (v != null && !v.isEmpty()) out.add(v);
        }
        return out;
    }

    private void emitObjectRows(IngestContext ctx, DocSink sink, String nodeRef,
                                JsonNode arr, String collection, boolean alsoDirect) {
        if (!arr.isArray()) return;
        for (JsonNode e : arr) {
            String schema = e.path("schema").asText(null);
            String name = e.path("name").asText(null);
            Ids id = IdCodec.nodeObjectRow(collection, nodeRef, schema, name);
            Document d = Splitter.flatten(ctx, sink, collection, id.key(), e);
            d.put("runId", ctx.runId());
            d.put("nodeRef", nodeRef);
            // Resolved where the catalogue knows the object; null is legitimate and the verifier
            // reports it rather than the mapper hiding it.
            String objectRef = ctx.objectId(schema, name);
            if (objectRef != null) d.put("objectRef", objectRef);
            if (alsoDirect) d.put("alsoDirectTables", true);
            d.put("key", id.key());
            sink.emit(collection, d);
        }
    }

    /**
     * {@code sequences[].operations} is a map of operation → line numbers. Each line becomes its
     * own row, which is what makes "where is this sequence used" an indexed lookup instead of an
     * array scan inside a document.
     */
    private void emitSequences(IngestContext ctx, DocSink sink, String nodeRef, JsonNode arr) {
        if (!arr.isArray()) return;
        for (JsonNode s : arr) {
            String schema = s.path("schema").asText(null);
            String name = s.path("name").asText(null);
            JsonNode ops = s.path("operations");
            if (!ops.isObject()) continue;

            // The sequence itself, once per (schema, name) — ING-017. Emitted from every use and
            // deduplicated by its key, the same way am_db_object is: which node happened to be
            // read first must not decide whether the entity exists.
            //
            // Before this, a sequence existed only as two string columns on its use rows. Nothing
            // resolved them, so OrphanRefChecker had no field to check, and "which sequences does
            // this system use" was a distinct() over uses rather than a lookup of entities.
            String sequenceRef = IdCodec.sequenceObj(ctx.runId(), schema, name).key();
            Document seq = new Document()
                    .append("runId", ctx.runId())
                    .append("schemaName", schema)
                    .append("sequenceName", name);
            seq.put("key", sequenceRef);
            sink.emit(Collections.SEQUENCE_OBJ, seq);

            var it = ops.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> op = it.next();
                if (!op.getValue().isArray()) continue;
                for (JsonNode lineNode : op.getValue()) {
                    int line = lineNode.asInt();
                    Ids id = IdCodec.nested(Collections.SEQUENCE_USE,
                            nodeRef, schema, name, op.getKey(), String.valueOf(line));
                    Document d = new Document()
                            .append("runId", ctx.runId())
                            .append("nodeRef", nodeRef)
                            .append("schemaName", schema)
                            .append("sequenceName", name)
                            // The resolved reference. The two name fields stay: a use row should
                            // be readable without a join, and removing them would change the
                            // shape the existing screens read.
                            .append("sequenceRef", sequenceRef)
                            .append("operation", op.getKey())
                            .append("line", line);
                    d.put("key", id.key());
                    sink.emit(Collections.SEQUENCE_USE, d);
                }
            }
        }
    }
}
