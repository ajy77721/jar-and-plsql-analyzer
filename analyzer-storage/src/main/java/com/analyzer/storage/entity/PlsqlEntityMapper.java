package com.analyzer.storage.entity;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Maps one {@code api/nodes/<nodeId>.json} to the flow-node entity spine, in a single streaming
 * pass. PLAN-09 Increment E, under PLAN-11's field rules.
 *
 * <h2>The rules this implements, and where each one is</h2>
 *
 * <ol>
 *   <li><b>Read by {@code (artifact, path)}, never by bare field name</b> (PLAN-11 §2.4). This class
 *       maps <em>one</em> artifact — {@code api/nodes/*.json} — and says so in {@link #ARTIFACT}. It
 *       has no field-name table shared with any other artifact, which is what makes the
 *       {@code endpoints} / {@code modules} count-versus-collection collisions (§2.2, §2.3)
 *       structurally impossible here rather than merely avoided: there is nothing to collide with.
 *       A JAR-side mapper is a <em>second class</em> with its own {@code ARTIFACT}, not a second
 *       branch in this one.</li>
 *   <li><b>Scalar-or-array becomes a child collection with an ordinal, always</b> (§2.1) —
 *       {@link OrdinalValues}.</li>
 *   <li><b>Emit nothing for null</b> (rule 4). A table entry with a null {@code name} produces no
 *       {@link TableUsageRow}; the corpus has 68 of them out of 40,748.</li>
 *   <li><b>Never key on {@code depth}, node ordinals or array position</b> (rule 5). {@code depth}
 *       is a column; the keys are {@code (analysis, nodeId)} and
 *       {@code (analysis, nodeId, tableKey, operation, usage)}.</li>
 *   <li><b>Fail loudly on an unobserved shape</b> (rule 6) — {@link EntityShapeException}, raised by
 *       {@link #str}, {@link #integer}, {@link #array} and {@link OrdinalValues} rather than by a
 *       validation pass, so no field can be read without being checked.</li>
 * </ol>
 *
 * <h2>Why it streams</h2>
 *
 * Jackson stays streaming throughout the codebase because a real {@code analysis.json} reached
 * 912 MB. A node document is far smaller (2.05 KB average, 370 KB at the top end) so holding one
 * whole would not itself be fatal — but {@code subtreeTables[]} is the unbounded part of it, growing
 * with everything reachable below the node, and it is the part this mapper turns into the most rows.
 * So the top level is walked token by token and <b>each array element is materialised on its own and
 * released</b>: peak heap is one table entry, not one node document, and the fields the mapper does
 * not map ({@code variables}, {@code cursors}, {@code dynamicSql}, {@code exceptionHandlers}, …) are
 * {@code skipChildren()}'d without ever being built.
 *
 * <h2>Rows are handed to a sink, not returned in a list</h2>
 *
 * For the same reason. A node with 3,000 subtree tables across 5 operations is 15,000 rows; returning
 * them would put the thing the streaming avoids straight back on the heap, and the only consumer
 * batches them into a bulk insert anyway.
 */
public final class PlsqlEntityMapper {

    /**
     * The artifact this mapper reads. PLAN-11 §2.4: a field means what it means <em>in its
     * artifact</em>, so the artifact is part of every path this class resolves and of every shape
     * complaint it raises.
     */
    public static final String ARTIFACT = "api/nodes/*.json";

    private final ObjectMapper mapper;

    public PlsqlEntityMapper(ObjectMapper mapper) {
        this.mapper = mapper == null ? new ObjectMapper() : mapper;
    }

    /** Where the mapped rows go. Three sinks rather than one tagged union: the writer batches by
     *  collection, and a union would make it re-discriminate what the mapper already knows. */
    public interface RowSink {
        void node(FlowNodeRow row);

        void edge(FlowEdgeRow row);

        void tableUsage(TableUsageRow row);
    }

    /**
     * Maps one node document.
     *
     * <p>The stream is <b>not</b> closed here: the caller opened it and owns it, which is the same
     * ownership rule {@code ArtifactScanRepository#arrayRows} follows so that an abandoned read still
     * releases an S3 connection at exactly one place.
     *
     * @return the number of rows emitted, so a caller can log an ingest without counting twice
     */
    public long map(String analysis, InputStream in, RowSink sink) throws IOException {
        try (JsonParser p = mapper.getFactory().createParser(in)) {
            if (p.nextToken() != JsonToken.START_OBJECT) {
                throw new EntityShapeException(ARTIFACT, "$",
                        String.valueOf(p.currentToken()), "an object");
            }
            return mapObject(analysis, p, sink);
        }
    }

    private long mapObject(String analysis, JsonParser p, RowSink sink) throws IOException {
        String nodeId = null, name = null, schema = null, objectType = null, sourceFile = null;
        Integer depth = null, lineStart = null, lineEnd = null, linesOfCode = null;
        List<FlowNodeRow.Parameter> parameters = new ArrayList<>();
        List<FlowNodeRow.StatementCount> statements = new ArrayList<>();

        // Deferred, because the table arrays can precede nodeId in the document and a usage row
        // needs it. Held as raw rows with the node fields filled in at the end — cheaper than a
        // second pass and bounded by the same one-element-at-a-time reading below, because only the
        // small scalar projection of each entry is kept, never the entry itself.
        // A LinkedHashMap and not a list: the map key is the row key
        // (tableKey, operation, usage), which is what collapses the tables[]/directTables[]
        // duplication in one place instead of leaving it to every consumer to notice. Insertion
        // order is preserved so a stored run reads back in document order, which makes a diff
        // against the artifact legible.
        java.util.LinkedHashMap<String, PendingUsage> pending = new java.util.LinkedHashMap<>();
        List<PendingEdge> pendingEdges = new ArrayList<>();

        while (p.nextToken() == JsonToken.FIELD_NAME) {
            String field = p.currentName();
            p.nextToken();
            switch (field) {
                case "nodeId" -> nodeId = str(p, field);
                case "name" -> name = str(p, field);
                case "schema" -> schema = str(p, field);
                case "objectType" -> objectType = str(p, field);
                case "sourceFile" -> sourceFile = str(p, field);
                case "depth" -> depth = integer(p, field);
                case "lineStart" -> lineStart = integer(p, field);
                case "lineEnd" -> lineEnd = integer(p, field);
                case "linesOfCode" -> linesOfCode = integer(p, field);
                case "parameters" -> readParameters(p, parameters);
                case "statementSummary" -> readStatementSummary(p, statements);
                case "calls" -> readCalls(p, pendingEdges);
                // tables[] and directTables[] hold the same content in every document observed —
                // two names kept for the two readers that want them. BOTH are mapped, to DIRECT
                // usage, and the (tableKey, operation, usage) de-duplication below collapses them.
                // Mapping only one would silently lose a node's tables the day they diverge; mapping
                // both without de-duplicating would double every DIRECT row, and a caller counting
                // "how many nodes write this table" would get twice the answer.
                case "tables", "directTables" -> readTables(p, field, TableUsageRow.DIRECT, pending);
                case "subtreeTables" -> readTables(p, field, TableUsageRow.SUBTREE, pending);
                // Everything else in the document is either already an entity of its own (cursors,
                // dynamicSql, sequences — PLAN-08 Q61's four facets, deliberately out of this
                // slice, see the class comment on MongoPlsqlEntityIndex) or not modelled yet.
                // skipChildren() is what keeps this a streaming pass: the subtree is discarded at
                // the token level and never becomes objects.
                default -> p.skipChildren();
            }
        }

        if (nodeId == null || nodeId.isBlank()) {
            // Not a shape error — a document with no node id is not a node, and it is the one field
            // everything downstream keys on. Loud, per rule 6.
            throw new EntityShapeException(ARTIFACT, "nodeId", "absent", "a non-blank string");
        }

        long emitted = 0;
        sink.node(new FlowNodeRow(analysis, nodeId, name, schema, objectType, depth,
                lineStart, lineEnd, linesOfCode, sourceFile,
                List.copyOf(parameters), List.copyOf(statements)));
        emitted++;
        for (PendingEdge e : pendingEdges) {
            sink.edge(new FlowEdgeRow(analysis, nodeId, e.toNodeId, e.schema, e.objectName,
                    e.packageName, e.type, e.argCount, e.count, e.lines));
            emitted++;
        }
        for (PendingUsage u : pending.values()) {
            sink.tableUsage(new TableUsageRow(analysis, nodeId, name, u.tableKey, u.schema, u.name,
                    u.objectType, u.operation, u.usage, u.totalUsageCount, u.directUsageCount,
                    u.lines));
            emitted++;
        }
        return emitted;
    }

    /* ─────────────────────────── children ─────────────────────────── */

    private void readParameters(JsonParser p, List<FlowNodeRow.Parameter> out) throws IOException {
        int ordinal = 0;
        for (JsonNode e : elements(p, "parameters")) {
            // The ordinal is the parameter's position in the signature, which is meaningful — it is
            // how the third argument of a call binds. It is stored, and it is still not a key
            // (rule 5): the key is (nodeId, ordinal) as a whole, and nothing outside the node
            // references it.
            out.add(new FlowNodeRow.Parameter(ordinal++,
                    text(e, "name", "parameters[].name"),
                    text(e, "dataType", "parameters[].dataType"),
                    text(e, "direction", "parameters[].direction"),
                    number(e, "line", "parameters[].line")));
        }
    }

    /**
     * {@code statementSummary} — an object whose keys are dialect tokens and whose values are counts.
     *
     * <p>PLAN-11 §3: <b>never an enum</b>. It says 11–28 tokens; this corpus has 43 distinct ones
     * across 712 node documents, which is exactly why. The keys are stored as data.
     */
    private void readStatementSummary(JsonParser p, List<FlowNodeRow.StatementCount> out)
            throws IOException {
        if (p.currentToken() == JsonToken.VALUE_NULL) return;
        if (p.currentToken() != JsonToken.START_OBJECT) {
            throw new EntityShapeException(ARTIFACT, "statementSummary",
                    String.valueOf(p.currentToken()), "an object of token -> count");
        }
        while (p.nextToken() == JsonToken.FIELD_NAME) {
            String token = p.currentName();
            p.nextToken();
            if (p.currentToken() == JsonToken.VALUE_NULL) continue;   // rule 4
            out.add(new FlowNodeRow.StatementCount(token,
                    integer(p, "statementSummary." + token)));
        }
    }

    private void readCalls(JsonParser p, List<PendingEdge> out) throws IOException {
        for (JsonNode e : elements(p, "calls")) {
            String target = text(e, "target", "calls[].target");
            String schema = text(e, "schema", "calls[].schema");
            String object = text(e, "objectName", "calls[].objectName");
            // The edge's identity is the callee's node id. `target` is what the analyser resolved
            // it to and is present in every observed call; the schema$object composition is the
            // same key the crawler builds, and is the fallback rather than the primary so that an
            // unresolved call is not silently turned into a resolved-looking one.
            String toNodeId = target != null && !target.isBlank() ? target
                    : (schema == null || object == null ? null : schema + "$" + object);
            if (toNodeId == null) continue;   // rule 4: no callee, no edge
            out.add(new PendingEdge(toNodeId, schema, object,
                    text(e, "packageName", "calls[].packageName"),
                    text(e, "type", "calls[].type"),
                    number(e, "argCount", "calls[].argCount"),
                    number(e, "count", "calls[].count"),
                    ints(e.get("lines"), "calls[].lines")));
        }
    }

    /**
     * {@code tables[]} / {@code directTables[]} / {@code subtreeTables[]} → {@link TableUsageRow}.
     *
     * <p>One row per {@code (table, operation)}, not one per table: "which nodes <em>write</em>
     * table X" is a different question from "which nodes touch it", and a single row carrying an
     * operations object could only answer the second. The fan-out is bounded by the 9 operation
     * tokens the corpus observes.
     */
    private void readTables(JsonParser p, String path, String usage,
                            java.util.Map<String, PendingUsage> out) throws IOException {
        for (JsonNode e : elements(p, path)) {
            String schema = text(e, "schema", path + "[].schema");
            String name = text(e, "name", path + "[].name");
            String tableKey = TableUsageRow.keyOf(schema, name);
            // Rule 4, and the concrete reason for it: 68 of 40,748 observed table entries have a
            // null name. Coercing them would put a table called "null" into the index.
            if (tableKey == null) continue;

            String objectType = text(e, "objectType", path + "[].objectType");
            Integer total = number(e, "totalUsageCount", path + "[].totalUsageCount");
            Integer direct = number(e, "directUsageCount", path + "[].directUsageCount");

            JsonNode operations = e.get("operations");
            if (operations == null || operations.isNull()) continue;   // rule 4
            if (!operations.isObject()) {
                throw new EntityShapeException(ARTIFACT, path + "[].operations",
                        operations.getNodeType().toString().toLowerCase(),
                        "an object of operation -> lines");
            }
            for (java.util.Iterator<Map.Entry<String, JsonNode>> it = operations.fields();
                 it.hasNext(); ) {
                Map.Entry<String, JsonNode> op = it.next();
                out.putIfAbsent(tableKey + ' ' + op.getKey() + ' ' + usage,
                        new PendingUsage(tableKey, schema, name, objectType, op.getKey(), usage,
                                total, direct,
                                ints(op.getValue(), path + "[].operations." + op.getKey())));
            }
        }
    }

    /* ─────────────────────────── shape checks (rule 6) ─────────────────────────── */

    /**
     * Materialises an array one element at a time.
     *
     * <p>{@code readValueAsTree} per element rather than for the whole array: {@code subtreeTables}
     * is the unbounded part of a node document and this is what keeps peak heap at one entry. The
     * iterable is single-use and tied to the parser's position, which is why it is private.
     */
    private Iterable<JsonNode> elements(JsonParser p, String path) throws IOException {
        if (p.currentToken() == JsonToken.VALUE_NULL) return List.of();   // rule 4
        if (p.currentToken() != JsonToken.START_ARRAY) {
            throw new EntityShapeException(ARTIFACT, path, String.valueOf(p.currentToken()),
                    "an array");
        }
        return () -> new java.util.Iterator<>() {
            private JsonNode next = advance();

            private JsonNode advance() {
                try {
                    if (p.nextToken() == JsonToken.END_ARRAY) return null;
                    if (p.currentToken() != JsonToken.START_OBJECT) {
                        throw new EntityShapeException(ARTIFACT, path + "[]",
                                String.valueOf(p.currentToken()), "an object");
                    }
                    return p.readValueAsTree();
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            }

            @Override public boolean hasNext() { return next != null; }

            @Override public JsonNode next() {
                JsonNode current = next;
                if (current == null) throw new java.util.NoSuchElementException();
                next = advance();
                return current;
            }
        };
    }

    private static String str(JsonParser p, String path) throws IOException {
        JsonToken t = p.currentToken();
        if (t == JsonToken.VALUE_NULL) return null;                       // rule 4
        if (t != JsonToken.VALUE_STRING) {
            throw new EntityShapeException(ARTIFACT, path, String.valueOf(t), "a string");
        }
        return p.getText();
    }

    private static Integer integer(JsonParser p, String path) throws IOException {
        JsonToken t = p.currentToken();
        if (t == JsonToken.VALUE_NULL) return null;                       // rule 4
        if (t != JsonToken.VALUE_NUMBER_INT) {
            throw new EntityShapeException(ARTIFACT, path, String.valueOf(t), "an integer");
        }
        return p.getIntValue();
    }

    private static String text(JsonNode owner, String field, String path) {
        JsonNode v = owner.get(field);
        if (v == null || v.isNull()) return null;                         // rule 4
        if (!v.isTextual()) {
            throw new EntityShapeException(ARTIFACT, path,
                    v.getNodeType().toString().toLowerCase(), "a string");
        }
        return v.textValue();
    }

    private static Integer number(JsonNode owner, String field, String path) {
        JsonNode v = owner.get(field);
        if (v == null || v.isNull()) return null;                         // rule 4
        if (!v.isIntegralNumber()) {
            throw new EntityShapeException(ARTIFACT, path,
                    v.getNodeType().toString().toLowerCase(), "an integer");
        }
        return v.intValue();
    }

    private static List<Integer> ints(JsonNode v, String path) {
        if (v == null || v.isNull()) return List.of();                    // rule 4
        if (!v.isArray()) {
            throw new EntityShapeException(ARTIFACT, path,
                    v.getNodeType().toString().toLowerCase(), "an array of integers");
        }
        List<Integer> out = new ArrayList<>(v.size());
        for (JsonNode e : v) {
            if (e.isNull()) continue;
            if (!e.isIntegralNumber()) {
                throw new EntityShapeException(ARTIFACT, path + "[]",
                        e.getNodeType().toString().toLowerCase(), "an integer");
            }
            out.add(e.intValue());
        }
        return out;
    }

    private record PendingUsage(String tableKey, String schema, String name, String objectType,
                                String operation, String usage, Integer totalUsageCount,
                                Integer directUsageCount, List<Integer> lines) {
    }

    private record PendingEdge(String toNodeId, String schema, String objectName,
                               String packageName, String type, Integer argCount, Integer count,
                               List<Integer> lines) {
    }

    /** Convenience for a caller that wants only one kind of row and will discard the rest. */
    public static RowSink sinkOf(Consumer<FlowNodeRow> nodes,
                                 Consumer<FlowEdgeRow> edges,
                                 Consumer<TableUsageRow> usages) {
        return new RowSink() {
            @Override public void node(FlowNodeRow row) { if (nodes != null) nodes.accept(row); }

            @Override public void edge(FlowEdgeRow row) { if (edges != null) edges.accept(row); }

            @Override public void tableUsage(TableUsageRow row) {
                if (usages != null) usages.accept(row);
            }
        };
    }
}
