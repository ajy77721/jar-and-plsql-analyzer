package com.plsqlanalyzer.web.parser.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ShallowTree;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static com.plsqlanalyzer.web.parser.service.StaticAnalysisReader.textOrNull;

/**
 * Estate facets — <b>views</b> (and materialized views), <b>triggers</b> and <b>package global
 * variables</b> — synthesized from the analysis's own artifacts, in the same read-through /
 * sidecar shape as {@link SequenceFacetReader} and {@link NodeScanFacetReader}.
 *
 * <h2>Where the data comes from (grammar-first)</h2>
 *
 * <p>Nothing here re-parses SQL. Every field is read from an artifact the crawl already wrote
 * <em>from the ANTLR parse tree</em> (or from the DB catalog the crawl validated against):
 *
 * <ul>
 *   <li><b>Views</b> — {@code api/tables/index.json}. The table index's per-view entry carries
 *       {@code objectType} ({@code VIEW}/{@code MATERIALIZED_VIEW}), {@code dbColumns} (the column
 *       list), {@code viewDefinition} (the defining SQL), {@code viewTableOperations} (the base
 *       tables the view reads, with joins), {@code viewJoins}, and {@code usedBy} (the nodes that
 *       read the view). All of these are produced by the dialect visitors' {@code visitCreate_view}
 *       / {@code visitCreate_materialized_view} and the table enrichers.</li>
 *   <li><b>Triggers</b> — {@code api/tables/index.json}'s per-table {@code triggers[]} plus the
 *       {@code TRIGGER} nodes in {@code api/index.json} / {@code api/nodes/*.json}. The DB-discovery
 *       path ({@code TriggerDiscoverer}) already carries {@code definition} (the body), {@code
 *       triggerType} (e.g. {@code AFTER STATEMENT}), {@code status} and {@code tableOps} (what the
 *       body touches); the source-parsed path carries the node id, and its {@code api/nodes/*.json}
 *       detail carries the trigger body's own {@code tables} (its table operations), plus the
 *       grammar-captured {@code triggerWhenClause}/{@code triggerLevel}/{@code triggerBody}.</li>
 *   <li><b>Package globals</b> — {@code api/nodes/*.json}'s {@code externalPackageVarRefDetails},
 *       one row per {@code PKG.VAR} read/write with the referencing node, line and access type,
 *       merged with each package node's {@code packageGlobalDeclarations} (the grammar-captured
 *       {@code name/dataType/defaultValue/constant} of its package-level variables — see
 *       {@link #getGlobals(String)}).</li>
 * </ul>
 *
 * <h2>Honest degradation per dialect</h2>
 *
 * <p>Each facet is empty — never wrong — where the concept or the artifact does not exist:
 * MySQL has no materialized views; PostgreSQL triggers have no body (the dependency edge is the
 * handler function); package globals exist only in Oracle PL/SQL, so the other four dialects
 * report none. Where a field was not persisted (e.g. an MV whose refresh metadata predates this
 * crawl, or a reference-only global with no declaration) it is returned as JSON null /
 * {@code declared:false} rather than guessed.
 */
class EstateFacetReader {

    /** Upper bound on nodes visited while closing a view/trigger chain, so a pathological graph
     *  cannot make one request unbounded. A truncated chain says so. */
    private static final int CHAIN_CAP = 512;

    private final StaticAnalysisReader r;

    EstateFacetReader(StaticAnalysisReader r) {
        this.r = r;
    }

    /* ─────────────────────────── shared helpers ─────────────────────────── */

    private JsonNode readTree(ArtifactKey key) throws IOException {
        try (InputStream in = r.openRead(key)) {
            return r.mapper.readTree(in);
        }
    }

    private List<JsonNode> readAll(List<ArtifactKey> files) throws IOException {
        return ShallowTree.parallelMap(r.store(), files, this::readTree);
    }

    /** The last dotted segment, upper-cased — how the artifacts key object names. */
    static String norm(String s) {
        if (s == null) return "";
        String t = s.trim();
        int dot = t.lastIndexOf('.');
        if (dot >= 0) t = t.substring(dot + 1);
        return t.toUpperCase(Locale.ROOT);
    }

    private static boolean isView(String objectType) {
        return "VIEW".equalsIgnoreCase(objectType) || "MATERIALIZED_VIEW".equalsIgnoreCase(objectType);
    }

    /** The tables index ({@code {tables:[...]}}), memoized for this analysis, or null when absent. */
    private JsonNode tablesIndex(String name) {
        String ck = r.cacheKey(name, "estate-tables-raw");
        JsonNode cached = r.synthesizedCache.get(ck);
        if (cached != null) return cached.isNull() ? null : cached;
        JsonNode doc;
        try {
            doc = r.getTables(name);
        } catch (Exception e) {
            doc = null;
        }
        r.synthesizedCache.put(ck, doc == null ? r.mapper.nullNode() : doc);
        return doc;
    }

    /** {@code NAME -> objectType} over every table entry, so a base table can be classified. */
    private static Map<String, String> typeIndex(JsonNode tables) {
        Map<String, String> out = new HashMap<>();
        for (JsonNode t : tables.path("tables")) {
            String n = textOrNull(t, "name");
            if (n == null) continue;
            String ot = textOrNull(t, "objectType");
            if (ot != null) out.put(norm(n), ot);
        }
        return out;
    }

    private static JsonNode entryFor(JsonNode tables, String name) {
        for (JsonNode t : tables.path("tables")) {
            if (norm(textOrNull(t, "name")).equals(norm(name))) return t;
        }
        return null;
    }

    /** The node details of the analysis, keyed by nodeId — the node-scan shape the other facets use. */
    private Map<String, JsonNode> nodeDetails(String name) throws IOException {
        Map<String, JsonNode> out = new LinkedHashMap<>();
        for (JsonNode n : readAll(r.collectNodeFiles(name))) {
            String id = textOrNull(n, "nodeId");
            if (id != null) out.put(id, n);
        }
        return out;
    }

    /* ═══════════════════════════════ VIEWS ═══════════════════════════════ */

    /** Every view / materialized view, one row each, in tables-index order. Never null. */
    JsonNode getViews(String name) throws IOException {
        ObjectMapper m = r.mapper;
        String ck = r.cacheKey(name, "views");
        JsonNode cached = r.synthesizedCache.get(ck);
        if (cached != null) return cached;

        ArrayNode out = m.createArrayNode();
        JsonNode tables = tablesIndex(name);
        if (tables != null) {
            for (JsonNode t : tables.path("tables")) {
                if (isView(textOrNull(t, "objectType"))) out.add(viewRow(m, t));
            }
        }
        r.synthesizedCache.put(ck, out);
        return out;
    }

    private ObjectNode viewRow(ObjectMapper m, JsonNode t) {
        ObjectNode row = m.createObjectNode();
        String vname = textOrNull(t, "name");
        String schema = textOrNull(t, "schema");
        String ot = textOrNull(t, "objectType");
        row.put("name", vname != null ? vname : "");
        row.put("schema", schema != null ? schema : "");
        row.put("objectType", ot != null ? ot : "VIEW");
        row.put("isMaterialized", "MATERIALIZED_VIEW".equalsIgnoreCase(ot));

        ArrayNode columns = row.putArray("columns");
        for (JsonNode c : t.path("dbColumns")) {
            ObjectNode col = m.createObjectNode();
            col.put("name", textOrNull(c, "columnName") != null ? textOrNull(c, "columnName") : "");
            col.put("dataType", textOrNull(c, "dataType") != null ? textOrNull(c, "dataType") : "");
            col.put("nullable", c.path("nullable").asBoolean(true));
            columns.add(col);
        }
        row.put("columnCount", columns.size());
        row.put("definition", textOrNull(t, "viewDefinition") != null ? textOrNull(t, "viewDefinition") : "");

        ArrayNode base = row.putArray("baseTables");
        Set<String> seen = new HashSet<>();
        String self = norm(vname);
        for (JsonNode op : t.path("viewTableOperations")) {
            String bn = textOrNull(op, "tableName");
            if (bn == null || bn.isBlank()) continue;
            String key = norm(bn);
            // The table index carries a view's own name as one of its table operations (the view
            // node's own op). A view does not read itself — dropping the self-edge keeps the chain
            // acyclic and the base-table list honest.
            if (key.equals(self)) continue;
            if (!seen.add(key)) continue;
            ObjectNode b = m.createObjectNode();
            b.put("name", bn);
            b.put("schema", textOrNull(op, "schema") != null ? textOrNull(op, "schema") : "");
            b.put("operation", textOrNull(op, "operation") != null ? textOrNull(op, "operation") : "SELECT");
            base.add(b);
        }
        row.put("baseTableCount", base.size());

        ArrayNode usedBy = row.putArray("usedBy");
        for (JsonNode u : t.path("usedBy")) usedBy.add(usedByRow(m, u));
        row.put("usedByCount", usedBy.size());

        // MV refresh metadata — persisted on the per-view entry by the crawl's Oracle ALL_MVIEWS
        // enrichment (TableValidationService.bulkQueryMViewRefresh), read straight off the entry.
        // Absent for plain views and for dialects without a refresh concept: reported null, never
        // guessed.
        String mode = textOrNull(t, "mvRefreshMode");
        if (mode != null) row.put("mvRefreshMode", mode); else row.putNull("mvRefreshMode");
        String method = textOrNull(t, "mvRefreshMethod");
        if (method != null) row.put("mvRefreshMethod", method); else row.putNull("mvRefreshMethod");
        String lastRefresh = textOrNull(t, "lastRefresh");
        if (lastRefresh != null) row.put("lastRefresh", lastRefresh); else row.putNull("lastRefresh");
        return row;
    }

    private static ObjectNode usedByRow(ObjectMapper m, JsonNode u) {
        ObjectNode o = m.createObjectNode();
        o.put("nodeId", textOrNull(u, "nodeId") != null ? textOrNull(u, "nodeId") : "");
        o.put("nodeName", textOrNull(u, "nodeName") != null ? textOrNull(u, "nodeName") : "");
        o.put("nodeDepth", u.path("nodeDepth").asInt(0));
        JsonNode ops = u.get("operations");
        if (ops != null) o.set("operations", ops);
        return o;
    }

    private ObjectNode findViewRow(ObjectMapper m, String name, String view) throws IOException {
        for (JsonNode row : getViews(name)) {
            if (norm(textOrNull(row, "name")).equals(norm(view))) return (ObjectNode) row;
        }
        return null;
    }

    /** One view's full detail: its row plus its chain and its used-by list, or null when absent. */
    JsonNode getViewDetail(String name, String view) throws IOException {
        ObjectNode row = findViewRow(r.mapper, name, view);
        if (row == null) return null;
        ObjectNode out = row.deepCopy();
        out.set("chain", viewChain(name, view));
        out.set("usedBy", row.path("usedBy").deepCopy());
        return out;
    }

    JsonNode getViewUsedBy(String name, String view) throws IOException {
        ObjectNode row = findViewRow(r.mapper, name, view);
        if (row == null) return null;
        ObjectMapper m = r.mapper;
        ObjectNode out = m.createObjectNode();
        out.put("view", row.path("name").asText(""));
        out.put("schema", row.path("schema").asText(""));
        out.set("usedBy", row.path("usedBy").deepCopy());
        out.put("count", row.path("usedBy").size());
        return out;
    }

    /**
     * The full view-on-view chain: the starting view, every view it transitively reads, and the
     * terminal base tables — closed recursively over {@code viewTableOperations}.
     *
     * <p>Breadth-first over the view→view edges. {@code levels} is one entry per hop (level 0 is the
     * view itself), so a view A → view B → base table T resolves to
     * {@code [[A],[B],[T]]}. A base table that is itself a view already seen is reported as a cycle
     * rather than re-expanded. Bounded by {@link #CHAIN_CAP}.
     */
    JsonNode viewChain(String name, String view) throws IOException {
        ObjectMapper m = r.mapper;
        JsonNode tables = tablesIndex(name);
        Map<String, String> types = tables != null ? typeIndex(tables) : Map.of();

        ObjectNode out = m.createObjectNode();
        ObjectNode start = findViewRow(m, name, view);
        out.put("view", start != null ? start.path("name").asText("") : norm(view));
        out.put("kind", start != null ? start.path("objectType").asText("VIEW") : "VIEW");

        ArrayNode levels = m.createArrayNode();
        ArrayNode nodes = m.createArrayNode();
        ArrayNode edges = m.createArrayNode();
        ArrayNode baseTables = m.createArrayNode();
        Set<String> visited = new HashSet<>();
        Set<String> edgesSeen = new HashSet<>();
        boolean cyclic = false;
        boolean truncated = false;

        String startName = start != null ? norm(start.path("name").asText("")) : norm(view);
        List<String> frontier = new ArrayList<>();
        if (!startName.isEmpty()) { frontier.add(startName); visited.add(startName); }
        ArrayNode level0 = m.createArrayNode();
        if (start != null) level0.add(start.path("name").asText(""));
        levels.add(level0);
        if (start != null) nodes.add(chainNode(m, start.path("name").asText(""), start.path("schema").asText(""),
                start.path("objectType").asText("VIEW")));

        int hops = 0;
        while (!frontier.isEmpty()) {
            if (hops++ > CHAIN_CAP) { truncated = true; break; }
            ArrayNode nextLevel = m.createArrayNode();
            List<String> next = new ArrayList<>();
            for (String v : frontier) {
                JsonNode vRow = findViewRow(m, name, v);
                if (vRow == null) continue;
                for (JsonNode b : vRow.path("baseTables")) {
                    String bn = textOrNull(b, "name");
                    if (bn == null) continue;
                    String bnorm = norm(bn);
                    String btype = types.getOrDefault(bnorm, "TABLE");
                    String ekey = norm(v) + "->" + bnorm;
                    if (edgesSeen.add(ekey)) {
                        ObjectNode e = m.createObjectNode();
                        e.put("from", v);
                        e.put("to", bnorm);
                        e.put("via", textOrNull(b, "operation") != null ? textOrNull(b, "operation") : "SELECT");
                        edges.add(e);
                    }
                    if (isView(btype)) {
                        if (visited.contains(bnorm)) { cyclic = true; continue; }
                        visited.add(bnorm);
                        next.add(bnorm);
                        nextLevel.add(bnorm);
                        JsonNode bRow = findViewRow(m, name, bnorm);
                        nodes.add(chainNode(m, bRow != null ? bRow.path("name").asText("") : bnorm,
                                bRow != null ? bRow.path("schema").asText("") : "",
                                bRow != null ? bRow.path("objectType").asText("VIEW") : "VIEW"));
                    } else {
                        if (!containsText(baseTables, bnorm)) baseTables.add(bnorm);
                        if (!containsText(level0, bnorm)) { /* terminal appears at its own hop */ }
                    }
                }
            }
            if (nextLevel.size() > 0) levels.add(nextLevel);
            else {
                // terminal base tables form the final level
                if (baseTables.size() > 0) levels.add(baseTables.deepCopy());
                break;
            }
            frontier = next;
            if (frontier.isEmpty() && baseTables.size() > 0) {
                // append the terminal level once the view frontier is exhausted
                boolean lastIsBase = levels.size() > 0
                        && levels.get(levels.size() - 1).equals(baseTables);
                if (!lastIsBase) levels.add(baseTables.deepCopy());
            }
        }
        // Any terminal tables reached at the deepest view hop are already appended above; ensure the
        // base-table level is present when the start view read base tables directly.
        if (levels.size() == 1 && baseTables.size() > 0) levels.add(baseTables.deepCopy());

        out.set("levels", levels);
        out.set("nodes", nodes);
        out.set("edges", edges);
        out.set("baseTables", baseTables);
        out.put("cyclic", cyclic);
        out.put("truncated", truncated);
        return out;
    }

    private static ObjectNode chainNode(ObjectMapper m, String name, String schema, String objectType) {
        ObjectNode n = m.createObjectNode();
        n.put("name", name);
        n.put("schema", schema);
        n.put("objectType", objectType);
        n.put("kind", isView(objectType) ? "VIEW" : "TABLE");
        return n;
    }

    private static boolean containsText(JsonNode arr, String text) {
        for (JsonNode n : arr) if (n.asText("").equalsIgnoreCase(text)) return true;
        return false;
    }

    /** {@code {VIEW_NAME -> {view, schema, usedBy[], count}}} — the whole-analysis used-by map. */
    JsonNode getViewsUsedByMap(String name) throws IOException {
        ObjectMapper m = r.mapper;
        ObjectNode out = m.createObjectNode();
        for (JsonNode row : getViews(name)) {
            ObjectNode entry = m.createObjectNode();
            entry.put("view", row.path("name").asText(""));
            entry.put("schema", row.path("schema").asText(""));
            entry.set("usedBy", row.path("usedBy").deepCopy());
            entry.put("count", row.path("usedBy").size());
            out.set(norm(textOrNull(row, "name")), entry);
        }
        return out;
    }

    /* ══════════════════════════════ TRIGGERS ══════════════════════════════ */

    /** Every trigger, one row each, keyed by {@code SCHEMA.NAME}. Never null. */
    JsonNode getTriggers(String name) throws IOException {
        ObjectMapper m = r.mapper;
        String ck = r.cacheKey(name, "triggers");
        JsonNode cached = r.synthesizedCache.get(ck);
        if (cached != null) return cached;

        JsonNode tables = tablesIndex(name);
        Map<String, ObjectNode> byKey = new LinkedHashMap<>();

        // 1. Per-table triggers[] — the richest source (DB-discovered rows carry definition/status).
        if (tables != null) {
            for (JsonNode t : tables.path("tables")) {
                String tableName = textOrNull(t, "name");
                if (tableName == null) continue;
                String tableSchema = textOrNull(t, "schema");
                for (JsonNode trg : t.path("triggers")) {
                    String tn = textOrNull(trg, "name");
                    if (tn == null || tn.isBlank()) continue;
                    String tschema = textOrNull(trg, "schema");
                    String key = key(tschema, tn);
                    ObjectNode row = byKey.computeIfAbsent(key, k -> newTriggerRow(m, tn, tschema));
                    row.put("table", tableName);
                    if (tableSchema != null) row.put("tableSchema", tableSchema);
                    mergeTrigger(row, m, trg);
                }
            }
        }

        // 2. TRIGGER nodes in the index — covers triggers the table index did not attach.
        Map<String, JsonNode> details = nodeDetails(name);
        for (JsonNode n : details.values()) {
            if (!"TRIGGER".equalsIgnoreCase(textOrNull(n, "objectType"))) continue;
            String tn0 = textOrNull(n, "objectName");
            if (tn0 == null) tn0 = textOrNull(n, "name");
            if (tn0 == null) continue;
            final String tn = tn0;
            String tschema = textOrNull(n, "schema");
            String key = key(tschema, tn);
            ObjectNode row = byKey.computeIfAbsent(key, k -> newTriggerRow(m, tn, tschema));
            if (row.path("nodeId").asText("").isEmpty()) row.put("nodeId", textOrNull(n, "nodeId") != null ? textOrNull(n, "nodeId") : "");
            if (row.path("table").asText("").isEmpty()) {
                String tt = textOrNull(n, "triggerTable");
                if (tt != null) row.put("table", tt);
            }
            putIfAbsent(row, "timing", textOrNull(n, "triggerTiming"));
            putIfAbsent(row, "event", textOrNull(n, "triggerEvent"));
            putIfAbsent(row, "whenClause", textOrNull(n, "triggerWhenClause"));
            putIfAbsent(row, "level", textOrNull(n, "triggerLevel"));
            // The persisted body (source-parsed trigger) wins; the source-line slice is the fallback
            // for triggers whose body was never persisted (older analyses / DB rows without a body).
            if (row.path("body").asText("").isBlank()) {
                String tb = textOrNull(n, "triggerBody");
                if (tb != null && !tb.isBlank()) row.put("body", tb);
                else putIfAbsent(row, "body", bodyFromSource(name, n));
            }
            if (row.path("touches").isEmpty()) setTouches(row, m, n.path("tables"));
        }

        ArrayNode out = m.createArrayNode();
        for (ObjectNode row : byKey.values()) finalizeTrigger(m, row);
        byKey.values().forEach(out::add);
        r.synthesizedCache.put(ck, out);
        return out;
    }

    private static String key(String schema, String name) {
        String s = schema == null ? "" : norm(schema);
        return (s.isEmpty() ? "" : s + ".") + norm(name);
    }

    private ObjectNode newTriggerRow(ObjectMapper m, String name, String schema) {
        ObjectNode row = m.createObjectNode();
        row.put("name", name);
        row.put("schema", schema != null ? schema : "");
        row.put("table", "");
        row.put("timing", "");
        row.put("event", "");
        row.put("level", "");
        row.put("triggerType", "");
        row.put("status", "");
        row.put("source", "");
        row.put("body", "");
        row.put("nodeId", "");
        row.putNull("whenClause");
        row.putArray("touches");
        return row;
    }

    private void mergeTrigger(ObjectNode row, ObjectMapper m, JsonNode trg) {
        putIfAbsent(row, "timing", textOrNull(trg, "timing"));
        putIfAbsent(row, "event", textOrNull(trg, "event"));
        putIfAbsent(row, "triggerType", textOrNull(trg, "triggerType"));
        putIfAbsent(row, "status", textOrNull(trg, "status"));
        putIfAbsent(row, "source", textOrNull(trg, "source"));
        putIfAbsent(row, "whenClause", textOrNull(trg, "whenClause"));
        putIfAbsent(row, "level", textOrNull(trg, "level"));
        putIfAbsent(row, "body", textOrNull(trg, "definition"));
        putIfAbsent(row, "nodeId", textOrNull(trg, "nodeId"));
        if (row.path("touches").isEmpty() && trg.has("tableOps")) setTouches(row, m, trg.path("tableOps"));
    }

    private static void putIfAbsent(ObjectNode row, String field, String value) {
        if (value == null || value.isBlank()) return;
        if (row.path(field).asText("").isBlank()) row.put(field, value);
    }

    /** Sets {@code touches[]} from either a table index {@code tableOps[]} or a node detail
     *  {@code tables[]} array — both list {@code {tableName|name, operation|operations}}. */
    private void setTouches(ObjectNode row, ObjectMapper m, JsonNode ops) {
        if (ops == null || !ops.isArray()) return;
        ArrayNode touches = (ArrayNode) row.get("touches");
        Set<String> seen = new HashSet<>();
        for (JsonNode op : ops) {
            String tn = textOrNull(op, "tableName");
            if (tn == null) tn = textOrNull(op, "name");
            if (tn == null) continue;
            String normTn = norm(tn);
            if (!seen.add(normTn)) continue;
            ObjectNode t = m.createObjectNode();
            t.put("name", tn);
            t.put("schema", textOrNull(op, "schema") != null ? textOrNull(op, "schema") : "");
            t.put("objectType", textOrNull(op, "objectType") != null ? textOrNull(op, "objectType") : "TABLE");
            String operation = textOrNull(op, "operation");
            if (operation == null) {
                JsonNode opsMap = op.get("operations");
                operation = firstKey(opsMap);
            }
            t.put("operation", operation != null ? operation : "SELECT");
            touches.add(t);
        }
    }

    private static String firstKey(JsonNode obj) {
        if (obj == null || !obj.isObject() || obj.isEmpty()) return null;
        return obj.fieldNames().next();
    }

    /** The trigger body sliced out of the analysis's own {@code sources/} copy, or null. This is a
     *  line slice of an already-parsed source (never a re-parse). */
    private String bodyFromSource(String name, JsonNode node) {
        String sourceFile = textOrNull(node, "sourceFile");
        if (sourceFile == null || sourceFile.isBlank()) return null;
        try {
            Map<String, ArtifactKey> index = r.sourceKeyIndex(name);
            String src = r.getSourceFromIndex(index, sourceFile);
            if (src == null || src.isEmpty()) return null;
            String[] lines = src.split("\n", -1);
            int from = Math.max(1, node.path("lineStart").asInt(1));
            int to = Math.min(lines.length, node.path("lineEnd").asInt(lines.length));
            if (from > to) return null;
            StringBuilder sb = new StringBuilder();
            for (int i = from; i <= to; i++) {
                if (sb.length() > 0) sb.append('\n');
                sb.append(lines[i - 1]);
            }
            return sb.length() == 0 ? null : sb.toString();
        } catch (Exception e) {
            return null;
        }
    }

    /** Derives the row/statement level from what is persisted, never guessed. */
    private static String levelOf(String triggerType, String body) {
        String tt = triggerType == null ? "" : triggerType.toUpperCase(Locale.ROOT);
        if (tt.contains("STATEMENT")) return "STATEMENT";
        if (tt.contains("EACH ROW") || tt.endsWith("ROW")) return "ROW";
        if (body != null) {
            String b = body.toUpperCase(Locale.ROOT);
            if (b.contains("FOR EACH ROW")) return "ROW";
        }
        return "";
    }

    private void finalizeTrigger(ObjectMapper m, ObjectNode row) {
        // A level persisted by the crawl (source-parsed trigger: the grammar's FOR EACH ROW) is
        // authoritative; only derive it from triggerType/body when the crawl did not supply one.
        String level = row.path("level").asText("");
        if (level.isBlank()) {
            level = levelOf(row.path("triggerType").asText(""), row.path("body").asText(""));
        }
        row.put("level", level);
        row.put("touchesCount", row.path("touches").size());
        row.put("bodyAvailable", !row.path("body").asText("").isBlank());
    }

    private ObjectNode findTriggerRow(ObjectMapper m, String name, String trigger) throws IOException {
        for (JsonNode row : getTriggers(name)) {
            if (norm(textOrNull(row, "name")).equals(norm(trigger))) return (ObjectNode) row;
        }
        return null;
    }

    JsonNode getTriggerDetail(String name, String trigger) throws IOException {
        ObjectNode row = findTriggerRow(r.mapper, name, trigger);
        if (row == null) return null;
        ObjectNode out = row.deepCopy();
        out.set("chain", triggerChain(name, trigger));
        out.set("firedBy", firedBy(name, row.path("table").asText("")));
        return out;
    }

    JsonNode getTriggerUsedBy(String name, String trigger) throws IOException {
        ObjectNode row = findTriggerRow(r.mapper, name, trigger);
        if (row == null) return null;
        ObjectMapper m = r.mapper;
        ObjectNode out = m.createObjectNode();
        out.put("trigger", row.path("name").asText(""));
        out.put("schema", row.path("schema").asText(""));
        out.put("table", row.path("table").asText(""));
        JsonNode fired = firedBy(name, row.path("table").asText(""));
        out.set("firedBy", fired);
        out.put("count", fired.size());
        return out;
    }

    /** The procedures whose DML on the trigger's table can fire it — the table's own {@code usedBy}
     *  filtered to DML operations. */
    private ArrayNode firedBy(String name, String table) throws IOException {
        ArrayNode out = r.mapper.createArrayNode();
        if (table == null || table.isBlank()) return out;
        JsonNode tables = tablesIndex(name);
        if (tables == null) return out;
        JsonNode entry = entryFor(tables, table);
        if (entry == null) return out;
        for (JsonNode u : entry.path("usedBy")) {
            JsonNode ops = u.path("operations");
            if (ops.isObject()) {
                boolean dml = false;
                for (String op : new String[]{"INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE"}) {
                    if (ops.has(op)) { dml = true; break; }
                }
                if (!dml) continue;
            }
            out.add(usedByRow(r.mapper, u));
        }
        return out;
    }

    /**
     * The trigger chain: a trigger whose body touches a table that has other triggers, transitively
     * — closed over {@code touches[]} and the table→trigger binding, and across the views a trigger
     * reads. Level 0 is the trigger; level 1 the triggers on the tables it touches, and so on.
     */
    JsonNode triggerChain(String name, String trigger) throws IOException {
        ObjectMapper m = r.mapper;
        Map<String, ObjectNode> byName = new LinkedHashMap<>();
        for (JsonNode t : getTriggers(name)) byName.put(norm(textOrNull(t, "name")), (ObjectNode) t);

        // table name -> triggers bound to it
        Map<String, List<String>> triggersOnTable = new LinkedHashMap<>();
        for (ObjectNode t : byName.values()) {
            String tab = norm(t.path("table").asText(""));
            if (!tab.isEmpty()) triggersOnTable.computeIfAbsent(tab, k -> new ArrayList<>())
                    .add(norm(t.path("name").asText("")));
        }

        ObjectNode out = m.createObjectNode();
        ObjectNode start = byName.get(norm(trigger));
        out.put("trigger", start != null ? start.path("name").asText("") : norm(trigger));
        out.put("table", start != null ? start.path("table").asText("") : "");

        ArrayNode levels = m.createArrayNode();
        ArrayNode nodes = m.createArrayNode();
        ArrayNode edges = m.createArrayNode();
        ArrayNode touchedTables = m.createArrayNode();
        Set<String> visited = new HashSet<>();
        Set<String> edgesSeen = new HashSet<>();
        boolean cyclic = false;
        boolean truncated = false;

        String startName = norm(trigger);
        ArrayNode level0 = m.createArrayNode();
        if (start != null) {
            level0.add(start.path("name").asText(""));
            visited.add(startName);
            nodes.add(chainTriggerNode(m, start));
        }
        levels.add(level0);

        List<String> frontier = start != null ? List.of(startName) : List.of();
        int hops = 0;
        while (!frontier.isEmpty()) {
            if (hops++ > CHAIN_CAP) { truncated = true; break; }
            ArrayNode nextLevel = m.createArrayNode();
            List<String> next = new ArrayList<>();
            for (String tname : frontier) {
                ObjectNode t = byName.get(tname);
                if (t == null) continue;
                for (JsonNode touch : t.path("touches")) {
                    String tab = norm(textOrNull(touch, "name"));
                    if (tab.isEmpty()) continue;
                    if (!containsText(touchedTables, tab)) touchedTables.add(tab);
                    for (String other : triggersOnTable.getOrDefault(tab, List.of())) {
                        if (other.equals(tname)) continue; // a trigger does not chain to itself
                        String ekey = tname + "->" + other;
                        if (edgesSeen.add(ekey)) {
                            ObjectNode e = m.createObjectNode();
                            e.put("from", tname);
                            e.put("to", other);
                            e.put("via", tab);
                            edges.add(e);
                        }
                        if (visited.contains(other)) { cyclic = true; continue; }
                        visited.add(other);
                        next.add(other);
                        nextLevel.add(other);
                        ObjectNode o = byName.get(other);
                        if (o != null) nodes.add(chainTriggerNode(m, o));
                    }
                }
            }
            if (nextLevel.size() == 0) break;
            levels.add(nextLevel);
            frontier = next;
        }

        out.set("levels", levels);
        out.set("nodes", nodes);
        out.set("edges", edges);
        out.set("touchedTables", touchedTables);
        out.put("cyclic", cyclic);
        out.put("truncated", truncated);
        return out;
    }

    /** {@code {TRIGGER_NAME -> {trigger, table, firedBy[], count}}} — the whole-analysis used-by map. */
    JsonNode getTriggersUsedByMap(String name) throws IOException {
        ObjectMapper m = r.mapper;
        ObjectNode out = m.createObjectNode();
        for (JsonNode row : getTriggers(name)) {
            ObjectNode entry = m.createObjectNode();
            entry.put("trigger", row.path("name").asText(""));
            entry.put("schema", row.path("schema").asText(""));
            entry.put("table", row.path("table").asText(""));
            ArrayNode fired = firedBy(name, row.path("table").asText(""));
            entry.set("firedBy", fired);
            entry.put("count", fired.size());
            out.set(norm(textOrNull(row, "name")), entry);
        }
        return out;
    }

    private static ObjectNode chainTriggerNode(ObjectMapper m, ObjectNode row) {
        ObjectNode n = m.createObjectNode();
        n.put("name", row.path("name").asText(""));
        n.put("schema", row.path("schema").asText(""));
        n.put("table", row.path("table").asText(""));
        n.put("event", row.path("event").asText(""));
        n.put("timing", row.path("timing").asText(""));
        return n;
    }

    /* ══════════════════════════ PACKAGE GLOBALS ══════════════════════════ */

    /**
     * Package global variables, grouped by package, with their estate-wide read/write usage.
     *
     * <p>Oracle-only by nature: package-level variables exist only in PL/SQL packages, so MySQL,
     * PostgreSQL, SQL Server and DB2 report an empty facet. Declarations ({@code dataType},
     * {@code defaultValue}, {@code constant}) come from the package node's persisted
     * {@code packageGlobalDeclarations}; a variable known only through a reference (no declaration
     * found, e.g. an analysis predating declaration persistence) is reported with
     * {@code declared:false} and null type/default rather than a guess.
     */
    JsonNode getGlobals(String name) throws IOException {
        ObjectMapper m = r.mapper;
        String ck = r.cacheKey(name, "globals");
        JsonNode cached = r.synthesizedCache.get(ck);
        if (cached != null) return cached;

        List<JsonNode> nodeFiles = readAll(r.collectNodeFiles(name));

        // Declaration lists persisted per package (PACKAGE_BODY/PACKAGE_SPEC node details): the
        // declaring package plus name/dataType/defaultValue/constant — the exact shape each
        // variable row uses. Keyed package → variable → declaration so a ref can pick up its own
        // declaration, and a declared-but-unreferenced variable still appears.
        Map<String, Map<String, JsonNode>> declarations = new LinkedHashMap<>();
        for (JsonNode node : nodeFiles) {
            JsonNode decls = node.get("packageGlobalDeclarations");
            if (decls == null || !decls.isArray() || decls.isEmpty()) continue;
            String pkg = textOrNull(node, "packageName");
            if (pkg == null || pkg.isBlank()) pkg = textOrNull(node, "objectName");
            if (pkg == null || pkg.isBlank()) pkg = textOrNull(node, "name");
            if (pkg == null || pkg.isBlank()) continue;
            Map<String, JsonNode> byVar = declarations.computeIfAbsent(norm(pkg), k -> new LinkedHashMap<>());
            for (JsonNode d : decls) {
                String vn = textOrNull(d, "name");
                if (vn != null && !vn.isBlank()) byVar.putIfAbsent(norm(vn), d);
            }
        }

        Map<String, ObjectNode> byPackage = new LinkedHashMap<>();
        for (JsonNode node : nodeFiles) {
            JsonNode refs = node.get("externalPackageVarRefDetails");
            if (refs == null || !refs.isArray()) continue;
            for (JsonNode ref : refs) {
                String pkg = textOrNull(ref, "packageName");
                String var = textOrNull(ref, "variableName");
                if (pkg == null || pkg.isBlank() || var == null || var.isBlank()) continue;
                String pkey = norm(pkg);
                ObjectNode row = byPackage.computeIfAbsent(pkey, k -> newPackageRow(m, k));
                addVariable(row, norm(var), declarationFor(declarations, pkey, var));
                row.put("usageCount", row.get("usageCount").asInt() + 1);
                String access = textOrNull(ref, "accessType");
                if ("WRITE".equalsIgnoreCase(access)) row.put("writeCount", row.get("writeCount").asInt() + 1);
                else if ("READ".equalsIgnoreCase(access)) row.put("readCount", row.get("readCount").asInt() + 1);
            }
        }

        // Declared-but-unreferenced packages/variables still surface (declared:true), appended after
        // the referenced ones so the common case keeps its existing order.
        for (Map.Entry<String, Map<String, JsonNode>> e : declarations.entrySet()) {
            ObjectNode row = byPackage.computeIfAbsent(e.getKey(), k -> newPackageRow(m, k));
            for (JsonNode d : e.getValue().values()) addVariable(row, norm(textOrNull(d, "name")), d);
        }

        ArrayNode out = m.createArrayNode();
        for (ObjectNode row : byPackage.values()) row.put("variableCount", row.path("variables").size());
        byPackage.values().forEach(out::add);
        r.synthesizedCache.put(ck, out);
        return out;
    }

    private static ObjectNode newPackageRow(ObjectMapper m, String pkey) {
        ObjectNode o = m.createObjectNode();
        o.put("package", pkey);
        o.putArray("variables");
        o.put("usageCount", 0);
        o.put("readCount", 0);
        o.put("writeCount", 0);
        return o;
    }

    private static JsonNode declarationFor(Map<String, Map<String, JsonNode>> declarations,
                                           String pkey, String var) {
        Map<String, JsonNode> byVar = declarations.get(pkey);
        return byVar == null ? null : byVar.get(norm(var));
    }

    /** Adds a variable row; when a persisted declaration exists it fills declared/type/default/const
     *  instead of the honest {@code declared:false} fallback used for reference-only variables. */
    private static void addVariable(ObjectNode row, String var, JsonNode decl) {
        ArrayNode vars = (ArrayNode) row.get("variables");
        for (JsonNode v : vars) if (v.path("name").asText("").equalsIgnoreCase(var)) return;
        ObjectNode v = row.objectNode();
        v.put("name", var);
        if (decl != null) {
            v.put("declared", true);
            String dt = textOrNull(decl, "dataType");
            if (dt != null) v.put("dataType", dt); else v.putNull("dataType");
            String dv = textOrNull(decl, "defaultValue");
            if (dv != null) v.put("defaultValue", dv); else v.putNull("defaultValue");
            v.put("constant", decl.path("constant").asBoolean(false));
        } else {
            v.putNull("dataType");
            v.putNull("defaultValue");
            v.put("constant", false);
            v.put("declared", false);
        }
        vars.add(v);
    }

    private ObjectNode findGlobalRow(ObjectMapper m, String name, String pkg) throws IOException {
        for (JsonNode row : getGlobals(name)) {
            if (norm(textOrNull(row, "package")).equals(norm(pkg))) return (ObjectNode) row;
        }
        return null;
    }

    JsonNode getGlobalDetail(String name, String pkg) throws IOException {
        ObjectNode row = findGlobalRow(r.mapper, name, pkg);
        if (row == null) return null;
        ObjectNode out = row.deepCopy();
        out.set("usedBy", globalUsedBy(name, null, norm(pkg)));
        return out;
    }

    JsonNode getGlobalUsedBy(String name, String variable) throws IOException {
        ObjectMapper m = r.mapper;
        ObjectNode out = m.createObjectNode();
        out.put("name", variable != null ? variable : "");
        JsonNode usedBy = globalUsedBy(name, variable, null);
        out.set("usedBy", usedBy);
        out.put("count", usedBy.size());
        return out;
    }

    /** {@code {VARIABLE_NAME -> {name, usedBy[], count}}} across every package. */
    JsonNode getGlobalsUsedByMap(String name) throws IOException {
        ObjectMapper m = r.mapper;
        Map<String, ArrayNode> byVar = new LinkedHashMap<>();
        for (JsonNode node : readAll(r.collectNodeFiles(name))) {
            JsonNode refs = node.get("externalPackageVarRefDetails");
            if (refs == null || !refs.isArray()) continue;
            String nodeId = textOrNull(node, "nodeId");
            String nodeName = textOrNull(node, "name");
            String sourceFile = textOrNull(node, "sourceFile");
            for (JsonNode ref : refs) {
                String v = norm(textOrNull(ref, "variableName"));
                if (v.isEmpty()) continue;
                ArrayNode list = byVar.computeIfAbsent(v, k -> m.createArrayNode());
                ObjectNode u = m.createObjectNode();
                u.put("package", norm(textOrNull(ref, "packageName")));
                u.put("variable", v);
                u.put("procedureId", nodeId != null ? nodeId : "");
                u.put("procedureName", nodeName != null ? nodeName : "");
                u.put("accessType", textOrNull(ref, "accessType") != null ? textOrNull(ref, "accessType") : "UNKNOWN");
                u.put("lineNumber", ref.path("line").asInt(0));
                u.put("sourceFile", sourceFile != null ? sourceFile : "");
                list.add(u);
            }
        }
        ObjectNode out = m.createObjectNode();
        for (Map.Entry<String, ArrayNode> e : byVar.entrySet()) {
            ObjectNode entry = m.createObjectNode();
            entry.put("name", e.getKey());
            entry.set("usedBy", e.getValue());
            entry.put("count", e.getValue().size());
            out.set(e.getKey(), entry);
        }
        return out;
    }

    /**
     * Where a global is read/written across the estate. With {@code variable} set, matches by
     * variable name across every package; otherwise matches every variable of one package.
     */
    private ArrayNode globalUsedBy(String name, String variable, String pkg) throws IOException {
        ObjectMapper m = r.mapper;
        ArrayNode out = m.createArrayNode();
        for (JsonNode node : readAll(r.collectNodeFiles(name))) {
            JsonNode refs = node.get("externalPackageVarRefDetails");
            if (refs == null || !refs.isArray()) continue;
            String nodeId = textOrNull(node, "nodeId");
            String nodeName = textOrNull(node, "name");
            String sourceFile = textOrNull(node, "sourceFile");
            for (JsonNode ref : refs) {
                String p = norm(textOrNull(ref, "packageName"));
                String v = norm(textOrNull(ref, "variableName"));
                boolean match = variable != null
                        ? v.equals(norm(variable))
                        : p.equals(pkg);
                if (!match) continue;
                ObjectNode u = m.createObjectNode();
                u.put("package", p);
                u.put("variable", v);
                u.put("procedureId", nodeId != null ? nodeId : "");
                u.put("procedureName", nodeName != null ? nodeName : "");
                u.put("accessType", textOrNull(ref, "accessType") != null ? textOrNull(ref, "accessType") : "UNKNOWN");
                u.put("lineNumber", ref.path("line").asInt(0));
                u.put("sourceFile", sourceFile != null ? sourceFile : "");
                out.add(u);
            }
        }
        return out;
    }
}
