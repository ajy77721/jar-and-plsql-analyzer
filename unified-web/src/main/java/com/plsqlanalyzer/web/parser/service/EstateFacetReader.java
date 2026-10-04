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
 *
 * <h2>Detail enrichment and cross-estate links</h2>
 *
 * <p>Beyond the base row, each detail payload carries the estate's own cross-references, all read
 * from artifacts the crawl already wrote (never a re-parse):
 *
 * <ul>
 *   <li><b>Views</b> — the joins inside the definition ({@code viewJoins}), the view's own indexes
 *       and its base tables' indexes ({@code dbIndexes}), constraints ({@code dbConstraints}),
 *       used-by rows classified by the referencing node's object type, the reverse view-on-view and
 *       trigger→view edges ({@code usedByViews}/{@code usedByTriggers}), the trigger-bound base
 *       tables in the chain ({@code triggerBoundTables}), and a capped sample of dynamic SQL that
 *       names the view.</li>
 *   <li><b>Triggers</b> — catalog-only {@code dbTriggers[]} rows are merged in (capturing SQL Server
 *       INSTEAD OF and Oracle COMPOUND triggers the source parser never saw, with
 *       {@code insteadOf}/{@code compound}/{@code dbDiscovered} flags and an honest empty body),
 *       and the {@code willFire} inverse map lists the procedures whose DML on the trigger's table
 *       matches the trigger's event, with the matching operation and line.</li>
 *   <li><b>Globals</b> — per-variable read/write counts, and a per-procedure footprint
 *       ({@code byProcedure}) of which variables each procedure reads and writes.</li>
 * </ul>
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

    /**
     * The dedicated parse-time package-global declaration artifact
     * ({@code api/globals/declarations.json}: {@code {declarations:{PACKAGE:[{name,dataType,
     * defaultValue,constant}]}}}), or {@code null} when the analysis has none. A crawler output,
     * not a node scan — read directly, like {@code getTables}, and absent for older analyses.
     */
    private JsonNode globalsDeclarations(String name) {
        try {
            ArtifactKey key = r.resolveKey(name, "api/globals/declarations.json");
            if (!r.store().exists(key)) return null;
            return readTree(key);
        } catch (Exception e) {
            return null;
        }
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

    /**
     * Whether a tables-index entry is a <em>defined</em> view rather than a bare name reference.
     *
     * <p>A view is only real when the crawl has something to define it with: the defining SQL
     * ({@code viewDefinition}, from the source parse or the catalog) or the catalog's own
     * confirmation ({@code dbFound=true}, which also covers a definition-less catalog view such as
     * an Oracle MV whose query text was not fetched). An entry that is neither — the common
     * cross-schema case, where a procedure references a view the enrichment never resolved — is a
     * name-only reference and must not be reported as a view of this estate.
     */
    private static boolean hasViewIdentity(JsonNode t) {
        String def = textOrNull(t, "viewDefinition");
        if (def != null && !def.isBlank()) return true;
        return t.path("dbFound").asBoolean(false);
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
            // nodeId -> objectType over the analysis index, so a usedBy row can be classified as a
            // procedure / function / view / trigger rather than shown as an opaque node id. Computed
            // once per getViews() call (which is itself memoized) so it is invalidated with the rest.
            Map<String, String> nodeTypes = nodeTypeMap(name);
            for (JsonNode t : tables.path("tables")) {
                if (!isView(textOrNull(t, "objectType"))) continue;
                // A view row must be a DEFINED view. A name-only reference — a proc that reads a
                // view the crawl never resolved (e.g. a cross-schema DB2 view outside the enriched
                // schema, so dbFound=false and no definition) — is not a view the estate defines,
                // and must not appear as one. It still surfaces through the referencing node's
                // used-by/table list; only the phantom VIEW row is suppressed. Identity is
                // "has a defining SQL text" OR "the catalog confirmed it (dbFound=true)".
                if (!hasViewIdentity(t)) continue;
                out.add(viewRow(m, t, nodeTypes, tables));
            }
        }
        r.synthesizedCache.put(ck, out);
        return out;
    }

    private ObjectNode viewRow(ObjectMapper m, JsonNode t, Map<String, String> nodeTypes, JsonNode tables) {
        ObjectNode row = m.createObjectNode();
        String vname = textOrNull(t, "name");
        String schema = textOrNull(t, "schema");
        String ot = textOrNull(t, "objectType");
        row.put("name", vname != null ? vname : "");
        row.put("schema", schema != null ? schema : "");
        row.put("objectType", ot != null ? ot : "VIEW");
        row.put("isMaterialized", "MATERIALIZED_VIEW".equalsIgnoreCase(ot));

        ArrayNode columns = row.putArray("columns");
        int ordinal = 0;
        for (JsonNode c : t.path("dbColumns")) {
            ObjectNode col = m.createObjectNode();
            col.put("name", textOrNull(c, "columnName") != null ? textOrNull(c, "columnName") : "");
            col.put("dataType", textOrNull(c, "dataType") != null ? textOrNull(c, "dataType") : "");
            col.put("nullable", c.path("nullable").asBoolean(true));
            // Additive: the 1-based position and the catalog's own declared length/column id, when
            // the crawl captured them.
            col.put("ordinal", ++ordinal);
            if (c.hasNonNull("columnId")) col.put("columnId", c.path("columnId").asInt());
            if (c.hasNonNull("dataLength")) col.put("dataLength", c.path("dataLength").asInt());
            columns.add(col);
        }
        row.put("columnCount", columns.size());
        row.put("definition", textOrNull(t, "viewDefinition") != null ? textOrNull(t, "viewDefinition") : "");

        // Joins INSIDE the view definition — the catalog/grammar's own join edges. Present only when
        // the visitor captured them (MySQL/DB2/Oracle views); absent means "not captured", not "no
        // joins", so the list is emitted as an (empty) array and the UI says so honestly.
        ArrayNode joins = row.putArray("joins");
        for (JsonNode j : t.path("viewJoins")) {
            ObjectNode jo = m.createObjectNode();
            jo.put("joinType", textOrNull(j, "joinType") != null ? textOrNull(j, "joinType") : "JOIN");
            jo.put("leftTable", textOrNull(j, "leftTable") != null ? textOrNull(j, "leftTable") : "");
            jo.put("leftTableAlias", textOrNull(j, "leftTableAlias") != null ? textOrNull(j, "leftTableAlias") : "");
            jo.put("joinedTable", textOrNull(j, "joinedTable") != null ? textOrNull(j, "joinedTable") : "");
            jo.put("joinedTableAlias", textOrNull(j, "joinedTableAlias") != null ? textOrNull(j, "joinedTableAlias") : "");
            jo.put("condition", textOrNull(j, "condition") != null ? textOrNull(j, "condition") : "");
            jo.put("line", j.path("line").asInt(0));
            joins.add(jo);
        }
        row.put("joinCount", joins.size());

        // Constraints on the view's own columns, straight off the entry's dbConstraints.
        ArrayNode constraints = row.putArray("constraints");
        for (JsonNode c : t.path("dbConstraints")) {
            String cn = textOrNull(c, "constraintName");
            String ct = textOrNull(c, "constraintType");
            String cc = textOrNull(c, "searchCondition");
            if (cn == null && ct == null && cc == null) continue;
            ObjectNode co = m.createObjectNode();
            co.put("name", cn != null ? cn : "");
            co.put("type", ct != null ? ct : "");
            co.put("condition", cc != null ? cc : "");
            constraints.add(co);
        }
        row.put("constraintCount", constraints.size());

        // Indexes on the view itself (an MV's snapshot index), grouped one row per index.
        ArrayNode indexes = groupIndexes(m, t.path("dbIndexes"));
        row.set("indexes", indexes);
        row.put("indexCount", indexes.size());

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
        for (JsonNode u : t.path("usedBy")) usedBy.add(usedByRow(m, u, nodeTypes, tables));
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

    /** As {@link #usedByRow(ObjectMapper, JsonNode)} but also stamps the referencing node's
     *  object type, so a used-by list can distinguish a procedure from another view or a trigger. */
    private static ObjectNode usedByRow(ObjectMapper m, JsonNode u, Map<String, String> nodeTypes, JsonNode tables) {
        ObjectNode o = usedByRow(m, u);
        o.put("nodeType", classifyNode(textOrNull(u, "nodeId"), textOrNull(u, "nodeName"), nodeTypes, tables));
        return o;
    }

    /**
     * The object type of a referencing node: the analysis index's own {@code objectType} for the
     * node id, else the tables index's type for the node's name, else {@code UNKNOWN}. Never guessed
     * to a specific type — an unrecognised reference stays {@code UNKNOWN}.
     */
    private static String classifyNode(String nodeId, String nodeName,
                                       Map<String, String> nodeTypes, JsonNode tables) {
        String ot = nodeId == null ? null : nodeTypes.get(nodeId);
        if (ot == null && nodeName != null && tables != null) {
            JsonNode te = entryFor(tables, nodeName);
            if (te != null) ot = textOrNull(te, "objectType");
        }
        return ot != null && !ot.isBlank() ? ot.toUpperCase(Locale.ROOT) : "UNKNOWN";
    }

    /** {@code nodeId -> objectType} over the analysis index. Empty (never null) when absent. */
    private Map<String, String> nodeTypeMap(String name) {
        Map<String, String> out = new HashMap<>();
        try {
            JsonNode idx = r.getIndex(name);
            for (JsonNode n : idx.path("nodes")) {
                String id = textOrNull(n, "nodeId");
                String ot = textOrNull(n, "objectType");
                if (id != null && ot != null) out.put(id, ot.toUpperCase(Locale.ROOT));
            }
        } catch (Exception e) {
            // A missing/unreadable index means every reference classifies as UNKNOWN — honest.
        }
        return out;
    }

    /**
     * Groups a catalog {@code dbIndexes[]} list (one row per index column) into one row per index:
     * {@code {name, unique, columns[]}}. Order-preserving; never null.
     */
    private static ArrayNode groupIndexes(ObjectMapper m, JsonNode dbIndexes) {
        ArrayNode out = m.createArrayNode();
        if (dbIndexes == null || !dbIndexes.isArray()) return out;
        Map<String, ObjectNode> byName = new LinkedHashMap<>();
        for (JsonNode ix : dbIndexes) {
            String nm = textOrNull(ix, "indexName");
            if (nm == null) nm = textOrNull(ix, "name");
            if (nm == null || nm.isBlank()) continue;
            final String fname = nm;
            ObjectNode o = byName.computeIfAbsent(nm.toUpperCase(Locale.ROOT), k -> {
                ObjectNode n = m.createObjectNode();
                n.put("name", fname);
                n.put("unique", "UNIQUE".equalsIgnoreCase(textOrNull(ix, "uniqueness")));
                n.putArray("columns");
                return n;
            });
            String col = textOrNull(ix, "columnName");
            if (col != null && !col.isBlank()) ((ArrayNode) o.get("columns")).add(col);
        }
        byName.values().forEach(out::add);
        return out;
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
        JsonNode tables = tablesIndex(name);
        // Indexes on the view's BASE tables (captured by the crawl's dbIndexes enrichment), so the
        // detail answers "what is indexed under this view" without opening each base table.
        out.set("baseTableIndexes", baseTableIndexes(r.mapper, row, tables));
        // The reverse estate edges: other views that read this view, and triggers bound to tables
        // whose body touches this view. Both are derived from the same artifacts the chain and the
        // triggers facet already use — no re-parse.
        out.set("usedByViews", usedByViews(name, row));
        out.set("usedByTriggers", usedByTriggers(name, row));
        // Cross-estate: base tables in the chain that carry triggers, with a clickable trigger list.
        out.set("triggerBoundTables", triggerBoundTables(name, row, tables));
        // Dynamic SQL statements that name this view, sampled (capped) — the "is this view reached
        // through EXECUTE IMMEDIATE / DBMS_SQL" question, answered from the dynamic-SQL facet.
        out.set("dynamicSqlRefs", dynamicSqlRefs(name, row));
        return out;
    }

    /** One flat row per index on a base table: {@code {table, tableSchema, name, unique, columns[]}}. */
    private ArrayNode baseTableIndexes(ObjectMapper m, ObjectNode row, JsonNode tables) {
        ArrayNode out = m.createArrayNode();
        if (tables == null) return out;
        for (JsonNode b : row.path("baseTables")) {
            String bn = textOrNull(b, "name");
            if (bn == null) continue;
            JsonNode te = entryFor(tables, bn);
            if (te == null) continue;
            for (JsonNode ix : groupIndexes(m, te.path("dbIndexes"))) {
                ObjectNode o = (ObjectNode) ix;
                o.put("table", textOrNull(te, "name") != null ? textOrNull(te, "name") : bn);
                o.put("tableSchema", textOrNull(te, "schema") != null ? textOrNull(te, "schema") : "");
                out.add(o);
            }
        }
        return out;
    }

    /** Other views that read this view (the inverse of the view-on-view chain). */
    private ArrayNode usedByViews(String name, ObjectNode row) throws IOException {
        ArrayNode out = r.mapper.createArrayNode();
        String self = norm(row.path("name").asText(""));
        if (self.isEmpty()) return out;
        for (JsonNode v : getViews(name)) {
            if (norm(v.path("name").asText("")).equals(self)) continue;
            for (JsonNode b : v.path("baseTables")) {
                if (!norm(textOrNull(b, "name")).equals(self)) continue;
                ObjectNode o = r.mapper.createObjectNode();
                o.put("name", v.path("name").asText(""));
                o.put("schema", v.path("schema").asText(""));
                o.put("objectType", v.path("objectType").asText("VIEW"));
                o.put("operation", textOrNull(b, "operation") != null ? textOrNull(b, "operation") : "SELECT");
                out.add(o);
                break;
            }
        }
        return out;
    }

    /** Triggers whose body touches this view — the trigger→view edge, from the triggers facet. */
    private ArrayNode usedByTriggers(String name, ObjectNode row) throws IOException {
        ArrayNode out = r.mapper.createArrayNode();
        String self = norm(row.path("name").asText(""));
        if (self.isEmpty()) return out;
        for (JsonNode t : getTriggers(name)) {
            for (JsonNode touch : t.path("touches")) {
                if (!norm(textOrNull(touch, "name")).equals(self)) continue;
                ObjectNode o = r.mapper.createObjectNode();
                o.put("name", t.path("name").asText(""));
                o.put("schema", t.path("schema").asText(""));
                o.put("table", t.path("table").asText(""));
                o.put("timing", t.path("timing").asText(""));
                o.put("event", t.path("event").asText(""));
                o.put("operation", textOrNull(touch, "operation") != null ? textOrNull(touch, "operation") : "SELECT");
                out.add(o);
                break;
            }
        }
        return out;
    }

    /**
     * The base tables this view depends on that carry at least one trigger, each with a summary of
     * those triggers. This is the "the chain reaches a trigger-bound table" cross-link: it lets the
     * view detail point at the trigger that fires when the base table is written.
     */
    private ArrayNode triggerBoundTables(String name, ObjectNode row, JsonNode tables) throws IOException {
        ArrayNode out = r.mapper.createArrayNode();
        if (tables == null) return out;
        Set<String> names = new LinkedHashSet<>();
        for (JsonNode b : row.path("baseTables")) {
            String bn = textOrNull(b, "name");
            if (bn != null && !bn.isBlank()) names.add(norm(bn));
        }
        for (JsonNode b : viewChain(name, row.path("name").asText("")).path("baseTables")) {
            if (b.isTextual() && !b.asText().isBlank()) names.add(norm(b.asText()));
        }
        for (String bn : names) {
            JsonNode te = entryFor(tables, bn);
            if (te == null) continue;
            ArrayNode trs = triggerSummaries(r.mapper, te);
            if (trs.size() == 0) continue;
            ObjectNode o = r.mapper.createObjectNode();
            o.put("table", textOrNull(te, "name") != null ? textOrNull(te, "name") : bn);
            o.put("schema", textOrNull(te, "schema") != null ? textOrNull(te, "schema") : "");
            o.set("triggers", trs);
            out.add(o);
        }
        return out;
    }

    /** {@code {name, timing, event, triggerType, status}} for each trigger attached to a table entry
     *  (source-parsed {@code triggers[]} plus catalog {@code dbTriggers[]}), de-duplicated by name. */
    private static ArrayNode triggerSummaries(ObjectMapper m, JsonNode tableEntry) {
        ArrayNode out = m.createArrayNode();
        Set<String> seen = new HashSet<>();
        for (JsonNode trg : tableEntry.path("triggers")) {
            String tn = textOrNull(trg, "name");
            if (tn == null || !seen.add(tn.toUpperCase(Locale.ROOT))) continue;
            out.add(triggerSummary(m, tn, textOrNull(trg, "timing"), textOrNull(trg, "event"),
                    textOrNull(trg, "triggerType"), textOrNull(trg, "status")));
        }
        for (JsonNode trg : tableEntry.path("dbTriggers")) {
            String tn = textOrNull(trg, "triggerName");
            if (tn == null || !seen.add(tn.toUpperCase(Locale.ROOT))) continue;
            out.add(triggerSummary(m, tn, timingOf(textOrNull(trg, "triggerType")),
                    textOrNull(trg, "triggeringEvent"), textOrNull(trg, "triggerType"),
                    textOrNull(trg, "status")));
        }
        return out;
    }

    private static ObjectNode triggerSummary(ObjectMapper m, String name, String timing, String event,
                                             String triggerType, String status) {
        ObjectNode o = m.createObjectNode();
        o.put("name", name != null ? name : "");
        o.put("timing", timing != null ? timing : "");
        o.put("event", event != null ? event : "");
        o.put("triggerType", triggerType != null ? triggerType : "");
        o.put("status", status != null ? status : "");
        return o;
    }

    /** A capped sample of dynamic-SQL statements whose expression names this view. */
    private ArrayNode dynamicSqlRefs(String name, ObjectNode row) {
        ArrayNode out = r.mapper.createArrayNode();
        String vn = row.path("name").asText("");
        if (vn.isBlank()) return out;
        String needle = vn.toUpperCase(Locale.ROOT);
        try {
            for (JsonNode d : r.getDynamicSql(name)) {
                String expr = d.path("sqlExpression").asText("");
                if (expr.isEmpty() || !expr.toUpperCase(Locale.ROOT).contains(needle)) continue;
                ObjectNode o = r.mapper.createObjectNode();
                o.put("procedureId", d.path("procedureId").asText(""));
                o.put("procedureName", d.path("procedureName").asText(""));
                o.put("lineNumber", d.path("lineNumber").asInt(0));
                o.put("type", d.path("type").asText(""));
                o.put("sqlExpression", expr);
                out.add(o);
                if (out.size() >= 20) break;
            }
        } catch (Exception e) {
            // No dynamic-SQL facet → an empty (honest) list.
        }
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
            // The trigger body's own callees, when the crawl captured them on the node detail —
            // the cross-estate edge from a trigger body into the procedures (and their globals) it
            // calls. Absent means "not captured", so the field is simply not set.
            if (n.path("calls").isArray() && n.path("calls").size() > 0) {
                row.set("bodyCalls", n.path("calls").deepCopy());
            }
        }

        // 2b. TRIGGER nodes in the index that have no node-detail file (older/partial artifacts).
        //     The procedures facet no longer lists these nodes, so the triggers facet must own them
        //     even without a detail file — identity, table/event/timing come off the index row.
        for (JsonNode n : r.getIndex(name).path("nodes")) {
            if (!"TRIGGER".equalsIgnoreCase(textOrNull(n, "objectType"))) continue;
            String tn0 = textOrNull(n, "objectName");
            if (tn0 == null) tn0 = textOrNull(n, "name");
            if (tn0 == null) continue;
            final String tn = tn0;
            String tschema = textOrNull(n, "schema");
            String key = key(tschema, tn);
            ObjectNode row = byKey.computeIfAbsent(key, k -> newTriggerRow(m, tn, tschema));
            if (row.path("nodeId").asText("").isEmpty()) {
                String id = textOrNull(n, "nodeId");
                if (id != null) row.put("nodeId", id);
            }
            if (row.path("table").asText("").isEmpty()) {
                String tt = textOrNull(n, "triggerTable");
                if (tt != null) row.put("table", tt);
            }
            putIfAbsent(row, "timing", textOrNull(n, "triggerTiming"));
            putIfAbsent(row, "event", textOrNull(n, "triggerEvent"));
        }

        // 3. Catalog-discovered triggers (dbTriggers[]) — the DB-discovery enrichment attaches these
        //    to the table entry. They carry triggerType / triggeringEvent / status and are the ONLY
        //    source for triggers the source parser never saw: SQL Server INSTEAD OF triggers (often
        //    on a view), Oracle COMPOUND triggers, and DB2's catalog rows. No body is available on
        //    this path — the trigger body stays the honest empty string.
        if (tables != null) {
            for (JsonNode t : tables.path("tables")) {
                String tableName = textOrNull(t, "name");
                String tableSchema = textOrNull(t, "schema");
                for (JsonNode dt : t.path("dbTriggers")) {
                    String tn = textOrNull(dt, "triggerName");
                    if (tn == null || tn.isBlank()) continue;
                    String owner = textOrNull(dt, "owner");
                    String k = key(owner != null && !owner.isBlank() ? owner : tableSchema, tn);
                    ObjectNode row = byKey.get(k);
                    boolean created = false;
                    if (row == null) { row = newTriggerRow(m, tn, owner); byKey.put(k, row); created = true; }
                    if (row.path("table").asText("").isEmpty() && tableName != null) row.put("table", tableName);
                    if (row.path("tableSchema").asText("").isEmpty() && tableSchema != null) row.put("tableSchema", tableSchema);
                    putIfAbsent(row, "triggerType", textOrNull(dt, "triggerType"));
                    putIfAbsent(row, "event", textOrNull(dt, "triggeringEvent"));
                    putIfAbsent(row, "timing", timingOf(textOrNull(dt, "triggerType")));
                    putIfAbsent(row, "status", textOrNull(dt, "status"));
                    putIfAbsent(row, "source", "DATABASE");
                    // Only a row this catalog list ALONE produced is "catalog only"; a row already
                    // seen in triggers[] or a TRIGGER node keeps its richer provenance.
                    if (created) row.put("dbDiscovered", true);
                }
            }
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
            // SQL Server's transition pseudo-tables (INSERTED / DELETED) are the trigger's
            // in-memory delta tables, never real base tables. A body scanned at the source (a
            // DB-discovered trigger whose body the dialect visitor did not parse) can list them as
            // SELECT sources; they are filtered at the source too (TriggerDiscoverer's SKIP_TABLES
            // and the SQL Server visitor's insideTriggerBody guard), but a stale artifact from an
            // earlier crawl would still carry them, so the reader drops them as well.
            if ("INSERTED".equals(normTn) || "DELETED".equals(normTn)) continue;
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
            // Both levels are spelled out in the trigger body's DDL. STATEMENT is checked first:
            // "FOR EACH STATEMENT" does not contain "FOR EACH ROW", but checking it first keeps
            // the two mutually exclusive and self-documenting. DB2's statement-level triggers
            // (and SQL Server's table-level ones) carry only "FOR EACH STATEMENT" here, so
            // scanning for ROW alone left their level empty.
            if (b.contains("FOR EACH STATEMENT")) return "STATEMENT";
            if (b.contains("FOR EACH ROW")) return "ROW";
        }
        return "";
    }

    /** The timing part of a catalog trigger type: BEFORE / AFTER / INSTEAD OF, else empty. */
    private static String timingOf(String triggerType) {
        if (triggerType == null) return "";
        String t = triggerType.toUpperCase(Locale.ROOT);
        if (t.contains("INSTEAD OF")) return "INSTEAD OF";
        if (t.contains("BEFORE")) return "BEFORE";
        if (t.contains("AFTER")) return "AFTER";
        return "";
    }

    /** Splits an event string ({@code "INSERT,UPDATE"}, {@code "INSERT OR UPDATE"}) into its parts. */
    private ArrayNode splitEvents(String event) {
        ArrayNode out = r.mapper.createArrayNode();
        if (event == null || event.isBlank()) return out;
        for (String part : event.toUpperCase(Locale.ROOT).split("\\s*(?:,|\\bOR\\b)\\s*")) {
            String p = part.trim();
            if (!p.isEmpty()) out.add(p);
        }
        return out;
    }

    private static Set<String> eventSet(String event) {
        Set<String> out = new LinkedHashSet<>();
        if (event == null || event.isBlank()) return out;
        for (String part : event.toUpperCase(Locale.ROOT).split("\\s*(?:,|\\bOR\\b)\\s*")) {
            String p = part.trim();
            if (!p.isEmpty()) out.add(p);
        }
        return out;
    }

    private static boolean isDml(String op) {
        return "INSERT".equals(op) || "UPDATE".equals(op) || "DELETE".equals(op)
                || "MERGE".equals(op) || "TRUNCATE".equals(op);
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
        // Per-dialect honesty flags derived from what was persisted (never guessed): an INSTEAD OF
        // trigger (SQL Server / Oracle / Postgres) and an Oracle COMPOUND trigger are surfaced as
        // their own flags so the UI can label them rather than showing a blank timing.
        String tt = row.path("triggerType").asText("").toUpperCase(Locale.ROOT);
        row.put("insteadOf", tt.contains("INSTEAD OF")
                || "INSTEAD OF".equalsIgnoreCase(row.path("timing").asText("")));
        row.put("compound", tt.contains("COMPOUND"));
        row.put("eventList", splitEvents(row.path("event").asText("")));
        // True when this row exists only because of the catalog's dbTriggers[] list (no body, no
        // node). Set on the merge path; defaulted here so every row carries the field.
        row.put("dbDiscovered", row.path("dbDiscovered").asBoolean(false));
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
        ArrayNode fired = firedBy(name, row.path("table").asText(""), row.path("event").asText(""));
        out.set("firedBy", fired);
        // The inverse map, event-matched: of the procedures that write the trigger's table, the ones
        // whose operation actually matches the trigger's event (INSERT trigger → procedures that
        // INSERT). An insert into T fires trigger X, so every procedure that inserts into T is listed
        // here with the matching operation and line.
        ArrayNode willFire = willFire(fired);
        out.set("willFire", willFire);
        out.put("firedByCount", fired.size());
        out.put("willFireCount", willFire.size());
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
        ArrayNode fired = firedBy(name, row.path("table").asText(""), row.path("event").asText(""));
        out.set("firedBy", fired);
        ArrayNode willFire = willFire(fired);
        out.set("willFire", willFire);
        out.put("count", fired.size());
        out.put("willFireCount", willFire.size());
        return out;
    }

    /**
     * The procedures whose DML on the trigger's table can fire it — the table's own {@code usedBy}
     * filtered to DML operations, each stamped with the operations that match the trigger's event
     * ({@code matchingOperations}) and whether any does ({@code firesOnEvent}). When the trigger's
     * event is unknown every DML entry counts as a match, so the list degrades honestly rather than
     * emptying.
     */
    private ArrayNode firedBy(String name, String table, String event) throws IOException {
        ArrayNode out = r.mapper.createArrayNode();
        if (table == null || table.isBlank()) return out;
        JsonNode tables = tablesIndex(name);
        if (tables == null) return out;
        JsonNode entry = entryFor(tables, table);
        if (entry == null) return out;
        Set<String> events = eventSet(event);
        for (JsonNode u : entry.path("usedBy")) {
            JsonNode ops = u.path("operations");
            ObjectNode matching = r.mapper.createObjectNode();
            boolean anyDml = false;
            boolean anyMatch = false;
            if (ops.isObject()) {
                Iterator<Map.Entry<String, JsonNode>> it = ops.fields();
                while (it.hasNext()) {
                    Map.Entry<String, JsonNode> e = it.next();
                    String op = e.getKey().toUpperCase(Locale.ROOT);
                    if (!isDml(op)) continue;
                    anyDml = true;
                    if (events.isEmpty() || events.contains(op)) {
                        matching.set(op, e.getValue().deepCopy());
                        anyMatch = true;
                    }
                }
            } else {
                // No operations map to test against — keep the entry (the pre-existing behaviour)
                // and treat it as an unknown match rather than dropping a real writer.
                anyDml = true;
                anyMatch = true;
            }
            if (!anyDml) continue;
            ObjectNode row = usedByRow(r.mapper, u);
            row.set("matchingOperations", matching);
            row.put("firesOnEvent", anyMatch);
            out.add(row);
        }
        return out;
    }

    /** The subset of {@code firedBy} whose operation matches the trigger's event. */
    private static ArrayNode willFire(ArrayNode firedBy) {
        ArrayNode out = firedBy.arrayNode();
        for (JsonNode u : firedBy) {
            if (u.path("firesOnEvent").asBoolean(false)) out.add(u.deepCopy());
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
            ArrayNode fired = firedBy(name, row.path("table").asText(""), row.path("event").asText(""));
            entry.set("firedBy", fired);
            ArrayNode willFire = willFire(fired);
            entry.set("willFire", willFire);
            entry.put("count", fired.size());
            entry.put("willFireCount", willFire.size());
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

        // Declaration lists persisted per package — the declaring package plus
        // name/dataType/defaultValue/constant, the exact shape each variable row uses. Keyed
        // package → variable → declaration so a ref can pick up its own declaration, and a
        // declared-but-unreferenced variable still appears. Two sources are merged:
        //   1. api/globals/declarations.json — the parse-time capture, written INDEPENDENT of
        //      whether a PACKAGE_BODY/PACKAGE_SPEC node was crawled. This is the one that makes a
        //      member-entry-point analysis (no package node at all) report real globals.
        //   2. per-node packageGlobalDeclarations — older analyses, package-as-entry-point
        //      aggregation, and DB2 module variables.
        Map<String, Map<String, JsonNode>> declarations = new LinkedHashMap<>();
        JsonNode declDoc = globalsDeclarations(name);
        if (declDoc != null) {
            JsonNode byPkg = declDoc.path("declarations");
            if (byPkg.isObject()) {
                Iterator<Map.Entry<String, JsonNode>> it = byPkg.fields();
                while (it.hasNext()) {
                    Map.Entry<String, JsonNode> e = it.next();
                    if (!e.getValue().isArray()) continue;
                    Map<String, JsonNode> byVar = declarations.computeIfAbsent(
                            norm(e.getKey()), k -> new LinkedHashMap<>());
                    for (JsonNode d : e.getValue()) {
                        String vn = textOrNull(d, "name");
                        if (vn != null && !vn.isBlank()) byVar.putIfAbsent(norm(vn), d);
                    }
                }
            }
        }
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

        // Estate-level guard behind the visitor's grammar-first gate: a record variable, a cursor
        // FOR-loop record and a table alias all share the X.Y shape with a genuine package-global
        // reference, so a ref is only admitted when its qualifier is a real package in this estate
        // (a PACKAGE_BODY/PACKAGE_SPEC node or a package with declarations), carries the
        // conventional package-name token, or its variable is a declared global somewhere. Artifacts
        // written before the visitor gate therefore cannot leak the record/alias noise either.
        // Built AFTER the merge so the dedicated declarations artifact also vouches for its packages.
        PackageGlobalScope scope = packageGlobalScope(nodeFiles);
        for (Map.Entry<String, Map<String, JsonNode>> e : declarations.entrySet()) {
            scope.addPackage(e.getKey());
            for (String v : e.getValue().keySet()) scope.addVariable(v);
        }

        Map<String, ObjectNode> byPackage = new LinkedHashMap<>();
        // Per-package, per-variable read/write tallies, so each variable row can carry its own
        // counts (the package totals alone cannot say which variable is hot).
        Map<String, Map<String, int[]>> varCounts = new LinkedHashMap<>();
        for (JsonNode node : nodeFiles) {
            JsonNode refs = node.get("externalPackageVarRefDetails");
            if (refs == null || !refs.isArray()) continue;
            for (JsonNode ref : refs) {
                String pkg = textOrNull(ref, "packageName");
                String var = textOrNull(ref, "variableName");
                if (pkg == null || pkg.isBlank() || var == null || var.isBlank()) continue;
                String pkey = norm(pkg);
                if (!scope.admits(pkey, norm(var))) continue;
                ObjectNode row = byPackage.computeIfAbsent(pkey, k -> newPackageRow(m, k));
                addVariable(row, norm(var), declarationFor(declarations, pkey, var));
                row.put("usageCount", row.get("usageCount").asInt() + 1);
                int[] counts = varCounts.computeIfAbsent(pkey, k -> new LinkedHashMap<>())
                        .computeIfAbsent(norm(var), k -> new int[2]);
                String access = textOrNull(ref, "accessType");
                if ("WRITE".equalsIgnoreCase(access)) {
                    row.put("writeCount", row.get("writeCount").asInt() + 1);
                    counts[1]++;
                } else if ("READ".equalsIgnoreCase(access)) {
                    row.put("readCount", row.get("readCount").asInt() + 1);
                    counts[0]++;
                }
            }
        }

        // Declared-but-unreferenced packages/variables still surface (declared:true), appended after
        // the referenced ones so the common case keeps its existing order.
        for (Map.Entry<String, Map<String, JsonNode>> e : declarations.entrySet()) {
            ObjectNode row = byPackage.computeIfAbsent(e.getKey(), k -> newPackageRow(m, k));
            for (JsonNode d : e.getValue().values()) addVariable(row, norm(textOrNull(d, "name")), d);
        }

        ArrayNode out = m.createArrayNode();
        for (Map.Entry<String, ObjectNode> e : byPackage.entrySet()) {
            ObjectNode row = e.getValue();
            Map<String, int[]> counts = varCounts.get(e.getKey());
            for (JsonNode v : row.path("variables")) {
                int[] c = counts == null ? null : counts.get(norm(v.path("name").asText("")));
                ((ObjectNode) v).put("readCount", c == null ? 0 : c[0]);
                ((ObjectNode) v).put("writeCount", c == null ? 0 : c[1]);
            }
            row.put("variableCount", row.path("variables").size());
        }
        byPackage.values().forEach(out::add);
        r.synthesizedCache.put(ck, out);
        return out;
    }

    /**
     * The estate's package-global scope: which package names are real packages (a
     * {@code PACKAGE_BODY}/{@code PACKAGE_SPEC} node, or a node carrying persisted global
     * declarations) and which variable names are declared globals anywhere. A ref whose qualifier
     * is neither a known package nor the conventional {@code PKG} name pattern, and whose variable
     * is not a declared global, is a record-variable / FOR-loop-record / table-alias row and is
     * rejected — the estate-level half of the globals-facet noise fix.
     */
    private static final class PackageGlobalScope {
        private final Set<String> packages = new HashSet<>();
        private final Set<String> variables = new HashSet<>();

        void addPackage(String pkg) {
            if (pkg != null && !pkg.isBlank()) packages.add(pkg);
        }

        void addVariable(String var) {
            if (var != null && !var.isBlank()) variables.add(var);
        }

        boolean admits(String pkg, String var) {
            if (pkg == null || pkg.isEmpty() || var == null || var.isEmpty()) return false;
            if (packages.contains(pkg)) return true;
            // The conventional package-name token, so a reference-only global from an analysis that
            // predates declaration persistence (no package node crawled) still surfaces.
            if (pkg.contains("PKG")) return true;
            // A declared global of any package: covers a cross-package ref whose own package node
            // was not part of this analysis.
            return variables.contains(var);
        }
    }

    private static PackageGlobalScope packageGlobalScope(List<JsonNode> nodeFiles) {
        PackageGlobalScope scope = new PackageGlobalScope();
        for (JsonNode node : nodeFiles) {
            JsonNode decls = node.get("packageGlobalDeclarations");
            boolean hasDecls = decls != null && decls.isArray() && !decls.isEmpty();
            String ot = textOrNull(node, "objectType");
            boolean pkgNode = ot != null
                    && (ot.equalsIgnoreCase("PACKAGE_BODY") || ot.equalsIgnoreCase("PACKAGE_SPEC"));
            if (pkgNode || hasDecls) {
                String pkg = textOrNull(node, "packageName");
                if (pkg == null || pkg.isBlank()) pkg = textOrNull(node, "objectName");
                if (pkg == null || pkg.isBlank()) pkg = textOrNull(node, "name");
                if (pkg != null && !pkg.isBlank()) scope.packages.add(norm(pkg));
            }
            if (hasDecls) {
                for (JsonNode d : decls) {
                    String vn = textOrNull(d, "name");
                    if (vn != null && !vn.isBlank()) scope.variables.add(norm(vn));
                }
            }
        }
        return scope;
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
        // The reverse view: one entry per procedure that reads/writes this package, with the
        // variables and lines it touches — the per-procedure globals footprint for this package.
        ArrayNode byProc = globalsByProcedure(name, norm(pkg));
        out.set("byProcedure", byProc);
        out.put("procedureCount", byProc.size());
        return out;
    }

    /**
     * Each procedure's footprint on one package's globals: {@code {procedureId, procedureName,
     * sourceFile, reads[], writes[], readCount, writeCount}}, one entry per node that references the
     * package. Read straight from the node details' {@code externalPackageVarRefDetails}.
     */
    private ArrayNode globalsByProcedure(String name, String pkg) throws IOException {
        ArrayNode out = r.mapper.createArrayNode();
        if (pkg == null || pkg.isEmpty()) return out;
        List<JsonNode> nodeFiles = readAll(r.collectNodeFiles(name));
        PackageGlobalScope scope = packageGlobalScope(nodeFiles);
        for (JsonNode node : nodeFiles) {
            JsonNode refs = node.get("externalPackageVarRefDetails");
            if (refs == null || !refs.isArray()) continue;
            String nodeId = textOrNull(node, "nodeId");
            String nodeName = textOrNull(node, "name");
            String sourceFile = textOrNull(node, "sourceFile");
            ObjectNode p = null;
            for (JsonNode ref : refs) {
                String refPkg = norm(textOrNull(ref, "packageName"));
                if (!refPkg.equals(pkg)) continue;
                if (!scope.admits(refPkg, norm(textOrNull(ref, "variableName")))) continue;
                if (p == null) {
                    p = r.mapper.createObjectNode();
                    p.put("procedureId", nodeId != null ? nodeId : "");
                    p.put("procedureName", nodeName != null ? nodeName : "");
                    p.put("sourceFile", sourceFile != null ? sourceFile : "");
                    p.putArray("reads");
                    p.putArray("writes");
                    p.put("readCount", 0);
                    p.put("writeCount", 0);
                }
                ObjectNode u = r.mapper.createObjectNode();
                u.put("variable", norm(textOrNull(ref, "variableName")));
                u.put("lineNumber", ref.path("line").asInt(0));
                if ("WRITE".equalsIgnoreCase(textOrNull(ref, "accessType"))) {
                    ((ArrayNode) p.get("writes")).add(u);
                    p.put("writeCount", p.path("writeCount").asInt() + 1);
                } else {
                    ((ArrayNode) p.get("reads")).add(u);
                    p.put("readCount", p.path("readCount").asInt() + 1);
                }
            }
            if (p != null) out.add(p);
        }
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
        List<JsonNode> nodeFiles = readAll(r.collectNodeFiles(name));
        PackageGlobalScope scope = packageGlobalScope(nodeFiles);
        for (JsonNode node : nodeFiles) {
            JsonNode refs = node.get("externalPackageVarRefDetails");
            if (refs == null || !refs.isArray()) continue;
            String nodeId = textOrNull(node, "nodeId");
            String nodeName = textOrNull(node, "name");
            String sourceFile = textOrNull(node, "sourceFile");
            for (JsonNode ref : refs) {
                String v = norm(textOrNull(ref, "variableName"));
                if (v.isEmpty()) continue;
                String refPkg = norm(textOrNull(ref, "packageName"));
                if (!scope.admits(refPkg, v)) continue;
                ArrayNode list = byVar.computeIfAbsent(v, k -> m.createArrayNode());
                ObjectNode u = m.createObjectNode();
                u.put("package", refPkg);
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
        List<JsonNode> nodeFiles = readAll(r.collectNodeFiles(name));
        PackageGlobalScope scope = packageGlobalScope(nodeFiles);
        for (JsonNode node : nodeFiles) {
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
                if (!scope.admits(p, v)) continue;
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

    /* ══════════════════════════════ SCHEDULED JOBS ══════════════════════════════ */

    /**
     * Every scheduled job discovered by the crawler (Oracle DBMS_SCHEDULER/DBMS_JOB, SQL Server
     * Agent, pg_cron, MySQL EVENT), one row each: {@code name, schema, type, schedule, enabled,
     * target, targetNodeId, nodeId, callCount}. Never null — an analysis with no scheduler catalog
     * (or none crawled) returns an empty array, which pages as an empty facet.
     */
    JsonNode getJobs(String name) throws IOException {
        ObjectMapper m = r.mapper;
        String ck = r.cacheKey(name, "jobs");
        JsonNode cached = r.synthesizedCache.get(ck);
        if (cached != null) return cached;

        ArrayNode out = m.createArrayNode();
        try {
            JsonNode doc = r.getJobsIndex(name);
            JsonNode jobs = doc != null ? doc.path("jobs") : null;
            if (jobs != null && jobs.isArray()) {
                for (JsonNode j : jobs) out.add(j.deepCopy());
            }
        } catch (Exception e) {
            // No jobs artifact / unreadable: the honest answer is an empty facet, not a 500.
            out = m.createArrayNode();
        }
        r.synthesizedCache.put(ck, out);
        return out;
    }
}
