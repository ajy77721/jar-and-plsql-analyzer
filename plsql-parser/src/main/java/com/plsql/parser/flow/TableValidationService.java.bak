package com.plsql.parser.flow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.*;
import java.util.*;

/**
 * Validates and enriches table metadata in api/tables/index.json at analysis time.
 *
 * Query budget (first run, any number of tables):
 *   1 × all_objects  — confirm existence, resolve TABLE/VIEW/SYNONYM
 *   1 × all_tab_columns  — columns for every confirmed table (batched ≤500)
 *   1 × all_constraints + all_cons_columns  — constraints
 *   1 × all_indexes + all_ind_columns       — indexes
 *   1 × all_views                           — view text (VIEW-type only)
 *   ──────────────────────────────────────
 *   5 queries total regardless of table count.
 *
 * Re-run: tables that already have dbColumns stored are excluded from the enrichment
 * sets → 0 metadata queries on a fully-enriched analysis.
 */
public class TableValidationService {

    private static final Logger log = LoggerFactory.getLogger(TableValidationService.class);
    private static final int BATCH_SIZE = 500;
    private static final int MAX_VIEW_DEF_LENGTH = 32000;

    public static void validate(DbConnectionManager connManager, ArtifactStore store, ArtifactKey outputDir) {
        validateMultiple(connManager, store, Collections.singletonList(outputDir));
    }

    public static void validateMultiple(DbConnectionManager connManager, ArtifactStore store,
                                        List<ArtifactKey> outputDirs) {
        if (outputDirs == null || outputDirs.isEmpty() || store == null) return;

        ObjectMapper mapper = new ObjectMapper();

        // ── 1. Read index files ───────────────────────────────────────────────────
        // allNames      = every table name seen (need all_objects check)
        // enrichNames   = names that are missing dbColumns (need full metadata fetch)
        // viewDefNames  = VIEW/MVIEW names whose viewDefinition is not yet stored
        Map<ArtifactKey, ObjectNode> rootPerDir = new LinkedHashMap<>();
        Set<String> allNames    = new LinkedHashSet<>();
        Set<String> enrichNames = new LinkedHashSet<>();
        Set<String> viewDefNames = new LinkedHashSet<>();

        for (ArtifactKey dir : outputDirs) {
            ArtifactKey indexFile = dir.resolve("api", "tables", "index.json");
            if (!store.exists(indexFile)) continue;
            try (InputStream in = store.read(indexFile)) {
                ObjectNode root = (ObjectNode) mapper.readTree(in);
                rootPerDir.put(dir, root);
                JsonNode tables = root.get("tables");
                if (tables == null || !tables.isArray()) continue;
                for (JsonNode t : tables) {
                    String name   = t.has("name")   ? t.get("name").asText().toUpperCase()   : null;
                    String schema = t.has("schema") ? t.get("schema").asText().toUpperCase() : null;
                    if (name == null || name.isBlank()) continue;
                    allNames.add(name);
                    // Only enrich if metadata not already stored (skip on re-run)
                    if (!t.has("dbColumns")) enrichNames.add(name);
                    // Separately track views/mviews that are missing their definition
                    // (may have columns cached but viewDefinition added later).
                    // Store schema-qualified key so backfill can resolve type via foundObjects.
                    String resolvedType = t.has("dbResolvedType") ? t.get("dbResolvedType").asText() : "";
                    boolean isViewType = "VIEW".equals(resolvedType) || "MATERIALIZED VIEW".equals(resolvedType);
                    // Also include entries where dbColumns present but dbResolvedType not yet stored
                    // (analyses from before dbResolvedType was persisted) — section 3b filters by foundObjects
                    if (!t.has("viewDefinition") && (isViewType || (t.has("dbColumns") && resolvedType.isEmpty()))) {
                        String vKey = schema != null ? schema + "." + name : name;
                        viewDefNames.add(vKey);
                    }
                }
            } catch (Exception e) {
                log.warn("[TableValidation] Cannot read {}: {}", indexFile, e.getMessage());
            }
        }

        if (rootPerDir.isEmpty()) return;

        log.info("[TableValidation] {} names across {} dir(s); {} need enrichment",
                allNames.size(), rootPerDir.size(), enrichNames.size());

        // Check-out one connection from the pool; return it when done.
        // getAnyConnection() retries with backoff so transient ORA-17800 is survived.
        Connection conn;
        try {
            Connection c = connManager.getAnyConnection();
            // Ping — replace with a fresh connection if the pooled one is stale
            boolean valid = false;
            try { valid = c.isValid(5); } catch (Exception ignored) {}
            if (!valid) {
                connManager.closeAndDiscard(c);
                c = connManager.getAnyConnection();
            }
            conn = c;
        } catch (SQLException connectEx) {
            throw new RuntimeException("TableValidationService: cannot connect to Oracle: "
                    + connectEx.getMessage(), connectEx);
        }
        try {

            // ── 2. all_objects: confirm existence + resolve type ──────────────────────
            // Keys: "OWNER.NAME" → type  and  "NAME" → type (short fallback)
            Map<String, String> foundObjects = bulkQueryObjects(conn, allNames);

            long confirmedCount = foundObjects.keySet().stream().filter(k -> k.contains(".")).count();
            log.info("[TableValidation] Confirmed {} / {}", confirmedCount, allNames.size());

            // Build name → [owner, objectType] from the qualified keys
            // Short-key (name only) keeps first match; schema-qualified key for precise multi-schema lookup
            Map<String, String[]> resolvedInfo = new LinkedHashMap<>();
            for (Map.Entry<String, String> e : foundObjects.entrySet()) {
                String key = e.getKey();
                if (!key.contains(".")) continue; // skip short-key entries
                String[] parts = key.split("\\.", 2);
                resolvedInfo.putIfAbsent(parts[1], new String[]{parts[0], e.getValue()});
                // Gap 2: also store schema-qualified so same-name tables in different schemas resolve correctly
                resolvedInfo.putIfAbsent(parts[0] + "." + parts[1], new String[]{parts[0], e.getValue()});
            }

            // ── 3. Metadata queries — only for confirmed names that need enrichment ──
            // confirmedEnrich = enrichNames ∩ confirmed
            Set<String> confirmedEnrich = new LinkedHashSet<>();
            for (String name : enrichNames) {
                if (resolvedInfo.containsKey(name)) confirmedEnrich.add(name);
            }

            // Gap 1: follow synonyms one level so metadata queries use the real table name
            Map<String, String[]> synonymTargets = resolveSynonyms(conn, resolvedInfo, foundObjects);

            // Build namesForMetadata: replace synonym names with their target table names
            Map<String, String> synTargetToSource = new LinkedHashMap<>();
            Set<String> namesForMetadata = new LinkedHashSet<>();
            for (String name : confirmedEnrich) {
                String[] synInfo = synonymTargets.get(name);
                if (synInfo != null) {
                    namesForMetadata.add(synInfo[1]); // query by target name
                    synTargetToSource.put(synInfo[1], name);
                } else {
                    namesForMetadata.add(name);
                }
            }

            Map<String, List<ObjectNode>> columnsMap     = Collections.emptyMap();
            Map<String, List<ObjectNode>> constraintsMap = Collections.emptyMap();
            Map<String, List<ObjectNode>> indexesMap     = Collections.emptyMap();
            Map<String, String>           viewDefMap     = new LinkedHashMap<>();

            if (!namesForMetadata.isEmpty()) {
                columnsMap     = bulkQueryColumns(conn, mapper, namesForMetadata);
                constraintsMap = new LinkedHashMap<>(bulkQueryConstraints(conn, mapper, namesForMetadata));
                // Gap 8: also collect CHECK constraints (separate query — search_condition is LONG)
                bulkQueryCheckConstraints(conn, mapper, namesForMetadata, constraintsMap);
                indexesMap     = bulkQueryIndexes(conn, mapper, namesForMetadata);

                // Gap 3: VIEW defs from all_views, MVIEW defs from all_mviews (separate tables)
                Set<String> viewNames  = new LinkedHashSet<>();
                Set<String> mviewNames = new LinkedHashSet<>();
                for (String name : confirmedEnrich) {
                    String[] synInfo = synonymTargets.get(name);
                    String lookupName = synInfo != null ? synInfo[1] : name;
                    // Try schema-qualified key first (foundObjects may be keyed as SCHEMA.NAME)
                    String type = foundObjects.get(name);
                    if (type == null) {
                        // scan foundObjects for a schema-qualified match
                        for (Map.Entry<String, String> fo : foundObjects.entrySet()) {
                            String k = fo.getKey();
                            if (k.endsWith("." + name) || k.equals(name)) { type = fo.getValue(); break; }
                        }
                    }
                    if (type == null) { String[] ri = resolvedInfo.get(name); if (ri != null) type = ri[1]; }
                    if ("VIEW".equals(type)) viewNames.add(lookupName);
                    else if ("MATERIALIZED VIEW".equals(type)) mviewNames.add(lookupName);
                }
                if (!viewNames.isEmpty())  viewDefMap.putAll(bulkQueryViewDefs(conn, viewNames));
                if (!mviewNames.isEmpty()) viewDefMap.putAll(bulkQueryMViewDefs(conn, mviewNames));

                log.info("[TableValidation] Enriched {} tables — cols={} views={}",
                        namesForMetadata.size(), columnsMap.size(), viewDefMap.size());
            } else {
                log.info("[TableValidation] All metadata already stored — skipping enrichment queries");
                synonymTargets = Collections.emptyMap();
            }

            // ── 3b. Backfill view definitions for views that already have columns but no def ──
            // Handles analyses enriched before viewDefinition was stored (columns cached, def missing).
            // viewDefNames entries may be schema-qualified (SCHEMA.NAME) or bare (NAME).
            Set<String> backfillViews  = new LinkedHashSet<>();
            Set<String> backfillMViews = new LinkedHashSet<>();
            for (String vKey : viewDefNames) {
                // Split schema-qualified key back into bare name for enrichNames check
                int dot = vKey.indexOf('.');
                String bareName = dot >= 0 ? vKey.substring(dot + 1) : vKey;
                if (enrichNames.contains(bareName)) continue; // already handled in block above
                // Resolve type: try schema-qualified first, then bare name, then suffix scan
                String type = foundObjects.get(vKey);
                if (type == null) type = foundObjects.get(bareName);
                if (type == null) {
                    String[] ri = resolvedInfo.get(vKey);
                    if (ri == null) ri = resolvedInfo.get(bareName);
                    if (ri != null) type = ri[1];
                }
                if (type == null) {
                    // Suffix scan: old analysis where dbResolvedType was not persisted — check all_objects result
                    String sfx = "." + bareName;
                    for (Map.Entry<String, String> fo : foundObjects.entrySet()) {
                        if (fo.getKey().endsWith(sfx)) { type = fo.getValue(); break; }
                    }
                }
                if ("VIEW".equals(type)) backfillViews.add(bareName);
                else if ("MATERIALIZED VIEW".equals(type)) backfillMViews.add(bareName);
                // If type is still null and the entry came from dbColumns-present path, try VIEW
                // (safe guess: MVIEW entries would have dbResolvedType from modern runs)
                else if (type == null && dot >= 0) backfillViews.add(bareName);
            }
            if (!backfillViews.isEmpty()) {
                viewDefMap.putAll(bulkQueryViewDefs(conn, backfillViews));
                log.info("[TableValidation] Backfilled view defs for {} views", backfillViews.size());
            }
            if (!backfillMViews.isEmpty()) {
                viewDefMap.putAll(bulkQueryMViewDefs(conn, backfillMViews));
                log.info("[TableValidation] Backfilled mview defs for {} mviews", backfillMViews.size());
            }

            // ── 3c. Triggers: ALL_TRIGGERS for every confirmed table with DML ops ────
            // Keyed "OWNER.TABLE_NAME" → list of trigger summaries.
            Map<String, List<ObjectNode>> triggersMap = bulkQueryTriggers(conn, mapper, allNames);

            // ── 4. Patch each index.json ──────────────────────────────────────────────
            for (Map.Entry<ArtifactKey, ObjectNode> dirEntry : rootPerDir.entrySet()) {
                ArtifactKey dir        = dirEntry.getKey();
                ObjectNode root        = dirEntry.getValue();
                ArtifactKey indexFile  = dir.resolve("api", "tables", "index.json");

                JsonNode tablesNode = root.get("tables");
                if (tablesNode == null || !tablesNode.isArray()) continue;

                ArrayNode patched = mapper.createArrayNode();
                int foundCount = 0, notFoundCount = 0;

                for (JsonNode t : tablesNode) {
                    ObjectNode tObj = (ObjectNode) t;
                    String name   = t.has("name")   ? t.get("name").asText().toUpperCase()   : null;
                    String schema = t.has("schema")  ? t.get("schema").asText().toUpperCase() : null;
                    if (name == null) { patched.add(tObj); continue; }

                    // Resolve type — prefer schema-qualified key if schema is known
                    String fullKey    = schema != null ? schema + "." + name : name;
                    String resolvedType = foundObjects.get(fullKey);
                    if (resolvedType == null) resolvedType = foundObjects.get(name);

                    boolean dbFound = resolvedType != null;
                    tObj.put("dbFound",        dbFound);
                    tObj.put("dbValidated",    true);
                    tObj.put("dbResolvedType", dbFound ? resolvedType : "UNKNOWN");
                    // Normalise to the underscore form used elsewhere (node tables[]) so the UI
                    // sees a single consistent token, e.g. MATERIALIZED_VIEW (not "MATERIALIZED VIEW").
                    if (dbFound) tObj.put("objectType", resolvedType.replace(" ", "_"));

                    if (dbFound) {
                        foundCount++;
                        // Gap 2: prefer schema-qualified resolvedInfo key when schema is known
                        String[] info = (schema != null && resolvedInfo.containsKey(schema + "." + name))
                                ? resolvedInfo.get(schema + "." + name)
                                : resolvedInfo.get(name);
                        String owner = info != null ? info[0] : schema;
                        if (owner != null && !tObj.has("schema")) tObj.put("schema", owner);

                        // Gap 1: synonyms → metadata is keyed under the underlying target name/owner
                        String[] synInfo = synonymTargets.get(name);
                        String metaName  = synInfo != null ? synInfo[1] : name;
                        String metaOwner = synInfo != null ? synInfo[0] : owner;
                        String ownerKey  = metaOwner != null ? metaOwner + "." + metaName : metaName;

                        // Attach metadata only if not already present (skip re-run)
                        if (!tObj.has("dbColumns")) {
                            List<ObjectNode> cols = columnsMap.get(ownerKey);
                            if (cols != null) {
                                ArrayNode arr = mapper.createArrayNode();
                                cols.forEach(arr::add);
                                tObj.set("dbColumns", arr);
                            }
                        }
                        if (!tObj.has("dbConstraints")) {
                            List<ObjectNode> cons = constraintsMap.get(ownerKey);
                            if (cons != null) {
                                ArrayNode arr = mapper.createArrayNode();
                                cons.forEach(arr::add);
                                tObj.set("dbConstraints", arr);
                            }
                        }
                        if (!tObj.has("dbIndexes")) {
                            List<ObjectNode> idxs = indexesMap.get(ownerKey);
                            if (idxs != null) {
                                ArrayNode arr = mapper.createArrayNode();
                                idxs.forEach(arr::add);
                                tObj.set("dbIndexes", arr);
                            }
                        }
                        if (!tObj.has("viewDefinition")) {
                            String viewDef = viewDefMap.get(ownerKey);
                            // Fallback 1: bare name (owner mismatch for cross-schema views / synonyms)
                            if (viewDef == null) viewDef = viewDefMap.get(metaName);
                            // Fallback 2: scan for any key ending with ".<metaName>" (any owner)
                            if (viewDef == null) {
                                String suffix = "." + metaName;
                                for (Map.Entry<String, String> ve : viewDefMap.entrySet()) {
                                    if (ve.getKey().endsWith(suffix)) { viewDef = ve.getValue(); break; }
                                }
                            }
                            if (viewDef != null) tObj.put("viewDefinition", viewDef);
                        }
                        // Gap: parse the view/MV's own SELECT body through the normal FROM/JOIN
                        // extractor (never done before — DependencyCrawler.isNonParsableType
                        // excludes VIEW/MATERIALIZED_VIEW from the main crawl queue) so the
                        // tables/joins the view depends on are visible in the UI too.
                        if (!tObj.has("viewTableOperations")) {
                            ViewDefinitionAnalyzer.attachTo(tObj, mapper, com.plsqlanalyzer.config.DbType.ORACLE,
                                    name, "MATERIALIZED VIEW".equals(resolvedType));
                        }
                        // Attach triggers (always refresh — trigger state can change between runs)
                        List<ObjectNode> trigs = triggersMap.get(ownerKey);
                        if (trigs == null) trigs = triggersMap.get(metaName);
                        if (trigs != null) {
                            ArrayNode arr = mapper.createArrayNode();
                            trigs.forEach(arr::add);
                            tObj.set("dbTriggers", arr);
                        }
                    } else {
                        notFoundCount++;
                        // Gap 5: clear stale metadata if the table no longer exists in Oracle
                        tObj.remove("dbColumns");
                        tObj.remove("dbConstraints");
                        tObj.remove("dbIndexes");
                        tObj.remove("viewDefinition");
                        tObj.remove("dbTriggers");
                        log.debug("[TableValidation] NOT FOUND: {}", fullKey);
                    }

                    patched.add(tObj);
                }

                root.set("tables", patched);
                root.put("totalTables",    foundCount);
                root.put("totalTablesRaw", foundCount + notFoundCount);
                root.put("dbValidated",    true);

                try (OutputStream out = store.write(indexFile)) {
                    mapper.writerWithDefaultPrettyPrinter().writeValue(out, root);
                } catch (Exception e) {
                    log.error("[TableValidation] Write failed for {}: {}", indexFile, e.getMessage());
                }

                // Patch api/index.json so totalTables matches the validated count
                patchMainIndex(store, dir, foundCount, mapper);

                log.info("[TableValidation] {} — found={} notFound={} triggers={}",
                        dir.leaf(), foundCount, notFoundCount, triggersMap.size());
            }

        } catch (Exception e) {
            connManager.closeAndDiscard(conn);
            conn = null;
            log.error("[TableValidation] Fatal error — tables/index.json NOT enriched: {}", e.getMessage(), e);
            throw new RuntimeException("TableValidationService failed: " + e.getMessage(), e);
        } finally {
            if (conn != null) connManager.returnToPool(conn);
        }
    }

    // ─── query helpers — each uses table_name IN (...) batched at BATCH_SIZE ────
    // No owner = ? filter: one query covers all schemas at once.
    // Result key: "OWNER.TABLE_NAME" (owner comes from the query result row).

    private static Map<String, String> bulkQueryObjects(Connection conn, Set<String> names) {
        Map<String, String> result = new LinkedHashMap<>();
        List<String> nameList = new ArrayList<>(names);
        for (int i = 0; i < nameList.size(); i += BATCH_SIZE) {
            List<String> batch = nameList.subList(i, Math.min(i + BATCH_SIZE, nameList.size()));
            String ph  = ph(batch.size());
            // Prefer MATERIALIZED VIEW / VIEW over TABLE for a given owner.name: an Oracle MV also
            // has a same-named container TABLE row in all_objects. Without this precedence the
            // arbitrary row order lets putIfAbsent keep TABLE, mis-tagging MVs. Real base tables
            // only have a TABLE row, so they stay TABLE.
            String sql = "SELECT owner, object_name, object_type FROM all_objects " +
                    "WHERE object_name IN (" + ph + ") " +
                    "AND object_type IN ('TABLE','VIEW','MATERIALIZED VIEW','SYNONYM') " +
                    "ORDER BY owner, object_name, " +
                    "DECODE(object_type,'MATERIALIZED VIEW',1,'VIEW',2,'SYNONYM',3,'TABLE',4,5)";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                bind(ps, 1, batch);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String owner   = rs.getString("owner").toUpperCase();
                        String objName = rs.getString("object_name").toUpperCase();
                        String objType = rs.getString("object_type");
                        result.putIfAbsent(owner + "." + objName, objType);
                        result.putIfAbsent(objName, objType); // short-key fallback
                    }
                }
            } catch (Exception e) {
                log.warn("[TableValidation] all_objects error: {}", e.getMessage());
            }
        }
        return result;
    }

    private static Map<String, List<ObjectNode>> bulkQueryColumns(
            Connection conn, ObjectMapper mapper, Set<String> names) {
        Map<String, List<ObjectNode>> result = new LinkedHashMap<>();
        List<String> nameList = new ArrayList<>(names);
        for (int i = 0; i < nameList.size(); i += BATCH_SIZE) {
            List<String> batch = nameList.subList(i, Math.min(i + BATCH_SIZE, nameList.size()));
            String ph  = ph(batch.size());
            String sql = "SELECT owner, table_name, column_name, data_type, data_length, " +
                    "data_precision, data_scale, nullable, column_id, data_default " +
                    "FROM all_tab_columns WHERE table_name IN (" + ph + ") " +
                    "ORDER BY owner, table_name, column_id";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                bind(ps, 1, batch);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String key = rs.getString("owner").toUpperCase() + "." +
                                rs.getString("table_name").toUpperCase();
                        ObjectNode col = mapper.createObjectNode();
                        col.put("columnName", rs.getString("column_name"));
                        col.put("dataType",   rs.getString("data_type"));
                        col.put("dataLength", rs.getInt("data_length"));
                        Object prec = rs.getObject("data_precision");
                        if (prec != null) col.put("dataPrecision", ((Number) prec).intValue());
                        Object scale = rs.getObject("data_scale");
                        if (scale != null) col.put("dataScale", ((Number) scale).intValue());
                        col.put("nullable", "Y".equals(rs.getString("nullable")));
                        col.put("columnId", rs.getInt("column_id"));
                        String def = rs.getString("data_default");
                        if (def != null) col.put("dataDefault", def.trim());
                        result.computeIfAbsent(key, k -> new ArrayList<>()).add(col);
                    }
                }
            } catch (Exception e) {
                log.warn("[TableValidation] all_tab_columns error: {}", e.getMessage());
            }
        }
        return result;
    }

    private static Map<String, List<ObjectNode>> bulkQueryConstraints(
            Connection conn, ObjectMapper mapper, Set<String> names) {
        Map<String, List<ObjectNode>> result = new LinkedHashMap<>();
        List<String> nameList = new ArrayList<>(names);
        for (int i = 0; i < nameList.size(); i += BATCH_SIZE) {
            List<String> batch = nameList.subList(i, Math.min(i + BATCH_SIZE, nameList.size()));
            String ph  = ph(batch.size());
            // PRIMARY, UNIQUE, FOREIGN KEY — all have matching all_cons_columns rows
            String sql = "SELECT c.owner, c.table_name, c.constraint_name, c.constraint_type, " +
                    "cc.column_name, cc.position, c.r_constraint_name " +
                    "FROM all_constraints c " +
                    "JOIN all_cons_columns cc " +
                    "  ON c.owner = cc.owner AND c.constraint_name = cc.constraint_name " +
                    "WHERE c.table_name IN (" + ph + ") " +
                    "AND c.constraint_type IN ('P','U','R') " +
                    "ORDER BY c.owner, c.table_name, c.constraint_type, c.constraint_name, cc.position";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                bind(ps, 1, batch);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String key = rs.getString("owner").toUpperCase() + "." +
                                rs.getString("table_name").toUpperCase();
                        ObjectNode con = mapper.createObjectNode();
                        con.put("constraintName", rs.getString("constraint_name"));
                        con.put("constraintType", rs.getString("constraint_type"));
                        con.put("columnName",     rs.getString("column_name"));
                        con.put("position",       rs.getInt("position"));
                        String ref = rs.getString("r_constraint_name");
                        if (ref != null) con.put("refConstraint", ref);
                        result.computeIfAbsent(key, k -> new ArrayList<>()).add(con);
                    }
                }
            } catch (Exception e) {
                log.warn("[TableValidation] all_constraints error: {}", e.getMessage());
            }
        }
        return result;
    }

    /** Gap 8: CHECK constraints — separate query because search_condition is a LONG column. */
    private static void bulkQueryCheckConstraints(
            Connection conn, ObjectMapper mapper, Set<String> names,
            Map<String, List<ObjectNode>> result) {
        List<String> nameList = new ArrayList<>(names);
        for (int i = 0; i < nameList.size(); i += BATCH_SIZE) {
            List<String> batch = nameList.subList(i, Math.min(i + BATCH_SIZE, nameList.size()));
            String ph  = ph(batch.size());
            // search_condition is LONG — must be the last selected column for Oracle JDBC
            String sql = "SELECT c.owner, c.table_name, c.constraint_name, c.search_condition " +
                    "FROM all_constraints c " +
                    "WHERE c.table_name IN (" + ph + ") AND c.constraint_type = 'C' " +
                    "ORDER BY c.owner, c.table_name, c.constraint_name";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                bind(ps, 1, batch);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String key = rs.getString("owner").toUpperCase() + "." +
                                rs.getString("table_name").toUpperCase();
                        ObjectNode con = mapper.createObjectNode();
                        con.put("constraintName", rs.getString("constraint_name"));
                        con.put("constraintType", "C");
                        // LONG column: getString may truncate at 32767 chars — acceptable for CHECK constraints
                        try {
                            String sc = rs.getString("search_condition");
                            if (sc != null && !sc.isBlank()) con.put("searchCondition", sc.trim());
                        } catch (Exception ignored) {}
                        result.computeIfAbsent(key, k -> new ArrayList<>()).add(con);
                    }
                }
            } catch (Exception e) {
                log.warn("[TableValidation] check_constraints error: {}", e.getMessage());
            }
        }
    }

    private static Map<String, List<ObjectNode>> bulkQueryIndexes(
            Connection conn, ObjectMapper mapper, Set<String> names) {
        Map<String, List<ObjectNode>> result = new LinkedHashMap<>();
        List<String> nameList = new ArrayList<>(names);
        for (int i = 0; i < nameList.size(); i += BATCH_SIZE) {
            List<String> batch = nameList.subList(i, Math.min(i + BATCH_SIZE, nameList.size()));
            String ph  = ph(batch.size());
            String sql = "SELECT i.table_owner, i.table_name, i.index_name, i.uniqueness, " +
                    "ic.column_name, ic.column_position " +
                    "FROM all_indexes i " +
                    "JOIN all_ind_columns ic " +
                    "  ON i.owner = ic.index_owner AND i.index_name = ic.index_name " +
                    "WHERE i.table_name IN (" + ph + ") " +
                    "ORDER BY i.table_owner, i.table_name, i.index_name, ic.column_position";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                bind(ps, 1, batch);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String key = rs.getString("table_owner").toUpperCase() + "." +
                                rs.getString("table_name").toUpperCase();
                        ObjectNode idx = mapper.createObjectNode();
                        idx.put("indexName",  rs.getString("index_name"));
                        idx.put("uniqueness", rs.getString("uniqueness"));
                        idx.put("columnName", rs.getString("column_name"));
                        idx.put("position",   rs.getInt("column_position"));
                        result.computeIfAbsent(key, k -> new ArrayList<>()).add(idx);
                    }
                }
            } catch (Exception e) {
                log.warn("[TableValidation] all_indexes error: {}", e.getMessage());
            }
        }
        return result;
    }

    /** Gap 4: TO_CLOB avoids Oracle LONG truncation; Gap 9: cap at MAX_VIEW_DEF_LENGTH. */
    private static Map<String, String> bulkQueryViewDefs(Connection conn, Set<String> names) {
        Map<String, String> result = new LinkedHashMap<>();
        List<String> nameList = new ArrayList<>(names);
        for (int i = 0; i < nameList.size(); i += BATCH_SIZE) {
            List<String> batch = nameList.subList(i, Math.min(i + BATCH_SIZE, nameList.size()));
            String ph  = ph(batch.size());
            // Oracle's TO_LOB/TO_CLOB conversion of a LONG column is only valid in the select list
            // of an INSERT ... SELECT — a bare SELECT (as here) raises ORA-00932 "expected CHAR got
            // LONG" regardless of ORDER BY. Read the LONG column directly instead: the JDBC driver
            // returns it as a String, and capViewDef() below already truncates at
            // MAX_VIEW_DEF_LENGTH (32000 chars) — comfortably under the driver's own LONG fetch
            // limit, so there is no additional data loss from skipping the TO_CLOB workaround.
            String sql = "SELECT owner, view_name, text FROM all_views " +
                    "WHERE view_name IN (" + ph + ")";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                bind(ps, 1, batch);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String owner    = rs.getString("owner").toUpperCase();
                        String viewName = rs.getString("view_name").toUpperCase();
                        String key = owner + "." + viewName;
                        String text = rs.getString("text");
                        if (text != null) {
                            String capped = capViewDef(text);
                            result.putIfAbsent(key, capped);
                            result.putIfAbsent(viewName, capped); // bare-name fallback key
                        }
                    }
                }
            } catch (Exception e) {
                log.warn("[TableValidation] all_views error: {}", e.getMessage());
            }
        }
        return result;
    }

    /** Gap 3: MATERIALIZED VIEW text lives in all_mviews.query, not all_views.text. */
    private static Map<String, String> bulkQueryMViewDefs(Connection conn, Set<String> names) {
        Map<String, String> result = new LinkedHashMap<>();
        List<String> nameList = new ArrayList<>(names);
        for (int i = 0; i < nameList.size(); i += BATCH_SIZE) {
            List<String> batch = nameList.subList(i, Math.min(i + BATCH_SIZE, nameList.size()));
            String ph  = ph(batch.size());
            // all_mviews.query is a LONG column — same TO_LOB/INSERT-only restriction as
            // bulkQueryViewDefs above (ORA-00932), same fix: read it directly, no TO_CLOB.
            String sql = "SELECT owner, mview_name, query FROM all_mviews " +
                    "WHERE mview_name IN (" + ph + ")";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                bind(ps, 1, batch);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String owner     = rs.getString("owner").toUpperCase();
                        String mviewName = rs.getString("mview_name").toUpperCase();
                        String key = owner + "." + mviewName;
                        String text = rs.getString("query");
                        if (text != null) {
                            String capped = capViewDef(text);
                            result.putIfAbsent(key, capped);
                            result.putIfAbsent(mviewName, capped); // bare-name fallback key
                        }
                    }
                }
            } catch (Exception e) {
                log.warn("[TableValidation] all_mviews error: {}", e.getMessage());
            }
        }
        return result;
    }

    /** Query ALL_TRIGGERS for every table name; keyed "OWNER.TABLE_NAME" and bare "TABLE_NAME". */
    private static Map<String, List<ObjectNode>> bulkQueryTriggers(
            Connection conn, ObjectMapper mapper, Set<String> names) {
        Map<String, List<ObjectNode>> result = new LinkedHashMap<>();
        List<String> nameList = new ArrayList<>(names);
        for (int i = 0; i < nameList.size(); i += BATCH_SIZE) {
            List<String> batch = nameList.subList(i, Math.min(i + BATCH_SIZE, nameList.size()));
            String ph = ph(batch.size());
            String sql = "SELECT owner, trigger_name, table_owner, table_name, " +
                    "trigger_type, triggering_event, status " +
                    "FROM all_triggers WHERE table_name IN (" + ph + ") " +
                    "ORDER BY table_owner, table_name, trigger_name";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                bind(ps, 1, batch);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String tableOwner = rs.getString("table_owner").toUpperCase();
                        String tableName  = rs.getString("table_name").toUpperCase();
                        String qualKey    = tableOwner + "." + tableName;
                        ObjectNode trig = mapper.createObjectNode();
                        trig.put("triggerName",      rs.getString("trigger_name"));
                        trig.put("owner",            rs.getString("owner"));
                        trig.put("triggerType",      rs.getString("trigger_type"));
                        trig.put("triggeringEvent",  rs.getString("triggering_event"));
                        trig.put("status",           rs.getString("status"));
                        result.computeIfAbsent(qualKey,   k -> new ArrayList<>()).add(trig);
                        result.computeIfAbsent(tableName, k -> new ArrayList<>()).add(trig);
                    }
                }
            } catch (Exception e) {
                log.warn("[TableValidation] all_triggers error: {}", e.getMessage());
            }
        }
        return result;
    }

    /**
     * Gap 1: follow synonyms one level deep.
     * Returns synonymName → [targetOwner, targetName, targetType].
     * Also updates foundObjects and resolvedInfo in place with the resolved type.
     */
    private static Map<String, String[]> resolveSynonyms(Connection conn,
                                                          Map<String, String[]> resolvedInfo,
                                                          Map<String, String> foundObjects) {
        Set<String> synonymNames = new LinkedHashSet<>();
        for (Map.Entry<String, String[]> e : resolvedInfo.entrySet()) {
            if (e.getKey().contains(".")) continue; // only short keys
            if ("SYNONYM".equals(e.getValue()[1])) synonymNames.add(e.getKey());
        }
        if (synonymNames.isEmpty()) return Collections.emptyMap();

        Map<String, String[]> targets = new LinkedHashMap<>();
        List<String> nameList = new ArrayList<>(synonymNames);
        for (int i = 0; i < nameList.size(); i += BATCH_SIZE) {
            List<String> batch = nameList.subList(i, Math.min(i + BATCH_SIZE, nameList.size()));
            String ph = ph(batch.size());
            String sql = "SELECT s.synonym_name, s.table_owner, s.table_name, o.object_type " +
                    "FROM all_synonyms s " +
                    "LEFT JOIN all_objects o ON o.owner = s.table_owner " +
                    "  AND o.object_name = s.table_name " +
                    "  AND o.object_type IN ('TABLE','VIEW','MATERIALIZED VIEW') " +
                    "WHERE s.synonym_name IN (" + ph + ")";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                bind(ps, 1, batch);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String synName    = rs.getString("synonym_name").toUpperCase();
                        String targetOwner = rs.getString("table_owner");
                        String targetName  = rs.getString("table_name");
                        String targetType  = rs.getString("object_type");
                        if (targetOwner == null || targetName == null || targetType == null) continue;
                        targetOwner = targetOwner.toUpperCase();
                        targetName  = targetName.toUpperCase();
                        targetType  = targetType.toUpperCase();
                        targets.putIfAbsent(synName, new String[]{targetOwner, targetName, targetType});
                        // Reflect resolved type back into the lookup maps
                        foundObjects.put(synName, targetType);
                        foundObjects.put(targetOwner + "." + synName, targetType);
                        resolvedInfo.put(synName, new String[]{targetOwner, targetType});
                    }
                }
            } catch (Exception e) {
                log.warn("[TableValidation] synonym resolution error: {}", e.getMessage());
            }
        }
        log.info("[TableValidation] Synonyms resolved: {} of {}", targets.size(), synonymNames.size());
        return targets;
    }

    /** Gap 9: cap stored view definition text. */
    private static String capViewDef(String text) {
        if (text == null) return null;
        if (text.length() <= MAX_VIEW_DEF_LENGTH) return text;
        return text.substring(0, MAX_VIEW_DEF_LENGTH) + "\n/* [truncated — definition exceeds 32 000 chars] */";
    }

    /** Patch totalTables in api/index.json to match the validated count. */
    private static void patchMainIndex(ArtifactStore store, ArtifactKey dir, int validatedCount,
                                       ObjectMapper mapper) {
        ArtifactKey mainIndex = dir.resolve("api", "index.json");
        if (!store.exists(mainIndex)) return;
        try {
            ObjectNode idx;
            try (InputStream in = store.read(mainIndex)) {
                idx = (ObjectNode) mapper.readTree(in);
            }
            idx.put("totalTables", validatedCount);
            try (OutputStream out = store.write(mainIndex)) {
                mapper.writerWithDefaultPrettyPrinter().writeValue(out, idx);
            }
        } catch (Exception e) {
            log.warn("[TableValidation] Could not patch api/index.json: {}", e.getMessage());
        }
    }

    // ── tiny utilities ────────────────────────────────────────────────────────────

    private static String ph(int n) {
        return String.join(",", Collections.nCopies(n, "?"));
    }

    private static void bind(PreparedStatement ps, int startIdx, List<String> values) throws SQLException {
        for (int i = 0; i < values.size(); i++) ps.setString(startIdx + i, values.get(i));
    }
}
