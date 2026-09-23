package com.plsql.parser.flow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.plsqlanalyzer.config.DbType;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.*;
import java.util.*;

/**
 * DB2 equivalent of SqlServerTableEnricher/TableValidationService: enriches each
 * table entry in api/tables/index.json with dbColumns / dbConstraints / dbIndexes /
 * dbTriggers / viewDefinition + dbFound/dbResolvedType, using the SYSCAT.* catalog
 * views — so the UI's table-detail panel renders the same rich data it does for Oracle.
 */
public class Db2TableEnricher {

    private static final Logger log = LoggerFactory.getLogger(Db2TableEnricher.class);

    public static void enrich(DbConnectionManager connManager, ArtifactStore store,
                              List<ArtifactKey> outputDirs) {
        if (outputDirs == null || outputDirs.isEmpty() || store == null) return;
        ObjectMapper mapper = new ObjectMapper();

        Map<ArtifactKey, ObjectNode> rootPerDir = new LinkedHashMap<>();
        Set<String> names = new LinkedHashSet<>();   // bare UPPER names to look up
        for (ArtifactKey dir : outputDirs) {
            ArtifactKey indexFile = dir.resolve("api", "tables", "index.json");
            if (!store.exists(indexFile)) continue;
            try (InputStream in = store.read(indexFile)) {
                ObjectNode root = (ObjectNode) mapper.readTree(in);
                rootPerDir.put(dir, root);
                JsonNode tables = root.get("tables");
                if (tables != null && tables.isArray()) {
                    for (JsonNode t : tables) {
                        String n = t.has("name") ? t.get("name").asText() : null;
                        if (n != null && !n.isBlank() && !n.startsWith("@") && !n.startsWith("#"))
                            names.add(n.toUpperCase());
                    }
                }
            } catch (Exception e) {
                log.warn("[Db2Enricher] cannot read {}: {}", indexFile, e.getMessage());
            }
        }
        if (rootPerDir.isEmpty() || names.isEmpty()) return;

        String schema = connManager.getDb2Schema();
        if (schema == null || schema.isBlank()) {
            log.warn("[Db2Enricher] no DB2 schema configured; skipping");
            return;
        }
        schema = schema.trim().toUpperCase();

        Connection conn = null;
        try {
            conn = connManager.getAnyConnection();
            Map<String, String>            typeMap = queryObjectTypes(conn, schema, names);   // SCHEMA.NAME -> TABLE/VIEW
            Map<String, List<ObjectNode>>  colsMap = queryColumns(conn, mapper, schema, names);
            Map<String, List<ObjectNode>>  consMap = queryConstraints(conn, mapper, schema, names);
            Map<String, List<ObjectNode>>  idxMap  = queryIndexes(conn, mapper, schema, names);
            Map<String, List<ObjectNode>>  trigMap = queryTriggers(conn, mapper, schema, names);
            Map<String, String>            viewMap = queryViewDefs(conn, schema, names);

            for (Map.Entry<ArtifactKey, ObjectNode> de : rootPerDir.entrySet()) {
                ObjectNode root = de.getValue();
                JsonNode tablesNode = root.get("tables");
                if (tablesNode == null || !tablesNode.isArray()) continue;
                int found = 0;
                for (JsonNode tn : tablesNode) {
                    ObjectNode t = (ObjectNode) tn;
                    String name = t.has("name") ? t.get("name").asText().toUpperCase() : null;
                    if (name == null) continue;
                    String tschema = t.has("schema") ? t.get("schema").asText().toUpperCase() : null;
                    String key = resolveKey(tschema, name, typeMap, colsMap);
                    String rtype = key != null ? typeMap.get(key) : null;
                    boolean dbFound = key != null && (rtype != null || colsMap.containsKey(key));
                    t.put("dbValidated", true);
                    t.put("dbFound", dbFound);
                    t.put("dbResolvedType", rtype != null ? rtype : (dbFound ? "TABLE" : "UNKNOWN"));
                    if (dbFound) {
                        found++;
                        if (rtype != null) t.put("objectType", rtype);
                        attach(t, "dbColumns", colsMap.get(key), mapper);
                        attach(t, "dbConstraints", consMap.get(key), mapper);
                        attach(t, "dbIndexes", idxMap.get(key), mapper);
                        attach(t, "dbTriggers", trigMap.get(key), mapper);
                        if (viewMap.containsKey(key)) t.put("viewDefinition", viewMap.get(key));
                        // MQTs (rtype "MATERIALIZED VIEW", see queryObjectTypes) already get their
                        // viewDefinition from the lookup above: per IBM's SYSCAT.VIEWS docs, "Each
                        // row represents a view OR materialized query table", and its TEXT column
                        // is "Full text of the view or materialized query table CREATE statement,
                        // exactly as typed" — i.e. SYSCAT.VIEWS carries one row per MQT, keyed by
                        // the same VIEWSCHEMA/VIEWNAME as the MQT's TABSCHEMA/TABNAME, holding the
                        // full "CREATE TABLE ... AS (fullselect) DATA INITIALLY DEFERRED REFRESH
                        // ..." text. SYSCAT.TABLES itself never stores this (only TYPE/REFRESH/
                        // PROPERTY metadata about the MQT, no query text), which is why a separate
                        // fetch for MQTs isn't needed here: queryViewDefs() above already queries
                        // SYSCAT.VIEWS over the full (unfiltered by type) `names` set, so an MQT's
                        // row is picked up by that same call and merged into viewMap like any
                        // ordinary view. attachTo is keyed off viewDefinition being non-blank, not
                        // off object type, so it already covers MQTs once viewDefinition is set.
                        ViewDefinitionAnalyzer.attachTo(t, mapper, DbType.DB2, name,
                                "MATERIALIZED VIEW".equals(rtype));
                    }
                }
                root.put("dbValidated", true);
                try (OutputStream out = store.write(de.getKey().resolve("api", "tables", "index.json"))) {
                    mapper.writerWithDefaultPrettyPrinter().writeValue(out, root);
                } catch (Exception e) {
                    log.warn("[Db2Enricher] write failed: {}", e.getMessage());
                }
                log.info("[Db2Enricher] enriched {} tables in {}", found, de.getKey().leaf());
            }
            connManager.returnToPool(conn); conn = null;
        } catch (SQLException e) {
            connManager.closeAndDiscard(conn); conn = null;
            log.warn("[Db2Enricher] failed: {}", e.getMessage());
        } finally {
            if (conn != null) connManager.closeAndDiscard(conn);
        }
    }

    private static String resolveKey(String schema, String name, Map<String,String> typeMap,
                                     Map<String,List<ObjectNode>> colsMap) {
        if (schema != null) {
            String k = schema + "." + name;
            if (typeMap.containsKey(k) || colsMap.containsKey(k)) return k;
        }
        // fall back to any schema match for this bare name
        for (String k : typeMap.keySet()) if (k.endsWith("." + name)) return k;
        for (String k : colsMap.keySet()) if (k.endsWith("." + name)) return k;
        return null;
    }

    private static void attach(ObjectNode t, String field, List<ObjectNode> vals, ObjectMapper mapper) {
        if (vals == null || vals.isEmpty()) return;
        ArrayNode arr = mapper.createArrayNode();
        vals.forEach(arr::add);
        t.set(field, arr);
    }

    private static String inClause(Set<String> names) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String n : names) {
            if (!first) sb.append(",");
            sb.append("'").append(n.replace("'", "''")).append("'");
            first = false;
        }
        return sb.toString();
    }

    private static String lit(String s) {
        return "'" + s.replace("'", "''") + "'";
    }

    private static Map<String, String> queryObjectTypes(Connection conn, String schema, Set<String> names) throws SQLException {
        Map<String, String> m = new LinkedHashMap<>();
        String sql =
            "SELECT TABSCHEMA sch, TABNAME nm, TYPE td FROM SYSCAT.TABLES " +
            "WHERE TABSCHEMA = " + lit(schema) + " AND TABNAME IN (" + inClause(names) + ")";
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String td = rs.getString("td");
                td = td != null ? td.trim() : "";
                // TYPE: V=view, S=materialized query table (MQT) — DB2's equivalent of a
                // materialized view. Without this an MQT was misclassified as a plain TABLE and
                // its defining query was never even looked for. "MATERIALIZED VIEW" (with a
                // space) matches the dbResolvedType Oracle's TableValidationService already
                // writes — tableDetail.js's isView check compares against that exact string.
                String type = "V".equals(td) ? "VIEW" : ("S".equals(td) ? "MATERIALIZED VIEW" : "TABLE");
                m.put((rs.getString("sch").trim() + "." + rs.getString("nm").trim()).toUpperCase(), type);
            }
        }
        return m;
    }

    private static Map<String, List<ObjectNode>> queryColumns(Connection conn, ObjectMapper mapper, String schema, Set<String> names) throws SQLException {
        Map<String, List<ObjectNode>> m = new LinkedHashMap<>();
        String sql =
            "SELECT TABSCHEMA sch, TABNAME tbl, COLNAME col, TYPENAME dtype, LENGTH ml, " +
            "SCALE sc, NULLS nul, DEFAULT ddef, COLNO cid " +
            "FROM SYSCAT.COLUMNS " +
            "WHERE TABSCHEMA = " + lit(schema) + " AND TABNAME IN (" + inClause(names) + ") " +
            "ORDER BY TABNAME, COLNO";
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String key = (rs.getString("sch").trim() + "." + rs.getString("tbl").trim()).toUpperCase();
                ObjectNode col = mapper.createObjectNode();
                col.put("columnName", rs.getString("col").trim());
                col.put("dataType", rs.getString("dtype") != null ? rs.getString("dtype").trim() : null);
                col.put("dataLength", rs.getInt("ml"));
                int sc = rs.getInt("sc"); if (sc > 0) col.put("dataScale", sc);
                col.put("nullable", "Y".equalsIgnoreCase(rs.getString("nul") != null ? rs.getString("nul").trim() : ""));
                col.put("columnId", rs.getInt("cid") + 1);
                String ddef = rs.getString("ddef"); if (ddef != null) col.put("dataDefault", ddef.trim());
                m.computeIfAbsent(key, k -> new ArrayList<>()).add(col);
            }
        }
        return m;
    }

    private static Map<String, List<ObjectNode>> queryConstraints(Connection conn, ObjectMapper mapper, String schema, Set<String> names) throws SQLException {
        Map<String, List<ObjectNode>> m = new LinkedHashMap<>();
        String in = inClause(names);
        // PK / UNIQUE / FK columns via TABCONST + KEYCOLUSE
        String pkSql =
            "SELECT tc.TABSCHEMA sch, tc.TABNAME tbl, tc.CONSTNAME cname, tc.TYPE ctype, " +
            "kc.COLNAME col, kc.COLSEQ pos " +
            "FROM SYSCAT.TABCONST tc " +
            "JOIN SYSCAT.KEYCOLUSE kc ON kc.CONSTNAME = tc.CONSTNAME AND kc.TABSCHEMA = tc.TABSCHEMA AND kc.TABNAME = tc.TABNAME " +
            "WHERE tc.TABSCHEMA = " + lit(schema) + " AND tc.TABNAME IN (" + in + ") AND tc.TYPE IN ('P','U','F') " +
            "ORDER BY tc.TABNAME, tc.CONSTNAME, kc.COLSEQ";
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(pkSql)) {
            while (rs.next()) {
                String key = (rs.getString("sch").trim() + "." + rs.getString("tbl").trim()).toUpperCase();
                String type = rs.getString("ctype") != null ? rs.getString("ctype").trim() : "";
                ObjectNode c = mapper.createObjectNode();
                c.put("constraintName", rs.getString("cname").trim());
                // P=primary, U=unique, F=foreign key (referential)
                c.put("constraintType", "P".equals(type) ? "P" : ("F".equals(type) ? "R" : "U"));
                c.put("columnName", rs.getString("col").trim());
                c.put("position", rs.getInt("pos"));
                m.computeIfAbsent(key, k -> new ArrayList<>()).add(c);
            }
        }
        // FK referenced-table targets via SYSCAT.REFERENCES
        String fkSql =
            "SELECT TABSCHEMA sch, TABNAME tbl, CONSTNAME cname, " +
            "REFTABSCHEMA reftsch, REFTABNAME reftbl " +
            "FROM SYSCAT.REFERENCES " +
            "WHERE TABSCHEMA = " + lit(schema) + " AND TABNAME IN (" + in + ")";
        Map<String, String> fkRefs = new LinkedHashMap<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(fkSql)) {
            while (rs.next()) {
                String cname = rs.getString("cname").trim();
                String ref = rs.getString("reftsch").trim() + "." + rs.getString("reftbl").trim();
                fkRefs.put(cname, ref);
            }
        }
        // attach refConstraint target onto FK constraint rows
        if (!fkRefs.isEmpty()) {
            for (List<ObjectNode> list : m.values()) {
                for (ObjectNode c : list) {
                    if ("R".equals(c.path("constraintType").asText())) {
                        String ref = fkRefs.get(c.path("constraintName").asText());
                        if (ref != null) c.put("refConstraint", ref);
                    }
                }
            }
        }
        return m;
    }

    private static Map<String, List<ObjectNode>> queryIndexes(Connection conn, ObjectMapper mapper, String schema, Set<String> names) throws SQLException {
        Map<String, List<ObjectNode>> m = new LinkedHashMap<>();
        String sql =
            "SELECT i.TABSCHEMA sch, i.TABNAME tbl, i.INDNAME idx, i.UNIQUERULE uniq, " +
            "ic.COLNAME col, ic.COLSEQ pos " +
            "FROM SYSCAT.INDEXES i " +
            "JOIN SYSCAT.INDEXCOLUSE ic ON ic.INDNAME = i.INDNAME AND ic.INDSCHEMA = i.INDSCHEMA " +
            "WHERE i.TABSCHEMA = " + lit(schema) + " AND i.TABNAME IN (" + inClause(names) + ") " +
            "ORDER BY i.TABNAME, i.INDNAME, ic.COLSEQ";
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String key = (rs.getString("sch").trim() + "." + rs.getString("tbl").trim()).toUpperCase();
                String uniq = rs.getString("uniq") != null ? rs.getString("uniq").trim() : "D";
                ObjectNode idx = mapper.createObjectNode();
                idx.put("indexName", rs.getString("idx").trim());
                // UNIQUERULE: U=unique, P=primary key (unique), D=duplicates allowed
                idx.put("uniqueness", "D".equals(uniq) ? "NONUNIQUE" : "UNIQUE");
                idx.put("columnName", rs.getString("col").trim());
                idx.put("position", rs.getInt("pos"));
                m.computeIfAbsent(key, k -> new ArrayList<>()).add(idx);
            }
        }
        return m;
    }

    private static Map<String, List<ObjectNode>> queryTriggers(Connection conn, ObjectMapper mapper, String schema, Set<String> names) throws SQLException {
        Map<String, List<ObjectNode>> m = new LinkedHashMap<>();
        String sql =
            "SELECT TABSCHEMA sch, TABNAME tbl, TRIGNAME trig, TRIGEVENT ev, TRIGTIME tm " +
            "FROM SYSCAT.TRIGGERS " +
            "WHERE TABSCHEMA = " + lit(schema) + " AND TABNAME IN (" + inClause(names) + ")";
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String key = (rs.getString("sch").trim() + "." + rs.getString("tbl").trim()).toUpperCase();
                String ev = rs.getString("ev") != null ? rs.getString("ev").trim() : "";
                String tm = rs.getString("tm") != null ? rs.getString("tm").trim() : "";
                ObjectNode tr = mapper.createObjectNode();
                tr.put("triggerName", rs.getString("trig").trim());
                tr.put("owner", rs.getString("sch").trim());
                // TRIGTIME: B=BEFORE, A=AFTER, I=INSTEAD OF
                tr.put("triggerType", "B".equals(tm) ? "BEFORE" : ("I".equals(tm) ? "INSTEAD OF" : "AFTER"));
                // TRIGEVENT: I=INSERT, U=UPDATE, D=DELETE
                tr.put("triggeringEvent", "I".equals(ev) ? "INSERT" : ("U".equals(ev) ? "UPDATE" : ("D".equals(ev) ? "DELETE" : ev)));
                tr.put("status", "ENABLED");
                m.computeIfAbsent(key, k -> new ArrayList<>()).add(tr);
            }
        }
        return m;
    }

    /**
     * Fetches the full CREATE-statement text for every VIEW and MQT (materialized query table)
     * whose name is in {@code names}. SYSCAT.VIEWS carries a row for both object kinds — IBM's
     * catalog reference documents it as "Each row represents a view or materialized query
     * table", with TEXT holding the "Full text of the view or materialized query table CREATE
     * statement, exactly as typed". SYSCAT.TABLES has no equivalent text column for MQTs (only
     * TYPE/REFRESH/PROPERTY metadata), so this single query — run over the unfiltered table-name
     * set, not just names already known to be VIEWs — is also the correct/only source for MQT
     * definitions; no separate MQT-specific query is needed (unlike Oracle, where MV text lives
     * in a genuinely separate catalog view, ALL_MVIEWS.QUERY, from ALL_VIEWS.TEXT).
     */
    private static Map<String, String> queryViewDefs(Connection conn, String schema, Set<String> names) throws SQLException {
        Map<String, String> m = new LinkedHashMap<>();
        String sql =
            "SELECT VIEWSCHEMA sch, VIEWNAME nm, TEXT def FROM SYSCAT.VIEWS " +
            "WHERE VIEWSCHEMA = " + lit(schema) + " AND VIEWNAME IN (" + inClause(names) + ")";
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String def = rs.getString("def");
                m.put((rs.getString("sch").trim() + "." + rs.getString("nm").trim()).toUpperCase(),
                      def != null ? def.trim() : null);
            }
        }
        return m;
    }
}
