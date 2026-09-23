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
 * MySQL equivalent of TableValidationService / SqlServerTableEnricher: enriches each
 * table entry in api/tables/index.json with dbColumns / dbConstraints / dbIndexes /
 * dbTriggers / viewDefinition + dbFound/dbResolvedType, using the information_schema
 * catalog views — so the UI's table-detail panel renders the same rich data it does
 * for Oracle and SQL Server. Emits the SAME per-field JSON shape as SqlServerTableEnricher.
 */
public class MySqlTableEnricher {

    private static final Logger log = LoggerFactory.getLogger(MySqlTableEnricher.class);

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
                log.warn("[MySqlEnricher] cannot read {}: {}", indexFile, e.getMessage());
            }
        }
        if (rootPerDir.isEmpty() || names.isEmpty()) return;

        String schema = connManager.getMySqlSchema();
        if (schema == null || schema.isBlank()) {
            log.warn("[MySqlEnricher] no MySQL schema resolved from JDBC url — skipping enrichment");
            return;
        }

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
                    String sch = t.has("schema") ? t.get("schema").asText().toUpperCase() : null;
                    String key = resolveKey(sch, name, typeMap, colsMap);
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
                        // No MV concept in MySQL.
                        ViewDefinitionAnalyzer.attachTo(t, mapper, DbType.MYSQL, name, false);
                    }
                }
                root.put("dbValidated", true);
                try (OutputStream out = store.write(de.getKey().resolve("api", "tables", "index.json"))) {
                    mapper.writerWithDefaultPrettyPrinter().writeValue(out, root);
                } catch (Exception e) {
                    log.warn("[MySqlEnricher] write failed: {}", e.getMessage());
                }
                log.info("[MySqlEnricher] enriched {} tables in {}", found, de.getKey().leaf());
            }
            connManager.returnToPool(conn); conn = null;
        } catch (SQLException e) {
            connManager.closeAndDiscard(conn); conn = null;
            log.warn("[MySqlEnricher] failed: {}", e.getMessage());
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
            "SELECT TABLE_SCHEMA sch, TABLE_NAME nm, TABLE_TYPE td " +
            "FROM information_schema.TABLES " +
            "WHERE TABLE_SCHEMA = " + lit(schema) + " AND UPPER(TABLE_NAME) IN (" + inClause(names) + ")";
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String td = rs.getString("td");
                String type = td != null && td.toUpperCase().contains("VIEW") ? "VIEW" : "TABLE";
                m.put((rs.getString("sch") + "." + rs.getString("nm")).toUpperCase(), type);
            }
        }
        return m;
    }

    private static Map<String, List<ObjectNode>> queryColumns(Connection conn, ObjectMapper mapper, String schema, Set<String> names) throws SQLException {
        Map<String, List<ObjectNode>> m = new LinkedHashMap<>();
        String sql =
            "SELECT TABLE_SCHEMA sch, TABLE_NAME tbl, COLUMN_NAME col, DATA_TYPE dtype, " +
            "COLUMN_TYPE ctype, CHARACTER_MAXIMUM_LENGTH ml, NUMERIC_PRECISION pr, NUMERIC_SCALE sc, " +
            "IS_NULLABLE nul, COLUMN_KEY ckey, COLUMN_DEFAULT ddef, ORDINAL_POSITION cid, EXTRA extra " +
            "FROM information_schema.COLUMNS " +
            "WHERE TABLE_SCHEMA = " + lit(schema) + " AND UPPER(TABLE_NAME) IN (" + inClause(names) + ") " +
            "ORDER BY TABLE_NAME, ORDINAL_POSITION";
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String key = (rs.getString("sch") + "." + rs.getString("tbl")).toUpperCase();
                ObjectNode col = mapper.createObjectNode();
                col.put("columnName", rs.getString("col"));
                col.put("dataType", rs.getString("dtype"));
                long ml = rs.getLong("ml"); if (!rs.wasNull()) col.put("dataLength", ml);
                int pr = rs.getInt("pr"); if (!rs.wasNull() && pr > 0) col.put("dataPrecision", pr);
                int sc = rs.getInt("sc"); if (!rs.wasNull() && sc > 0) col.put("dataScale", sc);
                col.put("nullable", "YES".equalsIgnoreCase(rs.getString("nul")));
                col.put("columnId", rs.getInt("cid"));
                String ddef = rs.getString("ddef"); if (ddef != null) col.put("dataDefault", ddef);
                String extra = rs.getString("extra");
                if (extra != null && extra.toLowerCase().contains("generated")) col.put("computed", true);
                m.computeIfAbsent(key, k -> new ArrayList<>()).add(col);
            }
        }
        return m;
    }

    private static Map<String, List<ObjectNode>> queryConstraints(Connection conn, ObjectMapper mapper, String schema, Set<String> names) throws SQLException {
        Map<String, List<ObjectNode>> m = new LinkedHashMap<>();
        String in = inClause(names);
        String sql =
            "SELECT kcu.TABLE_SCHEMA sch, kcu.TABLE_NAME tbl, tc.CONSTRAINT_NAME cname, " +
            "tc.CONSTRAINT_TYPE ctype, kcu.COLUMN_NAME col, kcu.ORDINAL_POSITION pos, " +
            "kcu.REFERENCED_TABLE_NAME reftbl, kcu.REFERENCED_COLUMN_NAME refcol " +
            "FROM information_schema.TABLE_CONSTRAINTS tc " +
            "JOIN information_schema.KEY_COLUMN_USAGE kcu " +
            "  ON kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA " +
            "  AND kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME " +
            "  AND kcu.TABLE_NAME = tc.TABLE_NAME " +
            "WHERE tc.TABLE_SCHEMA = " + lit(schema) + " AND UPPER(tc.TABLE_NAME) IN (" + in + ") " +
            "ORDER BY tbl, cname, pos";
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String key = (rs.getString("sch") + "." + rs.getString("tbl")).toUpperCase();
                String ctype = rs.getString("ctype");
                ObjectNode c = mapper.createObjectNode();
                c.put("constraintName", rs.getString("cname"));
                c.put("constraintType", mapConstraintType(ctype));
                c.put("columnName", rs.getString("col"));
                c.put("position", rs.getInt("pos"));
                String reftbl = rs.getString("reftbl");
                if (reftbl != null) {
                    String refcol = rs.getString("refcol");
                    c.put("refConstraint", refcol != null ? reftbl + "." + refcol : reftbl);
                }
                m.computeIfAbsent(key, k -> new ArrayList<>()).add(c);
            }
        }
        return m;
    }

    private static String mapConstraintType(String ctype) {
        if (ctype == null) return "C";
        String u = ctype.toUpperCase();
        if (u.contains("PRIMARY")) return "P";
        if (u.contains("UNIQUE")) return "U";
        if (u.contains("FOREIGN")) return "R";
        if (u.contains("CHECK")) return "C";
        return "C";
    }

    private static Map<String, List<ObjectNode>> queryIndexes(Connection conn, ObjectMapper mapper, String schema, Set<String> names) throws SQLException {
        Map<String, List<ObjectNode>> m = new LinkedHashMap<>();
        String sql =
            "SELECT TABLE_SCHEMA sch, TABLE_NAME tbl, INDEX_NAME idx, NON_UNIQUE nonuniq, " +
            "COLUMN_NAME col, SEQ_IN_INDEX pos " +
            "FROM information_schema.STATISTICS " +
            "WHERE TABLE_SCHEMA = " + lit(schema) + " AND UPPER(TABLE_NAME) IN (" + inClause(names) + ") " +
            "ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX";
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String key = (rs.getString("sch") + "." + rs.getString("tbl")).toUpperCase();
                ObjectNode idx = mapper.createObjectNode();
                idx.put("indexName", rs.getString("idx"));
                idx.put("uniqueness", rs.getInt("nonuniq") == 0 ? "UNIQUE" : "NONUNIQUE");
                idx.put("columnName", rs.getString("col"));
                idx.put("position", rs.getInt("pos"));
                m.computeIfAbsent(key, k -> new ArrayList<>()).add(idx);
            }
        }
        return m;
    }

    private static Map<String, List<ObjectNode>> queryTriggers(Connection conn, ObjectMapper mapper, String schema, Set<String> names) throws SQLException {
        Map<String, List<ObjectNode>> m = new LinkedHashMap<>();
        String sql =
            "SELECT EVENT_OBJECT_SCHEMA sch, EVENT_OBJECT_TABLE tbl, TRIGGER_NAME trig, " +
            "EVENT_MANIPULATION ev, ACTION_TIMING timing " +
            "FROM information_schema.TRIGGERS " +
            "WHERE EVENT_OBJECT_SCHEMA = " + lit(schema) + " AND UPPER(EVENT_OBJECT_TABLE) IN (" + inClause(names) + ")";
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String key = (rs.getString("sch") + "." + rs.getString("tbl")).toUpperCase();
                ObjectNode tr = mapper.createObjectNode();
                tr.put("triggerName", rs.getString("trig"));
                tr.put("owner", rs.getString("sch"));
                tr.put("triggerType", rs.getString("timing"));
                tr.put("triggeringEvent", rs.getString("ev"));
                tr.put("status", "ENABLED");
                m.computeIfAbsent(key, k -> new ArrayList<>()).add(tr);
            }
        }
        return m;
    }

    private static Map<String, String> queryViewDefs(Connection conn, String schema, Set<String> names) throws SQLException {
        Map<String, String> m = new LinkedHashMap<>();
        String sql =
            "SELECT TABLE_SCHEMA sch, TABLE_NAME nm, VIEW_DEFINITION def " +
            "FROM information_schema.VIEWS " +
            "WHERE TABLE_SCHEMA = " + lit(schema) + " AND UPPER(TABLE_NAME) IN (" + inClause(names) + ")";
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                m.put((rs.getString("sch") + "." + rs.getString("nm")).toUpperCase(), rs.getString("def"));
            }
        }
        return m;
    }
}
