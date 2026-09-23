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
 * PostgreSQL equivalent of Db2TableEnricher/SqlServerTableEnricher: enriches each table entry
 * in api/tables/index.json with dbColumns / dbConstraints / dbIndexes / dbTriggers /
 * viewDefinition + dbFound/dbResolvedType, using information_schema + pg_catalog — so the UI's
 * table-detail panel renders the same rich data it does for the other dialects.
 *
 * PostgreSQL objects can live in any schema (the demo uses "erp" while the connect user is
 * "postgres"), so this searches ALL user schemas by table name rather than one fixed schema.
 */
public class PostgresTableEnricher {

    private static final Logger log = LoggerFactory.getLogger(PostgresTableEnricher.class);

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
                log.warn("[PostgresEnricher] cannot read {}: {}", indexFile, e.getMessage());
            }
        }
        if (rootPerDir.isEmpty() || names.isEmpty()) return;

        Connection conn = null;
        try {
            conn = connManager.getAnyConnection();
            Map<String, String>            typeMap = queryObjectTypes(conn, names);
            Map<String, List<ObjectNode>>  colsMap = queryColumns(conn, mapper, names);
            Map<String, List<ObjectNode>>  consMap = queryConstraints(conn, mapper, names);
            Map<String, List<ObjectNode>>  idxMap  = queryIndexes(conn, mapper, names);
            Map<String, List<ObjectNode>>  trigMap = queryTriggers(conn, mapper, names);
            Map<String, String>            viewMap = queryViewDefs(conn, names);

            // Gap: materialized views live in pg_matviews/pg_class (relkind='m'), not in
            // information_schema.tables/.views at all — queryObjectTypes()/queryViewDefs() above
            // never see them, so without this a Postgres MV was invisible (not even classified
            // as a TABLE — dbFound would be false). typeMap/viewMap entries here take precedence
            // for the matching key since a name cannot be both an ordinary view and an MV.
            Map<String, String> matViewDefs = queryMatViews(conn, names);
            for (String key : matViewDefs.keySet()) typeMap.put(key, "MATERIALIZED VIEW");
            viewMap.putAll(matViewDefs);

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
                        ViewDefinitionAnalyzer.attachTo(t, mapper, DbType.POSTGRES, name,
                                "MATERIALIZED VIEW".equals(rtype));
                    }
                }
                root.put("dbValidated", true);
                try (OutputStream out = store.write(de.getKey().resolve("api", "tables", "index.json"))) {
                    mapper.writerWithDefaultPrettyPrinter().writeValue(out, root);
                } catch (Exception e) {
                    log.warn("[PostgresEnricher] write failed: {}", e.getMessage());
                }
                log.info("[PostgresEnricher] enriched {} tables in {}", found, de.getKey().leaf());
            }
            connManager.returnToPool(conn); conn = null;
        } catch (SQLException e) {
            connManager.closeAndDiscard(conn); conn = null;
            log.warn("[PostgresEnricher] failed: {}", e.getMessage());
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

    /** IN clause of UPPER-cased bare names, matched case-insensitively via upper(...). */
    private static String upperInClause(Set<String> names) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String n : names) {
            if (!first) sb.append(",");
            sb.append("'").append(n.replace("'", "''")).append("'");
            first = false;
        }
        return sb.toString();
    }

    private static final String SYS = "('pg_catalog','information_schema')";

    private static Map<String, String> queryObjectTypes(Connection conn, Set<String> names) throws SQLException {
        Map<String, String> m = new LinkedHashMap<>();
        String sql =
            "SELECT table_schema sch, table_name nm, table_type td FROM information_schema.tables " +
            "WHERE upper(table_name) IN (" + upperInClause(names) + ") AND table_schema NOT IN " + SYS;
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String td = rs.getString("td");
                String type = (td != null && td.toUpperCase().contains("VIEW")) ? "VIEW" : "TABLE";
                m.put((rs.getString("sch") + "." + rs.getString("nm")).toUpperCase(), type);
            }
        }
        return m;
    }

    private static Map<String, List<ObjectNode>> queryColumns(Connection conn, ObjectMapper mapper, Set<String> names) throws SQLException {
        Map<String, List<ObjectNode>> m = new LinkedHashMap<>();
        String sql =
            "SELECT table_schema sch, table_name tbl, column_name col, data_type dtype, " +
            "character_maximum_length ml, numeric_scale sc, is_nullable nul, column_default ddef, ordinal_position cid " +
            "FROM information_schema.columns " +
            "WHERE upper(table_name) IN (" + upperInClause(names) + ") AND table_schema NOT IN " + SYS + " " +
            "ORDER BY table_schema, table_name, ordinal_position";
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String key = (rs.getString("sch") + "." + rs.getString("tbl")).toUpperCase();
                ObjectNode col = mapper.createObjectNode();
                col.put("columnName", rs.getString("col"));
                col.put("dataType", rs.getString("dtype"));
                int ml = rs.getInt("ml"); if (!rs.wasNull()) col.put("dataLength", ml);
                int sc = rs.getInt("sc"); if (!rs.wasNull() && sc > 0) col.put("dataScale", sc);
                col.put("nullable", "YES".equalsIgnoreCase(rs.getString("nul")));
                col.put("columnId", rs.getInt("cid"));
                String ddef = rs.getString("ddef"); if (ddef != null) col.put("dataDefault", ddef.trim());
                m.computeIfAbsent(key, k -> new ArrayList<>()).add(col);
            }
        }
        return m;
    }

    private static Map<String, List<ObjectNode>> queryConstraints(Connection conn, ObjectMapper mapper, Set<String> names) throws SQLException {
        Map<String, List<ObjectNode>> m = new LinkedHashMap<>();
        String in = upperInClause(names);
        String sql =
            "SELECT tc.table_schema sch, tc.table_name tbl, tc.constraint_name cname, tc.constraint_type ctype, " +
            "kcu.column_name col, kcu.ordinal_position pos " +
            "FROM information_schema.table_constraints tc " +
            "JOIN information_schema.key_column_usage kcu ON kcu.constraint_name = tc.constraint_name " +
            "AND kcu.table_schema = tc.table_schema AND kcu.table_name = tc.table_name " +
            "WHERE upper(tc.table_name) IN (" + in + ") AND tc.table_schema NOT IN " + SYS + " " +
            "AND tc.constraint_type IN ('PRIMARY KEY','UNIQUE','FOREIGN KEY') " +
            "ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position";
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String key = (rs.getString("sch") + "." + rs.getString("tbl")).toUpperCase();
                String type = rs.getString("ctype");
                ObjectNode c = mapper.createObjectNode();
                c.put("constraintName", rs.getString("cname"));
                c.put("constraintType", "PRIMARY KEY".equals(type) ? "P" : ("FOREIGN KEY".equals(type) ? "R" : "U"));
                c.put("columnName", rs.getString("col"));
                c.put("position", rs.getInt("pos"));
                m.computeIfAbsent(key, k -> new ArrayList<>()).add(c);
            }
        }
        // FK referenced-table targets
        String fkSql =
            "SELECT tc.constraint_name cname, ccu.table_schema reftsch, ccu.table_name reftbl " +
            "FROM information_schema.table_constraints tc " +
            "JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name " +
            "WHERE tc.constraint_type = 'FOREIGN KEY' AND upper(tc.table_name) IN (" + in + ") " +
            "AND tc.table_schema NOT IN " + SYS;
        Map<String, String> fkRefs = new LinkedHashMap<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(fkSql)) {
            while (rs.next()) fkRefs.put(rs.getString("cname"), rs.getString("reftsch") + "." + rs.getString("reftbl"));
        }
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

    private static Map<String, List<ObjectNode>> queryIndexes(Connection conn, ObjectMapper mapper, Set<String> names) throws SQLException {
        Map<String, List<ObjectNode>> m = new LinkedHashMap<>();
        String sql =
            "SELECT schemaname sch, tablename tbl, indexname idx, indexdef def FROM pg_indexes " +
            "WHERE upper(tablename) IN (" + upperInClause(names) + ") AND schemaname NOT IN " + SYS;
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String key = (rs.getString("sch") + "." + rs.getString("tbl")).toUpperCase();
                String def = rs.getString("def") != null ? rs.getString("def") : "";
                boolean unique = def.toUpperCase().contains("CREATE UNIQUE");
                // Parse the (col, col, …) list from the index definition.
                String cols = "";
                int lp = def.lastIndexOf('('), rp = def.lastIndexOf(')');
                if (lp >= 0 && rp > lp) cols = def.substring(lp + 1, rp);
                int pos = 1;
                for (String col : cols.split(",")) {
                    String cn = col.trim();
                    if (cn.isEmpty()) continue;
                    ObjectNode idx = mapper.createObjectNode();
                    idx.put("indexName", rs.getString("idx"));
                    idx.put("uniqueness", unique ? "UNIQUE" : "NONUNIQUE");
                    idx.put("columnName", cn);
                    idx.put("position", pos++);
                    m.computeIfAbsent(key, k -> new ArrayList<>()).add(idx);
                }
            }
        }
        return m;
    }

    private static Map<String, List<ObjectNode>> queryTriggers(Connection conn, ObjectMapper mapper, Set<String> names) throws SQLException {
        Map<String, List<ObjectNode>> m = new LinkedHashMap<>();
        String sql =
            "SELECT trigger_schema sch, event_object_table tbl, trigger_name trig, " +
            "event_manipulation ev, action_timing tm FROM information_schema.triggers " +
            "WHERE upper(event_object_table) IN (" + upperInClause(names) + ") AND trigger_schema NOT IN " + SYS;
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String key = (rs.getString("sch") + "." + rs.getString("tbl")).toUpperCase();
                ObjectNode tr = mapper.createObjectNode();
                tr.put("triggerName", rs.getString("trig"));
                tr.put("owner", rs.getString("sch"));
                tr.put("triggerType", rs.getString("tm"));            // BEFORE / AFTER / INSTEAD OF
                tr.put("triggeringEvent", rs.getString("ev"));        // INSERT / UPDATE / DELETE
                tr.put("status", "ENABLED");
                m.computeIfAbsent(key, k -> new ArrayList<>()).add(tr);
            }
        }
        return m;
    }

    /**
     * Gap: Postgres materialized views do not appear in information_schema.tables/.views at
     * all — they live in pg_matviews (and pg_class with relkind='m'). Without this query a
     * Postgres MV was never even detected, let alone its definition fetched.
     */
    private static Map<String, String> queryMatViews(Connection conn, Set<String> names) throws SQLException {
        Map<String, String> m = new LinkedHashMap<>();
        String sql =
            "SELECT schemaname sch, matviewname nm, definition def FROM pg_matviews " +
            "WHERE upper(matviewname) IN (" + upperInClause(names) + ") AND schemaname NOT IN " + SYS;
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String def = rs.getString("def");
                m.put((rs.getString("sch") + "." + rs.getString("nm")).toUpperCase(), def != null ? def.trim() : null);
            }
        }
        return m;
    }

    private static Map<String, String> queryViewDefs(Connection conn, Set<String> names) throws SQLException {
        Map<String, String> m = new LinkedHashMap<>();
        String sql =
            "SELECT table_schema sch, table_name nm, view_definition def FROM information_schema.views " +
            "WHERE upper(table_name) IN (" + upperInClause(names) + ") AND table_schema NOT IN " + SYS;
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String def = rs.getString("def");
                m.put((rs.getString("sch") + "." + rs.getString("nm")).toUpperCase(), def != null ? def.trim() : null);
            }
        }
        return m;
    }
}
