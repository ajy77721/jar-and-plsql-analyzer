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
 * SQL Server equivalent of TableValidationService: enriches each table entry in
 * api/tables/index.json with dbColumns / dbConstraints / dbIndexes / dbTriggers /
 * viewDefinition + dbFound/dbResolvedType, using the sys.* catalog views — so the
 * UI's table-detail panel renders the same rich data it does for Oracle.
 */
public class SqlServerTableEnricher {

    private static final Logger log = LoggerFactory.getLogger(SqlServerTableEnricher.class);

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
                log.warn("[SqlServerEnricher] cannot read {}: {}", indexFile, e.getMessage());
            }
        }
        if (rootPerDir.isEmpty() || names.isEmpty()) return;

        Connection conn = null;
        try {
            conn = connManager.getAnyConnection();
            Map<String, String>            typeMap = queryObjectTypes(conn, names);       // SCHEMA.NAME -> TABLE/VIEW
            Map<String, List<ObjectNode>>  colsMap = queryColumns(conn, mapper, names);
            Map<String, List<ObjectNode>>  consMap = queryConstraints(conn, mapper, names);
            Map<String, List<ObjectNode>>  idxMap  = queryIndexes(conn, mapper, names);
            Map<String, List<ObjectNode>>  trigMap = queryTriggers(conn, mapper, names);
            Map<String, String>            viewMap = queryViewDefs(conn, names);

            for (Map.Entry<ArtifactKey, ObjectNode> de : rootPerDir.entrySet()) {
                ObjectNode root = de.getValue();
                JsonNode tablesNode = root.get("tables");
                if (tablesNode == null || !tablesNode.isArray()) continue;
                int found = 0;
                for (JsonNode tn : tablesNode) {
                    ObjectNode t = (ObjectNode) tn;
                    String name = t.has("name") ? t.get("name").asText().toUpperCase() : null;
                    if (name == null) continue;
                    String schema = t.has("schema") ? t.get("schema").asText().toUpperCase() : null;
                    String key = resolveKey(schema, name, typeMap, colsMap);
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
                        // No MV concept in SQL Server — indexed views are ordinary views.
                        ViewDefinitionAnalyzer.attachTo(t, mapper, DbType.SQLSERVER, name, false);
                    }
                }
                root.put("dbValidated", true);
                try (OutputStream out = store.write(de.getKey().resolve("api", "tables", "index.json"))) {
                    mapper.writerWithDefaultPrettyPrinter().writeValue(out, root);
                } catch (Exception e) {
                    log.warn("[SqlServerEnricher] write failed: {}", e.getMessage());
                }
                log.info("[SqlServerEnricher] enriched {} tables in {}", found, de.getKey().leaf());
            }
            connManager.returnToPool(conn); conn = null;
        } catch (SQLException e) {
            connManager.closeAndDiscard(conn); conn = null;
            log.warn("[SqlServerEnricher] failed: {}", e.getMessage());
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

    private static Map<String, String> queryObjectTypes(Connection conn, Set<String> names) throws SQLException {
        Map<String, String> m = new LinkedHashMap<>();
        String sql = "SELECT SCHEMA_NAME(o.schema_id) sch, o.name nm, o.type_desc td " +
                "FROM sys.objects o WHERE o.type IN ('U','V') AND UPPER(o.name) IN (" + inClause(names) + ")";
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String td = rs.getString("td");
                String type = td != null && td.contains("VIEW") ? "VIEW" : "TABLE";
                m.put((rs.getString("sch") + "." + rs.getString("nm")).toUpperCase(), type);
            }
        }
        return m;
    }

    private static Map<String, List<ObjectNode>> queryColumns(Connection conn, ObjectMapper mapper, Set<String> names) throws SQLException {
        Map<String, List<ObjectNode>> m = new LinkedHashMap<>();
        String sql =
            "SELECT SCHEMA_NAME(o.schema_id) sch, o.name tbl, c.name col, ty.name dtype, " +
            "c.max_length ml, c.precision pr, c.scale sc, c.is_nullable nul, c.column_id cid, " +
            "dc.definition ddef, c.is_computed comp " +
            "FROM sys.columns c " +
            "JOIN sys.objects o ON o.object_id = c.object_id " +
            "JOIN sys.types ty ON ty.user_type_id = c.user_type_id " +
            "LEFT JOIN sys.default_constraints dc ON dc.object_id = c.default_object_id " +
            "WHERE o.type IN ('U','V') AND UPPER(o.name) IN (" + inClause(names) + ") " +
            "ORDER BY o.name, c.column_id";
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String key = (rs.getString("sch") + "." + rs.getString("tbl")).toUpperCase();
                ObjectNode col = mapper.createObjectNode();
                col.put("columnName", rs.getString("col"));
                col.put("dataType", rs.getString("dtype"));
                col.put("dataLength", rs.getInt("ml"));
                int pr = rs.getInt("pr"); if (pr > 0) col.put("dataPrecision", pr);
                int sc = rs.getInt("sc"); if (sc > 0) col.put("dataScale", sc);
                col.put("nullable", rs.getBoolean("nul"));
                col.put("columnId", rs.getInt("cid"));
                String ddef = rs.getString("ddef"); if (ddef != null) col.put("dataDefault", ddef);
                if (rs.getBoolean("comp")) col.put("computed", true);
                m.computeIfAbsent(key, k -> new ArrayList<>()).add(col);
            }
        }
        return m;
    }

    private static Map<String, List<ObjectNode>> queryConstraints(Connection conn, ObjectMapper mapper, Set<String> names) throws SQLException {
        Map<String, List<ObjectNode>> m = new LinkedHashMap<>();
        String in = inClause(names);
        // PK / UNIQUE
        String pkSql =
            "SELECT SCHEMA_NAME(o.schema_id) sch, o.name tbl, kc.name cname, kc.type_desc ctype, " +
            "col.name col, ic.key_ordinal pos " +
            "FROM sys.key_constraints kc " +
            "JOIN sys.objects o ON o.object_id = kc.parent_object_id " +
            "JOIN sys.index_columns ic ON ic.object_id = kc.parent_object_id AND ic.index_id = kc.unique_index_id " +
            "JOIN sys.columns col ON col.object_id = ic.object_id AND col.column_id = ic.column_id " +
            "WHERE UPPER(o.name) IN (" + in + ") ORDER BY o.name, kc.name, ic.key_ordinal";
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(pkSql)) {
            while (rs.next()) {
                String key = (rs.getString("sch") + "." + rs.getString("tbl")).toUpperCase();
                ObjectNode c = mapper.createObjectNode();
                c.put("constraintName", rs.getString("cname"));
                c.put("constraintType", rs.getString("ctype").contains("PRIMARY") ? "P" : "U");
                c.put("columnName", rs.getString("col"));
                c.put("position", rs.getInt("pos"));
                m.computeIfAbsent(key, k -> new ArrayList<>()).add(c);
            }
        }
        // FK
        String fkSql =
            "SELECT SCHEMA_NAME(po.schema_id) sch, po.name tbl, fk.name cname, " +
            "col.name col, fkc.constraint_column_id pos, " +
            "SCHEMA_NAME(ro.schema_id)+'.'+ro.name reftbl " +
            "FROM sys.foreign_keys fk " +
            "JOIN sys.objects po ON po.object_id = fk.parent_object_id " +
            "JOIN sys.objects ro ON ro.object_id = fk.referenced_object_id " +
            "JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id " +
            "JOIN sys.columns col ON col.object_id = fkc.parent_object_id AND col.column_id = fkc.parent_column_id " +
            "WHERE UPPER(po.name) IN (" + in + ") ORDER BY po.name, fk.name, fkc.constraint_column_id";
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(fkSql)) {
            while (rs.next()) {
                String key = (rs.getString("sch") + "." + rs.getString("tbl")).toUpperCase();
                ObjectNode c = mapper.createObjectNode();
                c.put("constraintName", rs.getString("cname"));
                c.put("constraintType", "R");   // referential / FK
                c.put("columnName", rs.getString("col"));
                c.put("position", rs.getInt("pos"));
                c.put("refConstraint", rs.getString("reftbl"));
                m.computeIfAbsent(key, k -> new ArrayList<>()).add(c);
            }
        }
        return m;
    }

    private static Map<String, List<ObjectNode>> queryIndexes(Connection conn, ObjectMapper mapper, Set<String> names) throws SQLException {
        Map<String, List<ObjectNode>> m = new LinkedHashMap<>();
        String sql =
            "SELECT SCHEMA_NAME(o.schema_id) sch, o.name tbl, i.name idx, i.is_unique uniq, " +
            "col.name col, ic.key_ordinal pos " +
            "FROM sys.indexes i " +
            "JOIN sys.objects o ON o.object_id = i.object_id " +
            "JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id " +
            "JOIN sys.columns col ON col.object_id = ic.object_id AND col.column_id = ic.column_id " +
            "WHERE i.name IS NOT NULL AND o.type IN ('U','V') AND ic.key_ordinal > 0 " +
            "AND UPPER(o.name) IN (" + inClause(names) + ") ORDER BY o.name, i.name, ic.key_ordinal";
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String key = (rs.getString("sch") + "." + rs.getString("tbl")).toUpperCase();
                ObjectNode idx = mapper.createObjectNode();
                idx.put("indexName", rs.getString("idx"));
                idx.put("uniqueness", rs.getBoolean("uniq") ? "UNIQUE" : "NONUNIQUE");
                idx.put("columnName", rs.getString("col"));
                idx.put("position", rs.getInt("pos"));
                m.computeIfAbsent(key, k -> new ArrayList<>()).add(idx);
            }
        }
        return m;
    }

    private static Map<String, List<ObjectNode>> queryTriggers(Connection conn, ObjectMapper mapper, Set<String> names) throws SQLException {
        Map<String, List<ObjectNode>> m = new LinkedHashMap<>();
        String sql =
            "SELECT SCHEMA_NAME(o.schema_id) sch, o.name tbl, tr.name trig, " +
            "CASE WHEN tr.is_instead_of_trigger=1 THEN 'INSTEAD OF' ELSE 'AFTER' END timing, " +
            "STUFF((SELECT ','+te.type_desc COLLATE DATABASE_DEFAULT FROM sys.trigger_events te " +
            "       WHERE te.object_id=tr.object_id FOR XML PATH('')),1,1,'') ev, tr.is_disabled dis " +
            "FROM sys.triggers tr " +
            "JOIN sys.objects o ON o.object_id = tr.parent_id " +
            "WHERE tr.parent_class=1 AND UPPER(o.name) IN (" + inClause(names) + ")";
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String key = (rs.getString("sch") + "." + rs.getString("tbl")).toUpperCase();
                ObjectNode tr = mapper.createObjectNode();
                tr.put("triggerName", rs.getString("trig"));
                tr.put("owner", rs.getString("sch"));
                tr.put("triggerType", rs.getString("timing"));
                tr.put("triggeringEvent", rs.getString("ev"));
                tr.put("status", rs.getBoolean("dis") ? "DISABLED" : "ENABLED");
                m.computeIfAbsent(key, k -> new ArrayList<>()).add(tr);
            }
        }
        return m;
    }

    private static Map<String, String> queryViewDefs(Connection conn, Set<String> names) throws SQLException {
        Map<String, String> m = new LinkedHashMap<>();
        String sql =
            "SELECT SCHEMA_NAME(o.schema_id) sch, o.name nm, mo.definition def " +
            "FROM sys.objects o JOIN sys.sql_modules mo ON mo.object_id = o.object_id " +
            "WHERE o.type='V' AND UPPER(o.name) IN (" + inClause(names) + ")";
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                m.put((rs.getString("sch") + "." + rs.getString("nm")).toUpperCase(), rs.getString("def"));
            }
        }
        return m;
    }
}
