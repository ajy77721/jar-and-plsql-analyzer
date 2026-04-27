package com.plsql.parser.flow;

import com.plsql.parser.model.SchemaTableInfo;

import java.io.Reader;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TriggerDiscoverer {

    private static final Set<String> DML_OPS = Set.of("INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE");
    private static final Set<String> SKIP_TABLES = Set.of("DUAL", "SET", "INTO", "VALUES", "WHERE", "TABLE");
    private static final Pattern DML_PATTERN = Pattern.compile(
            "(?i)\\b(INSERT\\s+INTO|UPDATE|DELETE\\s+FROM|MERGE\\s+INTO|SELECT\\s+[^;]*?FROM)\\s+([A-Z_][A-Z0-9_\\.]*)",
            Pattern.MULTILINE);
    private static final Pattern SEQ_PATTERN = Pattern.compile(
            "(?i)([A-Z_][A-Z0-9_]*(?:\\.[A-Z_][A-Z0-9_]*)?)\\.(NEXTVAL|CURRVAL)\\b");

    private final DbConnectionManager connManager;
    private final SchemaResolver schemaResolver;

    public TriggerDiscoverer(DbConnectionManager connManager, SchemaResolver schemaResolver) {
        this.connManager = connManager;
        this.schemaResolver = schemaResolver;
    }

    public List<Map<String, Object>> discover(Map<String, SchemaTableInfo> tableInfoMap) {
        List<Map<String, Object>> triggers = new ArrayList<>();
        if (connManager == null) return triggers;

        Map<String, String> dmlTables = collectDmlTables(tableInfoMap);
        if (dmlTables.isEmpty()) return triggers;

        // Build objectType lookup from tableInfoMap for fast resolution
        Map<String, String> knownTypes = new HashMap<>();
        for (var entry : tableInfoMap.entrySet()) {
            SchemaTableInfo info = entry.getValue();
            if (info.getObjectType() != null) {
                knownTypes.put(info.getTableName().toUpperCase(), info.getObjectType());
            }
        }

        try {
            Connection conn = connManager.getAnyConnection();

            // Group DML tables by schema for targeted queries
            Map<String, List<String>> bySchema = new LinkedHashMap<>();
            for (var entry : dmlTables.entrySet()) {
                String schema = entry.getValue();
                if (schema == null || schema.isEmpty()) schema = "CUSTOMER";
                bySchema.computeIfAbsent(schema, k -> new ArrayList<>()).add(entry.getKey());
            }

            List<Map<String, String>> metaRows = new ArrayList<>();

            // Query each schema separately — more targeted, avoids cross-schema visibility issues
            for (var schemaEntry : bySchema.entrySet()) {
                String schema = schemaEntry.getKey();
                List<String> tables = schemaEntry.getValue();

                // Chunk into batches of 100 to avoid ORA-01795 (max 1000 expressions in IN)
                for (int i = 0; i < tables.size(); i += 100) {
                    List<String> batch = tables.subList(i, Math.min(i + 100, tables.size()));
                    String inClause = buildInClause(batch);

                    String metaSql = "SELECT t.TRIGGER_NAME, t.OWNER, t.TABLE_OWNER, t.TABLE_NAME, "
                            + "t.TRIGGERING_EVENT, t.TRIGGER_TYPE, t.STATUS, t.DESCRIPTION, t.TRIGGER_BODY "
                            + "FROM ALL_TRIGGERS t "
                            + "WHERE t.TABLE_OWNER = '" + schema.replace("'", "''") + "' "
                            + "AND t.TABLE_NAME IN (" + inClause + ") "
                            + "AND t.STATUS = 'ENABLED' "
                            + "ORDER BY t.TABLE_NAME, t.TRIGGER_NAME";

                    connManager.incrementDbCalls();
                    try (Statement stmt = conn.createStatement();
                         ResultSet rs = stmt.executeQuery(metaSql)) {
                        while (rs.next()) {
                            Map<String, String> row = new LinkedHashMap<>();
                            row.put("TRIGGER_NAME", rs.getString("TRIGGER_NAME"));
                            row.put("OWNER", rs.getString("OWNER"));
                            row.put("TABLE_OWNER", rs.getString("TABLE_OWNER"));
                            row.put("TABLE_NAME", rs.getString("TABLE_NAME"));
                            row.put("TRIGGERING_EVENT", rs.getString("TRIGGERING_EVENT"));
                            row.put("TRIGGER_TYPE", rs.getString("TRIGGER_TYPE"));
                            row.put("STATUS", rs.getString("STATUS"));
                            row.put("DESCRIPTION", rs.getString("DESCRIPTION"));
                            row.put("TRIGGER_BODY", readLongColumn(rs, "TRIGGER_BODY"));
                            metaRows.add(row);
                        }
                    }
                    System.err.println("[TriggerDiscoverer] Schema " + schema + " batch "
                            + (i / 100 + 1) + " (" + batch.size() + " tables): "
                            + metaRows.size() + " triggers so far");
                }
            }

            System.err.println("[TriggerDiscoverer] Phase 1: found " + metaRows.size()
                    + " enabled triggers for " + dmlTables.size() + " DML tables across "
                    + bySchema.size() + " schemas");
            if (metaRows.isEmpty() && !dmlTables.isEmpty()) {
                List<String> sampleNames = new ArrayList<>(dmlTables.keySet());
                if (sampleNames.size() > 5) sampleNames = sampleNames.subList(0, 5);
                Map.Entry<String, List<String>> firstSchema = bySchema.entrySet().iterator().next();
                System.err.println("[TriggerDiscoverer] No triggers found. Schemas queried: " + bySchema.keySet()
                        + " Sample tables: " + sampleNames);
                System.err.println("[TriggerDiscoverer] Connection user: " + conn.getMetaData().getUserName());
            }

            // Phase 2: For each trigger, fetch full source from ALL_SOURCE / DBMS_METADATA (fallback to TRIGGER_BODY)
            int withBody = 0;
            for (Map<String, String> meta : metaRows) {
                String trigOwner = meta.get("OWNER");
                String trigName = meta.get("TRIGGER_NAME");
                String sourceBody = fetchTriggerSource(conn, trigOwner, trigName);
                if (sourceBody == null || sourceBody.isBlank()) {
                    sourceBody = meta.get("TRIGGER_BODY");
                }
                if (sourceBody != null && !sourceBody.isBlank()) {
                    withBody++;
                } else {
                    System.err.println("[TriggerDiscoverer] WARNING: No body for trigger "
                            + trigOwner + "." + trigName
                            + " (ALL_SOURCE empty, TRIGGER_BODY null, DBMS_METADATA failed)");
                }
                triggers.add(mapRowFromMeta(meta, sourceBody, knownTypes));
            }

            System.err.println("[TriggerDiscoverer] Phase 2: built " + triggers.size()
                    + " trigger records (" + withBody + " with body, "
                    + (triggers.size() - withBody) + " without)");
        } catch (SQLException e) {
            System.err.println("[TriggerDiscoverer] ALL_TRIGGERS query failed: " + e.getMessage());
            e.printStackTrace(System.err);
        }
        return triggers;
    }

    private String fetchTriggerSource(Connection conn, String owner, String triggerName) {
        // Try 1: ALL_SOURCE
        try {
            String sql = "SELECT TEXT FROM ALL_SOURCE WHERE OWNER = ? AND NAME = ? AND TYPE = 'TRIGGER' ORDER BY LINE";
            connManager.incrementDbCalls();
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, owner);
                ps.setString(2, triggerName);
                try (ResultSet rs = ps.executeQuery()) {
                    StringBuilder sb = new StringBuilder();
                    while (rs.next()) {
                        sb.append(rs.getString("TEXT"));
                    }
                    if (sb.length() > 0) return sb.toString();
                }
            }
        } catch (SQLException e) {
            System.err.println("[TriggerDiscoverer] ALL_SOURCE failed for "
                    + owner + "." + triggerName + ": " + e.getMessage());
        }

        // Try 2: DBMS_METADATA.GET_DDL
        try {
            String sql = "SELECT DBMS_METADATA.GET_DDL('TRIGGER', ?, ?) FROM DUAL";
            connManager.incrementDbCalls();
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, triggerName);
                ps.setString(2, owner);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        String ddl = rs.getString(1);
                        if (ddl != null && !ddl.isBlank()) return ddl;
                    }
                }
            }
        } catch (SQLException e) {
            System.err.println("[TriggerDiscoverer] DBMS_METADATA.GET_DDL failed for "
                    + owner + "." + triggerName + ": " + e.getMessage());
        }

        return null;
    }

    private static String readLongColumn(ResultSet rs, String columnName) {
        try {
            Reader reader = rs.getCharacterStream(columnName);
            if (reader == null) return null;
            StringBuilder sb = new StringBuilder(4096);
            char[] buf = new char[4096];
            int len;
            while ((len = reader.read(buf)) != -1) {
                sb.append(buf, 0, len);
            }
            reader.close();
            return sb.length() > 0 ? sb.toString() : null;
        } catch (Exception e) {
            try {
                return rs.getString(columnName);
            } catch (Exception e2) {
                return null;
            }
        }
    }

    private Map<String, String> collectDmlTables(Map<String, SchemaTableInfo> tableInfoMap) {
        Map<String, String> dmlTables = new LinkedHashMap<>();
        for (var entry : tableInfoMap.entrySet()) {
            SchemaTableInfo info = entry.getValue();
            boolean hasDml = false;
            for (String op : info.getOperations()) {
                if (DML_OPS.contains(op.toUpperCase())) { hasDml = true; break; }
            }
            if (!hasDml) continue;
            dmlTables.put(info.getTableName().toUpperCase(),
                    info.getSchema() != null ? info.getSchema().toUpperCase() : null);
        }
        return dmlTables;
    }

    private String buildInClause(List<String> tableNames) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < tableNames.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append("'").append(tableNames.get(i).replace("'", "''")).append("'");
        }
        return sb.toString();
    }

    private Map<String, Object> mapRowFromMeta(Map<String, String> meta, String sourceBody,
                                                Map<String, String> knownTypes) {
        Map<String, Object> trig = new LinkedHashMap<>();
        trig.put("name", meta.get("TRIGGER_NAME"));
        trig.put("schema", meta.get("TABLE_OWNER"));
        String tableName = meta.get("TABLE_NAME");
        trig.put("tableName", tableName);
        trig.put("event", meta.get("TRIGGERING_EVENT"));
        trig.put("triggerType", meta.get("TRIGGER_TYPE"));
        trig.put("status", meta.get("STATUS"));

        // Resolve target table's objectType
        String targetType = resolveObjectType(tableName, knownTypes);
        if (targetType != null) trig.put("targetObjectType", targetType);

        String desc = meta.get("DESCRIPTION");
        if (desc != null) {
            String timing = desc.trim().split("\\s+")[0].toUpperCase();
            if ("BEFORE".equals(timing) || "AFTER".equals(timing) || "INSTEAD".equals(timing)) {
                trig.put("timing", "INSTEAD".equals(timing) ? "INSTEAD OF" : timing);
            }
        }
        if (trig.get("timing") == null) {
            String tt = (String) trig.get("triggerType");
            if (tt != null) {
                String upper = tt.toUpperCase();
                if (upper.startsWith("BEFORE")) trig.put("timing", "BEFORE");
                else if (upper.startsWith("AFTER")) trig.put("timing", "AFTER");
                else if (upper.startsWith("INSTEAD")) trig.put("timing", "INSTEAD OF");
            }
        }

        String body = (sourceBody != null && !sourceBody.isBlank()) ? sourceBody : null;
        if (body != null) {
            trig.put("definition", body.trim());
            trig.put("tableOps", extractDmlFromBody(body, knownTypes));
            trig.put("sequences", extractSequencesFromBody(body));
        }
        trig.put("source", "DATABASE");
        return trig;
    }

    private String resolveObjectType(String tableName, Map<String, String> knownTypes) {
        if (tableName == null) return null;
        String upper = tableName.toUpperCase();
        String known = knownTypes.get(upper);
        if (known != null) return known;
        if (schemaResolver != null) return schemaResolver.resolveObjectType(upper);
        return null;
    }

    public List<Map<String, Object>> extractDmlFromBody(String body, Map<String, String> knownTypes) {
        List<Map<String, Object>> ops = new ArrayList<>();
        if (body == null) return ops;

        Matcher m = DML_PATTERN.matcher(body);
        Set<String> seen = new HashSet<>();
        while (m.find()) {
            String operation = normalizeOp(m.group(1).trim().toUpperCase());
            String table = m.group(2).trim().toUpperCase();
            if (SKIP_TABLES.contains(table)) continue;
            String key = operation + "|" + table;
            if (seen.add(key)) {
                Map<String, Object> op = new LinkedHashMap<>();
                op.put("operation", operation);
                op.put("tableName", table);
                String objType = resolveObjectType(table, knownTypes);
                if (objType != null) op.put("objectType", objType);
                ops.add(op);
            }
        }
        return ops;
    }

    public List<Map<String, Object>> extractSequencesFromBody(String body) {
        List<Map<String, Object>> seqs = new ArrayList<>();
        if (body == null) return seqs;

        Matcher m = SEQ_PATTERN.matcher(body);
        Set<String> seen = new HashSet<>();
        while (m.find()) {
            String seqRef = m.group(1).trim().toUpperCase();
            String operation = m.group(2).trim().toUpperCase();
            String key = seqRef + "|" + operation;
            if (seen.add(key)) {
                String schema = null;
                String seqName = seqRef;
                int dot = seqRef.indexOf('.');
                if (dot > 0) {
                    schema = seqRef.substring(0, dot);
                    seqName = seqRef.substring(dot + 1);
                }
                Map<String, Object> seq = new LinkedHashMap<>();
                seq.put("sequenceName", seqName);
                if (schema != null) seq.put("schema", schema);
                seq.put("operation", operation);
                seqs.add(seq);
            }
        }
        return seqs;
    }

    private String normalizeOp(String raw) {
        if (raw.startsWith("INSERT")) return "INSERT";
        if (raw.startsWith("DELETE")) return "DELETE";
        if (raw.startsWith("MERGE")) return "MERGE";
        if (raw.startsWith("SELECT")) return "SELECT";
        return raw;
    }
}
