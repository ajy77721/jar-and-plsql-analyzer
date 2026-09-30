package com.plsql.parser.flow;

import com.plsql.parser.PlSqlParserEngine;
import com.plsql.parser.model.CallInfo;
import com.plsql.parser.model.ParseResult;
import com.plsql.parser.model.ParsedObject;
import com.plsql.parser.model.SchemaTableInfo;
import com.plsql.parser.model.SubprogramInfo;
import com.plsql.parser.model.TableOperationInfo;

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

    /**
     * Number of trigger bodies the grammar parse could not handle, i.e. the number of times the
     * DML_PATTERN/SEQ_PATTERN text scan ran as the recovery path (Oracle wrapped sources, a
     * fragment the DB returned mid-statement …). Exposed so the regex-retirement rate of the
     * regex → grammar migration is observable.
     */
    public static final java.util.concurrent.atomic.AtomicLong TRIGGER_BODY_REGEX_FALLBACKS =
            new java.util.concurrent.atomic.AtomicLong();

    private static final java.util.concurrent.atomic.AtomicBoolean TRIGGER_FALLBACK_LOGGED =
            new java.util.concurrent.atomic.AtomicBoolean();

    /** Logs (once) and counts a use of the regex recovery path for a trigger body. */
    private void recordTriggerRegexFallback(String what) {
        long fallbackNo = TRIGGER_BODY_REGEX_FALLBACKS.incrementAndGet();
        if (TRIGGER_FALLBACK_LOGGED.compareAndSet(false, true)) {
            System.err.println("[TriggerDiscoverer] trigger body did not parse; using legacy regex "
                    + "fallback for " + what + " (#" + fallbackNo + ")");
        }
    }

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

        // SQL Server uses sys.* catalog views, not ALL_TRIGGERS.
        if (connManager.isSqlServer()) {
            return discoverSqlServer(dmlTables, knownTypes);
        }
        if (connManager.isMySQL())    return discoverMySql(dmlTables, knownTypes);
        if (connManager.isPostgres()) return discoverPostgres(dmlTables, knownTypes);
        if (connManager.isDB2())      return discoverDb2(dmlTables, knownTypes);

        Connection conn = null;
        try {
            conn = connManager.getAnyConnection();

            // Group DML tables by schema for targeted queries. Tables whose schema could not be
            // resolved are skipped rather than guessed at (a hardcoded "CUSTOMER" fallback used
            // to sit here) — ALL_TRIGGERS needs a real TABLE_OWNER to query, and a wrong guess
            // would silently miss (or misattribute) triggers on that table.
            Map<String, List<String>> bySchema = new LinkedHashMap<>();
            List<String> unresolvedSchemaTables = new ArrayList<>();
            for (var entry : dmlTables.entrySet()) {
                String schema = entry.getValue();
                if (schema == null || schema.isEmpty()) {
                    unresolvedSchemaTables.add(entry.getKey());
                    continue;
                }
                bySchema.computeIfAbsent(schema, k -> new ArrayList<>()).add(entry.getKey());
            }
            if (!unresolvedSchemaTables.isEmpty()) {
                System.err.println("[TriggerDiscoverer] Skipping trigger discovery for "
                        + unresolvedSchemaTables.size() + " table(s) with unresolved schema: "
                        + unresolvedSchemaTables);
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
            connManager.returnToPool(conn);
            conn = null;
        } catch (SQLException e) {
            connManager.closeAndDiscard(conn);
            conn = null;
            System.err.println("[TriggerDiscoverer] ALL_TRIGGERS query failed: " + e.getMessage());
            e.printStackTrace(System.err);
        } finally {
            if (conn != null) connManager.closeAndDiscard(conn);
        }
        return triggers;
    }

    /**
     * SQL Server trigger discovery via sys.triggers / sys.trigger_events / sys.sql_modules.
     * Produces the same record shape as the Oracle path so downstream writers are unchanged.
     */
    private List<Map<String, Object>> discoverSqlServer(Map<String, String> dmlTables,
                                                        Map<String, String> knownTypes) {
        List<Map<String, Object>> triggers = new ArrayList<>();
        Connection conn = null;
        try {
            conn = connManager.getAnyConnection();
            List<String> tables = new ArrayList<>(dmlTables.keySet());   // already uppercased
            for (int i = 0; i < tables.size(); i += 100) {
                List<String> batch = tables.subList(i, Math.min(i + 100, tables.size()));
                String inClause = buildInClause(batch);
                String sql =
                    "SELECT tr.name AS trigger_name, SCHEMA_NAME(o.schema_id) AS trig_schema, "
                  + "OBJECT_NAME(tr.parent_id) AS table_name, "
                  + "CASE WHEN tr.is_instead_of_trigger = 1 THEN 'INSTEAD OF' ELSE 'AFTER' END AS timing, "
                  + "STUFF((SELECT ',' + te.type_desc COLLATE DATABASE_DEFAULT "
                  + "       FROM sys.trigger_events te WHERE te.object_id = tr.object_id "
                  + "       FOR XML PATH('')), 1, 1, '') AS events, "
                  + "m.definition AS definition "
                  + "FROM sys.triggers tr "
                  + "JOIN sys.objects o ON o.object_id = tr.object_id "
                  + "LEFT JOIN sys.sql_modules m ON m.object_id = tr.object_id "
                  + "WHERE tr.is_disabled = 0 AND tr.parent_class = 1 "
                  + "AND UPPER(OBJECT_NAME(tr.parent_id)) IN (" + inClause + ")";
                connManager.incrementDbCalls();
                try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
                    while (rs.next()) {
                        String body = rs.getString("definition");
                        String tableName = rs.getString("table_name");
                        Map<String, Object> trig = new LinkedHashMap<>();
                        trig.put("name", rs.getString("trigger_name"));
                        trig.put("schema", rs.getString("trig_schema"));
                        trig.put("tableName", tableName);
                        trig.put("event", rs.getString("events"));
                        trig.put("timing", rs.getString("timing"));
                        trig.put("triggerType", rs.getString("timing"));
                        trig.put("status", "ENABLED");
                        String targetType = resolveObjectType(tableName, knownTypes);
                        if (targetType != null) trig.put("targetObjectType", targetType);
                        if (body != null && !body.isBlank()) {
                            trig.put("definition", body.trim());
                            trig.put("tableOps", extractDmlFromBody(body, knownTypes));
                            trig.put("sequences", extractSequencesFromBody(body));
                        }
                        trig.put("source", "DATABASE");
                        triggers.add(trig);
                    }
                }
            }
            System.err.println("[TriggerDiscoverer] SQL Server: found " + triggers.size()
                    + " triggers for " + dmlTables.size() + " DML tables");
            connManager.returnToPool(conn);
            conn = null;
        } catch (SQLException e) {
            connManager.closeAndDiscard(conn);
            conn = null;
            System.err.println("[TriggerDiscoverer] SQL Server trigger query failed: " + e.getMessage());
        } finally {
            if (conn != null) connManager.closeAndDiscard(conn);
        }
        return triggers;
    }

    /**
     * MySQL trigger discovery via information_schema.TRIGGERS. One row per trigger.
     */
    private List<Map<String, Object>> discoverMySql(Map<String, String> dmlTables,
                                                     Map<String, String> knownTypes) {
        List<Map<String, Object>> triggers = new ArrayList<>();
        Connection conn = null;
        try {
            conn = connManager.getAnyConnection();
            String schema = connManager.getMySqlSchema();
            List<String> tables = new ArrayList<>(dmlTables.keySet());   // already uppercased
            for (int i = 0; i < tables.size(); i += 100) {
                List<String> batch = tables.subList(i, Math.min(i + 100, tables.size()));
                String inClause = buildInClause(batch);
                String sql =
                    "SELECT TRIGGER_SCHEMA, TRIGGER_NAME, EVENT_OBJECT_TABLE, "
                  + "EVENT_MANIPULATION, ACTION_TIMING, ACTION_STATEMENT "
                  + "FROM information_schema.TRIGGERS "
                  // Scope by the referenced tables only (not a single schema/DB) so cross-database
                  // triggers on those tables are discovered too.
                  + "WHERE UPPER(EVENT_OBJECT_TABLE) IN (" + inClause + ")";
                connManager.incrementDbCalls();
                try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
                    while (rs.next()) {
                        String tableName = rs.getString("EVENT_OBJECT_TABLE");
                        String timing = rs.getString("ACTION_TIMING");
                        String body = rs.getString("ACTION_STATEMENT");
                        Map<String, Object> trig = new LinkedHashMap<>();
                        trig.put("name", rs.getString("TRIGGER_NAME"));
                        trig.put("schema", rs.getString("TRIGGER_SCHEMA"));
                        trig.put("tableName", tableName == null ? null : tableName.toUpperCase());
                        trig.put("event", rs.getString("EVENT_MANIPULATION"));
                        trig.put("timing", timing);
                        trig.put("triggerType", timing);
                        trig.put("status", "ENABLED");
                        String targetType = resolveObjectType(tableName, knownTypes);
                        if (targetType != null) trig.put("targetObjectType", targetType);
                        if (body != null && !body.isBlank()) {
                            trig.put("definition", body.trim());
                            trig.put("tableOps", extractDmlFromBody(body, knownTypes));
                            trig.put("sequences", extractSequencesFromBody(body));
                        } else {
                            trig.put("definition", null);
                        }
                        trig.put("source", "DATABASE");
                        triggers.add(trig);
                    }
                }
            }
            System.err.println("[TriggerDiscoverer] MySQL: found " + triggers.size()
                    + " triggers for " + dmlTables.size() + " DML tables");
            connManager.returnToPool(conn);
            conn = null;
        } catch (SQLException e) {
            connManager.closeAndDiscard(conn);
            conn = null;
            System.err.println("[TriggerDiscoverer] MySQL trigger query failed: " + e.getMessage());
        } finally {
            if (conn != null) connManager.closeAndDiscard(conn);
        }
        return triggers;
    }

    /**
     * PostgreSQL trigger discovery. information_schema.triggers has ONE ROW PER EVENT,
     * so rows sharing trigger_name + table are merged into one map with comma-joined events.
     * The handler function (proname + its schema) is resolved via pg_trigger -> pg_proc.
     */
    private List<Map<String, Object>> discoverPostgres(Map<String, String> dmlTables,
                                                        Map<String, String> knownTypes) {
        List<Map<String, Object>> triggers = new ArrayList<>();
        Connection conn = null;
        try {
            conn = connManager.getAnyConnection();
            String schema = connManager.getPostgresSchema();
            List<String> tables = new ArrayList<>(dmlTables.keySet());   // already uppercased
            for (int i = 0; i < tables.size(); i += 100) {
                List<String> batch = tables.subList(i, Math.min(i + 100, tables.size()));
                String inClause = buildInClause(batch);
                // Merge multi-event rows keyed by schema.trigger.table
                Map<String, Map<String, Object>> merged = new LinkedHashMap<>();
                Map<String, LinkedHashSet<String>> events = new HashMap<>();
                String sql =
                    "SELECT t.trigger_schema, t.trigger_name, t.event_object_table, "
                  + "t.event_manipulation, t.action_timing, t.action_statement, "
                  + "p.proname AS function_name, np.nspname AS function_schema "
                  + "FROM information_schema.triggers t "
                  + "LEFT JOIN pg_catalog.pg_trigger pt ON pt.tgname = t.trigger_name "
                  + "LEFT JOIN pg_catalog.pg_class c ON c.oid = pt.tgrelid AND c.relname = t.event_object_table "
                  + "LEFT JOIN pg_catalog.pg_proc p ON p.oid = pt.tgfoid "
                  + "LEFT JOIN pg_catalog.pg_namespace np ON np.oid = p.pronamespace "
                  // Scope by the analyzed DML tables only. getPostgresSchema() is unreliable
                  // (returns "public"/username, not the object schema e.g. "erp"), so do NOT
                  // filter by event_object_schema — the table-name IN clause already scopes it.
                  + "WHERE UPPER(t.event_object_table) IN (" + inClause + ")";
                connManager.incrementDbCalls();
                try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
                    while (rs.next()) {
                        String trigSchema = rs.getString("trigger_schema");
                        String trigName = rs.getString("trigger_name");
                        String tableName = rs.getString("event_object_table");
                        String upperTable = tableName == null ? null : tableName.toUpperCase();
                        String timing = rs.getString("action_timing");
                        String event = rs.getString("event_manipulation");
                        String body = rs.getString("action_statement");
                        String key = trigSchema + "|" + trigName + "|" + upperTable;

                        Map<String, Object> trig = merged.get(key);
                        if (trig == null) {
                            trig = new LinkedHashMap<>();
                            trig.put("name", trigName);
                            trig.put("schema", trigSchema);
                            trig.put("tableName", upperTable);
                            trig.put("timing", timing);
                            trig.put("triggerType", timing);
                            trig.put("status", "ENABLED");
                            String targetType = resolveObjectType(tableName, knownTypes);
                            if (targetType != null) trig.put("targetObjectType", targetType);
                            if (body != null && !body.isBlank()) {
                                trig.put("definition", body.trim());
                                trig.put("tableOps", extractDmlFromBody(body, knownTypes));
                                trig.put("sequences", extractSequencesFromBody(body));
                            } else {
                                trig.put("definition", null);
                            }
                            String fnName = rs.getString("function_name");
                            String fnSchema = rs.getString("function_schema");
                            if (fnName != null) trig.put("functionName", fnName);
                            if (fnSchema != null) trig.put("functionSchema", fnSchema);
                            trig.put("source", "DATABASE");
                            merged.put(key, trig);
                            events.put(key, new LinkedHashSet<>());
                        }
                        if (event != null && !event.isBlank()) events.get(key).add(event);
                    }
                }
                for (var e : merged.entrySet()) {
                    Map<String, Object> trig = e.getValue();
                    trig.put("event", String.join(",", events.get(e.getKey())));
                    triggers.add(trig);
                }
            }
            System.err.println("[TriggerDiscoverer] PostgreSQL: found " + triggers.size()
                    + " triggers for " + dmlTables.size() + " DML tables");
            connManager.returnToPool(conn);
            conn = null;
        } catch (SQLException e) {
            connManager.closeAndDiscard(conn);
            conn = null;
            System.err.println("[TriggerDiscoverer] PostgreSQL trigger query failed: " + e.getMessage());
        } finally {
            if (conn != null) connManager.closeAndDiscard(conn);
        }
        return triggers;
    }

    /**
     * DB2 trigger discovery via SYSCAT.TRIGGERS. Mirrors Db2TableEnricher's query.
     * TRIGEVENT I/U/D -> INSERT/UPDATE/DELETE; TRIGTIME B/A/I -> BEFORE/AFTER/INSTEAD OF.
     */
    private List<Map<String, Object>> discoverDb2(Map<String, String> dmlTables,
                                                  Map<String, String> knownTypes) {
        List<Map<String, Object>> triggers = new ArrayList<>();
        Connection conn = null;
        try {
            conn = connManager.getAnyConnection();
            String schema = connManager.getDb2Schema();
            List<String> tables = new ArrayList<>(dmlTables.keySet());   // already uppercased
            for (int i = 0; i < tables.size(); i += 100) {
                List<String> batch = tables.subList(i, Math.min(i + 100, tables.size()));
                String inClause = buildInClause(batch);
                String sql =
                    "SELECT TRIGSCHEMA, TRIGNAME, TABNAME, TABSCHEMA, TRIGEVENT, TRIGTIME, TEXT "
                  + "FROM SYSCAT.TRIGGERS "
                  // Scope by referenced tables only (not one schema) so cross-schema triggers found.
                  + "WHERE UPPER(TABNAME) IN (" + inClause + ")";
                connManager.incrementDbCalls();
                try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
                    while (rs.next()) {
                        String tableName = rs.getString("TABNAME");
                        if (tableName != null) tableName = tableName.trim();
                        String ev = rs.getString("TRIGEVENT");
                        if (ev != null) ev = ev.trim();
                        String tm = rs.getString("TRIGTIME");
                        if (tm != null) tm = tm.trim();
                        String body = rs.getString("TEXT");
                        String trigSchema = rs.getString("TRIGSCHEMA");
                        String event = "I".equals(ev) ? "INSERT"
                                : "U".equals(ev) ? "UPDATE"
                                : "D".equals(ev) ? "DELETE" : ev;
                        String timing = "B".equals(tm) ? "BEFORE"
                                : "A".equals(tm) ? "AFTER"
                                : "I".equals(tm) ? "INSTEAD OF" : tm;
                        Map<String, Object> trig = new LinkedHashMap<>();
                        String trigName = rs.getString("TRIGNAME");
                        trig.put("name", trigName == null ? null : trigName.trim());
                        trig.put("schema", trigSchema == null ? null : trigSchema.trim());
                        trig.put("tableName", tableName == null ? null : tableName.toUpperCase());
                        trig.put("event", event);
                        trig.put("timing", timing);
                        trig.put("triggerType", timing);
                        trig.put("status", "ENABLED");
                        String targetType = resolveObjectType(tableName, knownTypes);
                        if (targetType != null) trig.put("targetObjectType", targetType);
                        if (body != null && !body.isBlank()) {
                            trig.put("definition", body.trim());
                            trig.put("tableOps", extractDmlFromBody(body, knownTypes));
                            trig.put("sequences", extractSequencesFromBody(body));
                        } else {
                            trig.put("definition", null);
                        }
                        trig.put("source", "DATABASE");
                        triggers.add(trig);
                    }
                }
            }
            System.err.println("[TriggerDiscoverer] DB2: found " + triggers.size()
                    + " triggers for " + dmlTables.size() + " DML tables");
            connManager.returnToPool(conn);
            conn = null;
        } catch (SQLException e) {
            connManager.closeAndDiscard(conn);
            conn = null;
            System.err.println("[TriggerDiscoverer] DB2 trigger query failed: " + e.getMessage());
        } finally {
            if (conn != null) connManager.closeAndDiscard(conn);
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

        // Grammar first: the trigger body is real PL/SQL, so let the ANTLR parser + the
        // Oracle visitor identify the DML targets structurally. Only if the body will not
        // parse (wrapped source, a fragment the DB returned mid-statement, …) does the
        // text scan below run as a recovery path.
        List<ParsedObject> parsed = parseBodyWithGrammar(body);
        if (parsed != null) {
            Set<String> seenOps = new HashSet<>();
            for (TableOperationInfo toi : collectTableOperations(parsed)) {
                if (toi.getTableName() == null) continue;
                String operation = normalizeOp(toi.getOperation() == null
                        ? "" : toi.getOperation().toUpperCase());
                if (!DML_OPS.contains(operation) && !"SELECT".equals(operation)) continue;
                String table = toi.getSchema() != null
                        ? (toi.getSchema() + "." + toi.getTableName()).toUpperCase()
                        : toi.getTableName().toUpperCase();
                if (SKIP_TABLES.contains(table)) continue;
                if (!seenOps.add(operation + "|" + table)) continue;
                Map<String, Object> op = new LinkedHashMap<>();
                op.put("operation", operation);
                op.put("tableName", table);
                String objType = resolveObjectType(table, knownTypes);
                if (objType != null) op.put("objectType", objType);
                ops.add(op);
            }
            return ops;
        }

        recordTriggerRegexFallback("DML operations");
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

        // Grammar first — the visitor already recognises seq.NEXTVAL / schema.seq.CURRVAL off
        // the general_element parse tree and records them as CallInfo(type=SEQUENCE). The text
        // scan below is only the recovery path for bodies that will not parse.
        List<ParsedObject> parsed = parseBodyWithGrammar(body);
        if (parsed != null) {
            Set<String> seenSeqs = new HashSet<>();
            for (CallInfo ci : collectCalls(parsed)) {
                if (!"SEQUENCE".equals(ci.getType()) || ci.getPackageName() == null) continue;
                String seqName = ci.getPackageName().toUpperCase();
                String schema = ci.getSchema() != null ? ci.getSchema().toUpperCase() : null;
                String operation = ci.getName() == null ? null : ci.getName().toUpperCase();
                if (operation == null) continue;
                String key = (schema != null ? schema + "." + seqName : seqName) + "|" + operation;
                if (!seenSeqs.add(key)) continue;
                Map<String, Object> seq = new LinkedHashMap<>();
                seq.put("sequenceName", seqName);
                if (schema != null) seq.put("schema", schema);
                seq.put("operation", operation);
                seqs.add(seq);
            }
            return seqs;
        }

        recordTriggerRegexFallback("sequence references");
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

    /**
     * Parse a trigger body with the real Oracle grammar. Returns the parsed objects, or
     * {@code null} when the body does not parse cleanly (in which case callers fall back to
     * their text scan rather than silently reporting nothing).
     */
    private List<ParsedObject> parseBodyWithGrammar(String body) {
        if (body == null || body.isBlank()) return null;
        try {
            // DBA_TRIGGERS.TRIGGER_BODY is only the block ([DECLARE] BEGIN … END;), which is a
            // valid *statement* but not a schema object, so the visitor produces no ParsedObject
            // for it on its own. Wrapping it as the body of a synthetic procedure makes it a
            // parseable unit without altering any of the statements inside it (a nested block is
            // legal PL/SQL). A body that already carries its own CREATE header is parsed as-is.
            String source = body.stripLeading().toUpperCase().startsWith("CREATE")
                    ? body
                    : "CREATE OR REPLACE PROCEDURE JAR$TRIGGER_BODY IS\nBEGIN\n" + body + "\nEND;\n";
            ParseResult result = new PlSqlParserEngine().parseContent(source, "trigger_body.sql");
            if (result == null || result.getObjects() == null || result.getObjects().isEmpty()) {
                return null;
            }
            if (result.getErrors() != null && !result.getErrors().isEmpty()) return null;
            return result.getObjects();
        } catch (Exception | StackOverflowError e) {
            return null;
        }
    }

    private List<TableOperationInfo> collectTableOperations(List<ParsedObject> objects) {
        List<TableOperationInfo> all = new ArrayList<>();
        for (ParsedObject po : objects) {
            if (po.getTableOperations() != null) all.addAll(po.getTableOperations());
            if (po.getSubprograms() != null) {
                for (SubprogramInfo sub : po.getSubprograms()) {
                    if (sub.getTableOperations() != null) all.addAll(sub.getTableOperations());
                }
            }
        }
        return all;
    }

    private List<CallInfo> collectCalls(List<ParsedObject> objects) {
        List<CallInfo> all = new ArrayList<>();
        for (ParsedObject po : objects) {
            if (po.getCalls() != null) all.addAll(po.getCalls());
            if (po.getSubprograms() != null) {
                for (SubprogramInfo sub : po.getSubprograms()) {
                    if (sub.getCalls() != null) all.addAll(sub.getCalls());
                }
            }
        }
        return all;
    }

    private String normalizeOp(String raw) {
        if (raw.startsWith("INSERT")) return "INSERT";
        if (raw.startsWith("DELETE")) return "DELETE";
        if (raw.startsWith("MERGE")) return "MERGE";
        if (raw.startsWith("SELECT")) return "SELECT";
        return raw;
    }
}
