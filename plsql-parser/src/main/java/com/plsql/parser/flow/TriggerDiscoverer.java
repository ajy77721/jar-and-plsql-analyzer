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
    private static final Set<String> SKIP_TABLES = Set.of("DUAL", "SET", "INTO", "VALUES", "WHERE", "TABLE",
            // SQL Server's trigger transition pseudo-tables. INSERTED / DELETED are the trigger's
            // in-memory delta tables (the affected rows), not schema objects — a body that selects
            // from them (`SELECT … FROM deleted`) was recorded here as a real SELECT on a table
            // literally named DELETED, which the estate trigger facet then showed as a "touched"
            // table. Filtered at this source (the funnel both the grammar and regex body scans pass
            // through) rather than only downstream.
            "INSERTED", "DELETED");
    private static final Pattern DML_PATTERN = Pattern.compile(
            "(?i)\\b(INSERT\\s+INTO|UPDATE|DELETE\\s+FROM|MERGE\\s+INTO|SELECT\\s+[^;]*?FROM)\\s+([A-Z_][A-Z0-9_\\.]*)",
            Pattern.MULTILINE);
    private static final Pattern SEQ_PATTERN = Pattern.compile(
            "(?i)([A-Z_][A-Z0-9_]*(?:\\.[A-Z_][A-Z0-9_]*)?)\\.(NEXTVAL|CURRVAL)\\b");
    /**
     * The {@code TRIGGER <name> … ON <table> …} header Oracle's ALL_SOURCE prepends to a trigger
     * body (see {@link #normalizeTriggerBody}). Matches the header only, up to the optional
     * {@code REFERENCING} / {@code FOR EACH ROW} / {@code WHEN (…)} clauses; the body follows.
     */
    private static final Pattern TRIGGER_HEADER = Pattern.compile(
            "(?is)^\\s*TRIGGER\\s+\\S+.*?\\bON\\b\\s+[\\w$#.]+"
            + "(?:\\s+REFERENCING\\s+\\S+\\s+\\S+)*"
            + "(?:\\s+FOR\\s+EACH\\s+ROW)?"
            + "(?:\\s+WHEN\\s*\\([^)]*\\))?\\s*");

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
    /**
     * Optional: routes trigger bodies through the persistent source cache (fetch-free on a
     * re-crawl of an unchanged trigger). Null in unit-test / standalone-trigger paths, where the
     * batched pull still runs but nothing is cached.
     */
    private final SourceDownloader downloader;

    public TriggerDiscoverer(DbConnectionManager connManager, SchemaResolver schemaResolver) {
        this(connManager, schemaResolver, null);
    }

    public TriggerDiscoverer(DbConnectionManager connManager, SchemaResolver schemaResolver,
                             SourceDownloader downloader) {
        this.connManager = connManager;
        this.schemaResolver = schemaResolver;
        this.downloader = downloader;
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

            // Phase 2: batched trigger-source pull THROUGH the source-cache chain. The old path
            // fired one ALL_SOURCE (then DBMS_METADATA) query PER trigger — the N+1 that bypassed
            // the cross-run source cache entirely. Now: cache hits first, then ONE batched
            // ALL_SOURCE query for every miss, then a per-trigger DBMS_METADATA fallback only for
            // the rare body still absent (wrapped/legacy), then the ALL_TRIGGERS TRIGGER_BODY.
            Map<String, String> sourceByKey = fetchTriggerSourcesBatched(conn, metaRows);
            int withBody = 0;
            for (Map<String, String> meta : metaRows) {
                String trigOwner = meta.get("OWNER");
                String trigName = meta.get("TRIGGER_NAME");
                String key = triggerKey(trigOwner, trigName);
                String sourceBody = sourceByKey.get(key);
                if (sourceBody == null || sourceBody.isBlank()) {
                    // Rare fallback: not in the cache and not returned by the batched pull.
                    sourceBody = fetchTriggerSourceDbmsMetadata(conn, trigOwner, trigName);
                }
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
                            trig.put("calls", extractCallsFromBody(body));
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
                            trig.put("calls", extractCallsFromBody(body));
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
                            trig.put("calls", extractCallsFromBody(body));
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

    private static String triggerKey(String owner, String name) {
        return (owner == null ? "" : owner.toUpperCase()) + "|"
                + (name == null ? "" : name.toUpperCase());
    }

    /**
     * Fetch trigger bodies for the whole discovered set with as few queries as possible:
     * <ol>
     *   <li>serve each {@code owner.trigger} from the persistent source cache when present,</li>
     *   <li>pull every remaining body in ONE batched ALL_SOURCE query per chunk of 100 pairs
     *       ({@code (OWNER, NAME) IN ((?,?),(?,?))}), and</li>
     *   <li>write the freshly pulled bodies back to the source cache.</li>
     * </ol>
     * Returns a map keyed by {@link #triggerKey}. Never throws — a failed chunk leaves its
     * triggers for the per-trigger DBMS_METADATA fallback / TRIGGER_BODY.
     */
    private Map<String, String> fetchTriggerSourcesBatched(Connection conn,
                                                           List<Map<String, String>> metaRows) {
        Map<String, String> out = new LinkedHashMap<>();
        List<String[]> misses = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (Map<String, String> meta : metaRows) {
            String owner = meta.get("OWNER");
            String name = meta.get("TRIGGER_NAME");
            if (name == null || owner == null) continue;
            String key = triggerKey(owner, name);
            if (!seen.add(key)) continue;
            if (downloader != null) {
                String cached = downloader.getCachedTriggerSource(owner, name);
                if (cached != null && !cached.isBlank()) {
                    out.put(key, cached);
                    continue;
                }
            }
            misses.add(new String[]{owner, name});
        }
        if (misses.isEmpty()) return out;

        for (int i = 0; i < misses.size(); i += 100) {
            List<String[]> chunk = misses.subList(i, Math.min(i + 100, misses.size()));
            StringBuilder in = new StringBuilder();
            for (int j = 0; j < chunk.size(); j++) {
                if (j > 0) in.append(",");
                in.append("(?,?)");
            }
            String sql = "SELECT OWNER, NAME, LINE, TEXT FROM ALL_SOURCE "
                    + "WHERE TYPE = 'TRIGGER' AND (OWNER, NAME) IN (" + in + ") "
                    + "ORDER BY OWNER, NAME, LINE";
            Map<String, StringBuilder> bodies = new LinkedHashMap<>();
            try {
                connManager.incrementDbCalls();
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    int p = 1;
                    for (String[] pair : chunk) {
                        ps.setString(p++, pair[0]);
                        ps.setString(p++, pair[1]);
                    }
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            String key = triggerKey(rs.getString("OWNER"), rs.getString("NAME"));
                            bodies.computeIfAbsent(key, k -> new StringBuilder())
                                    .append(rs.getString("TEXT"));
                        }
                    }
                }
            } catch (SQLException e) {
                System.err.println("[TriggerDiscoverer] batched ALL_SOURCE trigger pull failed: "
                        + e.getMessage());
            }
            for (String[] pair : chunk) {
                String key = triggerKey(pair[0], pair[1]);
                StringBuilder sb = bodies.get(key);
                if (sb == null || sb.length() == 0) continue;
                String src = sb.toString();
                out.put(key, src);
                if (downloader != null) downloader.storeTriggerSource(pair[0], pair[1], src);
            }
        }
        System.err.println("[TriggerDiscoverer] Phase 2: batched source pull for "
                + misses.size() + " trigger(s), " + out.size() + " served from cache/DB");
        return out;
    }

    /**
     * Last-resort per-trigger DBMS_METADATA fallback for a body the batched ALL_SOURCE pull did
     * not return (wrapped source, or a trigger whose ALL_SOURCE rows are absent). Rare by design —
     * the batched pull above is the normal path.
     */
    private String fetchTriggerSourceDbmsMetadata(Connection conn, String owner, String triggerName) {
        try {
            String sql = "SELECT DBMS_METADATA.GET_DDL('TRIGGER', ?, ?) FROM DUAL";
            connManager.incrementDbCalls();
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, triggerName);
                ps.setString(2, owner);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        String ddl = rs.getString(1);
                        if (ddl != null && !ddl.isBlank()) {
                            if (downloader != null) downloader.storeTriggerSource(owner, triggerName, ddl);
                            return ddl;
                        }
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
            trig.put("calls", extractCallsFromBody(body));
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
     * The outbound routine calls made by a trigger body — the cross-estate edge from a trigger
     * into the procedures/functions it invokes (and, transitively, the globals they touch).
     *
     * <p>Grammar-first: the body is a real routine body, so it is parsed by the SAME dialect parser
     * the rest of the crawl uses and its {@link CallInfo}s are read off the result. There is no
     * text-scan recovery path here on purpose — a regex over a trigger body cannot distinguish a
     * call from a keyword, and a wrong edge is worse than an absent one. A body that does not parse
     * (or a dialect whose parser reports errors on it) yields an empty list, honestly.
     *
     * <p>The returned maps have the same shape as a node detail's {@code calls[]}
     * ({@code target / packageName / objectName / type / schema / lines / count}), so the crawler
     * can turn them straight into {@link CallInfo}s for the trigger node and the estate facet's
     * {@code bodyCalls} picks them up with no re-parse at read time.
     */
    public List<Map<String, Object>> extractCallsFromBody(String body) {
        return extractCallsFromBody(body, connManager == null ? null
                : com.plsqlanalyzer.config.DbType.fromString(connManager.getDbType()));
    }

    /** As {@link #extractCallsFromBody(String)} but with the dialect supplied explicitly, so the
     *  extraction can be exercised without a live connection. */
    List<Map<String, Object>> extractCallsFromBody(String body, com.plsqlanalyzer.config.DbType dialect) {
        List<Map<String, Object>> out = new ArrayList<>();
        if (body == null || body.isBlank()) return out;
        List<ParsedObject> parsed = parseBodyWithDialectGrammar(body, dialect);
        if (parsed == null) return out;
        Set<String> seen = new HashSet<>();
        for (CallInfo ci : collectCalls(parsed)) {
            String name = ci.getName();
            if (name == null || name.isBlank()) continue;
            String type = ci.getType() != null ? ci.getType() : "EXTERNAL";
            // Built-ins (DBMS_OUTPUT.PUT_LINE, SQL Server NEXTVAL shims) and sequence pseudo-calls
            // are not cross-estate edges — the call graph does not follow them.
            if ("BUILTIN".equalsIgnoreCase(type) || "SEQUENCE".equalsIgnoreCase(type)) continue;
            String pkg = ci.getPackageName();
            String schema = ci.getSchema();
            String objectName = (pkg != null && !pkg.isBlank()) ? pkg + "." + name : name;
            String key = (schema != null ? schema.toUpperCase() : "") + "|" + objectName.toUpperCase();
            if (!seen.add(key)) continue;
            Map<String, Object> c = new LinkedHashMap<>();
            c.put("target", objectName);
            c.put("packageName", pkg);
            c.put("objectName", objectName);
            c.put("name", name);
            c.put("type", type);
            c.put("schema", schema);
            List<Integer> lines = new ArrayList<>();
            if (ci.getLine() > 0) lines.add(ci.getLine());
            c.put("lines", lines);
            c.put("count", lines.size());
            out.add(c);
        }
        return out;
    }

    /**
     * Parse a trigger body with the DIALECT's own grammar (SQL Server / MySQL / DB2 / PostgreSQL),
     * falling back to the Oracle PL/SQL grammar when the dialect is Oracle or unknown. Used for
     * call extraction, where a dialect-specific body (e.g. T-SQL's {@code EXEC dbo.usp_x @id}) can
     * only be recognised by that dialect's parser. Returns null when the body does not parse.
     */
    private List<ParsedObject> parseBodyWithDialectGrammar(String body, com.plsqlanalyzer.config.DbType dialect) {
        if (body == null || body.isBlank()) return null;
        if (dialect != null && dialect != com.plsqlanalyzer.config.DbType.ORACLE) {
            com.plsql.parser.multidb.StoredProcedureParser parser =
                    com.plsql.parser.multidb.StoredProcedureParserFactory.get(dialect);
            if (parser != null) {
                try {
                    ParseResult result = parser.parse(body, "TRIGGER_BODY", null);
                    if (result == null || result.getObjects() == null || result.getObjects().isEmpty()) return null;
                    if (result.getErrors() != null && !result.getErrors().isEmpty()) return null;
                    return result.getObjects();
                } catch (Exception | StackOverflowError e) {
                    return null;
                }
            }
        }
        return parseBodyWithGrammar(body);
    }

    /**
     * Parse a trigger body with the real Oracle grammar. Returns the parsed objects, or
     * {@code null} when the body does not parse cleanly (in which case callers fall back to
     * their text scan rather than silently reporting nothing).
     */
    private List<ParsedObject> parseBodyWithGrammar(String body) {
        if (body == null || body.isBlank()) return null;
        String normalized = normalizeTriggerBody(body);
        if (normalized == null || normalized.isBlank()) return null;
        try {
            // DBA_TRIGGERS.TRIGGER_BODY is only the block ([DECLARE] BEGIN … END;), which is a
            // valid *statement* but not a schema object, so the visitor produces no ParsedObject
            // for it on its own. Wrapping it as the body of a synthetic procedure makes it a
            // parseable unit without altering any of the statements inside it (a nested block is
            // legal PL/SQL). A body that already carries its own CREATE header is parsed as-is.
            String source = normalized.stripLeading().toUpperCase().startsWith("CREATE")
                    ? normalized
                    : "CREATE OR REPLACE PROCEDURE JAR$TRIGGER_BODY IS\nBEGIN\n" + normalized + "\nEND;\n";
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

    /**
     * Normalize a raw catalog trigger body into a shape the dialect parser accepts, before it is
     * handed to {@link #parseBodyWithGrammar}.
     *
     * <p>The body shape a LIVE catalog returns differs from the idealized fixture. Oracle's
     * {@code ALL_SOURCE} stores a trigger WITHOUT the leading {@code CREATE OR REPLACE}: the text
     * begins at the {@code TRIGGER} keyword and carries the whole header, e.g.
     * <pre>
     * TRIGGER erp_trg_so_stmt_ad
     * AFTER DELETE ON erp_sales_order
     * BEGIN
     *   erp_usp_write_audit_trail('SO_DELETE', …);
     * END erp_trg_so_stmt_ad;
     * </pre>
     * Fed to the parser verbatim — or wrapped in the synthetic procedure below — that fails at the
     * {@code TRIGGER} keyword, so the body yielded no calls / DML / sequences at all. Strip the
     * header and keep the block.
     *
     * <p>Shapes handled:
     * <ul>
     *   <li>a full {@code CREATE …} DDL (DBMS_METADATA fallback) — parsed as-is, unchanged;</li>
     *   <li>the Oracle {@code ALL_SOURCE} {@code TRIGGER …header… BEGIN … END} — header stripped,
     *       the block kept;</li>
     *   <li>a bare {@code [DECLARE] BEGIN … END} block — already valid, returned unchanged (no
     *       double-wrap);</li>
     *   <li>bare statements with no enclosing block — wrapped in a synthetic {@code BEGIN … END}
     *       (the same technique the dynamic-SQL recursive parse uses);</li>
     *   <li>a {@code NULL} / empty body — left to yield zero calls honestly.</li>
     * </ul>
     */
    static String normalizeTriggerBody(String body) {
        if (body == null) return null;
        String text = body.strip();
        if (text.isEmpty()) return text;

        // Full DDL already carries its own CREATE header — the existing path parses it as-is.
        if (startsWithKeyword(text, "CREATE")) return text;

        // Oracle ALL_SOURCE trigger text: "TRIGGER <name> … ON <table> … <block>".
        if (startsWithKeyword(text, "TRIGGER")) {
            int block = indexOfTopLevelBegin(text);
            if (block >= 0) return text.substring(block);
            // Header with no BEGIN (e.g. a bare CALL body): drop the header, wrap the remainder.
            String rest = stripTriggerHeader(text);
            return rest.isBlank() ? text : "BEGIN\n" + rest + "\nEND;";
        }

        // A real block (BEGIN … or DECLARE … BEGIN …) is already parseable — leave it alone.
        if (indexOfTopLevelBegin(text) >= 0) return text;

        // Bare statements with no enclosing block: synthesize one.
        return "BEGIN\n" + text + "\nEND;";
    }

    /** Keyword match at index 0 with a word boundary (so {@code TRIGGER} ≠ {@code TRIGGERED}). */
    private static boolean startsWithKeyword(String text, String keyword) {
        return text.regionMatches(true, 0, keyword, 0, keyword.length())
                && (text.length() == keyword.length()
                    || !Character.isLetterOrDigit(text.charAt(keyword.length())));
    }

    /** Keyword match at {@code idx} with a word boundary on both sides. */
    private static boolean keywordAt(String text, int idx, String keyword) {
        int end = idx + keyword.length();
        if (end > text.length()) return false;
        if (!text.regionMatches(true, idx, keyword, 0, keyword.length())) return false;
        boolean leftOk = idx == 0 || !Character.isLetterOrDigit(text.charAt(idx - 1))
                && text.charAt(idx - 1) != '_';
        boolean rightOk = end == text.length()
                || !Character.isLetterOrDigit(text.charAt(end)) && text.charAt(end) != '_';
        return leftOk && rightOk;
    }

    /**
     * Index of the first top-level {@code BEGIN} keyword, or {@code -1}. "Top-level" means not
     * inside a single-quoted literal, a {@code --} line comment, a {@code /* … *}{@code /} block
     * comment, nor nested in parentheses — so the {@code BEGIN} that opens the trigger's own block
     * is found (not one inside a {@code WHEN (…)} guard or a string).
     */
    private static int indexOfTopLevelBegin(String text) {
        int paren = 0;
        int i = 0;
        int n = text.length();
        while (i < n) {
            char c = text.charAt(i);
            if (c == '\'') {                                   // single-quoted literal
                i++;
                while (i < n) {
                    if (text.charAt(i) == '\'') {
                        if (i + 1 < n && text.charAt(i + 1) == '\'') { i += 2; continue; }
                        i++;
                        break;
                    }
                    i++;
                }
                continue;
            }
            if (c == '-' && i + 1 < n && text.charAt(i + 1) == '-') {   // line comment
                i += 2;
                while (i < n && text.charAt(i) != '\n') i++;
                continue;
            }
            if (c == '/' && i + 1 < n && text.charAt(i + 1) == '*') {   // block comment
                i += 2;
                while (i + 1 < n && !(text.charAt(i) == '*' && text.charAt(i + 1) == '/')) i++;
                i = Math.min(n, i + 2);
                continue;
            }
            if (c == '(') { paren++; i++; continue; }
            if (c == ')') { if (paren > 0) paren--; i++; continue; }
            if (paren == 0 && (c == 'B' || c == 'b') && keywordAt(text, i, "BEGIN")) return i;
            i++;
        }
        return -1;
    }

    /**
     * Remove the {@code TRIGGER <name> … ON <table> [REFERENCING …] [FOR EACH ROW] [WHEN (…)]}
     * header from a trigger text, returning the statement body that follows. Only used for the
     * rare header-with-no-BEGIN shape; the normal header+BEGIN case is cut at the BEGIN instead.
     */
    private static String stripTriggerHeader(String text) {
        Matcher m = TRIGGER_HEADER.matcher(text);
        return m.find() ? text.substring(m.end()) : text;
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
