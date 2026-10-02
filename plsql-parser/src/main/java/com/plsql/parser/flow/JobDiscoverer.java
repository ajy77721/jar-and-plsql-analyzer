package com.plsql.parser.flow;

import com.plsql.parser.PlSqlParserEngine;
import com.plsql.parser.model.CallInfo;
import com.plsql.parser.model.ParseResult;
import com.plsql.parser.model.ParsedObject;
import com.plsql.parser.model.SubprogramInfo;
import com.plsql.parser.multidb.SourceFetchStrategy;
import com.plsql.parser.multidb.SourceFetchStrategyFactory;
import com.plsql.parser.multidb.StoredProcedureParser;
import com.plsql.parser.multidb.StoredProcedureParserFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Discovers SCHEDULED JOBS from each dialect's scheduler catalog, after the routine crawl.
 *
 * <p>Scheduled jobs are the roots of many estates (Oracle DBMS_SCHEDULER / DBMS_JOB, SQL Server
 * Agent, pg_cron, MySQL EVENT) but were previously invisible — DBMS_SCHEDULER was suppressed as a
 * built-in, and the catalogs were never queried. This class is the counterpart of
 * {@link TriggerDiscoverer}: a post-crawl, purely-additive discovery step that never touches the
 * BFS. It asks the dialect's {@link SourceFetchStrategy#listJobs} for the jobs, then parses each
 * job's ACTION like a procedure (the same grammar engine the crawler uses for routines) so the
 * job's outbound calls — and therefore its target procedure — are the grammar's, not a regex's.
 *
 * <p>Honest degradation: a dialect or catalog that is not present (pg_cron not installed, no
 * privileges) yields an empty list, logged non-fatally. Discovery must never fail the crawl.
 */
public class JobDiscoverer {

    private static final Logger log = LoggerFactory.getLogger(JobDiscoverer.class);

    private final DbConnectionManager connManager;
    private final SchemaResolver schemaResolver;
    private final SourceDownloader downloader;

    public JobDiscoverer(DbConnectionManager connManager, SchemaResolver schemaResolver,
                         SourceDownloader downloader) {
        this.connManager = connManager;
        this.schemaResolver = schemaResolver;
        this.downloader = downloader;
    }

    /**
     * Discover all scheduled jobs visible to this connection. Never null; never throws.
     * Each map carries: {@code name, schema, schedule, enabled, target, targetSchema, action,
     * type, calls} (a {@code List<CallInfo>}).
     */
    public List<Map<String, Object>> discover() {
        List<Map<String, Object>> out = new ArrayList<>();
        if (connManager == null) return out;

        String dbType = dbTypeOf();
        SourceFetchStrategy strategy = SourceFetchStrategyFactory.get(dbType);
        Connection conn = null;
        List<SourceFetchStrategy.JobRef> jobs;
        try {
            conn = connManager.getAnyConnection();
            jobs = strategy.listJobs(conn, schemaFor(dbType));
        } catch (Exception e) {
            log.info("[JobDiscoverer] scheduler catalog unavailable (non-fatal): {}", e.getMessage());
            return out;
        } finally {
            if (conn != null) connManager.returnToPool(conn);
        }
        if (jobs == null || jobs.isEmpty()) return out;

        for (SourceFetchStrategy.JobRef j : jobs) {
            if (j == null || j.name == null || j.name.isBlank()) continue;
            String action = j.action;
            if ((action == null || action.isBlank()) && connManager.isOracle()) {
                // Oracle jobs whose action is a named program: the block lives in USER_SOURCE
                // type 'JOB' (per the task spec). Fetch it separately.
                try {
                    conn = connManager.getAnyConnection();
                    action = strategy.fetchJobAction(conn, j.name, j.schema);
                } catch (Exception e) {
                    log.debug("[JobDiscoverer] job action fetch failed for {}: {}", j.name, e.getMessage());
                } finally {
                    if (conn != null) { connManager.returnToPool(conn); conn = null; }
                }
            }

            List<CallInfo> calls = parseActionCalls(dbType, j, action);
            String target = j.target;
            String targetSchema = null;
            if (calls != null && !calls.isEmpty()) {
                CallInfo c = calls.get(0);
                if (c.getName() != null && !c.getName().isBlank()) {
                    target = c.getPackageName() != null && !c.getPackageName().isBlank()
                            ? c.getPackageName() + "." + c.getName() : c.getName();
                    targetSchema = c.getSchema();
                }
            } else if (target != null && target.contains(".")) {
                String[] parts = target.split("\\.");
                if (parts.length >= 2) {
                    target = parts[parts.length - 2] + "." + parts[parts.length - 1];
                }
            }

            Map<String, Object> m = new LinkedHashMap<>();
            m.put("name", j.name);
            m.put("schema", j.schema);
            m.put("schedule", j.schedule);
            m.put("enabled", j.enabled);
            m.put("target", target);
            m.put("targetSchema", targetSchema);
            m.put("action", action);
            m.put("type", j.type);
            m.put("calls", calls == null ? new ArrayList<CallInfo>() : calls);
            out.add(m);
        }
        log.info("[JobDiscoverer] discovered {} scheduled job(s)", out.size());
        return out;
    }

    private String dbTypeOf() {
        if (connManager.isMySQL()) return "MYSQL";
        if (connManager.isSqlServer()) return "SQLSERVER";
        if (connManager.isDB2()) return "DB2";
        if (connManager.isPostgres()) return "POSTGRES";
        return "ORACLE";
    }

    private String schemaFor(String dbType) {
        switch (dbType) {
            case "MYSQL":     return connManager.getMySqlSchema();
            case "SQLSERVER": return connManager.getSqlServerSchema();
            case "POSTGRES":  return connManager.getPostgresSchema();
            case "DB2":       return connManager.getDb2Schema();
            default:          return null; // Oracle: ALL_SCHEDULER_JOBS across accessible schemas
        }
    }

    /**
     * Parse a job's ACTION like a procedure, returning its outbound calls. Best-effort: an action
     * the grammar cannot parse yields an empty list (the target then falls back to the catalog's
     * own hint), never an exception.
     */
    private List<CallInfo> parseActionCalls(String dbType, SourceFetchStrategy.JobRef job,
                                            String action) {
        if (action == null || action.isBlank()) return new ArrayList<>();
        try {
            List<ParsedObject> objects = parseAction(dbType, job, action);
            if (objects == null || objects.isEmpty()) return new ArrayList<>();
            List<CallInfo> calls = new ArrayList<>();
            for (ParsedObject po : objects) {
                if (po.getCalls() != null) calls.addAll(po.getCalls());
                if (po.getSubprograms() != null) {
                    for (SubprogramInfo sub : po.getSubprograms()) {
                        if (sub.getCalls() != null) calls.addAll(sub.getCalls());
                    }
                }
            }
            return calls;
        } catch (Exception | StackOverflowError e) {
            log.debug("[JobDiscoverer] action parse failed for {}: {}", job.name, e.getMessage());
            return new ArrayList<>();
        }
    }

    private List<ParsedObject> parseAction(String dbType, SourceFetchStrategy.JobRef job,
                                           String action) {
        String safeName = sanitize(job.name);
        if ("MYSQL".equals(dbType)) {
            // information_schema.EVENTS.EVENT_DEFINITION is the DO body only; rebuild a CREATE
            // EVENT around it so the MySQL grammar (grammar-first) captures its CALL/DML.
            String body = action.strip();
            String ddl = "CREATE EVENT " + safeName + " ON SCHEDULE EVERY 1 DAY DO "
                    + (body.toUpperCase().startsWith("BEGIN") ? body : "BEGIN " + body + " END");
            return objectsOf(StoredProcedureParserFactory.get("MYSQL"), ddl, safeName, job.schema);
        }
        if ("SQLSERVER".equals(dbType)) {
            String ddl = action.stripLeading().toUpperCase().startsWith("CREATE")
                    ? action
                    : "CREATE PROCEDURE " + safeName + " AS\nBEGIN\n" + action + "\nEND";
            return objectsOf(StoredProcedureParserFactory.get("SQLSERVER"), ddl, safeName, job.schema);
        }
        if ("POSTGRES".equals(dbType)) {
            String ddl = action.stripLeading().toUpperCase().startsWith("CREATE")
                    ? action
                    : "CREATE PROCEDURE " + safeName + "() LANGUAGE plpgsql AS $$\nBEGIN\n"
                      + action + ";\nEND $$";
            return objectsOf(StoredProcedureParserFactory.get("POSTGRES"), ddl, safeName, job.schema);
        }
        if ("DB2".equals(dbType)) {
            String ddl = action.stripLeading().toUpperCase().startsWith("CREATE")
                    ? action
                    : "CREATE PROCEDURE " + safeName + "() BEGIN " + action + " END";
            return objectsOf(StoredProcedureParserFactory.get("DB2"), ddl, safeName, job.schema);
        }
        // Oracle: wrap the action as a synthetic procedure body. A bare `pkg.proc;` and a full
        // `BEGIN … END;` block are both legal inside the wrapper (a nested block is legal PL/SQL).
        String src = action.stripLeading().toUpperCase().startsWith("CREATE")
                ? action
                : "CREATE OR REPLACE PROCEDURE " + safeName + " IS\nBEGIN\n" + action + "\nEND;\n";
        ParseResult result = new PlSqlParserEngine().parseContent(src, "job_action.sql");
        if (result == null || result.getObjects() == null) return new ArrayList<>();
        return result.getObjects();
    }

    private List<ParsedObject> objectsOf(StoredProcedureParser parser, String ddl,
                                         String name, String schema) {
        if (parser == null) return new ArrayList<>();
        ParseResult result = parser.parse(ddl, name, schema);
        if (result == null || result.getObjects() == null) return new ArrayList<>();
        return result.getObjects();
    }

    private static String sanitize(String name) {
        if (name == null || name.isBlank()) return "JOB";
        String s = name.toUpperCase().replaceAll("[^A-Z0-9_$]", "_");
        return s.isEmpty() ? "JOB" : s;
    }
}
