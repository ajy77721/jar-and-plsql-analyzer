package com.plsql.parser.multidb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Fetches SQL Server stored procedure / function source via sys.sql_modules.
 */
public class SqlServerSourceFetchStrategy implements SourceFetchStrategy {

    /**
     * SQL Server source comes from sys.sql_modules.definition (a complete NVARCHAR(MAX) /
     * OBJECT_DEFINITION), so it is never truncated. Returning false lets genuine parse errors
     * surface instead of being masked.
     */
    @Override
    public boolean isTruncated(String source) {
        return false;
    }

    private static final Logger log = LoggerFactory.getLogger(SqlServerSourceFetchStrategy.class);

    @Override
    public String fetchSource(Connection conn, String objectName, String objectType,
                              String schema) throws SQLException {
        // Build qualified name for OBJECT_ID lookup
        String qualifiedName = schema != null
                ? "[" + schema + "].[" + objectName + "]"
                : objectName;

        // OBJECT_DEFINITION is the simplest path; falls back to sys.sql_modules for large objects
        String sql =
                "SELECT m.definition " +
                "FROM sys.sql_modules m " +
                "JOIN sys.objects o ON m.object_id = o.object_id " +
                "WHERE o.name = ? " +
                (schema != null ? "AND SCHEMA_NAME(o.schema_id) = ? " : "") +
                "AND o.type IN ('P', 'FN', 'IF', 'TF', 'V', 'TR')";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, objectName);
            if (schema != null) ps.setString(2, schema);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String def = rs.getString("definition");
                    return (def != null && !def.isBlank()) ? def : null;
                }
                log.debug("SQL Server: object not found in sys.sql_modules — {}", qualifiedName);
                return null;
            }
        } catch (SQLException e) {
            log.warn("SQL Server sys.sql_modules fetch failed for {}: {}", qualifiedName, e.getMessage());
            throw e;
        }
    }

    /** Resolves the schema (owner) of a routine by name, searching all schemas. */
    public String findSchema(Connection conn, String objectName) throws SQLException {
        String sql = "SELECT TOP 1 SCHEMA_NAME(o.schema_id) AS schema_name "
                + "FROM sys.objects o WHERE o.name = ? "
                + "AND o.type IN ('P','FN','IF','TF','V','TR')";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, objectName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getString("schema_name");
            }
        }
        return null;
    }

    @Override
    public List<ObjectRef> listObjects(Connection conn, String schema) throws SQLException {
        String sql = schema != null
                ? "SELECT o.name, o.type_desc, SCHEMA_NAME(o.schema_id) AS schema_name "
                + "FROM sys.objects o "
                + "WHERE o.type IN ('P','FN','IF','TF') AND SCHEMA_NAME(o.schema_id) = ? "
                + "ORDER BY o.type_desc, o.name"
                : "SELECT o.name, o.type_desc, SCHEMA_NAME(o.schema_id) AS schema_name "
                + "FROM sys.objects o "
                + "WHERE o.type IN ('P','FN','IF','TF') "
                + "ORDER BY schema_name, o.type_desc, o.name";

        List<ObjectRef> result = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            if (schema != null) ps.setString(1, schema);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String typeDesc = rs.getString("type_desc");
                    String normalizedType = typeDesc != null && typeDesc.contains("FUNCTION")
                            ? "FUNCTION" : "PROCEDURE";
                    result.add(new ObjectRef(
                            rs.getString("name"),
                            normalizedType,
                            rs.getString("schema_name")));
                }
            }
        }
        return result;
    }

    // ── Scheduled jobs (SQL Server Agent) ────────────────────────────────────────────────────
    // msdb.dbo.sysjobs holds the job; sysjobsteps holds the T-SQL command (step 1 is the usual
    // target); sysschedules/sysjobschedules hold the schedule. Jobs are server-scoped, not
    // schema-scoped, so the schema argument is ignored. The command is the action; its EXEC/CALL
    // target seeds the SCHEDULES edge, and it is also parsed like a routine by the crawler.
    @Override
    public List<JobRef> listJobs(Connection conn, String schema) throws SQLException {
        String sql =
                "SELECT j.name AS job_name, j.enabled AS job_enabled, "
              + "st.command AS step_command, s.name AS schedule_name, "
              + "s.freq_type, s.freq_interval, s.freq_subday_type, s.freq_subday_interval "
              + "FROM msdb.dbo.sysjobs j "
              + "LEFT JOIN msdb.dbo.sysjobsteps st ON st.job_id = j.job_id AND st.step_id = 1 "
              + "LEFT JOIN msdb.dbo.sysjobschedules js ON js.job_id = j.job_id "
              + "LEFT JOIN msdb.dbo.sysschedules s ON s.schedule_id = js.schedule_id "
              + "ORDER BY j.name";
        List<JobRef> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String name = rs.getString("job_name");
                if (name == null) continue;
                boolean enabled = rs.getBoolean("job_enabled");
                String command = rs.getString("step_command");
                String schedule = rs.getString("schedule_name");
                if (schedule == null || schedule.isBlank()) {
                    schedule = describeSqlAgentSchedule(rs);
                }
                out.add(new JobRef(name, "msdb", schedule, enabled,
                        JobActionParser.targetFromAction(command), command, "JOB"));
            }
        }
        return out;
    }

    /** Render a SQL Agent schedule row into a short human-readable string. */
    private static String describeSqlAgentSchedule(ResultSet rs) {
        try {
            int freqType = rs.getInt("freq_type");
            if (rs.wasNull()) return null;
            int interval = rs.getInt("freq_interval");
            return switch (freqType) {
                case 1 -> "ONCE";
                case 4 -> "DAILY every " + interval + " day(s)";
                case 8 -> "WEEKLY every " + interval + " week(s)";
                case 16 -> "MONTHLY day " + interval;
                case 32 -> "MONTHLY every " + interval + " month(s)";
                case 64 -> "ON SQL SERVER AGENT START";
                case 128 -> "WHEN IDLE";
                default -> "freq_type=" + freqType;
            };
        } catch (SQLException e) {
            return null;
        }
    }
}
