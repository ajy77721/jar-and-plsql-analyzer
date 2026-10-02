package com.plsql.parser.multidb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Fetches MySQL stored procedure / function source via INFORMATION_SCHEMA.ROUTINES.
 */
public class MySqlSourceFetchStrategy implements SourceFetchStrategy {

    /**
     * MySQL source is fetched whole (SHOW CREATE PROCEDURE/FUNCTION and
     * information_schema.routines.routine_definition is a complete LONGTEXT), so it is never
     * truncated. Returning false lets genuine parse errors surface instead of being masked.
     */
    @Override
    public boolean isTruncated(String source) {
        return false;
    }

    private static final Logger log = LoggerFactory.getLogger(MySqlSourceFetchStrategy.class);

    @Override
    public String fetchSource(Connection conn, String objectName, String objectType,
                              String schema) throws SQLException {
        String routineType = toMySqlRoutineType(objectType);
        // Use SHOW CREATE PROCEDURE/FUNCTION — returns full DDL including parameters and DEFINER,
        // which lets MySqlProcedureParser match SIG_PATTERN and extract params correctly.
        // The 3rd result-set column is "Create Procedure" or "Create Function".
        String safeName = objectName.replace("`", "``");
        // Qualify with the routine's database when known so CROSS-DATABASE calls
        // (e.g. analyzer_ref.usp_ref_log while connected to analyzerdb) resolve — otherwise
        // SHOW CREATE runs against the current DB only and the routine is "not found".
        String qualName = (schema != null && !schema.isBlank())
                ? "`" + schema.replace("`", "``") + "`.`" + safeName + "`"
                : "`" + safeName + "`";
        String showSql = "PROCEDURE".equals(routineType)
                ? "SHOW CREATE PROCEDURE " + qualName
                : "SHOW CREATE FUNCTION " + qualName;

        try (java.sql.Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(showSql)) {
            if (rs.next()) {
                String ddl = rs.getString(3); // "Create Procedure" / "Create Function" column
                if (ddl != null && !ddl.isBlank()) return ddl;
            }
        } catch (SQLException e) {
            if (!(e.getErrorCode() == 1305
                    || (e.getMessage() != null && e.getMessage().toLowerCase().contains("does not exist")))) {
                log.warn("SHOW CREATE {} failed for {}: {}", routineType, objectName, e.getMessage());
            }
        }
        // Fallback for CROSS-DATABASE routines: the routine isn't in the given/primary DB.
        // Find its actual database via information_schema, then SHOW CREATE there.
        try (java.sql.PreparedStatement ps = conn.prepareStatement(
                "SELECT ROUTINE_SCHEMA FROM information_schema.ROUTINES "
              + "WHERE ROUTINE_NAME = ? AND ROUTINE_TYPE = ? "
              + "AND ROUTINE_SCHEMA NOT IN ('mysql','information_schema','performance_schema','sys') LIMIT 1")) {
            ps.setString(1, objectName);
            ps.setString(2, routineType);
            try (ResultSet rs2 = ps.executeQuery()) {
                if (rs2.next()) {
                    String db = rs2.getString(1);
                    String q = "SHOW CREATE " + routineType + " `" + db.replace("`", "``") + "`.`" + safeName + "`";
                    try (java.sql.Statement st2 = conn.createStatement();
                         ResultSet rs3 = st2.executeQuery(q)) {
                        if (rs3.next()) {
                            String ddl = rs3.getString(3);
                            if (ddl != null && !ddl.isBlank()) return ddl;
                        }
                    }
                }
            }
        } catch (SQLException e) {
            log.debug("MySQL cross-DB routine lookup failed for {}: {}", objectName, e.getMessage());
        }
        return null;
    }

    @Override
    public List<ObjectRef> listObjects(Connection conn, String schema) throws SQLException {
        String sql = schema != null
                ? "SELECT ROUTINE_NAME, ROUTINE_TYPE, ROUTINE_SCHEMA FROM INFORMATION_SCHEMA.ROUTINES "
                + "WHERE ROUTINE_SCHEMA = ? ORDER BY ROUTINE_TYPE, ROUTINE_NAME"
                : "SELECT ROUTINE_NAME, ROUTINE_TYPE, ROUTINE_SCHEMA FROM INFORMATION_SCHEMA.ROUTINES "
                + "ORDER BY ROUTINE_SCHEMA, ROUTINE_TYPE, ROUTINE_NAME";

        List<ObjectRef> result = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            if (schema != null) ps.setString(1, schema);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(new ObjectRef(
                            rs.getString("ROUTINE_NAME"),
                            rs.getString("ROUTINE_TYPE"),
                            rs.getString("ROUTINE_SCHEMA")));
                }
            }
        }
        return result;
    }

    private String toMySqlRoutineType(String objectType) {
        if (objectType == null) return "PROCEDURE";
        return objectType.toUpperCase().contains("FUNCTION") ? "FUNCTION" : "PROCEDURE";
    }

    // ── Scheduled jobs (MySQL EVENT) ─────────────────────────────────────────────────────────
    // MySQL's scheduler is the EVENT object. information_schema.EVENTS carries the schedule
    // (EXECUTE_AT for a one-shot, INTERVAL_VALUE/INTERVAL_FIELD for a recurring one), the status
    // and the DO body (EVENT_DEFINITION). The body is what the crawler parses like a routine, so
    // a CALL inside it yields the event→proc SCHEDULES edge. The node type is EVENT, not JOB.
    @Override
    public List<JobRef> listJobs(Connection conn, String schema) throws SQLException {
        String sql = schema != null
                ? "SELECT EVENT_SCHEMA, EVENT_NAME, EVENT_DEFINITION, EXECUTE_AT, INTERVAL_VALUE, "
                + "INTERVAL_FIELD, STATUS FROM INFORMATION_SCHEMA.EVENTS WHERE EVENT_SCHEMA = ? "
                + "ORDER BY EVENT_NAME"
                : "SELECT EVENT_SCHEMA, EVENT_NAME, EVENT_DEFINITION, EXECUTE_AT, INTERVAL_VALUE, "
                + "INTERVAL_FIELD, STATUS FROM INFORMATION_SCHEMA.EVENTS ORDER BY EVENT_SCHEMA, EVENT_NAME";
        List<JobRef> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            if (schema != null) ps.setString(1, schema);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString("EVENT_NAME");
                    if (name == null) continue;
                    String body = rs.getString("EVENT_DEFINITION");
                    String intervalValue = rs.getString("INTERVAL_VALUE");
                    String intervalField = rs.getString("INTERVAL_FIELD");
                    String executeAt = rs.getString("EXECUTE_AT");
                    String schedule = (intervalValue != null && !intervalValue.isBlank())
                            ? "EVERY " + intervalValue + " " + intervalField
                            : (executeAt != null ? "AT " + executeAt : null);
                    String status = rs.getString("STATUS");
                    Boolean enabled = status == null ? null : "ENABLED".equalsIgnoreCase(status);
                    out.add(new JobRef(name, rs.getString("EVENT_SCHEMA"), schedule, enabled,
                            JobActionParser.targetFromAction(body), body, "EVENT"));
                }
            }
        }
        return out;
    }
}
