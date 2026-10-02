package com.plsql.parser.multidb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Fetches IBM DB2 stored procedure / function source via SYSCAT.ROUTINES.
 *
 * Primary catalog: SYSCAT.ROUTINES (DB2 LUW 9.7+)
 * The TEXT column holds the full CREATE [OR REPLACE] PROCEDURE/FUNCTION DDL.
 */
public class Db2SourceFetchStrategy implements SourceFetchStrategy {

    /**
     * DB2 source is read whole from SYSCAT.PROCEDURES/FUNCTIONS/ROUTINES.TEXT (a complete CLOB),
     * so it is never truncated. Returning false lets genuine parse errors surface instead of
     * being masked.
     */
    @Override
    public boolean isTruncated(String source) {
        return false;
    }

    private static final Logger log = LoggerFactory.getLogger(Db2SourceFetchStrategy.class);

    @Override
    public String fetchSource(Connection conn, String objectName, String objectType,
                              String schema) throws SQLException {
        // ROUTINEMODULENAME is selected so a module member's owning module is available to callers
        // that key it as MODULE.SUB (task #28); it is null for a non-module routine.
        String sql =
                "SELECT TEXT, ROUTINEMODULENAME FROM SYSCAT.ROUTINES " +
                "WHERE ROUTINENAME = ? " +
                (schema != null ? "AND ROUTINESCHEMA = ? " : "") +
                "AND ROUTINETYPE IN ('P', 'F') " +
                "ORDER BY SPECIFICNAME " +
                "FETCH FIRST 1 ROWS ONLY";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, objectName.toUpperCase());
            if (schema != null) ps.setString(2, schema.toUpperCase());

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String text = rs.getString("TEXT");
                    return (text != null && !text.isBlank()) ? text : null;
                }
                log.debug("DB2: routine not found in SYSCAT.ROUTINES — {}.{}", schema, objectName);
                return null;
            }
        } catch (SQLException e) {
            log.warn("DB2 SYSCAT.ROUTINES fetch failed for {}.{}: {}", schema, objectName, e.getMessage());
            throw e;
        }
    }

    /**
     * Fetch ONE exact overload by its {@code SPECIFICNAME} — unique per routine in DB2, so it
     * addresses the exact overload where ROUTINENAME alone cannot (name-based fetch would return
     * an arbitrary one via {@code FETCH FIRST 1 ROWS ONLY}).
     */
    @Override
    public String fetchSourceBySpecificName(Connection conn, String specificName, String schema)
            throws SQLException {
        if (specificName == null || specificName.isBlank()) return null;
        String sql = "SELECT TEXT, ROUTINEMODULENAME FROM SYSCAT.ROUTINES WHERE SPECIFICNAME = ? "
                + (schema != null ? "AND ROUTINESCHEMA = ? " : "")
                + "FETCH FIRST 1 ROWS ONLY";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, specificName);
            if (schema != null) ps.setString(2, schema.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String text = rs.getString("TEXT");
                    return (text != null && !text.isBlank()) ? text : null;
                }
                return null;
            }
        } catch (SQLException e) {
            log.warn("DB2 SYSCAT.ROUTINES fetch by specific name {} failed: {}", specificName, e.getMessage());
            throw e;
        }
    }

    @Override
    public List<ObjectRef> listObjects(Connection conn, String schema) throws SQLException {
        // SPECIFICNAME comes back with each row so the crawler can key every overload distinctly
        // (it is unique per routine; ROUTINENAME is not) and fetch it by specific name.
        // ROUTINEMODULENAME (task #28) identifies a module member's owning module; the crawler
        // uses it as the package slot so the member keys as MODULE.SUB. It is null for a
        // non-module routine, so the key stays the historical standalone one.
        String sql = schema != null
                ? "SELECT ROUTINENAME, ROUTINETYPE, ROUTINESCHEMA, SPECIFICNAME, ROUTINEMODULENAME " +
                  "FROM SYSCAT.ROUTINES " +
                  "WHERE ROUTINESCHEMA = ? AND ROUTINETYPE IN ('P', 'F') " +
                  "ORDER BY ROUTINESCHEMA, ROUTINENAME, SPECIFICNAME"
                : "SELECT ROUTINENAME, ROUTINETYPE, ROUTINESCHEMA, SPECIFICNAME, ROUTINEMODULENAME " +
                  "FROM SYSCAT.ROUTINES " +
                  "WHERE ROUTINETYPE IN ('P', 'F') " +
                  "ORDER BY ROUTINESCHEMA, ROUTINENAME, SPECIFICNAME";

        List<ObjectRef> result = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            if (schema != null) ps.setString(1, schema.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String typeCode = rs.getString("ROUTINETYPE");
                    String normalizedType = "F".equals(typeCode) ? "FUNCTION" : "PROCEDURE";
                    result.add(new ObjectRef(
                            rs.getString("ROUTINENAME"),
                            normalizedType,
                            rs.getString("ROUTINESCHEMA"),
                            null,
                            null,
                            rs.getString("SPECIFICNAME"),
                            rs.getString("ROUTINEMODULENAME")));
                }
            }
        }
        return result;
    }
}
