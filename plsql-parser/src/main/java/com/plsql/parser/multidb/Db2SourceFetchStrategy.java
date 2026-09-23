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
        String sql =
                "SELECT TEXT FROM SYSCAT.ROUTINES " +
                "WHERE ROUTINENAME = ? " +
                (schema != null ? "AND ROUTINESCHEMA = ? " : "") +
                "AND ROUTINETYPE IN ('P', 'F') " +
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

    @Override
    public List<ObjectRef> listObjects(Connection conn, String schema) throws SQLException {
        String sql = schema != null
                ? "SELECT ROUTINENAME, ROUTINETYPE, ROUTINESCHEMA " +
                  "FROM SYSCAT.ROUTINES " +
                  "WHERE ROUTINESCHEMA = ? AND ROUTINETYPE IN ('P', 'F') " +
                  "ORDER BY ROUTINESCHEMA, ROUTINENAME"
                : "SELECT ROUTINENAME, ROUTINETYPE, ROUTINESCHEMA " +
                  "FROM SYSCAT.ROUTINES " +
                  "WHERE ROUTINETYPE IN ('P', 'F') " +
                  "ORDER BY ROUTINESCHEMA, ROUTINENAME";

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
                            rs.getString("ROUTINESCHEMA")));
                }
            }
        }
        return result;
    }
}
