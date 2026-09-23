package com.plsqlanalyzer.web.util;

/**
 * Small helper to detect the SQL dialect from a JDBC URL prefix and to
 * produce consistent "not yet supported" messages for Oracle-only subsystems.
 *
 * Phase 1: used only to guard non-Oracle connections so they fail gracefully
 * instead of throwing raw Oracle-syntax errors. Oracle behavior is unchanged.
 */
public final class DbDialectSupport {

    public static final String ORACLE = "ORACLE";
    public static final String SQLSERVER = "SQLSERVER";
    public static final String MYSQL = "MYSQL";
    public static final String DB2 = "DB2";
    public static final String POSTGRES = "POSTGRES";
    public static final String UNKNOWN = "UNKNOWN";

    private DbDialectSupport() {
    }

    /** Returns ORACLE/SQLSERVER/MYSQL/DB2/UNKNOWN based on the JDBC URL prefix. */
    public static String dialectFromJdbcUrl(String jdbcUrl) {
        if (jdbcUrl == null) {
            return UNKNOWN;
        }
        String url = jdbcUrl.trim().toLowerCase();
        if (url.startsWith("jdbc:oracle")) {
            return ORACLE;
        }
        if (url.startsWith("jdbc:sqlserver")) {
            return SQLSERVER;
        }
        if (url.startsWith("jdbc:mysql")) {
            return MYSQL;
        }
        if (url.startsWith("jdbc:db2")) {
            return DB2;
        }
        if (url.startsWith("jdbc:postgresql")) {
            return POSTGRES;
        }
        return UNKNOWN;
    }

    public static boolean isOracle(String jdbcUrl) {
        return ORACLE.equals(dialectFromJdbcUrl(jdbcUrl));
    }

    /** e.g. unsupportedMessage("Database browse", "SQLSERVER"). */
    public static String unsupportedMessage(String feature, String dialect) {
        return feature + " is not yet supported for " + dialect;
    }
}
