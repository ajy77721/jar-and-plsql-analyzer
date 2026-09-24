package com.plsqlanalyzer.config;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

public enum DbType {
    ORACLE,
    MYSQL,
    SQLSERVER,
    DB2,
    POSTGRES;

    /**
     * Standard ("well-known") TCP port per database type — the server-side authority for the
     * setup screen's DB-type auto-detection. Each value matches the type's
     * {@code DbDialect.getDefaultPort()}, so a connection that omits its port already lands here;
     * this table additionally lets the UI <em>suggest</em> a type from a port the operator typed.
     *
     * <p>Keyed ascending for a stable wire order (the UI renders the table as-is).
     */
    private static final Map<Integer, DbType> BY_STANDARD_PORT = Collections.unmodifiableMap(
            new TreeMap<>(Map.of(
                    1433,  SQLSERVER,
                    1521,  ORACLE,
                    3306,  MYSQL,
                    50000, DB2,
                    5432,  POSTGRES)));

    /** Database type whose standard port is {@code port}, or {@code null} when none matches. */
    public static DbType suggestForPort(int port) {
        return BY_STANDARD_PORT.get(port);
    }

    /**
     * The whole standard-port table (ascending port order, immutable) for clients that want the
     * mapping rather than a single lookup — returned by {@code GET /api/plsql/config/db-type-suggest}.
     */
    public static Map<Integer, DbType> standardPortMap() {
        return BY_STANDARD_PORT;
    }

    /** This type's standard port. Mirrors {@code DbDialect.getDefaultPort()} without needing the registry. */
    public int getStandardPort() {
        for (Map.Entry<Integer, DbType> e : BY_STANDARD_PORT.entrySet()) {
            if (e.getValue() == this) return e.getKey();
        }
        return 0;
    }

    /**
     * Convenience for the HTTP layer: the same table with string keys/values, ready to serialise as
     * JSON (Jackson would otherwise emit enum names anyway — this just keeps the key a JSON string).
     */
    public static Map<String, String> standardPortMapByName() {
        Map<String, String> byName = new LinkedHashMap<>();
        BY_STANDARD_PORT.forEach((port, type) -> byName.put(String.valueOf(port), type.name()));
        return byName;
    }

    public static DbType fromString(String s) {
        if (s == null || s.isBlank()) return ORACLE;
        switch (s.trim().toUpperCase()) {
            case "MYSQL":                   return MYSQL;
            case "SQLSERVER":
            case "MSSQL":
            case "SQL_SERVER":
            case "SQL SERVER":              return SQLSERVER;
            case "DB2":
            case "IBM_DB2":
            case "IBMDB2":                  return DB2;
            case "POSTGRES":
            case "POSTGRESQL":
            case "POSTGRE":
            case "PG":                      return POSTGRES;
            default:                        return ORACLE;
        }
    }
}
