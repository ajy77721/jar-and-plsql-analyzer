package com.plsql.parser.multidb;

import com.plsqlanalyzer.config.DbType;

public final class StoredProcedureParserFactory {

    private static final StoredProcedureParser MYSQL      = new MySqlProcedureParser();
    private static final StoredProcedureParser SQL_SERVER = new SqlServerProcedureParser();
    private static final StoredProcedureParser DB2        = new Db2ProcedureParser();
    private static final StoredProcedureParser POSTGRES   = new PostgresProcedureParser();

    private StoredProcedureParserFactory() {}

    /**
     * Returns the correct parser for the given DB type.
     * Oracle returns null — use PlSqlParserEngine directly.
     */
    public static StoredProcedureParser get(DbType dbType) {
        if (dbType == null) return null;
        switch (dbType) {
            case MYSQL:     return MYSQL;
            case SQLSERVER: return SQL_SERVER;
            case DB2:       return DB2;
            case POSTGRES:  return POSTGRES;
            default:        return null; // Oracle uses PlSqlParserEngine
        }
    }

    public static StoredProcedureParser get(String dbTypeStr) {
        return get(DbType.fromString(dbTypeStr));
    }

    public static boolean supportsDbType(DbType dbType) {
        return dbType == DbType.MYSQL || dbType == DbType.SQLSERVER
                || dbType == DbType.DB2 || dbType == DbType.POSTGRES;
    }
}
