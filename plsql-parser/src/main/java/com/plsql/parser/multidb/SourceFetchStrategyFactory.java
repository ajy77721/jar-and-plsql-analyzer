package com.plsql.parser.multidb;

import com.plsqlanalyzer.config.DbType;

public final class SourceFetchStrategyFactory {

    private static final SourceFetchStrategy ORACLE     = new OracleSourceFetchStrategy();
    private static final SourceFetchStrategy MYSQL      = new MySqlSourceFetchStrategy();
    private static final SourceFetchStrategy SQL_SERVER = new SqlServerSourceFetchStrategy();
    private static final SourceFetchStrategy DB2        = new Db2SourceFetchStrategy();
    private static final SourceFetchStrategy POSTGRES   = new PostgresSourceFetchStrategy();

    private SourceFetchStrategyFactory() {}

    public static SourceFetchStrategy get(DbType dbType) {
        if (dbType == null) return ORACLE;
        switch (dbType) {
            case MYSQL:      return MYSQL;
            case SQLSERVER:  return SQL_SERVER;
            case DB2:        return DB2;
            case POSTGRES:   return POSTGRES;
            default:         return ORACLE;
        }
    }

    public static SourceFetchStrategy get(String dbTypeStr) {
        return get(DbType.fromString(dbTypeStr));
    }
}
