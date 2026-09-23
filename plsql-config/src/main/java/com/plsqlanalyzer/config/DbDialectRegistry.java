package com.plsqlanalyzer.config;

public final class DbDialectRegistry {

    private static final DbDialect ORACLE     = new OracleDialect();
    private static final DbDialect MYSQL      = new MySqlDialect();
    private static final DbDialect SQL_SERVER = new SqlServerDialect();
    private static final DbDialect DB2        = new Db2Dialect();
    private static final DbDialect POSTGRES   = new PostgresDialect();

    private DbDialectRegistry() {}

    public static DbDialect get(DbType type) {
        if (type == null) return ORACLE;
        switch (type) {
            case MYSQL:      return MYSQL;
            case SQLSERVER:  return SQL_SERVER;
            case DB2:        return DB2;
            case POSTGRES:   return POSTGRES;
            default:         return ORACLE;
        }
    }

    public static DbDialect get(String dbTypeStr) {
        return get(DbType.fromString(dbTypeStr));
    }
}
