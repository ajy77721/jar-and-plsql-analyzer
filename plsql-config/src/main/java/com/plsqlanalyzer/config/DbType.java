package com.plsqlanalyzer.config;

public enum DbType {
    ORACLE,
    MYSQL,
    SQLSERVER,
    DB2,
    POSTGRES;

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
