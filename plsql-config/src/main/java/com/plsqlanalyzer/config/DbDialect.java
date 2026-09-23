package com.plsqlanalyzer.config;

public interface DbDialect {
    String buildJdbcUrl(ConnectionConfig conn);
    int getDefaultPort();
    String getDriverClass();
    String getDisplayName();
}
